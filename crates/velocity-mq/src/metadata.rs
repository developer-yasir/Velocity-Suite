//! VelocityMQ Lock-Free Topic & Metadata Manager - Phase 5
//!
//! Provides zero-global-lock topic and partition-to-core mapping for thread-per-core
//! architectures. Uses Read-Copy-Update (RCU) via `arc-swap` to allow millions of
//! concurrent lock-free reads per second across all CPU cores while supporting
//! atomic metadata mutations (topic creation, partition rebalancing).

use arc_swap::ArcSwap;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Partition operational state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionState {
    Active,
    Rebalancing,
    Offline,
}

/// Metadata for an individual partition within a Kafka topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    /// Zero-based partition identifier within the topic.
    pub partition_id: i32,
    /// Hardware CPU core index / Event Loop ID responsible for this partition's WAL.
    pub assigned_core: usize,
    /// Current leader epoch for Kafka client compatibility.
    pub leader_epoch: i32,
    /// In-Sync Replica (ISR) core indexes.
    pub isr_cores: Vec<usize>,
    /// State of the partition.
    pub state: PartitionState,
}

/// Metadata for a Kafka topic containing all its configured partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    /// Name of the topic stored as zero-copy Bytes.
    pub name: Bytes,
    /// Partitions belonging to this topic, indexed by partition ID.
    pub partitions: Vec<PartitionMetadata>,
}

impl TopicMetadata {
    /// Creates a new topic metadata representation.
    pub fn new(name: Bytes, partition_count: usize, core_count: usize) -> Self {
        assert!(core_count > 0, "core_count must be at least 1");
        let mut partitions = Vec::with_capacity(partition_count);
        for p in 0..partition_count {
            // Round-robin partition distribution across available hardware cores
            let assigned_core = p % core_count;
            partitions.push(PartitionMetadata {
                partition_id: p as i32,
                assigned_core,
                leader_epoch: 0,
                isr_cores: vec![assigned_core],
                state: PartitionState::Active,
            });
        }
        Self { name, partitions }
    }

    /// Retrieves partition metadata by ID.
    #[inline(always)]
    pub fn get_partition(&self, partition_id: i32) -> Option<&PartitionMetadata> {
        if partition_id >= 0 && (partition_id as usize) < self.partitions.len() {
            Some(&self.partitions[partition_id as usize])
        } else {
            None
        }
    }
}

/// Immutable snapshot of the global routing table.
/// Read millions of times per second concurrently by all network threads.
#[derive(Debug, Clone)]
pub struct RoutingTable {
    /// Topic map keyed by topic name.
    topics: HashMap<Bytes, TopicMetadata>,
    /// Monotonically increasing generation number incremented on every RCU update.
    pub generation: usize,
}

impl RoutingTable {
    /// Creates an empty routing table.
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
            generation: 0,
        }
    }

    /// Resolves the hardware CPU core ID owning a specific topic partition.
    /// Lock-free, wait-free lookup.
    #[inline(always)]
    pub fn get_partition_core(&self, topic: &[u8], partition_id: i32) -> Option<usize> {
        self.topics
            .get(topic)
            .and_then(|t| t.get_partition(partition_id))
            .filter(|p| p.state == PartitionState::Active)
            .map(|p| p.assigned_core)
    }

    /// Looks up full topic metadata.
    #[inline(always)]
    pub fn get_topic(&self, topic: &[u8]) -> Option<&TopicMetadata> {
        self.topics.get(topic)
    }

    /// Returns true if the topic exists.
    #[inline(always)]
    pub fn contains_topic(&self, topic: &[u8]) -> bool {
        self.topics.contains_key(topic)
    }

    /// Total number of topics registered in this routing table.
    #[inline(always)]
    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }

    /// Returns all registered topics.
    #[inline(always)]
    pub fn all_topics(&self) -> &HashMap<Bytes, TopicMetadata> {
        &self.topics
    }
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Lock-Free Metadata Manager for VelocityMQ.
///
/// Bypasses `Arc<RwLock<HashMap>>` completely using Read-Copy-Update (RCU).
/// - Reads: Wait-free atomic pointer load via `ArcSwap::load()`.
/// - Writes: Atomic compare-and-swap / store of new immutable `RoutingTable` snapshots.
pub struct MetadataManager {
    /// RCU pointer to current active routing table.
    table: ArcSwap<RoutingTable>,
    /// Generation counter for cache invalidation tracking.
    generation: AtomicUsize,
    /// Total hardware CPU cores configured in the system.
    num_cores: usize,
}

impl MetadataManager {
    /// Creates a new MetadataManager for a given number of hardware CPU cores.
    pub fn new(num_cores: usize) -> Self {
        assert!(num_cores > 0, "num_cores must be at least 1");
        Self {
            table: ArcSwap::from_pointee(RoutingTable::new()),
            generation: AtomicUsize::new(0),
            num_cores,
        }
    }

    /// Resolves the CPU core ID owning a topic partition.
    ///
    /// # Performance
    /// Completely lock-free. Millions of concurrent calls per second execute with zero
    /// lock contention or cache-line ping-pong across CPU cores.
    #[inline(always)]
    pub fn lookup_partition_core(&self, topic: &[u8], partition_id: i32) -> Option<usize> {
        let guard = self.table.load();
        guard.get_partition_core(topic, partition_id)
    }

    /// Returns a full snapshot copy of topic metadata if it exists.
    #[inline(always)]
    pub fn get_topic_metadata(&self, topic: &[u8]) -> Option<TopicMetadata> {
        let guard = self.table.load();
        guard.get_topic(topic).cloned()
    }

    /// Returns the current routing table snapshot as an `Arc<RoutingTable>`.
    #[inline(always)]
    pub fn current_snapshot(&self) -> Arc<RoutingTable> {
        self.table.load_full()
    }

    /// Total number of configured CPU cores.
    #[inline(always)]
    pub fn num_cores(&self) -> usize {
        self.num_cores
    }

    /// Registers a new Kafka topic using RCU (Read-Copy-Update).
    ///
    /// 1. Loads current snapshot.
    /// 2. Clones the routing table and inserts the new topic.
    /// 3. Atomically publishes the new snapshot to `self.table`.
    pub fn register_topic(&self, topic_name: &[u8], partition_count: usize) {
        let topic_bytes = Bytes::copy_from_slice(topic_name);
        let new_topic = TopicMetadata::new(topic_bytes.clone(), partition_count, self.num_cores);

        let mut done = false;
        while !done {
            let current = self.table.load();
            let mut new_table = (**current).clone();
            new_table.topics.insert(topic_bytes.clone(), new_topic.clone());
            let next_gen = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
            new_table.generation = next_gen;

            let prev = self.table.compare_and_swap(&current, Arc::new(new_table));
            if Arc::ptr_eq(&prev, &current) {
                done = true;
            }
        }
    }

    /// Explicitly assigns a partition to a specific target hardware core (Partition Rebalance).
    /// Uses RCU to atomically update partition ownership across all threads.
    pub fn reassign_partition(&self, topic_name: &[u8], partition_id: i32, target_core: usize) -> bool {
        assert!(
            target_core < self.num_cores,
            "target_core {} exceeds available cores {}",
            target_core,
            self.num_cores
        );

        let mut success = false;
        let mut done = false;
        while !done {
            let current = self.table.load();
            let mut new_table = (**current).clone();

            if let Some(topic_meta) = new_table.topics.get_mut(topic_name) {
                if let Some(part_meta) = topic_meta.partitions.get_mut(partition_id as usize) {
                    part_meta.assigned_core = target_core;
                    part_meta.leader_epoch += 1;
                    success = true;
                } else {
                    return false;
                }
            } else {
                return false;
            }

            let next_gen = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
            new_table.generation = next_gen;

            let prev = self.table.compare_and_swap(&current, Arc::new(new_table));
            if Arc::ptr_eq(&prev, &current) {
                done = true;
            }
        }
        success
    }

    /// Unregisters/deletes a topic from the metadata routing table.
    pub fn unregister_topic(&self, topic_name: &[u8]) -> bool {
        let mut removed = false;
        let mut done = false;
        while !done {
            let current = self.table.load();
            let mut new_table = (**current).clone();

            if new_table.topics.remove(topic_name).is_some() {
                removed = true;
            } else {
                return false;
            }

            let next_gen = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
            new_table.generation = next_gen;

            let prev = self.table.compare_and_swap(&current, Arc::new(new_table));
            if Arc::ptr_eq(&prev, &current) {
                done = true;
            }
        }
        removed
    }

    /// Returns a list of all CPU cores that currently own at least one partition.
    pub fn active_cores(&self) -> Vec<usize> {
        let current = self.table.load();
        let mut cores = std::collections::BTreeSet::new();
        for topic in current.topics.values() {
            for part in &topic.partitions {
                if part.state == PartitionState::Active {
                    cores.insert(part.assigned_core);
                }
            }
        }
        cores.into_iter().collect()
    }

    /// Automatically registers a topic if it does not already exist (Developer Mode Auto-Topic Creation).
    /// Prevents blocking or errors when developers stream to new topics in local standalone mode.
    pub fn auto_create_topic_if_missing(&self, topic_name: &[u8], partitions: usize) {
        if self.get_topic_metadata(topic_name).is_none() {
            self.register_topic(topic_name, partitions.max(1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_lock_free_metadata_rcu_roundtrip() {
        let manager = MetadataManager::new(4);

        // Register two topics
        manager.register_topic(b"orders", 8);
        manager.register_topic(b"payments", 4);

        // Verify round-robin core assignments:
        // Core count = 4
        // orders partition 0 -> Core 0
        // orders partition 1 -> Core 1
        // orders partition 2 -> Core 2
        // orders partition 3 -> Core 3
        // orders partition 4 -> Core 0
        assert_eq!(manager.lookup_partition_core(b"orders", 0), Some(0));
        assert_eq!(manager.lookup_partition_core(b"orders", 1), Some(1));
        assert_eq!(manager.lookup_partition_core(b"orders", 2), Some(2));
        assert_eq!(manager.lookup_partition_core(b"orders", 3), Some(3));
        assert_eq!(manager.lookup_partition_core(b"orders", 4), Some(0));

        // Unknown partition / topic returns None
        assert_eq!(manager.lookup_partition_core(b"orders", 99), None);
        assert_eq!(manager.lookup_partition_core(b"non-existent", 0), None);

        // Reassign partition 0 to Core 3 (Rebalancing)
        let reassigned = manager.reassign_partition(b"orders", 0, 3);
        assert!(reassigned);
        assert_eq!(manager.lookup_partition_core(b"orders", 0), Some(3));
    }

    #[test]
    fn test_concurrent_lock_free_reads_during_active_rcu_updates() {
        let manager = Arc::new(MetadataManager::new(8));
        manager.register_topic(b"telemetry", 16);

        let running = Arc::new(AtomicBool::new(true));
        let num_reader_threads = 8;
        let mut reader_handles = Vec::new();

        // Spawn concurrent readers constantly querying topic metadata without locks
        for _ in 0..num_reader_threads {
            let m_clone = manager.clone();
            let r_clone = running.clone();
            reader_handles.push(thread::spawn(move || {
                let mut lookup_count = 0usize;
                while r_clone.load(Ordering::Relaxed) {
                    let core = m_clone.lookup_partition_core(b"telemetry", 5);
                    assert!(core.is_some(), "Concurrent lookup must never return corrupt/None");
                    lookup_count += 1;
                }
                lookup_count
            }));
        }

        // Simultaneously perform rapid RCU updates (registering and modifying topics)
        for i in 0..100 {
            let topic_name = format!("dynamic_topic_{}", i);
            manager.register_topic(topic_name.as_bytes(), 4);
            manager.reassign_partition(b"telemetry", 5, i % 8);
        }

        thread::sleep(Duration::from_millis(50));
        running.store(false, Ordering::Relaxed);

        let mut total_lookups = 0;
        for handle in reader_handles {
            total_lookups += handle.join().expect("Reader thread panicked");
        }

        println!("Total lock-free concurrent lookups completed during RCU: {}", total_lookups);
        assert!(total_lookups > 100_000, "Expected >100,000 lock-free lookups during RCU test");
    }
}

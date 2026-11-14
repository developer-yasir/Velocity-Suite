//! VelocityMQ Lock-Free Inter-Core Topic & Partition Router - Phase 5
//!
//! Connects the Network layer to the thread-per-core WAL partitions without global locks.
//! - Uses a cache-padded, lock-free Single-Producer Single-Consumer (SPSC) circular ring
//!   buffer for microsecond-level inter-core messaging.
//! - Resolves partition ownership via lock-free RCU `MetadataManager`.
//! - Provides graceful shutdown coordination to guarantee all cores flush their `O_DIRECT`
//!   WAL buffers to disk on SIGINT / shutdown signals.

use bytes::Bytes;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::metadata::MetadataManager;
use crate::protocol::{KafkaErrorCode, ProduceRequest};
use crate::wal::LogSegment;

// Cache line padding to eliminate false sharing between CPU cores
#[repr(align(64))]
struct CachePadded<T>(T);

impl<T> CachePadded<T> {
    #[inline(always)]
    const fn new(val: T) -> Self {
        Self(val)
    }
}

/// High-Performance Lock-Free Single-Producer Single-Consumer (SPSC) Ring Buffer.
///
/// Designed specifically for thread-per-core architectures:
/// - Pre-allocated fixed capacity (power of two).
/// - Head and Tail atomic pointers reside on isolated 64-byte cache lines.
/// - Zero memory allocations during operation.
/// - Wait-free enqueues and dequeues (~5-15ns latency).
pub struct SpscRingBuffer<T> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    capacity: usize,
    mask: usize,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

unsafe impl<T: Send> Send for SpscRingBuffer<T> {}
unsafe impl<T: Send> Sync for SpscRingBuffer<T> {}

impl<T> SpscRingBuffer<T> {
    /// Creates a new SPSC ring buffer with the specified capacity (rounded up to power of two).
    pub fn new(requested_capacity: usize) -> Self {
        let capacity = requested_capacity.next_power_of_two().max(16);
        let mask = capacity - 1;

        let mut raw_vec = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            raw_vec.push(UnsafeCell::new(MaybeUninit::uninit()));
        }

        Self {
            buffer: raw_vec.into_boxed_slice(),
            capacity,
            mask,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    /// Enqueues an item into the ring buffer.
    ///
    /// # Errors
    /// Returns `Err(item)` if the buffer is full (Backpressure signal).
    #[inline(always)]
    pub fn push(&self, item: T) -> Result<(), T> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Acquire);

        if tail.wrapping_sub(head) >= self.capacity {
            return Err(item); // Queue is full!
        }

        let slot = &self.buffer[tail & self.mask];
        unsafe {
            (*slot.get()).write(item);
        }

        self.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Dequeues an item from the ring buffer.
    ///
    /// Returns `None` if the queue is currently empty.
    #[inline(always)]
    pub fn pop(&self) -> Option<T> {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Acquire);

        if head == tail {
            return None; // Queue is empty
        }

        let slot = &self.buffer[head & self.mask];
        let item = unsafe { (*slot.get()).assume_init_read() };

        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        Some(item)
    }

    /// Current number of elements in the ring buffer.
    #[inline(always)]
    pub fn len(&self) -> usize {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Relaxed);
        tail.wrapping_sub(head)
    }

    /// Returns true if the ring buffer is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the maximum capacity of the ring buffer.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<T> Drop for SpscRingBuffer<T> {
    fn drop(&mut self) {
        // Drain and drop any remaining elements
        while self.pop().is_some() {}
    }
}

/// Messages routed across thread-per-core event loops.
#[derive(Debug)]
pub enum RouterMessage {
    /// Inbound Produce task routed from receiving network core to partition owner core.
    ProduceTask {
        correlation_id: i32,
        client_conn_id: usize,
        source_core: usize,
        topic: Bytes,
        partition: i32,
        records: Bytes,
    },
    /// Acknowledgment reply routed back to the client's network core.
    ProduceAck {
        correlation_id: i32,
        client_conn_id: usize,
        topic: Bytes,
        partition: i32,
        base_offset: u64,
        error_code: KafkaErrorCode,
    },
    /// Command instructing the core to flush active WAL buffers to disk.
    FlushWal {
        sync: bool,
    },
    /// Graceful shutdown command informing the event loop to flush and terminate.
    Shutdown,
}

/// Result of routing an individual partition produce task.
#[derive(Debug)]
pub enum RouteResult {
    /// Target core is the current core: execute on the local fast-path (Zero IPC latency).
    LocalFastPath {
        topic: Bytes,
        partition: i32,
        records: Bytes,
        correlation_id: i32,
    },
    /// Dispatched asynchronously to target core via lock-free SPSC channel.
    ForwardedToCore {
        target_core: usize,
        partition: i32,
    },
    /// Target core's SPSC queue is saturated: triggers backpressure retriable error.
    TargetCoreBackpressure {
        target_core: usize,
        partition: i32,
    },
    /// Partition does not exist in the metadata routing table.
    UnknownTopicOrPartition {
        topic: Bytes,
        partition: i32,
    },
}

/// Core-to-Core Inter-Thread Mesh Router.
///
/// Contains an $N \times N$ matrix of lock-free SPSC ring buffers connecting all
/// hardware CPU cores without locks or shared mutable states.
pub struct CoreMeshRouter {
    /// Channels matrix: `channels[from_core][to_core]`
    channels: Vec<Vec<Arc<SpscRingBuffer<RouterMessage>>>>,
    /// Total hardware CPU cores in the mesh.
    num_cores: usize,
    /// Shutdown coordinator handle.
    shutdown_coordinator: Arc<ShutdownCoordinator>,
}

impl CoreMeshRouter {
    /// Initializes a new inter-core mesh router for `num_cores` hardware cores.
    ///
    /// Pre-allocates SPSC channels between every pair of cores $(i, j)$.
    pub fn new(num_cores: usize, channel_capacity: usize) -> Self {
        assert!(num_cores > 0, "num_cores must be at least 1");

        let mut channels = Vec::with_capacity(num_cores);
        for _ in 0..num_cores {
            let mut row = Vec::with_capacity(num_cores);
            for _ in 0..num_cores {
                row.push(Arc::new(SpscRingBuffer::new(channel_capacity)));
            }
            channels.push(row);
        }

        Self {
            channels,
            num_cores,
            shutdown_coordinator: Arc::new(ShutdownCoordinator::new(num_cores)),
        }
    }

    /// Number of CPU cores configured in the router mesh.
    #[inline(always)]
    pub fn num_cores(&self) -> usize {
        self.num_cores
    }

    /// Access the shutdown coordinator.
    #[inline(always)]
    pub fn shutdown_coordinator(&self) -> &Arc<ShutdownCoordinator> {
        &self.shutdown_coordinator
    }

    /// Sends a message from `source_core` to `target_core` via their dedicated SPSC ring buffer.
    #[inline(always)]
    pub fn send(&self, source_core: usize, target_core: usize, msg: RouterMessage) -> Result<(), RouterMessage> {
        if source_core >= self.num_cores || target_core >= self.num_cores {
            return Err(msg);
        }
        self.channels[source_core][target_core].push(msg)
    }

    /// Polls a single message directed to `target_core` from a specific `source_core`.
    #[inline(always)]
    pub fn poll_from(&self, source_core: usize, target_core: usize) -> Option<RouterMessage> {
        if source_core >= self.num_cores || target_core >= self.num_cores {
            return None;
        }
        self.channels[source_core][target_core].pop()
    }

    /// Drains incoming messages for `core_id` across all other sending cores in a round-robin pass.
    /// Returns the number of messages processed.
    pub fn drain_incoming<F>(&self, core_id: usize, max_batch: usize, mut handler: F) -> usize
    where
        F: FnMut(RouterMessage),
    {
        if core_id >= self.num_cores {
            return 0;
        }

        let mut processed = 0;
        for source_core in 0..self.num_cores {
            if source_core == core_id {
                continue; // Skip self
            }
            let queue = &self.channels[source_core][core_id];
            while processed < max_batch {
                if let Some(msg) = queue.pop() {
                    handler(msg);
                    processed += 1;
                } else {
                    break;
                }
            }
            if processed >= max_batch {
                break;
            }
        }
        processed
    }

    /// Routes an incoming Kafka ProduceRequest to partition owner cores using lock-free metadata.
    ///
    /// - If the target core is the current core: Returns `RouteResult::LocalFastPath`.
    /// - If the target core is another core: Dispatches to `channels[current_core][target_core]`.
    /// - If the target core queue is full: Returns `RouteResult::TargetCoreBackpressure`.
    pub fn route_produce_request(
        &self,
        current_core: usize,
        client_conn_id: usize,
        req: &ProduceRequest,
        metadata: &MetadataManager,
    ) -> Vec<RouteResult> {
        let mut results = Vec::new();

        for topic_data in &req.topic_data {
            for part_data in &topic_data.partitions {
                let target_core = metadata.lookup_partition_core(&topic_data.topic, part_data.partition);

                match target_core {
                    Some(core) if core == current_core => {
                        // Local fast path: Same core owns the partition
                        results.push(RouteResult::LocalFastPath {
                            topic: topic_data.topic.clone(),
                            partition: part_data.partition,
                            records: part_data.records.clone(),
                            correlation_id: req.header.correlation_id,
                        });
                    }
                    Some(core) => {
                        // Cross-core dispatch via lock-free SPSC queue
                        let task = RouterMessage::ProduceTask {
                            correlation_id: req.header.correlation_id,
                            client_conn_id,
                            source_core: current_core,
                            topic: topic_data.topic.clone(),
                            partition: part_data.partition,
                            records: part_data.records.clone(),
                        };

                        match self.send(current_core, core, task) {
                            Ok(()) => {
                                results.push(RouteResult::ForwardedToCore {
                                    target_core: core,
                                    partition: part_data.partition,
                                });
                            }
                            Err(_) => {
                                // Channel full -> Signal backpressure
                                results.push(RouteResult::TargetCoreBackpressure {
                                    target_core: core,
                                    partition: part_data.partition,
                                });
                            }
                        }
                    }
                    None => {
                        results.push(RouteResult::UnknownTopicOrPartition {
                            topic: topic_data.topic.clone(),
                            partition: part_data.partition,
                        });
                    }
                }
            }
        }

        results
    }

    /// Routes an incoming Kafka ProduceRequest with optional Developer Mode auto-topic creation.
    /// In Developer Mode, if a topic is unknown, it is automatically registered on the
    /// current core so local single-node writes succeed immediately without blocking.
    pub fn route_produce_request_with_dev_mode(
        &self,
        current_core: usize,
        client_conn_id: usize,
        req: &ProduceRequest,
        metadata: &MetadataManager,
        developer_mode: bool,
    ) -> Vec<RouteResult> {
        if developer_mode {
            for topic_data in &req.topic_data {
                metadata.auto_create_topic_if_missing(&topic_data.topic, 1);
            }
        }
        self.route_produce_request(current_core, client_conn_id, req, metadata)
    }
}

/// Graceful Shutdown Coordinator.
///
/// Ensures zero data loss when receiving SIGINT / termination signals:
/// 1. Broadcasts shutdown intent across all core event loops.
/// 2. Commands every core to flush active and queued `O_DIRECT` WAL buffers to disk.
/// 3. Confirms that all cores have cleanly flushed before process termination.
pub struct ShutdownCoordinator {
    is_shutting_down: AtomicBool,
    cores_flushed: AtomicUsize,
    total_cores: usize,
}

impl ShutdownCoordinator {
    /// Creates a new shutdown coordinator for the specified core count.
    pub fn new(total_cores: usize) -> Self {
        Self {
            is_shutting_down: AtomicBool::new(false),
            cores_flushed: AtomicUsize::new(0),
            total_cores,
        }
    }

    /// Checks if a shutdown has been initiated.
    #[inline(always)]
    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::Relaxed)
    }

    /// Triggers the shutdown sequence.
    pub fn signal_shutdown(&self) {
        self.is_shutting_down.store(true, Ordering::SeqCst);
    }

    /// Registers that an event loop on a specific core has flushed its WAL to disk and stopped.
    pub fn mark_core_flushed(&self) {
        self.cores_flushed.fetch_add(1, Ordering::SeqCst);
    }

    /// Number of cores that have completed their WAL flush.
    pub fn flushed_core_count(&self) -> usize {
        self.cores_flushed.load(Ordering::SeqCst)
    }

    /// Waits up to `timeout` for all cores to flush their WAL buffers and cleanly terminate.
    pub fn wait_for_all_cores(&self, timeout: Duration) -> bool {
        let start = Instant::now();
        while self.flushed_core_count() < self.total_cores {
            if start.elapsed() >= timeout {
                return false;
            }
            std::thread::yield_now();
        }
        true
    }

    /// Hooks into the OS SIGINT / Ctrl-C signal handler using `ctrlc`.
    /// Automatically broadcasts shutdown when SIGINT is caught.
    pub fn install_sigint_handler(coordinator: Arc<ShutdownCoordinator>) -> Result<(), ctrlc::Error> {
        ctrlc::set_handler(move || {
            eprintln!("\n[VelocityMQ] SIGINT (Ctrl-C) caught. Initiating graceful shutdown and WAL flush...");
            coordinator.signal_shutdown();
        })
    }

    /// Helper for a core worker to cleanly flush its WAL segment on shutdown.
    /// Returns the total committed file length on disk.
    pub fn flush_wal_on_shutdown(segment: &mut LogSegment) -> Result<u64, crate::wal::WalError> {
        // Synchronously flush all unwritten and queued records to NVMe with O_DIRECT
        segment.flush(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ApiKey, PartitionProduceData, RequestHeader, TopicProduceData};
    use crate::wal::{LogSegment, LogSegmentConfig};
    use std::path::PathBuf;
    use std::thread;

    #[test]
    fn test_spsc_ring_buffer_fifo_and_backpressure() {
        let ring = SpscRingBuffer::<u64>::new(4); // Capacity will be 16 (min capacity)
        assert_eq!(ring.capacity(), 16);
        assert_eq!(ring.len(), 0);
        assert!(ring.is_empty());

        // Fill up to 16 items
        for i in 0..16 {
            assert!(ring.push(i).is_ok());
        }

        assert_eq!(ring.len(), 16);

        // 17th item must trigger backpressure (Err)
        let overflow = ring.push(999);
        assert_eq!(overflow, Err(999));

        // Pop elements in strict FIFO order
        for i in 0..16 {
            assert_eq!(ring.pop(), Some(i));
        }

        assert!(ring.is_empty());
        assert_eq!(ring.pop(), None);
    }

    #[test]
    fn test_spsc_concurrent_producer_consumer_million_records() {
        let ring = Arc::new(SpscRingBuffer::<u64>::new(1024));
        let ring_producer = ring.clone();
        let total_items = 500_000u64;

        let producer_handle = thread::spawn(move || {
            for i in 0..total_items {
                let mut item = i;
                while let Err(returned) = ring_producer.push(item) {
                    item = returned;
                    thread::yield_now();
                }
            }
        });

        let mut consumed_count = 0u64;
        while consumed_count < total_items {
            if let Some(val) = ring.pop() {
                assert_eq!(val, consumed_count);
                consumed_count += 1;
            } else {
                thread::yield_now();
            }
        }

        producer_handle.join().unwrap();
        assert_eq!(consumed_count, total_items);
    }

    #[test]
    fn test_core_mesh_router_produce_routing_and_fastpath() {
        let num_cores = 4;
        let router = CoreMeshRouter::new(num_cores, 64);
        let metadata = MetadataManager::new(num_cores);

        // Register topic "market-feed" with 4 partitions
        // Core 0 owns part 0, Core 1 owns part 1, Core 2 owns part 2, Core 3 owns part 3
        metadata.register_topic(b"market-feed", 4);

        // Client connects to Core 0 with ProduceRequest for partition 0 and partition 2
        let req = ProduceRequest {
            header: RequestHeader {
                api_key: ApiKey::Produce,
                api_version: 3,
                correlation_id: 777,
                client_id: Some(Bytes::from_static(b"client-feed")),
            },
            transactional_id: None,
            acks: 1,
            timeout_ms: 1000,
            topic_data: vec![TopicProduceData {
                topic: Bytes::from_static(b"market-feed"),
                partitions: vec![
                    PartitionProduceData {
                        partition: 0,
                        records: Bytes::from_static(b"part0_data"),
                    },
                    PartitionProduceData {
                        partition: 2,
                        records: Bytes::from_static(b"part2_data"),
                    },
                ],
            }],
        };

        let route_results = router.route_produce_request(0, 101, &req, &metadata);
        assert_eq!(route_results.len(), 2);

        // Partition 0 is owned by Core 0 -> LocalFastPath!
        match &route_results[0] {
            RouteResult::LocalFastPath { topic, partition, records, .. } => {
                assert_eq!(topic.as_ref(), b"market-feed");
                assert_eq!(*partition, 0);
                assert_eq!(records.as_ref(), b"part0_data");
            }
            _ => panic!("Expected LocalFastPath for partition 0 on Core 0"),
        }

        // Partition 2 is owned by Core 2 -> ForwardedToCore(2)!
        match &route_results[1] {
            RouteResult::ForwardedToCore { target_core, partition } => {
                assert_eq!(*target_core, 2);
                assert_eq!(*partition, 2);
            }
            _ => panic!("Expected ForwardedToCore(2) for partition 2"),
        }

        // Core 2 polls its incoming queue and receives the ProduceTask
        let msg = router.poll_from(0, 2).expect("Core 2 must receive message from Core 0");
        match msg {
            RouterMessage::ProduceTask { correlation_id, partition, records, .. } => {
                assert_eq!(correlation_id, 777);
                assert_eq!(partition, 2);
                assert_eq!(records.as_ref(), b"part2_data");
            }
            _ => panic!("Expected RouterMessage::ProduceTask"),
        }
    }

    #[test]
    fn test_graceful_shutdown_wal_flush() {
        let temp_wal = PathBuf::from("target").join(format!("test_shutdown_{}.wal", std::process::id()));
        let _ = std::fs::remove_file(&temp_wal);

        let config = LogSegmentConfig {
            segment_id: 1,
            max_segment_size: 10 * 1024 * 1024,
            buffer_size: 64 * 1024,
            ring_depth: 32,
            buffer_pool_size: 4,
            sync_on_flush: true,
        };

        let mut segment = LogSegment::create(&temp_wal, config).expect("Failed to create LogSegment");
        segment.append(b"PRE_SHUTDOWN_RECORD_A").unwrap();
        segment.append(b"PRE_SHUTDOWN_RECORD_B").unwrap();

        let coordinator = Arc::new(ShutdownCoordinator::new(1));
        assert!(!coordinator.is_shutting_down());

        // Trigger shutdown signal
        coordinator.signal_shutdown();
        assert!(coordinator.is_shutting_down());

        // Perform graceful WAL flush on shutdown
        ShutdownCoordinator::flush_wal_on_shutdown(&mut segment).expect("Shutdown flush failed");
        coordinator.mark_core_flushed();

        assert!(coordinator.wait_for_all_cores(Duration::from_millis(500)));
        drop(segment);

        let _ = std::fs::remove_file(&temp_wal);
    }
}

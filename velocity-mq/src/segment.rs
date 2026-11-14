//! VelocityMQ Segmented Log Engine - Phase 9
//!
//! Upgrades the single-file Write-Ahead Log into a Segmented Log architecture
//! with seamless, zero-copy log rolling and automatic data retention.
//!
//! Architectural Principles:
//! 1. Log Rolling: Fixed-size memory-aligned io_uring segments (e.g. 500MB - 1GB).
//!    When active segment capacity is reached, it is sealed (read-only) and a new
//!    active segment is opened with zero dropped ProduceRequests.
//! 2. RCU Segment Catalog: Uses `arc-swap::ArcSwap` for lock-free inspection by readers
//!    and the background retention reaper thread without blocking the write path.
//! 3. Binary Offset Indexing: Appends 16-byte index records (`offset -> physical_position`)
//!    matching Kafka's offset index format for O(log N) record seeking.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::wal::{
    current_time_ms, AppendResult, LogSegment, LogSegmentConfig, WalError,
    RECORD_HEADER_SIZE,
};

/// Generates standard Kafka log and index filenames given a prefix and 20-digit zero-padded base offset.
#[inline]
pub fn segment_filenames(prefix: &str, base_offset: u64) -> (String, String) {
    (
        format!("{}_{:020}.log", prefix, base_offset),
        format!("{}_{:020}.index", prefix, base_offset),
    )
}

// ============================================================================
// Binary Offset Index (.index)
// ============================================================================

/// Fixed size of a binary offset index entry: 8 bytes offset + 8 bytes position = 16 bytes.
pub const INDEX_ENTRY_SIZE: usize = 16;

/// High-performance binary index mapping logical record offsets to physical file positions.
pub struct OffsetIndex {
    path: PathBuf,
    file: File,
    entries: Vec<(u64, u64)>, // (offset, position)
    last_indexed_offset: u64,
}

impl OffsetIndex {
    /// Opens or creates an offset index file.
    pub fn open_or_create<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path_buf)?;

        let mut entries = Vec::new();
        let file_len = file.metadata()?.len();

        if file_len >= INDEX_ENTRY_SIZE as u64 {
            let mut buf = vec![0u8; file_len as usize];
            file.read_exact(&mut buf)?;

            for chunk in buf.chunks_exact(INDEX_ENTRY_SIZE) {
                let offset = u64::from_be_bytes([
                    chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
                ]);
                let position = u64::from_be_bytes([
                    chunk[8], chunk[9], chunk[10], chunk[11], chunk[12], chunk[13], chunk[14], chunk[15],
                ]);
                entries.push((offset, position));
            }
        }

        let last_indexed_offset = entries.last().map(|(o, _)| *o).unwrap_or(0);

        Ok(Self {
            path: path_buf,
            file,
            entries,
            last_indexed_offset,
        })
    }

    /// Appends an index entry mapping `offset` to physical `position` on disk.
    pub fn append(&mut self, offset: u64, position: u64) -> std::io::Result<()> {
        let mut bytes = [0u8; INDEX_ENTRY_SIZE];
        bytes[0..8].copy_from_slice(&offset.to_be_bytes());
        bytes[8..16].copy_from_slice(&position.to_be_bytes());

        self.file.write_all(&bytes)?;
        self.entries.push((offset, position));
        self.last_indexed_offset = offset;
        Ok(())
    }

    /// Binary searches the greatest indexed offset less than or equal to `target_offset`.
    pub fn lookup(&self, target_offset: u64) -> Option<u64> {
        if self.entries.is_empty() {
            return None;
        }

        match self.entries.binary_search_by_key(&target_offset, |&(o, _)| o) {
            Ok(idx) => Some(self.entries[idx].1),
            Err(idx) => {
                if idx == 0 {
                    Some(self.entries[0].1)
                } else {
                    Some(self.entries[idx - 1].1)
                }
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline]
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

// ============================================================================
// Segment Metadata & Lock-Free Catalog
// ============================================================================

/// Immutable metadata describing a single WAL segment on disk.
pub struct SegmentMeta {
    pub base_offset: u64,
    pub log_path: PathBuf,
    pub index_path: PathBuf,
    pub size_bytes: AtomicU64,
    pub record_count: AtomicU64,
    pub created_at_ms: u64,
    pub closed_at_ms: AtomicU64,
    pub is_sealed: AtomicBool,
}

impl SegmentMeta {
    pub fn new(base_offset: u64, log_path: PathBuf, index_path: PathBuf) -> Self {
        Self {
            base_offset,
            log_path,
            index_path,
            size_bytes: AtomicU64::new(0),
            record_count: AtomicU64::new(0),
            created_at_ms: current_time_ms(),
            closed_at_ms: AtomicU64::new(0),
            is_sealed: AtomicBool::new(false),
        }
    }

    /// Seals the segment, updating closed timestamp and marking read-only.
    pub fn seal(&self, final_size: u64) {
        self.size_bytes.store(final_size, Ordering::Release);
        self.closed_at_ms.store(current_time_ms(), Ordering::Release);
        self.is_sealed.store(true, Ordering::Release);
    }

    #[inline]
    pub fn is_sealed(&self) -> bool {
        self.is_sealed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes.load(Ordering::Acquire)
    }

    #[inline]
    pub fn record_count(&self) -> u64 {
        self.record_count.load(Ordering::Acquire)
    }

    #[inline]
    pub fn closed_at_ms(&self) -> u64 {
        self.closed_at_ms.load(Ordering::Acquire)
    }

    /// Checks if this sealed segment has exceeded the given retention duration.
    pub fn is_expired(&self, retention_ms: u64, now_ms: u64) -> bool {
        if !self.is_sealed() {
            return false; // Active segment is never expired
        }
        let closed = self.closed_at_ms();
        closed > 0 && now_ms.saturating_sub(closed) >= retention_ms
    }
}

/// Shared RCU catalog of segments for a single partition.
/// Enables the background Retention Reaper to read and clean segments lock-free.
pub struct SegmentCatalog {
    pub partition_id: String,
    pub sealed_segments: ArcSwap<Vec<Arc<SegmentMeta>>>,
    pub active_meta: ArcSwap<SegmentMeta>,
    pub total_bytes: AtomicU64,
}

impl SegmentCatalog {
    pub fn new(partition_id: String, initial_active: Arc<SegmentMeta>) -> Self {
        Self {
            partition_id,
            sealed_segments: ArcSwap::from_pointee(Vec::new()),
            active_meta: ArcSwap::from(initial_active),
            total_bytes: AtomicU64::new(0),
        }
    }

    /// Appends a newly sealed segment into the RCU catalog.
    pub fn add_sealed_segment(&self, meta: Arc<SegmentMeta>) {
        let size = meta.size_bytes();
        self.total_bytes.fetch_add(size, Ordering::SeqCst);

        // Lock-free RCU update
        self.sealed_segments.rcu(|current| {
            let mut new_vec = (**current).clone();
            new_vec.push(meta.clone());
            new_vec
        });
    }

    /// Removes reaped segments from the catalog and updates total bytes.
    pub fn remove_sealed_segments(&self, reaped_offsets: &[u64]) {
        if reaped_offsets.is_empty() {
            return;
        }

        let mut bytes_subtracted = 0u64;

        self.sealed_segments.rcu(|current| {
            let mut new_vec = Vec::with_capacity(current.len());
            for s in current.iter() {
                if reaped_offsets.contains(&s.base_offset) {
                    bytes_subtracted += s.size_bytes();
                } else {
                    new_vec.push(s.clone());
                }
            }
            new_vec
        });

        self.total_bytes.fetch_sub(bytes_subtracted, Ordering::SeqCst);
    }

    /// Returns a snapshot reference of all sealed segments.
    #[inline]
    pub fn get_sealed_segments(&self) -> Arc<Vec<Arc<SegmentMeta>>> {
        self.sealed_segments.load_full()
    }

    #[inline]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Acquire)
    }
}

// ============================================================================
// SegmentedLog Configuration
// ============================================================================

/// Configuration parameters for SegmentedLog.
#[derive(Debug, Clone)]
pub struct SegmentedLogConfig {
    pub log_dir: PathBuf,
    pub partition_id: String,
    pub max_segment_bytes: u64,
    pub buffer_size: usize,
    pub ring_depth: u32,
    pub buffer_pool_size: usize,
    pub sync_on_flush: bool,
    pub index_interval_bytes: usize,
}

impl Default for SegmentedLogConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("data").join("wal"),
            partition_id: "partition-0".into(),
            max_segment_bytes: 500 * 1024 * 1024, // 500 MB default roll size
            buffer_size: 64 * 1024,               // 64 KB write buffer
            ring_depth: 64,
            buffer_pool_size: 16,
            sync_on_flush: false,
            index_interval_bytes: 4096,           // Index entry every 4KB
        }
    }
}

// ============================================================================
// SegmentedLog (Thread-Per-Core Shared-Nothing)
// ============================================================================

/// Segmented Write-Ahead Log supporting zero-copy append, seamless rolling,
/// and automated zero-blocking retention reaping.
pub struct SegmentedLog {
    config: SegmentedLogConfig,
    active_segment: LogSegment,
    active_index: OffsetIndex,
    active_meta: Arc<SegmentMeta>,
    pub catalog: Arc<SegmentCatalog>,
    bytes_since_last_index: usize,
    next_segment_id: u64,
}

impl SegmentedLog {
    /// Opens or creates a SegmentedLog at `config.log_dir`.
    pub fn open_or_create(config: SegmentedLogConfig) -> Result<Self, WalError> {
        let _ = std::fs::create_dir_all(&config.log_dir);

        let base_offset = 0u64;
        let (log_name, idx_name) = segment_filenames(&config.partition_id, base_offset);
        let log_path = config.log_dir.join(&log_name);
        let idx_path = config.log_dir.join(&idx_name);

        let wal_cfg = LogSegmentConfig {
            segment_id: 1,
            max_segment_size: config.max_segment_bytes + (config.buffer_size as u64 * 8),
            buffer_size: config.buffer_size,
            ring_depth: config.ring_depth,
            buffer_pool_size: config.buffer_pool_size,
            sync_on_flush: config.sync_on_flush,
        };

        let active_segment = LogSegment::create_with_base_offset(&log_path, wal_cfg, base_offset)?;
        let active_index = OffsetIndex::open_or_create(&idx_path).map_err(WalError::Io)?;
        let active_meta = Arc::new(SegmentMeta::new(base_offset, log_path, idx_path));
        let catalog = Arc::new(SegmentCatalog::new(config.partition_id.clone(), active_meta.clone()));

        Ok(Self {
            config,
            active_segment,
            active_index,
            active_meta,
            catalog,
            bytes_since_last_index: 0,
            next_segment_id: 2,
        })
    }

    /// Appends a raw record payload to the segmented log.
    ///
    /// If the current active segment is full or reaches `max_segment_bytes`, it
    /// automatically rolls over to a new memory-aligned io_uring segment without
    /// dropping the incoming request or pausing the event loop.
    pub fn append(&mut self, payload: &[u8]) -> Result<AppendResult, WalError> {
        let total_record_size = RECORD_HEADER_SIZE + payload.len();

        // 1. Check if record exceeds active segment limit -> Trigger Roll
        if self.active_segment.file_offset() + (total_record_size as u64) > self.config.max_segment_bytes {
            self.roll_segment()?;
        }

        // 2. Attempt append to active segment
        let res = match self.active_segment.append(payload) {
            Ok(r) => r,
            Err(WalError::SegmentFull { .. }) => {
                // If segment full on page boundary, roll and append
                self.roll_segment()?;
                self.active_segment.append(payload)?
            }
            Err(e) => return Err(e),
        };

        // 3. Update index if interval threshold reached
        self.bytes_since_last_index += total_record_size;
        if self.bytes_since_last_index >= self.config.index_interval_bytes {
            let _ = self.active_index.append(res.record_offset, res.file_offset);
            self.bytes_since_last_index = 0;
        }

        // 4. Update metadata
        self.active_meta.record_count.fetch_add(1, Ordering::Relaxed);
        self.active_meta.size_bytes.store(self.active_segment.file_offset(), Ordering::Release);

        Ok(res)
    }

    /// Atomically seals the active segment and rolls over to a new memory-aligned segment.
    pub fn roll_segment(&mut self) -> Result<Arc<SegmentMeta>, WalError> {
        // 1. Flush & Sync active segment to guarantee persistence
        self.active_segment.flush(true)?;
        let _ = self.active_index.flush();

        let final_size = self.active_segment.file_offset();
        let sealed_meta = self.active_meta.clone();

        // 2. Mark old segment as sealed (read-only)
        sealed_meta.seal(final_size);

        // 3. Add to RCU catalog
        self.catalog.add_sealed_segment(sealed_meta.clone());

        // 4. Next base offset starts from next record offset
        let next_base_offset = self.active_segment.next_record_offset();
        let (log_name, idx_name) = segment_filenames(&self.config.partition_id, next_base_offset);
        let log_path = self.config.log_dir.join(&log_name);
        let idx_path = self.config.log_dir.join(&idx_name);

        let wal_cfg = LogSegmentConfig {
            segment_id: self.next_segment_id,
            max_segment_size: self.config.max_segment_bytes + (self.config.buffer_size as u64 * 8),
            buffer_size: self.config.buffer_size,
            ring_depth: self.config.ring_depth,
            buffer_pool_size: self.config.buffer_pool_size,
            sync_on_flush: self.config.sync_on_flush,
        };
        self.next_segment_id += 1;

        // 5. Open new page-aligned io_uring active segment
        let new_active_segment = LogSegment::create_with_base_offset(&log_path, wal_cfg, next_base_offset)?;
        let new_active_index = OffsetIndex::open_or_create(&idx_path).map_err(WalError::Io)?;
        let new_active_meta = Arc::new(SegmentMeta::new(next_base_offset, log_path, idx_path));

        self.active_segment = new_active_segment;
        self.active_index = new_active_index;
        self.active_meta = new_active_meta.clone();
        self.catalog.active_meta.store(new_active_meta);
        self.bytes_since_last_index = 0;

        crate::metrics::inc_segments_rolled();

        Ok(sealed_meta)
    }

    /// Flushes the active segment.
    #[inline]
    pub fn flush(&mut self, sync: bool) -> Result<u64, WalError> {
        let _ = self.active_index.flush();
        self.active_segment.flush(sync)
    }

    #[inline]
    pub fn active_base_offset(&self) -> u64 {
        self.active_meta.base_offset
    }

    #[inline]
    pub fn next_record_offset(&self) -> u64 {
        self.active_segment.next_record_offset()
    }

    #[inline]
    pub fn total_bytes(&self) -> u64 {
        self.catalog.total_bytes() + self.active_segment.file_offset()
    }

    #[inline]
    pub fn segment_count(&self) -> usize {
        self.catalog.get_sealed_segments().len() + 1
    }

    /// Closes all open segments.
    pub fn close(mut self) -> Result<(), WalError> {
        self.flush(true)?;
        self.active_segment.close()
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_index_append_and_lookup() {
        let temp_dir = std::env::temp_dir().join(format!("velmq_test_idx_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);
        let idx_path = temp_dir.join("test_000.index");

        let mut index = OffsetIndex::open_or_create(&idx_path).unwrap();
        index.append(0, 0).unwrap();
        index.append(100, 4096).unwrap();
        index.append(250, 12288).unwrap();
        index.append(500, 24576).unwrap();
        index.flush().unwrap();

        assert_eq!(index.len(), 4);
        assert_eq!(index.lookup(0), Some(0));
        assert_eq!(index.lookup(50), Some(0));
        assert_eq!(index.lookup(100), Some(4096));
        assert_eq!(index.lookup(180), Some(4096));
        assert_eq!(index.lookup(250), Some(12288));
        assert_eq!(index.lookup(499), Some(12288));
        assert_eq!(index.lookup(500), Some(24576));
        assert_eq!(index.lookup(1000), Some(24576));

        // Reopen from disk and assert durability
        drop(index);
        let reopened = OffsetIndex::open_or_create(&idx_path).unwrap();
        assert_eq!(reopened.len(), 4);
        assert_eq!(reopened.lookup(250), Some(12288));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_segmented_log_seamless_rolling() {
        let temp_dir = std::env::temp_dir().join(format!("velmq_test_seglog_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);

        // Small segment size (16 KB) to force multiple log rolls
        let config = SegmentedLogConfig {
            log_dir: temp_dir.clone(),
            partition_id: "telemetry-0".into(),
            max_segment_bytes: 16 * 1024, // 16 KB segment roll
            buffer_size: 4096,
            ring_depth: 32,
            buffer_pool_size: 4,
            sync_on_flush: false,
            index_interval_bytes: 512,
        };

        let mut log = SegmentedLog::open_or_create(config).unwrap();
        assert_eq!(log.segment_count(), 1);

        // Append 200 records (100 bytes each)
        let payload = vec![0xEEu8; 100];
        for i in 0..200 {
            let res = log.append(&payload).unwrap();
            assert_eq!(res.record_offset, i as u64);
        }
        log.flush(true).unwrap();

        // Must have rolled into multiple segments
        let count = log.segment_count();
        println!("[SegmentedLog Test] Appended 200 records, created {} segments", count);
        assert!(count >= 2, "Log must have rolled across multiple segments");

        let sealed = log.catalog.get_sealed_segments();
        assert_eq!(sealed.len(), count - 1);
        for s in sealed.iter() {
            assert!(s.is_sealed());
            assert!(s.size_bytes() > 0);
            assert!(s.log_path.exists());
            assert!(s.index_path.exists());
        }

        let _ = log.close();
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}

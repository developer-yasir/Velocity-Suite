//! VelocityMQ Zero-Blocking Retention Reaper - Phase 9
//!
//! Dedicated background OS thread enforcing automated log retention policies
//! without introducing filesystem metadata lock contention on the high-frequency write path.
//!
//! Architectural Principles:
//! 1. Isolated Background Thread: Runs completely decoupled from the reactor cores.
//! 2. Multi-Policy Retention:
//!    - Time-Based (`log.retention.hours`, default 168 hours / 7 days)
//!    - Size-Based (`log.retention.bytes`, byte ceiling per partition / cluster)
//! 3. Zero-Blocking Deletion: Deletes `.log` and `.index` files asynchronously off
//!    the write path so that filesystem metadata locks do not stall `O_DIRECT` appends.
//! 4. RCU Catalog Sync: Atomically purges deleted segments from the shared `SegmentCatalog`.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use dashmap::DashMap;

use crate::s3_archiver::S3Archiver;
use crate::segment::{SegmentCatalog, SegmentMeta};
use crate::wal::current_time_ms;

// ============================================================================
// Retention Policy & Reason
// ============================================================================

/// Trigger reason for segment deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionReason {
    /// Segment age exceeded `log.retention.hours`.
    AgeExpired {
        closed_at_ms: u64,
        age_ms: u64,
        limit_ms: u64,
    },
    /// Log size exceeded `log.retention.bytes`.
    SizeExceeded {
        current_bytes: u64,
        limit_bytes: u64,
    },
}

/// Description of a successfully reaped segment.
#[derive(Debug, Clone)]
pub struct ReapedSegment {
    pub partition_id: String,
    pub base_offset: u64,
    pub log_path: PathBuf,
    pub index_path: PathBuf,
    pub bytes_freed: u64,
    pub reason: RetentionReason,
}

/// Data retention configuration parameters.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Maximum age of a sealed segment in hours before it is eligible for deletion (default: 168 hours = 7 days).
    pub retention_hours: u64,
    /// Maximum total bytes per partition before oldest sealed segments are deleted (default: unlimited).
    pub retention_bytes: u64,
    /// Polling interval for background reaper scans.
    pub check_interval: Duration,
    /// Custom retention in milliseconds (overrides retention_hours if set).
    pub custom_retention_ms: Option<u64>,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            retention_hours: 168,           // 7 days
            retention_bytes: u64::MAX,      // Unlimited bytes
            check_interval: Duration::from_millis(500),
            custom_retention_ms: None,
        }
    }
}

impl RetentionPolicy {
    pub fn with_retention_hours(mut self, hours: u64) -> Self {
        self.retention_hours = hours;
        self.custom_retention_ms = None;
        self
    }

    pub fn with_retention_seconds(mut self, secs: u64) -> Self {
        self.custom_retention_ms = Some(secs.saturating_mul(1000));
        self
    }

    pub fn with_retention_ms(mut self, ms: u64) -> Self {
        self.custom_retention_ms = Some(ms);
        self
    }

    pub fn with_retention_bytes(mut self, bytes: u64) -> Self {
        self.retention_bytes = bytes;
        self
    }

    pub fn with_check_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }

    #[inline]
    pub fn retention_ms(&self) -> u64 {
        if let Some(ms) = self.custom_retention_ms {
            ms
        } else {
            self.retention_hours.saturating_mul(3600).saturating_mul(1000)
        }
    }
}

// ============================================================================
// Background Retention Reaper Thread
// ============================================================================

/// Zero-blocking background OS thread monitoring and reaping expired WAL segments,
/// with integrated AWS S3 Tiered Storage archive hooks.
pub struct RetentionReaper {
    pub policy: RetentionPolicy,
    catalogs: DashMap<String, Arc<SegmentCatalog>>,
    running: Arc<AtomicBool>,
    total_reaped_segments: Arc<AtomicU64>,
    total_reaped_bytes: Arc<AtomicU64>,
    archived_segments: Arc<AtomicU64>,
    archived_bytes: Arc<AtomicU64>,
    failed_archives: Arc<AtomicU64>,
    archiver: Arc<Mutex<Option<Arc<S3Archiver>>>>,
    in_flight_uploads: Arc<Mutex<HashSet<(String, u64)>>>,
    tokio_handle: tokio::runtime::Handle,
    _tokio_runtime: Option<Arc<tokio::runtime::Runtime>>,
    worker_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl RetentionReaper {
    /// Spawns a dedicated background OS thread running the Retention Reaper.
    pub fn new(policy: RetentionPolicy) -> Arc<Self> {
        Self::with_archiver_opt(policy, None)
    }

    /// Spawns a dedicated background OS thread running the Retention Reaper with S3 Tiered Storage enabled.
    pub fn with_archiver(policy: RetentionPolicy, archiver: Arc<S3Archiver>) -> Arc<Self> {
        Self::with_archiver_opt(policy, Some(archiver))
    }

    /// Spawns a dedicated background OS thread with an optional S3 Archiver.
    pub fn with_archiver_opt(
        policy: RetentionPolicy,
        archiver: Option<Arc<S3Archiver>>,
    ) -> Arc<Self> {
        let (tokio_handle, tokio_rt) = match tokio::runtime::Handle::try_current() {
            Ok(h) => (h, None),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_name("velmq-s3-worker")
                    .enable_all()
                    .build()
                    .expect("Failed to create Tokio runtime for S3 Tiered Archiver");
                let handle = rt.handle().clone();
                (handle, Some(Arc::new(rt)))
            }
        };

        let running = Arc::new(AtomicBool::new(true));

        let reaper = Arc::new(Self {
            policy: policy.clone(),
            catalogs: DashMap::new(),
            running: running.clone(),
            total_reaped_segments: Arc::new(AtomicU64::new(0)),
            total_reaped_bytes: Arc::new(AtomicU64::new(0)),
            archived_segments: Arc::new(AtomicU64::new(0)),
            archived_bytes: Arc::new(AtomicU64::new(0)),
            failed_archives: Arc::new(AtomicU64::new(0)),
            archiver: Arc::new(Mutex::new(archiver)),
            in_flight_uploads: Arc::new(Mutex::new(HashSet::new())),
            tokio_handle,
            _tokio_runtime: tokio_rt,
            worker_handle: Mutex::new(None),
        });

        let r_weak = Arc::downgrade(&reaper);
        let check_interval = policy.check_interval;

        let handle = thread::Builder::new()
            .name("velmq-wal-reaper".into())
            .spawn(move || {
                while running.load(Ordering::Relaxed) {
                    thread::sleep(check_interval);
                    if let Some(r) = r_weak.upgrade() {
                        let _ = r.sweep_once();
                    } else {
                        break;
                    }
                }
            })
            .expect("Failed to spawn RetentionReaper thread");

        *reaper.worker_handle.lock().unwrap() = Some(handle);
        reaper
    }

    /// Dynamically sets or updates the S3 Archiver client.
    pub fn set_archiver(&self, archiver: Arc<S3Archiver>) {
        *self.archiver.lock().unwrap() = Some(archiver);
    }

    /// Clears the S3 Archiver, reverting to local-only file deletion.
    pub fn clear_archiver(&self) {
        *self.archiver.lock().unwrap() = None;
    }

    /// Returns `true` if S3 Tiered Storage is currently active.
    pub fn is_tiered_storage_enabled(&self) -> bool {
        self.archiver.lock().unwrap().is_some()
    }

    /// Registers a partition's SegmentCatalog for automated retention enforcement.
    pub fn register_catalog(&self, catalog: Arc<SegmentCatalog>) {
        self.catalogs.insert(catalog.partition_id.clone(), catalog);
    }

    /// Unregisters a partition's SegmentCatalog.
    pub fn unregister_catalog(&self, partition_id: &str) {
        self.catalogs.remove(partition_id);
    }

    /// Executes a single retention scan across all registered partition catalogs.
    /// Returns the list of reaped segments.
    pub fn sweep_once(&self) -> Vec<ReapedSegment> {
        let now_ms = current_time_ms();
        let retention_ms = self.policy.retention_ms();
        let retention_bytes = self.policy.retention_bytes;

        let mut all_reaped = Vec::new();
        let archiver_opt = self.archiver.lock().unwrap().clone();

        for entry in self.catalogs.iter() {
            let catalog = entry.value();
            let sealed = catalog.get_sealed_segments();
            if sealed.is_empty() {
                continue;
            }

            let mut candidates: Vec<(Arc<SegmentMeta>, RetentionReason)> = Vec::new();

            // 1. Time-Based Retention Evaluation
            for s in sealed.iter() {
                if s.is_expired(retention_ms, now_ms) {
                    let closed = s.closed_at_ms();
                    candidates.push((
                        s.clone(),
                        RetentionReason::AgeExpired {
                            closed_at_ms: closed,
                            age_ms: now_ms.saturating_sub(closed),
                            limit_ms: retention_ms,
                        },
                    ));
                }
            }

            // 2. Size-Based Retention Evaluation
            let mut projected_bytes = catalog.total_bytes();
            for (c, _) in &candidates {
                projected_bytes = projected_bytes.saturating_sub(c.size_bytes());
            }

            if projected_bytes > retention_bytes {
                // Identify oldest sealed segments not yet marked for deletion
                for s in sealed.iter() {
                    if projected_bytes <= retention_bytes {
                        break;
                    }
                    if !candidates.iter().any(|(c, _)| c.base_offset == s.base_offset) {
                        candidates.push((
                            s.clone(),
                            RetentionReason::SizeExceeded {
                                current_bytes: projected_bytes,
                                limit_bytes: retention_bytes,
                            },
                        ));
                        projected_bytes = projected_bytes.saturating_sub(s.size_bytes());
                    }
                }
            }

            if candidates.is_empty() {
                continue;
            }

            // 3. Deletion Hook: AWS S3 Tiered Storage vs Local Unlink
            if let Some(ref archiver) = archiver_opt {
                let mut in_flight_lock = self.in_flight_uploads.lock().unwrap();

                for (s, reason) in candidates {
                    let key = (catalog.partition_id.clone(), s.base_offset);
                    if in_flight_lock.contains(&key) {
                        // Already in-flight in a background Tokio task; skip duplicate dispatch
                        continue;
                    }

                    in_flight_lock.insert(key);

                    let archiver_clone = archiver.clone();
                    let catalog_clone = catalog.clone();
                    let in_flight_clone = self.in_flight_uploads.clone();
                    let partition_id = catalog.partition_id.clone();
                    let base_offset = s.base_offset;
                    let log_path = s.log_path.clone();
                    let index_path = s.index_path.clone();
                    let segment_bytes = s.size_bytes();
                    let archived_segs = self.archived_segments.clone();
                    let archived_bts = self.archived_bytes.clone();
                    let failed_archs = self.failed_archives.clone();
                    let total_reaped_segs = self.total_reaped_segments.clone();
                    let total_reaped_bts = self.total_reaped_bytes.clone();

                    // Requirement 4: Spawn as separate detached Tokio task
                    self.tokio_handle.spawn(async move {
                        // Requirement 1: Streaming chunked upload via ByteStream
                        let upload_res = archiver_clone
                            .archive_segment(
                                &partition_id,
                                base_offset,
                                &log_path,
                                &index_path,
                            )
                            .await;

                        match upload_res {
                            Ok((log_etag, _index_etag)) => {
                                // Requirement 3: ONLY if S3 upload returns 200 OK (ETag verified),
                                // call std::fs::remove_file to delete local NVMe files.
                                let _ = std::fs::remove_file(&log_path);
                                let _ = std::fs::remove_file(&index_path);

                                // Purge from catalog after local file removal
                                catalog_clone.remove_sealed_segments(&[base_offset]);

                                archived_segs.fetch_add(1, Ordering::Relaxed);
                                archived_bts.fetch_add(segment_bytes, Ordering::Relaxed);
                                total_reaped_segs.fetch_add(1, Ordering::Relaxed);
                                total_reaped_bts.fetch_add(segment_bytes, Ordering::Relaxed);

                                crate::metrics::inc_s3_archived(1, segment_bytes);

                                println!(
                                    "[Reaper S3 Archiver] Partition {} Segment {} archived to S3 (ETag: {}). Local NVMe unlinked.",
                                    partition_id, base_offset, log_etag
                                );
                            }
                            Err(err) => {
                                // Requirement 3: If S3 upload fails, keep local files and retry on next cycle!
                                failed_archs.fetch_add(1, Ordering::Relaxed);
                                crate::metrics::inc_s3_failures(1);
                                eprintln!(
                                    "[Reaper S3 Archiver] Upload failed for partition {} base_offset {}: {}. Retaining local file for next cycle retry.",
                                    partition_id, base_offset, err
                                );
                            }
                        }

                        // Release in-flight lock to allow retry if failed, or clean up if succeeded
                        in_flight_clone.lock().unwrap().remove(&(partition_id, base_offset));
                    });

                    all_reaped.push(ReapedSegment {
                        partition_id: catalog.partition_id.clone(),
                        base_offset: s.base_offset,
                        log_path: s.log_path.clone(),
                        index_path: s.index_path.clone(),
                        bytes_freed: s.size_bytes(),
                        reason,
                    });
                }
            } else {
                // Local-Only Retention (Direct unlinking when S3 is disabled)
                let candidate_offsets: Vec<u64> = candidates.iter().map(|(s, _)| s.base_offset).collect();
                catalog.remove_sealed_segments(&candidate_offsets);

                for (s, reason) in candidates {
                    let bytes = s.size_bytes();

                    let _ = std::fs::remove_file(&s.log_path);
                    let _ = std::fs::remove_file(&s.index_path);

                    self.total_reaped_segments.fetch_add(1, Ordering::Relaxed);
                    self.total_reaped_bytes.fetch_add(bytes, Ordering::Relaxed);

                    crate::metrics::inc_segments_reaped(1, bytes);

                    all_reaped.push(ReapedSegment {
                        partition_id: catalog.partition_id.clone(),
                        base_offset: s.base_offset,
                        log_path: s.log_path.clone(),
                        index_path: s.index_path.clone(),
                        bytes_freed: bytes,
                        reason,
                    });
                }
            }
        }

        all_reaped
    }

    #[inline]
    pub fn total_reaped_segments(&self) -> u64 {
        self.total_reaped_segments.load(Ordering::Acquire)
    }

    #[inline]
    pub fn total_reaped_bytes(&self) -> u64 {
        self.total_reaped_bytes.load(Ordering::Acquire)
    }

    #[inline]
    pub fn archived_segments(&self) -> u64 {
        self.archived_segments.load(Ordering::Acquire)
    }

    #[inline]
    pub fn archived_bytes(&self) -> u64 {
        self.archived_bytes.load(Ordering::Acquire)
    }

    #[inline]
    pub fn failed_archives(&self) -> u64 {
        self.failed_archives.load(Ordering::Acquire)
    }

    #[inline]
    pub fn in_flight_count(&self) -> usize {
        self.in_flight_uploads.lock().unwrap().len()
    }

    /// Stops the background reaper thread.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Release);
        if let Some(h) = self.worker_handle.lock().unwrap().take() {
            let _ = h.join();
        }
    }
}

impl Drop for RetentionReaper {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::{SegmentedLog, SegmentedLogConfig};

    #[test]
    fn test_time_based_segment_retention_reaping() {
        let temp_dir = std::env::temp_dir().join(format!("velmq_test_time_reaper_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);

        let config = SegmentedLogConfig {
            log_dir: temp_dir.clone(),
            partition_id: "test-time-0".into(),
            max_segment_bytes: 8 * 1024, // 8 KB roll
            buffer_size: 4096,
            ring_depth: 32,
            buffer_pool_size: 4,
            sync_on_flush: false,
            index_interval_bytes: 512,
        };

        let mut log = SegmentedLog::open_or_create(config).unwrap();

        // Append to create multiple segments
        let payload = vec![0xABu8; 128];
        for _ in 0..150 {
            log.append(&payload).unwrap();
        }
        log.flush(true).unwrap();

        let initial_count = log.segment_count();
        assert!(initial_count >= 2, "Must have at least 2 segments");

        // Manually adjust closed_at_ms to simulate elapsed time (100ms ago)
        let sealed = log.catalog.get_sealed_segments();
        for s in sealed.iter() {
            s.closed_at_ms.store(current_time_ms() - 200, Ordering::Release);
        }

        // Configure reaper with 50ms retention limit
        let policy = RetentionPolicy {
            retention_hours: 0,
            retention_bytes: u64::MAX,
            check_interval: Duration::from_millis(50),
            custom_retention_ms: None,
        }
        .with_check_interval(Duration::from_millis(50));

        let reaper = RetentionReaper::new(policy);
        reaper.register_catalog(log.catalog.clone());

        // Trigger sweep
        let reaped = reaper.sweep_once();
        assert!(!reaped.is_empty(), "Expired sealed segments must be reaped");

        for r in &reaped {
            println!(
                "[Reaper Test] Reaped segment {} (freed {} bytes): {:?}",
                r.base_offset, r.bytes_freed, r.reason
            );
            assert!(!r.log_path.exists(), "Reaped log file must be deleted from disk");
            assert!(!r.index_path.exists(), "Reaped index file must be deleted from disk");
        }

        // Active segment must NEVER be deleted
        assert!(log.segment_count() >= 1);

        reaper.shutdown();
        let _ = log.close();
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_size_based_segment_retention_reaping() {
        let temp_dir = std::env::temp_dir().join(format!("velmq_test_size_reaper_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);

        let config = SegmentedLogConfig {
            log_dir: temp_dir.clone(),
            partition_id: "test-size-0".into(),
            max_segment_bytes: 8 * 1024, // 8 KB roll
            buffer_size: 4096,
            ring_depth: 32,
            buffer_pool_size: 4,
            sync_on_flush: false,
            index_interval_bytes: 512,
        };

        let mut log = SegmentedLog::open_or_create(config).unwrap();

        // Write enough data to create 4+ segments
        let payload = vec![0xCDu8; 128];
        for _ in 0..300 {
            log.append(&payload).unwrap();
        }
        log.flush(true).unwrap();

        let initial_count = log.segment_count();
        assert!(initial_count >= 3);

        let total_bytes = log.total_bytes();
        println!("[Size Reaper Test] Initial segments: {}, Total bytes: {}", initial_count, total_bytes);

        // Cap retention bytes to 10 KB (forces deletion of older sealed segments)
        let policy = RetentionPolicy {
            retention_hours: 1000, // Very high time limit (not time-triggered)
            retention_bytes: 10 * 1024, // 10 KB ceiling
            check_interval: Duration::from_millis(50),
            custom_retention_ms: None,
        };

        let reaper = RetentionReaper::new(policy);
        reaper.register_catalog(log.catalog.clone());

        let reaped = reaper.sweep_once();
        assert!(!reaped.is_empty(), "Size-exceeded segments must be reaped");

        for r in &reaped {
            println!(
                "[Size Reaper Test] Reaped segment {} (freed {} bytes): {:?}",
                r.base_offset, r.bytes_freed, r.reason
            );
            assert!(!r.log_path.exists());
            assert!(!r.index_path.exists());
        }

        reaper.shutdown();
        let _ = log.close();
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}

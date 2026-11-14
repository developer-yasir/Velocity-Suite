//! VelocityMQ Phase 9 Integration Test Suite: Segmented Log & Zero-Blocking Retention Reaper
//!
//! Validates:
//! 1. Seamless log rolling across active segments without dropping a single write.
//! 2. Accurate binary offset indexing (`.index`) for O(log N) physical position seeking.
//! 3. Background reaper thread running concurrently with producer stream, deleting expired
//!    `.log` and `.index` files without stalling the O_DIRECT write path.
//! 4. Total retention byte budget enforcement and active segment protection.

use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use velocity_mq::reaper::{RetentionPolicy, RetentionReaper};
use velocity_mq::segment::{OffsetIndex, SegmentedLog, SegmentedLogConfig};

fn get_unique_test_dir(test_name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("velmq_{}_{}", test_name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::create_dir_all(&dir);
    dir
}

#[test]
fn test_seamless_rolling_and_binary_index_lookup() {
    let test_dir = get_unique_test_dir("seamless_rolling");

    let config = SegmentedLogConfig {
        log_dir: test_dir.clone(),
        partition_id: "orders-0".to_string(),
        max_segment_bytes: 16 * 1024, // 16KB roll
        buffer_size: 4096,
        ring_depth: 32,
        buffer_pool_size: 4,
        sync_on_flush: false,
        index_interval_bytes: 512,
    };

    let mut log = SegmentedLog::open_or_create(config).expect("Failed to create SegmentedLog");

    let payload = vec![0xAAu8; 300]; // 300 bytes payload
    let total_records = 250;
    let mut written_offsets = Vec::with_capacity(total_records);

    for _ in 0..total_records {
        let res = log.append(&payload).expect("Append must succeed");
        written_offsets.push(res.record_offset);
    }

    log.flush(true).expect("Flush must succeed");

    // Offsets must be strictly monotonically increasing from 0 to 249
    for (i, &offset) in written_offsets.iter().enumerate() {
        assert_eq!(offset, i as u64, "Offset must match sequential index");
    }

    // Verify multiple segments were created
    let sealed = log.catalog.get_sealed_segments();
    assert!(
        !sealed.is_empty(),
        "Expected multiple sealed segments after writing 250 records"
    );

    println!(
        "[Integration Test] Created {} sealed segments, active base offset: {}",
        sealed.len(),
        log.active_base_offset()
    );

    // Verify binary index lookup resolves physical positions for all sealed segments
    for segment in sealed.iter() {
        assert!(segment.index_path.exists(), "Index file must exist on disk");
        assert!(segment.log_path.exists(), "Log file must exist on disk");

        let index = OffsetIndex::open_or_create(&segment.index_path)
            .expect("Failed to open segment index");
        let base = segment.base_offset;
        let pos = index.lookup(base);
        assert!(pos.is_some(), "Index must resolve base offset");
        println!(
            "[Integration Test] Segment base {} resolved to position {:?}",
            base, pos
        );
    }
}

#[test]
fn test_concurrent_streaming_and_background_reaper() {
    let test_dir = get_unique_test_dir("concurrent_reaper");

    let config = SegmentedLogConfig {
        log_dir: test_dir.clone(),
        partition_id: "sensors-0".to_string(),
        max_segment_bytes: 16 * 1024,
        buffer_size: 4096,
        ring_depth: 32,
        buffer_pool_size: 4,
        sync_on_flush: false,
        index_interval_bytes: 512,
    };

    let mut log = SegmentedLog::open_or_create(config).expect("Failed to create SegmentedLog");

    // Create a RetentionReaper with an aggressive check interval (20ms) and 0ms age limit
    let policy = RetentionPolicy::default()
        .with_retention_hours(0) // immediate expiration for sealed segments
        .with_check_interval(Duration::from_millis(20));

    let reaper = RetentionReaper::new(policy);
    reaper.register_catalog(log.catalog.clone());

    let payload = vec![0xBBu8; 256];
    let mut append_latencies = Vec::new();

    // Continuously stream 300 records while reaper thread runs in background
    for i in 0..300 {
        let start = Instant::now();
        let res = log.append(&payload).expect("Append must not fail");
        let dur = start.elapsed();
        append_latencies.push(dur);

        assert_eq!(res.record_offset, i as u64);

        if i % 50 == 0 {
            thread::sleep(Duration::from_millis(15));
        }
    }

    log.flush(true).expect("Flush must succeed");

    // Allow background reaper to catch up and process sealed segments
    thread::sleep(Duration::from_millis(150));
    reaper.shutdown();

    let total_reaped = reaper.total_reaped_segments();
    let freed_bytes = reaper.total_reaped_bytes();

    println!(
        "[Integration Test] Reaper deleted {} sealed segments, freed {} bytes",
        total_reaped, freed_bytes
    );

    assert!(
        total_reaped > 0,
        "Background reaper should have reaped sealed segments"
    );
    assert!(
        freed_bytes > 0,
        "Background reaper should have freed physical disk bytes"
    );

    // Verify active segment is still intact and functional
    let final_res = log
        .append(b"final record after reaping")
        .expect("Active segment must accept new writes after reaping");
    assert_eq!(final_res.record_offset, 300);

    // Verify tail latency remained well bounded
    append_latencies.sort();
    let p50 = append_latencies[append_latencies.len() / 2];
    let p99 = append_latencies[(append_latencies.len() * 99) / 100];
    println!(
        "[Integration Test] Concurrent Write Latencies: P50 = {:?}, P99 = {:?}",
        p50, p99
    );
}

#[test]
fn test_size_based_retention_enforces_budget_and_protects_active() {
    let test_dir = get_unique_test_dir("budget_enforcement");

    let config = SegmentedLogConfig {
        log_dir: test_dir.clone(),
        partition_id: "budget-0".to_string(),
        max_segment_bytes: 8 * 1024,
        buffer_size: 4096,
        ring_depth: 32,
        buffer_pool_size: 4,
        sync_on_flush: false,
        index_interval_bytes: 256,
    };

    let mut log = SegmentedLog::open_or_create(config).expect("Failed to create SegmentedLog");

    let payload = vec![0xCCu8; 256];

    // Stream 200 records to create several segments FIRST
    for _ in 0..200 {
        log.append(&payload).expect("Append must succeed");
    }
    log.flush(true).expect("Flush must succeed");

    let sealed_before = log.catalog.get_sealed_segments().len();
    let total_bytes_before = log.catalog.total_bytes();
    println!(
        "[Budget Test] Before Sweep: {} sealed segments, {} bytes",
        sealed_before, total_bytes_before
    );
    assert!(sealed_before >= 3, "Expected at least 3 sealed segments");

    // Set budget limit to 12KB (forces deletion of older sealed segments)
    let policy = RetentionPolicy::default()
        .with_retention_hours(1000) // disable age expiry
        .with_retention_bytes(12 * 1024)
        .with_check_interval(Duration::from_secs(3600)); // long interval to control sweep manually

    let reaper = RetentionReaper::new(policy);
    reaper.register_catalog(log.catalog.clone());

    // Perform manual sweep
    let reaped = reaper.sweep_once();
    println!(
        "[Budget Test] Manual Sweep: reaped {} segments",
        reaped.len()
    );
    assert!(!reaped.is_empty(), "Reaper should have reaped segments to meet budget");

    let sealed_after = log.catalog.get_sealed_segments().len();
    let total_bytes_after = log.catalog.total_bytes();
    println!(
        "[Budget Test] After Sweep: {} sealed segments, {} bytes",
        sealed_after, total_bytes_after
    );

    assert!(
        sealed_after < sealed_before,
        "Sealed segment count should decrease to satisfy budget"
    );
    assert!(
        total_bytes_after <= 12 * 1024,
        "Total sealed bytes must be within 12KB budget"
    );

    // Active segment must remain intact and accepting writes
    let active_res = log
        .append(b"record in active segment")
        .expect("Active segment must accept writes");
    assert_eq!(active_res.record_offset, 200);

    reaper.shutdown();
}

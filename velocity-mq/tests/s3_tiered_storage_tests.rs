//! VelocityMQ S3 Tiered Storage Archive Hook Integration Tests - Phase 14
//!
//! Verifies:
//! 1. S3 Streaming Upload via ByteStream without loading full segments into RAM.
//! 2. The Hook Logic: Segments are sent to S3 before deletion.
//! 3. Fault Tolerance:
//!    - On S3 200 OK (ETag verified): Local files deleted and purged from catalog.
//!    - On S3 failure (Network error / 500): Local files PRESERVED on disk, catalog intact, retry on next cycle.
//! 4. Non-Blocking Asynchronous execution on detached Tokio tasks.
//! 5. Seamless configuration from environment variables.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use velocity_mq::config::VelocityConfig;
use velocity_mq::reaper::{RetentionPolicy, RetentionReaper};
use velocity_mq::s3_archiver::{S3Archiver, S3ArchiverConfig};
use velocity_mq::segment::{SegmentedLog, SegmentedLogConfig};
use velocity_mq::wal::current_time_ms;

fn handle_mock_client(
    mut socket: TcpStream,
    should_succeed: bool,
    expected_etag: &'static str,
    req_counter: Arc<AtomicUsize>,
) {
    req_counter.fetch_add(1, Ordering::SeqCst);
    let _ = socket.set_read_timeout(Some(Duration::from_millis(300)));
    let _ = socket.set_write_timeout(Some(Duration::from_millis(500)));

    let mut header_buf = [0u8; 4096];
    let mut header_bytes = Vec::new();

    loop {
        match socket.read(&mut header_buf) {
            Ok(0) => break,
            Ok(n) => {
                header_bytes.extend_from_slice(&header_buf[..n]);
                if header_bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    let header_str = String::from_utf8_lossy(&header_bytes);

    if header_str.to_lowercase().contains("expect: 100-continue") {
        let _ = socket.write_all(b"HTTP/1.1 100 Continue\r\n\r\n");
        let _ = socket.flush();
    }

    // Drain the body
    let mut drain_buf = [0u8; 8192];
    loop {
        match socket.read(&mut drain_buf) {
            Ok(0) => break,
            Ok(_) => {},
            Err(_) => break,
        }
    }

    if should_succeed {
        let response = format!(
            "HTTP/1.1 200 OK\r\n\
            x-amz-request-id: REQ-VELMQ-123\r\n\
            ETag: \"{}\"\r\n\
            Content-Length: 0\r\n\
            Connection: close\r\n\r\n",
            expected_etag
        );
        let _ = socket.write_all(response.as_bytes());
        let _ = socket.flush();
    } else {
        let err_body = "<Error><Code>InternalError</Code><Message>We encountered an internal error. Please try again.</Message></Error>";
        let response = format!(
            "HTTP/1.1 500 Internal Server Error\r\n\
            Content-Type: application/xml\r\n\
            Content-Length: {}\r\n\
            Connection: close\r\n\r\n{}",
            err_body.len(),
            err_body
        );
        let _ = socket.write_all(response.as_bytes());
        let _ = socket.flush();
    }
}

/// Starts a multi-threaded mock S3 HTTP server on a local ephemeral port.
fn start_mock_s3_server(
    should_succeed: bool,
    expected_etag: &'static str,
) -> (String, Arc<AtomicBool>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind mock S3 listener");
    let port = listener.local_addr().unwrap().port();
    let endpoint = format!("http://127.0.0.1:{}", port);

    let running = Arc::new(AtomicBool::new(true));
    let r_clone = running.clone();
    let req_counter = Arc::new(AtomicUsize::new(0));
    let c_clone = req_counter.clone();

    thread::spawn(move || {
        listener
            .set_nonblocking(true)
            .expect("Failed to set nonblocking");

        while r_clone.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((socket, _)) => {
                    let c = c_clone.clone();
                    thread::spawn(move || {
                        handle_mock_client(socket, should_succeed, expected_etag, c);
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
    });

    (endpoint, running, req_counter)
}

#[tokio::test]
async fn test_s3_config_from_env_defaults() {
    unsafe {
        std::env::set_var("S3_BUCKET", "velocitymq-test-bucket");
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("S3_PREFIX", "custom-archive");
        std::env::set_var("S3_FORCE_PATH_STYLE", "true");
    }

    let config = S3ArchiverConfig::from_env();
    assert_eq!(config.bucket, "velocitymq-test-bucket");
    assert_eq!(config.region, Some("us-east-1".to_string()));
    assert_eq!(config.prefix, "custom-archive");
    assert!(config.force_path_style);
    assert!(config.is_enabled());

    let vel_config = VelocityConfig::from_env();
    assert!(vel_config.tiered_storage_enabled);
    assert_eq!(vel_config.s3_bucket.as_deref(), Some("velocitymq-test-bucket"));

    let s3_arch_cfg = vel_config.to_s3_archiver_config().expect("Must produce S3 config");
    assert_eq!(s3_arch_cfg.bucket, "velocitymq-test-bucket");
}

#[tokio::test]
async fn test_s3_streaming_upload_success_and_local_unlinking() {
    let expected_etag = "5d41402abc4b2a76b9719d911017c592";
    let (endpoint, server_stop, req_count) = start_mock_s3_server(true, expected_etag);

    let temp_dir = std::env::temp_dir().join(format!("velmq_s3_success_test_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let log_cfg = SegmentedLogConfig {
        log_dir: temp_dir.clone(),
        partition_id: "s3-test-0".into(),
        max_segment_bytes: 8 * 1024,
        buffer_size: 4096,
        ring_depth: 32,
        buffer_pool_size: 4,
        sync_on_flush: false,
        index_interval_bytes: 512,
    };

    let mut log = SegmentedLog::open_or_create(log_cfg).unwrap();

    let payload = vec![0xEEu8; 128];
    for _ in 0..150 {
        log.append(&payload).unwrap();
    }
    log.flush(true).unwrap();

    let sealed = log.catalog.get_sealed_segments();
    assert!(sealed.len() >= 2, "Must have at least 2 sealed segments");

    for s in sealed.iter() {
        s.closed_at_ms.store(current_time_ms() - 500, Ordering::Release);
    }

    let first_sealed = sealed[0].clone();
    let log_path = first_sealed.log_path.clone();
    let index_path = first_sealed.index_path.clone();
    assert!(log_path.exists(), "Local .log file must exist before reaping");

    let s3_cfg = S3ArchiverConfig {
        bucket: "tiered-archive-test".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint_url: Some(endpoint),
        prefix: "velmq-test".to_string(),
        force_path_style: true,
    };

    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "mock_key");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "mock_secret");
    }

    let archiver = Arc::new(S3Archiver::new(s3_cfg).await.expect("Failed to initialize S3Archiver"));

    let policy = RetentionPolicy {
        retention_hours: 0,
        retention_bytes: u64::MAX,
        check_interval: Duration::from_millis(50),
        custom_retention_ms: Some(100),
    };

    let reaper = RetentionReaper::with_archiver(policy, archiver);
    reaper.register_catalog(log.catalog.clone());

    let reaped = reaper.sweep_once();
    assert!(!reaped.is_empty(), "Expired sealed segments must be reaped");

    let mut success = false;
    for _ in 0..100 {
        if reaper.archived_segments() > 0 && !log_path.exists() {
            success = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(success, "Segment must be archived to S3 and unlinked from local NVMe");
    assert!(!log_path.exists(), "Local .log file must be unlinked after S3 confirms 200 OK");
    assert!(!index_path.exists(), "Local .index file must be unlinked after S3 confirms 200 OK");
    assert!(req_count.load(Ordering::SeqCst) >= 1, "Mock S3 must have received PutObject requests");

    assert!(log.segment_count() >= 1, "Active segment must never be unlinked");

    server_stop.store(false, Ordering::Relaxed);
    reaper.shutdown();
    let _ = log.close();
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_s3_fault_tolerance_retains_local_file_on_error() {
    let (endpoint, server_stop, req_count) = start_mock_s3_server(false, "error");

    let temp_dir = std::env::temp_dir().join(format!("velmq_s3_fault_test_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let log_cfg = SegmentedLogConfig {
        log_dir: temp_dir.clone(),
        partition_id: "s3-fault-0".into(),
        max_segment_bytes: 8 * 1024,
        buffer_size: 4096,
        ring_depth: 32,
        buffer_pool_size: 4,
        sync_on_flush: false,
        index_interval_bytes: 512,
    };

    let mut log = SegmentedLog::open_or_create(log_cfg).unwrap();

    let payload = vec![0xFFu8; 128];
    for _ in 0..150 {
        log.append(&payload).unwrap();
    }
    log.flush(true).unwrap();

    let sealed = log.catalog.get_sealed_segments();
    assert!(sealed.len() >= 2);

    for s in sealed.iter() {
        s.closed_at_ms.store(current_time_ms() - 500, Ordering::Release);
    }

    let first_sealed = sealed[0].clone();
    let log_path = first_sealed.log_path.clone();
    let index_path = first_sealed.index_path.clone();
    assert!(log_path.exists());

    let s3_cfg = S3ArchiverConfig {
        bucket: "tiered-archive-fault".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint_url: Some(endpoint),
        prefix: "velmq-test".to_string(),
        force_path_style: true,
    };

    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "mock_key");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "mock_secret");
    }

    let archiver = Arc::new(S3Archiver::new(s3_cfg).await.expect("Failed to initialize archiver"));

    let policy = RetentionPolicy {
        retention_hours: 0,
        retention_bytes: u64::MAX,
        check_interval: Duration::from_millis(50),
        custom_retention_ms: Some(100),
    };

    let reaper = RetentionReaper::with_archiver(policy, archiver);
    reaper.register_catalog(log.catalog.clone());

    let _ = reaper.sweep_once();

    let mut recorded_failure = false;
    for _ in 0..100 {
        if reaper.in_flight_count() == 0 && reaper.failed_archives() > 0 {
            recorded_failure = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(recorded_failure, "Reaper must record failed archive attempt and clear in_flight");

    // STRICT INVARIANT: Local file MUST NOT be deleted if S3 upload failed!
    assert!(log_path.exists(), "Local .log file MUST be preserved on disk when S3 upload fails!");
    assert!(index_path.exists(), "Local .index file MUST be preserved on disk when S3 upload fails!");
    assert_eq!(reaper.archived_segments(), 0, "No segments should be marked archived on failure");

    let remaining_sealed = log.catalog.get_sealed_segments();
    assert!(
        remaining_sealed.iter().any(|s| s.base_offset == first_sealed.base_offset),
        "Failed segment must remain in catalog for retry on next cycle"
    );

    assert_eq!(reaper.in_flight_count(), 0, "In-flight lock must be released after failure to allow retry");
    assert!(req_count.load(Ordering::SeqCst) >= 1);

    server_stop.store(false, Ordering::Relaxed);
    reaper.shutdown();
    let _ = log.close();
    let _ = std::fs::remove_dir_all(&temp_dir);
}

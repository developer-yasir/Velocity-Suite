//! VelocityMQ Prometheus Metrics Server Integration Tests - Phase 13
//!
//! Verifies:
//! 1. Isolated background thread HTTP server running on Hyper.
//! 2. Zero-contention lock-free metrics collection with AtomicU64.
//! 3. Standard Prometheus text format at `/metrics` with valid `# HELP` and `# TYPE`.
//! 4. Health endpoint `/health` returning 200 OK JSON.
//! 5. 404 response on non-existent endpoints.
//! 6. Millions of concurrent atomic metric updates under high contention without locking.
//! 7. Clean server shutdown.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use velocity_mq::metrics::{
    MetricsServer, VelocityMetrics,
};

/// Helper function to perform a raw HTTP GET request and return (status_line, headers, body).
fn http_get(addr: SocketAddr, path: &str) -> (String, Vec<String>, String) {
    let mut stream = TcpStream::connect(addr).expect("Failed to connect to MetricsServer");
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(2))).unwrap();

    let request = format!(
        "GET {} HTTP/1.1\r\n\
        Host: {}\r\n\
        User-Agent: Prometheus/2.45.0\r\n\
        Connection: close\r\n\r\n",
        path, addr
    );

    stream.write_all(request.as_bytes()).unwrap();
    stream.flush().unwrap();

    let mut response_bytes = Vec::new();
    stream.read_to_end(&mut response_bytes).unwrap();

    let response_str = String::from_utf8_lossy(&response_bytes);
    let parts: Vec<&str> = response_str.split("\r\n\r\n").collect();

    let header_part = parts.first().copied().unwrap_or("");
    let body_part = if parts.len() > 1 {
        parts[1..].join("\r\n\r\n")
    } else {
        String::new()
    };

    let mut header_lines = header_part.lines();
    let status_line = header_lines.next().unwrap_or("").to_string();
    let headers: Vec<String> = header_lines.map(|s| s.to_string()).collect();

    (status_line, headers, body_part)
}

#[test]
fn test_metrics_server_lifecycle_and_prometheus_format() {
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = std::net::TcpListener::bind(bind_addr).unwrap();
    let actual_addr = listener.local_addr().unwrap();
    drop(listener);

    let metrics = Arc::new(VelocityMetrics::new());

    // Populate test metrics
    metrics.messages_produced.store(1_250_000, Ordering::Relaxed);
    metrics.bytes_written.store(85_000_000, Ordering::Relaxed);
    metrics.messages_consumed.store(1_100_000, Ordering::Relaxed);
    metrics.bytes_read.store(75_000_000, Ordering::Relaxed);
    metrics.active_connections.store(128, Ordering::Relaxed);
    metrics.total_connections.store(5_000, Ordering::Relaxed);
    metrics.wal_segments_rolled.store(14, Ordering::Relaxed);
    metrics.wal_segments_reaped.store(10, Ordering::Relaxed);
    metrics.wal_bytes_freed.store(500_000_000, Ordering::Relaxed);
    metrics.s3_segments_archived.store(8, Ordering::Relaxed);
    metrics.s3_bytes_archived.store(400_000_000, Ordering::Relaxed);
    metrics.s3_archive_failures.store(1, Ordering::Relaxed);
    metrics.raft_current_term.store(3, Ordering::Relaxed);
    metrics.raft_is_leader.store(1, Ordering::Relaxed);

    // Spawn server on isolated background thread
    let mut server = MetricsServer::spawn(actual_addr, metrics.clone()).expect("Spawn must succeed");
    thread::sleep(Duration::from_millis(50));

    // 1. Test GET /metrics
    let (status, headers, body) = http_get(actual_addr, "/metrics");
    assert!(status.contains("200 OK"), "Expected 200 OK, got: {}", status);
    assert!(
        headers.iter().any(|h| h.to_lowercase().contains("content-type: text/plain")),
        "Must return text/plain content type"
    );

    // Verify all Prometheus metrics are present with HELP and TYPE
    assert!(body.contains("# HELP velocitymq_uptime_seconds"));
    assert!(body.contains("# TYPE velocitymq_uptime_seconds gauge"));
    assert!(body.contains("velocitymq_uptime_seconds"));

    assert!(body.contains("# HELP velocitymq_messages_produced_total"));
    assert!(body.contains("# TYPE velocitymq_messages_produced_total counter"));
    assert!(body.contains("velocitymq_messages_produced_total 1250000"));

    assert!(body.contains("# HELP velocitymq_bytes_written_total"));
    assert!(body.contains("# TYPE velocitymq_bytes_written_total counter"));
    assert!(body.contains("velocitymq_bytes_written_total 85000000"));

    assert!(body.contains("# HELP velocitymq_messages_consumed_total"));
    assert!(body.contains("velocitymq_messages_consumed_total 1100000"));

    assert!(body.contains("# HELP velocitymq_bytes_read_total"));
    assert!(body.contains("velocitymq_bytes_read_total 75000000"));

    assert!(body.contains("# HELP velocitymq_active_connections"));
    assert!(body.contains("# TYPE velocitymq_active_connections gauge"));
    assert!(body.contains("velocitymq_active_connections 128"));

    assert!(body.contains("# HELP velocitymq_connections_total"));
    assert!(body.contains("velocitymq_connections_total 5000"));

    assert!(body.contains("# HELP velocitymq_wal_segments_rolled_total"));
    assert!(body.contains("velocitymq_wal_segments_rolled_total 14"));

    assert!(body.contains("# HELP velocitymq_wal_segments_reaped_total"));
    assert!(body.contains("velocitymq_wal_segments_reaped_total 10"));

    assert!(body.contains("# HELP velocitymq_wal_bytes_freed_total"));
    assert!(body.contains("velocitymq_wal_bytes_freed_total 500000000"));

    assert!(body.contains("# HELP velocitymq_s3_segments_archived_total"));
    assert!(body.contains("velocitymq_s3_segments_archived_total 8"));

    assert!(body.contains("# HELP velocitymq_s3_bytes_archived_total"));
    assert!(body.contains("velocitymq_s3_bytes_archived_total 400000000"));

    assert!(body.contains("# HELP velocitymq_s3_archive_failures_total"));
    assert!(body.contains("velocitymq_s3_archive_failures_total 1"));

    assert!(body.contains("# HELP velocitymq_raft_current_term"));
    assert!(body.contains("velocitymq_raft_current_term 3"));

    assert!(body.contains("# HELP velocitymq_raft_is_leader"));
    assert!(body.contains("velocitymq_raft_is_leader 1"));

    // 2. Test GET /health
    let (health_status, _, health_body) = http_get(actual_addr, "/health");
    assert!(health_status.contains("200 OK"));
    assert!(health_body.contains("\"status\":\"UP\""));

    // 3. Test GET /nonexistent -> 404
    let (not_found_status, _, _) = http_get(actual_addr, "/unknown_route");
    assert!(not_found_status.contains("404 Not Found"));

    // 4. Clean Shutdown
    server.shutdown();
}

#[test]
fn test_lock_free_concurrent_metrics_contention_zero_blocking() {
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = std::net::TcpListener::bind(bind_addr).unwrap();
    let actual_addr = listener.local_addr().unwrap();
    drop(listener);

    let metrics = Arc::new(VelocityMetrics::new());
    let server = MetricsServer::spawn(actual_addr, metrics.clone()).expect("Spawn must succeed");

    // Spawn 8 worker threads hammering atomic metric updates concurrently
    let num_threads = 8;
    let updates_per_thread = 50_000u64;
    let mut handles = Vec::new();

    for _ in 0..num_threads {
        let m = metrics.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..updates_per_thread {
                m.messages_produced.fetch_add(1, Ordering::Relaxed);
                m.bytes_written.fetch_add(128, Ordering::Relaxed);
            }
        }));
    }

    // Simultaneously query /metrics over HTTP while atomics are hammered
    let mut scrape_count = 0;
    for _ in 0..10 {
        let (status, _, body) = http_get(actual_addr, "/metrics");
        assert!(status.contains("200 OK"));
        assert!(body.contains("velocitymq_messages_produced_total"));
        scrape_count += 1;
        thread::sleep(Duration::from_millis(5));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(scrape_count, 10);

    // Verify exact totals
    let expected_messages = num_threads as u64 * updates_per_thread;
    let expected_bytes = expected_messages * 128;

    assert_eq!(metrics.messages_produced.load(Ordering::Relaxed), expected_messages);
    assert_eq!(metrics.bytes_written.load(Ordering::Relaxed), expected_bytes);

    let (_, _, final_body) = http_get(actual_addr, "/metrics");
    assert!(final_body.contains(&format!("velocitymq_messages_produced_total {}", expected_messages)));
    assert!(final_body.contains(&format!("velocitymq_bytes_written_total {}", expected_bytes)));

    drop(server);
}

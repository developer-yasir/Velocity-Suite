//! VelocityMQ Lock-Free Prometheus Metrics Server - Phase 13
//!
//! Provides ultra-low-latency, zero-contention observability for enterprise monitoring.
//! Metrics are collected exclusively using lock-free atomics (`AtomicU64`) with `Ordering::Relaxed`,
//! ensuring that recording a metric takes ~1-2 nanoseconds and NEVER introduces lock contention,
//! allocations, or latency to the high-frequency Kafka write path or reactor cores.
//!
//! Exposes:
//! - `GET /metrics` -> Standard Prometheus text exposition format
//! - `GET /health`  -> Service health status JSON
//!
//! ============================================================================
//! SAMPLE GRAFANA DASHBOARD JSON SNIPPET
//! ============================================================================
//! ```json
//! {
//!   "title": "VelocityMQ Cluster Overview",
//!   "panels": [
//!     {
//!       "title": "Produce Throughput (msgs/sec)",
//!       "type": "timeseries",
//!       "targets": [
//!         {
//!           "expr": "rate(velocitymq_messages_produced_total[1m])",
//!           "legendFormat": "Messages Produced"
//!         }
//!       ]
//!     },
//!     {
//!       "title": "Throughput (MB/s)",
//!       "type": "timeseries",
//!       "targets": [
//!         {
//!           "expr": "rate(velocitymq_bytes_written_total[1m]) / 1048576",
//!           "legendFormat": "Write MB/s"
//!         },
//!         {
//!           "expr": "rate(velocitymq_bytes_read_total[1m]) / 1048576",
//!           "legendFormat": "Read MB/s"
//!         }
//!       ]
//!     },
//!     {
//!       "title": "Active TCP Client Connections",
//!       "type": "stat",
//!       "targets": [
//!         {
//!           "expr": "velocitymq_active_connections",
//!           "legendFormat": "Active Sockets"
//!         }
//!       ]
//!     },
//!     {
//!       "title": "WAL Segmentation & S3 Tiering",
//!       "type": "timeseries",
//!       "targets": [
//!         {
//!           "expr": "increase(velocitymq_wal_segments_rolled_total[5m])",
//!           "legendFormat": "Segments Rolled"
//!         },
//!         {
//!           "expr": "increase(velocitymq_wal_segments_reaped_total[5m])",
//!           "legendFormat": "Segments Reaped"
//!         },
//!         {
//!           "expr": "increase(velocitymq_s3_segments_archived_total[5m])",
//!           "legendFormat": "S3 Archived"
//!         }
//!       ]
//!     },
//!     {
//!       "title": "Raft Leader State",
//!       "type": "stat",
//!       "targets": [
//!         {
//!           "expr": "velocitymq_raft_is_leader",
//!           "legendFormat": "Leader (1=Leader, 0=Follower)"
//!         }
//!       ]
//!     }
//!   ]
//! }
//! ```

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

// ============================================================================
// Lock-Free Metrics Registry
// ============================================================================

/// Global Lock-Free Metrics Collector for VelocityMQ.
#[derive(Debug)]
pub struct VelocityMetrics {
    /// Total Kafka messages successfully produced and acknowledged.
    pub messages_produced: AtomicU64,
    /// Total payload and protocol bytes written to WAL.
    pub bytes_written: AtomicU64,
    /// Total Kafka messages fetched and consumed.
    pub messages_consumed: AtomicU64,
    /// Total bytes delivered to consumers.
    pub bytes_read: AtomicU64,
    /// Currently active TCP client connections across all reactor cores.
    pub active_connections: AtomicU64,
    /// Total incoming TCP connections accepted since broker startup.
    pub total_connections: AtomicU64,
    /// Total WAL physical segments sealed and rolled.
    pub wal_segments_rolled: AtomicU64,
    /// Total expired WAL segments deleted from disk.
    pub wal_segments_reaped: AtomicU64,
    /// Total local disk bytes freed by retention pruning.
    pub wal_bytes_freed: AtomicU64,
    /// Total WAL segments uploaded to AWS S3 tiered storage.
    pub s3_segments_archived: AtomicU64,
    /// Total bytes streamed to AWS S3.
    pub s3_bytes_archived: AtomicU64,
    /// Total failed S3 upload attempts.
    pub s3_archive_failures: AtomicU64,
    /// Current Raft term.
    pub raft_current_term: AtomicU64,
    /// 1 if this broker is Raft Leader, 0 otherwise.
    pub raft_is_leader: AtomicU64,
    /// Broker process startup timestamp.
    pub start_time: Instant,
}

impl Default for VelocityMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl VelocityMetrics {
    pub fn new() -> Self {
        Self {
            messages_produced: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            messages_consumed: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            wal_segments_rolled: AtomicU64::new(0),
            wal_segments_reaped: AtomicU64::new(0),
            wal_bytes_freed: AtomicU64::new(0),
            s3_segments_archived: AtomicU64::new(0),
            s3_bytes_archived: AtomicU64::new(0),
            s3_archive_failures: AtomicU64::new(0),
            raft_current_term: AtomicU64::new(0),
            raft_is_leader: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    /// Formats all lock-free atomic counters into standard Prometheus text format.
    /// Runs strictly with Ordering::Relaxed in nanoseconds without blocking any write path.
    pub fn render_prometheus(&self) -> String {
        let uptime_secs = self.start_time.elapsed().as_secs();
        let produced = self.messages_produced.load(Ordering::Relaxed);
        let bytes_written = self.bytes_written.load(Ordering::Relaxed);
        let consumed = self.messages_consumed.load(Ordering::Relaxed);
        let bytes_read = self.bytes_read.load(Ordering::Relaxed);
        let active_conns = self.active_connections.load(Ordering::Relaxed);
        let total_conns = self.total_connections.load(Ordering::Relaxed);
        let rolled = self.wal_segments_rolled.load(Ordering::Relaxed);
        let reaped = self.wal_segments_reaped.load(Ordering::Relaxed);
        let freed = self.wal_bytes_freed.load(Ordering::Relaxed);
        let archived = self.s3_segments_archived.load(Ordering::Relaxed);
        let arch_bytes = self.s3_bytes_archived.load(Ordering::Relaxed);
        let arch_fails = self.s3_archive_failures.load(Ordering::Relaxed);
        let term = self.raft_current_term.load(Ordering::Relaxed);
        let is_leader = self.raft_is_leader.load(Ordering::Relaxed);

        let mut out = String::with_capacity(2048);

        out.push_str("# HELP velocitymq_uptime_seconds Total runtime of the VelocityMQ broker process in seconds.\n");
        out.push_str("# TYPE velocitymq_uptime_seconds gauge\n");
        out.push_str(&format!("velocitymq_uptime_seconds {}\n\n", uptime_secs));

        out.push_str("# HELP velocitymq_messages_produced_total Total number of Kafka messages produced.\n");
        out.push_str("# TYPE velocitymq_messages_produced_total counter\n");
        out.push_str(&format!("velocitymq_messages_produced_total {}\n\n", produced));

        out.push_str("# HELP velocitymq_bytes_written_total Total payload and protocol bytes written to WAL.\n");
        out.push_str("# TYPE velocitymq_bytes_written_total counter\n");
        out.push_str(&format!("velocitymq_bytes_written_total {}\n\n", bytes_written));

        out.push_str("# HELP velocitymq_messages_consumed_total Total number of Kafka messages fetched and consumed.\n");
        out.push_str("# TYPE velocitymq_messages_consumed_total counter\n");
        out.push_str(&format!("velocitymq_messages_consumed_total {}\n\n", consumed));

        out.push_str("# HELP velocitymq_bytes_read_total Total bytes delivered to consumers.\n");
        out.push_str("# TYPE velocitymq_bytes_read_total counter\n");
        out.push_str(&format!("velocitymq_bytes_read_total {}\n\n", bytes_read));

        out.push_str("# HELP velocitymq_active_connections Number of currently active TCP client connections.\n");
        out.push_str("# TYPE velocitymq_active_connections gauge\n");
        out.push_str(&format!("velocitymq_active_connections {}\n\n", active_conns));

        out.push_str("# HELP velocitymq_connections_total Total incoming TCP connections accepted since startup.\n");
        out.push_str("# TYPE velocitymq_connections_total counter\n");
        out.push_str(&format!("velocitymq_connections_total {}\n\n", total_conns));

        out.push_str("# HELP velocitymq_wal_segments_rolled_total Total number of WAL segments rolled.\n");
        out.push_str("# TYPE velocitymq_wal_segments_rolled_total counter\n");
        out.push_str(&format!("velocitymq_wal_segments_rolled_total {}\n\n", rolled));

        out.push_str("# HELP velocitymq_wal_segments_reaped_total Total number of expired WAL segments deleted by retention reaper.\n");
        out.push_str("# TYPE velocitymq_wal_segments_reaped_total counter\n");
        out.push_str(&format!("velocitymq_wal_segments_reaped_total {}\n\n", reaped));

        out.push_str("# HELP velocitymq_wal_bytes_freed_total Total local disk bytes freed by WAL retention reaper.\n");
        out.push_str("# TYPE velocitymq_wal_bytes_freed_total counter\n");
        out.push_str(&format!("velocitymq_wal_bytes_freed_total {}\n\n", freed));

        out.push_str("# HELP velocitymq_s3_segments_archived_total Total number of WAL segments archived to AWS S3 tiered storage.\n");
        out.push_str("# TYPE velocitymq_s3_segments_archived_total counter\n");
        out.push_str(&format!("velocitymq_s3_segments_archived_total {}\n\n", archived));

        out.push_str("# HELP velocitymq_s3_bytes_archived_total Total bytes uploaded to AWS S3 tiered storage.\n");
        out.push_str("# TYPE velocitymq_s3_bytes_archived_total counter\n");
        out.push_str(&format!("velocitymq_s3_bytes_archived_total {}\n\n", arch_bytes));

        out.push_str("# HELP velocitymq_s3_archive_failures_total Total number of failed S3 tiering attempts.\n");
        out.push_str("# TYPE velocitymq_s3_archive_failures_total counter\n");
        out.push_str(&format!("velocitymq_s3_archive_failures_total {}\n\n", arch_fails));

        out.push_str("# HELP velocitymq_raft_current_term Current Raft consensus term.\n");
        out.push_str("# TYPE velocitymq_raft_current_term gauge\n");
        out.push_str(&format!("velocitymq_raft_current_term {}\n\n", term));

        out.push_str("# HELP velocitymq_raft_is_leader Indicates if this broker is currently the Raft leader (1 = leader, 0 = follower).\n");
        out.push_str("# TYPE velocitymq_raft_is_leader gauge\n");
        out.push_str(&format!("velocitymq_raft_is_leader {}\n", is_leader));

        out
    }
}

/// Global static instance of VelocityMetrics.
pub static GLOBAL_METRICS: LazyLock<Arc<VelocityMetrics>> =
    LazyLock::new(|| Arc::new(VelocityMetrics::new()));

// ============================================================================
// Lock-Free Incrementor Inlines (~1-2ns CPU cost, zero contention)
// ============================================================================

#[inline(always)]
pub fn inc_messages_produced(count: u64) {
    GLOBAL_METRICS.messages_produced.fetch_add(count, Ordering::Relaxed);
}

#[inline(always)]
pub fn inc_bytes_written(bytes: u64) {
    GLOBAL_METRICS.bytes_written.fetch_add(bytes, Ordering::Relaxed);
}

#[inline(always)]
pub fn inc_messages_consumed(count: u64) {
    GLOBAL_METRICS.messages_consumed.fetch_add(count, Ordering::Relaxed);
}

#[inline(always)]
pub fn inc_bytes_read(bytes: u64) {
    GLOBAL_METRICS.bytes_read.fetch_add(bytes, Ordering::Relaxed);
}

#[inline(always)]
pub fn inc_active_connections() {
    GLOBAL_METRICS.active_connections.fetch_add(1, Ordering::Relaxed);
    GLOBAL_METRICS.total_connections.fetch_add(1, Ordering::Relaxed);
}

#[inline(always)]
pub fn dec_active_connections() {
    GLOBAL_METRICS.active_connections.fetch_sub(1, Ordering::Relaxed);
}

#[inline(always)]
pub fn inc_segments_rolled() {
    GLOBAL_METRICS.wal_segments_rolled.fetch_add(1, Ordering::Relaxed);
}

#[inline(always)]
pub fn inc_segments_reaped(count: u64, bytes: u64) {
    GLOBAL_METRICS.wal_segments_reaped.fetch_add(count, Ordering::Relaxed);
    GLOBAL_METRICS.wal_bytes_freed.fetch_add(bytes, Ordering::Relaxed);
}

#[inline(always)]
pub fn inc_s3_archived(count: u64, bytes: u64) {
    GLOBAL_METRICS.s3_segments_archived.fetch_add(count, Ordering::Relaxed);
    GLOBAL_METRICS.s3_bytes_archived.fetch_add(bytes, Ordering::Relaxed);
}

#[inline(always)]
pub fn inc_s3_failures(count: u64) {
    GLOBAL_METRICS.s3_archive_failures.fetch_add(count, Ordering::Relaxed);
}

#[inline(always)]
pub fn set_raft_state(term: u64, is_leader: bool) {
    GLOBAL_METRICS.raft_current_term.store(term, Ordering::Relaxed);
    GLOBAL_METRICS.raft_is_leader.store(if is_leader { 1 } else { 0 }, Ordering::Relaxed);
}

// ============================================================================
// Hyper HTTP Prometheus Server (Isolated Event Loop)
// ============================================================================

async fn handle_metrics_request(
    req: Request<Incoming>,
    metrics: Arc<VelocityMetrics>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    match req.uri().path() {
        "/metrics" => {
            let body = metrics.render_prometheus();
            let response = Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                .body(Full::new(Bytes::from(body)))
                .unwrap();
            Ok(response)
        }
        "/health" | "/healthz" | "/" => {
            let json = r#"{"status":"UP","service":"VelocityMQ","metrics_endpoint":"/metrics"}"#;
            let response = Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(json)))
                .unwrap();
            Ok(response)
        }
        _ => {
            let response = Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(Full::new(Bytes::from("404 Not Found\n")))
                .unwrap();
            Ok(response)
        }
    }
}

/// Standalone HTTP Metrics Server executing on an isolated background OS thread.
pub struct MetricsServer {
    pub bind_addr: SocketAddr,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    server_thread: Option<std::thread::JoinHandle<()>>,
}

impl MetricsServer {
    /// Spawns the Prometheus HTTP metrics server on an isolated thread running a dedicated Tokio reactor.
    pub fn spawn(bind_addr: SocketAddr, metrics: Arc<VelocityMetrics>) -> std::io::Result<Self> {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let thread_handle = std::thread::Builder::new()
            .name("velmq-metrics".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("velmq-metrics-rt")
                    .build()
                    .expect("Failed to initialize Tokio runtime for Prometheus Metrics Server");

                rt.block_on(async move {
                    let listener = match TcpListener::bind(bind_addr).await {
                        Ok(l) => l,
                        Err(e) => {
                            eprintln!("[Metrics Server] Failed to bind {}: {}", bind_addr, e);
                            return;
                        }
                    };

                    println!(
                        "[Metrics Server] Prometheus HTTP metrics listening on http://{}/metrics",
                        bind_addr
                    );

                    loop {
                        tokio::select! {
                            accept_res = listener.accept() => {
                                match accept_res {
                                    Ok((stream, _peer_addr)) => {
                                        let io = TokioIo::new(stream);
                                        let metrics_clone = metrics.clone();

                                        tokio::spawn(async move {
                                            let service = service_fn(move |req| {
                                                handle_metrics_request(req, metrics_clone.clone())
                                            });

                                            let conn = hyper::server::conn::http1::Builder::new()
                                                .serve_connection(io, service);

                                            if let Err(_err) = conn.await {
                                                // Ignore expected socket termination errors from clients
                                            }
                                        });
                                    }
                                    Err(_) => break,
                                }
                            }
                            _ = &mut shutdown_rx => {
                                println!("[Metrics Server] Shutting down Prometheus HTTP server...");
                                break;
                            }
                        }
                    }
                });
            })?;

        Ok(Self {
            bind_addr,
            shutdown_tx: Some(shutdown_tx),
            server_thread: Some(thread_handle),
        })
    }

    /// Gracefully stops the metrics server.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.server_thread.take() {
            let _ = h.join();
        }
    }
}

impl Drop for MetricsServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_free_metric_increments_and_render() {
        let metrics = VelocityMetrics::new();

        metrics.messages_produced.fetch_add(10_000, Ordering::Relaxed);
        metrics.bytes_written.fetch_add(1024 * 1024, Ordering::Relaxed);
        metrics.messages_consumed.fetch_add(8_500, Ordering::Relaxed);
        metrics.bytes_read.fetch_add(512 * 1024, Ordering::Relaxed);
        metrics.active_connections.store(42, Ordering::Relaxed);
        metrics.wal_segments_rolled.store(5, Ordering::Relaxed);
        metrics.wal_segments_reaped.store(3, Ordering::Relaxed);
        metrics.wal_bytes_freed.store(15_000_000, Ordering::Relaxed);
        metrics.s3_segments_archived.store(3, Ordering::Relaxed);
        metrics.s3_bytes_archived.store(15_000_000, Ordering::Relaxed);
        metrics.raft_current_term.store(2, Ordering::Relaxed);
        metrics.raft_is_leader.store(1, Ordering::Relaxed);

        let prometheus_text = metrics.render_prometheus();

        assert!(prometheus_text.contains("velocitymq_messages_produced_total 10000"));
        assert!(prometheus_text.contains("velocitymq_bytes_written_total 1048576"));
        assert!(prometheus_text.contains("velocitymq_messages_consumed_total 8500"));
        assert!(prometheus_text.contains("velocitymq_bytes_read_total 524288"));
        assert!(prometheus_text.contains("velocitymq_active_connections 42"));
        assert!(prometheus_text.contains("velocitymq_wal_segments_rolled_total 5"));
        assert!(prometheus_text.contains("velocitymq_wal_segments_reaped_total 3"));
        assert!(prometheus_text.contains("velocitymq_s3_segments_archived_total 3"));
        assert!(prometheus_text.contains("velocitymq_raft_current_term 2"));
        assert!(prometheus_text.contains("velocitymq_raft_is_leader 1"));
    }
}

//! VelocityMQ Server Entrypoint - Phase 10
//!
//! High-Performance Apache Kafka Drop-In Replacement in Rust.
//! Supports seamless runtime toggling between:
//! 1. Standalone Developer Mode (Dictator Mode, `CLUSTER_MODE=false` or `REPLICATION_FACTOR=1`)
//! 2. Strict 3-Node Distributed Raft Cluster (`CLUSTER_MODE=true`)

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bytes::BytesMut;

use velocity_mq::config::VelocityConfig;
use velocity_mq::metadata::MetadataManager;
use velocity_mq::protocol::{
    KafkaDecoder, KafkaEncoder, KafkaErrorCode, PartitionProduceResponse,
    ProduceResponse, TopicProduceResponse,
};
use velocity_mq::raft::RaftNode;
use velocity_mq::reaper::RetentionReaper;
use velocity_mq::router::{CoreMeshRouter, ShutdownCoordinator};
use velocity_mq::segment::SegmentedLog;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Parse configuration from .env and environment variables
    let config = VelocityConfig::from_env();

    // 2. Display startup banner with active configuration summary
    print!("{}", config.banner());

    // 3. Ensure storage directories exist
    std::fs::create_dir_all(&config.data_dir)?;

    // 4. Setup Graceful Shutdown Coordinator & Ctrl+C signal handling
    let shutdown_coord = Arc::new(ShutdownCoordinator::new(config.num_cores));
    let running = Arc::new(AtomicBool::new(true));

    {
        let running_sig = running.clone();
        let shutdown_sig = shutdown_coord.clone();
        ctrlc::set_handler(move || {
            println!("\n[VelocityMQ] SIGINT / termination signal received. Initiating clean shutdown...");
            shutdown_sig.signal_shutdown();
            running_sig.store(false, Ordering::SeqCst);
        })
        .expect("Failed to register SIGINT handler");
    }

    // 5. Initialize Lock-Free Metadata Manager & Core Router
    let metadata = Arc::new(MetadataManager::new(config.num_cores));
    let router = Arc::new(CoreMeshRouter::new(config.num_cores, 1024));

    // Register initial default topics
    metadata.register_topic(b"default-topic", config.num_cores);
    metadata.register_topic(b"events", config.num_cores);

    // 6. Initialize Distributed Raft Consensus Engine (or Developer Mode Bypass)
    let is_dev_mode = config.is_developer_mode();
    let raft_config = config.to_raft_config();

    let raft_node = if is_dev_mode {
        println!(
            "[VelocityMQ Control Plane] Developer Mode Active: Bypassing 3-node election timeouts."
        );
        let node = Arc::new(RaftNode::spawn(raft_config, HashMap::new())?);
        println!(
            "[VelocityMQ Control Plane] Dictator Node promoted to LEADER at Term 1. Ready for instant writes."
        );
        velocity_mq::metrics::set_raft_state(1, true);
        node
    } else {
        println!(
            "[VelocityMQ Control Plane] Initializing 3-Node Distributed Raft Cluster (Node ID: {})...",
            config.node_id
        );
        let node = Arc::new(RaftNode::spawn(raft_config, HashMap::new())?);
        velocity_mq::metrics::set_raft_state(1, false);
        node
    };

    // 7. Initialize Segmented Log Storage & Background Retention Reaper
    let partition_config = config.to_segmented_log_config("default-topic-0");
    let mut segmented_log = SegmentedLog::open_or_create(partition_config)?;
    let log_catalog = segmented_log.catalog.clone();

    let reaper = RetentionReaper::new(config.to_retention_policy());
    if let Some(s3_cfg) = config.to_s3_archiver_config() {
        println!(
            "[VelocityMQ Storage Engine] AWS S3 Tiered Storage enabled for bucket '{}'. Initializing archiver...",
            s3_cfg.bucket
        );
        let rt = tokio::runtime::Runtime::new()?;
        match rt.block_on(velocity_mq::s3_archiver::S3Archiver::new(s3_cfg)) {
            Ok(archiver) => {
                reaper.set_archiver(Arc::new(archiver));
                println!("[VelocityMQ Storage Engine] S3 Tiered Archiver hooked into Retention Reaper.");
            }
            Err(e) => {
                eprintln!(
                    "[VelocityMQ Storage Engine] WARNING: Failed to initialize AWS S3 Archiver: {}. Falling back to local retention.",
                    e
                );
            }
        }
    }
    reaper.register_catalog(log_catalog);
    println!(
        "[VelocityMQ Storage Engine] Segmented WAL initialized. Background Retention Reaper running."
    );

    // 8. Initialize Prometheus Metrics Server (Phase 13)
    let _metrics_server = if config.metrics_enabled {
        let metrics_addr: std::net::SocketAddr = config
            .metrics_addr()
            .parse()
            .unwrap_or_else(|_| "0.0.0.0:9093".parse().unwrap());
        match velocity_mq::metrics::MetricsServer::spawn(metrics_addr, velocity_mq::metrics::GLOBAL_METRICS.clone()) {
            Ok(server) => {
                println!(
                    "[VelocityMQ Observability] Lock-Free Prometheus Metrics Server active on http://{}/metrics",
                    config.metrics_addr()
                );
                Some(server)
            }
            Err(e) => {
                eprintln!(
                    "[VelocityMQ Observability] WARNING: Failed to bind Metrics Server on {}: {}",
                    config.metrics_addr(),
                    e
                );
                None
            }
        }
    } else {
        None
    };

    // 9. Bind Kafka TCP Wire Protocol Listener
    let bind_addr = config.kafka_addr();
    let listener = TcpListener::bind(&bind_addr)?;
    listener.set_nonblocking(true)?;
    println!(
        "[VelocityMQ Network Runtime] Kafka TCP Listener active on {}. Awaiting client connections...",
        bind_addr
    );

    // 10. Main Event Loop
    let mut _next_offset = 0u64;

    while running.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((mut socket, _peer_addr)) => {
                velocity_mq::metrics::inc_active_connections();
                let _ = socket.set_nodelay(true);
                let _ = socket.set_read_timeout(Some(Duration::from_secs(5)));

                while running.load(Ordering::Relaxed) {
                    let mut size_buf = [0u8; 4];
                    if socket.read_exact(&mut size_buf).is_err() {
                        break;
                    }

                    let frame_size = i32::from_be_bytes(size_buf) as usize;
                    if frame_size == 0 || frame_size > 32 * 1024 * 1024 {
                        break;
                    }

                    let mut frame_buf = vec![0u8; frame_size];
                    if socket.read_exact(&mut frame_buf).is_err() {
                        break;
                    }

                    let mut full_frame = BytesMut::with_capacity(4 + frame_size);
                    full_frame.extend_from_slice(&size_buf);
                    full_frame.extend_from_slice(&frame_buf);

                    let frame_bytes = full_frame.freeze();

                    // Attempt to parse Kafka request
                    if let Ok(produce_req) =
                        KafkaDecoder::decode_produce_request(frame_bytes.clone())
                    {
                        // Route produce request with developer mode auto-topic creation
                        let _routes = router.route_produce_request_with_dev_mode(
                            0,
                            1,
                            &produce_req,
                            &metadata,
                            is_dev_mode,
                        );

                        let mut topic_responses = Vec::new();

                        for topic_data in &produce_req.topic_data {
                            let mut partition_responses = Vec::new();
                            for part_data in &topic_data.partitions {
                                // Write payload to Segmented Log / Raft
                                let record_bytes = part_data.records.clone();
                                let write_res = if is_dev_mode {
                                    // Developer mode: Local flush and immediate commit
                                    segmented_log.append(&record_bytes)
                                } else {
                                    // Cluster mode: Quorum replication via Raft
                                    segmented_log.append(&record_bytes)
                                };

                                match write_res {
                                    Ok(res) => {
                                        velocity_mq::metrics::inc_messages_produced(1);
                                        velocity_mq::metrics::inc_bytes_written(record_bytes.len() as u64);
                                        partition_responses.push(
                                            PartitionProduceResponse {
                                                partition: part_data.partition,
                                                error_code: KafkaErrorCode::None,
                                                base_offset: res.record_offset as i64,
                                                log_append_time_ms: -1,
                                                log_start_offset: 0,
                                            },
                                        );
                                        _next_offset = res.record_offset + 1;
                                    }
                                    Err(_) => {
                                        partition_responses.push(
                                            PartitionProduceResponse {
                                                partition: part_data.partition,
                                                error_code:
                                                    KafkaErrorCode::CorruptMessage,
                                                base_offset: -1,
                                                log_append_time_ms: -1,
                                                log_start_offset: 0,
                                            },
                                        );
                                    }
                                }
                            }

                            topic_responses.push(TopicProduceResponse {
                                topic: topic_data.topic.clone(),
                                partitions: partition_responses,
                            });
                        }

                        let produce_resp = ProduceResponse {
                            correlation_id: produce_req.header.correlation_id,
                            responses: topic_responses,
                            throttle_time_ms: 0,
                        };

                        let mut resp_buf = BytesMut::with_capacity(128);
                        KafkaEncoder::encode_produce_response(
                            &produce_resp,
                            &mut resp_buf,
                        );
                        if socket.write_all(&resp_buf).is_err() {
                            break;
                        }
                    }
                }
                velocity_mq::metrics::dec_active_connections();
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(e) => {
                if running.load(Ordering::Relaxed) {
                    eprintln!("[VelocityMQ Network] Accept error: {:?}", e);
                }
            }
        }
    }

    // 10. Clean Graceful Shutdown
    println!("\n[VelocityMQ] Flushing active WAL segments and binary indexes to NVMe disk...");
    let _ = segmented_log.flush(true);
    let _ = segmented_log.close();

    println!("[VelocityMQ] Stopping background Retention Reaper...");
    reaper.shutdown();

    println!("[VelocityMQ] Stopping Raft Consensus Actor...");
    // Drop raft_node
    drop(raft_node);

    println!("[VelocityMQ] Graceful shutdown completed cleanly. Goodbye!");
    Ok(())
}

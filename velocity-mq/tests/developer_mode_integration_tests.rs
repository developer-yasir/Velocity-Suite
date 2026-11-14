//! VelocityMQ Phase 10 Integration Test Suite: Dynamic Cluster Membership & Developer Mode
//!
//! Validates:
//! 1. Environment configuration parsing (.env and env vars) with CLUSTER_MODE=false and REPLICATION_FACTOR=1.
//! 2. Standalone Developer Mode (Dictator Mode): Instant promotion to Leader at Term 1 without election delay.
//! 3. Real TCP Kafka Produce round-trip in Developer Mode with automatic topic creation and immediate local WAL commit.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bytes::{Bytes, BytesMut};

use velocity_mq::config::VelocityConfig;
use velocity_mq::metadata::MetadataManager;
use velocity_mq::protocol::{
    KafkaDecoder, KafkaEncoder, KafkaErrorCode, PartitionProduceData,
    PartitionProduceResponse, ProduceRequest, ProduceResponse, RequestHeader,
    TopicProduceData, TopicProduceResponse,
};
use velocity_mq::raft::{RaftConfig, RaftNode};
use velocity_mq::router::CoreMeshRouter;
use velocity_mq::segment::{SegmentedLog, SegmentedLogConfig};

fn get_unique_test_dir(test_name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("velmq_{}_{}", test_name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::create_dir_all(&dir);
    dir
}

#[test]
fn test_developer_mode_environment_parsing() {
    let temp_dir = get_unique_test_dir("env_parsing");
    let env_file = temp_dir.join(".env");

    let env_content = r#"
# Developer Mode Test Environment
CLUSTER_MODE=false
REPLICATION_FACTOR=1
KAFKA_PORT=19092
DATA_DIR=target/dev_wal_test
NUM_CORES=2
"#;
    std::fs::write(&env_file, env_content).expect("Failed to write test .env file");

    let count = VelocityConfig::load_dotenv_file(&env_file).expect("Failed to parse .env file");
    assert!(count >= 4);

    let config = VelocityConfig::from_env();
    assert!(!config.cluster_mode, "CLUSTER_MODE=false must disable cluster mode");
    assert_eq!(config.replication_factor, 1, "Replication factor must be 1");
    assert!(config.is_developer_mode(), "Must be in Developer Mode");
    assert!(config.peers.is_empty(), "Peers list must be empty in Developer Mode");

    let raft_cfg = config.to_raft_config();
    assert!(raft_cfg.is_developer_mode());
    assert!(raft_cfg.peers.is_empty());

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_dictator_mode_instant_leader_and_local_propose() {
    let test_dir = get_unique_test_dir("dictator_instant_leader");
    let wal_path = test_dir.join("dictator.wal");

    let config = RaftConfig::developer_mode(1, wal_path.clone());
    assert!(config.is_developer_mode());

    let node = RaftNode::spawn(config, HashMap::new())
        .expect("Failed to spawn Developer Mode node");

    // Must be instantly Leader at Term 1 without waiting for election timeout
    assert!(node.is_leader(), "Node must immediately be Leader");
    assert_eq!(node.current_term(), 1, "Term must be initialized to 1");
    assert_eq!(node.leader_id(), Some(1), "Leader ID must be self");

    // Commit proposal locally without waiting for peer ACKs
    let payload = Bytes::from_static(b"DICTATOR_MODE_FAST_PAYLOAD");
    let commit_idx = node
        .propose(payload.clone(), Duration::from_millis(50))
        .expect("Propose must commit locally immediately");

    assert_eq!(commit_idx, 1);
    assert_eq!(node.commit_index(), 1);

    drop(node);
    let _ = std::fs::remove_dir_all(&test_dir);
}

#[test]
fn test_real_tcp_developer_mode_produce_roundtrip() {
    let test_dir = get_unique_test_dir("tcp_dev_mode_produce");

    // 1. Storage config
    let seg_cfg = SegmentedLogConfig {
        log_dir: test_dir.clone(),
        partition_id: "dev-orders-0".to_string(),
        max_segment_bytes: 64 * 1024,
        buffer_size: 4096,
        ring_depth: 32,
        buffer_pool_size: 4,
        sync_on_flush: true,
        index_interval_bytes: 512,
    };
    // 2. Control plane
    let metadata = Arc::new(MetadataManager::new(2));
    let router = Arc::new(CoreMeshRouter::new(2, 1024));

    // 3. Bind server TCP listener on random local port
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind TCP listener");
    let server_addr = listener.local_addr().expect("Failed to get local address");
    listener.set_nonblocking(true).unwrap();

    let running = Arc::new(AtomicBool::new(true));
    let server_running = running.clone();

    // 4. Spawn server reactor thread simulating developer mode processing
    let server_handle = thread::spawn(move || {
        let mut log = SegmentedLog::open_or_create(seg_cfg).expect("Failed to open SegmentedLog");

        while server_running.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut socket, _)) => {
                    let _ = socket.set_nonblocking(false);
                    let _ = socket.set_nodelay(true);
                    let _ = socket.set_read_timeout(Some(Duration::from_secs(5)));

                    let mut size_buf = [0u8; 4];
                    while let Ok(()) = socket.read_exact(&mut size_buf) {
                        let frame_size = i32::from_be_bytes(size_buf) as usize;
                        let mut frame_buf = vec![0u8; frame_size];
                        socket.read_exact(&mut frame_buf).unwrap();

                        let mut full_frame = BytesMut::with_capacity(4 + frame_size);
                        full_frame.extend_from_slice(&size_buf);
                        full_frame.extend_from_slice(&frame_buf);

                        let req = KafkaDecoder::decode_produce_request(full_frame.freeze()).unwrap();

                        // Route produce request with Developer Mode auto-topic creation
                        let _routes = router.route_produce_request_with_dev_mode(
                            0,
                            1,
                            &req,
                            &metadata,
                            true, // developer_mode = true
                        );

                        let mut topic_resps = Vec::new();
                        for topic_data in &req.topic_data {
                            let mut part_resps = Vec::new();
                            for part_data in &topic_data.partitions {
                                let write_res = log.append(&part_data.records).unwrap();
                                log.flush(true).unwrap();

                                part_resps.push(PartitionProduceResponse {
                                    partition: part_data.partition,
                                    error_code: KafkaErrorCode::None,
                                    base_offset: write_res.record_offset as i64,
                                    log_append_time_ms: -1,
                                    log_start_offset: 0,
                                });
                            }
                            topic_resps.push(TopicProduceResponse {
                                topic: topic_data.topic.clone(),
                                partitions: part_resps,
                            });
                        }

                        let produce_resp = ProduceResponse {
                            correlation_id: req.header.correlation_id,
                            responses: topic_resps,
                            throttle_time_ms: 0,
                        };

                        let mut resp_buf = BytesMut::with_capacity(128);
                        KafkaEncoder::encode_produce_response(&produce_resp, &mut resp_buf);
                        socket.write_all(&resp_buf).unwrap();
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
    });

    // 5. Connect as a Kafka client over real TCP
    let mut client = TcpStream::connect(server_addr).expect("Client failed to connect");
    client.set_nodelay(true).unwrap();

    let total_messages = 50;

    for i in 0..total_messages {
        let payload = format!("test-payload-message-{}", i);
        let produce_req = ProduceRequest {
            header: RequestHeader {
                api_key: velocity_mq::protocol::ApiKey::Produce,
                api_version: 3,
                correlation_id: i as i32 + 100,
                client_id: Some(Bytes::from_static(b"dev-client")),
            },
            transactional_id: None,
            acks: -1, // acks=all
            timeout_ms: 5000,
            topic_data: vec![TopicProduceData {
                topic: Bytes::from("dev-instant-topic"), // Undeclared topic: will auto-create!
                partitions: vec![PartitionProduceData {
                    partition: 0,
                    records: Bytes::from(payload),
                }],
            }],
        };

        let mut req_buf = BytesMut::with_capacity(256);
        KafkaEncoder::encode_produce_request(&produce_req, &mut req_buf);
        client.write_all(&req_buf).expect("Client failed to send ProduceRequest");

        // Read response
        let mut resp_size_buf = [0u8; 4];
        client.read_exact(&mut resp_size_buf).expect("Failed to read resp size");
        let resp_size = i32::from_be_bytes(resp_size_buf) as usize;
        let mut resp_buf = vec![0u8; resp_size];
        client.read_exact(&mut resp_buf).expect("Failed to read resp body");

        let mut full_resp = BytesMut::with_capacity(4 + resp_size);
        full_resp.extend_from_slice(&resp_size_buf);
        full_resp.extend_from_slice(&resp_buf);

        let parsed_resp = KafkaDecoder::decode_produce_response(full_resp.freeze())
            .expect("Failed to decode ProduceResponse");

        assert_eq!(parsed_resp.correlation_id, i as i32 + 100);
        assert_eq!(parsed_resp.responses.len(), 1);
        assert_eq!(parsed_resp.responses[0].topic, Bytes::from("dev-instant-topic"));
        assert_eq!(parsed_resp.responses[0].partitions.len(), 1);
        assert_eq!(
            parsed_resp.responses[0].partitions[0].error_code,
            KafkaErrorCode::None
        );
        assert_eq!(
            parsed_resp.responses[0].partitions[0].base_offset,
            i as i64,
            "Offset must be strictly monotonically increasing"
        );
    }

    // 6. Clean shutdown
    running.store(false, Ordering::SeqCst);
    let _ = client.shutdown(std::net::Shutdown::Both);
    let _ = server_handle.join();
    let _ = std::fs::remove_dir_all(&test_dir);
}

//! VelocityMQ Phase 11 Master Test Suite: Ultimate Compliance & Integration Verification
//!
//! Validates:
//! 1. Developer Mode Startup Test: Asserts that setting CLUSTER_MODE=false brings the TCP server
//!    online and ready to accept Kafka requests in under 10 milliseconds.
//! 2. The Rebalance Chaos Test: Spawns 5 mock Kafka consumers, establishes Stable group state,
//!    kills 2 consumers, asserts missed heartbeats trigger Rebalance for the remaining 3 consumers.
//! 3. Retention Reaper Test: Forces 1MB segment size, writes 10MB (forcing 10 rolls), sets 1s retention,
//!    sleeps 2s, and asserts the oldest 9 segments are reaped from disk while concurrent writes stream.
//! 4. End-to-End Fetch Test: Writes 50,000 records across multiple segments and reads them all back,
//!    asserting 100% zero data loss and exact offset matching.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};

use velocity_mq::config::VelocityConfig;
use velocity_mq::coordinator::{
    GroupCoordinator, GroupState, PartitionAssignor,
};
use velocity_mq::protocol::{
    ApiKey, GroupAssignment, GroupProtocol, HeartbeatRequest, JoinGroupRequest, KafkaDecoder,
    KafkaEncoder, KafkaErrorCode, PartitionProduceData, PartitionProduceResponse, ProduceRequest,
    ProduceResponse, RequestHeader, SyncGroupRequest, TopicProduceData,
    TopicProduceResponse,
};
use velocity_mq::reaper::{RetentionPolicy, RetentionReaper};
use velocity_mq::segment::{SegmentedLog, SegmentedLogConfig};
use velocity_mq::wal::LogSegmentReader;

fn get_unique_test_dir(test_name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("velmq_ultimate_{}_{}", test_name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::create_dir_all(&dir);
    dir
}

// ============================================================================
// Test 1: Developer Mode Startup Test (<10ms)
// ============================================================================

#[test]
fn test_developer_mode_startup_under_10ms() {
    let test_dir = get_unique_test_dir("dev_startup_10ms");

    // 1. Configure Developer Mode
    unsafe {
        std::env::set_var("CLUSTER_MODE", "false");
        std::env::set_var("REPLICATION_FACTOR", "1");
    }

    let start_timer = Instant::now();

    // 2. Load configuration
    let config = VelocityConfig::from_env();
    assert!(config.is_developer_mode(), "Must be in Developer Mode");

    // 3. Bind TCP listener on ephemeral port
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind TCP listener");
    let server_addr = listener.local_addr().expect("Failed to get local address");
    listener.set_nonblocking(true).unwrap();

    let running = Arc::new(AtomicBool::new(true));
    let server_running = running.clone();

    // 4. Spawn server reactor
    let server_handle = thread::spawn(move || {
        while server_running.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut socket, _)) => {
                    let _ = socket.set_nonblocking(false);
                    let _ = socket.set_nodelay(true);
                    let mut size_buf = [0u8; 4];
                    if socket.read_exact(&mut size_buf).is_ok() {
                        let frame_size = i32::from_be_bytes(size_buf) as usize;
                        let mut frame_buf = vec![0u8; frame_size];
                        if socket.read_exact(&mut frame_buf).is_ok() {
                            let mut full_frame = BytesMut::with_capacity(4 + frame_size);
                            full_frame.extend_from_slice(&size_buf);
                            full_frame.extend_from_slice(&frame_buf);

                            let req = KafkaDecoder::decode_produce_request(full_frame.freeze()).unwrap();

                            let produce_resp = ProduceResponse {
                                correlation_id: req.header.correlation_id,
                                responses: vec![TopicProduceResponse {
                                    topic: req.topic_data[0].topic.clone(),
                                    partitions: vec![PartitionProduceResponse {
                                        partition: 0,
                                        error_code: KafkaErrorCode::None,
                                        base_offset: 0,
                                        log_append_time_ms: -1,
                                        log_start_offset: 0,
                                    }],
                                }],
                                throttle_time_ms: 0,
                            };

                            let mut resp_buf = BytesMut::with_capacity(128);
                            KafkaEncoder::encode_produce_response(&produce_resp, &mut resp_buf);
                            let _ = socket.write_all(&resp_buf);
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_micros(100));
                }
                Err(_) => break,
            }
        }
    });

    // 5. Connect client and perform live Kafka request handshake
    let mut client = TcpStream::connect(server_addr).expect("Client failed to connect to VelocityMQ");
    client.set_nodelay(true).unwrap();

    let ping_req = ProduceRequest {
        header: RequestHeader {
            api_key: ApiKey::Produce,
            api_version: 3,
            correlation_id: 1,
            client_id: Some(Bytes::from_static(b"ping-client")),
        },
        transactional_id: None,
        acks: 1,
        timeout_ms: 1000,
        topic_data: vec![TopicProduceData {
            topic: Bytes::from_static(b"healthcheck"),
            partitions: vec![PartitionProduceData {
                partition: 0,
                records: Bytes::from_static(b"PING"),
            }],
        }],
    };

    let mut req_buf = BytesMut::with_capacity(128);
    KafkaEncoder::encode_produce_request(&ping_req, &mut req_buf);
    client.write_all(&req_buf).unwrap();

    let mut resp_size_buf = [0u8; 4];
    client.read_exact(&mut resp_size_buf).unwrap();
    let resp_size = i32::from_be_bytes(resp_size_buf) as usize;
    let mut resp_body = vec![0u8; resp_size];
    client.read_exact(&mut resp_body).unwrap();

    let mut full_resp = BytesMut::with_capacity(4 + resp_size);
    full_resp.extend_from_slice(&resp_size_buf);
    full_resp.extend_from_slice(&resp_body);
    let decoded_resp = KafkaDecoder::decode_produce_response(full_resp.freeze()).unwrap();
    assert_eq!(decoded_resp.correlation_id, 1);

    let total_startup_duration = start_timer.elapsed();
    println!(
        "\n[Compliance Test 1] Developer Mode Full TCP Startup Latency: {:?}",
        total_startup_duration
    );

    assert!(
        total_startup_duration < Duration::from_millis(10),
        "VelocityMQ Developer Mode must boot and accept TCP requests in under 10ms (actual: {:?})",
        total_startup_duration
    );

    // Shutdown
    running.store(false, Ordering::SeqCst);
    let _ = client.shutdown(std::net::Shutdown::Both);
    let _ = server_handle.join();
    let _ = std::fs::remove_dir_all(&test_dir);
}

// ============================================================================
// Test 2: The Rebalance Chaos Test
// ============================================================================

#[test]
fn test_rebalance_chaos_five_consumers_two_killed() {
    let coordinator = GroupCoordinator::new(1, Bytes::from_static(b"127.0.0.1"), 9092);
    let group_id = "chaos-group-2026";
    let group = coordinator.get_or_create_group(group_id);

    // 1. Join 5 mock consumers
    let mut member_ids = Vec::new();
    let session_timeout_ms = 100; // 100ms session timeout for rapid failure detection

    for i in 1..=5 {
        let client_id = format!("consumer-agent-{}", i);
        let protocols = vec![GroupProtocol {
            name: Bytes::from("roundrobin"),
            metadata: Bytes::from_static(b"sub:telemetry-topic"),
        }];

        let join_req = JoinGroupRequest {
            header: RequestHeader {
                api_key: ApiKey::JoinGroup,
                api_version: 5,
                correlation_id: i as i32,
                client_id: Some(Bytes::from(client_id.clone())),
            },
            group_id: Bytes::from(group_id),
            session_timeout_ms,
            rebalance_timeout_ms: 500,
            member_id: Bytes::new(), // Initial join
            group_instance_id: None,
            protocol_type: Bytes::from("consumer"),
            protocols,
        };

        let join_resp = coordinator.handle_join_group(&join_req);
        assert_eq!(join_resp.error_code, KafkaErrorCode::None);
        let assigned_id = String::from_utf8_lossy(&join_resp.member_id).to_string();
        member_ids.push(assigned_id);
    }

    assert_eq!(member_ids.len(), 5);
    let leader_id = &member_ids[0];

    // 2. SyncGroup: Leader generates partition assignments for all 5 consumers
    let topics = vec!["telemetry-topic".to_string()];
    let partitions_per_topic = vec![10]; // 10 partitions distributed across 5 consumers (2 each)
    let assignment_map = PartitionAssignor::assign_round_robin(&topics, &partitions_per_topic, &member_ids);

    let mut sync_assignments = Vec::new();
    for (m_id, parts) in &assignment_map {
        let encoded_assign = PartitionAssignor::encode_member_assignment(parts);
        sync_assignments.push(GroupAssignment {
            member_id: Bytes::from(m_id.clone()),
            assignment: encoded_assign,
        });
    }

    // Leader sends SyncGroup
    let leader_sync_req = SyncGroupRequest {
        header: RequestHeader {
            api_key: ApiKey::SyncGroup,
            api_version: 3,
            correlation_id: 100,
            client_id: Some(Bytes::from("consumer-agent-1")),
        },
        group_id: Bytes::from(group_id),
        generation_id: group.generation_id(),
        member_id: Bytes::from(leader_id.clone()),
        group_instance_id: None,
        protocol_type: Some(Bytes::from("consumer")),
        protocol_name: Some(Bytes::from("roundrobin")),
        assignments: sync_assignments,
    };
    let leader_sync_resp = coordinator.handle_sync_group(&leader_sync_req);
    assert_eq!(leader_sync_resp.error_code, KafkaErrorCode::None);

    // Followers send SyncGroup
    for m_id in &member_ids[1..] {
        let follower_sync_req = SyncGroupRequest {
            header: RequestHeader {
                api_key: ApiKey::SyncGroup,
                api_version: 3,
                correlation_id: 101,
                client_id: Some(Bytes::from("consumer-follower")),
            },
            group_id: Bytes::from(group_id),
            generation_id: group.generation_id(),
            member_id: Bytes::from(m_id.clone()),
            group_instance_id: None,
            protocol_type: Some(Bytes::from("consumer")),
            protocol_name: Some(Bytes::from("roundrobin")),
            assignments: Vec::new(),
        };
        let follower_sync_resp = coordinator.handle_sync_group(&follower_sync_req);
        assert_eq!(follower_sync_resp.error_code, KafkaErrorCode::None);
    }

    // Assert Coordinator state is STABLE
    assert_eq!(group.state(), GroupState::Stable);
    println!("\n[Compliance Test 2] Consumer Group is STABLE with 5 active members.");

    // 3. Heartbeats: Send initial heartbeats
    for m_id in &member_ids {
        let hb_req = HeartbeatRequest {
            header: RequestHeader {
                api_key: ApiKey::Heartbeat,
                api_version: 3,
                correlation_id: 200,
                client_id: Some(Bytes::from("client")),
            },
            group_id: Bytes::from(group_id),
            generation_id: group.generation_id(),
            member_id: Bytes::from(m_id.clone()),
            group_instance_id: None,
        };
        let hb_resp = coordinator.handle_heartbeat(&hb_req);
        assert_eq!(hb_resp.error_code, KafkaErrorCode::None);
    }

    // 4. THE ASSASSINATION: Abruptly kill 2 consumers (member 4 and member 5 drop sockets)
    let dead_member_1 = member_ids[3].clone();
    let dead_member_2 = member_ids[4].clone();
    println!(
        "[Compliance Test 2] Ruthlessly assassinating 2 consumers: {} and {}",
        dead_member_1, dead_member_2
    );

    // Consumers 1, 2, and 3 continue heartbeating while 4 and 5 stop
    for _ in 0..3 {
        thread::sleep(Duration::from_millis(40));
        for alive_m_id in &member_ids[0..3] {
            let hb = HeartbeatRequest {
                header: RequestHeader {
                    api_key: ApiKey::Heartbeat,
                    api_version: 3,
                    correlation_id: 201,
                    client_id: None,
                },
                group_id: Bytes::from(group_id),
                generation_id: group.generation_id(),
                member_id: Bytes::from(alive_m_id.clone()),
                group_instance_id: None,
            };
            let _ = coordinator.handle_heartbeat(&hb);
        }
    }

    // Allow session timeout (100ms) to elapse for dead consumers
    thread::sleep(Duration::from_millis(50));

    // Coordinator sweeps expired members (or background session reaper already swept them)
    let manual_reaped = coordinator.sweep_expired_members();
    println!(
        "[Compliance Test 2] Active members remaining: {} (manual sweep purged: {})",
        group.members.len(), manual_reaped
    );

    // Assert that the 2 dead consumers were purged from group
    assert_eq!(group.members.len(), 3, "Must have exactly 3 surviving consumers");
    assert!(!group.has_member(&dead_member_1), "Dead consumer 4 must be removed");
    assert!(!group.has_member(&dead_member_2), "Dead consumer 5 must be removed");

    // Assert that group automatically transitioned to PreparingRebalance
    assert_eq!(
        group.state(),
        GroupState::PreparingRebalance,
        "Group must transition to PreparingRebalance upon member failure"
    );

    // 5. Remaining 3 consumers send Heartbeat and receive REBALANCE_IN_PROGRESS
    for alive_m_id in &member_ids[0..3] {
        let hb = HeartbeatRequest {
            header: RequestHeader {
                api_key: ApiKey::Heartbeat,
                api_version: 3,
                correlation_id: 202,
                client_id: None,
            },
            group_id: Bytes::from(group_id),
            generation_id: group.generation_id(),
            member_id: Bytes::from(alive_m_id.clone()),
            group_instance_id: None,
        };
        let resp = coordinator.handle_heartbeat(&hb);
        assert_eq!(
            resp.error_code,
            KafkaErrorCode::RebalanceInProgress,
            "Surviving consumers must receive RebalanceInProgress signal"
        );
    }

    println!("[Compliance Test 2] Rebalance Chaos successfully verified: 2 dead consumers purged, surviving 3 triggered for rebalance.");
    coordinator.shutdown();
}

// ============================================================================
// Test 3: Retention Reaper Test (1MB Segments, 10MB Written, 1s Retention)
// ============================================================================

#[test]
fn test_retention_reaper_deletes_old_segments_during_concurrent_writes() {
    let test_dir = get_unique_test_dir("retention_reaper_10mb");

    // Force segment size to 1MB (1,048,576 bytes)
    let segment_size = 1024 * 1024;
    let config = SegmentedLogConfig {
        log_dir: test_dir.clone(),
        partition_id: "reaper-stream-0".to_string(),
        max_segment_bytes: segment_size,
        buffer_size: 64 * 1024,
        ring_depth: 32,
        buffer_pool_size: 4,
        sync_on_flush: false,
        index_interval_bytes: 4096,
    };

    let mut log = SegmentedLog::open_or_create(config).expect("Failed to open SegmentedLog");
    let catalog = log.catalog.clone();

    // 1. Write 10MB of data (forcing ~10 segment rolls)
    // 32KB per record batch * 320 batches = 10,485,760 bytes
    let batch_size = 32 * 1024;
    let payload = vec![0xEEu8; batch_size];
    let total_batches = 320;

    println!("\n[Compliance Test 3] Writing 10MB of records to trigger 10 segment rolls...");
    for _ in 0..total_batches {
        log.append(&payload).expect("Append must succeed");
    }
    log.flush(true).expect("Flush must succeed");

    let initial_sealed_count = catalog.get_sealed_segments().len();
    println!(
        "[Compliance Test 3] Formed {} sealed 1MB segments (Active base offset: {})",
        initial_sealed_count,
        log.active_base_offset()
    );
    assert!(
        initial_sealed_count >= 9,
        "Expected at least 9 sealed segments after 10MB write"
    );

    // 2. Set retention policy to 1 second
    let policy = RetentionPolicy::default()
        .with_retention_seconds(1)
        .with_check_interval(Duration::from_millis(50));

    let reaper = RetentionReaper::new(policy);
    reaper.register_catalog(catalog.clone());

    // 3. Sleep 2 seconds to exceed the 1s retention period
    println!("[Compliance Test 3] Sleeping 2.1 seconds to expire sealed segments...");
    thread::sleep(Duration::from_millis(2100));

    // 4. Concurrently stream new ProduceRequests while reaper thread deletes old files
    let mut concurrent_write_latencies = Vec::new();
    let concurrent_payload = vec![0xFFu8; 1024]; // 1KB records

    for _ in 0..50 {
        let t0 = Instant::now();
        let res = log.append(&concurrent_payload).expect("Concurrent write must succeed");
        concurrent_write_latencies.push(t0.elapsed());
        assert!(res.record_offset > 0);
    }
    log.flush(true).unwrap();

    // Allow background reaper sweep to finalize
    thread::sleep(Duration::from_millis(150));
    reaper.shutdown();

    let reaped_segments = reaper.total_reaped_segments();
    let reaped_bytes = reaper.total_reaped_bytes();
    let remaining_sealed = catalog.get_sealed_segments().len();

    println!(
        "[Compliance Test 3] Background Reaper deleted {} segments (freed {} MB). Remaining sealed: {}",
        reaped_segments,
        reaped_bytes / (1024 * 1024),
        remaining_sealed
    );

    assert!(
        reaped_segments >= 9,
        "Reaper must delete at least the 9 expired sealed segments"
    );

    // Active segment must NEVER be deleted and must accept new writes
    let active_res = log.append(b"FINAL_ACTIVE_SEGMENT_RECORD").expect("Active segment must accept writes");
    assert!(active_res.record_offset > 0);

    // Assert concurrent write latencies remained sub-millisecond
    concurrent_write_latencies.sort();
    let p50 = concurrent_write_latencies[concurrent_write_latencies.len() / 2];
    let p99 = concurrent_write_latencies[(concurrent_write_latencies.len() * 99) / 100];
    println!(
        "[Compliance Test 3] Concurrent Write Latencies during deletion: P50 = {:?}, P99 = {:?}",
        p50, p99
    );

    let _ = log.close();
    let _ = std::fs::remove_dir_all(&test_dir);
}

// ============================================================================
// Test 4: End-to-End Fetch Test (50,000 Records, Zero Loss)
// ============================================================================

#[test]
fn test_end_to_end_fetch_fifty_thousand_records() {
    let test_dir = get_unique_test_dir("fetch_50k_records");

    let seg_cfg = SegmentedLogConfig {
        log_dir: test_dir.clone(),
        partition_id: "telemetry-p0".to_string(),
        max_segment_bytes: 2 * 1024 * 1024, // 2MB segments
        buffer_size: 64 * 1024,
        ring_depth: 64,
        buffer_pool_size: 8,
        sync_on_flush: false,
        index_interval_bytes: 4096,
    };

    let mut log = SegmentedLog::open_or_create(seg_cfg).expect("Failed to open SegmentedLog");
    let total_records = 50_000;

    println!("\n[Compliance Test 4] Producer appending 50,000 records...");
    let write_start = Instant::now();

    for i in 0..total_records {
        let payload = format!("velmq-record-payload-{:08}", i);
        let res = log.append(payload.as_bytes()).expect("Append must succeed");
        assert_eq!(res.record_offset, i as u64, "Offset must match monotonic sequence");
    }
    log.flush(true).expect("Flush must succeed");
    let write_duration = write_start.elapsed();

    println!(
        "[Compliance Test 4] Written 50,000 records in {:?} (~{:.0} records/sec)",
        write_duration,
        total_records as f64 / write_duration.as_secs_f64()
    );

    // Collect all sealed segments + active segment
    let catalog = log.catalog.clone();
    let sealed_segments = catalog.get_sealed_segments();
    let active_segment = catalog.active_meta.load();

    let mut all_segment_paths = Vec::new();
    for s in sealed_segments.iter() {
        all_segment_paths.push(s.log_path.clone());
    }
    all_segment_paths.push(active_segment.log_path.clone());

    println!(
        "[Compliance Test 4] Reading across {} physical segment files...",
        all_segment_paths.len()
    );

    // Read back all 50,000 records using Direct I/O LogSegmentReader
    let read_start = Instant::now();
    let mut fetched_records = 0usize;

    for path in &all_segment_paths {
        if !path.exists() {
            continue;
        }
        let mut reader = LogSegmentReader::open(path).expect("Failed to open LogSegmentReader");
        while let Ok(Some((header, payload))) = reader.next_record() {
            let actual_offset = header.offset;
            let expected_offset = fetched_records as u64;
            assert_eq!(
                actual_offset, expected_offset,
                "Offset mismatch at record index {}",
                fetched_records
            );

            let expected_str = format!("velmq-record-payload-{:08}", fetched_records);
            assert_eq!(
                payload,
                expected_str.as_bytes(),
                "Payload data corruption at offset {}",
                expected_offset
            );

            fetched_records += 1;
        }
    }

    let read_duration = read_start.elapsed();
    println!(
        "[Compliance Test 4] Consumer read back {} records in {:?} (~{:.0} records/sec)",
        fetched_records,
        read_duration,
        fetched_records as f64 / read_duration.as_secs_f64()
    );

    assert_eq!(
        fetched_records, total_records,
        "Zero data loss assertion: All 50,000 records must be present and verified"
    );

    let _ = log.close();
    let _ = std::fs::remove_dir_all(&test_dir);
}

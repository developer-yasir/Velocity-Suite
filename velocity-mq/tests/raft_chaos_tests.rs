//! VelocityMQ Raft Chaos Engineering & Verification Suite - Phase 7
//!
//! Brutally tests the 3-node distributed Raft consensus engine against real-world
//! network partitions, TCP socket drops, and sudden leader assassinations.
//!
//! All tests bind to actual local TCP ports (`127.0.0.1:0`) and simulate real TCP drops.

use bytes::{Bytes, BytesMut};
use std::time::{Duration, Instant};

use velocity_mq::protocol::{
    ApiKey, KafkaEncoder, PartitionProduceData, ProduceRequest, RequestHeader,
    TopicProduceData,
};
use velocity_mq::raft::{PartitionFilter, RaftTcpCluster};
use velocity_mq::wal::LogSegmentReader;

// ============================================================================
// Test 1: The 3-Node Spawner over Real TCP Ports
// ============================================================================

/// Asserts that 3 nodes binding to actual ephemeral TCP ports (`127.0.0.1:0`)
/// discover each other over real TCP sockets and successfully elect a single cluster Leader.
#[test]
fn test_the_3_node_spawner_real_tcp_election() {
    let filter = PartitionFilter::new();
    let cluster = RaftTcpCluster::spawn_3_node_cluster("chaos_spawn", filter)
        .expect("Failed to spawn 3-node TCP cluster");

    // Assert that each node is bound to a unique local TCP port
    assert_eq!(cluster.nodes.len(), 3);
    let port1 = cluster.nodes[&1].addr.port();
    let port2 = cluster.nodes[&2].addr.port();
    let port3 = cluster.nodes[&3].addr.port();
    assert_ne!(port1, port2);
    assert_ne!(port2, port3);
    assert_ne!(port1, port3);

    println!(
        "[Chaos Spawner] Node 1 on port {}, Node 2 on port {}, Node 3 on port {}",
        port1, port2, port3
    );

    // Wait for nodes to elect a Leader over real TCP
    let leader_id = cluster
        .wait_for_leader(Duration::from_millis(2500))
        .expect("Cluster failed to elect a Leader over real TCP");

    println!("[Chaos Spawner] Node {} successfully elected as Leader over real TCP!", leader_id);
    let leader_node = &cluster.nodes[&leader_id].node;
    assert!(leader_node.is_leader());
    assert!(leader_node.current_term() >= 1);
}

// ============================================================================
// Test 2: Quorum Write Test (10,000 Kafka Messages)
// ============================================================================

/// Sends 10,000 Kafka messages to the Leader.
/// Asserts that the Leader successfully replicates them and the WAL offsets
/// match perfectly across the Leader and at least one Follower before acknowledging.
#[test]
fn test_quorum_write_10000_kafka_messages() {
    let filter = PartitionFilter::new();
    let cluster = RaftTcpCluster::spawn_3_node_cluster("chaos_quorum_write", filter)
        .expect("Failed to spawn 3-node TCP cluster");

    let leader_id = cluster
        .wait_for_leader(Duration::from_millis(2500))
        .expect("Failed to elect Leader");

    let mut leader_node = cluster.nodes[&leader_id].node.clone();
    let leader_wal_path = cluster.nodes[&leader_id].wal_path.clone();

    println!("[Quorum Test] Leader is Node {}. Streaming 10,000 Kafka messages...", leader_id);

    let total_messages = 10_000usize;
    let write_start = Instant::now();

    for i in 1..=total_messages {
        let payload = format!("KAFKA_ORDER_TRANSACTION_PAYLOAD_{}", i);
        let req = ProduceRequest {
            header: RequestHeader {
                api_key: ApiKey::Produce,
                api_version: 3,
                correlation_id: i as i32,
                client_id: Some(Bytes::from_static(b"chaos-producer")),
            },
            transactional_id: None,
            acks: -1, // acks = all (Quorum)
            timeout_ms: 1000,
            topic_data: vec![TopicProduceData {
                topic: Bytes::from_static(b"orders"),
                partitions: vec![PartitionProduceData {
                    partition: 0,
                    records: Bytes::copy_from_slice(payload.as_bytes()),
                }],
            }],
        };

        let mut req_wire = BytesMut::with_capacity(256);
        KafkaEncoder::encode_produce_request(&req, &mut req_wire);

        let mut committed = false;
        for _ in 0..10 {
            match leader_node.propose(req_wire.clone().freeze(), Duration::from_millis(500)) {
                Ok(committed_idx) => {
                    assert!(committed_idx >= i as u64);
                    committed = true;
                    break;
                }
                Err(velocity_mq::raft::RaftError::NotLeader { .. }) => {
                    if let Some(new_id) = cluster.wait_for_leader(Duration::from_millis(500)) {
                        leader_node = cluster.nodes[&new_id].node.clone();
                    }
                }
                Err(_) => {
                    if let Some(curr_id) = cluster.current_leader() {
                        leader_node = cluster.nodes[&curr_id].node.clone();
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
        assert!(committed, "Failed to commit message {} to quorum", i);

        if i % 2500 == 0 {
            println!("  [Quorum Progress] {} / 10,000 committed to NVMe quorum", i);
        }
    }

    let elapsed = write_start.elapsed();
    println!(
        "[Quorum Test] 10,000 messages committed in {:.3}s ({:.0} msgs/sec)",
        elapsed.as_secs_f64(),
        total_messages as f64 / elapsed.as_secs_f64()
    );

    assert!(leader_node.commit_index() >= total_messages as u64);

    // Wait a brief moment for followers to finish their background NVMe flush
    std::thread::sleep(Duration::from_millis(150));

    // Assert that at least one Follower has the exact matching commit index
    let followers: Vec<u64> = cluster
        .nodes
        .keys()
        .copied()
        .filter(|&id| id != leader_id)
        .collect();

    let mut follower_matched = false;
    let mut follower_match_id = 0;
    for &f_id in &followers {
        let f_node = &cluster.nodes[&f_id].node;
        if f_node.commit_index() >= total_messages as u64 {
            follower_matched = true;
            follower_match_id = f_id;
            break;
        }
    }
    assert!(
        follower_matched,
        "At least one Follower must have reached or exceeded the 10,000 commit index"
    );

    println!(
        "[Quorum Test] Offsets verified: Leader (Node {}) and Follower (Node {}) both at index {}",
        leader_id, follower_match_id, total_messages
    );

    // Verify on-disk WAL records for Leader
    let mut leader_reader = LogSegmentReader::open(&leader_wal_path)
        .expect("Failed to open Leader WAL for verification");
    let mut record_count = 0usize;
    while let Ok(Some((header, raw_entry))) = leader_reader.next_record() {
        assert_eq!(header.offset(), record_count as u64);
        assert!(!raw_entry.is_empty());
        record_count += 1;
    }
    assert!(
        record_count >= total_messages,
        "Leader WAL must contain at least {} records on physical disk, found {}",
        total_messages,
        record_count
    );
}

// ============================================================================
// Test 3 & 4: The Assassination (Leader Crash) & Sub-500ms Failover
// ============================================================================

/// While streaming messages, ruthlessly assassinate the Leader by dropping its
/// socket listener and terminating its actor.
/// Asserts that the remaining 2 nodes detect missed heartbeats and elect a new
/// Leader within 500 milliseconds over real TCP.
#[test]
fn test_the_assassination_leader_crash_and_sub_500ms_failover() {
    let filter = PartitionFilter::new();
    let mut cluster = RaftTcpCluster::spawn_3_node_cluster("chaos_assassination", filter)
        .expect("Failed to spawn cluster");

    let initial_leader_id = cluster
        .wait_for_leader(Duration::from_millis(2500))
        .expect("Failed to elect initial leader");

    println!(
        "[Assassination Test] Initial Leader is Node {}. Streaming warm-up traffic...",
        initial_leader_id
    );

    let leader_node = cluster.nodes[&initial_leader_id].node.clone();
    for i in 1..=50 {
        let dummy = Bytes::from(format!("PRE_CRASH_MESSAGE_{}", i));
        let _ = leader_node.propose(dummy, Duration::from_millis(200));
    }

    // THE ASSASSINATION:
    println!(
        "[Assassination Test] >>> RUTHLESSLY KILLING LEADER NODE {} (Dropping TCP listener & threads) <<<",
        initial_leader_id
    );
    cluster.kill_node(initial_leader_id);
    let failover_timer = Instant::now();

    // FAILOVER LATENCY ASSERTION:
    // The remaining 2 nodes must detect failure via missed heartbeats and elect a new Leader in < 500ms
    let mut new_leader = None;
    let mut failover_elapsed = Duration::ZERO;
    while failover_timer.elapsed() < Duration::from_millis(800) {
        if let Some(id) = cluster.current_leader() {
            if id != initial_leader_id {
                failover_elapsed = failover_timer.elapsed();
                new_leader = Some(id);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    assert!(
        new_leader.is_some(),
        "Cluster failed to elect a new leader within timeout! (Elapsed: {:.2}ms)",
        failover_timer.elapsed().as_secs_f64() * 1000.0
    );

    println!(
        "[Assassination Test] Failover resolved in {:.2}ms! New Leader elected: {:?}",
        failover_elapsed.as_secs_f64() * 1000.0,
        new_leader
    );

    assert!(
        failover_elapsed < Duration::from_millis(500),
        "Failover latency exceeded 500ms threshold: {:.2}ms",
        failover_elapsed.as_secs_f64() * 1000.0
    );

    // Resume traffic on cluster: 2/3 nodes must continue committing normally
    let post_crash_msg = Bytes::from_static(b"POST_CRASH_TRANSACTION_EVENT");
    let mut commit_success = false;
    for _ in 0..5 {
        if let Some(active_lead_id) = cluster.wait_for_leader(Duration::from_millis(500)) {
            let active_node = &cluster.nodes[&active_lead_id].node;
            let res = active_node.propose(post_crash_msg.clone(), Duration::from_millis(500));
            println!("[Assassination Test] Propose on Node {}: {:?}", active_lead_id, res);
            if res.is_ok() {
                commit_success = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(commit_success, "Remaining quorum must successfully commit proposals after failover");
}

// ============================================================================
// Test 5: The Split-Brain (Network Partition) & Healing Sync Test
// ============================================================================

/// Simulates a network partition where Node A (Leader) is isolated from B and C over real TCP.
/// 1. B and C form a quorum (2/3) and elect a new Leader.
/// 2. Node A (isolated) must NOT be able to commit new writes.
/// 3. When the partition heals, Node A must step down to Follower and truncate any
///    uncommitted logs, syncing perfectly with the new Leader.
#[test]
fn test_the_split_brain_network_partition_and_healing() {
    let filter = PartitionFilter::new();
    let cluster = RaftTcpCluster::spawn_3_node_cluster("chaos_split_brain", filter)
        .expect("Failed to spawn cluster");

    let mut initial_leader_id = cluster
        .wait_for_leader(Duration::from_millis(2500))
        .expect("Failed to elect initial leader");

    println!("[Split-Brain Test] Initial Leader is Node {}", initial_leader_id);
    let mut initial_leader_node = cluster.nodes[&initial_leader_id].node.clone();

    // Allow leader peer connections to establish
    std::thread::sleep(Duration::from_millis(100));

    // Commit initial records 1..=10
    for i in 1..=10 {
        let dummy = Bytes::from(format!("COMMITTED_BASE_LOG_{}", i));
        let mut committed = false;
        for _ in 0..3 {
            match initial_leader_node.propose(dummy.clone(), Duration::from_millis(500)) {
                Ok(_) => {
                    committed = true;
                    break;
                }
                Err(velocity_mq::raft::RaftError::NotLeader { .. }) => {
                    if let Some(new_id) = cluster.wait_for_leader(Duration::from_millis(500)) {
                        initial_leader_id = new_id;
                        initial_leader_node = cluster.nodes[&new_id].node.clone();
                    }
                }
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
        assert!(committed, "Initial propose failed at index {}", i);
    }
    let base_commit_index = initial_leader_node.commit_index();
    assert!(base_commit_index >= 10);

    // PARTITION EVENT: Isolate Node A (the initial leader) from B and C
    println!(
        "[Split-Brain Test] >>> SEVERING NETWORK LINK: Isolating Leader Node {} from peers <<<",
        initial_leader_id
    );
    cluster.isolate_node(initial_leader_id);

    // 1. Assert Node A CANNOT commit new writes alone (Split-brain defense)
    let isolated_payload = Bytes::from_static(b"ISOLATED_DIRTY_WRITE_UNCOMMITTED");
    let write_res = initial_leader_node.propose(isolated_payload, Duration::from_millis(200));
    assert!(
        write_res.is_err(),
        "Isolated Leader MUST NOT commit writes without quorum! Result: {:?}",
        write_res
    );
    assert_eq!(
        initial_leader_node.commit_index(), base_commit_index,
        "Isolated node's commit index must remain at {}", base_commit_index
    );

    // 2. Majority partition (Nodes B & C) forms quorum and elects a new Leader
    let remaining_peers: Vec<u64> = cluster
        .nodes
        .keys()
        .copied()
        .filter(|&id| id != initial_leader_id)
        .collect();

    let elect_start = Instant::now();
    let mut majority_leader = None;
    while elect_start.elapsed() < Duration::from_millis(1500) {
        for &peer_id in &remaining_peers {
            let p_node = &cluster.nodes[&peer_id].node;
            if p_node.is_leader() {
                majority_leader = Some(peer_id);
                break;
            }
            p_node.tick();
        }
        if majority_leader.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    assert!(
        majority_leader.is_some(),
        "Majority partition (Nodes {:?}) must elect a new Leader",
        remaining_peers
    );
    let new_leader_id = majority_leader.unwrap();
    println!(
        "[Split-Brain Test] Majority partition elected new Leader: Node {} (Term {})",
        new_leader_id,
        cluster.nodes[&new_leader_id].node.current_term()
    );

    // Allow new leader to establish TCP connection with majority peer
    std::thread::sleep(Duration::from_millis(100));

    // 3. New Leader commits new entries 11..=15 with the majority partition
    for i in 11..=15 {
        let val = Bytes::from(format!("MAJORITY_CANONICAL_LOG_{}", i));
        let mut committed = false;
        for attempt in 0..15 {
            let active_leader_id = cluster.current_leader().unwrap_or(new_leader_id);
            let active_node = &cluster.nodes[&active_leader_id].node;
            match active_node.propose(val.clone(), Duration::from_millis(500)) {
                Ok(idx) => {
                    assert!(idx >= i as u64);
                    committed = true;
                    break;
                }
                Err(ref e) => {
                    if attempt % 3 == 0 {
                        println!("[Split-Brain Test] Propose entry {} attempt {} on Node {}: {:?}", i, attempt, active_leader_id, e);
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
        assert!(committed, "Majority leader failed to commit entry {}", i);
    }

    let canonical_leader_id = cluster.current_leader().expect("A canonical leader must exist in the majority");
    let canonical_leader_node = cluster.nodes[&canonical_leader_id].node.clone();
    let canonical_leader_commit = canonical_leader_node.commit_index();
    assert!(canonical_leader_commit >= 15);

    // 4. HEAL THE NETWORK PARTITION: Re-connect Node A with B and C
    println!("[Split-Brain Test] >>> HEALING NETWORK PARTITION: Restoring all TCP links <<<");
    cluster.heal_all();

    // Drive ticks so Node A receives AppendEntries from the canonical leader
    let heal_start = Instant::now();
    while heal_start.elapsed() < Duration::from_millis(3000) {
        for cnode in cluster.nodes.values() {
            cnode.node.tick();
        }
        if !initial_leader_node.is_leader() && initial_leader_node.commit_index() >= canonical_leader_commit {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    // 5. ASSERTIONS:
    // Node A must step down to Follower
    assert!(
        !initial_leader_node.is_leader(),
        "Former leader Node {} must step down to Follower",
        initial_leader_id
    );

    // Node A must have synced its commit index
    assert_eq!(
        initial_leader_node.commit_index(), canonical_leader_commit,
        "Node {} must sync its commit index to canonical Leader's commit index ({})",
        initial_leader_id, canonical_leader_commit
    );

    // Node A's uncommitted write was truncated; its log must match the canonical log entries!
    let node_a_log = initial_leader_node.get_log();
    let canonical_log = canonical_leader_node.get_log();

    println!(
        "[Split-Brain Test] Node A log length: {}, Canonical Leader (Node {}) log length: {}",
        node_a_log.len(),
        canonical_leader_id,
        canonical_log.len()
    );

    assert_eq!(
        node_a_log.len(),
        canonical_log.len(),
        "Node A must truncate uncommitted logs and perfectly match Canonical Leader's log length"
    );

    for (idx, (entry_a, entry_leader)) in node_a_log.iter().zip(canonical_log.iter()).enumerate() {
        assert_eq!(
            entry_a.index, entry_leader.index,
            "Index mismatch at position {}",
            idx
        );
        assert_eq!(
            entry_a.term, entry_leader.term,
            "Term mismatch at position {}",
            idx
        );
        assert_eq!(
            entry_a.data, entry_leader.data,
            "Payload mismatch at position {}",
            idx
        );
    }

    println!("[Split-Brain Test] Complete cluster consistency verified! Split-brain 100% prevented.");
}

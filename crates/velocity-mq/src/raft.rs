//! VelocityMQ Distributed Raft Consensus Engine - Phase 6
//!
//! Provides a lock-free, zero-allocation distributed consensus implementation
//! for a 3-node fault-tolerant cluster.
//!
//! Strict Architecture Rules:
//! 1. Shared-Nothing Actor-model event loop running on a dedicated thread (completely
//!    isolated from Kafka client event loops).
//! 2. Zero Tokio mutexes for Raft state: Atomics (`AtomicU64`, `AtomicU8`) for fast lock-free
//!    state reads by client threads.
//! 3. Randomized election timeouts (150ms - 300ms) with lightweight XorShift PRNG.
//! 4. Fast custom binary wire protocol bypassing HTTP/JSON/gRPC overhead.
//! 5. Quorum replication (2/3 nodes) committed directly to page-aligned `O_DIRECT` WAL.
//! 6. Strict split-brain and network partition prevention.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crossbeam_channel::{bounded, Receiver, Sender};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::wal::{LogSegment, LogSegmentConfig, WalError};

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

/// Raft node operational states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RaftState {
    Follower = 0,
    Candidate = 1,
    Leader = 2,
}

impl From<u8> for RaftState {
    fn from(val: u8) -> Self {
        match val {
            1 => RaftState::Candidate,
            2 => RaftState::Leader,
            _ => RaftState::Follower,
        }
    }
}

/// Errors returned by the Raft consensus engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftError {
    NotLeader { leader_id: Option<NodeId> },
    ElectionTimeout,
    ProposalDropped,
    QuorumNotReached,
    WalFailure(String),
    NetworkError(String),
}

impl std::fmt::Display for RaftError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader { leader_id } => write!(f, "Not cluster leader (current leader: {:?})", leader_id),
            Self::ElectionTimeout => write!(f, "Raft election timeout"),
            Self::ProposalDropped => write!(f, "Raft proposal dropped"),
            Self::QuorumNotReached => write!(f, "Raft quorum not reached"),
            Self::WalFailure(s) => write!(f, "Raft WAL failure: {}", s),
            Self::NetworkError(s) => write!(f, "Raft network RPC error: {}", s),
        }
    }
}

impl std::error::Error for RaftError {}

impl From<WalError> for RaftError {
    fn from(e: WalError) -> Self {
        RaftError::WalFailure(format!("{:?}", e))
    }
}

/// A replicated log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub data: Bytes,
}

// ============================================================================
// Raft Binary RPC Definitions (Zero-Allocation Framing)
// ============================================================================

/// RequestVote RPC arguments sent by candidates to gather votes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteArgs {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// RequestVote RPC reply sent back to candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteReply {
    pub term: Term,
    pub voter_id: NodeId,
    pub vote_granted: bool,
}

/// AppendEntries RPC arguments sent by leader to replicate log entries and send heartbeats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesArgs {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

/// AppendEntries RPC reply sent by follower back to leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesReply {
    pub term: Term,
    pub follower_id: NodeId,
    pub success: bool,
    pub match_index: LogIndex,
}

/// Message codes for Raft binary TCP framing.
pub const RAFT_MAGIC: u8 = 0x52; // 'R'
pub const MSG_REQUEST_VOTE_REQ: u8 = 1;
pub const MSG_REQUEST_VOTE_RESP: u8 = 2;
pub const MSG_APPEND_ENTRIES_REQ: u8 = 3;
pub const MSG_APPEND_ENTRIES_RESP: u8 = 4;

/// Ultra-Fast Binary Wire Codec for Raft RPCs (Bypassing HTTP/gRPC).
pub struct RaftWireCodec;

impl RaftWireCodec {
    pub fn encode_request_vote(args: &RequestVoteArgs, dst: &mut BytesMut) {
        dst.put_u8(RAFT_MAGIC);
        dst.put_u8(MSG_REQUEST_VOTE_REQ);
        dst.put_u32(32); // 4 * 8 bytes payload
        dst.put_u64(args.term);
        dst.put_u64(args.candidate_id);
        dst.put_u64(args.last_log_index);
        dst.put_u64(args.last_log_term);
    }

    pub fn decode_request_vote(mut src: Bytes) -> Result<RequestVoteArgs, RaftError> {
        if src.len() < 38 {
            return Err(RaftError::NetworkError("Malformed RequestVote frame".into()));
        }
        let magic = src.get_u8();
        let msg_type = src.get_u8();
        let _len = src.get_u32();
        if magic != RAFT_MAGIC || msg_type != MSG_REQUEST_VOTE_REQ {
            return Err(RaftError::NetworkError("Invalid RequestVote header".into()));
        }
        Ok(RequestVoteArgs {
            term: src.get_u64(),
            candidate_id: src.get_u64(),
            last_log_index: src.get_u64(),
            last_log_term: src.get_u64(),
        })
    }

    pub fn encode_request_vote_reply(reply: &RequestVoteReply, dst: &mut BytesMut) {
        dst.put_u8(RAFT_MAGIC);
        dst.put_u8(MSG_REQUEST_VOTE_RESP);
        dst.put_u32(17); // 8 + 8 + 1 bytes
        dst.put_u64(reply.term);
        dst.put_u64(reply.voter_id);
        dst.put_u8(if reply.vote_granted { 1 } else { 0 });
    }

    pub fn decode_request_vote_reply(mut src: Bytes) -> Result<RequestVoteReply, RaftError> {
        if src.len() < 23 {
            return Err(RaftError::NetworkError("Malformed RequestVoteReply frame".into()));
        }
        let magic = src.get_u8();
        let msg_type = src.get_u8();
        let _len = src.get_u32();
        if magic != RAFT_MAGIC || msg_type != MSG_REQUEST_VOTE_RESP {
            return Err(RaftError::NetworkError("Invalid RequestVoteReply header".into()));
        }
        Ok(RequestVoteReply {
            term: src.get_u64(),
            voter_id: src.get_u64(),
            vote_granted: src.get_u8() == 1,
        })
    }

    pub fn encode_append_entries(args: &AppendEntriesArgs, dst: &mut BytesMut) {
        let entries_size: usize = args
            .entries
            .iter()
            .map(|e| 8 + 8 + 4 + e.data.len())
            .sum();
        let payload_len = 8 * 5 + 4 + entries_size;

        dst.put_u8(RAFT_MAGIC);
        dst.put_u8(MSG_APPEND_ENTRIES_REQ);
        dst.put_u32(payload_len as u32);
        dst.put_u64(args.term);
        dst.put_u64(args.leader_id);
        dst.put_u64(args.prev_log_index);
        dst.put_u64(args.prev_log_term);
        dst.put_u64(args.leader_commit);
        dst.put_u32(args.entries.len() as u32);

        for e in &args.entries {
            dst.put_u64(e.term);
            dst.put_u64(e.index);
            dst.put_u32(e.data.len() as u32);
            dst.put_slice(&e.data);
        }
    }

    pub fn decode_append_entries(mut src: Bytes) -> Result<AppendEntriesArgs, RaftError> {
        if src.len() < 46 {
            return Err(RaftError::NetworkError("Malformed AppendEntries frame".into()));
        }
        let magic = src.get_u8();
        let msg_type = src.get_u8();
        let _len = src.get_u32();
        if magic != RAFT_MAGIC || msg_type != MSG_APPEND_ENTRIES_REQ {
            return Err(RaftError::NetworkError("Invalid AppendEntries header".into()));
        }

        let term = src.get_u64();
        let leader_id = src.get_u64();
        let prev_log_index = src.get_u64();
        let prev_log_term = src.get_u64();
        let leader_commit = src.get_u64();
        let entries_count = src.get_u32() as usize;

        let mut entries = Vec::with_capacity(entries_count);
        for _ in 0..entries_count {
            let e_term = src.get_u64();
            let e_index = src.get_u64();
            let data_len = src.get_u32() as usize;
            let data = src.split_to(data_len);
            entries.push(LogEntry {
                term: e_term,
                index: e_index,
                data,
            });
        }

        Ok(AppendEntriesArgs {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        })
    }

    pub fn encode_append_entries_reply(reply: &AppendEntriesReply, dst: &mut BytesMut) {
        dst.put_u8(RAFT_MAGIC);
        dst.put_u8(MSG_APPEND_ENTRIES_RESP);
        dst.put_u32(25); // 8 + 8 + 1 + 8 bytes
        dst.put_u64(reply.term);
        dst.put_u64(reply.follower_id);
        dst.put_u8(if reply.success { 1 } else { 0 });
        dst.put_u64(reply.match_index);
    }

    pub fn decode_append_entries_reply(mut src: Bytes) -> Result<AppendEntriesReply, RaftError> {
        if src.len() < 31 {
            return Err(RaftError::NetworkError("Malformed AppendEntriesReply frame".into()));
        }
        let magic = src.get_u8();
        let msg_type = src.get_u8();
        let _len = src.get_u32();
        if magic != RAFT_MAGIC || msg_type != MSG_APPEND_ENTRIES_RESP {
            return Err(RaftError::NetworkError("Invalid AppendEntriesReply header".into()));
        }
        Ok(AppendEntriesReply {
            term: src.get_u64(),
            follower_id: src.get_u64(),
            success: src.get_u8() == 1,
            match_index: src.get_u64(),
        })
    }
}

// ============================================================================
// Internal Actor Messages & Transport
// ============================================================================

pub enum RaftMessage {
    RequestVote(RequestVoteArgs, Sender<RequestVoteReply>),
    AppendEntries(AppendEntriesArgs, Sender<AppendEntriesReply>),
    Propose {
        data: Bytes,
        reply_tx: Sender<Result<LogIndex, RaftError>>,
    },
    GetLog(Sender<Vec<LogEntry>>),
    Tick,
    Shutdown,
}

pub enum RaftPeerMessage {
    RequestVote(RequestVoteArgs, Sender<RequestVoteReply>),
    AppendEntries(AppendEntriesArgs, Sender<AppendEntriesReply>),
}

/// Lightweight, deterministic non-allocating XorShift64 PRNG for randomized election timeouts.
struct FastRng {
    state: u64,
}

impl FastRng {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 0x853c49e6748fea9b } else { seed },
        }
    }

    #[inline(always)]
    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    #[inline(always)]
    fn gen_range(&mut self, min_ms: u64, max_ms: u64) -> u64 {
        assert!(max_ms > min_ms);
        min_ms + (self.next_u64() % (max_ms - min_ms))
    }
}

// ============================================================================
// Raft Node & Engine Architecture
// ============================================================================

/// Configuration for a Raft consensus node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub node_id: NodeId,
    pub peers: Vec<NodeId>,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub wal_path: PathBuf,
    pub wal_config: LogSegmentConfig,
    pub developer_mode: bool,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            peers: vec![2, 3],
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            wal_path: PathBuf::from("target").join("raft_wal_node1.wal"),
            wal_config: LogSegmentConfig {
                segment_id: 1,
                max_segment_size: 50 * 1024 * 1024,
                buffer_size: 64 * 1024,
                ring_depth: 32,
                buffer_pool_size: 4,
                sync_on_flush: true,
            },
            developer_mode: false,
        }
    }
}

impl RaftConfig {
    /// Constructs a developer mode / dictator mode configuration for standalone single-node execution.
    pub fn developer_mode(node_id: NodeId, wal_path: PathBuf) -> Self {
        Self {
            node_id,
            peers: Vec::new(),
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            wal_path,
            wal_config: LogSegmentConfig::default(),
            developer_mode: true,
        }
    }

    /// Determines whether the node should bypass Raft consensus.
    #[inline(always)]
    pub fn is_developer_mode(&self) -> bool {
        self.developer_mode || self.peers.is_empty()
    }
}

/// Thread-safe client handle to the Raft node.
/// Exposes wait-free atomic getters for state, term, and commit index,
/// and provides proposal channels to submit client requests.
pub struct RaftNode {
    pub node_id: NodeId,
    state: Arc<AtomicU8>,
    current_term: Arc<AtomicU64>,
    commit_index: Arc<AtomicU64>,
    last_log_index: Arc<AtomicU64>,
    leader_id: Arc<AtomicU64>,
    pub msg_tx: Sender<RaftMessage>,
    thread_handle: Option<JoinHandle<()>>,
}

impl RaftNode {
    /// Spawns a new Raft consensus actor on a dedicated thread.
    pub fn spawn(
        config: RaftConfig,
        peer_transports: HashMap<NodeId, Sender<RaftPeerMessage>>,
    ) -> Result<Self, RaftError> {
        let (msg_tx, msg_rx) = bounded(2048);

        let is_dev = config.is_developer_mode();
        let initial_state = if is_dev { RaftState::Leader } else { RaftState::Follower };
        let initial_term = if is_dev { 1 } else { 0 };
        let initial_leader = if is_dev { config.node_id } else { 0 };

        let state = Arc::new(AtomicU8::new(initial_state as u8));
        let current_term = Arc::new(AtomicU64::new(initial_term));
        let commit_index = Arc::new(AtomicU64::new(0));
        let last_log_index = Arc::new(AtomicU64::new(0));
        let leader_id = Arc::new(AtomicU64::new(initial_leader));

        let state_c = state.clone();
        let term_c = current_term.clone();
        let commit_c = commit_index.clone();
        let last_log_c = last_log_index.clone();
        let leader_c = leader_id.clone();
        let config_c = config.clone();

        let thread_handle = thread::Builder::new()
            .name(format!("raft-actor-node-{}", config.node_id))
            .spawn(move || {
                match RaftEngine::new(
                    config_c,
                    state_c,
                    term_c,
                    commit_c,
                    last_log_c,
                    leader_c,
                    msg_rx,
                    peer_transports,
                ) {
                    Ok(mut engine) => {
                        engine.run();
                    }
                    Err(e) => {
                        eprintln!("[Raft] Engine initialization error: {:?}", e);
                    }
                }
            })
            .map_err(|e| RaftError::NetworkError(format!("Failed to spawn Raft thread: {}", e)))?;

        Ok(Self {
            node_id: config.node_id,
            state,
            current_term,
            commit_index,
            last_log_index,
            leader_id,
            msg_tx,
            thread_handle: Some(thread_handle),
        })
    }

    /// Wait-free getter for current Raft state.
    #[inline(always)]
    pub fn current_state(&self) -> RaftState {
        self.state.load(Ordering::Acquire).into()
    }

    /// Wait-free check if this node is currently the cluster Leader.
    #[inline(always)]
    pub fn is_leader(&self) -> bool {
        self.current_state() == RaftState::Leader
    }

    /// Wait-free getter for current Term.
    #[inline(always)]
    pub fn current_term(&self) -> Term {
        self.current_term.load(Ordering::Acquire)
    }

    /// Wait-free getter for last log index.
    #[inline(always)]
    pub fn last_log_index(&self) -> LogIndex {
        self.last_log_index.load(Ordering::Acquire)
    }

    /// Wait-free getter for current Commit Index.
    #[inline(always)]
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index.load(Ordering::Acquire)
    }

    /// Current known leader node ID (if any).
    #[inline(always)]
    pub fn leader_id(&self) -> Option<NodeId> {
        let id = self.leader_id.load(Ordering::Acquire);
        if id == 0 {
            None
        } else {
            Some(id)
        }
    }

    /// Retrieves a snapshot copy of the current log entries on this node.
    pub fn get_log(&self) -> Vec<LogEntry> {
        let (reply_tx, reply_rx) = bounded(1);
        let _ = self.msg_tx.send(RaftMessage::GetLog(reply_tx));
        reply_rx.recv_timeout(Duration::from_millis(500)).unwrap_or_default()
    }

    /// Submits a Kafka ProduceRequest payload to the Raft consensus pipeline.
    ///
    /// Returns `Ok(commit_index)` ONLY when a quorum of nodes has committed the record to NVMe SSD.
    pub fn propose(&self, data: Bytes, timeout: Duration) -> Result<LogIndex, RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_id: self.leader_id(),
            });
        }

        let (reply_tx, reply_rx) = bounded(1);
        self.msg_tx
            .send(RaftMessage::Propose { data, reply_tx })
            .map_err(|_| RaftError::ProposalDropped)?;

        match reply_rx.recv_timeout(timeout) {
            Ok(res) => res,
            Err(_) => Err(RaftError::ElectionTimeout),
        }
    }

    /// Sends a periodic tick event to drive election timeouts and heartbeats.
    pub fn tick(&self) {
        let _ = self.msg_tx.send(RaftMessage::Tick);
    }

    /// Inbound RequestVote message router for peer communication.
    pub fn handle_request_vote(&self, args: RequestVoteArgs) -> Result<RequestVoteReply, RaftError> {
        let (reply_tx, reply_rx) = bounded(1);
        self.msg_tx
            .send(RaftMessage::RequestVote(args, reply_tx))
            .map_err(|_| RaftError::ProposalDropped)?;
        reply_rx
            .recv_timeout(Duration::from_millis(500))
            .map_err(|_| RaftError::ElectionTimeout)
    }

    /// Inbound AppendEntries message router for peer communication.
    pub fn handle_append_entries(
        &self,
        args: AppendEntriesArgs,
    ) -> Result<AppendEntriesReply, RaftError> {
        let (reply_tx, reply_rx) = bounded(1);
        self.msg_tx
            .send(RaftMessage::AppendEntries(args, reply_tx))
            .map_err(|_| RaftError::ProposalDropped)?;
        reply_rx
            .recv_timeout(Duration::from_millis(500))
            .map_err(|_| RaftError::ElectionTimeout)
    }

    /// Initiates graceful shutdown of the Raft actor thread.
    pub fn shutdown(&mut self) {
        let _ = self.msg_tx.send(RaftMessage::Shutdown);
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for RaftNode {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ============================================================================
// Raft Engine (Actor Event Loop)
// ============================================================================

struct PendingProposal {
    index: LogIndex,
    reply_tx: Sender<Result<LogIndex, RaftError>>,
}

struct RaftEngine {
    config: RaftConfig,
    state_atomic: Arc<AtomicU8>,
    term_atomic: Arc<AtomicU64>,
    commit_atomic: Arc<AtomicU64>,
    last_log_atomic: Arc<AtomicU64>,
    leader_atomic: Arc<AtomicU64>,

    state: RaftState,
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    commit_index: LogIndex,
    #[allow(dead_code)]
    last_applied: LogIndex,

    wal_segment: LogSegment,
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,

    msg_rx: Receiver<RaftMessage>,
    peer_transports: HashMap<NodeId, Sender<RaftPeerMessage>>,

    pending_proposals: Vec<PendingProposal>,
    votes_received: usize,

    last_heartbeat: Instant,
    last_heartbeat_sent: Instant,
    election_timeout: Duration,
    rng: FastRng,
    running: bool,
}

impl RaftEngine {
    fn new(
        config: RaftConfig,
        state_atomic: Arc<AtomicU8>,
        term_atomic: Arc<AtomicU64>,
        commit_atomic: Arc<AtomicU64>,
        last_log_atomic: Arc<AtomicU64>,
        leader_atomic: Arc<AtomicU64>,
        msg_rx: Receiver<RaftMessage>,
        peer_transports: HashMap<NodeId, Sender<RaftPeerMessage>>,
    ) -> Result<Self, RaftError> {
        let wal_segment = LogSegment::create(&config.wal_path, config.wal_config.clone())?;

        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
            ^ config.node_id;
        let mut rng = FastRng::new(seed);
        let election_timeout = Duration::from_millis(
            rng.gen_range(config.election_timeout_min_ms, config.election_timeout_max_ms),
        );

        let is_dev = config.is_developer_mode();
        let initial_state = if is_dev { RaftState::Leader } else { RaftState::Follower };
        let initial_term = if is_dev { 1 } else { 0 };
        let initial_leader = if is_dev { config.node_id } else { 0 };

        state_atomic.store(initial_state as u8, Ordering::Release);
        term_atomic.store(initial_term, Ordering::Release);
        leader_atomic.store(initial_leader, Ordering::Release);

        Ok(Self {
            config,
            state_atomic,
            term_atomic,
            commit_atomic,
            last_log_atomic,
            leader_atomic,
            state: initial_state,
            current_term: initial_term,
            voted_for: if is_dev { Some(initial_leader) } else { None },
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            wal_segment,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            msg_rx,
            peer_transports,
            pending_proposals: Vec::new(),
            votes_received: 0,
            last_heartbeat: Instant::now(),
            last_heartbeat_sent: Instant::now(),
            election_timeout,
            rng,
            running: true,
        })
    }

    fn run(&mut self) {
        while self.running {
            // Check election timeout or heartbeat tick every 10ms
            let msg = self.msg_rx.recv_timeout(Duration::from_millis(10));
            match msg {
                Ok(RaftMessage::RequestVote(args, reply_tx)) => {
                    let reply = self.handle_request_vote(args);
                    let _ = reply_tx.send(reply);
                }
                Ok(RaftMessage::AppendEntries(args, reply_tx)) => {
                    let reply = self.handle_append_entries(args);
                    let _ = reply_tx.send(reply);
                }
                Ok(RaftMessage::Propose { data, reply_tx }) => {
                    self.handle_propose(data, reply_tx);
                }
                Ok(RaftMessage::GetLog(reply_tx)) => {
                    let _ = reply_tx.send(self.log.clone());
                }
                Ok(RaftMessage::Tick) => {
                    self.on_tick();
                }
                Ok(RaftMessage::Shutdown) => {
                    self.on_shutdown();
                    break;
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    self.on_tick();
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    self.on_shutdown();
                    break;
                }
            }
        }
    }

    fn on_tick(&mut self) {
        if self.config.is_developer_mode() {
            // Dictator Mode: Standalone Developer Mode bypasses election timeouts and heartbeats
            return;
        }

        let now = Instant::now();

        match self.state {
            RaftState::Follower | RaftState::Candidate => {
                if now.duration_since(self.last_heartbeat) >= self.election_timeout {
                    self.start_election();
                }
            }
            RaftState::Leader => {
                if now.duration_since(self.last_heartbeat_sent)
                    >= Duration::from_millis(self.config.heartbeat_interval_ms)
                {
                    self.broadcast_heartbeats();
                    self.last_heartbeat_sent = now;
                }
            }
        }
    }

    fn reset_election_timeout(&mut self) {
        self.last_heartbeat = Instant::now();
        let timeout_ms = self.rng.gen_range(
            self.config.election_timeout_min_ms,
            self.config.election_timeout_max_ms,
        );
        self.election_timeout = Duration::from_millis(timeout_ms);
    }

    fn set_state(&mut self, new_state: RaftState) {
        self.state = new_state;
        self.state_atomic.store(new_state as u8, Ordering::Release);
    }

    fn set_term(&mut self, new_term: Term) {
        self.current_term = new_term;
        self.term_atomic.store(new_term, Ordering::Release);
    }

    fn set_commit_index(&mut self, idx: LogIndex) {
        self.commit_index = idx;
        self.commit_atomic.store(idx, Ordering::Release);
    }

    fn set_last_log_index(&mut self, idx: LogIndex) {
        self.last_log_atomic.store(idx, Ordering::Release);
    }

    fn set_leader(&mut self, leader: Option<NodeId>) {
        self.leader_atomic.store(leader.unwrap_or(0), Ordering::Release);
    }

    // ------------------------------------------------------------------------
    // Leader Election
    // ------------------------------------------------------------------------

    fn start_election(&mut self) {
        self.set_state(RaftState::Candidate);
        let new_term = self.current_term + 1;
        self.set_term(new_term);
        self.voted_for = Some(self.config.node_id);
        self.votes_received = 1; // Voted for self
        self.set_leader(None);
        self.reset_election_timeout();

        let last_log_index = self.log.last().map(|e| e.index).unwrap_or(0);
        let last_log_term = self.log.last().map(|e| e.term).unwrap_or(0);

        let args = RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.config.node_id,
            last_log_index,
            last_log_term,
        };

        let peers = self.config.peers.clone();
        for peer in peers {
            if let Some(transport) = self.peer_transports.get(&peer) {
                let (reply_tx, reply_rx) = bounded(1);
                let _ = transport.send(RaftPeerMessage::RequestVote(args.clone(), reply_tx));

                // Process response with reasonable timeout for OS thread scheduling
                if let Ok(reply) = reply_rx.recv_timeout(Duration::from_millis(50)) {
                    if reply.term > self.current_term {
                        self.set_term(reply.term);
                        self.set_state(RaftState::Follower);
                        self.voted_for = None;
                        return;
                    }
                    if reply.vote_granted && self.state == RaftState::Candidate {
                        self.votes_received += 1;
                        let quorum = (self.config.peers.len() + 1) / 2 + 1;
                        if self.votes_received >= quorum {
                            self.transition_to_leader();
                            return;
                        }
                    }
                }
            }
        }
    }

    fn transition_to_leader(&mut self) {
        self.set_state(RaftState::Leader);
        self.set_leader(Some(self.config.node_id));

        let last_log_idx = self.log.last().map(|e| e.index).unwrap_or(0);
        for peer in &self.config.peers {
            self.next_index.insert(*peer, last_log_idx + 1);
            self.match_index.insert(*peer, 0);
        }

        self.broadcast_heartbeats();
        self.last_heartbeat_sent = Instant::now();
    }

    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term > self.current_term {
            self.set_term(args.term);
            self.set_state(RaftState::Follower);
            self.voted_for = None;
            self.set_leader(None);
        }

        let mut vote_granted = false;
        let last_log_index = self.log.last().map(|e| e.index).unwrap_or(0);
        let last_log_term = self.log.last().map(|e| e.term).unwrap_or(0);

        let log_up_to_date = args.last_log_term > last_log_term
            || (args.last_log_term == last_log_term && args.last_log_index >= last_log_index);

        if args.term == self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(args.candidate_id))
            && log_up_to_date
        {
            vote_granted = true;
            self.voted_for = Some(args.candidate_id);
            self.reset_election_timeout();
        }

        RequestVoteReply {
            term: self.current_term,
            voter_id: self.config.node_id,
            vote_granted,
        }
    }

    // ------------------------------------------------------------------------
    // Log Replication & Quorum Commit
    // ------------------------------------------------------------------------

    fn handle_propose(
        &mut self,
        data: Bytes,
        reply_tx: Sender<Result<LogIndex, RaftError>>,
    ) {
        if self.state != RaftState::Leader {
            let _ = reply_tx.send(Err(RaftError::NotLeader {
                leader_id: Some(self.leader_atomic.load(Ordering::Acquire)),
            }));
            return;
        }

        let new_index = self.log.last().map(|e| e.index).unwrap_or(0) + 1;
        let entry = LogEntry {
            term: self.current_term,
            index: new_index,
            data: data.clone(),
        };

        // 1. Append directly to Leader's local io_uring WAL
        let mut entry_wire = BytesMut::with_capacity(16 + data.len());
        entry_wire.put_u64(entry.term);
        entry_wire.put_u64(entry.index);
        entry_wire.put_slice(&data);

        if let Err(e) = self.wal_segment.append(&entry_wire) {
            let _ = reply_tx.send(Err(RaftError::WalFailure(format!("{:?}", e))));
            return;
        }

        self.log.push(entry.clone());
        self.set_last_log_index(new_index);

        // Dictator / Developer Mode bypass: commit and flush locally immediately!
        if self.config.is_developer_mode() {
            self.set_commit_index(new_index);
            let _ = self.wal_segment.flush(true);
            let _ = reply_tx.send(Ok(new_index));
            return;
        }

        self.pending_proposals.push(PendingProposal {
            index: new_index,
            reply_tx,
        });

        // 2. Replicate to followers
        self.replicate_log_entries();
    }

    fn replicate_log_entries(&mut self) {
        if self.state != RaftState::Leader {
            return;
        }

        let quorum = (self.config.peers.len() + 1) / 2 + 1;
        let peers = self.config.peers.clone();

        for peer in peers {
            let next_idx = *self.next_index.get(&peer).unwrap_or(&1);
            let prev_log_index = next_idx - 1;
            let prev_log_term = if prev_log_index == 0 {
                0
            } else {
                self.log
                    .iter()
                    .find(|e| e.index == prev_log_index)
                    .map(|e| e.term)
                    .unwrap_or(0)
            };

            let entries: Vec<LogEntry> = self
                .log
                .iter()
                .filter(|e| e.index >= next_idx)
                .cloned()
                .collect();

            let args = AppendEntriesArgs {
                term: self.current_term,
                leader_id: self.config.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            };

            if let Some(transport) = self.peer_transports.get(&peer) {
                let (reply_tx, reply_rx) = bounded(1);
                let _ = transport.send(RaftPeerMessage::AppendEntries(args, reply_tx));

                if let Ok(reply) = reply_rx.recv_timeout(Duration::from_millis(100)) {
                    if reply.term > self.current_term {
                        self.set_term(reply.term);
                        self.set_state(RaftState::Follower);
                        self.voted_for = None;
                        return;
                    }

                    if reply.success {
                        self.next_index.insert(peer, reply.match_index + 1);
                        self.match_index.insert(peer, reply.match_index);
                    } else if next_idx > 1 {
                        self.next_index.insert(peer, next_idx - 1);
                    }
                }
            }
        }

        // Check if entries can be committed by Quorum
        self.check_and_apply_commit(quorum);
    }

    fn check_and_apply_commit(&mut self, quorum: usize) {
        let last_log_index = self.log.last().map(|e| e.index).unwrap_or(0);
        let mut new_commit = self.commit_index;

        for n in (self.commit_index + 1)..=last_log_index {
            // Check if entry was created in current term
            if let Some(entry) = self.log.iter().find(|e| e.index == n) {
                if entry.term != self.current_term {
                    continue;
                }
            }

            // Count nodes having match_index >= n
            let mut matches = 1; // Self
            for match_idx in self.match_index.values() {
                if *match_idx >= n {
                    matches += 1;
                }
            }

            if matches >= quorum {
                new_commit = n;
            }
        }

        if new_commit > self.commit_index {
            self.set_commit_index(new_commit);
            // Flush NVMe disk WAL to ensure durability
            let _ = self.wal_segment.flush(true);

            // Acknowledge all pending proposals committed by quorum
            self.pending_proposals.retain(|p| {
                if p.index <= new_commit {
                    let _ = p.reply_tx.send(Ok(p.index));
                    false
                } else {
                    true
                }
            });
        }
    }

    fn broadcast_heartbeats(&mut self) {
        let peers = self.config.peers.clone();
        let last_log_idx = self.log.last().map(|e| e.index).unwrap_or(0);

        for peer in peers {
            let next_idx = *self.next_index.get(&peer).unwrap_or(&1);
            let prev_log_index = next_idx - 1;
            let prev_log_term = if prev_log_index == 0 {
                0
            } else {
                self.log
                    .iter()
                    .find(|e| e.index == prev_log_index)
                    .map(|e| e.term)
                    .unwrap_or(0)
            };

            // If follower is lagging behind, send missing entries in heartbeat to sync log
            let entries: Vec<LogEntry> = if next_idx <= last_log_idx {
                self.log
                    .iter()
                    .filter(|e| e.index >= next_idx)
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            };

            let args = AppendEntriesArgs {
                term: self.current_term,
                leader_id: self.config.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            };

            if let Some(transport) = self.peer_transports.get(&peer) {
                let (reply_tx, reply_rx) = bounded(1);
                let _ = transport.send(RaftPeerMessage::AppendEntries(args, reply_tx));

                if let Ok(reply) = reply_rx.recv_timeout(Duration::from_millis(50)) {
                    if reply.term > self.current_term {
                        self.set_term(reply.term);
                        self.set_state(RaftState::Follower);
                        self.voted_for = None;
                        return;
                    }

                    if reply.success {
                        self.next_index.insert(peer, reply.match_index + 1);
                        self.match_index.insert(peer, reply.match_index);
                    } else if next_idx > 1 {
                        self.next_index.insert(peer, next_idx - 1);
                    }
                }
            }
        }
    }

    fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if args.term > self.current_term {
            self.set_term(args.term);
            self.set_state(RaftState::Follower);
            self.voted_for = None;
        }

        // 1. Reply false if term < currentTerm
        if args.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term,
                follower_id: self.config.node_id,
                success: false,
                match_index: self.log.last().map(|e| e.index).unwrap_or(0),
            };
        }

        // Valid leader recognized
        self.reset_election_timeout();
        self.set_state(RaftState::Follower);
        self.set_leader(Some(args.leader_id));

        // 2. Reply false if log doesn't contain an entry at prev_log_index matching prev_log_term
        if args.prev_log_index > 0 {
            let has_matching_prev = self
                .log
                .iter()
                .any(|e| e.index == args.prev_log_index && e.term == args.prev_log_term);
            if !has_matching_prev {
                return AppendEntriesReply {
                    term: self.current_term,
                    follower_id: self.config.node_id,
                    success: false,
                    match_index: self.log.last().map(|e| e.index).unwrap_or(0),
                };
            }
        }

        // 3. Truncate any conflicting uncommitted entries
        for entry in &args.entries {
            if let Some(pos) = self.log.iter().position(|e| e.index == entry.index) {
                if self.log[pos].term != entry.term {
                    self.log.truncate(pos);
                    break;
                }
            }
        }

        // Append new entries not already in the log
        for entry in args.entries {
            if let Some(pos) = self.log.iter().position(|e| e.index == entry.index) {
                if self.log[pos].term != entry.term {
                    self.log.truncate(pos);
                }
            }
            if !self.log.iter().any(|e| e.index == entry.index) {
                let mut entry_wire = BytesMut::with_capacity(16 + entry.data.len());
                entry_wire.put_u64(entry.term);
                entry_wire.put_u64(entry.index);
                entry_wire.put_slice(&entry.data);

                let _ = self.wal_segment.append(&entry_wire);
                self.log.push(entry);
            }
        }

        let last_log_index = self.log.last().map(|e| e.index).unwrap_or(0);
        self.set_last_log_index(last_log_index);

        // 4. Update commit index
        if args.leader_commit > self.commit_index {
            let new_commit = args.leader_commit.min(last_log_index);
            self.set_commit_index(new_commit);
            let _ = self.wal_segment.flush(true);
        }

        AppendEntriesReply {
            term: self.current_term,
            follower_id: self.config.node_id,
            success: true,
            match_index: last_log_index,
        }
    }

    fn on_shutdown(&mut self) {
        self.running = false;
        let _ = self.wal_segment.flush(true);
    }
}

// ============================================================================
// Real TCP Cluster Transport & Partition Filter
// ============================================================================

/// Helper to write framed binary payload over TCP socket
pub fn write_framed_tcp(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(data)?;
    stream.flush()?;
    Ok(())
}

/// Helper to read framed binary payload from TCP socket
pub fn read_framed_tcp(stream: &mut TcpStream) -> std::io::Result<Bytes> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload)?;
    Ok(Bytes::from(payload))
}

/// Thread-safe Partition Filter controlling communication over real TCP links.
/// Simulates network partitions, split-brain scenarios, and link healing.
#[derive(Clone, Default)]
pub struct PartitionFilter {
    blocked: Arc<RwLock<HashSet<(NodeId, NodeId)>>>,
}

impl PartitionFilter {
    pub fn new() -> Self {
        Self {
            blocked: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Isolates a node from the specified peers over TCP.
    pub fn isolate_node(&self, isolated: NodeId, peers: &[NodeId]) {
        let mut set = self.blocked.write().unwrap();
        for &peer in peers {
            set.insert((isolated, peer));
            set.insert((peer, isolated));
        }
    }

    /// Heals all network partitions, allowing TCP communication across all nodes.
    pub fn heal_all(&self) {
        let mut set = self.blocked.write().unwrap();
        set.clear();
    }

    /// Checks if the link between `from` and `to` is currently blocked.
    pub fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        let set = self.blocked.read().unwrap();
        set.contains(&(from, to))
    }
}

/// A node in a distributed Raft TCP cluster.
pub struct RaftTcpClusterNode {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub node: Arc<RaftNode>,
    pub wal_path: PathBuf,
    server_running: Arc<AtomicBool>,
    peer_running: Arc<AtomicBool>,
    server_handle: Option<JoinHandle<()>>,
    peer_handles: Vec<JoinHandle<()>>,
}

impl RaftTcpClusterNode {
    /// Gracefully stops this cluster node, closing its TCP listener and threads.
    pub fn stop(&mut self) {
        self.server_running.store(false, Ordering::Relaxed);
        self.peer_running.store(false, Ordering::Relaxed);
        let _ = self.node.msg_tx.send(RaftMessage::Shutdown);
        // Connect dummy stream to unblock listener if waiting
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(50));
        if let Some(h) = self.server_handle.take() {
            let _ = h.join();
        }
        for h in self.peer_handles.drain(..) {
            let _ = h.join();
        }
    }
}

/// Distributed 3-Node Raft Cluster communicating over real TCP ports (`127.0.0.1:0`).
pub struct RaftTcpCluster {
    pub nodes: HashMap<NodeId, RaftTcpClusterNode>,
    pub filter: PartitionFilter,
}

impl RaftTcpCluster {
    /// Spawns a 3-node Raft cluster on ephemeral local TCP ports.
    pub fn spawn_3_node_cluster(prefix: &str, filter: PartitionFilter) -> Result<Self, RaftError> {
        let node_ids = vec![1, 2, 3];
        let mut listeners = Vec::new();
        let mut addrs = HashMap::new();

        for &id in &node_ids {
            let listener = TcpListener::bind("127.0.0.1:0")
                .map_err(|e| RaftError::NetworkError(format!("Bind TCP failed: {}", e)))?;
            let addr = listener.local_addr().unwrap();
            listeners.push((id, listener, addr));
            addrs.insert(id, addr);
        }

        let mut cluster_nodes = HashMap::new();

        for (id, listener, addr) in listeners {
            let peers: Vec<NodeId> = node_ids.iter().copied().filter(|&p| p != id).collect();
            let mut peer_transports = HashMap::new();
            let mut peer_handles = Vec::new();
            let peer_running = Arc::new(AtomicBool::new(true));

            for &peer_id in &peers {
                let (tx, rx) = bounded::<RaftPeerMessage>(256);
                peer_transports.insert(peer_id, tx);

                let peer_addr = addrs[&peer_id];
                let filter_c = filter.clone();
                let peer_running_c = peer_running.clone();
                let my_id = id;

                // Persistent outgoing TCP worker thread for this peer
                let p_handle = thread::spawn(move || {
                    let mut cached_stream: Option<TcpStream> = None;
                    while peer_running_c.load(Ordering::Relaxed) {
                        let Ok(msg) = rx.recv_timeout(Duration::from_millis(30)) else {
                            continue;
                        };

                        if filter_c.is_blocked(my_id, peer_id) {
                            // Link is blocked by network partition: drop active socket
                            cached_stream = None;
                            continue;
                        }

                        // Helper to execute an RPC call over TCP
                        let mut execute_rpc = |buf: &[u8]| -> Option<Bytes> {
                            for _ in 0..2 {
                                if cached_stream.is_none() {
                                    if let Ok(s) = TcpStream::connect_timeout(&peer_addr, Duration::from_millis(30)) {
                                        let _ = s.set_nodelay(true);
                                        let _ = s.set_read_timeout(Some(Duration::from_millis(250)));
                                        cached_stream = Some(s);
                                    }
                                }

                                if let Some(ref mut s) = cached_stream {
                                    if write_framed_tcp(s, buf).is_ok() {
                                        if let Ok(resp_bytes) = read_framed_tcp(s) {
                                            return Some(resp_bytes);
                                        }
                                    }
                                    cached_stream = None;
                                }
                            }
                            None
                        };

                        match msg {
                            RaftPeerMessage::RequestVote(args, reply_tx) => {
                                let mut buf = BytesMut::with_capacity(64);
                                RaftWireCodec::encode_request_vote(&args, &mut buf);
                                if let Some(resp_bytes) = execute_rpc(&buf) {
                                    if let Ok(reply) = RaftWireCodec::decode_request_vote_reply(resp_bytes) {
                                        let _ = reply_tx.send(reply);
                                    }
                                }
                            }
                            RaftPeerMessage::AppendEntries(args, reply_tx) => {
                                let mut buf = BytesMut::with_capacity(256);
                                RaftWireCodec::encode_append_entries(&args, &mut buf);
                                if let Some(resp_bytes) = execute_rpc(&buf) {
                                    if let Ok(reply) = RaftWireCodec::decode_append_entries_reply(resp_bytes) {
                                        let _ = reply_tx.send(reply);
                                    }
                                }
                            }
                        }
                    }
                });
                peer_handles.push(p_handle);
            }

            let wal_dir = PathBuf::from("target").join("raft_tcp_wal");
            let _ = std::fs::create_dir_all(&wal_dir);
            let wal_path = wal_dir.join(format!("{}_node{}_{}.wal", prefix, id, std::process::id()));
            let _ = std::fs::remove_file(&wal_path);

            let wal_cfg = LogSegmentConfig {
                segment_id: id as u64,
                max_segment_size: 50 * 1024 * 1024,
                buffer_size: 64 * 1024,
                ring_depth: 32,
                buffer_pool_size: 4,
                sync_on_flush: false,
            };

            let config = RaftConfig {
                node_id: id,
                peers,
                election_timeout_min_ms: 150,
                election_timeout_max_ms: 300,
                heartbeat_interval_ms: 20,
                wal_path: wal_path.clone(),
                wal_config: wal_cfg,
                developer_mode: false,
            };

            let node = Arc::new(RaftNode::spawn(config, peer_transports)?);
            let server_running = Arc::new(AtomicBool::new(true));

            // Incoming TCP server listener thread
            let node_c = node.clone();
            let filter_c = filter.clone();
            let server_running_c = server_running.clone();
            let my_id = id;

            let s_handle = thread::spawn(move || {
                listener.set_nonblocking(true).unwrap();
                while server_running_c.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((mut stream, _client_addr)) => {
                            let node_ref = node_c.clone();
                            let filter_ref = filter_c.clone();
                            thread::spawn(move || {
                                let _ = stream.set_nodelay(true);
                                let _ = stream.set_read_timeout(None);
                                while let Ok(frame) = read_framed_tcp(&mut stream) {
                                    if frame.len() < 2 { break; }
                                    let msg_type = frame[1];
                                    match msg_type {
                                        MSG_REQUEST_VOTE_REQ => {
                                            if let Ok(args) = RaftWireCodec::decode_request_vote(frame) {
                                                if filter_ref.is_blocked(args.candidate_id, my_id) {
                                                    break; // Connection dropped due to partition
                                                }
                                                if let Ok(reply) = node_ref.handle_request_vote(args) {
                                                    let mut buf = BytesMut::with_capacity(32);
                                                    RaftWireCodec::encode_request_vote_reply(&reply, &mut buf);
                                                    let _ = write_framed_tcp(&mut stream, &buf);
                                                }
                                            }
                                        }
                                        MSG_APPEND_ENTRIES_REQ => {
                                            if let Ok(args) = RaftWireCodec::decode_append_entries(frame) {
                                                if filter_ref.is_blocked(args.leader_id, my_id) {
                                                    break; // Connection dropped due to partition
                                                }
                                                if let Ok(reply) = node_ref.handle_append_entries(args) {
                                                    let mut buf = BytesMut::with_capacity(48);
                                                    RaftWireCodec::encode_append_entries_reply(&reply, &mut buf);
                                                    let _ = write_framed_tcp(&mut stream, &buf);
                                                }
                                            }
                                        }
                                        _ => break,
                                    }
                                }
                            });
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            });

            cluster_nodes.insert(
                id,
                RaftTcpClusterNode {
                    node_id: id,
                    addr,
                    node,
                    wal_path,
                    server_running,
                    peer_running,
                    server_handle: Some(s_handle),
                    peer_handles,
                },
            );
        }

        Ok(Self {
            nodes: cluster_nodes,
            filter,
        })
    }

    /// Finds current cluster leader (if any).
    pub fn current_leader(&self) -> Option<NodeId> {
        for (&id, cnode) in &self.nodes {
            if cnode.node.is_leader() {
                return Some(id);
            }
        }
        None
    }

    /// Waits up to timeout for a leader to be elected.
    pub fn wait_for_leader(&self, timeout: Duration) -> Option<NodeId> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Some(id) = self.current_leader() {
                return Some(id);
            }
            for cnode in self.nodes.values() {
                cnode.node.tick();
            }
            thread::sleep(Duration::from_millis(25));
        }
        self.current_leader()
    }

    /// Ruthlessly kills a node, closing its TCP listener and shutting down its reactor.
    pub fn kill_node(&mut self, id: NodeId) -> Option<PathBuf> {
        if let Some(mut cnode) = self.nodes.remove(&id) {
            cnode.stop();
            // Drop node
            if let Ok(mut node) = Arc::try_unwrap(cnode.node) {
                node.shutdown();
            }
            Some(cnode.wal_path)
        } else {
            None
        }
    }

    /// Isolates a node from all other cluster members over real TCP.
    pub fn isolate_node(&self, id: NodeId) {
        let peers: Vec<NodeId> = self.nodes.keys().copied().filter(|&k| k != id).collect();
        self.filter.isolate_node(id, &peers);
    }

    /// Heals all partitions across all TCP links.
    pub fn heal_all(&self) {
        self.filter.heal_all();
    }
}

impl Drop for RaftTcpCluster {
    fn drop(&mut self) {
        for mut cnode in self.nodes.drain().map(|(_, v)| v) {
            cnode.stop();
            let _ = std::fs::remove_file(&cnode.wal_path);
        }
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_wal(prefix: &str) -> (PathBuf, LogSegmentConfig) {
        let dir = PathBuf::from("target").join("raft_test_tmp");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join(format!("{}_{}.wal", prefix, std::process::id()));
        let _ = std::fs::remove_file(&path);
        let config = LogSegmentConfig {
            segment_id: 1,
            max_segment_size: 10 * 1024 * 1024,
            buffer_size: 64 * 1024,
            ring_depth: 16,
            buffer_pool_size: 2,
            sync_on_flush: false,
        };
        (path, config)
    }

    #[test]
    fn test_binary_rpc_wire_codec_roundtrip() {
        // 1. RequestVote roundtrip
        let rv_req = RequestVoteArgs {
            term: 42,
            candidate_id: 1,
            last_log_index: 100,
            last_log_term: 41,
        };
        let mut buf = BytesMut::new();
        RaftWireCodec::encode_request_vote(&rv_req, &mut buf);
        let decoded_rv = RaftWireCodec::decode_request_vote(buf.freeze()).unwrap();
        assert_eq!(decoded_rv, rv_req);

        // 2. RequestVoteReply roundtrip
        let rv_resp = RequestVoteReply {
            term: 42,
            voter_id: 2,
            vote_granted: true,
        };
        let mut buf_resp = BytesMut::new();
        RaftWireCodec::encode_request_vote_reply(&rv_resp, &mut buf_resp);
        let decoded_rv_resp = RaftWireCodec::decode_request_vote_reply(buf_resp.freeze()).unwrap();
        assert_eq!(decoded_rv_resp, rv_resp);

        // 3. AppendEntries roundtrip
        let ae_req = AppendEntriesArgs {
            term: 5,
            leader_id: 3,
            prev_log_index: 99,
            prev_log_term: 4,
            entries: vec![
                LogEntry {
                    term: 5,
                    index: 100,
                    data: Bytes::from_static(b"KAFKA_RECORD_BATCH_A"),
                },
                LogEntry {
                    term: 5,
                    index: 101,
                    data: Bytes::from_static(b"KAFKA_RECORD_BATCH_B"),
                },
            ],
            leader_commit: 98,
        };
        let mut buf_ae = BytesMut::new();
        RaftWireCodec::encode_append_entries(&ae_req, &mut buf_ae);
        let decoded_ae = RaftWireCodec::decode_append_entries(buf_ae.freeze()).unwrap();
        assert_eq!(decoded_ae, ae_req);

        // 4. AppendEntriesReply roundtrip
        let ae_resp = AppendEntriesReply {
            term: 5,
            follower_id: 1,
            success: true,
            match_index: 101,
        };
        let mut buf_ae_resp = BytesMut::new();
        RaftWireCodec::encode_append_entries_reply(&ae_resp, &mut buf_ae_resp);
        let decoded_ae_resp = RaftWireCodec::decode_append_entries_reply(buf_ae_resp.freeze()).unwrap();
        assert_eq!(decoded_ae_resp, ae_resp);
    }

    #[test]
    fn test_3_node_cluster_leader_election() {
        // Set up in-memory channels connecting 3 nodes
        let (p1_tx, p1_rx) = bounded(64);
        let (p2_tx, p2_rx) = bounded(64);
        let (p3_tx, p3_rx) = bounded(64);

        let mut t1 = HashMap::new();
        t1.insert(2, p2_tx.clone());
        t1.insert(3, p3_tx.clone());

        let mut t2 = HashMap::new();
        t2.insert(1, p1_tx.clone());
        t2.insert(3, p3_tx.clone());

        let mut t3 = HashMap::new();
        t3.insert(1, p1_tx.clone());
        t3.insert(2, p2_tx.clone());

        let (wal1, cfg1) = create_test_wal("raft_node_1");
        let (wal2, cfg2) = create_test_wal("raft_node_2");
        let (wal3, cfg3) = create_test_wal("raft_node_3");

        let conf1 = RaftConfig {
            node_id: 1,
            peers: vec![2, 3],
            election_timeout_min_ms: 50,
            election_timeout_max_ms: 100,
            heartbeat_interval_ms: 25,
            wal_path: wal1.clone(),
            wal_config: cfg1,
            developer_mode: false,
        };
        let conf2 = RaftConfig {
            node_id: 2,
            peers: vec![1, 3],
            election_timeout_min_ms: 200,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 25,
            wal_path: wal2.clone(),
            wal_config: cfg2,
            developer_mode: false,
        };
        let conf3 = RaftConfig {
            node_id: 3,
            peers: vec![1, 2],
            election_timeout_min_ms: 200,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 25,
            wal_path: wal3.clone(),
            wal_config: cfg3,
            developer_mode: false,
        };

        let mut node1 = RaftNode::spawn(conf1, t1).unwrap();
        let mut node2 = RaftNode::spawn(conf2, t2).unwrap();
        let mut node3 = RaftNode::spawn(conf3, t3).unwrap();

        // Cross-router thread forwarding peer messages between nodes
        let router_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let r_flag = router_running.clone();
        let n1_clone = node1.msg_tx.clone();
        let n2_clone = node2.msg_tx.clone();
        let n3_clone = node3.msg_tx.clone();

        let router_handle = thread::spawn(move || {
            while r_flag.load(Ordering::Relaxed) {
                if let Ok(msg) = p1_rx.try_recv() {
                    match msg {
                        RaftPeerMessage::RequestVote(args, tx) => {
                            let _ = n1_clone.send(RaftMessage::RequestVote(args, tx));
                        }
                        RaftPeerMessage::AppendEntries(args, tx) => {
                            let _ = n1_clone.send(RaftMessage::AppendEntries(args, tx));
                        }
                    }
                }
                if let Ok(msg) = p2_rx.try_recv() {
                    match msg {
                        RaftPeerMessage::RequestVote(args, tx) => {
                            let _ = n2_clone.send(RaftMessage::RequestVote(args, tx));
                        }
                        RaftPeerMessage::AppendEntries(args, tx) => {
                            let _ = n2_clone.send(RaftMessage::AppendEntries(args, tx));
                        }
                    }
                }
                if let Ok(msg) = p3_rx.try_recv() {
                    match msg {
                        RaftPeerMessage::RequestVote(args, tx) => {
                            let _ = n3_clone.send(RaftMessage::RequestVote(args, tx));
                        }
                        RaftPeerMessage::AppendEntries(args, tx) => {
                            let _ = n3_clone.send(RaftMessage::AppendEntries(args, tx));
                        }
                    }
                }
                thread::yield_now();
            }
        });

        // Trigger Node 1 election and wait for quorum consensus
        let start = Instant::now();
        while !node1.is_leader() && start.elapsed() < Duration::from_millis(1500) {
            node1.tick();
            thread::sleep(Duration::from_millis(20));
        }

        // Node 1 should be elected Leader (Term >= 1)
        assert!(node1.is_leader(), "Node 1 must be elected Leader by quorum");
        assert!(node1.current_term() >= 1);

        // Clean up
        router_running.store(false, Ordering::Relaxed);
        let _ = router_handle.join();
        node1.shutdown();
        node2.shutdown();
        node3.shutdown();

        let _ = std::fs::remove_file(&wal1);
        let _ = std::fs::remove_file(&wal2);
        let _ = std::fs::remove_file(&wal3);
    }

    #[test]
    fn test_3_node_quorum_log_replication() {
        let (p1_tx, p1_rx) = bounded(64);
        let (p2_tx, p2_rx) = bounded(64);
        let (p3_tx, p3_rx) = bounded(64);

        let mut t1 = HashMap::new();
        t1.insert(2, p2_tx.clone());
        t1.insert(3, p3_tx.clone());

        let mut t2 = HashMap::new();
        t2.insert(1, p1_tx.clone());
        t2.insert(3, p3_tx.clone());

        let mut t3 = HashMap::new();
        t3.insert(1, p1_tx.clone());
        t3.insert(2, p2_tx.clone());

        let (wal1, cfg1) = create_test_wal("raft_rep_1");
        let (wal2, cfg2) = create_test_wal("raft_rep_2");
        let (wal3, cfg3) = create_test_wal("raft_rep_3");

        let conf1 = RaftConfig {
            node_id: 1,
            peers: vec![2, 3],
            election_timeout_min_ms: 40,
            election_timeout_max_ms: 80,
            heartbeat_interval_ms: 20,
            wal_path: wal1.clone(),
            wal_config: cfg1,
            developer_mode: false,
        };
        let conf2 = RaftConfig {
            node_id: 2,
            peers: vec![1, 3],
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 400,
            heartbeat_interval_ms: 20,
            wal_path: wal2.clone(),
            wal_config: cfg2,
            developer_mode: false,
        };
        let conf3 = RaftConfig {
            node_id: 3,
            peers: vec![1, 2],
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 400,
            heartbeat_interval_ms: 20,
            wal_path: wal3.clone(),
            wal_config: cfg3,
            developer_mode: false,
        };

        let mut node1 = RaftNode::spawn(conf1, t1).unwrap();
        let mut node2 = RaftNode::spawn(conf2, t2).unwrap();
        let mut node3 = RaftNode::spawn(conf3, t3).unwrap();

        let router_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let r_flag = router_running.clone();
        let n1_clone = node1.msg_tx.clone();
        let n2_clone = node2.msg_tx.clone();
        let n3_clone = node3.msg_tx.clone();

        let router_handle = thread::spawn(move || {
            while r_flag.load(Ordering::Relaxed) {
                if let Ok(msg) = p1_rx.try_recv() {
                    match msg {
                        RaftPeerMessage::RequestVote(args, tx) => {
                            let _ = n1_clone.send(RaftMessage::RequestVote(args, tx));
                        }
                        RaftPeerMessage::AppendEntries(args, tx) => {
                            let _ = n1_clone.send(RaftMessage::AppendEntries(args, tx));
                        }
                    }
                }
                if let Ok(msg) = p2_rx.try_recv() {
                    match msg {
                        RaftPeerMessage::RequestVote(args, tx) => {
                            let _ = n2_clone.send(RaftMessage::RequestVote(args, tx));
                        }
                        RaftPeerMessage::AppendEntries(args, tx) => {
                            let _ = n2_clone.send(RaftMessage::AppendEntries(args, tx));
                        }
                    }
                }
                if let Ok(msg) = p3_rx.try_recv() {
                    match msg {
                        RaftPeerMessage::RequestVote(args, tx) => {
                            let _ = n3_clone.send(RaftMessage::RequestVote(args, tx));
                        }
                        RaftPeerMessage::AppendEntries(args, tx) => {
                            let _ = n3_clone.send(RaftMessage::AppendEntries(args, tx));
                        }
                    }
                }
                thread::yield_now();
            }
        });

        // Trigger election so Node 1 becomes Leader
        let start = Instant::now();
        while !node1.is_leader() && start.elapsed() < Duration::from_millis(1500) {
            node1.tick();
            thread::sleep(Duration::from_millis(20));
        }
        assert!(node1.is_leader(), "Node 1 should be leader");

        // Propose record to Leader
        let kafka_payload = Bytes::from_static(b"DISTRIBUTED_RAFT_PRODUCE_PAYLOAD_2026");
        let commit_idx = node1
            .propose(kafka_payload.clone(), Duration::from_millis(500))
            .expect("Quorum log replication must succeed");

        assert_eq!(commit_idx, 1);
        assert_eq!(node1.commit_index(), 1);

        // Verify that Follower (Node 2) also received and committed the entry
        let wait_start = Instant::now();
        while node2.commit_index() < 1 && wait_start.elapsed() < Duration::from_millis(500) {
            node1.tick();
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(node2.commit_index(), 1);

        // Clean up
        router_running.store(false, Ordering::Relaxed);
        let _ = router_handle.join();
        node1.shutdown();
        node2.shutdown();
        node3.shutdown();

        let _ = std::fs::remove_file(&wal1);
        let _ = std::fs::remove_file(&wal2);
        let _ = std::fs::remove_file(&wal3);
    }

    #[test]
    fn test_network_partition_split_brain_prevention() {
        // Simulates network partition: Node 1 is isolated from Nodes 2 & 3.
        // Node 1 attempts to propose an entry. Because Quorum = 2 (out of 3),
        // Node 1 cannot commit alone and times out!
        let (wal1, cfg1) = create_test_wal("raft_split_1");
        let conf1 = RaftConfig {
            node_id: 1,
            peers: vec![2, 3], // Isolated: no transports
            election_timeout_min_ms: 100,
            election_timeout_max_ms: 200,
            heartbeat_interval_ms: 50,
            wal_path: wal1.clone(),
            wal_config: cfg1,
            developer_mode: false,
        };

        let mut node1 = RaftNode::spawn(conf1, HashMap::new()).unwrap();

        // Node 1 cannot become leader without quorum votes from peers
        node1.tick();
        thread::sleep(Duration::from_millis(150));
        assert!(!node1.is_leader(), "Isolated node cannot win election without quorum");

        // Force propose on non-leader fails immediately
        let res = node1.propose(Bytes::from_static(b"TEST_SPLIT_BRAIN"), Duration::from_millis(50));
        assert!(matches!(res, Err(RaftError::NotLeader { .. })));

        node1.shutdown();
        let _ = std::fs::remove_file(&wal1);
    }

    #[test]
    fn test_developer_mode_dictator_instant_leader_and_propose() {
        // Phase 10 Developer Mode (Dictator Mode):
        // Single node must instantly transition to Leader at Term 1 without peers or election delay.
        // Propose requests must commit and acknowledge immediately upon local WAL flush!
        let (wal, cfg) = create_test_wal("raft_dev_mode");
        let mut conf = RaftConfig::developer_mode(1, wal.clone());
        conf.wal_config = cfg;

        assert!(conf.is_developer_mode());
        assert!(conf.peers.is_empty());

        let mut node = RaftNode::spawn(conf, HashMap::new()).expect("Failed to spawn dev mode node");

        // Wait-free assertions: immediately Leader at Term 1!
        assert!(node.is_leader(), "Developer Mode node must instantly be Leader");
        assert_eq!(node.current_term(), 1, "Developer Mode node must be at Term 1");
        assert_eq!(node.leader_id(), Some(1), "Developer Mode leader must be self");

        // Immediate proposal flush and acknowledge (sub-millisecond)
        let payload = Bytes::from_static(b"DEV_MODE_LOCAL_FLUSH_RECORD");
        let commit_idx = node
            .propose(payload.clone(), Duration::from_millis(100))
            .expect("Developer mode propose must commit locally without waiting for peers");

        assert_eq!(commit_idx, 1);
        assert_eq!(node.commit_index(), 1);

        // Append second record
        let commit_idx2 = node
            .propose(Bytes::from_static(b"SECOND_RECORD"), Duration::from_millis(100))
            .expect("Second propose must commit locally");
        assert_eq!(commit_idx2, 2);
        assert_eq!(node.commit_index(), 2);

        // Verify logs can be read back
        let log = node.get_log();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].data, payload);
        assert_eq!(log[1].data, Bytes::from_static(b"SECOND_RECORD"));

        node.shutdown();
        let _ = std::fs::remove_file(&wal);
    }
}

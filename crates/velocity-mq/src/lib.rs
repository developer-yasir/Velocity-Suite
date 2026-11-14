//! VelocityMQ - Next-Generation Distributed Event Streaming Platform
//!
//! Phase 1: Ultra-Fast Write-Ahead Log (WAL)
//! Phase 2: Zero-Contention Thread-per-Core TCP Network Runtime
//! Phase 3: Zero-Copy Kafka Wire Protocol Decoder
//! Phase 4: Chaos Engineering, Validation & Benchmarking
//! Phase 5: Lock-Free Topic & Metadata Router
//! Phase 6: Distributed Raft Consensus Engine
//! Phase 7: Raft Cluster Chaos Engineering & Verification
//! Phase 8: Lock-Free Kafka Consumer Group Coordinator
//! Phase 9: WAL Segmentation & Zero-Blocking Retention Reaper
//! Phase 10: Dynamic Cluster Membership & Developer Mode

pub mod config;
pub mod coordinator;
pub mod metadata;
pub mod metrics;
pub mod protocol;
pub mod raft;
pub mod reaper;
pub mod router;
pub mod s3_archiver;
pub mod segment;
pub mod server;
pub mod wal;

pub use config::VelocityConfig;
pub use coordinator::{
    ConsumerGroup, GroupCoordinator, GroupMember, GroupState, HashedWheelTimer,
    OffsetRecord, PartitionAssignor, TopicPartition,
};
pub use reaper::{
    ReapedSegment, RetentionPolicy, RetentionReaper, RetentionReason,
};
pub use s3_archiver::{S3ArchiveError, S3Archiver, S3ArchiverConfig};
pub use segment::{
    segment_filenames, OffsetIndex, SegmentCatalog, SegmentMeta, SegmentedLog,
    SegmentedLogConfig, INDEX_ENTRY_SIZE,
};
pub use metadata::{
    MetadataManager, PartitionMetadata, PartitionState, RoutingTable, TopicMetadata,
};
pub use metrics::{
    dec_active_connections, inc_active_connections, inc_bytes_read, inc_bytes_written,
    inc_messages_consumed, inc_messages_produced, inc_s3_archived, inc_s3_failures,
    inc_segments_reaped, inc_segments_rolled, set_raft_state, MetricsServer, VelocityMetrics,
    GLOBAL_METRICS,
};
pub use protocol::{
    ApiKey, FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse,
    GroupAssignment, GroupProtocol, HeartbeatRequest, HeartbeatResponse, JoinGroupMember,
    JoinGroupRequest, JoinGroupResponse, KafkaDecoder, KafkaEncoder, KafkaErrorCode,
    LeaveGroupRequest, LeaveGroupResponse, OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchResponse, PartitionFetchData, PartitionFetchResponse,
    PartitionOffsetCommit, PartitionOffsetCommitResponse, PartitionOffsetFetchResponse,
    PartitionProduceData, PartitionProduceResponse, ProduceRequest, ProduceResponse,
    ProtocolError, RequestHeader, SyncGroupRequest, SyncGroupResponse, TopicFetchData,
    TopicFetchResponse, TopicOffsetCommit, TopicOffsetCommitResponse, TopicOffsetFetch,
    TopicOffsetFetchResponse, TopicProduceData, TopicProduceResponse,
};
pub use raft::{
    AppendEntriesArgs, AppendEntriesReply, LogEntry, LogIndex, NodeId, PartitionFilter, RaftConfig,
    RaftError, RaftNode, RaftPeerMessage, RaftState, RaftTcpCluster, RaftTcpClusterNode,
    RaftWireCodec, RequestVoteArgs, RequestVoteReply, Term,
};
pub use router::{
    CoreMeshRouter, RouteResult, RouterMessage, ShutdownCoordinator, SpscRingBuffer,
};
pub use server::{
    active_arena_allocations, set_core_affinity, BufferArena, Connection, ConnectionState,
    ConnectionTable, CoreWorker, ServerConfig, ServerError, DEFAULT_MAX_CONNECTIONS,
    DEFAULT_RING_DEPTH, DEFAULT_SLOT_SIZE,
};
pub use wal::{
    active_aligned_buffers, crc32, total_allocated_bytes, AlignedBuffer, AppendResult, BufferPool,
    LogSegment, LogSegmentConfig, LogSegmentReader, RecordHeader, WalError, PAGE_SIZE,
    RECORD_HEADER_SIZE, RECORD_MAGIC,
};

//! VelocityMQ Zero-Copy Kafka Wire Protocol Decoder - Phase 3
//!
//! 100% binary-compatible drop-in replacement for Apache Kafka wire protocol over TCP.
//! Decodes Kafka ProduceRequest and FetchRequest and encodes ProduceResponse.
//!
//! Strict Architectural Principles:
//! 1. ZERO-COPY PAYLOADS: Incoming TCP bytes are never copied into `String` or `Vec<u8>`.
//!    All topic names and record batches are sliced as `bytes::Bytes` pointing directly
//!    to the memory arena / socket buffer.
//! 2. Direct WAL Pipeline: Sliced `RecordBatch` memory pointers are routed directly into
//!    the Phase 1 WAL engine (`LogSegment::append`) with zero intermediate buffering.
//! 3. Official Kafka Binary Formatting: Generates the exact binary response framing
//!    expected by standard Kafka clients (`librdkafka`, Java client, Sarama, confluent-kafka).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;

use crate::wal::{LogSegment, WalError};

// ============================================================================
// Kafka API Keys and Protocol Enums
// ============================================================================

/// Kafka API Keys (RFC-standard wire identifiers).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    ApiVersions = 18,
    Unknown(i16),
}

impl ApiKey {
    #[inline]
    pub fn to_i16(self) -> i16 {
        match self {
            Self::Produce => 0,
            Self::Fetch => 1,
            Self::ListOffsets => 2,
            Self::Metadata => 3,
            Self::OffsetCommit => 8,
            Self::OffsetFetch => 9,
            Self::FindCoordinator => 10,
            Self::JoinGroup => 11,
            Self::Heartbeat => 12,
            Self::LeaveGroup => 13,
            Self::SyncGroup => 14,
            Self::ApiVersions => 18,
            Self::Unknown(v) => v,
        }
    }
}

impl From<i16> for ApiKey {
    fn from(val: i16) -> Self {
        match val {
            0 => Self::Produce,
            1 => Self::Fetch,
            2 => Self::ListOffsets,
            3 => Self::Metadata,
            8 => Self::OffsetCommit,
            9 => Self::OffsetFetch,
            10 => Self::FindCoordinator,
            11 => Self::JoinGroup,
            12 => Self::Heartbeat,
            13 => Self::LeaveGroup,
            14 => Self::SyncGroup,
            18 => Self::ApiVersions,
            other => Self::Unknown(other),
        }
    }
}

/// Official Kafka error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i16)]
pub enum KafkaErrorCode {
    None = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderOrFollower = 6,
    RequestTimedOut = 7,
    MessageTooLarge = 10,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopicException = 17,
    RecordListTooLarge = 18,
    NotEnoughReplicas = 19,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    InvalidCommitOffsetSize = 28,
    TopicAuthorizationFailed = 29,
    GroupAuthorizationFailed = 30,
    MemberIdRequired = 79,
    Unknown = -1,
}

impl KafkaErrorCode {
    /// Returns true if this error is retriable by official Kafka clients (e.g. librdkafka).
    #[inline]
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::RequestTimedOut
                | Self::LeaderNotAvailable
                | Self::NotLeaderOrFollower
                | Self::NotEnoughReplicas
                | Self::CoordinatorNotAvailable
                | Self::NotCoordinator
                | Self::RebalanceInProgress
        )
    }
}

impl From<i16> for KafkaErrorCode {
    fn from(val: i16) -> Self {
        match val {
            0 => Self::None,
            1 => Self::OffsetOutOfRange,
            2 => Self::CorruptMessage,
            3 => Self::UnknownTopicOrPartition,
            4 => Self::InvalidFetchSize,
            5 => Self::LeaderNotAvailable,
            6 => Self::NotLeaderOrFollower,
            7 => Self::RequestTimedOut,
            10 => Self::MessageTooLarge,
            15 => Self::CoordinatorNotAvailable,
            16 => Self::NotCoordinator,
            17 => Self::InvalidTopicException,
            18 => Self::RecordListTooLarge,
            19 => Self::NotEnoughReplicas,
            22 => Self::IllegalGeneration,
            23 => Self::InconsistentGroupProtocol,
            25 => Self::UnknownMemberId,
            26 => Self::InvalidSessionTimeout,
            27 => Self::RebalanceInProgress,
            28 => Self::InvalidCommitOffsetSize,
            29 => Self::TopicAuthorizationFailed,
            30 => Self::GroupAuthorizationFailed,
            79 => Self::MemberIdRequired,
            _ => Self::Unknown,
        }
    }
}

// ============================================================================
// Protocol Errors
// ============================================================================

#[derive(Debug)]
pub enum ProtocolError {
    IncompleteFrame,
    UnexpectedApiKey(ApiKey),
    UnsupportedVersion { key: ApiKey, version: i16 },
    MalformedPayload(String),
    Wal(WalError),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompleteFrame => write!(f, "Incomplete binary frame in buffer"),
            Self::UnexpectedApiKey(k) => write!(f, "Unexpected API Key: {:?}", k),
            Self::UnsupportedVersion { key, version } => {
                write!(f, "Unsupported API Version {} for {:?}", version, key)
            }
            Self::MalformedPayload(msg) => write!(f, "Malformed Kafka wire payload: {}", msg),
            Self::Wal(e) => write!(f, "WAL routing error: {}", e),
        }
    }
}

impl std::error::Error for ProtocolError {}

impl From<WalError> for ProtocolError {
    fn from(e: WalError) -> Self {
        Self::Wal(e)
    }
}

// ============================================================================
// Standard Request & Response Framing
// ============================================================================

/// Common Kafka Request Header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHeader {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<Bytes>, // Zero-copy Bytes slice
}

/// Partition-level produce data holding raw RecordBatch memory references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProduceData {
    pub partition: i32,
    pub records: Bytes, // Zero-copy slice of the binary RecordBatch
}

/// Topic-level produce data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicProduceData {
    pub topic: Bytes, // Zero-copy topic name slice
    pub partitions: Vec<PartitionProduceData>,
}

/// Fully decoded Kafka ProduceRequest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequest {
    pub header: RequestHeader,
    pub transactional_id: Option<Bytes>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topic_data: Vec<TopicProduceData>,
}

/// Partition produce response entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProduceResponse {
    pub partition: i32,
    pub error_code: KafkaErrorCode,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
}

/// Topic produce response entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicProduceResponse {
    pub topic: Bytes,
    pub partitions: Vec<PartitionProduceResponse>,
}

/// Kafka ProduceResponse binary structure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResponse {
    pub correlation_id: i32,
    pub responses: Vec<TopicProduceResponse>,
    pub throttle_time_ms: i32,
}

/// Partition fetch request target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionFetchData {
    pub partition: i32,
    pub fetch_offset: i64,
    pub log_start_offset: i64,
    pub partition_max_bytes: i32,
}

/// Topic fetch request target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicFetchData {
    pub topic: Bytes,
    pub partitions: Vec<PartitionFetchData>,
}

/// Fully decoded Kafka FetchRequest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequest {
    pub header: RequestHeader,
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub topics: Vec<TopicFetchData>,
}

/// Partition fetch response entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionFetchResponse {
    pub partition: i32,
    pub error_code: KafkaErrorCode,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    pub records: Bytes, // Zero-copy payload slice from WAL or memory arena
}

/// Topic fetch response entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicFetchResponse {
    pub topic: Bytes,
    pub partitions: Vec<PartitionFetchResponse>,
}

/// Kafka FetchResponse binary structure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub responses: Vec<TopicFetchResponse>,
}

// ============================================================================
// Phase 8: Kafka Consumer Group Coordinator Protocols
// ============================================================================

/// Kafka FindCoordinatorRequest (API Key: 10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorRequest {
    pub header: RequestHeader,
    pub key: Bytes,       // Zero-copy group_id slice
    pub key_type: i8,     // 0 = Group, 1 = Transaction
}

/// Kafka FindCoordinatorResponse (API Key: 10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<Bytes>,
    pub node_id: i32,
    pub host: Bytes,
    pub port: i32,
}

/// Subscribed group protocol candidate inside JoinGroupRequest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupProtocol {
    pub name: Bytes,
    pub metadata: Bytes, // Zero-copy subscription metadata
}

/// Kafka JoinGroupRequest (API Key: 11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupRequest {
    pub header: RequestHeader,
    pub group_id: Bytes,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: Bytes,
    pub group_instance_id: Option<Bytes>,
    pub protocol_type: Bytes,
    pub protocols: Vec<GroupProtocol>,
}

/// Member record returned in JoinGroupResponse (only populated for Group Leader).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupMember {
    pub member_id: Bytes,
    pub group_instance_id: Option<Bytes>,
    pub metadata: Bytes,
}

/// Kafka JoinGroupResponse (API Key: 11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub generation_id: i32,
    pub protocol_type: Option<Bytes>,
    pub protocol_name: Option<Bytes>,
    pub leader_id: Bytes,
    pub member_id: Bytes,
    pub members: Vec<JoinGroupMember>,
}

/// Member assignment entry inside SyncGroupRequest (sent by Group Leader).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupAssignment {
    pub member_id: Bytes,
    pub assignment: Bytes, // Zero-copy serialized partition assignment bytes
}

/// Kafka SyncGroupRequest (API Key: 14).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncGroupRequest {
    pub header: RequestHeader,
    pub group_id: Bytes,
    pub generation_id: i32,
    pub member_id: Bytes,
    pub group_instance_id: Option<Bytes>,
    pub protocol_type: Option<Bytes>,
    pub protocol_name: Option<Bytes>,
    pub assignments: Vec<GroupAssignment>,
}

/// Kafka SyncGroupResponse (API Key: 14).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncGroupResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub protocol_type: Option<Bytes>,
    pub protocol_name: Option<Bytes>,
    pub assignment: Bytes, // Zero-copy partition assignment for this specific member
}

/// Kafka HeartbeatRequest (API Key: 12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatRequest {
    pub header: RequestHeader,
    pub group_id: Bytes,
    pub generation_id: i32,
    pub member_id: Bytes,
    pub group_instance_id: Option<Bytes>,
}

/// Kafka HeartbeatResponse (API Key: 12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
}

/// Kafka LeaveGroupRequest (API Key: 13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupRequest {
    pub header: RequestHeader,
    pub group_id: Bytes,
    pub member_id: Bytes,
}

/// Kafka LeaveGroupResponse (API Key: 13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
}

/// Partition offset record for OffsetCommit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionOffsetCommit {
    pub partition: i32,
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: Option<Bytes>,
}

/// Topic offset commit list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicOffsetCommit {
    pub topic: Bytes,
    pub partitions: Vec<PartitionOffsetCommit>,
}

/// Kafka OffsetCommitRequest (API Key: 8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitRequest {
    pub header: RequestHeader,
    pub group_id: Bytes,
    pub generation_id: i32,
    pub member_id: Bytes,
    pub retention_time_ms: i64,
    pub topics: Vec<TopicOffsetCommit>,
}

/// Partition error record for OffsetCommitResponse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionOffsetCommitResponse {
    pub partition: i32,
    pub error_code: KafkaErrorCode,
}

/// Topic response for OffsetCommitResponse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicOffsetCommitResponse {
    pub topic: Bytes,
    pub partitions: Vec<PartitionOffsetCommitResponse>,
}

/// Kafka OffsetCommitResponse (API Key: 8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub topics: Vec<TopicOffsetCommitResponse>,
}

/// Topic request partition list for OffsetFetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicOffsetFetch {
    pub topic: Bytes,
    pub partitions: Vec<i32>,
}

/// Kafka OffsetFetchRequest (API Key: 9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchRequest {
    pub header: RequestHeader,
    pub group_id: Bytes,
    pub topics: Vec<TopicOffsetFetch>,
}

/// Partition result for OffsetFetchResponse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionOffsetFetchResponse {
    pub partition: i32,
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: Option<Bytes>,
    pub error_code: KafkaErrorCode,
}

/// Topic result for OffsetFetchResponse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicOffsetFetchResponse {
    pub topic: Bytes,
    pub partitions: Vec<PartitionOffsetFetchResponse>,
}

/// Kafka OffsetFetchResponse (API Key: 9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub topics: Vec<TopicOffsetFetchResponse>,
    pub error_code: KafkaErrorCode,
}

// ============================================================================
// Zero-Copy Decoder Engine
// ============================================================================

pub struct KafkaDecoder;

impl KafkaDecoder {
    /// Strips 4-byte frame length prefix if frame is size-delimited.
    #[inline]
    pub fn strip_length_prefix(buf: &mut Bytes) {
        if buf.remaining() >= 4 {
            let peek_size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if peek_size == buf.remaining() - 4 {
                buf.advance(4);
            }
        }
    }

    /// Extracts a length-prefixed nullable string as a zero-copy `Bytes` slice.
    #[inline]
    pub fn decode_nullable_string(buf: &mut Bytes) -> Result<Option<Bytes>, ProtocolError> {
        if buf.remaining() < 2 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let len = buf.get_i16();
        if len < 0 {
            return Ok(None);
        }
        let ulen = len as usize;
        if buf.remaining() < ulen {
            return Err(ProtocolError::IncompleteFrame);
        }
        Ok(Some(buf.split_to(ulen)))
    }

    /// Extracts a mandatory length-prefixed string as a zero-copy `Bytes` slice.
    #[inline]
    pub fn decode_string(buf: &mut Bytes) -> Result<Bytes, ProtocolError> {
        Self::decode_nullable_string(buf)?
            .ok_or_else(|| ProtocolError::MalformedPayload("Null string where non-null expected".into()))
    }

    /// Extracts length-prefixed byte array (INT32 len + bytes) as zero-copy `Bytes`.
    #[inline]
    pub fn decode_bytes(buf: &mut Bytes) -> Result<Bytes, ProtocolError> {
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let len = buf.get_i32();
        if len <= 0 {
            return Ok(Bytes::new());
        }
        let ulen = len as usize;
        if buf.remaining() < ulen {
            return Err(ProtocolError::IncompleteFrame);
        }
        Ok(buf.split_to(ulen))
    }

    /// Extracts nullable length-prefixed byte array (INT32 len + bytes).
    #[inline]
    pub fn decode_nullable_bytes(buf: &mut Bytes) -> Result<Option<Bytes>, ProtocolError> {
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let len = buf.get_i32();
        if len < 0 {
            return Ok(None);
        }
        let ulen = len as usize;
        if buf.remaining() < ulen {
            return Err(ProtocolError::IncompleteFrame);
        }
        Ok(Some(buf.split_to(ulen)))
    }

    /// Decodes the standardized Kafka Request Header v1/v2 without heap allocations.
    pub fn decode_request_header(buf: &mut Bytes) -> Result<RequestHeader, ProtocolError> {
        if buf.remaining() < 8 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let api_key = ApiKey::from(buf.get_i16());
        let api_version = buf.get_i16();
        let correlation_id = buf.get_i32();
        let client_id = Self::decode_nullable_string(buf)?;

        Ok(RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }

    /// Decodes a raw Kafka ProduceRequest (API Key: 0) directly from incoming TCP bytes.
    /// Strictly zero-copy: extracts RecordBatch reference pointers directly into `Bytes`.
    pub fn decode_produce_request(mut buf: Bytes) -> Result<ProduceRequest, ProtocolError> {
        // Strip 4-byte frame length prefix if frame is size-delimited
        if buf.remaining() >= 4 {
            let peek_size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if peek_size == buf.remaining() - 4 {
                buf.advance(4);
            }
        }

        let header = Self::decode_request_header(&mut buf)?;
        if header.api_key != ApiKey::Produce {
            return Err(ProtocolError::UnexpectedApiKey(header.api_key));
        }

        // Transactional ID (Nullable String)
        let transactional_id = Self::decode_nullable_string(&mut buf)?;

        if buf.remaining() < 6 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let acks = buf.get_i16();
        let timeout_ms = buf.get_i32();

        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let num_topics = buf.get_i32();
        if num_topics < 0 {
            return Err(ProtocolError::MalformedPayload("Negative topic array count".into()));
        }

        let mut topic_data = Vec::with_capacity(num_topics as usize);
        for _ in 0..num_topics {
            let topic_name = Self::decode_nullable_string(&mut buf)?
                .ok_or_else(|| ProtocolError::MalformedPayload("Null topic name in produce request".into()))?;

            if buf.remaining() < 4 {
                return Err(ProtocolError::IncompleteFrame);
            }
            let num_partitions = buf.get_i32();
            if num_partitions < 0 {
                return Err(ProtocolError::MalformedPayload("Negative partition array count".into()));
            }

            let mut partitions = Vec::with_capacity(num_partitions as usize);
            for _ in 0..num_partitions {
                if buf.remaining() < 8 {
                    return Err(ProtocolError::IncompleteFrame);
                }
                let partition = buf.get_i32();
                let records_len = buf.get_i32();
                if records_len < 0 {
                    return Err(ProtocolError::MalformedPayload("Negative records length".into()));
                }

                let u_records_len = records_len as usize;
                if buf.remaining() < u_records_len {
                    return Err(ProtocolError::IncompleteFrame);
                }

                // CRITICAL ZERO-COPY: Slices memory pointer directly without copying bytes
                let records = buf.split_to(u_records_len);

                partitions.push(PartitionProduceData {
                    partition,
                    records,
                });
            }

            topic_data.push(TopicProduceData {
                topic: topic_name,
                partitions,
            });
        }

        Ok(ProduceRequest {
            header,
            transactional_id,
            acks,
            timeout_ms,
            topic_data,
        })
    }

    /// Decodes a raw Kafka FetchRequest (API Key: 1).
    pub fn decode_fetch_request(mut buf: Bytes) -> Result<FetchRequest, ProtocolError> {
        if buf.remaining() >= 4 {
            let peek_size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if peek_size == buf.remaining() - 4 {
                buf.advance(4);
            }
        }

        let header = Self::decode_request_header(&mut buf)?;
        if header.api_key != ApiKey::Fetch {
            return Err(ProtocolError::UnexpectedApiKey(header.api_key));
        }

        if buf.remaining() < 16 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let replica_id = buf.get_i32();
        let max_wait_ms = buf.get_i32();
        let min_bytes = buf.get_i32();
        let max_bytes = buf.get_i32();
        let isolation_level = if buf.remaining() > 0 && header.api_version >= 4 {
            buf.get_i8()
        } else {
            0
        };

        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let num_topics = buf.get_i32();
        let mut topics = Vec::with_capacity(num_topics.max(0) as usize);

        for _ in 0..num_topics {
            let topic_name = Self::decode_nullable_string(&mut buf)?
                .ok_or_else(|| ProtocolError::MalformedPayload("Null topic name in fetch request".into()))?;

            if buf.remaining() < 4 {
                return Err(ProtocolError::IncompleteFrame);
            }
            let num_parts = buf.get_i32();
            let mut partitions = Vec::with_capacity(num_parts.max(0) as usize);

            for _ in 0..num_parts {
                if buf.remaining() < 12 {
                    return Err(ProtocolError::IncompleteFrame);
                }
                let partition = buf.get_i32();
                let fetch_offset = buf.get_i64();
                let log_start_offset = if header.api_version >= 5 && buf.remaining() >= 8 {
                    buf.get_i64()
                } else {
                    0
                };

                if buf.remaining() < 4 {
                    return Err(ProtocolError::IncompleteFrame);
                }
                let partition_max_bytes = buf.get_i32();

                partitions.push(PartitionFetchData {
                    partition,
                    fetch_offset,
                    log_start_offset,
                    partition_max_bytes,
                });
            }

            topics.push(TopicFetchData {
                topic: topic_name,
                partitions,
            });
        }

        Ok(FetchRequest {
            header,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            topics,
        })
    }

    /// Decodes a binary Kafka ProduceResponse into structured data.
    pub fn decode_produce_response(mut buf: Bytes) -> Result<ProduceResponse, ProtocolError> {
        if buf.remaining() >= 4 {
            let peek_size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if peek_size == buf.remaining() - 4 {
                buf.advance(4);
            }
        }

        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let correlation_id = buf.get_i32();

        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let num_topics = buf.get_i32();
        let mut responses = Vec::with_capacity(num_topics.max(0) as usize);

        for _ in 0..num_topics {
            let topic = Self::decode_nullable_string(&mut buf)?
                .ok_or_else(|| ProtocolError::MalformedPayload("Null topic in response".into()))?;

            if buf.remaining() < 4 {
                return Err(ProtocolError::IncompleteFrame);
            }
            let num_parts = buf.get_i32();
            let mut partitions = Vec::with_capacity(num_parts.max(0) as usize);

            for _ in 0..num_parts {
                if buf.remaining() < 26 {
                    return Err(ProtocolError::IncompleteFrame);
                }
                let partition = buf.get_i32();
                let error_code = KafkaErrorCode::from(buf.get_i16());
                let base_offset = buf.get_i64();
                let log_append_time_ms = buf.get_i64();
                let log_start_offset = buf.get_i64();

                partitions.push(PartitionProduceResponse {
                    partition,
                    error_code,
                    base_offset,
                    log_append_time_ms,
                    log_start_offset,
                });
            }

            responses.push(TopicProduceResponse {
                topic,
                partitions,
            });
        }

        let throttle_time_ms = if buf.remaining() >= 4 {
            buf.get_i32()
        } else {
            0
        };

        Ok(ProduceResponse {
            correlation_id,
            responses,
            throttle_time_ms,
        })
    }

    /// Decodes a raw Kafka FindCoordinatorRequest (API Key: 10).
    pub fn decode_find_coordinator_request(mut buf: Bytes) -> Result<FindCoordinatorRequest, ProtocolError> {
        Self::strip_length_prefix(&mut buf);
        let header = Self::decode_request_header(&mut buf)?;
        if header.api_key != ApiKey::FindCoordinator {
            return Err(ProtocolError::UnexpectedApiKey(header.api_key));
        }
        let key = Self::decode_string(&mut buf)?;
        let key_type = if buf.remaining() >= 1 { buf.get_i8() } else { 0 };
        Ok(FindCoordinatorRequest { header, key, key_type })
    }

    /// Decodes a raw Kafka FindCoordinatorResponse (API Key: 10).
    pub fn decode_find_coordinator_response(mut buf: Bytes) -> Result<FindCoordinatorResponse, ProtocolError> {
        Self::strip_length_prefix(&mut buf);
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let correlation_id = buf.get_i32();
        let throttle_time_ms = if buf.remaining() >= 4 { buf.get_i32() } else { 0 };
        if buf.remaining() < 2 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let error_code = KafkaErrorCode::from(buf.get_i16());
        let error_message = Self::decode_nullable_string(&mut buf)?;
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let node_id = buf.get_i32();
        let host = Self::decode_string(&mut buf)?;
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let port = buf.get_i32();

        Ok(FindCoordinatorResponse {
            correlation_id,
            throttle_time_ms,
            error_code,
            error_message,
            node_id,
            host,
            port,
        })
    }

    /// Decodes a raw Kafka JoinGroupRequest (API Key: 11).
    pub fn decode_join_group_request(mut buf: Bytes) -> Result<JoinGroupRequest, ProtocolError> {
        Self::strip_length_prefix(&mut buf);
        let header = Self::decode_request_header(&mut buf)?;
        if header.api_key != ApiKey::JoinGroup {
            return Err(ProtocolError::UnexpectedApiKey(header.api_key));
        }
        let group_id = Self::decode_string(&mut buf)?;
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let session_timeout_ms = buf.get_i32();
        let rebalance_timeout_ms = if header.api_version >= 1 && buf.remaining() >= 4 {
            buf.get_i32()
        } else {
            session_timeout_ms
        };
        let member_id = Self::decode_string(&mut buf)?;
        let group_instance_id = if header.api_version >= 5 {
            Self::decode_nullable_string(&mut buf)?
        } else {
            None
        };
        let protocol_type = Self::decode_string(&mut buf)?;

        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let num_protocols = buf.get_i32();
        let mut protocols = Vec::with_capacity(num_protocols.max(0) as usize);
        for _ in 0..num_protocols {
            let name = Self::decode_string(&mut buf)?;
            let metadata = Self::decode_bytes(&mut buf)?;
            protocols.push(GroupProtocol { name, metadata });
        }

        Ok(JoinGroupRequest {
            header,
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
        })
    }

    /// Decodes a raw Kafka JoinGroupResponse (API Key: 11).
    pub fn decode_join_group_response(mut buf: Bytes) -> Result<JoinGroupResponse, ProtocolError> {
        Self::strip_length_prefix(&mut buf);
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let correlation_id = buf.get_i32();
        let throttle_time_ms = if buf.remaining() >= 4 { buf.get_i32() } else { 0 };
        if buf.remaining() < 6 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let error_code = KafkaErrorCode::from(buf.get_i16());
        let generation_id = buf.get_i32();
        let protocol_type = Self::decode_nullable_string(&mut buf)?;
        let protocol_name = Self::decode_nullable_string(&mut buf)?;
        let leader_id = Self::decode_string(&mut buf)?;
        let member_id = Self::decode_string(&mut buf)?;

        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let num_members = buf.get_i32();
        let mut members = Vec::with_capacity(num_members.max(0) as usize);
        for _ in 0..num_members {
            let m_id = Self::decode_string(&mut buf)?;
            let g_inst = Self::decode_nullable_string(&mut buf)?;
            let metadata = Self::decode_bytes(&mut buf)?;
            members.push(JoinGroupMember {
                member_id: m_id,
                group_instance_id: g_inst,
                metadata,
            });
        }

        Ok(JoinGroupResponse {
            correlation_id,
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type,
            protocol_name,
            leader_id,
            member_id,
            members,
        })
    }

    /// Decodes a raw Kafka SyncGroupRequest (API Key: 14).
    pub fn decode_sync_group_request(mut buf: Bytes) -> Result<SyncGroupRequest, ProtocolError> {
        Self::strip_length_prefix(&mut buf);
        let header = Self::decode_request_header(&mut buf)?;
        if header.api_key != ApiKey::SyncGroup {
            return Err(ProtocolError::UnexpectedApiKey(header.api_key));
        }
        let group_id = Self::decode_string(&mut buf)?;
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let generation_id = buf.get_i32();
        let member_id = Self::decode_string(&mut buf)?;
        let group_instance_id = if header.api_version >= 3 {
            Self::decode_nullable_string(&mut buf)?
        } else {
            None
        };
        let protocol_type = if header.api_version >= 5 {
            Self::decode_nullable_string(&mut buf)?
        } else {
            None
        };
        let protocol_name = if header.api_version >= 5 {
            Self::decode_nullable_string(&mut buf)?
        } else {
            None
        };

        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let num_assignments = buf.get_i32();
        let mut assignments = Vec::with_capacity(num_assignments.max(0) as usize);
        for _ in 0..num_assignments {
            let m_id = Self::decode_string(&mut buf)?;
            let assignment = Self::decode_bytes(&mut buf)?;
            assignments.push(GroupAssignment {
                member_id: m_id,
                assignment,
            });
        }

        Ok(SyncGroupRequest {
            header,
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            assignments,
        })
    }

    /// Decodes a raw Kafka SyncGroupResponse (API Key: 14).
    pub fn decode_sync_group_response(mut buf: Bytes) -> Result<SyncGroupResponse, ProtocolError> {
        Self::strip_length_prefix(&mut buf);
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let correlation_id = buf.get_i32();
        let throttle_time_ms = if buf.remaining() >= 4 { buf.get_i32() } else { 0 };
        if buf.remaining() < 2 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let error_code = KafkaErrorCode::from(buf.get_i16());
        let protocol_type = Self::decode_nullable_string(&mut buf)?;
        let protocol_name = Self::decode_nullable_string(&mut buf)?;
        let assignment = Self::decode_bytes(&mut buf)?;

        Ok(SyncGroupResponse {
            correlation_id,
            throttle_time_ms,
            error_code,
            protocol_type,
            protocol_name,
            assignment,
        })
    }

    /// Decodes a raw Kafka HeartbeatRequest (API Key: 12).
    pub fn decode_heartbeat_request(mut buf: Bytes) -> Result<HeartbeatRequest, ProtocolError> {
        Self::strip_length_prefix(&mut buf);
        let header = Self::decode_request_header(&mut buf)?;
        if header.api_key != ApiKey::Heartbeat {
            return Err(ProtocolError::UnexpectedApiKey(header.api_key));
        }
        let group_id = Self::decode_string(&mut buf)?;
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let generation_id = buf.get_i32();
        let member_id = Self::decode_string(&mut buf)?;
        let group_instance_id = if header.api_version >= 3 {
            Self::decode_nullable_string(&mut buf)?
        } else {
            None
        };

        Ok(HeartbeatRequest {
            header,
            group_id,
            generation_id,
            member_id,
            group_instance_id,
        })
    }

    /// Decodes a raw Kafka HeartbeatResponse (API Key: 12).
    pub fn decode_heartbeat_response(mut buf: Bytes) -> Result<HeartbeatResponse, ProtocolError> {
        Self::strip_length_prefix(&mut buf);
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let correlation_id = buf.get_i32();
        let throttle_time_ms = if buf.remaining() >= 4 { buf.get_i32() } else { 0 };
        if buf.remaining() < 2 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let error_code = KafkaErrorCode::from(buf.get_i16());

        Ok(HeartbeatResponse {
            correlation_id,
            throttle_time_ms,
            error_code,
        })
    }

    /// Decodes a raw Kafka LeaveGroupRequest (API Key: 13).
    pub fn decode_leave_group_request(mut buf: Bytes) -> Result<LeaveGroupRequest, ProtocolError> {
        Self::strip_length_prefix(&mut buf);
        let header = Self::decode_request_header(&mut buf)?;
        if header.api_key != ApiKey::LeaveGroup {
            return Err(ProtocolError::UnexpectedApiKey(header.api_key));
        }
        let group_id = Self::decode_string(&mut buf)?;
        let member_id = Self::decode_string(&mut buf)?;

        Ok(LeaveGroupRequest {
            header,
            group_id,
            member_id,
        })
    }

    /// Decodes a raw Kafka LeaveGroupResponse (API Key: 13).
    pub fn decode_leave_group_response(mut buf: Bytes) -> Result<LeaveGroupResponse, ProtocolError> {
        Self::strip_length_prefix(&mut buf);
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let correlation_id = buf.get_i32();
        let throttle_time_ms = if buf.remaining() >= 4 { buf.get_i32() } else { 0 };
        if buf.remaining() < 2 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let error_code = KafkaErrorCode::from(buf.get_i16());

        Ok(LeaveGroupResponse {
            correlation_id,
            throttle_time_ms,
            error_code,
        })
    }

    /// Decodes a raw Kafka OffsetCommitRequest (API Key: 8).
    pub fn decode_offset_commit_request(mut buf: Bytes) -> Result<OffsetCommitRequest, ProtocolError> {
        Self::strip_length_prefix(&mut buf);
        let header = Self::decode_request_header(&mut buf)?;
        if header.api_key != ApiKey::OffsetCommit {
            return Err(ProtocolError::UnexpectedApiKey(header.api_key));
        }
        let group_id = Self::decode_string(&mut buf)?;
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let generation_id = buf.get_i32();
        let member_id = Self::decode_string(&mut buf)?;
        let retention_time_ms = if header.api_version >= 2 && buf.remaining() >= 8 {
            buf.get_i64()
        } else {
            -1
        };

        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let num_topics = buf.get_i32();
        let mut topics = Vec::with_capacity(num_topics.max(0) as usize);

        for _ in 0..num_topics {
            let topic = Self::decode_string(&mut buf)?;
            if buf.remaining() < 4 {
                return Err(ProtocolError::IncompleteFrame);
            }
            let num_parts = buf.get_i32();
            let mut partitions = Vec::with_capacity(num_parts.max(0) as usize);

            for _ in 0..num_parts {
                if buf.remaining() < 12 {
                    return Err(ProtocolError::IncompleteFrame);
                }
                let partition = buf.get_i32();
                let offset = buf.get_i64();
                let leader_epoch = if header.api_version >= 6 && buf.remaining() >= 4 {
                    buf.get_i32()
                } else {
                    -1
                };
                let metadata = Self::decode_nullable_string(&mut buf)?;
                partitions.push(PartitionOffsetCommit {
                    partition,
                    offset,
                    leader_epoch,
                    metadata,
                });
            }

            topics.push(TopicOffsetCommit { topic, partitions });
        }

        Ok(OffsetCommitRequest {
            header,
            group_id,
            generation_id,
            member_id,
            retention_time_ms,
            topics,
        })
    }

    /// Decodes a raw Kafka OffsetCommitResponse (API Key: 8).
    pub fn decode_offset_commit_response(mut buf: Bytes) -> Result<OffsetCommitResponse, ProtocolError> {
        Self::strip_length_prefix(&mut buf);
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let correlation_id = buf.get_i32();
        let throttle_time_ms = if buf.remaining() >= 4 { buf.get_i32() } else { 0 };

        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let num_topics = buf.get_i32();
        let mut topics = Vec::with_capacity(num_topics.max(0) as usize);

        for _ in 0..num_topics {
            let topic = Self::decode_string(&mut buf)?;
            if buf.remaining() < 4 {
                return Err(ProtocolError::IncompleteFrame);
            }
            let num_parts = buf.get_i32();
            let mut partitions = Vec::with_capacity(num_parts.max(0) as usize);

            for _ in 0..num_parts {
                if buf.remaining() < 6 {
                    return Err(ProtocolError::IncompleteFrame);
                }
                let partition = buf.get_i32();
                let error_code = KafkaErrorCode::from(buf.get_i16());
                partitions.push(PartitionOffsetCommitResponse { partition, error_code });
            }

            topics.push(TopicOffsetCommitResponse { topic, partitions });
        }

        Ok(OffsetCommitResponse {
            correlation_id,
            throttle_time_ms,
            topics,
        })
    }

    /// Decodes a raw Kafka OffsetFetchRequest (API Key: 9).
    pub fn decode_offset_fetch_request(mut buf: Bytes) -> Result<OffsetFetchRequest, ProtocolError> {
        Self::strip_length_prefix(&mut buf);
        let header = Self::decode_request_header(&mut buf)?;
        if header.api_key != ApiKey::OffsetFetch {
            return Err(ProtocolError::UnexpectedApiKey(header.api_key));
        }
        let group_id = Self::decode_string(&mut buf)?;

        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let num_topics = buf.get_i32();
        let mut topics = Vec::with_capacity(num_topics.max(0) as usize);

        for _ in 0..num_topics {
            let topic = Self::decode_string(&mut buf)?;
            if buf.remaining() < 4 {
                return Err(ProtocolError::IncompleteFrame);
            }
            let num_parts = buf.get_i32();
            let mut partitions = Vec::with_capacity(num_parts.max(0) as usize);
            for _ in 0..num_parts {
                if buf.remaining() < 4 {
                    return Err(ProtocolError::IncompleteFrame);
                }
                partitions.push(buf.get_i32());
            }
            topics.push(TopicOffsetFetch { topic, partitions });
        }

        Ok(OffsetFetchRequest {
            header,
            group_id,
            topics,
        })
    }

    /// Decodes a raw Kafka OffsetFetchResponse (API Key: 9).
    pub fn decode_offset_fetch_response(mut buf: Bytes) -> Result<OffsetFetchResponse, ProtocolError> {
        Self::strip_length_prefix(&mut buf);
        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let correlation_id = buf.get_i32();
        let throttle_time_ms = if buf.remaining() >= 4 { buf.get_i32() } else { 0 };

        if buf.remaining() < 4 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let num_topics = buf.get_i32();
        let mut topics = Vec::with_capacity(num_topics.max(0) as usize);

        for _ in 0..num_topics {
            let topic = Self::decode_string(&mut buf)?;
            if buf.remaining() < 4 {
                return Err(ProtocolError::IncompleteFrame);
            }
            let num_parts = buf.get_i32();
            let mut partitions = Vec::with_capacity(num_parts.max(0) as usize);

            for _ in 0..num_parts {
                if buf.remaining() < 16 {
                    return Err(ProtocolError::IncompleteFrame);
                }
                let partition = buf.get_i32();
                let offset = buf.get_i64();
                let leader_epoch = if buf.remaining() >= 4 { buf.get_i32() } else { -1 };
                let metadata = Self::decode_nullable_string(&mut buf)?;
                if buf.remaining() < 2 {
                    return Err(ProtocolError::IncompleteFrame);
                }
                let error_code = KafkaErrorCode::from(buf.get_i16());
                partitions.push(PartitionOffsetFetchResponse {
                    partition,
                    offset,
                    leader_epoch,
                    metadata,
                    error_code,
                });
            }

            topics.push(TopicOffsetFetchResponse { topic, partitions });
        }

        let error_code = if buf.remaining() >= 2 {
            KafkaErrorCode::from(buf.get_i16())
        } else {
            KafkaErrorCode::None
        };

        Ok(OffsetFetchResponse {
            correlation_id,
            throttle_time_ms,
            topics,
            error_code,
        })
    }
}

// ============================================================================
// Direct WAL Routing Pipeline
// ============================================================================

impl ProduceRequest {
    /// Zero-copy pipeline: routes the decoded ProduceRequest RecordBatch memory
    /// directly into the Phase 1 WAL engine memory pointers.
    ///
    /// Generates the exact binary ProduceResponse with assigned base offsets.
    pub fn route_to_wal(&self, segment: &mut LogSegment) -> Result<ProduceResponse, WalError> {
        self.route_to_wal_with_backpressure(segment, false)
    }

    /// Routes the ProduceRequest to the WAL engine with OOM / backpressure protection.
    /// If `is_backpressured` is true, returns a retriable Kafka error (NotEnoughReplicas)
    /// to signal the client to back off, protecting the server from memory exhaustion.
    pub fn route_to_wal_with_backpressure(
        &self,
        segment: &mut LogSegment,
        is_backpressured: bool,
    ) -> Result<ProduceResponse, WalError> {
        let mut topic_responses = Vec::with_capacity(self.topic_data.len());

        for topic_entry in &self.topic_data {
            let mut partition_responses = Vec::with_capacity(topic_entry.partitions.len());

            for part in &topic_entry.partitions {
                if is_backpressured {
                    // OOM Protection: Return RETRIABLE_ERROR (NotEnoughReplicas)
                    // Official Kafka clients back off and retry automatically!
                    partition_responses.push(PartitionProduceResponse {
                        partition: part.partition,
                        error_code: KafkaErrorCode::NotEnoughReplicas,
                        base_offset: -1,
                        log_append_time_ms: -1,
                        log_start_offset: 0,
                    });
                } else {
                    // Direct zero-copy memory pointer slice passed to WAL engine
                    let append_res = segment.append(part.records.as_ref())?;

                    partition_responses.push(PartitionProduceResponse {
                        partition: part.partition,
                        error_code: KafkaErrorCode::None,
                        base_offset: append_res.record_offset as i64,
                        log_append_time_ms: -1,
                        log_start_offset: 0,
                    });
                }
            }

            topic_responses.push(TopicProduceResponse {
                topic: topic_entry.topic.clone(), // Zero-copy refcount increment
                partitions: partition_responses,
            });
        }

        Ok(ProduceResponse {
            correlation_id: self.header.correlation_id,
            responses: topic_responses,
            throttle_time_ms: if is_backpressured { 50 } else { 0 },
        })
    }
}

// ============================================================================
// Binary Encoder for Official Kafka Clients
// ============================================================================

pub struct KafkaEncoder;

impl KafkaEncoder {
    /// Encodes a ProduceRequest into the exact Kafka binary wire layout.
    pub fn encode_produce_request(request: &ProduceRequest, dst: &mut BytesMut) {
        let start_len = dst.len();
        dst.put_i32(0); // 4-byte size placeholder

        // Request Header
        dst.put_i16(request.header.api_key.to_i16());
        dst.put_i16(request.header.api_version);
        dst.put_i32(request.header.correlation_id);
        if let Some(ref client_id) = request.header.client_id {
            dst.put_i16(client_id.len() as i16);
            dst.put_slice(client_id);
        } else {
            dst.put_i16(-1);
        }

        // transactional_id
        if let Some(ref tx_id) = request.transactional_id {
            dst.put_i16(tx_id.len() as i16);
            dst.put_slice(tx_id);
        } else {
            dst.put_i16(-1);
        }

        dst.put_i16(request.acks);
        dst.put_i32(request.timeout_ms);

        // Topic Data Array
        dst.put_i32(request.topic_data.len() as i32);
        for topic in &request.topic_data {
            dst.put_i16(topic.topic.len() as i16);
            dst.put_slice(&topic.topic);

            dst.put_i32(topic.partitions.len() as i32);
            for part in &topic.partitions {
                dst.put_i32(part.partition);
                dst.put_i32(part.records.len() as i32);
                dst.put_slice(&part.records);
            }
        }

        // Backfill size
        let total_size = (dst.len() - start_len - 4) as i32;
        let size_bytes = total_size.to_be_bytes();
        dst[start_len..start_len + 4].copy_from_slice(&size_bytes);
    }

    /// Encodes a ProduceResponse into the exact binary wire layout expected by
    /// librdkafka and Java Kafka clients.
    pub fn encode_produce_response(response: &ProduceResponse, dst: &mut BytesMut) {
        let start_len = dst.len();

        // 1. Placeholder for 4-byte message size delimiter
        dst.put_i32(0);

        // 2. Correlation ID
        dst.put_i32(response.correlation_id);

        // 3. Topic Responses Array
        dst.put_i32(response.responses.len() as i32);
        for topic_resp in &response.responses {
            // Topic Name (INT16 len + UTF-8 bytes)
            dst.put_i16(topic_resp.topic.len() as i16);
            dst.put_slice(&topic_resp.topic);

            // Partition Responses Array
            dst.put_i32(topic_resp.partitions.len() as i32);
            for part in &topic_resp.partitions {
                dst.put_i32(part.partition);
                dst.put_i16(part.error_code as i16);
                dst.put_i64(part.base_offset);
                dst.put_i64(part.log_append_time_ms);
                dst.put_i64(part.log_start_offset);
            }
        }

        // 4. Throttle Time (ms)
        dst.put_i32(response.throttle_time_ms);

        // 5. Backfill 4-byte message size in big-endian network byte order
        let message_size = (dst.len() - start_len - 4) as i32;
        let size_bytes = message_size.to_be_bytes();
        dst[start_len..start_len + 4].copy_from_slice(&size_bytes);
    }

    /// Helper to encode a length-prefixed non-null string.
    #[inline]
    pub fn encode_string(val: &[u8], dst: &mut BytesMut) {
        dst.put_i16(val.len() as i16);
        dst.put_slice(val);
    }

    /// Helper to encode a length-prefixed nullable string (-1 for null).
    #[inline]
    pub fn encode_nullable_string(val: Option<&[u8]>, dst: &mut BytesMut) {
        if let Some(s) = val {
            dst.put_i16(s.len() as i16);
            dst.put_slice(s);
        } else {
            dst.put_i16(-1);
        }
    }

    /// Helper to encode length-prefixed byte array.
    #[inline]
    pub fn encode_bytes(val: &[u8], dst: &mut BytesMut) {
        dst.put_i32(val.len() as i32);
        dst.put_slice(val);
    }

    /// Helper to encode nullable length-prefixed byte array.
    #[inline]
    pub fn encode_nullable_bytes(val: Option<&[u8]>, dst: &mut BytesMut) {
        if let Some(b) = val {
            dst.put_i32(b.len() as i32);
            dst.put_slice(b);
        } else {
            dst.put_i32(-1);
        }
    }

    /// Encodes a standard Kafka Request Header.
    pub fn encode_request_header(header: &RequestHeader, dst: &mut BytesMut) {
        dst.put_i16(header.api_key.to_i16());
        dst.put_i16(header.api_version);
        dst.put_i32(header.correlation_id);
        Self::encode_nullable_string(header.client_id.as_deref(), dst);
    }

    /// Encodes a FindCoordinatorRequest into binary wire layout.
    pub fn encode_find_coordinator_request(req: &FindCoordinatorRequest, dst: &mut BytesMut) {
        let start = dst.len();
        dst.put_i32(0);
        Self::encode_request_header(&req.header, dst);
        Self::encode_string(&req.key, dst);
        dst.put_i8(req.key_type);
        let size = (dst.len() - start - 4) as i32;
        dst[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encodes a FindCoordinatorResponse into binary wire layout.
    pub fn encode_find_coordinator_response(resp: &FindCoordinatorResponse, dst: &mut BytesMut) {
        let start = dst.len();
        dst.put_i32(0);
        dst.put_i32(resp.correlation_id);
        dst.put_i32(resp.throttle_time_ms);
        dst.put_i16(resp.error_code as i16);
        Self::encode_nullable_string(resp.error_message.as_deref(), dst);
        dst.put_i32(resp.node_id);
        Self::encode_string(&resp.host, dst);
        dst.put_i32(resp.port);
        let size = (dst.len() - start - 4) as i32;
        dst[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encodes a JoinGroupRequest into binary wire layout.
    pub fn encode_join_group_request(req: &JoinGroupRequest, dst: &mut BytesMut) {
        let start = dst.len();
        dst.put_i32(0);
        Self::encode_request_header(&req.header, dst);
        Self::encode_string(&req.group_id, dst);
        dst.put_i32(req.session_timeout_ms);
        dst.put_i32(req.rebalance_timeout_ms);
        Self::encode_string(&req.member_id, dst);
        if req.header.api_version >= 5 {
            Self::encode_nullable_string(req.group_instance_id.as_deref(), dst);
        }
        Self::encode_string(&req.protocol_type, dst);
        dst.put_i32(req.protocols.len() as i32);
        for proto in &req.protocols {
            Self::encode_string(&proto.name, dst);
            Self::encode_bytes(&proto.metadata, dst);
        }
        let size = (dst.len() - start - 4) as i32;
        dst[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encodes a JoinGroupResponse into binary wire layout.
    pub fn encode_join_group_response(resp: &JoinGroupResponse, dst: &mut BytesMut) {
        let start = dst.len();
        dst.put_i32(0);
        dst.put_i32(resp.correlation_id);
        dst.put_i32(resp.throttle_time_ms);
        dst.put_i16(resp.error_code as i16);
        dst.put_i32(resp.generation_id);
        Self::encode_nullable_string(resp.protocol_type.as_deref(), dst);
        Self::encode_nullable_string(resp.protocol_name.as_deref(), dst);
        Self::encode_string(&resp.leader_id, dst);
        Self::encode_string(&resp.member_id, dst);
        dst.put_i32(resp.members.len() as i32);
        for member in &resp.members {
            Self::encode_string(&member.member_id, dst);
            Self::encode_nullable_string(member.group_instance_id.as_deref(), dst);
            Self::encode_bytes(&member.metadata, dst);
        }
        let size = (dst.len() - start - 4) as i32;
        dst[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encodes a SyncGroupRequest into binary wire layout.
    pub fn encode_sync_group_request(req: &SyncGroupRequest, dst: &mut BytesMut) {
        let start = dst.len();
        dst.put_i32(0);
        Self::encode_request_header(&req.header, dst);
        Self::encode_string(&req.group_id, dst);
        dst.put_i32(req.generation_id);
        Self::encode_string(&req.member_id, dst);
        if req.header.api_version >= 3 {
            Self::encode_nullable_string(req.group_instance_id.as_deref(), dst);
        }
        if req.header.api_version >= 5 {
            Self::encode_nullable_string(req.protocol_type.as_deref(), dst);
            Self::encode_nullable_string(req.protocol_name.as_deref(), dst);
        }
        dst.put_i32(req.assignments.len() as i32);
        for assign in &req.assignments {
            Self::encode_string(&assign.member_id, dst);
            Self::encode_bytes(&assign.assignment, dst);
        }
        let size = (dst.len() - start - 4) as i32;
        dst[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encodes a SyncGroupResponse into binary wire layout.
    pub fn encode_sync_group_response(resp: &SyncGroupResponse, dst: &mut BytesMut) {
        let start = dst.len();
        dst.put_i32(0);
        dst.put_i32(resp.correlation_id);
        dst.put_i32(resp.throttle_time_ms);
        dst.put_i16(resp.error_code as i16);
        Self::encode_nullable_string(resp.protocol_type.as_deref(), dst);
        Self::encode_nullable_string(resp.protocol_name.as_deref(), dst);
        Self::encode_bytes(&resp.assignment, dst);
        let size = (dst.len() - start - 4) as i32;
        dst[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encodes a HeartbeatRequest into binary wire layout.
    pub fn encode_heartbeat_request(req: &HeartbeatRequest, dst: &mut BytesMut) {
        let start = dst.len();
        dst.put_i32(0);
        Self::encode_request_header(&req.header, dst);
        Self::encode_string(&req.group_id, dst);
        dst.put_i32(req.generation_id);
        Self::encode_string(&req.member_id, dst);
        Self::encode_nullable_string(req.group_instance_id.as_deref(), dst);
        let size = (dst.len() - start - 4) as i32;
        dst[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encodes a HeartbeatResponse into binary wire layout.
    pub fn encode_heartbeat_response(resp: &HeartbeatResponse, dst: &mut BytesMut) {
        let start = dst.len();
        dst.put_i32(0);
        dst.put_i32(resp.correlation_id);
        dst.put_i32(resp.throttle_time_ms);
        dst.put_i16(resp.error_code as i16);
        let size = (dst.len() - start - 4) as i32;
        dst[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encodes a LeaveGroupRequest into binary wire layout.
    pub fn encode_leave_group_request(req: &LeaveGroupRequest, dst: &mut BytesMut) {
        let start = dst.len();
        dst.put_i32(0);
        Self::encode_request_header(&req.header, dst);
        Self::encode_string(&req.group_id, dst);
        Self::encode_string(&req.member_id, dst);
        let size = (dst.len() - start - 4) as i32;
        dst[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encodes a LeaveGroupResponse into binary wire layout.
    pub fn encode_leave_group_response(resp: &LeaveGroupResponse, dst: &mut BytesMut) {
        let start = dst.len();
        dst.put_i32(0);
        dst.put_i32(resp.correlation_id);
        dst.put_i32(resp.throttle_time_ms);
        dst.put_i16(resp.error_code as i16);
        let size = (dst.len() - start - 4) as i32;
        dst[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encodes an OffsetCommitRequest into binary wire layout.
    pub fn encode_offset_commit_request(req: &OffsetCommitRequest, dst: &mut BytesMut) {
        let start = dst.len();
        dst.put_i32(0);
        Self::encode_request_header(&req.header, dst);
        Self::encode_string(&req.group_id, dst);
        dst.put_i32(req.generation_id);
        Self::encode_string(&req.member_id, dst);
        if req.header.api_version >= 2 {
            dst.put_i64(req.retention_time_ms);
        }
        dst.put_i32(req.topics.len() as i32);
        for topic in &req.topics {
            Self::encode_string(&topic.topic, dst);
            dst.put_i32(topic.partitions.len() as i32);
            for part in &topic.partitions {
                dst.put_i32(part.partition);
                dst.put_i64(part.offset);
                if req.header.api_version >= 6 {
                    dst.put_i32(part.leader_epoch);
                }
                Self::encode_nullable_string(part.metadata.as_deref(), dst);
            }
        }
        let size = (dst.len() - start - 4) as i32;
        dst[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encodes an OffsetCommitResponse into binary wire layout.
    pub fn encode_offset_commit_response(resp: &OffsetCommitResponse, dst: &mut BytesMut) {
        let start = dst.len();
        dst.put_i32(0);
        dst.put_i32(resp.correlation_id);
        dst.put_i32(resp.throttle_time_ms);
        dst.put_i32(resp.topics.len() as i32);
        for topic in &resp.topics {
            Self::encode_string(&topic.topic, dst);
            dst.put_i32(topic.partitions.len() as i32);
            for part in &topic.partitions {
                dst.put_i32(part.partition);
                dst.put_i16(part.error_code as i16);
            }
        }
        let size = (dst.len() - start - 4) as i32;
        dst[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encodes an OffsetFetchRequest into binary wire layout.
    pub fn encode_offset_fetch_request(req: &OffsetFetchRequest, dst: &mut BytesMut) {
        let start = dst.len();
        dst.put_i32(0);
        Self::encode_request_header(&req.header, dst);
        Self::encode_string(&req.group_id, dst);
        dst.put_i32(req.topics.len() as i32);
        for topic in &req.topics {
            Self::encode_string(&topic.topic, dst);
            dst.put_i32(topic.partitions.len() as i32);
            for &p in &topic.partitions {
                dst.put_i32(p);
            }
        }
        let size = (dst.len() - start - 4) as i32;
        dst[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encodes an OffsetFetchResponse into binary wire layout.
    pub fn encode_offset_fetch_response(resp: &OffsetFetchResponse, dst: &mut BytesMut) {
        let start = dst.len();
        dst.put_i32(0);
        dst.put_i32(resp.correlation_id);
        dst.put_i32(resp.throttle_time_ms);
        dst.put_i32(resp.topics.len() as i32);
        for topic in &resp.topics {
            Self::encode_string(&topic.topic, dst);
            dst.put_i32(topic.partitions.len() as i32);
            for part in &topic.partitions {
                dst.put_i32(part.partition);
                dst.put_i64(part.offset);
                dst.put_i32(part.leader_epoch);
                Self::encode_nullable_string(part.metadata.as_deref(), dst);
                dst.put_i16(part.error_code as i16);
            }
        }
        dst.put_i16(resp.error_code as i16);
        let size = (dst.len() - start - 4) as i32;
        dst[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encodes a FetchResponse into the standard binary format.
    pub fn encode_fetch_response(response: &FetchResponse, dst: &mut BytesMut) {
        let start_len = dst.len();

        dst.put_i32(0); // Size placeholder
        dst.put_i32(response.correlation_id);
        dst.put_i32(response.throttle_time_ms);
        dst.put_i16(response.error_code as i16);

        // Responses Array
        dst.put_i32(response.responses.len() as i32);
        for topic_resp in &response.responses {
            dst.put_i16(topic_resp.topic.len() as i16);
            dst.put_slice(&topic_resp.topic);

            dst.put_i32(topic_resp.partitions.len() as i32);
            for part in &topic_resp.partitions {
                dst.put_i32(part.partition);
                dst.put_i16(part.error_code as i16);
                dst.put_i64(part.high_watermark);
                dst.put_i64(part.last_stable_offset);
                dst.put_i64(part.log_start_offset);

                // Records payload
                dst.put_i32(part.records.len() as i32);
                dst.put_slice(&part.records);
            }
        }

        let message_size = (dst.len() - start_len - 4) as i32;
        let size_bytes = message_size.to_be_bytes();
        dst[start_len..start_len + 4].copy_from_slice(&size_bytes);
    }
}

// ============================================================================
// Built-in Unit Tests & Raw Hex-Byte Dumps
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::LogSegmentConfig;
    use std::path::PathBuf;

    /// Helper to convert hex strings to zero-copy Bytes.
    fn hex_to_bytes(hex: &str) -> Bytes {
        let clean: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
        let mut bytes = Vec::with_capacity(clean.len() / 2);
        for i in (0..clean.len()).step_by(2) {
            let byte = u8::from_str_radix(&clean[i..i + 2], 16).expect("Invalid hex character");
            bytes.push(byte);
        }
        Bytes::from(bytes)
    }

    /// Actual Kafka ProduceRequest (v3) wire hex dump:
    /// - size: 103 bytes (0x00000067)
    /// - api_key: 0 (Produce)
    /// - api_version: 3
    /// - correlation_id: 101 (0x00000065)
    /// - client_id: "velocitymq-producer" (19 bytes)
    /// - transactional_id: null (-1)
    /// - acks: 1
    /// - timeout_ms: 1500 (0x000005dc)
    /// - topics: 1 ("telemetry-events", 16 bytes)
    /// - partitions: 1 (partition: 0)
    /// - records: 32 bytes of RecordBatch binary payload
    const RAW_KAFKA_PRODUCE_HEX: &str = "\
        00 00 00 67 \
        00 00 \
        00 03 \
        00 00 00 65 \
        00 13 76 65 6c 6f 63 69 74 79 6d 71 2d 70 72 6f 64 75 63 65 72 \
        ff ff \
        00 01 \
        00 00 05 dc \
        00 00 00 01 \
        00 10 74 65 6c 65 6d 65 74 72 79 2d 65 76 65 6e 74 73 \
        00 00 00 01 \
        00 00 00 00 \
        00 00 00 20 \
        aa bb cc dd ee ff 00 11 22 33 44 55 66 77 88 99 \
        01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f 10";

    /// Actual Kafka FetchRequest (v4) wire hex dump:
    /// - size: 88 bytes (0x00000058)
    /// - api_key: 1 (Fetch)
    /// - api_version: 4
    /// - correlation_id: 202 (0x000000ca)
    /// - client_id: "velocitymq-consumer" (19 bytes)
    /// - replica_id: -1 (0xffffffff)
    /// - max_wait_ms: 500 (0x000001f4)
    /// - min_bytes: 1 (0x00000001)
    /// - max_bytes: 1048576 (0x00100000)
    /// - isolation_level: 0
    /// - topics: 1 ("telemetry-events", 16 bytes)
    /// - partitions: 1 (partition: 0, fetch_offset: 1000, partition_max_bytes: 1048576)
    const RAW_KAFKA_FETCH_HEX: &str = "\
        00 00 00 58 \
        00 01 \
        00 04 \
        00 00 00 ca \
        00 13 76 65 6c 6f 63 69 74 79 6d 71 2d 63 6f 6e 73 75 6d 65 72 \
        ff ff ff ff \
        00 00 01 f4 \
        00 00 00 01 \
        00 10 00 00 \
        00 \
        00 00 00 01 \
        00 10 74 65 6c 65 6d 65 74 72 79 2d 65 76 65 6e 74 73 \
        00 00 00 01 \
        00 00 00 00 \
        00 00 00 00 00 00 03 e8 \
        00 10 00 00";

    #[test]
    fn test_zero_copy_produce_request_parsing() {
        let raw_bytes = hex_to_bytes(RAW_KAFKA_PRODUCE_HEX);
        let base_ptr = raw_bytes.as_ptr();

        let req = KafkaDecoder::decode_produce_request(raw_bytes.clone())
            .expect("Failed to decode raw Kafka ProduceRequest");

        assert_eq!(req.header.api_key, ApiKey::Produce);
        assert_eq!(req.header.api_version, 3);
        assert_eq!(req.header.correlation_id, 101);
        assert_eq!(req.header.client_id.as_deref(), Some(b"velocitymq-producer".as_slice()));
        assert_eq!(req.acks, 1);
        assert_eq!(req.timeout_ms, 1500);

        assert_eq!(req.topic_data.len(), 1);
        let topic = &req.topic_data[0];
        assert_eq!(topic.topic.as_ref(), b"telemetry-events");
        assert_eq!(topic.partitions.len(), 1);

        let part = &topic.partitions[0];
        assert_eq!(part.partition, 0);
        assert_eq!(part.records.len(), 32);

        // STRICT ZERO-COPY PROOF:
        // Assert that the record batch payload pointer points directly inside the original buffer!
        let records_ptr = part.records.as_ptr();
        assert!(
            records_ptr >= base_ptr && records_ptr < unsafe { base_ptr.add(raw_bytes.len()) },
            "Zero-copy violation: Record payload was copied to a new memory allocation!"
        );
    }

    #[test]
    fn test_zero_copy_fetch_request_parsing() {
        let raw_bytes = hex_to_bytes(RAW_KAFKA_FETCH_HEX);
        let base_ptr = raw_bytes.as_ptr();

        let req = KafkaDecoder::decode_fetch_request(raw_bytes.clone())
            .expect("Failed to decode raw Kafka FetchRequest");

        assert_eq!(req.header.api_key, ApiKey::Fetch);
        assert_eq!(req.header.api_version, 4);
        assert_eq!(req.header.correlation_id, 202);
        assert_eq!(req.header.client_id.as_deref(), Some(b"velocitymq-consumer".as_slice()));
        assert_eq!(req.replica_id, -1);
        assert_eq!(req.max_wait_ms, 500);
        assert_eq!(req.min_bytes, 1);
        assert_eq!(req.max_bytes, 1048576);
        assert_eq!(req.isolation_level, 0);

        assert_eq!(req.topics.len(), 1);
        let topic = &req.topics[0];
        assert_eq!(topic.topic.as_ref(), b"telemetry-events");
        assert_eq!(topic.partitions.len(), 1);

        let part = &topic.partitions[0];
        assert_eq!(part.partition, 0);
        assert_eq!(part.fetch_offset, 1000);
        assert_eq!(part.partition_max_bytes, 1048576);

        // Strict zero-copy assertion for topic name
        let topic_ptr = topic.topic.as_ptr();
        assert!(
            topic_ptr >= base_ptr && topic_ptr < unsafe { base_ptr.add(raw_bytes.len()) },
            "Zero-copy violation: Topic name was copied into new heap memory!"
        );
    }

    #[test]
    fn test_direct_wal_routing_pipeline_and_response_encoding() {
        // 1. Decode raw Kafka ProduceRequest
        let raw_bytes = hex_to_bytes(RAW_KAFKA_PRODUCE_HEX);
        let req = KafkaDecoder::decode_produce_request(raw_bytes)
            .expect("Failed to decode produce request");

        // 2. Initialize Phase 1 WAL LogSegment
        let temp_path = PathBuf::from("target").join(format!("velmq_proto_wal_{}.wal", std::process::id()));
        let _ = std::fs::remove_file(&temp_path);

        let config = LogSegmentConfig {
            segment_id: 1,
            max_segment_size: 10 * 1024 * 1024,
            buffer_size: 64 * 1024,
            ring_depth: 64,
            buffer_pool_size: 8,
            sync_on_flush: true,
        };

        let mut segment = LogSegment::create(&temp_path, config)
            .expect("Failed to create LogSegment for routing");

        // 3. Route decoded RecordBatch memory pointers directly into WAL engine
        let produce_response = req.route_to_wal(&mut segment)
            .expect("Failed to route produce request to WAL");

        assert_eq!(produce_response.correlation_id, 101);
        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(produce_response.responses[0].partitions[0].base_offset, 0);
        assert_eq!(produce_response.responses[0].partitions[0].error_code, KafkaErrorCode::None);

        segment.flush(true).expect("WAL flush failed");
        drop(segment);
        let _ = std::fs::remove_file(&temp_path);

        // 4. Encode ProduceResponse to binary wire format expected by official Kafka clients
        let mut encoded_resp = BytesMut::with_capacity(128);
        KafkaEncoder::encode_produce_response(&produce_response, &mut encoded_resp);

        // 5. Verify encoded binary response layout
        let mut resp_bytes = encoded_resp.freeze();
        let message_size = resp_bytes.get_i32();
        assert_eq!(message_size as usize, resp_bytes.remaining());

        let corr_id = resp_bytes.get_i32();
        assert_eq!(corr_id, 101);

        let num_topics = resp_bytes.get_i32();
        assert_eq!(num_topics, 1);

        let topic_len = resp_bytes.get_i16();
        assert_eq!(topic_len, 16);
        let topic_name = resp_bytes.split_to(16);
        assert_eq!(topic_name.as_ref(), b"telemetry-events");

        let num_parts = resp_bytes.get_i32();
        assert_eq!(num_parts, 1);

        let part_idx = resp_bytes.get_i32();
        assert_eq!(part_idx, 0);

        let err_code = resp_bytes.get_i16();
        assert_eq!(err_code, 0); // None / Success

        let base_offset = resp_bytes.get_i64();
        assert_eq!(base_offset, 0); // Assigned WAL offset
    }

    #[test]
    fn test_consumer_group_protocol_zero_copy_roundtrip() {
        // 1. FindCoordinator
        let find_req = FindCoordinatorRequest {
            header: RequestHeader {
                api_key: ApiKey::FindCoordinator,
                api_version: 1,
                correlation_id: 301,
                client_id: Some(Bytes::from_static(b"consumer-client-1")),
            },
            key: Bytes::from_static(b"payment-processor-group"),
            key_type: 0,
        };
        let mut buf = BytesMut::with_capacity(128);
        KafkaEncoder::encode_find_coordinator_request(&find_req, &mut buf);
        let decoded_find = KafkaDecoder::decode_find_coordinator_request(buf.freeze())
            .expect("FindCoordinator decode failed");
        assert_eq!(decoded_find.header.correlation_id, 301);
        assert_eq!(decoded_find.key.as_ref(), b"payment-processor-group");
        assert_eq!(decoded_find.key_type, 0);

        let find_resp = FindCoordinatorResponse {
            correlation_id: 301,
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            node_id: 1,
            host: Bytes::from_static(b"127.0.0.1"),
            port: 9092,
        };
        let mut buf_resp = BytesMut::with_capacity(128);
        KafkaEncoder::encode_find_coordinator_response(&find_resp, &mut buf_resp);
        let decoded_find_resp = KafkaDecoder::decode_find_coordinator_response(buf_resp.freeze())
            .expect("FindCoordinatorResponse decode failed");
        assert_eq!(decoded_find_resp.node_id, 1);
        assert_eq!(decoded_find_resp.host.as_ref(), b"127.0.0.1");
        assert_eq!(decoded_find_resp.port, 9092);

        // 2. JoinGroup
        let join_req = JoinGroupRequest {
            header: RequestHeader {
                api_key: ApiKey::JoinGroup,
                api_version: 2,
                correlation_id: 302,
                client_id: Some(Bytes::from_static(b"consumer-client-1")),
            },
            group_id: Bytes::from_static(b"payment-processor-group"),
            session_timeout_ms: 10000,
            rebalance_timeout_ms: 30000,
            member_id: Bytes::from_static(b""),
            group_instance_id: None,
            protocol_type: Bytes::from_static(b"consumer"),
            protocols: vec![GroupProtocol {
                name: Bytes::from_static(b"roundrobin"),
                metadata: Bytes::from_static(b"SUBSCRIPTION_TOPICS_ORDERS"),
            }],
        };
        let mut join_wire = BytesMut::with_capacity(256);
        KafkaEncoder::encode_join_group_request(&join_req, &mut join_wire);
        let decoded_join = KafkaDecoder::decode_join_group_request(join_wire.freeze())
            .expect("JoinGroup decode failed");
        assert_eq!(decoded_join.group_id.as_ref(), b"payment-processor-group");
        assert_eq!(decoded_join.protocols.len(), 1);
        assert_eq!(decoded_join.protocols[0].name.as_ref(), b"roundrobin");
        assert_eq!(decoded_join.protocols[0].metadata.as_ref(), b"SUBSCRIPTION_TOPICS_ORDERS");

        // 3. SyncGroup
        let sync_req = SyncGroupRequest {
            header: RequestHeader {
                api_key: ApiKey::SyncGroup,
                api_version: 3,
                correlation_id: 303,
                client_id: Some(Bytes::from_static(b"consumer-client-1")),
            },
            group_id: Bytes::from_static(b"payment-processor-group"),
            generation_id: 1,
            member_id: Bytes::from_static(b"consumer-client-1-uuid123"),
            group_instance_id: None,
            protocol_type: Some(Bytes::from_static(b"consumer")),
            protocol_name: Some(Bytes::from_static(b"roundrobin")),
            assignments: vec![GroupAssignment {
                member_id: Bytes::from_static(b"consumer-client-1-uuid123"),
                assignment: Bytes::from_static(b"PARTITION_ASSIGNMENT_0_1_2"),
            }],
        };
        let mut sync_wire = BytesMut::with_capacity(256);
        KafkaEncoder::encode_sync_group_request(&sync_req, &mut sync_wire);
        let decoded_sync = KafkaDecoder::decode_sync_group_request(sync_wire.freeze())
            .expect("SyncGroup decode failed");
        assert_eq!(decoded_sync.generation_id, 1);
        assert_eq!(decoded_sync.assignments.len(), 1);
        assert_eq!(decoded_sync.assignments[0].assignment.as_ref(), b"PARTITION_ASSIGNMENT_0_1_2");

        // 4. Heartbeat
        let hb_req = HeartbeatRequest {
            header: RequestHeader {
                api_key: ApiKey::Heartbeat,
                api_version: 3,
                correlation_id: 304,
                client_id: Some(Bytes::from_static(b"consumer-client-1")),
            },
            group_id: Bytes::from_static(b"payment-processor-group"),
            generation_id: 1,
            member_id: Bytes::from_static(b"consumer-client-1-uuid123"),
            group_instance_id: None,
        };
        let mut hb_wire = BytesMut::with_capacity(128);
        KafkaEncoder::encode_heartbeat_request(&hb_req, &mut hb_wire);
        let decoded_hb = KafkaDecoder::decode_heartbeat_request(hb_wire.freeze())
            .expect("Heartbeat decode failed");
        assert_eq!(decoded_hb.group_id.as_ref(), b"payment-processor-group");
        assert_eq!(decoded_hb.generation_id, 1);

        // 5. OffsetCommit & OffsetFetch
        let commit_req = OffsetCommitRequest {
            header: RequestHeader {
                api_key: ApiKey::OffsetCommit,
                api_version: 6,
                correlation_id: 305,
                client_id: Some(Bytes::from_static(b"consumer-client-1")),
            },
            group_id: Bytes::from_static(b"payment-processor-group"),
            generation_id: 1,
            member_id: Bytes::from_static(b"consumer-client-1-uuid123"),
            retention_time_ms: 86400000,
            topics: vec![TopicOffsetCommit {
                topic: Bytes::from_static(b"orders-stream"),
                partitions: vec![PartitionOffsetCommit {
                    partition: 0,
                    offset: 9999,
                    leader_epoch: 1,
                    metadata: Some(Bytes::from_static(b"committed_by_node_1")),
                }],
            }],
        };
        let mut commit_wire = BytesMut::with_capacity(256);
        KafkaEncoder::encode_offset_commit_request(&commit_req, &mut commit_wire);
        let decoded_commit = KafkaDecoder::decode_offset_commit_request(commit_wire.freeze())
            .expect("OffsetCommit decode failed");
        assert_eq!(decoded_commit.topics[0].topic.as_ref(), b"orders-stream");
        assert_eq!(decoded_commit.topics[0].partitions[0].offset, 9999);
        assert_eq!(decoded_commit.topics[0].partitions[0].metadata.as_deref(), Some(b"committed_by_node_1".as_slice()));

        let fetch_req = OffsetFetchRequest {
            header: RequestHeader {
                api_key: ApiKey::OffsetFetch,
                api_version: 3,
                correlation_id: 306,
                client_id: Some(Bytes::from_static(b"consumer-client-1")),
            },
            group_id: Bytes::from_static(b"payment-processor-group"),
            topics: vec![TopicOffsetFetch {
                topic: Bytes::from_static(b"orders-stream"),
                partitions: vec![0],
            }],
        };
        let mut fetch_wire = BytesMut::with_capacity(128);
        KafkaEncoder::encode_offset_fetch_request(&fetch_req, &mut fetch_wire);
        let decoded_fetch = KafkaDecoder::decode_offset_fetch_request(fetch_wire.freeze())
            .expect("OffsetFetch decode failed");
        assert_eq!(decoded_fetch.topics[0].topic.as_ref(), b"orders-stream");
        assert_eq!(decoded_fetch.topics[0].partitions, vec![0]);
    }
}

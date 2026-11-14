//! VelocityMQ Configuration & Developer Mode - Phase 10
//!
//! Provides fast zero-dependency `.env` and environment variable configuration parsing,
//! supporting seamless toggling between strict 3-Node Distributed Raft and Standalone
//! Developer Mode (Dictator Mode).
//!
//! Strict Requirements:
//! 1. Fast configuration parser inspecting `.env` and environment variables.
//! 2. Automatic Developer Mode activation on `CLUSTER_MODE=false` or `REPLICATION_FACTOR=1`.
//! 3. Conversion helpers for Raft, WAL segmentation, Retention Reaper, and Network runtime.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crate::raft::RaftConfig;
use crate::reaper::RetentionPolicy;
use crate::segment::SegmentedLogConfig;
use crate::server::ServerConfig;
use crate::wal::LogSegmentConfig;

/// VelocityMQ Core System Configuration
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VelocityConfig {
    /// Node identifier within the cluster (1, 2, 3...)
    pub node_id: u64,
    /// Whether multi-node Raft clustering is enabled (default: true).
    pub cluster_mode: bool,
    /// Partition replication factor (1 = local standalone, 3 = quorum distributed).
    pub replication_factor: usize,
    /// Host interface to bind Kafka TCP listener (e.g. "0.0.0.0" or "127.0.0.1").
    pub kafka_host: String,
    /// Kafka TCP Wire Protocol port (default: 9092).
    pub kafka_port: u16,
    /// Host interface for Raft peer RPCs (e.g. "0.0.0.0" or "127.0.0.1").
    pub raft_host: String,
    /// Internal Raft binary TCP RPC port (default: 9093).
    pub raft_port: u16,
    /// List of Raft peer endpoints, e.g. "2@127.0.0.1:9094,3@127.0.0.1:9095".
    pub peers: Vec<String>,
    /// Base directory for WAL segments and binary indexes.
    pub data_dir: PathBuf,
    /// Number of dedicated hardware CPU cores / Event Loops (default: available CPUs or 4).
    pub num_cores: usize,
    /// Maximum size of an individual WAL segment in bytes before rolling (default: 500MB).
    pub log_segment_bytes: u64,
    /// Time-based log retention in hours (default: 168 hours = 7 days).
    pub log_retention_hours: u64,
    /// Optional high-resolution log retention in seconds (overrides hours if set, e.g. from LOG_RETENTION_MINUTES).
    pub log_retention_seconds: Option<u64>,
    /// Maximum retention byte ceiling across all partitions (default: unlimited).
    pub log_retention_bytes: u64,
    /// Size of pre-allocated page-aligned WAL write buffer in bytes (default: 64KB).
    pub buffer_size: usize,
    /// Maximum concurrent TCP client connections per core (default: 10,000).
    pub max_connections: usize,
    /// Whether AWS S3 Tiered Storage is enabled.
    pub tiered_storage_enabled: bool,
    /// Target AWS S3 Bucket for cold segment archiving.
    pub s3_bucket: Option<String>,
    /// AWS Region for S3 storage (e.g. "us-east-1").
    pub s3_region: Option<String>,
    /// Optional custom S3 endpoint URL (e.g. for MinIO / LocalStack).
    pub s3_endpoint: Option<String>,
    /// Object key prefix for S3 archives (default: "velocitymq").
    pub s3_prefix: String,
    /// Whether to force path style addressing on S3 requests.
    pub s3_force_path_style: bool,
    /// Whether Prometheus HTTP metrics server is enabled (default: true).
    pub metrics_enabled: bool,
    /// Host interface to bind Prometheus HTTP metrics listener (default: "0.0.0.0").
    pub metrics_host: String,
    /// Port for Prometheus HTTP metrics endpoint (default: 9093).
    pub metrics_port: u16,
}

impl Default for VelocityConfig {
    fn default() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(1);

        Self {
            node_id: 1,
            cluster_mode: true,
            replication_factor: 3,
            kafka_host: "0.0.0.0".to_string(),
            kafka_port: 9092,
            raft_host: "0.0.0.0".to_string(),
            raft_port: 9093,
            peers: vec![
                "2@127.0.0.1:9094".to_string(),
                "3@127.0.0.1:9095".to_string(),
            ],
            data_dir: PathBuf::from("data").join("wal"),
            num_cores: cores,
            log_segment_bytes: 500 * 1024 * 1024, // 500 MB
            log_retention_hours: 168,             // 7 days
            log_retention_seconds: None,
            log_retention_bytes: u64::MAX,        // Unlimited
            buffer_size: 64 * 1024,               // 64 KB
            max_connections: 10_000,
            tiered_storage_enabled: false,
            s3_bucket: None,
            s3_region: None,
            s3_endpoint: None,
            s3_prefix: "velocitymq".to_string(),
            s3_force_path_style: false,
            metrics_enabled: true,
            metrics_host: "0.0.0.0".to_string(),
            metrics_port: 9093,
        }
    }
}

impl VelocityConfig {
    /// Loads configuration from `.env` file (if present) and process environment variables.
    pub fn from_env() -> Self {
        // Fast .env parser
        let _ = Self::load_dotenv_file(".env");

        let mut config = Self::default();

        // 1. Cluster Mode & Replication Factor (Developer Mode triggers)
        if let Ok(val) = std::env::var("CLUSTER_MODE") {
            config.cluster_mode = parse_bool(&val);
        }

        if let Ok(val) = std::env::var("REPLICATION_FACTOR") {
            if let Ok(rf) = val.trim().parse::<usize>() {
                config.replication_factor = rf;
                if rf == 1 {
                    // REPLICATION_FACTOR=1 automatically activates Developer Mode
                    config.cluster_mode = false;
                }
            }
        }

        // If CLUSTER_MODE=false, automatically set replication_factor to 1 and clear peers
        if !config.cluster_mode {
            config.replication_factor = 1;
            config.peers.clear();
        }

        // 2. Node Identity
        if let Ok(val) = std::env::var("NODE_ID").or_else(|_| std::env::var("VELOCITY_NODE_ID")) {
            if let Ok(id) = val.trim().parse::<u64>() {
                config.node_id = id;
            }
        }

        // 3. Kafka Network Binding
        if let Ok(val) = std::env::var("KAFKA_HOST").or_else(|_| std::env::var("HOST")) {
            config.kafka_host = val.trim().to_string();
        }
        if let Ok(val) = std::env::var("KAFKA_PORT").or_else(|_| std::env::var("PORT")) {
            if let Ok(port) = val.trim().parse::<u16>() {
                config.kafka_port = port;
            }
        }

        // 4. Raft Network Binding & Peers
        if let Ok(val) = std::env::var("RAFT_HOST") {
            config.raft_host = val.trim().to_string();
        }
        if let Ok(val) = std::env::var("RAFT_PORT") {
            if let Ok(port) = val.trim().parse::<u16>() {
                config.raft_port = port;
            }
        }
        if let Ok(val) = std::env::var("PEERS").or_else(|_| std::env::var("CLUSTER_PEERS")) {
            let parsed_peers: Vec<String> = val
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !parsed_peers.is_empty() {
                config.peers = parsed_peers;
            }
        }

        // 5. Storage & Cores
        if let Ok(val) = std::env::var("DATA_DIR").or_else(|_| std::env::var("LOG_DIR")) {
            config.data_dir = PathBuf::from(val.trim());
        }
        if let Ok(val) = std::env::var("NUM_CORES").or_else(|_| std::env::var("VELOCITY_CORES")) {
            if let Ok(cores) = val.trim().parse::<usize>() {
                if cores > 0 {
                    config.num_cores = cores;
                }
            }
        }

        // 6. WAL Segmentation & Retention
        if let Ok(val) = std::env::var("LOG_SEGMENT_BYTES") {
            if let Ok(bytes) = val.trim().parse::<u64>() {
                config.log_segment_bytes = bytes;
            }
        }
        if let Ok(val) = std::env::var("LOG_RETENTION_HOURS") {
            if let Ok(hours) = val.trim().parse::<u64>() {
                config.log_retention_hours = hours;
            }
        }
        if let Ok(val) = std::env::var("LOG_RETENTION_MINUTES") {
            if let Ok(mins) = val.trim().parse::<u64>() {
                config.log_retention_seconds = Some(mins * 60);
            }
        }
        if let Ok(val) = std::env::var("LOG_RETENTION_SECONDS") {
            if let Ok(secs) = val.trim().parse::<u64>() {
                config.log_retention_seconds = Some(secs);
            }
        }
        if let Ok(val) = std::env::var("LOG_RETENTION_BYTES") {
            if let Ok(bytes) = val.trim().parse::<u64>() {
                config.log_retention_bytes = bytes;
            }
        }
        if let Ok(val) = std::env::var("MAX_CONNECTIONS") {
            if let Ok(conns) = val.trim().parse::<usize>() {
                config.max_connections = conns;
            }
        }

        // 7. AWS S3 Tiered Storage Configuration
        if let Ok(val) = std::env::var("S3_BUCKET").or_else(|_| std::env::var("AWS_S3_BUCKET")) {
            let b = val.trim().to_string();
            if !b.is_empty() {
                config.s3_bucket = Some(b);
            }
        }
        if let Ok(val) = std::env::var("AWS_REGION").or_else(|_| std::env::var("AWS_DEFAULT_REGION")) {
            let r = val.trim().to_string();
            if !r.is_empty() {
                config.s3_region = Some(r);
            }
        }
        if let Ok(val) = std::env::var("S3_ENDPOINT")
            .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
            .or_else(|_| std::env::var("AWS_ENDPOINT"))
        {
            let ep = val.trim().to_string();
            if !ep.is_empty() {
                config.s3_endpoint = Some(ep);
            }
        }
        if let Ok(val) = std::env::var("S3_PREFIX") {
            let p = val.trim().to_string();
            if !p.is_empty() {
                config.s3_prefix = p;
            }
        }
        if let Ok(val) = std::env::var("S3_FORCE_PATH_STYLE") {
            config.s3_force_path_style = parse_bool(&val);
        }
        if let Ok(val) = std::env::var("TIERED_STORAGE_ENABLED") {
            config.tiered_storage_enabled = parse_bool(&val);
        } else {
            // Auto-enable tiered storage if a bucket is explicitly configured
            config.tiered_storage_enabled = config.s3_bucket.is_some();
        }

        // 8. Prometheus Metrics Server Configuration
        if let Ok(val) = std::env::var("METRICS_ENABLED") {
            config.metrics_enabled = parse_bool(&val);
        }
        if let Ok(val) = std::env::var("METRICS_HOST") {
            let h = val.trim().to_string();
            if !h.is_empty() {
                config.metrics_host = h;
            }
        }
        if let Ok(val) = std::env::var("METRICS_PORT") {
            if let Ok(port) = val.trim().parse::<u16>() {
                config.metrics_port = port;
            }
        }

        config
    }

    /// Fast, zero-dependency `.env` file parser.
    /// Sets parsed values into process environment if not already present.
    pub fn load_dotenv_file<P: AsRef<Path>>(path: P) -> std::io::Result<usize> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };

        let reader = BufReader::new(file);
        let mut count = 0;

        for line in reader.lines() {
            let line = line?;
            let trimmed = line.trim();

            // Skip comments and empty lines
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            // Split on the first '='
            if let Some((key, val)) = trimmed.split_once('=') {
                let key = key.trim();
                let mut val = val.trim();

                // Strip outer quotes if present
                if (val.starts_with('"') && val.ends_with('"'))
                    || (val.starts_with('\'') && val.ends_with('\''))
                {
                    if val.len() >= 2 {
                        val = &val[1..val.len() - 1];
                    }
                }

                if !key.is_empty() {
                    // Only set if not already defined in environment
                    if std::env::var_os(key).is_none() {
                        unsafe {
                            std::env::set_var(key, val);
                        }
                        count += 1;
                    }
                }
            }
        }

        Ok(count)
    }

    /// Returns `true` if Developer Mode (Dictator Mode) is active.
    /// In Developer Mode, Raft consensus timeouts are bypassed, and writes commit locally.
    #[inline(always)]
    pub fn is_developer_mode(&self) -> bool {
        !self.cluster_mode || self.replication_factor == 1 || self.peers.is_empty()
    }

    /// Kafka TCP socket address string.
    #[inline(always)]
    pub fn kafka_addr(&self) -> String {
        format!("{}:{}", self.kafka_host, self.kafka_port)
    }

    /// Prometheus HTTP metrics socket address string.
    #[inline(always)]
    pub fn metrics_addr(&self) -> String {
        format!("{}:{}", self.metrics_host, self.metrics_port)
    }

    /// Raft internal binary RPC socket address string.
    #[inline(always)]
    pub fn raft_addr(&self) -> String {
        format!("{}:{}", self.raft_host, self.raft_port)
    }

    /// Generates a `RaftConfig` mapped from this system configuration.
    pub fn to_raft_config(&self) -> RaftConfig {
        let is_dev = self.is_developer_mode();
        let wal_path = self.data_dir.join(format!("raft_node_{}.wal", self.node_id));

        let peer_ids: Vec<u64> = if is_dev {
            Vec::new()
        } else {
            self.peers
                .iter()
                .filter_map(|p| {
                    p.split_once('@')
                        .and_then(|(id_str, _)| id_str.parse::<u64>().ok())
                })
                .collect()
        };

        RaftConfig {
            node_id: self.node_id,
            peers: peer_ids,
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            wal_path,
            wal_config: LogSegmentConfig {
                segment_id: self.node_id,
                max_segment_size: self.log_segment_bytes,
                buffer_size: self.buffer_size,
                ring_depth: 32,
                buffer_pool_size: 4,
                sync_on_flush: true,
            },
            developer_mode: is_dev,
        }
    }

    /// Generates a `SegmentedLogConfig` for a specific partition.
    pub fn to_segmented_log_config(&self, partition_id: &str) -> SegmentedLogConfig {
        SegmentedLogConfig {
            log_dir: self.data_dir.clone(),
            partition_id: partition_id.to_string(),
            max_segment_bytes: self.log_segment_bytes,
            buffer_size: self.buffer_size,
            ring_depth: 32,
            buffer_pool_size: 4,
            sync_on_flush: false,
            index_interval_bytes: 4096,
        }
    }

    /// Generates a `RetentionPolicy` mapped from this system configuration.
    pub fn to_retention_policy(&self) -> RetentionPolicy {
        let mut policy = RetentionPolicy::default()
            .with_retention_hours(self.log_retention_hours)
            .with_retention_bytes(self.log_retention_bytes);
        if let Some(secs) = self.log_retention_seconds {
            policy = policy.with_retention_seconds(secs);
        }
        policy
    }

    /// Generates an `S3ArchiverConfig` if tiered storage is enabled.
    pub fn to_s3_archiver_config(&self) -> Option<crate::s3_archiver::S3ArchiverConfig> {
        if !self.tiered_storage_enabled {
            return None;
        }
        let bucket = self.s3_bucket.as_ref()?.clone();
        if bucket.trim().is_empty() {
            return None;
        }
        Some(crate::s3_archiver::S3ArchiverConfig {
            bucket,
            region: self.s3_region.clone(),
            endpoint_url: self.s3_endpoint.clone(),
            prefix: self.s3_prefix.clone(),
            force_path_style: self.s3_force_path_style,
        })
    }

    /// Generates a `ServerConfig` for the given hardware core.
    pub fn to_server_config(&self, core_id: usize) -> ServerConfig {
        let addr: SocketAddr = self
            .kafka_addr()
            .parse()
            .unwrap_or_else(|_| "0.0.0.0:9092".parse().unwrap());

        ServerConfig {
            bind_addr: addr,
            core_id,
            max_connections: self.max_connections,
            slot_size: 4096,
            ring_depth: 1024,
        }
    }

    /// Returns a high-performance formatted startup banner.
    pub fn banner(&self) -> String {
        let mode_desc = if self.is_developer_mode() {
            "[STANDALONE DEVELOPER MODE / DICTATOR MODE]"
        } else {
            "[3-NODE DISTRIBUTED RAFT CLUSTER]"
        };

        let replication_desc = if self.is_developer_mode() {
            "1 (Standalone: acks=all resolves locally in <5µs)"
        } else {
            "3 (Quorum: 2/3 NVMe commit required)"
        };

        let retention_desc = if let Some(secs) = self.log_retention_seconds {
            if secs % 60 == 0 {
                format!("{} Minutes", secs / 60)
            } else {
                format!("{} Seconds", secs)
            }
        } else {
            format!("{} Hours", self.log_retention_hours)
        };

        format!(
            r#"
================================================================================
  __      __  _            _ _         __  __  ____  
  \ \    / / | |          (_) |       |  \/  |/ __ \ 
   \ \  / /__| | ___   ___ _| |_ _   _| \  / | |  | |
    \ \/ / _ \ |/ _ \ / __| | __| | | | |\/| | |  | |
     \  /  __/ | (_) | (__| | |_| |_| | |  | | |__| |
      \/ \___|_|\___/ \___|_|\__|\__, |_|  |_|\___\_\
                                  __/ |              
                                 |___/               
  VelocityMQ - Next-Generation Distributed Event Streaming Platform (Rust)
================================================================================
  Status:             RUNNING
  Execution Mode:     {}
  Node ID:            {}
  Kafka TCP Listener: {}
  Raft RPC Listener:  {}
  Storage Path:       {}
  Active CPU Cores:   {} (Thread-per-Core Event Loops)
  Replication Factor: {}
  Segment Rolling:    {} MB
  Retention Policy:   {} / {}
  Tiered Storage:     {}
  Prometheus Metrics: {}
================================================================================
"#,
            mode_desc,
            self.node_id,
            self.kafka_addr(),
            if self.is_developer_mode() {
                "DISABLED (Bypassed in Developer Mode)".to_string()
            } else {
                self.raft_addr()
            },
            self.data_dir.display(),
            self.num_cores,
            replication_desc,
            self.log_segment_bytes / (1024 * 1024),
            retention_desc,
            if self.log_retention_bytes == u64::MAX {
                "UNLIMITED Bytes".to_string()
            } else {
                format!("{} MB Limit", self.log_retention_bytes / (1024 * 1024))
            },
            if self.tiered_storage_enabled {
                format!(
                    "AWS S3 ENABLED (Bucket: '{}', Prefix: '{}')",
                    self.s3_bucket.as_deref().unwrap_or("unknown"),
                    self.s3_prefix
                )
            } else {
                "DISABLED (Local NVMe Retention Only)".to_string()
            },
            if self.metrics_enabled {
                format!("http://{}/metrics", self.metrics_addr())
            } else {
                "DISABLED".to_string()
            }
        )
    }
}

/// Helper to parse loose boolean values.
fn parse_bool(val: &str) -> bool {
    let lower = val.trim().to_lowercase();
    match lower.as_str() {
        "false" | "0" | "no" | "f" | "off" | "disabled" => false,
        _ => true,
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_cluster_mode() {
        let config = VelocityConfig::default();
        assert!(config.cluster_mode);
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.node_id, 1);
        assert_eq!(config.kafka_port, 9092);
        assert_eq!(config.raft_port, 9093);
        assert!(!config.is_developer_mode());
    }

    #[test]
    fn test_developer_mode_via_replication_factor_1() {
        let mut config = VelocityConfig::default();
        config.replication_factor = 1;
        config.cluster_mode = false;
        config.peers.clear();

        assert!(config.is_developer_mode());
        let raft_cfg = config.to_raft_config();
        assert!(raft_cfg.is_developer_mode());
        assert!(raft_cfg.peers.is_empty());
    }

    #[test]
    fn test_parse_bool_variants() {
        assert!(!parse_bool("false"));
        assert!(!parse_bool("0"));
        assert!(!parse_bool("no"));
        assert!(!parse_bool("OFF"));
        assert!(!parse_bool("disabled"));
        assert!(parse_bool("true"));
        assert!(parse_bool("1"));
        assert!(parse_bool("yes"));
    }

    #[test]
    fn test_fast_dotenv_parser() {
        let temp_dir = std::env::temp_dir().join(format!("velmq_dotenv_test_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);
        let env_file = temp_dir.join(".env");

        let content = r#"
# Sample VelocityMQ Configuration
CUSTOM_CLUSTER_MODE=false
CUSTOM_REPLICATION_FACTOR=1
CUSTOM_KAFKA_PORT=9099
DATA_DIR=/var/velocitymq/wal
"#;
        std::fs::write(&env_file, content).unwrap();

        let count = VelocityConfig::load_dotenv_file(&env_file).unwrap();
        assert!(count >= 3);

        assert_eq!(std::env::var("CUSTOM_CLUSTER_MODE").unwrap(), "false");
        assert_eq!(std::env::var("CUSTOM_REPLICATION_FACTOR").unwrap(), "1");
        assert_eq!(std::env::var("CUSTOM_KAFKA_PORT").unwrap(), "9099");

        unsafe {
            std::env::remove_var("CUSTOM_CLUSTER_MODE");
            std::env::remove_var("CUSTOM_REPLICATION_FACTOR");
            std::env::remove_var("CUSTOM_KAFKA_PORT");
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_container_docker_env_configuration() {
        unsafe {
            std::env::set_var("HOST", "0.0.0.0");
            std::env::set_var("PORT", "9092");
            std::env::set_var("CLUSTER_MODE", "false");
            std::env::set_var("LOG_RETENTION_HOURS", "168");
            std::env::set_var("DATA_DIR", "/var/lib/velocitymq/data");
            std::env::set_var("METRICS_ENABLED", "true");
            std::env::set_var("METRICS_HOST", "0.0.0.0");
            std::env::set_var("METRICS_PORT", "9093");
        }

        let config = VelocityConfig::from_env();
        assert_eq!(config.kafka_host, "0.0.0.0");
        assert_eq!(config.kafka_port, 9092);
        assert_eq!(config.kafka_addr(), "0.0.0.0:9092");
        assert!(!config.cluster_mode);
        assert!(config.is_developer_mode());
        assert_eq!(config.log_retention_hours, 168);
        assert_eq!(config.data_dir, PathBuf::from("/var/lib/velocitymq/data"));
        assert!(config.metrics_enabled);
        assert_eq!(config.metrics_host, "0.0.0.0");
        assert_eq!(config.metrics_port, 9093);
        assert_eq!(config.metrics_addr(), "0.0.0.0:9093");

        unsafe {
            std::env::remove_var("HOST");
            std::env::remove_var("PORT");
            std::env::remove_var("CLUSTER_MODE");
            std::env::remove_var("LOG_RETENTION_HOURS");
            std::env::remove_var("DATA_DIR");
            std::env::remove_var("METRICS_ENABLED");
            std::env::remove_var("METRICS_HOST");
            std::env::remove_var("METRICS_PORT");
        }
    }
}

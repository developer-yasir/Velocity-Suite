//! VelocityMQ S3 Tiered Storage Archiver - Phase 14
//!
//! Provides zero-heap-churn streaming uploads of expired and sealed WAL segments
//! to AWS S3 (or S3-compatible object stores like MinIO / LocalStack) before unlinking
//! them from local NVMe SSD storage.
//!
//! Architectural Principles:
//! 1. Zero-Heap Bounded Memory Streaming: Streams multi-gigabyte `.log` and `.index`
//!    files in chunks via `aws_sdk_s3::primitives::ByteStream::from_path`. Never buffers full segments in RAM.
//! 2. Strong Fault Tolerance: Guarantees non-empty ETag validation before any local file deletion.
//! 3. Environment Variable Autoconfiguration: Reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
//!    `AWS_REGION`, `S3_BUCKET`, `S3_ENDPOINT`, `S3_PREFIX` seamlessly.
//! 4. Asynchronous Non-Blocking Execution: Designed to run as detached background Tokio tasks.

use std::fmt;
use std::path::Path;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

/// Errors returned during S3 tiered storage operations.
#[derive(Debug)]
pub enum S3ArchiveError {
    /// Local filesystem I/O error.
    IoError(std::io::Error),
    /// Failed to initialize chunked ByteStream.
    ByteStreamError(String),
    /// S3 PutObject or GetObject API failure.
    UploadError(String),
    /// S3 response missing valid ETag verification.
    MissingETag(String),
    /// Invalid or missing configuration.
    ConfigError(String),
}

impl fmt::Display for S3ArchiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoError(e) => write!(f, "S3 Archiver I/O error: {}", e),
            Self::ByteStreamError(s) => write!(f, "S3 Archiver ByteStream error: {}", s),
            Self::UploadError(s) => write!(f, "S3 Archiver upload error: {}", s),
            Self::MissingETag(s) => write!(f, "S3 Archiver ETag validation error: {}", s),
            Self::ConfigError(s) => write!(f, "S3 Archiver config error: {}", s),
        }
    }
}

impl std::error::Error for S3ArchiveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IoError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for S3ArchiveError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

/// Configuration parameters for AWS S3 Tiered Storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ArchiverConfig {
    /// Target AWS S3 Bucket (e.g., "velocitymq-cold-archive").
    pub bucket: String,
    /// AWS Region (e.g., "us-east-1", "eu-west-1").
    pub region: Option<String>,
    /// Custom S3 endpoint URL (e.g., "http://localhost:9000" for MinIO / LocalStack).
    pub endpoint_url: Option<String>,
    /// S3 object key prefix (default: "velocitymq").
    pub prefix: String,
    /// Whether to force path style addressing (useful for MinIO / LocalStack).
    pub force_path_style: bool,
}

impl Default for S3ArchiverConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            region: None,
            endpoint_url: None,
            prefix: "velocitymq".to_string(),
            force_path_style: false,
        }
    }
}

impl S3ArchiverConfig {
    /// Loads S3 configuration from environment variables.
    pub fn from_env() -> Self {
        let bucket = std::env::var("S3_BUCKET")
            .or_else(|_| std::env::var("AWS_S3_BUCKET"))
            .unwrap_or_default()
            .trim()
            .to_string();

        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let endpoint_url = std::env::var("S3_ENDPOINT")
            .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
            .or_else(|_| std::env::var("AWS_ENDPOINT"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let prefix = std::env::var("S3_PREFIX")
            .unwrap_or_else(|_| "velocitymq".to_string())
            .trim()
            .to_string();

        let force_path_style = std::env::var("S3_FORCE_PATH_STYLE")
            .map(|v| match v.trim().to_lowercase().as_str() {
                "true" | "1" | "yes" => true,
                _ => false,
            })
            .unwrap_or(false);

        Self {
            bucket,
            region,
            endpoint_url,
            prefix,
            force_path_style,
        }
    }

    /// Returns `true` if S3 archiving is enabled with a valid bucket name.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        !self.bucket.trim().is_empty()
    }
}

/// S3 Archiver client responsible for streaming expired WAL segments to AWS S3.
#[derive(Clone)]
pub struct S3Archiver {
    pub config: S3ArchiverConfig,
    client: Client,
}

impl S3Archiver {
    /// Initializes an S3Archiver using the provided configuration and standard AWS credentials.
    pub async fn new(config: S3ArchiverConfig) -> Result<Self, S3ArchiveError> {
        if !config.is_enabled() {
            return Err(S3ArchiveError::ConfigError(
                "S3 bucket name cannot be empty. Set S3_BUCKET or AWS_S3_BUCKET.".to_string(),
            ));
        }

        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(ref region) = config.region {
            config_loader = config_loader.region(aws_config::Region::new(region.clone()));
        }
        if let Some(ref endpoint) = config.endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint.clone());
        }

        let sdk_config = config_loader.load().await;

        let mut s3_conf_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if config.force_path_style {
            s3_conf_builder = s3_conf_builder.force_path_style(true);
        }

        let client = Client::from_conf(s3_conf_builder.build());

        Ok(Self { config, client })
    }

    /// Initializes S3Archiver with a pre-configured `aws_sdk_s3::Client` (useful for unit testing and custom clients).
    pub fn from_client(config: S3ArchiverConfig, client: Client) -> Self {
        Self { config, client }
    }

    /// Formats the S3 object key for a segment artifact:
    /// e.g. `{prefix}/topics/{partition_id}/{base_offset:020}.log`
    pub fn format_s3_key(&self, partition_id: &str, base_offset: u64, ext: &str) -> String {
        let filename = format!("{:020}.{}", base_offset, ext.trim_start_matches('.'));
        if self.config.prefix.is_empty() {
            format!("topics/{}/{}", partition_id, filename)
        } else {
            format!(
                "{}/topics/{}/{}",
                self.config.prefix.trim_matches('/'),
                partition_id,
                filename
            )
        }
    }

    /// Streams a single local file to S3 using `ByteStream::from_path`.
    ///
    /// Crucial Invariant: The file is streamed in chunks directly from NVMe storage;
    /// it is NEVER fully buffered into heap RAM.
    /// Returns the verified S3 ETag on success.
    pub async fn upload_file<P: AsRef<Path>>(
        &self,
        file_path: P,
        s3_key: &str,
    ) -> Result<String, S3ArchiveError> {
        let path = file_path.as_ref();
        if !path.exists() {
            return Err(S3ArchiveError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Local segment file not found: {}", path.display()),
            )));
        }

        // Stream from disk in chunks using ByteStream::from_path
        let body = ByteStream::from_path(path)
            .await
            .map_err(|e| S3ArchiveError::ByteStreamError(format!("Failed to stream {}: {:?}", path.display(), e)))?;

        let resp = self
            .client
            .put_object()
            .bucket(&self.config.bucket)
            .key(s3_key)
            .body(body)
            .send()
            .await
            .map_err(|e| S3ArchiveError::UploadError(format!("PutObject failed for key '{}': {:?}", s3_key, e)))?;

        let etag = resp
            .e_tag()
            .ok_or_else(|| S3ArchiveError::MissingETag(format!("Missing ETag in S3 response for key '{}'", s3_key)))?;

        let trimmed_etag = etag.trim().trim_matches('"');
        if trimmed_etag.is_empty() {
            return Err(S3ArchiveError::MissingETag(format!(
                "Empty ETag returned from S3 for key '{}'",
                s3_key
            )));
        }

        Ok(trimmed_etag.to_string())
    }

    /// Streams both `.log` and `.index` for a sealed segment to S3.
    ///
    /// Returns `(log_etag, index_etag)` if and only if both uploads succeed and return ETags.
    pub async fn archive_segment(
        &self,
        partition_id: &str,
        base_offset: u64,
        log_path: &Path,
        index_path: &Path,
    ) -> Result<(String, String), S3ArchiveError> {
        let log_key = self.format_s3_key(partition_id, base_offset, "log");
        let index_key = self.format_s3_key(partition_id, base_offset, "index");

        // 1. Stream the .log file
        let log_etag = self.upload_file(log_path, &log_key).await?;

        // 2. Stream the .index file (if present)
        let index_etag = if index_path.exists() {
            self.upload_file(index_path, &index_key).await?
        } else {
            "omitted".to_string()
        };

        Ok((log_etag, index_etag))
    }

    /// Streams an archived segment back from S3 as a chunked `ByteStream`.
    pub async fn get_segment_byte_stream(
        &self,
        partition_id: &str,
        base_offset: u64,
        ext: &str,
    ) -> Result<ByteStream, S3ArchiveError> {
        let s3_key = self.format_s3_key(partition_id, base_offset, ext);

        let resp = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(&s3_key)
            .send()
            .await
            .map_err(|e| S3ArchiveError::UploadError(format!("GetObject failed for key '{}': {:?}", s3_key, e)))?;

        Ok(resp.body)
    }

    /// Checks if a segment exists in S3 (via HeadObject).
    pub async fn segment_exists(
        &self,
        partition_id: &str,
        base_offset: u64,
        ext: &str,
    ) -> bool {
        let s3_key = self.format_s3_key(partition_id, base_offset, ext);
        self.client
            .head_object()
            .bucket(&self.config.bucket)
            .key(&s3_key)
            .send()
            .await
            .is_ok()
    }
}

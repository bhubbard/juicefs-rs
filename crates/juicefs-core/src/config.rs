//! Volume format and client/chunk configuration structures.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::DEFAULT_BLOCK_SIZE;

/// Persistent volume format stored in metadata under the `format` setting key.
///
/// Field names use `PascalCase` JSON keys to remain compatible with Go `JuiceFS`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Format {
    /// Volume name (used as object storage prefix).
    pub name: String,
    /// Unique volume UUID.
    #[serde(rename = "UUID")]
    pub uuid: String,
    /// Storage backend type (`file`, `s3`, `mem`, etc.).
    pub storage: String,
    /// Bucket URL or local root path.
    pub bucket: String,
    /// Optional access key.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub access_key: String,
    /// Optional secret key.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub secret_key: String,
    /// Optional session token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub session_token: String,
    /// Block size in bytes (default 4 MiB).
    pub block_size: usize,
    /// Compression algorithm (`none`, `lz4`, `zstd`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub compression: String,
    /// Whether to use `chunks/<id%256:02X>/...` hash prefixing.
    #[serde(default)]
    pub hash_prefix: bool,
    /// Optional capacity limit in bytes (`0` = unlimited).
    #[serde(default)]
    pub capacity: u64,
    /// Optional inode limit (`0` = unlimited).
    #[serde(default)]
    pub inodes: u64,
    /// Number of days to retain deleted files in `.trash`.
    #[serde(default = "default_trash_days")]
    pub trash_days: i32,
    /// Metadata schema version.
    #[serde(default = "default_meta_version")]
    pub meta_version: i32,
}

const fn default_trash_days() -> i32 {
    1
}

const fn default_meta_version() -> i32 {
    1
}

impl Format {
    /// Creates a new [`Format`] with sensible defaults and a fresh UUID.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        storage: impl Into<String>,
        bucket: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            uuid: Uuid::new_v4().to_string(),
            storage: storage.into(),
            bucket: bucket.into(),
            access_key: String::new(),
            secret_key: String::new(),
            session_token: String::new(),
            block_size: DEFAULT_BLOCK_SIZE,
            compression: "none".to_owned(),
            hash_prefix: false,
            capacity: 0,
            inodes: 0,
            trash_days: 1,
            meta_version: 1,
        }
    }
}

/// Runtime metadata client configuration.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Number of retries for transient metadata errors.
    pub retries: usize,
    /// Mount point or client label.
    pub mount_point: String,
    /// Read-only mount flag.
    pub read_only: bool,
    /// Heartbeat interval.
    pub heartbeat: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            retries: 10,
            mount_point: "juicefs-rs".to_owned(),
            read_only: false,
            heartbeat: Duration::from_secs(12),
        }
    }
}

/// Configuration for the chunk and block cache store.
#[derive(Debug, Clone)]
pub struct ChunkConfig {
    /// Block size in bytes (must match [`Format::block_size`]).
    pub block_size: usize,
    /// Compression algorithm.
    pub compression: String,
    /// Whether object keys use hash prefixing.
    pub hash_prefix: bool,
    /// Optional local disk cache directory.
    pub cache_dir: Option<PathBuf>,
    /// Maximum disk cache size in bytes.
    pub cache_size: u64,
    /// Maximum in-memory page cache bytes.
    pub buffer_size: usize,
    /// Writeback mode enabled.
    pub writeback: bool,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            compression: "none".to_owned(),
            hash_prefix: false,
            cache_dir: None,
            cache_size: 100 << 20,  // 100 MiB
            buffer_size: 300 << 20, // 300 MiB
            writeback: false,
        }
    }
}

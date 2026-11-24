//! Object storage abstraction and backends (`Memory`, `File`, `S3`, `Gcs`, `Azure`, `Encrypted`, `Sharded`) for `JuiceFS`.

pub mod cloud;
pub mod encrypt;
pub mod error;
pub mod file;
pub mod memory;
pub mod prefixed;
pub mod s3;
pub mod sharded;

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use juicefs_core::Format;

pub use cloud::{AzureStore, GcsStore};
pub use encrypt::EncryptedStore;
pub use error::ObjectError;
pub use file::FileStore;
pub use memory::MemoryStore;
pub use prefixed::PrefixedStore;
pub use s3::S3Store;
pub use sharded::ShardedStore;

/// Metadata for a stored object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Object key relative to the store root.
    pub key: String,
    /// Size in bytes.
    pub size: u64,
    /// Last modification timestamp.
    pub mtime: SystemTime,
    /// Whether this entry represents a directory marker.
    pub is_dir: bool,
}

/// Capability and part-size limits of an [`ObjectStorage`] backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Limits {
    /// Whether the backend supports multipart uploads.
    pub is_support_multipart_upload: bool,
    /// Whether the backend supports server-side part copying.
    pub is_support_upload_part_copy: bool,
    /// Minimum size in bytes for non-trailing multipart parts.
    pub min_part_size: u64,
    /// Maximum size in bytes for a single multipart part (`0` if unspecified).
    pub max_part_size: u64,
    /// Maximum number of parts allowed in one upload (`0` if unspecified).
    pub max_part_count: u64,
}

/// Session metadata returned when initiating a multipart upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartUpload {
    /// Backend-assigned identifier for this multipart upload.
    pub upload_id: String,
    /// Minimum size in bytes for non-trailing parts.
    pub min_part_size: u64,
    /// Maximum number of parts allowed.
    pub max_count: u64,
}

/// Metadata describing a single uploaded part in a multipart upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Part {
    /// 1-based part number.
    pub num: usize,
    /// Size of the part payload in bytes.
    pub size: usize,
    /// Entity tag / digest identifying the part payload.
    pub etag: String,
}

/// Async object storage interface modeled after `JuiceFS` `pkg/object.ObjectStorage`.
#[async_trait]
pub trait ObjectStorage: Send + Sync + std::fmt::Debug {
    /// Human-readable description of this backend.
    fn description(&self) -> String;

    /// Returns capability and size limits for this backend.
    fn limits(&self) -> Limits {
        Limits::default()
    }

    /// Ensures the underlying bucket or root directory exists.
    async fn create(&self) -> Result<(), ObjectError>;

    /// Reads `limit` bytes (or until EOF if `limit == 0`) starting at `offset`.
    async fn get(&self, key: &str, offset: u64, limit: usize) -> Result<Bytes, ObjectError>;

    /// Writes `data` to `key`, replacing any existing object.
    async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectError>;

    /// Returns metadata for `key`, or [`ObjectError::NotFound`].
    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectError>;

    /// Deletes `key` (idempotent: succeeding if already absent).
    async fn delete(&self, key: &str) -> Result<(), ObjectError>;

    /// Copies `src` to `dst`.
    async fn copy(&self, dst: &str, src: &str) -> Result<(), ObjectError>;

    /// Lists all objects whose key starts with `prefix`.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, ObjectError>;

    /// Initiates a multipart upload for `key`.
    async fn create_multipart_upload(&self, _key: &str) -> Result<MultipartUpload, ObjectError> {
        Err(ObjectError::Unsupported(
            "multipart upload is not supported".to_owned(),
        ))
    }

    /// Uploads a single part (`num` is 1-based) for an active multipart upload.
    async fn upload_part(
        &self,
        _key: &str,
        _upload_id: &str,
        _num: usize,
        _data: Bytes,
    ) -> Result<Part, ObjectError> {
        Err(ObjectError::Unsupported(
            "multipart upload is not supported".to_owned(),
        ))
    }

    /// Completes an active multipart upload by assembling `parts` in ascending order.
    async fn complete_upload(
        &self,
        _key: &str,
        _upload_id: &str,
        _parts: &[Part],
    ) -> Result<(), ObjectError> {
        Err(ObjectError::Unsupported(
            "multipart upload is not supported".to_owned(),
        ))
    }

    /// Aborts an active multipart upload and cleans up any staged parts.
    async fn abort_upload(&self, _key: &str, _upload_id: &str) -> Result<(), ObjectError> {
        Err(ObjectError::Unsupported(
            "multipart upload is not supported".to_owned(),
        ))
    }
}

/// Validates an object key against path traversal and empty segments.
pub fn validate_key(key: &str) -> Result<(), ObjectError> {
    if key.is_empty() || key.starts_with('/') || key.contains("..") {
        return Err(ObjectError::InvalidKey(key.to_owned()));
    }
    Ok(())
}

/// Computes a lowercase hexadecimal SHA-256 digest for part entity tags.
pub(crate) fn sha256_hex(data: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, data);
    let mut out = String::with_capacity(digest.as_ref().len() * 2);
    for byte in digest.as_ref() {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Validates that `parts` is non-empty and sorted in strictly ascending `num` order.
pub(crate) fn validate_parts_order(parts: &[Part]) -> Result<(), ObjectError> {
    if parts.is_empty() {
        return Err(ObjectError::Backend(
            "cannot complete multipart upload with zero parts".to_owned(),
        ));
    }
    let mut prev = 0usize;
    for part in parts {
        if part.num <= prev {
            return Err(ObjectError::Backend(format!(
                "multipart parts must be in strictly ascending order (got {} after {prev})",
                part.num
            )));
        }
        prev = part.num;
    }
    Ok(())
}

/// Constructs an [`ObjectStorage`] from a volume [`Format`], wrapped with `<name>/` prefixing.
pub fn open_from_format(format: &Format) -> Result<Arc<dyn ObjectStorage>, ObjectError> {
    let base: Arc<dyn ObjectStorage> = match format.storage.to_ascii_lowercase().as_str() {
        "mem" | "memory" => Arc::new(MemoryStore::new()),
        "file" | "local" => {
            let root = format
                .bucket
                .strip_prefix("file://")
                .unwrap_or(&format.bucket);
            Arc::new(FileStore::new(root)?)
        }
        "s3" | "oss" | "cos" | "minio" | "b2" | "obs" | "bos" | "tos" => {
            Arc::new(S3Store::from_format(format)?)
        }
        "gs" | "gcs" => Arc::new(GcsStore::from_format(format)?),
        "azblob" | "azure" | "wasb" => Arc::new(AzureStore::from_format(format)?),
        other => {
            return Err(ObjectError::UnsupportedBackend(other.to_owned()));
        }
    };

    if format.name.is_empty() {
        Ok(base)
    } else {
        Ok(Arc::new(PrefixedStore::new(base, &format.name)))
    }
}

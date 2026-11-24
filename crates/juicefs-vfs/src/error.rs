//! Error types for `juicefs-vfs`.

use thiserror::Error;

/// Errors returned by VFS operations.
#[derive(Debug, Error)]
pub enum VfsError {
    /// Core domain error.
    #[error(transparent)]
    Core(#[from] juicefs_core::CoreError),

    /// Metadata engine error.
    #[error(transparent)]
    Meta(#[from] juicefs_meta::MetaError),

    /// Chunk store error.
    #[error(transparent)]
    Chunk(#[from] juicefs_chunk::ChunkError),

    /// Object storage error.
    #[error(transparent)]
    Object(#[from] juicefs_object::ObjectError),

    /// Invalid filesystem path.
    #[error("invalid path: {0}")]
    InvalidPath(String),

    /// Too many symbolic link levels.
    #[error("too many levels of symbolic links")]
    TooManySymlinks,

    /// Permission denied (`EACCES`).
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// Malformed POSIX ACL payload.
    #[error("invalid ACL: {0}")]
    InvalidAcl(String),

    /// JSON serialization/deserialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Local I/O error.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

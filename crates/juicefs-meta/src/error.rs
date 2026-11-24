//! Error types for `juicefs-meta`.

use thiserror::Error;

/// Errors returned by metadata engines.
#[derive(Debug, Error)]
pub enum MetaError {
    /// Target path or inode does not exist (`ENOENT`).
    #[error("no such file or directory: {0}")]
    NotFound(String),

    /// Target already exists (`EEXIST`).
    #[error("file exists: {0}")]
    AlreadyExists(String),

    /// Expected a directory (`ENOTDIR`).
    #[error("not a directory: {0}")]
    NotDir(String),

    /// Operation not permitted on a directory (`EISDIR`).
    #[error("is a directory: {0}")]
    IsDir(String),

    /// Directory is not empty (`ENOTEMPTY`).
    #[error("directory not empty: {0}")]
    NotEmpty(String),

    /// Invalid argument (`EINVAL`).
    #[error("invalid argument: {0}")]
    InvalidInput(String),

    /// Volume has not been formatted yet.
    #[error("volume is not formatted")]
    NotFormatted,

    /// Lock contention (`EAGAIN` / `EWOULDBLOCK`).
    #[error("resource temporarily unavailable: {0}")]
    WouldBlock(String),

    /// Directory quota exceeded (`EDQUOT`).
    #[error("quota exceeded: {0}")]
    QuotaExceeded(String),

    /// Core domain error.
    #[error(transparent)]
    Core(#[from] juicefs_core::CoreError),

    /// SQLite database error.
    #[error("database error: {0}")]
    Sql(#[from] rusqlite::Error),

    /// JSON serialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Background task join error.
    #[error("task error: {0}")]
    Task(String),
}

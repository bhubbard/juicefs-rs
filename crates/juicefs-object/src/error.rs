//! Error types for `juicefs-object`.

use thiserror::Error;

/// Errors returned by object storage backends.
#[derive(Debug, Error)]
pub enum ObjectError {
    /// Object was not found.
    #[error("object not found: {0}")]
    NotFound(String),

    /// Invalid or unsafe object key.
    #[error("invalid object key: {0}")]
    InvalidKey(String),

    /// Requested byte range is out of bounds.
    #[error("offset {offset} out of bounds for object of size {size}")]
    InvalidRange {
        /// Requested offset.
        offset: u64,
        /// Actual object size.
        size: u64,
    },

    /// Unsupported storage backend name.
    #[error("unsupported object storage backend: {0}")]
    UnsupportedBackend(String),

    /// Operation is not supported by this backend.
    #[error("unsupported operation: {0}")]
    Unsupported(String),

    /// Encryption or decryption failure.
    #[error("crypto error: {0}")]
    Crypto(String),

    /// Local filesystem I/O error.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// Remote backend error.
    #[error("backend error: {0}")]
    Backend(String),
}

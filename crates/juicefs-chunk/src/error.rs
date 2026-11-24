//! Error types for `juicefs-chunk`.

use std::sync::Arc;

use thiserror::Error;

/// Errors returned by chunk and block cache operations.
#[derive(Debug, Error)]
pub enum ChunkError {
    /// Core domain or compression error.
    #[error(transparent)]
    Core(#[from] juicefs_core::CoreError),

    /// Underlying object storage error.
    #[error(transparent)]
    Object(#[from] juicefs_object::ObjectError),

    /// Local disk cache I/O error.
    #[error("cache i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// Read offset was beyond slice length.
    #[error("offset {offset} beyond slice length {length}")]
    OutOfBounds {
        /// Requested offset.
        offset: usize,
        /// Total slice length.
        length: usize,
    },

    /// Error shared across concurrent deduplicated callers in `SingleFlight`.
    #[error(transparent)]
    Shared(#[from] Arc<ChunkError>),
}

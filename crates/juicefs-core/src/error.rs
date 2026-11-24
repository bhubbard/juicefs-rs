//! Core error types for `juicefs-core`.

use thiserror::Error;

/// Errors produced by core domain operations.
#[derive(Debug, Error)]
pub enum CoreError {
    /// Invalid node type byte.
    #[error("invalid node type: {0}")]
    InvalidNodeType(u8),

    /// Corrupt binary slice buffer.
    #[error("corrupt slice buffer: length {0} is not a multiple of 24")]
    CorruptSliceBuffer(usize),

    /// Unsupported compression algorithm.
    #[error("unsupported compression algorithm: {0}")]
    UnsupportedCompression(String),

    /// Compression or decompression failure.
    #[error("compression error: {0}")]
    Compression(String),

    /// Invalid configuration or format.
    #[error("invalid format: {0}")]
    InvalidFormat(String),
}

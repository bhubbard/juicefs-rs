//! Error types for `juicefs-webdav`.

use thiserror::Error;

/// Errors returned by the `WebDAV` server.
#[derive(Debug, Error)]
pub enum WebDavError {
    /// Underlying VFS error.
    #[error("vfs error: {0}")]
    Vfs(#[from] juicefs_vfs::VfsError),

    /// Metadata engine error.
    #[error("metadata error: {0}")]
    Meta(#[from] juicefs_meta::MetaError),

    /// Network or local I/O error.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// HTTP protocol error.
    #[error("http error: {0}")]
    Http(#[from] http::Error),
}

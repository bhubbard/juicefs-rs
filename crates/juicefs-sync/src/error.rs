//! Error types for `juicefs-sync`.

use thiserror::Error;

/// Errors returned by object synchronization operations.
#[derive(Debug, Error)]
pub enum SyncError {
    /// Underlying object storage error.
    #[error("object storage error: {0}")]
    Object(#[from] juicefs_object::ObjectError),

    /// Background sync task failed to join.
    #[error("sync task failed: {0}")]
    Task(String),
}

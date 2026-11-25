//! FUSE error types and POSIX `libc` errno mapping.

use std::io::ErrorKind;

use juicefs_meta::MetaError;
use juicefs_vfs::VfsError;
use thiserror::Error;

/// Errors returned by [`crate::JuiceFsFuse`] and mount/unmount helpers.
#[derive(Debug, Error)]
pub enum FuseError {
    /// Error carrying an explicit POSIX `errno` value.
    #[error("fuse posix error (errno {errno}): {message}")]
    Posix {
        /// POSIX `libc` error number (e.g., `libc::ENOENT`).
        errno: libc::c_int,
        /// Context describing why the error occurred.
        message: String,
    },

    /// Underlying VFS error.
    #[error(transparent)]
    Vfs(#[from] VfsError),

    /// Underlying metadata engine error.
    #[error(transparent)]
    Meta(#[from] MetaError),

    /// Local OS I/O error (for example from `umount`/`fusermount`).
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

impl FuseError {
    /// Constructs a [`FuseError::Posix`] from a `libc` errno code and message.
    #[must_use]
    pub fn from_errno(errno: libc::c_int, message: impl Into<String>) -> Self {
        Self::Posix {
            errno,
            message: message.into(),
        }
    }

    /// Returns a `libc::EBADF` error for an invalid or mismatched file handle.
    #[must_use]
    pub fn bad_fd(message: impl Into<String>) -> Self {
        Self::from_errno(libc::EBADF, message)
    }

    /// Returns a `libc::EROFS` error for a mutation on a read-only mount.
    #[must_use]
    pub fn read_only(message: impl Into<String>) -> Self {
        Self::from_errno(libc::EROFS, message)
    }

    /// Returns a `libc::EINVAL` error for invalid arguments.
    #[must_use]
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::from_errno(libc::EINVAL, message)
    }

    /// Maps this error to its POSIX `libc` errno integer value.
    #[must_use]
    pub fn errno(&self) -> libc::c_int {
        match self {
            Self::Posix { errno, .. } => *errno,
            Self::Vfs(err) => vfs_error_to_errno(err),
            Self::Meta(err) => meta_error_to_errno(err),
            Self::Io(err) => err
                .raw_os_error()
                .unwrap_or_else(|| io_kind_to_errno(err.kind())),
        }
    }
}

/// Maps a [`MetaError`] to a POSIX `libc` errno value.
#[must_use]
pub const fn meta_error_to_errno(err: &MetaError) -> libc::c_int {
    match err {
        MetaError::NotFound(_) => libc::ENOENT,
        MetaError::AlreadyExists(_) => libc::EEXIST,
        MetaError::NotDir(_) => libc::ENOTDIR,
        MetaError::IsDir(_) => libc::EISDIR,
        MetaError::NotEmpty(_) => libc::ENOTEMPTY,
        MetaError::InvalidInput(_) => libc::EINVAL,
        MetaError::WouldBlock(_) => libc::EAGAIN,
        MetaError::QuotaExceeded(_) => libc::EDQUOT,
        MetaError::NotFormatted
        | MetaError::Core(_)
        | MetaError::Sql(_)
        | MetaError::Json(_)
        | MetaError::Task(_)
        | MetaError::Redis(_) => libc::EIO,
    }
}

/// Maps a [`VfsError`] to a POSIX `libc` errno value.
#[must_use]
pub fn vfs_error_to_errno(err: &VfsError) -> libc::c_int {
    match err {
        VfsError::Meta(meta_err) => meta_error_to_errno(meta_err),
        VfsError::PermissionDenied(_) => libc::EACCES,
        VfsError::InvalidPath(_) | VfsError::InvalidAcl(_) => libc::EINVAL,
        VfsError::TooManySymlinks => libc::ELOOP,
        VfsError::Io(io_err) => io_err
            .raw_os_error()
            .unwrap_or_else(|| io_kind_to_errno(io_err.kind())),
        VfsError::Core(_) | VfsError::Chunk(_) | VfsError::Object(_) | VfsError::Json(_) => {
            libc::EIO
        }
    }
}

const fn io_kind_to_errno(kind: ErrorKind) -> libc::c_int {
    match kind {
        ErrorKind::NotFound => libc::ENOENT,
        ErrorKind::PermissionDenied => libc::EACCES,
        ErrorKind::AlreadyExists => libc::EEXIST,
        ErrorKind::InvalidInput | ErrorKind::InvalidData => libc::EINVAL,
        ErrorKind::WouldBlock => libc::EAGAIN,
        _ => libc::EIO,
    }
}

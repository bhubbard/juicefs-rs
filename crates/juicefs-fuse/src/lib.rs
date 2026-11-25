//! Pure-Rust FUSE protocol adapter for `JuiceFS` [`juicefs_vfs::Vfs`].
//!
//! Exposes [`JuiceFsFuse`], [`MountConfig`], FUSE wire/protocol types
//! ([`FuseAttr`], [`FuseEntry`], [`FuseDirEntry`], [`FuseStatFs`], [`FuseError`]),
//! and platform [`unmount`] helpers without any C or `libfuse` build dependencies.

pub mod adapter;
pub mod config;
pub mod error;
pub mod handle;
pub mod mount;
pub mod types;

pub use adapter::JuiceFsFuse;
pub use config::MountConfig;
pub use error::{FuseError, meta_error_to_errno, vfs_error_to_errno};
pub use handle::OpenFileHandle;
pub use mount::unmount;
pub use types::{
    FuseAttr, FuseDirEntry, FuseEntry, FuseStatFs, mode_to_node_type, node_type_to_mode,
};

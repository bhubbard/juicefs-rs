//! Configuration for mounting and serving a `JuiceFS` FUSE filesystem.

use std::path::PathBuf;
use std::time::Duration;

/// Mount configuration options for [`crate::JuiceFsFuse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountConfig {
    /// Local mountpoint directory path.
    pub mountpoint: PathBuf,
    /// Allow users other than the mounting user to access the mount.
    pub allow_other: bool,
    /// Mount the filesystem in read-only mode (`EROFS` on mutations).
    pub read_only: bool,
    /// Kernel dentry cache timeout for [`crate::FuseEntry`].
    pub entry_ttl: Duration,
    /// Kernel attribute cache timeout for [`crate::FuseEntry`].
    pub attr_ttl: Duration,
}

impl Default for MountConfig {
    fn default() -> Self {
        Self {
            mountpoint: PathBuf::new(),
            allow_other: false,
            read_only: false,
            entry_ttl: Duration::from_secs(1),
            attr_ttl: Duration::from_secs(1),
        }
    }
}

impl MountConfig {
    /// Creates a [`MountConfig`] targeting `mountpoint` with default TTLs (`1s`).
    #[must_use]
    pub fn new(mountpoint: impl Into<PathBuf>) -> Self {
        Self {
            mountpoint: mountpoint.into(),
            ..Self::default()
        }
    }

    /// Sets [`Self::allow_other`].
    #[must_use]
    pub const fn with_allow_other(mut self, allow_other: bool) -> Self {
        self.allow_other = allow_other;
        self
    }

    /// Sets [`Self::read_only`].
    #[must_use]
    pub const fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Sets [`Self::entry_ttl`].
    #[must_use]
    pub const fn with_entry_ttl(mut self, entry_ttl: Duration) -> Self {
        self.entry_ttl = entry_ttl;
        self
    }

    /// Sets [`Self::attr_ttl`].
    #[must_use]
    pub const fn with_attr_ttl(mut self, attr_ttl: Duration) -> Self {
        self.attr_ttl = attr_ttl;
        self
    }
}

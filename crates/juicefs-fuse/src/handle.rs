//! Open file-handle tracking for [`crate::JuiceFsFuse`].

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

use juicefs_core::Ino;
use parking_lot::RwLock;

use crate::error::FuseError;

/// Metadata associated with an open FUSE file or directory handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenFileHandle {
    /// Numeric FUSE file handle (`fh >= 1`).
    pub fh: u64,
    /// Target inode.
    pub inode: Ino,
    /// POSIX open flags (`O_RDONLY`, `O_WRONLY`, `O_RDWR`, `O_APPEND`, etc.).
    pub flags: i32,
    /// Whether this handle was opened as a directory stream.
    pub is_dir: bool,
    /// Whether writes have occurred since the last `flush`/`fsync`.
    pub dirty: bool,
    /// `flock` lock owners acquired through this handle, released on `release`.
    pub(crate) flock_owners: BTreeSet<u64>,
}

/// Thread-safe table of active open FUSE handles.
#[derive(Debug)]
pub(crate) struct HandleTable {
    next_fh: AtomicU64,
    handles: RwLock<HashMap<u64, OpenFileHandle>>,
}

impl Default for HandleTable {
    fn default() -> Self {
        Self::new()
    }
}

impl HandleTable {
    pub(crate) fn new() -> Self {
        Self {
            next_fh: AtomicU64::new(1),
            handles: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn allocate(&self, inode: Ino, flags: i32, is_dir: bool) -> u64 {
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        let handle = OpenFileHandle {
            fh,
            inode,
            flags,
            is_dir,
            dirty: false,
            flock_owners: BTreeSet::new(),
        };
        self.handles.write().insert(fh, handle);
        fh
    }

    pub(crate) fn get(&self, fh: u64) -> Option<OpenFileHandle> {
        self.handles.read().get(&fh).cloned()
    }

    pub(crate) fn validate(&self, inode: Ino, fh: u64) -> Result<OpenFileHandle, FuseError> {
        let guard = self.handles.read();
        let handle = guard
            .get(&fh)
            .ok_or_else(|| FuseError::bad_fd(format!("invalid file handle {fh}")))?;
        if handle.inode != inode {
            return Err(FuseError::bad_fd(format!(
                "file handle {fh} belongs to inode {}, not {inode}",
                handle.inode
            )));
        }
        Ok(handle.clone())
    }

    pub(crate) fn set_dirty(&self, inode: Ino, fh: u64, dirty: bool) -> Result<(), FuseError> {
        let mut guard = self.handles.write();
        let handle = guard
            .get_mut(&fh)
            .ok_or_else(|| FuseError::bad_fd(format!("invalid file handle {fh}")))?;
        if handle.inode != inode {
            return Err(FuseError::bad_fd(format!(
                "file handle {fh} belongs to inode {}, not {inode}",
                handle.inode
            )));
        }
        handle.dirty = dirty;
        Ok(())
    }

    pub(crate) fn record_flock_owner(
        &self,
        inode: Ino,
        fh: u64,
        owner: u64,
        unlock: bool,
    ) -> Result<(), FuseError> {
        let mut guard = self.handles.write();
        let handle = guard
            .get_mut(&fh)
            .ok_or_else(|| FuseError::bad_fd(format!("invalid file handle {fh}")))?;
        if handle.inode != inode {
            return Err(FuseError::bad_fd(format!(
                "file handle {fh} belongs to inode {}, not {inode}",
                handle.inode
            )));
        }
        if unlock {
            handle.flock_owners.remove(&owner);
        } else {
            handle.flock_owners.insert(owner);
        }
        Ok(())
    }

    pub(crate) fn remove(&self, inode: Ino, fh: u64) -> Result<OpenFileHandle, FuseError> {
        let mut guard = self.handles.write();
        let Some(existing) = guard.get(&fh) else {
            return Err(FuseError::bad_fd(format!("invalid file handle {fh}")));
        };
        if existing.inode != inode {
            return Err(FuseError::bad_fd(format!(
                "file handle {fh} belongs to inode {}, not {inode}",
                existing.inode
            )));
        }
        Ok(guard
            .remove(&fh)
            .expect("verified present under write lock"))
    }

    pub(crate) fn len(&self) -> usize {
        self.handles.read().len()
    }
}

//! Core `JuiceFsFuse` adapter implementing POSIX FUSE operations over [`Vfs`].

use juicefs_core::{Attr, Ino, NodeType, SetAttrFlags};
use juicefs_meta::{FlockType, MetaError, PlockRecord};
use juicefs_vfs::{AccessMode, Vfs};

use crate::config::MountConfig;
use crate::error::FuseError;
use crate::handle::{HandleTable, OpenFileHandle};
use crate::types::{FuseAttr, FuseDirEntry, FuseEntry, FuseStatFs, mode_to_node_type};

const RENAME_NOREPLACE_FLAG: u32 = 1;

/// Pure-Rust FUSE protocol adapter wrapping a [`Vfs`] instance.
#[derive(Debug)]
pub struct JuiceFsFuse {
    vfs: Vfs,
    config: MountConfig,
    handles: HandleTable,
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<JuiceFsFuse>();
};

impl JuiceFsFuse {
    /// Creates a new [`JuiceFsFuse`] adapter for `vfs` with the given `config`.
    #[must_use]
    pub fn new(vfs: Vfs, config: MountConfig) -> Self {
        Self {
            vfs,
            config,
            handles: HandleTable::new(),
        }
    }

    /// Returns a reference to the underlying [`Vfs`].
    #[must_use]
    pub const fn vfs(&self) -> &Vfs {
        &self.vfs
    }

    /// Returns the active [`MountConfig`].
    #[must_use]
    pub const fn config(&self) -> &MountConfig {
        &self.config
    }

    /// Returns the number of currently open file and directory handles.
    #[must_use]
    pub fn open_handle_count(&self) -> usize {
        self.handles.len()
    }

    /// Returns a snapshot of an open handle if `fh` is currently active.
    #[must_use]
    pub fn get_handle(&self, fh: u64) -> Option<OpenFileHandle> {
        self.handles.get(fh)
    }

    fn ensure_writable(&self) -> Result<(), FuseError> {
        if self.config.read_only {
            return Err(FuseError::read_only("filesystem is mounted read-only"));
        }
        Ok(())
    }

    fn make_entry(&self, inode: Ino, attr: &Attr) -> FuseEntry {
        FuseEntry::from_vfs_entry(inode, attr, self.config.entry_ttl, self.config.attr_ttl)
    }

    /// Looks up a directory entry `name` inside `parent`.
    pub async fn lookup(&self, parent: Ino, name: &str) -> Result<FuseEntry, FuseError> {
        let (ino, attr) = self.vfs.meta().lookup(parent, name).await?;
        Ok(self.make_entry(ino, &attr))
    }

    /// Reads POSIX attributes for `inode`.
    pub async fn getattr(&self, inode: Ino) -> Result<FuseAttr, FuseError> {
        let attr = self.vfs.meta().getattr(inode).await?;
        Ok(FuseAttr::from_vfs_attr(inode, &attr))
    }

    /// Updates POSIX attributes and/or file length for `inode` according to `mask`.
    pub async fn setattr(
        &self,
        inode: Ino,
        attr: &Attr,
        mask: SetAttrFlags,
    ) -> Result<FuseAttr, FuseError> {
        self.ensure_writable()?;
        let mut updated = if mask.contains(SetAttrFlags::SIZE) {
            self.vfs.meta().truncate(inode, attr.length).await?
        } else {
            return Ok(FuseAttr::from_vfs_attr(
                inode,
                &self.vfs.meta().setattr(inode, attr, mask).await?,
            ));
        };

        if has_non_size_attr_flags(mask) {
            updated = self.vfs.meta().setattr(inode, attr, mask).await?;
        }
        Ok(FuseAttr::from_vfs_attr(inode, &updated))
    }

    /// Creates a regular file or special device node (`FIFO`, socket, device) in `parent`.
    pub async fn mknod(
        &self,
        parent: Ino,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
    ) -> Result<FuseEntry, FuseError> {
        self.ensure_writable()?;
        let (typ, perm) = mode_to_node_type(mode)?;
        let (ino, mut attr) = self
            .vfs
            .meta()
            .mknod(parent, name, typ, perm, uid, gid)
            .await?;
        if matches!(typ, NodeType::BlockDev | NodeType::CharDev) {
            attr.rdev = rdev;
        }
        Ok(self.make_entry(ino, &attr))
    }

    /// Creates a directory `name` inside `parent`.
    pub async fn mkdir(
        &self,
        parent: Ino,
        name: &str,
        mode: u16,
        uid: u32,
        gid: u32,
    ) -> Result<FuseEntry, FuseError> {
        self.ensure_writable()?;
        let (ino, attr) = self
            .vfs
            .meta()
            .mkdir(parent, name, mode & 0o7777, uid, gid)
            .await?;
        Ok(self.make_entry(ino, &attr))
    }

    /// Creates and opens a regular file `name` inside `parent`, returning `(entry, fh)`.
    pub async fn create(
        &self,
        parent: Ino,
        name: &str,
        mode: u16,
        flags: i32,
        uid: u32,
        gid: u32,
    ) -> Result<(FuseEntry, u64), FuseError> {
        self.ensure_writable()?;
        let (ino, attr) = match self
            .vfs
            .meta()
            .mknod(parent, name, NodeType::File, mode & 0o7777, uid, gid)
            .await
        {
            Ok(created) => created,
            Err(MetaError::AlreadyExists(_)) if (flags & libc::O_EXCL) == 0 => {
                let (existing_ino, mut existing_attr) =
                    self.vfs.meta().lookup(parent, name).await?;
                if existing_attr.is_dir() {
                    return Err(MetaError::IsDir(name.to_owned()).into());
                }
                if (flags & libc::O_TRUNC) != 0 && existing_attr.is_file() {
                    existing_attr = self.vfs.meta().truncate(existing_ino, 0).await?;
                }
                (existing_ino, existing_attr)
            }
            Err(err) => return Err(err.into()),
        };

        let fh = self.handles.allocate(ino, flags, false);
        Ok((self.make_entry(ino, &attr), fh))
    }

    /// Opens an existing inode with POSIX `flags`, returning a tracked file handle `fh`.
    pub async fn open(&self, inode: Ino, flags: i32) -> Result<u64, FuseError> {
        let mutating_flags = libc::O_WRONLY | libc::O_RDWR | libc::O_TRUNC | libc::O_APPEND;
        if self.config.read_only && (flags & mutating_flags) != 0 {
            return Err(FuseError::read_only(
                "cannot open for writing on read-only mount",
            ));
        }

        let attr = self.vfs.meta().getattr(inode).await?;
        if attr.is_dir() && (flags & (libc::O_WRONLY | libc::O_RDWR | libc::O_TRUNC)) != 0 {
            return Err(MetaError::IsDir(format!("inode {inode}")).into());
        }

        if (flags & libc::O_TRUNC) != 0
            && (flags & (libc::O_WRONLY | libc::O_RDWR)) != 0
            && attr.is_file()
        {
            self.vfs.meta().truncate(inode, 0).await?;
        }

        Ok(self.handles.allocate(inode, flags, attr.is_dir()))
    }

    /// Reads up to `size` bytes from `inode` via open handle `fh` starting at `offset`.
    pub async fn read(
        &self,
        inode: Ino,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, FuseError> {
        let handle = self.handles.validate(inode, fh)?;
        if handle.is_dir {
            return Err(MetaError::IsDir(format!("inode {inode}")).into());
        }
        if (handle.flags & libc::O_ACCMODE) == libc::O_WRONLY {
            return Err(FuseError::bad_fd(format!(
                "file handle {fh} is open write-only"
            )));
        }

        let mut buf = vec![0u8; size as usize];
        let n = self.vfs.read_at(inode, offset, &mut buf).await?;
        buf.truncate(n);
        Ok(buf)
    }

    /// Writes `data` to `inode` via open handle `fh` starting at `offset`.
    pub async fn write(
        &self,
        inode: Ino,
        fh: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<usize, FuseError> {
        self.ensure_writable()?;
        let handle = self.handles.validate(inode, fh)?;
        if handle.is_dir {
            return Err(MetaError::IsDir(format!("inode {inode}")).into());
        }
        if (handle.flags & libc::O_ACCMODE) == libc::O_RDONLY {
            return Err(FuseError::bad_fd(format!(
                "file handle {fh} is open read-only"
            )));
        }

        let write_offset = if (handle.flags & libc::O_APPEND) != 0 {
            self.vfs.meta().getattr(inode).await?.length
        } else {
            offset
        };

        let written = self.vfs.write_at(inode, write_offset, data).await?;
        self.handles.set_dirty(inode, fh, true)?;
        Ok(written)
    }

    /// Flushes dirty state and releases any POSIX byte-range locks for `lock_owner` on `fh`.
    pub async fn flush(&self, inode: Ino, fh: u64, lock_owner: u64) -> Result<(), FuseError> {
        let _ = self.handles.validate(inode, fh)?;
        self.handles.set_dirty(inode, fh, false)?;
        if lock_owner != 0 {
            self.vfs
                .meta()
                .set_lk(
                    inode,
                    lock_owner,
                    PlockRecord {
                        typ: FlockType::Unlock,
                        pid: 0,
                        start: 0,
                        end: u64::MAX,
                    },
                )
                .await?;
        }
        Ok(())
    }

    /// Synchronizes file data/metadata for open handle `fh`.
    pub async fn fsync(&self, inode: Ino, fh: u64, datasync: bool) -> Result<(), FuseError> {
        let _ = datasync;
        let _ = self.handles.validate(inode, fh)?;
        let _ = self.vfs.meta().getattr(inode).await?;
        self.handles.set_dirty(inode, fh, false)?;
        Ok(())
    }

    /// Releases an open file or directory handle `fh`, unlocking any associated `flock` owners.
    pub async fn release(&self, inode: Ino, fh: u64) -> Result<(), FuseError> {
        let handle = self.handles.remove(inode, fh)?;
        for owner in handle.flock_owners {
            self.vfs
                .meta()
                .flock(inode, owner, FlockType::Unlock)
                .await?;
        }
        Ok(())
    }

    /// Removes a file or symlink `name` from `parent`.
    pub async fn unlink(&self, parent: Ino, name: &str) -> Result<(), FuseError> {
        self.ensure_writable()?;
        self.vfs.meta().unlink(parent, name).await?;
        Ok(())
    }

    /// Removes an empty directory `name` from `parent`.
    pub async fn rmdir(&self, parent: Ino, name: &str) -> Result<(), FuseError> {
        self.ensure_writable()?;
        self.vfs.meta().rmdir(parent, name).await?;
        Ok(())
    }

    /// Renames `name` in `parent` to `new_name` in `new_parent`.
    pub async fn rename(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
        flags: u32,
    ) -> Result<(), FuseError> {
        self.ensure_writable()?;
        if (flags & !RENAME_NOREPLACE_FLAG) != 0 {
            return Err(FuseError::invalid(format!(
                "unsupported rename flags: {flags:#x}"
            )));
        }
        if (flags & RENAME_NOREPLACE_FLAG) != 0 {
            match self.vfs.meta().lookup(new_parent, new_name).await {
                Ok(_) => {
                    return Err(FuseError::from_errno(
                        libc::EEXIST,
                        format!("destination already exists: {new_name}"),
                    ));
                }
                Err(MetaError::NotFound(_)) => {}
                Err(err) => return Err(err.into()),
            }
        }
        self.vfs
            .meta()
            .rename(parent, name, new_parent, new_name)
            .await?;
        Ok(())
    }

    /// Creates a hard link `new_name` in `new_parent` pointing to `inode`.
    pub async fn link(
        &self,
        inode: Ino,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<FuseEntry, FuseError> {
        self.ensure_writable()?;
        let attr = self.vfs.meta().link(inode, new_parent, new_name).await?;
        Ok(self.make_entry(inode, &attr))
    }

    /// Creates a symbolic link `name` in `parent` pointing to `target`.
    pub async fn symlink(
        &self,
        parent: Ino,
        name: &str,
        target: &str,
        uid: u32,
        gid: u32,
    ) -> Result<FuseEntry, FuseError> {
        self.ensure_writable()?;
        let (ino, attr) = self
            .vfs
            .meta()
            .symlink(parent, name, target, uid, gid)
            .await?;
        Ok(self.make_entry(ino, &attr))
    }

    /// Reads the target path of symbolic link `inode`.
    pub async fn readlink(&self, inode: Ino) -> Result<String, FuseError> {
        let attr = self.vfs.meta().getattr(inode).await?;
        if !attr.is_symlink() {
            return Err(FuseError::invalid(format!(
                "inode {inode} is not a symbolic link"
            )));
        }
        Ok(self.vfs.meta().readlink(inode).await?)
    }

    /// Opens a directory `inode` for reading, returning a tracked handle `fh`.
    pub async fn opendir(&self, inode: Ino, flags: i32) -> Result<u64, FuseError> {
        let attr = self.vfs.meta().getattr(inode).await?;
        if !attr.is_dir() {
            return Err(MetaError::NotDir(format!("inode {inode}")).into());
        }
        Ok(self.handles.allocate(inode, flags, true))
    }

    /// Releases an open directory handle `fh`.
    pub async fn releasedir(&self, inode: Ino, fh: u64) -> Result<(), FuseError> {
        let handle = self.handles.validate(inode, fh)?;
        if !handle.is_dir {
            return Err(MetaError::NotDir(format!("inode {inode}")).into());
        }
        let _ = self.handles.remove(inode, fh)?;
        std::future::ready(()).await;
        Ok(())
    }

    /// Lists directory entries in `inode` starting at stream `offset`.
    pub async fn readdir(&self, inode: Ino, offset: u64) -> Result<Vec<FuseDirEntry>, FuseError> {
        let entries = self.vfs.meta().readdir(inode).await?;
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        if start >= entries.len() {
            return Ok(Vec::new());
        }
        Ok(entries
            .iter()
            .enumerate()
            .skip(start)
            .map(|(idx, entry)| FuseDirEntry::from_vfs_entry(entry, (idx + 1) as u64))
            .collect())
    }

    /// Returns filesystem capacity and inode statistics.
    pub async fn statfs(&self) -> Result<FuseStatFs, FuseError> {
        let stat = self.vfs.statfs().await?;
        Ok(FuseStatFs::from(stat))
    }

    /// Gets the value of extended attribute `name` on `inode`.
    pub async fn getxattr(&self, inode: Ino, name: &str) -> Result<Vec<u8>, FuseError> {
        if name.is_empty() {
            return Err(FuseError::invalid("xattr name must not be empty"));
        }
        Ok(self.vfs.meta().get_xattr(inode, name).await?)
    }

    /// Sets extended attribute `name` on `inode` with optional `XATTR_CREATE`/`XATTR_REPLACE` `flags`.
    pub async fn setxattr(
        &self,
        inode: Ino,
        name: &str,
        value: &[u8],
        flags: i32,
    ) -> Result<(), FuseError> {
        self.ensure_writable()?;
        if name.is_empty() {
            return Err(FuseError::invalid("xattr name must not be empty"));
        }
        let create_only = (flags & libc::XATTR_CREATE) != 0;
        let replace_only = (flags & libc::XATTR_REPLACE) != 0;
        if create_only && replace_only {
            return Err(FuseError::invalid(
                "XATTR_CREATE and XATTR_REPLACE cannot both be set",
            ));
        }

        if create_only {
            match self.vfs.meta().get_xattr(inode, name).await {
                Ok(_) => {
                    return Err(FuseError::from_errno(
                        libc::EEXIST,
                        format!("xattr {name} already exists"),
                    ));
                }
                Err(MetaError::NotFound(_)) => {}
                Err(err) => return Err(err.into()),
            }
        } else if replace_only {
            self.vfs.meta().get_xattr(inode, name).await?;
        }

        self.vfs.meta().set_xattr(inode, name, value).await?;
        Ok(())
    }

    /// Lists all extended attribute names stored on `inode`.
    pub async fn listxattr(&self, inode: Ino) -> Result<Vec<String>, FuseError> {
        let _ = self.vfs.meta().getattr(inode).await?;
        Ok(self.vfs.meta().list_xattr(inode).await?)
    }

    /// Removes extended attribute `name` from `inode`.
    pub async fn removexattr(&self, inode: Ino, name: &str) -> Result<(), FuseError> {
        self.ensure_writable()?;
        if name.is_empty() {
            return Err(FuseError::invalid("xattr name must not be empty"));
        }
        self.vfs.meta().remove_xattr(inode, name).await?;
        Ok(())
    }

    /// Acquires, upgrades, downgrades, or releases a whole-file `flock` lock on `inode`.
    pub async fn flock(
        &self,
        inode: Ino,
        fh: u64,
        owner: u64,
        lock_type: FlockType,
    ) -> Result<(), FuseError> {
        let _ = self.handles.validate(inode, fh)?;
        self.vfs.meta().flock(inode, owner, lock_type).await?;
        self.handles
            .record_flock_owner(inode, fh, owner, lock_type == FlockType::Unlock)?;
        Ok(())
    }

    /// Queries conflicting POSIX byte-range locks (`F_GETLK`) on `inode`.
    pub async fn getlk(
        &self,
        inode: Ino,
        fh: u64,
        owner: u64,
        lock: PlockRecord,
    ) -> Result<PlockRecord, FuseError> {
        let _ = self.handles.validate(inode, fh)?;
        Ok(self.vfs.meta().get_lk(inode, owner, lock).await?)
    }

    /// Acquires or releases a POSIX byte-range lock (`F_SETLK`) on `inode`.
    pub async fn setlk(
        &self,
        inode: Ino,
        fh: u64,
        owner: u64,
        lock: PlockRecord,
    ) -> Result<(), FuseError> {
        let _ = self.handles.validate(inode, fh)?;
        self.vfs.meta().set_lk(inode, owner, lock).await?;
        Ok(())
    }

    /// Checks whether `(uid, gid)` has POSIX `mask` permissions (`AccessMode`) on `inode`.
    pub async fn access(&self, inode: Ino, uid: u32, gid: u32, mask: u8) -> Result<(), FuseError> {
        self.vfs.access(inode, uid, gid, mask).await?;
        if self.config.read_only && (mask & AccessMode::WRITE) != 0 {
            return Err(FuseError::read_only(
                "write access denied on read-only mount",
            ));
        }
        Ok(())
    }
}

const fn has_non_size_attr_flags(mask: SetAttrFlags) -> bool {
    mask.contains(SetAttrFlags::MODE)
        || mask.contains(SetAttrFlags::UID)
        || mask.contains(SetAttrFlags::GID)
        || mask.contains(SetAttrFlags::ATIME)
        || mask.contains(SetAttrFlags::MTIME)
        || mask.contains(SetAttrFlags::CTIME)
        || mask.contains(SetAttrFlags::ATIME_NOW)
        || mask.contains(SetAttrFlags::MTIME_NOW)
        || mask.contains(SetAttrFlags::CTIME_NOW)
        || mask.contains(SetAttrFlags::TIER)
}

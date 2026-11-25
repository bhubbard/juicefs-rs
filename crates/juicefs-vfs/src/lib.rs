//! High-level POSIX Virtual File System (`VFS`) tying metadata and chunk storage together.

pub mod acl;
pub mod compact;
pub mod error;
pub mod internal;

use std::collections::VecDeque;
use std::sync::Arc;

use juicefs_chunk::CachedChunkStore;
use juicefs_core::{
    Attr, CHUNK_SIZE, ChunkConfig, ClientConfig, Entry, Format, Ino, NodeType, ROOT_INODE, Slice,
    StatFs,
};
use juicefs_meta::Meta;
use juicefs_object::open_from_format;

pub use acl::{AccessControlList, AccessMode, AclEntry};
pub use error::VfsError;
pub use internal::VolumeStats;

/// Maximum number of symbolic links followed while resolving a single path.
pub const MAX_SYMLINK_DEPTH: usize = 40;

/// Virtual File System client for a `JuiceFS` volume.
#[derive(Debug, Clone)]
pub struct Vfs {
    meta: Arc<dyn Meta>,
    chunks: CachedChunkStore,
    format: Format,
    _client_config: ClientConfig,
}

impl Vfs {
    /// Connects to a formatted volume using `meta`, opening its object store and chunk cache.
    pub async fn connect(
        meta: Arc<dyn Meta>,
        mut chunk_config: ChunkConfig,
        client_config: ClientConfig,
    ) -> Result<Self, VfsError> {
        let format = meta.load().await?;
        chunk_config.block_size = format.block_size;
        chunk_config.compression.clone_from(&format.compression);
        chunk_config.hash_prefix = format.hash_prefix;

        let store = open_from_format(&format)?;
        store.create().await?;
        let chunks = CachedChunkStore::new(store, chunk_config)?;

        Ok(Self {
            meta,
            chunks,
            format,
            _client_config: client_config,
        })
    }

    /// Returns the loaded volume [`Format`].
    #[must_use]
    pub const fn format(&self) -> &Format {
        &self.format
    }

    /// Returns a reference to the underlying metadata engine.
    #[must_use]
    pub fn meta(&self) -> &Arc<dyn Meta> {
        &self.meta
    }

    pub(crate) const fn chunks(&self) -> &CachedChunkStore {
        &self.chunks
    }

    /// Returns filesystem capacity and inode statistics.
    pub async fn statfs(&self) -> Result<StatFs, VfsError> {
        Ok(self.meta.statfs().await?)
    }

    /// Checks whether `(uid, gid)` has `mask` permissions (`AccessMode`) on `inode`.
    ///
    /// Root (`uid == 0`) bypasses read and write checks; execute on non-directories
    /// still requires at least one execute bit (`0o111`).
    pub async fn access(&self, inode: Ino, uid: u32, gid: u32, mask: u8) -> Result<(), VfsError> {
        let attr = self.meta.getattr(inode).await?;
        let req = mask & 0o7;
        if uid == 0 {
            if (req & AccessMode::EXEC) != 0 && !attr.is_dir() && (attr.mode & 0o111) == 0 {
                return Err(VfsError::PermissionDenied(format!(
                    "inode {inode}: execute permission denied"
                )));
            }
            return Ok(());
        }

        let allowed = match self
            .meta
            .get_xattr(inode, acl::POSIX_ACL_ACCESS_XATTR)
            .await
        {
            Ok(raw) => {
                let rule = AccessControlList::decode(&raw)?;
                rule.can_access(uid, &[gid], attr.uid, attr.gid, req)
            }
            Err(juicefs_meta::MetaError::NotFound(_)) => {
                acl::check_posix_mode(&attr, uid, gid, req)
            }
            Err(err) => return Err(err.into()),
        };

        if !allowed {
            return Err(VfsError::PermissionDenied(format!(
                "inode {inode}: access denied for uid {uid} gid {gid} mask {req:#o}"
            )));
        }
        Ok(())
    }

    /// Persists a POSIX access ACL rule on `inode`.
    pub async fn set_acl(&self, inode: Ino, acl: &AccessControlList) -> Result<(), VfsError> {
        let encoded = acl.encode();
        self.meta
            .set_xattr(inode, acl::POSIX_ACL_ACCESS_XATTR, &encoded)
            .await?;
        Ok(())
    }

    /// Reads the POSIX access ACL rule stored on `inode`.
    pub async fn get_acl(&self, inode: Ino) -> Result<AccessControlList, VfsError> {
        let raw = self
            .meta
            .get_xattr(inode, acl::POSIX_ACL_ACCESS_XATTR)
            .await?;
        AccessControlList::decode(&raw)
    }

    /// Reads a virtual internal file such as `.stats` or `.config`.
    pub async fn read_internal(&self, name: &str) -> Result<Vec<u8>, VfsError> {
        match name.trim_start_matches('/') {
            internal::STATS_NAME => {
                let statfs = self.meta.statfs().await?;
                internal::render_stats_json(&self.format, statfs, self.chunks.used_memory())
            }
            internal::CONFIG_NAME => {
                let format = self.meta.load().await?;
                internal::render_config_json(&format)
            }
            _ => Err(juicefs_meta::MetaError::NotFound(name.to_owned()).into()),
        }
    }

    /// Compacts fragmented chunks (`slices.len() > 1`) of the file at `path`.
    ///
    /// Returns the number of chunks compacted into single contiguous slices.
    pub async fn compact_file(&self, path: &str) -> Result<usize, VfsError> {
        compact::compact_file(self, path).await
    }

    /// Resolves a POSIX path (e.g. `"/a/b/c"`) to its `(Ino, Attr)` without following symlinks.
    pub async fn resolve_path(&self, path: &str) -> Result<(Ino, Attr), VfsError> {
        let mut cur = ROOT_INODE;
        let mut cur_attr = self.meta.getattr(cur).await?;
        for seg in split_path(path) {
            let (next_ino, next_attr) = self.meta.lookup(cur, seg).await?;
            cur = next_ino;
            cur_attr = next_attr;
        }
        Ok((cur, cur_attr))
    }

    /// Resolves a POSIX path, following symbolic links up to [`MAX_SYMLINK_DEPTH`] hops.
    pub async fn resolve_path_follow(&self, path: &str) -> Result<(Ino, Attr), VfsError> {
        self.resolve_segments_follow(split_path(path)).await
    }

    async fn resolve_segments_follow<'a>(
        &self,
        initial: impl IntoIterator<Item = &'a str>,
    ) -> Result<(Ino, Attr), VfsError> {
        let mut queue: VecDeque<String> = initial.into_iter().map(str::to_owned).collect();
        let mut cur = ROOT_INODE;
        let mut cur_attr = self.meta.getattr(cur).await?;
        let mut symlinks_followed = 0usize;

        while let Some(seg) = queue.pop_front() {
            if seg == "." || seg.is_empty() {
                continue;
            }
            if !cur_attr.is_dir() {
                return Err(juicefs_meta::MetaError::NotDir(format!("inode {cur}")).into());
            }
            if seg == ".." {
                let (next_ino, next_attr) = self.meta.lookup(cur, "..").await?;
                cur = next_ino;
                cur_attr = next_attr;
                continue;
            }
            let (next_ino, next_attr) = self.meta.lookup(cur, &seg).await?;
            if next_attr.is_symlink() {
                symlinks_followed += 1;
                if symlinks_followed > MAX_SYMLINK_DEPTH {
                    return Err(VfsError::TooManySymlinks);
                }
                let target = self.meta.readlink(next_ino).await?;
                if target.starts_with('/') {
                    cur = ROOT_INODE;
                    cur_attr = self.meta.getattr(cur).await?;
                }
                for part in split_path(&target).collect::<Vec<_>>().into_iter().rev() {
                    queue.push_front(part.to_owned());
                }
            } else {
                cur = next_ino;
                cur_attr = next_attr;
            }
        }

        Ok((cur, cur_attr))
    }

    /// Splits a path into `(parent_ino, child_name)` without following symlinks in parents.
    pub async fn resolve_parent(&self, path: &str) -> Result<(Ino, String), VfsError> {
        let parts: Vec<&str> = split_path(path).collect();
        let Some((&name, parents)) = parts.split_last() else {
            return Err(VfsError::InvalidPath(path.to_owned()));
        };
        let mut cur = ROOT_INODE;
        for seg in parents {
            let (next_ino, _) = self.meta.lookup(cur, seg).await?;
            cur = next_ino;
        }
        Ok((cur, name.to_owned()))
    }

    /// Splits a path into `(parent_ino, child_name)`, following symlinks in parent directories.
    pub async fn resolve_parent_follow(&self, path: &str) -> Result<(Ino, String), VfsError> {
        let parts: Vec<&str> = split_path(path).collect();
        let Some((&name, parents)) = parts.split_last() else {
            return Err(VfsError::InvalidPath(path.to_owned()));
        };
        if name == ".." {
            return Err(VfsError::InvalidPath(path.to_owned()));
        }
        let (parent_ino, parent_attr) = self
            .resolve_segments_follow(parents.iter().copied())
            .await?;
        if !parent_attr.is_dir() {
            return Err(juicefs_meta::MetaError::NotDir(format!("inode {parent_ino}")).into());
        }
        Ok((parent_ino, name.to_owned()))
    }

    /// Creates a symbolic link at `link_path` pointing to `target`.
    pub async fn symlink(&self, link_path: &str, target: &str) -> Result<(Ino, Attr), VfsError> {
        let (parent, name) = self.resolve_parent_follow(link_path).await?;
        Ok(self.meta.symlink(parent, &name, target, 0, 0).await?)
    }

    /// Reads the target string of the symbolic link at `path`.
    pub async fn readlink(&self, path: &str) -> Result<String, VfsError> {
        let (parent, name) = self.resolve_parent_follow(path).await?;
        let (ino, attr) = self.meta.lookup(parent, &name).await?;
        if !attr.is_symlink() {
            return Err(juicefs_meta::MetaError::InvalidInput(format!(
                "not a symbolic link: {path}"
            ))
            .into());
        }
        Ok(self.meta.readlink(ino).await?)
    }

    /// Creates a directory at `path`.
    pub async fn mkdir(&self, path: &str, mode: u16) -> Result<(Ino, Attr), VfsError> {
        let (parent, name) = self.resolve_parent(path).await?;
        Ok(self.meta.mkdir(parent, &name, mode, 0, 0).await?)
    }

    /// Creates a directory and all missing parent directories.
    pub async fn mkdir_p(&self, path: &str, mode: u16) -> Result<(Ino, Attr), VfsError> {
        let mut cur = ROOT_INODE;
        let mut cur_attr = self.meta.getattr(cur).await?;
        for seg in split_path(path) {
            match self.meta.lookup(cur, seg).await {
                Ok((ino, attr)) => {
                    cur = ino;
                    cur_attr = attr;
                }
                Err(juicefs_meta::MetaError::NotFound(_)) => {
                    let (ino, attr) = self.meta.mkdir(cur, seg, mode, 0, 0).await?;
                    cur = ino;
                    cur_attr = attr;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok((cur, cur_attr))
    }

    /// Creates a regular file at `path`, or returns it if `create` is paired with existing file.
    pub async fn create_file(&self, path: &str, mode: u16) -> Result<(Ino, Attr), VfsError> {
        let (parent, name) = self.resolve_parent(path).await?;
        Ok(self
            .meta
            .mknod(parent, &name, NodeType::File, mode, 0, 0)
            .await?)
    }

    /// Lists entries in the directory at `path`.
    pub async fn readdir(&self, path: &str) -> Result<Vec<Entry>, VfsError> {
        let (ino, _) = self.resolve_path(path).await?;
        Ok(self.meta.readdir(ino).await?)
    }

    /// Reads up to `buf.len()` bytes from `inode` starting at `offset`.
    pub async fn read_at(
        &self,
        inode: Ino,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, VfsError> {
        let attr = self.meta.getattr(inode).await?;
        if offset >= attr.length || buf.is_empty() {
            return Ok(0);
        }
        let max_read = buf.len().min((attr.length - offset) as usize);
        let mut total_read = 0;

        while total_read < max_read {
            let file_off = offset + total_read as u64;
            let indx = (file_off / CHUNK_SIZE) as u32;
            let chunk_off = (file_off % CHUNK_SIZE) as usize;
            let chunk_rem = (CHUNK_SIZE as usize - chunk_off).min(max_read - total_read);

            let slices = self.meta.read_chunk(inode, indx).await?;
            let read_in_chunk = self
                .read_from_slices(
                    &slices,
                    chunk_off,
                    &mut buf[total_read..total_read + chunk_rem],
                )
                .await?;
            total_read += read_in_chunk;
            if read_in_chunk < chunk_rem {
                break;
            }
        }
        Ok(total_read)
    }

    pub(crate) async fn read_from_slices(
        &self,
        slices: &[Slice],
        chunk_off: usize,
        out: &mut [u8],
    ) -> Result<usize, VfsError> {
        let mut cursor = 0usize;
        let mut out_pos = 0usize;

        for slice in slices {
            let slen = slice.len as usize;
            if cursor + slen <= chunk_off {
                cursor += slen;
                continue;
            }
            if out_pos >= out.len() {
                break;
            }
            let start_in_slice = chunk_off.saturating_sub(cursor);
            let avail = slen - start_in_slice;
            let take = avail.min(out.len() - out_pos);

            if slice.id == 0 {
                out[out_pos..out_pos + take].fill(0);
            } else {
                let slice_off = slice.off as usize + start_in_slice;
                let mut got = 0;
                while got < take {
                    let n = self
                        .chunks
                        .read_slice(
                            slice.id,
                            slice.size as usize,
                            slice_off + got,
                            &mut out[out_pos + got..out_pos + take],
                        )
                        .await?;
                    if n == 0 {
                        out[out_pos + got..out_pos + take].fill(0);
                        break;
                    }
                    got += n;
                }
            }
            out_pos += take;
            cursor += slen;
        }

        if out_pos < out.len() {
            out[out_pos..].fill(0);
            out_pos = out.len();
        }
        Ok(out_pos)
    }

    /// Writes `data` to `inode` starting at `offset`, splitting across 64 MiB chunk boundaries.
    pub async fn write_at(&self, inode: Ino, offset: u64, data: &[u8]) -> Result<usize, VfsError> {
        if data.is_empty() {
            return Ok(0);
        }
        let mut written = 0usize;
        while written < data.len() {
            let file_off = offset + written as u64;
            let indx = (file_off / CHUNK_SIZE) as u32;
            let chunk_off = (file_off % CHUNK_SIZE) as u32;
            let chunk_cap = (CHUNK_SIZE as usize) - (chunk_off as usize);
            let take = (data.len() - written).min(chunk_cap);

            let slice_id = self.meta.new_slice().await?;
            let slice_data = &data[written..written + take];
            self.chunks.write_slice(slice_id, slice_data).await?;

            let slice = Slice {
                id: slice_id,
                size: take as u32,
                off: 0,
                len: take as u32,
            };
            let new_len = file_off + take as u64;
            self.meta
                .write_chunk(inode, indx, chunk_off, slice, new_len)
                .await?;
            written += take;
        }
        Ok(written)
    }

    /// Writes an entire byte slice to `path`, creating or truncating the file.
    pub async fn write_file(&self, path: &str, data: &[u8]) -> Result<Attr, VfsError> {
        let ino = match self.resolve_path(path).await {
            Ok((ino, attr)) => {
                if attr.is_dir() {
                    return Err(juicefs_meta::MetaError::IsDir(path.to_owned()).into());
                }
                self.meta.truncate(ino, 0).await?;
                ino
            }
            Err(VfsError::Meta(juicefs_meta::MetaError::NotFound(_))) => {
                self.create_file(path, 0o644).await?.0
            }
            Err(e) => return Err(e),
        };
        self.write_at(ino, 0, data).await?;
        Ok(self.meta.getattr(ino).await?)
    }

    /// Reads the entire contents of the file at `path`.
    pub async fn read_file(&self, path: &str) -> Result<Vec<u8>, VfsError> {
        let (ino, attr) = self.resolve_path(path).await?;
        if attr.is_dir() {
            return Err(juicefs_meta::MetaError::IsDir(path.to_owned()).into());
        }
        let mut buf = vec![0u8; attr.length as usize];
        let n = self.read_at(ino, 0, &mut buf).await?;
        buf.truncate(n);
        Ok(buf)
    }

    /// Removes a file or symlink at `path`.
    pub async fn unlink(&self, path: &str) -> Result<(), VfsError> {
        let (parent, name) = self.resolve_parent(path).await?;
        Ok(self.meta.unlink(parent, &name).await?)
    }

    /// Removes an empty directory at `path`.
    pub async fn rmdir(&self, path: &str) -> Result<(), VfsError> {
        let (parent, name) = self.resolve_parent(path).await?;
        Ok(self.meta.rmdir(parent, &name).await?)
    }

    /// Recursively removes a file or directory tree at `path`.
    pub fn rm_rf<'a>(
        &'a self,
        path: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), VfsError>> + Send + 'a>>
    {
        Box::pin(async move {
            let (ino, attr) = self.resolve_path(path).await?;
            if attr.is_dir() {
                let entries = self.meta.readdir(ino).await?;
                for entry in entries {
                    let child_path = if path == "/" {
                        format!("/{}", entry.name)
                    } else {
                        format!("{}/{}", path.trim_end_matches('/'), entry.name)
                    };
                    self.rm_rf(&child_path).await?;
                }
                if ino != ROOT_INODE {
                    self.rmdir(path).await?;
                }
            } else {
                self.unlink(path).await?;
            }
            Ok(())
        })
    }

    /// Renames `src` to `dst`.
    pub async fn rename(&self, src: &str, dst: &str) -> Result<(), VfsError> {
        let (src_parent, src_name) = self.resolve_parent(src).await?;
        let (dst_parent, dst_name) = self.resolve_parent(dst).await?;
        Ok(self
            .meta
            .rename(src_parent, &src_name, dst_parent, &dst_name)
            .await?)
    }

    /// Deletes all unreferenced slices from object storage and purges their metadata records.
    pub async fn gc_orphans(&self) -> Result<usize, VfsError> {
        let orphans = self.meta.list_orphan_slices().await?;
        let mut count = 0;
        for (slice_id, size) in orphans {
            self.chunks.remove_slice(slice_id, size as usize).await?;
            self.meta.purge_orphan_slice(slice_id).await?;
            count += 1;
        }
        Ok(count)
    }

    /// Returns detailed inode, attribute, and chunk/slice layout for `path`.
    pub async fn info(&self, path: &str) -> Result<NodeInfo, VfsError> {
        let (inode, attr) = self.resolve_path(path).await?;
        let mut chunks = Vec::new();
        if attr.is_file() && attr.length > 0 {
            let num_chunks = attr.length.div_ceil(CHUNK_SIZE) as u32;
            for indx in 0..num_chunks {
                let slices = self.meta.read_chunk(inode, indx).await?;
                if !slices.is_empty() {
                    chunks.push((indx, slices));
                }
            }
        }
        Ok(NodeInfo {
            inode,
            attr,
            chunks,
        })
    }

    /// Computes recursive file, directory, and byte counts under `path`.
    pub async fn summary(&self, path: &str) -> Result<TreeSummary, VfsError> {
        let (inode, attr) = self.resolve_path(path).await?;
        let mut out = TreeSummary::default();
        self.summary_walk(inode, &attr, &mut out).await?;
        Ok(out)
    }

    fn summary_walk<'a>(
        &'a self,
        inode: Ino,
        attr: &'a Attr,
        out: &'a mut TreeSummary,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), VfsError>> + Send + 'a>>
    {
        Box::pin(async move {
            if attr.is_dir() {
                out.dirs += 1;
                for entry in self.meta.readdir(inode).await? {
                    self.summary_walk(entry.inode, &entry.attr, out).await?;
                }
            } else {
                out.files += 1;
                out.bytes += attr.length;
            }
            Ok(())
        })
    }

    /// Zero-copy clones a file or directory tree from `src` to `dst`.
    pub async fn clone_path(&self, src: &str, dst: &str) -> Result<(Ino, Attr), VfsError> {
        let (src_parent, src_name) = self.resolve_parent(src).await?;
        let (dst_parent, dst_name) = self.resolve_parent(dst).await?;
        Ok(self
            .meta
            .clone_entry(src_parent, &src_name, dst_parent, &dst_name)
            .await?)
    }

    /// Warms the local block cache by reading all non-sparse slices under `path`.
    pub async fn warmup(&self, path: &str) -> Result<(usize, u64), VfsError> {
        let (inode, attr) = self.resolve_path(path).await?;
        let mut files = 0usize;
        let mut bytes = 0u64;
        self.warmup_walk(inode, &attr, &mut files, &mut bytes)
            .await?;
        Ok((files, bytes))
    }

    fn warmup_walk<'a>(
        &'a self,
        inode: Ino,
        attr: &'a Attr,
        files: &'a mut usize,
        bytes: &'a mut u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), VfsError>> + Send + 'a>>
    {
        Box::pin(async move {
            if attr.is_dir() {
                for entry in self.meta.readdir(inode).await? {
                    self.warmup_walk(entry.inode, &entry.attr, files, bytes)
                        .await?;
                }
            } else if attr.is_file() && attr.length > 0 {
                let mut buf = vec![0u8; attr.length.min(1 << 20) as usize];
                let mut off = 0u64;
                while off < attr.length {
                    let n = self.read_at(inode, off, &mut buf).await?;
                    if n == 0 {
                        break;
                    }
                    off += n as u64;
                }
                *files += 1;
                *bytes += attr.length;
            }
            Ok(())
        })
    }

    /// Deletes all objects stored in the underlying volume prefix.
    pub async fn destroy_objects(&self) -> Result<usize, VfsError> {
        let store = self.chunks.blob_storage();
        let items = store.list("").await?;
        let mut deleted = 0usize;
        for item in items {
            store.delete(&item.key).await?;
            deleted += 1;
        }
        Ok(deleted)
    }

    /// Updates the storage tier ID on `path`.
    pub async fn set_tier(&self, path: &str, tier: u8) -> Result<Attr, VfsError> {
        let (ino, mut attr) = self.resolve_path(path).await?;
        attr.tier = tier;
        Ok(self
            .meta
            .setattr(ino, &attr, juicefs_core::SetAttrFlags::TIER)
            .await?)
    }

    /// Restores an archived object key for `days` days in the underlying object store.
    pub async fn restore_object(&self, key: &str, days: i32) -> Result<(), VfsError> {
        self.chunks.blob_storage().restore(key, days).await?;
        Ok(())
    }

    /// Flushes any pending asynchronous write-back uploads in the chunk store.
    pub async fn flush(&self) -> Result<(), VfsError> {
        self.chunks.flush().await?;
        Ok(())
    }
}

/// Detailed inode, attribute, and chunk/slice layout returned by [`Vfs::info`].
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Inode number.
    pub inode: Ino,
    /// Node attributes.
    pub attr: Attr,
    /// Non-empty chunks as `(chunk_index, slices)`.
    pub chunks: Vec<(u32, Vec<Slice>)>,
}

/// Recursive directory tree summary returned by [`Vfs::summary`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeSummary {
    /// Number of non-directory entries.
    pub files: u64,
    /// Number of directories (including the root of the walk).
    pub dirs: u64,
    /// Total logical file bytes.
    pub bytes: u64,
}

fn split_path(path: &str) -> impl Iterator<Item = &str> {
    path.split('/').filter(|seg| !seg.is_empty() && *seg != ".")
}

#[cfg(test)]
mod tests {
    use juicefs_meta::SqliteMeta;

    use super::*;

    #[tokio::test]
    async fn vfs_end_to_end_read_write_overwrite_gc() {
        let meta = Arc::new(SqliteMeta::open(":memory:").expect("sqlite"));
        let mut fmt = Format::new("testvol", "mem", "");
        fmt.block_size = 512;
        fmt.compression = "lz4".to_owned();
        meta.init(&fmt, false).await.expect("init");

        let vfs = Vfs::connect(meta, ChunkConfig::default(), ClientConfig::default())
            .await
            .expect("connect");

        vfs.mkdir_p("/a/b/c", 0o755).await.expect("mkdir_p");
        vfs.write_file("/a/b/c/data.bin", b"hello world!")
            .await
            .expect("write_file");

        let (ino, _) = vfs.resolve_path("/a/b/c/data.bin").await.expect("resolve");
        // Overwrite middle bytes to exercise slice interval resolution
        vfs.write_at(ino, 6, b"RUST!!").await.expect("overwrite");

        let content = vfs.read_file("/a/b/c/data.bin").await.expect("read_file");
        assert_eq!(&content, b"hello RUST!!");

        // Sparse write beyond EOF
        vfs.write_at(ino, 20, b"end").await.expect("sparse write");
        let sparse = vfs.read_file("/a/b/c/data.bin").await.expect("read sparse");
        assert_eq!(sparse.len(), 23);
        assert_eq!(&sparse[0..12], b"hello RUST!!");
        assert_eq!(&sparse[12..20], &[0u8; 8]);
        assert_eq!(&sparse[20..23], b"end");

        vfs.rm_rf("/a").await.expect("rm_rf");
        let deleted = vfs.gc_orphans().await.expect("gc");
        assert_eq!(deleted, 3);
    }
}

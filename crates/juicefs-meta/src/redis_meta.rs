//! `Redis`-backed metadata engine using `JuiceFS` Redis key conventions.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;

use async_trait::async_trait;
use juicefs_core::{
    Attr, CHUNK_SIZE, Entry, Format, Ino, MAX_SYMLINK, NodeType, ROOT_INODE, SLICE_BYTES,
    SetAttrFlags, Slice, SliceRecord, StatFs, TRASH_INODE, build_slice, decode_slices,
    encode_slice,
};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;

use crate::dump::{DumpedChunk, DumpedEntry, DumpedMeta, DumpedOrphanSlice, DumpedXattr};
use crate::util::{
    align4k, decode_dir_entry, decode_plocks, encode_dir_entry, encode_plocks,
    make_trash_entry_name, now_ns, now_secs, parse_trash_entry_name, update_plocks, validate_name,
};
use crate::{
    ChangeLogEntry, DirStat, FlockType, FsckReport, Meta, MetaError, NlinkMismatch, PlockRecord,
    Quota, QuotaTarget, SessionInfo,
};

const SLICE_REFS_KEY: &str = "sliceRefs";
const DIR_QUOTA_KEY: &str = "dirQuota";
const UG_QUOTA_KEY: &str = "ugQuota";
const DIR_STATS_KEY: &str = "dirStats";
const CHANGELOG_KEY: &str = "changeLog";
const NEXT_CHANGELOG_KEY: &str = "nextChangeLog";
const SESSIONS_KEY: &str = "sessions";
const NEXT_SESSION_KEY: &str = "nextSession";
const USED_SPACE_KEY: &str = "usedSpace";
const USED_INODES_KEY: &str = "usedInodes";
const ALL_INODES_KEY: &str = "allInodes";

/// `Redis`-backed [`Meta`] engine compatible with `JuiceFS` Redis key conventions.
#[derive(Clone)]
pub struct RedisMeta {
    conn: ConnectionManager,
    redacted_url: String,
}

impl fmt::Debug for RedisMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisMeta")
            .field("url", &self.redacted_url)
            .finish_non_exhaustive()
    }
}

impl RedisMeta {
    /// Redis hash key for volume settings (`"setting"`).
    pub const SETTING_KEY: &'static str = "setting";
    /// Redis counter key for inode allocation (`"nextInode"`).
    pub const NEXT_INODE_KEY: &'static str = "nextInode";
    /// Redis counter key for chunk/slice ID allocation (`"nextChunk"`).
    pub const NEXT_CHUNK_KEY: &'static str = "nextChunk";

    /// Returns the Redis key for inode `inode` (`i<ino>`).
    #[must_use]
    pub fn inode_key(inode: Ino) -> String {
        format!("i{}", inode.as_u64())
    }

    /// Returns the Redis hash key for directory `parent` (`d<parent>`).
    #[must_use]
    pub fn dir_key(parent: Ino) -> String {
        format!("d{}", parent.as_u64())
    }

    /// Returns the Redis key for chunk `indx` of `inode` (`c<ino>_<indx>`).
    #[must_use]
    pub fn chunk_key(inode: Ino, indx: u32) -> String {
        format!("c{}_{}", inode.as_u64(), indx)
    }

    /// Returns the Redis key for symlink target `inode` (`sym<ino>`).
    #[must_use]
    pub fn symlink_key(inode: Ino) -> String {
        format!("sym{}", inode.as_u64())
    }

    /// Returns the Redis hash key for extended attributes on `inode` (`x<ino>`).
    #[must_use]
    pub fn xattr_key(inode: Ino) -> String {
        format!("x{}", inode.as_u64())
    }

    /// Validates a `redis://` or `rediss://` metadata URL without opening a network connection.
    pub fn validate_url(url: &str) -> Result<(), MetaError> {
        let (is_tls, rest) = if let Some(rest) = url.strip_prefix("redis://") {
            (false, rest)
        } else if let Some(rest) = url.strip_prefix("rediss://") {
            (true, rest)
        } else {
            return Err(MetaError::InvalidInput(format!(
                "unsupported redis metadata URL scheme (expected redis:// or rediss://): {url}"
            )));
        };
        if rest.is_empty() || rest.starts_with('/') {
            return Err(MetaError::InvalidInput(format!(
                "missing host in redis URL: {url}"
            )));
        }
        let host_part = rest.rsplit('@').next().unwrap_or("");
        let host_only = host_part.split('/').next().unwrap_or("");
        if host_only.is_empty() || host_only.starts_with(':') {
            return Err(MetaError::InvalidInput(format!(
                "missing host in redis URL: {url}"
            )));
        }
        // Validate URL syntax with `redis::Client::open` even when TLS features are not compiled in.
        if is_tls {
            let normalized = format!("redis://{rest}");
            let _ = redis::Client::open(normalized.as_str())?;
        } else {
            let _ = redis::Client::open(url)?;
        }
        Ok(())
    }

    /// Connects to a `Redis` metadata server at `url` (`redis://` or `rediss://`).
    pub async fn connect(url: &str) -> Result<Self, MetaError> {
        Self::validate_url(url)?;
        let client = redis::Client::open(url)?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self {
            conn,
            redacted_url: redact_url(url),
        })
    }

    fn client(&self) -> ConnectionManager {
        self.conn.clone()
    }

    async fn read_format_opt(&self) -> Result<Option<Format>, MetaError> {
        let mut conn = self.client();
        let raw: Option<String> = conn.hget(Self::SETTING_KEY, "format").await?;
        match raw {
            Some(json) => Ok(Some(serde_json::from_str(&json)?)),
            None => Ok(None),
        }
    }

    async fn read_format(&self) -> Result<Format, MetaError> {
        self.read_format_opt().await?.ok_or(MetaError::NotFormatted)
    }

    async fn read_attr_internal(&self, inode: Ino) -> Result<Attr, MetaError> {
        let mut conn = self.client();
        let raw: Option<String> = conn.get(Self::inode_key(inode)).await?;
        let json = raw.ok_or_else(|| MetaError::NotFound(format!("inode {inode}")))?;
        Ok(serde_json::from_str(&json)?)
    }

    async fn write_attr_internal(&self, inode: Ino, attr: &Attr) -> Result<(), MetaError> {
        let mut conn = self.client();
        let json = serde_json::to_string(attr)?;
        let _: () = conn.set(Self::inode_key(inode), json).await?;
        let _: () = conn.sadd(ALL_INODES_KEY, inode.as_u64()).await?;
        Ok(())
    }

    async fn incr_counter(&self, key: &str, delta: i64) -> Result<i64, MetaError> {
        let mut conn = self.client();
        let val: i64 = conn.incr(key, delta).await?;
        Ok(val)
    }

    async fn get_counter(&self, key: &str) -> Result<i64, MetaError> {
        let mut conn = self.client();
        let val: Option<i64> = conn.get(key).await?;
        Ok(val.unwrap_or(0))
    }

    async fn update_parent_mtime(&self, parent: Ino, now: i64) -> Result<(), MetaError> {
        if let Ok(mut attr) = self.read_attr_internal(parent).await {
            attr.mtime = now / 1_000_000_000;
            attr.mtime_nsec = (now % 1_000_000_000) as u32;
            attr.ctime = attr.mtime;
            attr.ctime_nsec = attr.mtime_nsec;
            self.write_attr_internal(parent, &attr).await?;
        }
        Ok(())
    }

    async fn update_slice_ref(
        &self,
        slice_id: u64,
        size: u32,
        delta: i64,
    ) -> Result<(), MetaError> {
        if slice_id == 0 {
            return Ok(());
        }
        let mut conn = self.client();
        let field = slice_id.to_string();
        let cur: Option<String> = conn.hget(SLICE_REFS_KEY, &field).await?;
        let (cur_size, cur_refs) = match cur {
            Some(s) => parse_slice_ref(&s)?,
            None => (size, 0),
        };
        let new_refs = cur_refs + delta;
        let final_size = if cur_size == 0 { size } else { cur_size };
        let _: () = conn
            .hset(SLICE_REFS_KEY, &field, format!("{final_size}:{new_refs}"))
            .await?;
        Ok(())
    }

    async fn decrement_file_chunks(&self, inode: Ino) -> Result<(), MetaError> {
        let mut conn = self.client();
        let pattern = format!("c{}_*", inode.as_u64());
        let keys = scan_keys(&mut conn, &pattern).await?;
        for key in keys {
            let blob: Option<Vec<u8>> = conn.get(&key).await?;
            if let Some(bytes) = blob {
                for rec in decode_slices(&bytes)? {
                    if rec.id > 0 {
                        self.update_slice_ref(rec.id, rec.size, -1).await?;
                    }
                }
            }
            let _: () = conn.del(&key).await?;
        }
        Ok(())
    }

    async fn delete_inode_data(&self, inode: Ino) -> Result<(), MetaError> {
        let mut conn = self.client();
        let _: () = conn
            .del(&[
                Self::inode_key(inode),
                Self::symlink_key(inode),
                Self::xattr_key(inode),
                flock_key(inode),
                plock_key(inode),
            ])
            .await?;
        let _: () = conn.srem(ALL_INODES_KEY, inode.as_u64()).await?;
        Ok(())
    }

    async fn ensure_trash_node(&self, now: i64) -> Result<(), MetaError> {
        if self.read_attr_internal(TRASH_INODE).await.is_ok() {
            return Ok(());
        }
        let sec = now / 1_000_000_000;
        let nsec = (now % 1_000_000_000) as u32;
        let trash_attr = Attr {
            typ: NodeType::Directory,
            mode: 0o555,
            nlink: 2,
            atime: sec,
            mtime: sec,
            ctime: sec,
            atime_nsec: nsec,
            mtime_nsec: nsec,
            ctime_nsec: nsec,
            parent: ROOT_INODE,
            ..Attr::default()
        };
        self.write_attr_internal(TRASH_INODE, &trash_attr).await
    }

    async fn ancestor_dirs(&self, dirs: &[Ino]) -> Result<Vec<Ino>, MetaError> {
        let mut visited = BTreeSet::new();
        for &start in dirs {
            let mut cur = start;
            while cur.is_valid() && !cur.is_trash() && visited.insert(cur.as_u64()) {
                if cur == ROOT_INODE {
                    break;
                }
                let Ok(attr) = self.read_attr_internal(cur).await else {
                    break;
                };
                if attr.parent == cur || !attr.parent.is_valid() {
                    break;
                }
                cur = attr.parent;
            }
        }
        Ok(visited.into_iter().map(Ino).collect())
    }

    async fn file_parents(&self, inode: Ino, attr: &Attr) -> Result<Vec<Ino>, MetaError> {
        if attr.parent.is_valid() {
            return Ok(vec![attr.parent]);
        }
        let mut conn = self.client();
        let all_inos: Vec<u64> = conn.smembers(ALL_INODES_KEY).await?;
        let mut parents = Vec::new();
        for dir_u64 in all_inos {
            let dir_ino = Ino(dir_u64);
            let entries: BTreeMap<String, Vec<u8>> = conn.hgetall(Self::dir_key(dir_ino)).await?;
            for bytes in entries.into_values() {
                if let Ok((child, _)) = decode_dir_entry(&bytes)
                    && child == inode
                {
                    parents.push(dir_ino);
                    break;
                }
            }
        }
        Ok(parents)
    }

    async fn apply_dir_quota_delta(
        &self,
        dirs: &[Ino],
        delta_space: i64,
        delta_inodes: i64,
    ) -> Result<(), MetaError> {
        if delta_space == 0 && delta_inodes == 0 {
            return Ok(());
        }
        let mut conn = self.client();
        let mut quotas = Vec::new();
        for anc in self.ancestor_dirs(dirs).await? {
            let raw: Option<String> = conn.hget(DIR_QUOTA_KEY, anc.as_u64().to_string()).await?;
            if let Some(json) = raw {
                let q: Quota = serde_json::from_str(&json)?;
                if delta_space > 0 && q.max_space > 0 && q.used_space + delta_space > q.max_space {
                    return Err(MetaError::QuotaExceeded(format!(
                        "directory inode {} space quota exceeded ({} + {} > {})",
                        anc.as_u64(),
                        q.used_space,
                        delta_space,
                        q.max_space
                    )));
                }
                if delta_inodes > 0
                    && q.max_inodes > 0
                    && q.used_inodes + delta_inodes > q.max_inodes
                {
                    return Err(MetaError::QuotaExceeded(format!(
                        "directory inode {} inode quota exceeded ({} + {} > {})",
                        anc.as_u64(),
                        q.used_inodes,
                        delta_inodes,
                        q.max_inodes
                    )));
                }
                quotas.push((anc, q));
            }
        }
        for (anc, mut q) in quotas {
            q.used_space = (q.used_space + delta_space).max(0);
            q.used_inodes = (q.used_inodes + delta_inodes).max(0);
            let _: () = conn
                .hset(
                    DIR_QUOTA_KEY,
                    anc.as_u64().to_string(),
                    serde_json::to_string(&q)?,
                )
                .await?;
        }
        Ok(())
    }

    async fn apply_dir_stats_delta(
        &self,
        dirs: &[Ino],
        delta_len: i64,
        delta_space: i64,
        delta_inodes: i64,
    ) -> Result<(), MetaError> {
        if delta_len == 0 && delta_space == 0 && delta_inodes == 0 {
            return Ok(());
        }
        let mut conn = self.client();
        for anc in self.ancestor_dirs(dirs).await? {
            let field = anc.as_u64().to_string();
            let raw: Option<String> = conn.hget(DIR_STATS_KEY, &field).await?;
            let mut stat: DirStat = match raw {
                Some(json) => serde_json::from_str(&json)?,
                None => DirStat::default(),
            };
            stat.data_length = (stat.data_length + delta_len).max(0);
            stat.used_space = (stat.used_space + delta_space).max(0);
            stat.used_inodes = (stat.used_inodes + delta_inodes).max(0);
            let _: () = conn
                .hset(DIR_STATS_KEY, &field, serde_json::to_string(&stat)?)
                .await?;
        }
        Ok(())
    }

    async fn apply_ug_quota_targets(
        &self,
        targets: &[QuotaTarget],
        delta_space: i64,
        delta_inodes: i64,
    ) -> Result<(), MetaError> {
        if delta_space == 0 && delta_inodes == 0 {
            return Ok(());
        }
        let mut conn = self.client();
        let mut loaded = Vec::new();
        for &target in targets {
            let field = ug_field(target);
            let raw: Option<String> = conn.hget(UG_QUOTA_KEY, &field).await?;
            if let Some(json) = raw {
                let q: Quota = serde_json::from_str(&json)?;
                if delta_space > 0 && q.max_space > 0 && q.used_space + delta_space > q.max_space {
                    return Err(MetaError::QuotaExceeded(format!(
                        "{target:?} space quota exceeded ({} + {} > {})",
                        q.used_space, delta_space, q.max_space
                    )));
                }
                if delta_inodes > 0
                    && q.max_inodes > 0
                    && q.used_inodes + delta_inodes > q.max_inodes
                {
                    return Err(MetaError::QuotaExceeded(format!(
                        "{target:?} inode quota exceeded ({} + {} > {})",
                        q.used_inodes, delta_inodes, q.max_inodes
                    )));
                }
                loaded.push((field, q));
            }
        }
        for (field, mut q) in loaded {
            q.used_space = (q.used_space + delta_space).max(0);
            q.used_inodes = (q.used_inodes + delta_inodes).max(0);
            let _: () = conn
                .hset(UG_QUOTA_KEY, field, serde_json::to_string(&q)?)
                .await?;
        }
        Ok(())
    }

    async fn record_changelog(&self, entry: &serde_json::Value) -> Result<(), MetaError> {
        let mut conn = self.client();
        let id: i64 = conn.incr(NEXT_CHANGELOG_KEY, 1).await?;
        let record = ChangeLogEntry {
            id,
            entry: serde_json::to_string(entry)?,
        };
        let member = serde_json::to_string(&record)?;
        let _: () = conn.zadd(CHANGELOG_KEY, member, id).await?;
        Ok(())
    }

    async fn calc_dir_usage(&self, dir_ino: Ino) -> Result<(i64, i64, i64), MetaError> {
        let mut conn = self.client();
        let mut queue = vec![dir_ino];
        let mut visited = HashSet::new();
        visited.insert(dir_ino.as_u64());
        let mut total_len: i64 = 0;
        let mut total_aligned: i64 = 0;
        let mut total_inodes: i64 = 0;

        while let Some(cur) = queue.pop() {
            let raw_entries: BTreeMap<String, Vec<u8>> = conn.hgetall(Self::dir_key(cur)).await?;
            for bytes in raw_entries.into_values() {
                let (child_ino, typ) = decode_dir_entry(&bytes)?;
                total_inodes += 1;
                if typ == NodeType::Directory {
                    total_aligned += align4k(0);
                    if visited.insert(child_ino.as_u64()) {
                        queue.push(child_ino);
                    }
                } else {
                    let len = self
                        .read_attr_internal(child_ino)
                        .await
                        .map_or(0, |a| a.length);
                    total_len += len as i64;
                    total_aligned += align4k(len);
                }
            }
        }
        Ok((total_len, total_aligned, total_inodes))
    }

    async fn remove_existing_dst(
        &self,
        dst_parent: Ino,
        dst_name: &str,
        fmt: &Format,
    ) -> Result<(), MetaError> {
        let mut conn = self.client();
        let existing: Option<Vec<u8>> = conn.hget(Self::dir_key(dst_parent), dst_name).await?;
        let Some(bytes) = existing else {
            return Ok(());
        };
        let (dst_ino, dst_typ) = decode_dir_entry(&bytes)?;
        if dst_typ == NodeType::Directory {
            let child_count: usize = conn.hlen(Self::dir_key(dst_ino)).await?;
            if child_count > 0 {
                return Err(MetaError::NotEmpty(dst_name.to_owned()));
            }
            self.apply_dir_quota_delta(&[dst_parent], 0, -1).await?;
            let dst_attr = self.read_attr_internal(dst_ino).await?;
            if fmt.user_group_quota {
                self.apply_ug_quota_targets(
                    &[
                        QuotaTarget::User(dst_attr.uid),
                        QuotaTarget::Group(dst_attr.gid),
                    ],
                    0,
                    -1,
                )
                .await?;
            }
            if fmt.dir_stats {
                self.apply_dir_stats_delta(&[dst_parent], 0, -align4k(0), -1)
                    .await?;
            }
            let _: () = conn
                .hdel(DIR_QUOTA_KEY, dst_ino.as_u64().to_string())
                .await?;
            let _: () = conn
                .hdel(DIR_STATS_KEY, dst_ino.as_u64().to_string())
                .await?;
            let _: () = conn.del(Self::dir_key(dst_ino)).await?;
            self.delete_inode_data(dst_ino).await?;
            let mut pattr = self.read_attr_internal(dst_parent).await?;
            pattr.nlink = pattr.nlink.saturating_sub(1).max(2);
            self.write_attr_internal(dst_parent, &pattr).await?;
            let _ = self.incr_counter(USED_INODES_KEY, -1).await?;
        } else {
            let mut dst_attr = self.read_attr_internal(dst_ino).await?;
            self.apply_dir_quota_delta(&[dst_parent], -(dst_attr.length as i64), -1)
                .await?;
            if fmt.dir_stats {
                self.apply_dir_stats_delta(
                    &[dst_parent],
                    -(dst_attr.length as i64),
                    -align4k(dst_attr.length),
                    -1,
                )
                .await?;
            }
            if dst_attr.nlink <= 1 {
                if fmt.user_group_quota {
                    self.apply_ug_quota_targets(
                        &[
                            QuotaTarget::User(dst_attr.uid),
                            QuotaTarget::Group(dst_attr.gid),
                        ],
                        -(dst_attr.length as i64),
                        -1,
                    )
                    .await?;
                }
                self.decrement_file_chunks(dst_ino).await?;
                self.delete_inode_data(dst_ino).await?;
                let _ = self.incr_counter(USED_INODES_KEY, -1).await?;
                let _ = self
                    .incr_counter(USED_SPACE_KEY, -(dst_attr.length as i64))
                    .await?;
            } else {
                dst_attr.nlink -= 1;
                self.write_attr_internal(dst_ino, &dst_attr).await?;
            }
        }
        let _: () = conn.hdel(Self::dir_key(dst_parent), dst_name).await?;
        Ok(())
    }

    async fn purge_trash_recursive(&self, dir_ino: Ino) -> Result<u64, MetaError> {
        let mut conn = self.client();
        let entries: BTreeMap<String, Vec<u8>> = conn.hgetall(Self::dir_key(dir_ino)).await?;
        let mut deleted: u64 = 0;
        for (name, bytes) in entries {
            let (child_ino, typ) = decode_dir_entry(&bytes)?;
            if typ == NodeType::Directory {
                deleted += Box::pin(self.purge_trash_recursive(child_ino)).await?;
                let _: () = conn
                    .hdel(DIR_QUOTA_KEY, child_ino.as_u64().to_string())
                    .await?;
                let _: () = conn.del(Self::dir_key(child_ino)).await?;
                self.delete_inode_data(child_ino).await?;
                let _ = self.incr_counter(USED_INODES_KEY, -1).await?;
                deleted += 1;
            } else {
                let mut attr = self.read_attr_internal(child_ino).await?;
                if attr.nlink <= 1 {
                    self.decrement_file_chunks(child_ino).await?;
                    self.delete_inode_data(child_ino).await?;
                    let _ = self.incr_counter(USED_INODES_KEY, -1).await?;
                    let _ = self
                        .incr_counter(USED_SPACE_KEY, -(attr.length as i64))
                        .await?;
                    deleted += 1;
                } else {
                    attr.nlink -= 1;
                    self.write_attr_internal(child_ino, &attr).await?;
                }
            }
            let _: () = conn.hdel(Self::dir_key(dir_ino), &name).await?;
        }
        Ok(deleted)
    }

    async fn dump_entry_recursive(&self, inode: Ino) -> Result<DumpedEntry, MetaError> {
        let mut conn = self.client();
        let attr = self.read_attr_internal(inode).await?;
        let symlink = if attr.is_symlink() {
            conn.get(Self::symlink_key(inode)).await?
        } else {
            None
        };
        let raw_xattrs: BTreeMap<String, Vec<u8>> = conn.hgetall(Self::xattr_key(inode)).await?;
        let xattrs = raw_xattrs
            .into_iter()
            .map(|(name, value)| DumpedXattr { name, value })
            .collect();

        let mut chunks = Vec::new();
        if attr.is_file() {
            let pattern = format!("c{}_*", inode.as_u64());
            let mut keys = scan_keys(&mut conn, &pattern).await?;
            keys.sort();
            for key in keys {
                if let Some(indx_str) = key.strip_prefix(&format!("c{}_", inode.as_u64()))
                    && let Ok(indx) = indx_str.parse::<u32>()
                {
                    let blob: Option<Vec<u8>> = conn.get(&key).await?;
                    if let Some(bytes) = blob {
                        chunks.push(DumpedChunk {
                            indx,
                            slices: decode_slices(&bytes)?,
                        });
                    }
                }
            }
            chunks.sort_by_key(|c| c.indx);
        }

        let mut entries = BTreeMap::new();
        if attr.is_dir() {
            let dir_entries: BTreeMap<String, Vec<u8>> = conn.hgetall(Self::dir_key(inode)).await?;
            for (name, bytes) in dir_entries {
                let (child_ino, _) = decode_dir_entry(&bytes)?;
                let child = Box::pin(self.dump_entry_recursive(child_ino)).await?;
                entries.insert(name, child);
            }
        }

        Ok(DumpedEntry {
            inode,
            attr,
            symlink,
            xattrs,
            chunks,
            entries,
        })
    }

    async fn load_entry_recursive(
        &self,
        entry: &DumpedEntry,
        seen: &mut HashSet<u64>,
    ) -> Result<(), MetaError> {
        let mut conn = self.client();
        let ino = entry.inode;
        if seen.insert(ino.as_u64()) {
            self.write_attr_internal(ino, &entry.attr).await?;
            if let Some(target) = &entry.symlink {
                let _: () = conn.set(Self::symlink_key(ino), target).await?;
            }
            for x in &entry.xattrs {
                let _: () = conn.hset(Self::xattr_key(ino), &x.name, &x.value).await?;
            }
            for chunk in &entry.chunks {
                let mut blob = Vec::with_capacity(chunk.slices.len() * SLICE_BYTES);
                for rec in &chunk.slices {
                    blob.extend_from_slice(&encode_slice(*rec));
                    if rec.id > 0 {
                        self.update_slice_ref(rec.id, rec.size, 1).await?;
                    }
                }
                let _: () = conn.set(Self::chunk_key(ino, chunk.indx), blob).await?;
            }
        }
        for (name, child) in &entry.entries {
            let _: () = conn
                .hset(
                    Self::dir_key(ino),
                    name,
                    encode_dir_entry(child.inode, child.attr.typ),
                )
                .await?;
            Box::pin(self.load_entry_recursive(child, seen)).await?;
        }
        Ok(())
    }

    async fn clone_recursive(
        &self,
        src_ino: Ino,
        dst_parent: Ino,
        dst_name: &str,
        now: i64,
    ) -> Result<(Ino, Attr), MetaError> {
        let mut conn = self.client();
        let mut attr = self.read_attr_internal(src_ino).await?;
        let new_ino = Ino(self.incr_counter(Self::NEXT_INODE_KEY, 1).await? as u64);
        let _ = self.incr_counter(USED_INODES_KEY, 1).await?;

        let sec = now / 1_000_000_000;
        let nsec = (now % 1_000_000_000) as u32;
        attr.parent = dst_parent;
        attr.nlink = if attr.is_dir() { 2 } else { 1 };
        attr.atime = sec;
        attr.mtime = sec;
        attr.ctime = sec;
        attr.atime_nsec = nsec;
        attr.mtime_nsec = nsec;
        attr.ctime_nsec = nsec;

        self.write_attr_internal(new_ino, &attr).await?;
        let _: () = conn
            .hset(
                Self::dir_key(dst_parent),
                dst_name,
                encode_dir_entry(new_ino, attr.typ),
            )
            .await?;

        if attr.is_file() {
            if attr.length > 0 {
                let _ = self
                    .incr_counter(USED_SPACE_KEY, attr.length as i64)
                    .await?;
            }
            let pattern = format!("c{}_*", src_ino.as_u64());
            for key in scan_keys(&mut conn, &pattern).await? {
                if let Some(indx_str) = key.strip_prefix(&format!("c{}_", src_ino.as_u64()))
                    && let Ok(indx) = indx_str.parse::<u32>()
                    && let Some(blob) = conn.get::<_, Option<Vec<u8>>>(&key).await?
                {
                    let _: () = conn.set(Self::chunk_key(new_ino, indx), &blob).await?;
                    for rec in decode_slices(&blob)? {
                        if rec.id > 0 {
                            self.update_slice_ref(rec.id, rec.size, 1).await?;
                        }
                    }
                }
            }
        } else if attr.is_symlink() {
            if let Some(target) = conn
                .get::<_, Option<String>>(Self::symlink_key(src_ino))
                .await?
            {
                let _: () = conn.set(Self::symlink_key(new_ino), target).await?;
            }
        } else if attr.is_dir() {
            let mut pattr = self.read_attr_internal(dst_parent).await?;
            pattr.nlink += 1;
            self.write_attr_internal(dst_parent, &pattr).await?;
            let children: BTreeMap<String, Vec<u8>> = conn.hgetall(Self::dir_key(src_ino)).await?;
            for (cname, bytes) in children {
                let (cino, _) = decode_dir_entry(&bytes)?;
                Box::pin(self.clone_recursive(cino, new_ino, &cname, now)).await?;
            }
        }

        let xattrs: BTreeMap<String, Vec<u8>> = conn.hgetall(Self::xattr_key(src_ino)).await?;
        for (xname, xval) in xattrs {
            let _: () = conn.hset(Self::xattr_key(new_ino), xname, xval).await?;
        }

        Ok((new_ino, attr))
    }
}

fn redact_url(url: &str) -> String {
    if let Some((scheme, rest)) = url.split_once("://")
        && let Some((_, host)) = rest.split_once('@')
    {
        return format!("{scheme}://***@{host}");
    }
    url.to_owned()
}

fn flock_key(inode: Ino) -> String {
    format!("flock:{}", inode.as_u64())
}

fn plock_key(inode: Ino) -> String {
    format!("plock:{}", inode.as_u64())
}

fn ug_field(target: QuotaTarget) -> String {
    match target {
        QuotaTarget::User(uid) => format!("u:{uid}"),
        QuotaTarget::Group(gid) => format!("g:{gid}"),
    }
}

fn parse_ug_field(field: &str) -> Result<QuotaTarget, MetaError> {
    if let Some(id_str) = field.strip_prefix("u:") {
        let uid: u32 = id_str
            .parse()
            .map_err(|_| MetaError::InvalidInput(format!("invalid ug quota key: {field}")))?;
        return Ok(QuotaTarget::User(uid));
    }
    if let Some(id_str) = field.strip_prefix("g:") {
        let gid: u32 = id_str
            .parse()
            .map_err(|_| MetaError::InvalidInput(format!("invalid ug quota key: {field}")))?;
        return Ok(QuotaTarget::Group(gid));
    }
    Err(MetaError::InvalidInput(format!(
        "invalid ug quota key: {field}"
    )))
}

fn parse_slice_ref(raw: &str) -> Result<(u32, i64), MetaError> {
    let (size_str, refs_str) = raw
        .split_once(':')
        .ok_or_else(|| MetaError::InvalidInput(format!("corrupt slice ref: {raw}")))?;
    let size: u32 = size_str
        .parse()
        .map_err(|_| MetaError::InvalidInput(format!("corrupt slice ref size: {raw}")))?;
    let refs: i64 = refs_str
        .parse()
        .map_err(|_| MetaError::InvalidInput(format!("corrupt slice ref count: {raw}")))?;
    Ok((size, refs))
}

async fn scan_keys(conn: &mut ConnectionManager, pattern: &str) -> Result<Vec<String>, MetaError> {
    let mut cursor: u64 = 0;
    let mut keys = Vec::new();
    loop {
        let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(1000)
            .query_async(conn)
            .await?;
        keys.extend(batch);
        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }
    Ok(keys)
}

#[async_trait]
impl Meta for RedisMeta {
    async fn init(&self, format: &Format, force: bool) -> Result<(), MetaError> {
        let mut conn = self.client();
        let existing: Option<String> = conn.hget(Self::SETTING_KEY, "format").await?;
        if existing.is_some() && !force {
            return Err(MetaError::AlreadyExists(
                "volume already formatted".to_owned(),
            ));
        }
        if force {
            let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await?;
        }
        let format_json = serde_json::to_string(format)?;
        let _: () = conn.hset(Self::SETTING_KEY, "format", format_json).await?;
        let _: () = conn.set(Self::NEXT_INODE_KEY, 1i64).await?;
        let _: () = conn.set(Self::NEXT_CHUNK_KEY, 0i64).await?;
        let _: () = conn.set(USED_SPACE_KEY, 0i64).await?;
        let _: () = conn.set(USED_INODES_KEY, 1i64).await?;

        let now = now_ns();
        let sec = now / 1_000_000_000;
        let nsec = (now % 1_000_000_000) as u32;
        let root = Attr {
            typ: NodeType::Directory,
            mode: 0o777,
            nlink: 2,
            atime: sec,
            mtime: sec,
            ctime: sec,
            atime_nsec: nsec,
            mtime_nsec: nsec,
            ctime_nsec: nsec,
            parent: ROOT_INODE,
            ..Attr::default()
        };
        self.write_attr_internal(ROOT_INODE, &root).await?;
        if format.trash_days > 0 {
            self.ensure_trash_node(now).await?;
        }
        Ok(())
    }

    async fn load(&self) -> Result<Format, MetaError> {
        self.read_format().await
    }

    async fn statfs(&self) -> Result<StatFs, MetaError> {
        let fmt = self.read_format().await?;
        let used_space = self.get_counter(USED_SPACE_KEY).await?.max(0) as u64;
        let used_inodes = self.get_counter(USED_INODES_KEY).await?.max(0) as u64;
        let total_space = if fmt.capacity > 0 {
            fmt.capacity
        } else {
            1u64 << 50
        };
        let total_inodes = if fmt.inodes > 0 {
            fmt.inodes
        } else {
            10_000_000
        };
        Ok(StatFs {
            total_space,
            avail_space: total_space.saturating_sub(used_space),
            used_inodes,
            avail_inodes: total_inodes.saturating_sub(used_inodes),
        })
    }

    async fn lookup(&self, parent: Ino, name: &str) -> Result<(Ino, Attr), MetaError> {
        if name == "." {
            return Ok((parent, self.read_attr_internal(parent).await?));
        }
        if name == ".." {
            let pattr = self.read_attr_internal(parent).await?;
            let target = if pattr.parent.is_valid() {
                pattr.parent
            } else {
                ROOT_INODE
            };
            return Ok((target, self.read_attr_internal(target).await?));
        }
        if parent == ROOT_INODE
            && name == ".trash"
            && let Ok(tattr) = self.read_attr_internal(TRASH_INODE).await
        {
            return Ok((TRASH_INODE, tattr));
        }
        let mut conn = self.client();
        let raw: Option<Vec<u8>> = conn.hget(Self::dir_key(parent), name).await?;
        let bytes = raw.ok_or_else(|| MetaError::NotFound(name.to_owned()))?;
        let (ino, _) = decode_dir_entry(&bytes)?;
        let attr = self.read_attr_internal(ino).await?;
        Ok((ino, attr))
    }

    async fn getattr(&self, inode: Ino) -> Result<Attr, MetaError> {
        self.read_attr_internal(inode).await
    }

    async fn setattr(
        &self,
        inode: Ino,
        attr: &Attr,
        mask: SetAttrFlags,
    ) -> Result<Attr, MetaError> {
        let fmt = self.read_format_opt().await?;
        let mut cur = self.read_attr_internal(inode).await?;
        let prior_user = cur.uid;
        let prior_group = cur.gid;
        let now = now_ns();
        let sec_now = now / 1_000_000_000;
        let nsec_now = (now % 1_000_000_000) as u32;

        if mask.contains(SetAttrFlags::MODE) {
            cur.mode = attr.mode;
        }
        if mask.contains(SetAttrFlags::UID) {
            cur.uid = attr.uid;
        }
        if mask.contains(SetAttrFlags::GID) {
            cur.gid = attr.gid;
        }
        if fmt.is_some_and(|f| f.user_group_quota) && inode != ROOT_INODE && !inode.is_trash() {
            let space = if cur.is_file() { cur.length as i64 } else { 0 };
            if cur.uid != prior_user {
                self.apply_ug_quota_targets(&[QuotaTarget::User(cur.uid)], space, 1)
                    .await?;
                self.apply_ug_quota_targets(&[QuotaTarget::User(prior_user)], -space, -1)
                    .await?;
            }
            if cur.gid != prior_group {
                self.apply_ug_quota_targets(&[QuotaTarget::Group(cur.gid)], space, 1)
                    .await?;
                self.apply_ug_quota_targets(&[QuotaTarget::Group(prior_group)], -space, -1)
                    .await?;
            }
        }
        if mask.contains(SetAttrFlags::ATIME_NOW) {
            cur.atime = sec_now;
            cur.atime_nsec = nsec_now;
        } else if mask.contains(SetAttrFlags::ATIME) {
            cur.atime = attr.atime;
            cur.atime_nsec = attr.atime_nsec;
        }
        if mask.contains(SetAttrFlags::MTIME_NOW) {
            cur.mtime = sec_now;
            cur.mtime_nsec = nsec_now;
        } else if mask.contains(SetAttrFlags::MTIME) {
            cur.mtime = attr.mtime;
            cur.mtime_nsec = attr.mtime_nsec;
        }
        if mask.contains(SetAttrFlags::CTIME_NOW) {
            cur.ctime = sec_now;
            cur.ctime_nsec = nsec_now;
        } else if mask.contains(SetAttrFlags::CTIME) {
            cur.ctime = attr.ctime;
            cur.ctime_nsec = attr.ctime_nsec;
        } else {
            cur.ctime = sec_now;
            cur.ctime_nsec = nsec_now;
        }
        self.write_attr_internal(inode, &cur).await?;
        Ok(cur)
    }

    async fn mknod(
        &self,
        parent: Ino,
        name: &str,
        typ: NodeType,
        mode: u16,
        uid: u32,
        gid: u32,
    ) -> Result<(Ino, Attr), MetaError> {
        validate_name(name)?;
        if typ == NodeType::Directory {
            return self.mkdir(parent, name, mode, uid, gid).await;
        }
        let fmt = self.read_format_opt().await?;
        let pattr = self.read_attr_internal(parent).await?;
        if !pattr.is_dir() {
            return Err(MetaError::NotDir(format!("inode {parent}")));
        }
        let mut conn = self.client();
        if conn.hexists(Self::dir_key(parent), name).await? {
            return Err(MetaError::AlreadyExists(name.to_owned()));
        }
        self.apply_dir_quota_delta(&[parent], 0, 1).await?;
        if fmt.as_ref().is_some_and(|f| f.user_group_quota) {
            self.apply_ug_quota_targets(&[QuotaTarget::User(uid), QuotaTarget::Group(gid)], 0, 1)
                .await?;
        }
        let ino = Ino(self.incr_counter(Self::NEXT_INODE_KEY, 1).await? as u64);
        let _ = self.incr_counter(USED_INODES_KEY, 1).await?;
        let now = now_ns();
        let sec = now / 1_000_000_000;
        let nsec = (now % 1_000_000_000) as u32;
        let attr = Attr {
            typ,
            mode,
            uid,
            gid,
            nlink: 1,
            atime: sec,
            mtime: sec,
            ctime: sec,
            atime_nsec: nsec,
            mtime_nsec: nsec,
            ctime_nsec: nsec,
            parent,
            ..Attr::default()
        };
        self.write_attr_internal(ino, &attr).await?;
        let _: () = conn
            .hset(Self::dir_key(parent), name, encode_dir_entry(ino, typ))
            .await?;
        if fmt.as_ref().is_some_and(|f| f.dir_stats) {
            self.apply_dir_stats_delta(&[parent], 0, align4k(0), 1)
                .await?;
        }
        if fmt.as_ref().is_some_and(|f| f.change_log) {
            self.record_changelog(&serde_json::json!({
                "op": "CREATE",
                "parent": parent.as_u64(),
                "name": name,
                "inode": ino.as_u64(),
                "type": typ as u8,
                "ts": now,
            }))
            .await?;
        }
        self.update_parent_mtime(parent, now).await?;
        Ok((ino, attr))
    }

    async fn mkdir(
        &self,
        parent: Ino,
        name: &str,
        mode: u16,
        uid: u32,
        gid: u32,
    ) -> Result<(Ino, Attr), MetaError> {
        validate_name(name)?;
        let fmt = self.read_format_opt().await?;
        let mut pattr = self.read_attr_internal(parent).await?;
        if !pattr.is_dir() {
            return Err(MetaError::NotDir(format!("inode {parent}")));
        }
        let mut conn = self.client();
        if conn.hexists(Self::dir_key(parent), name).await? {
            return Err(MetaError::AlreadyExists(name.to_owned()));
        }
        self.apply_dir_quota_delta(&[parent], 0, 1).await?;
        if fmt.as_ref().is_some_and(|f| f.user_group_quota) {
            self.apply_ug_quota_targets(&[QuotaTarget::User(uid), QuotaTarget::Group(gid)], 0, 1)
                .await?;
        }
        let ino = Ino(self.incr_counter(Self::NEXT_INODE_KEY, 1).await? as u64);
        let _ = self.incr_counter(USED_INODES_KEY, 1).await?;
        let now = now_ns();
        let sec = now / 1_000_000_000;
        let nsec = (now % 1_000_000_000) as u32;
        let attr = Attr {
            typ: NodeType::Directory,
            mode,
            uid,
            gid,
            nlink: 2,
            atime: sec,
            mtime: sec,
            ctime: sec,
            atime_nsec: nsec,
            mtime_nsec: nsec,
            ctime_nsec: nsec,
            parent,
            ..Attr::default()
        };
        self.write_attr_internal(ino, &attr).await?;
        let _: () = conn
            .hset(
                Self::dir_key(parent),
                name,
                encode_dir_entry(ino, NodeType::Directory),
            )
            .await?;
        pattr.nlink += 1;
        pattr.mtime = sec;
        pattr.mtime_nsec = nsec;
        pattr.ctime = sec;
        pattr.ctime_nsec = nsec;
        self.write_attr_internal(parent, &pattr).await?;
        if fmt.as_ref().is_some_and(|f| f.dir_stats) {
            self.apply_dir_stats_delta(&[parent], 0, align4k(0), 1)
                .await?;
        }
        if fmt.as_ref().is_some_and(|f| f.change_log) {
            self.record_changelog(&serde_json::json!({
                "op": "MKDIR",
                "parent": parent.as_u64(),
                "name": name,
                "inode": ino.as_u64(),
                "ts": now,
            }))
            .await?;
        }
        Ok((ino, attr))
    }

    async fn symlink(
        &self,
        parent: Ino,
        name: &str,
        target: &str,
        uid: u32,
        gid: u32,
    ) -> Result<(Ino, Attr), MetaError> {
        validate_name(name)?;
        if target.is_empty() || target.len() > MAX_SYMLINK {
            return Err(MetaError::InvalidInput("invalid symlink target".to_owned()));
        }
        let fmt = self.read_format_opt().await?;
        let pattr = self.read_attr_internal(parent).await?;
        if !pattr.is_dir() {
            return Err(MetaError::NotDir(format!("inode {parent}")));
        }
        let mut conn = self.client();
        if conn.hexists(Self::dir_key(parent), name).await? {
            return Err(MetaError::AlreadyExists(name.to_owned()));
        }
        self.apply_dir_quota_delta(&[parent], 0, 1).await?;
        if fmt.as_ref().is_some_and(|f| f.user_group_quota) {
            self.apply_ug_quota_targets(&[QuotaTarget::User(uid), QuotaTarget::Group(gid)], 0, 1)
                .await?;
        }
        let ino = Ino(self.incr_counter(Self::NEXT_INODE_KEY, 1).await? as u64);
        let _ = self.incr_counter(USED_INODES_KEY, 1).await?;
        let now = now_ns();
        let sec = now / 1_000_000_000;
        let nsec = (now % 1_000_000_000) as u32;
        let attr = Attr {
            typ: NodeType::Symlink,
            mode: 0o777,
            uid,
            gid,
            nlink: 1,
            length: target.len() as u64,
            atime: sec,
            mtime: sec,
            ctime: sec,
            atime_nsec: nsec,
            mtime_nsec: nsec,
            ctime_nsec: nsec,
            parent,
            ..Attr::default()
        };
        self.write_attr_internal(ino, &attr).await?;
        let _: () = conn.set(Self::symlink_key(ino), target).await?;
        let _: () = conn
            .hset(
                Self::dir_key(parent),
                name,
                encode_dir_entry(ino, NodeType::Symlink),
            )
            .await?;
        if fmt.as_ref().is_some_and(|f| f.dir_stats) {
            self.apply_dir_stats_delta(
                &[parent],
                target.len() as i64,
                align4k(target.len() as u64),
                1,
            )
            .await?;
        }
        self.update_parent_mtime(parent, now).await?;
        Ok((ino, attr))
    }

    async fn readlink(&self, inode: Ino) -> Result<String, MetaError> {
        let mut conn = self.client();
        let target: Option<String> = conn.get(Self::symlink_key(inode)).await?;
        target.ok_or_else(|| MetaError::NotFound(format!("symlink {inode}")))
    }

    async fn link(&self, inode: Ino, new_parent: Ino, new_name: &str) -> Result<Attr, MetaError> {
        validate_name(new_name)?;
        let fmt = self.read_format_opt().await?;
        let pattr = self.read_attr_internal(new_parent).await?;
        if !pattr.is_dir() {
            return Err(MetaError::NotDir(format!("inode {new_parent}")));
        }
        let mut attr = self.read_attr_internal(inode).await?;
        if attr.is_dir() {
            return Err(MetaError::IsDir(format!("inode {inode}")));
        }
        let mut conn = self.client();
        if conn.hexists(Self::dir_key(new_parent), new_name).await? {
            return Err(MetaError::AlreadyExists(new_name.to_owned()));
        }
        self.apply_dir_quota_delta(&[new_parent], attr.length as i64, 1)
            .await?;
        let _: () = conn
            .hset(
                Self::dir_key(new_parent),
                new_name,
                encode_dir_entry(inode, attr.typ),
            )
            .await?;
        if fmt.as_ref().is_some_and(|f| f.dir_stats) {
            self.apply_dir_stats_delta(&[new_parent], attr.length as i64, align4k(attr.length), 1)
                .await?;
        }
        let now = now_ns();
        attr.nlink += 1;
        attr.parent = Ino(0);
        attr.ctime = now / 1_000_000_000;
        attr.ctime_nsec = (now % 1_000_000_000) as u32;
        self.write_attr_internal(inode, &attr).await?;
        self.update_parent_mtime(new_parent, now).await?;
        Ok(attr)
    }

    async fn unlink(&self, parent: Ino, name: &str) -> Result<(), MetaError> {
        validate_name(name)?;
        let fmt = self.read_format_opt().await?;
        let mut conn = self.client();
        let raw: Option<Vec<u8>> = conn.hget(Self::dir_key(parent), name).await?;
        let bytes = raw.ok_or_else(|| MetaError::NotFound(name.to_owned()))?;
        let (ino, typ) = decode_dir_entry(&bytes)?;
        if typ == NodeType::Directory {
            return Err(MetaError::IsDir(name.to_owned()));
        }
        let mut attr = self.read_attr_internal(ino).await?;
        self.apply_dir_quota_delta(&[parent], -(attr.length as i64), -1)
            .await?;
        if fmt.as_ref().is_some_and(|f| f.dir_stats) {
            self.apply_dir_stats_delta(&[parent], -(attr.length as i64), -align4k(attr.length), -1)
                .await?;
        }
        let _: () = conn.hdel(Self::dir_key(parent), name).await?;
        let now = now_ns();
        if attr.nlink <= 1 {
            if fmt.as_ref().is_some_and(|f| f.user_group_quota) {
                self.apply_ug_quota_targets(
                    &[QuotaTarget::User(attr.uid), QuotaTarget::Group(attr.gid)],
                    -(attr.length as i64),
                    -1,
                )
                .await?;
            }
            self.decrement_file_chunks(ino).await?;
            self.delete_inode_data(ino).await?;
            let _ = self.incr_counter(USED_INODES_KEY, -1).await?;
            let _ = self
                .incr_counter(USED_SPACE_KEY, -(attr.length as i64))
                .await?;
        } else {
            attr.nlink -= 1;
            attr.ctime = now / 1_000_000_000;
            attr.ctime_nsec = (now % 1_000_000_000) as u32;
            self.write_attr_internal(ino, &attr).await?;
        }
        if fmt.as_ref().is_some_and(|f| f.change_log) {
            self.record_changelog(&serde_json::json!({
                "op": "UNLINK",
                "parent": parent.as_u64(),
                "name": name,
                "inode": ino.as_u64(),
                "ts": now,
            }))
            .await?;
        }
        self.update_parent_mtime(parent, now).await?;
        Ok(())
    }

    async fn rmdir(&self, parent: Ino, name: &str) -> Result<(), MetaError> {
        validate_name(name)?;
        let fmt = self.read_format_opt().await?;
        let mut conn = self.client();
        let raw: Option<Vec<u8>> = conn.hget(Self::dir_key(parent), name).await?;
        let bytes = raw.ok_or_else(|| MetaError::NotFound(name.to_owned()))?;
        let (ino, typ) = decode_dir_entry(&bytes)?;
        if typ != NodeType::Directory {
            return Err(MetaError::NotDir(name.to_owned()));
        }
        let child_count: usize = conn.hlen(Self::dir_key(ino)).await?;
        if child_count > 0 {
            return Err(MetaError::NotEmpty(name.to_owned()));
        }
        let dir_attr = self.read_attr_internal(ino).await?;
        self.apply_dir_quota_delta(&[parent], 0, -1).await?;
        if fmt.as_ref().is_some_and(|f| f.user_group_quota) {
            self.apply_ug_quota_targets(
                &[
                    QuotaTarget::User(dir_attr.uid),
                    QuotaTarget::Group(dir_attr.gid),
                ],
                0,
                -1,
            )
            .await?;
        }
        if fmt.as_ref().is_some_and(|f| f.dir_stats) {
            self.apply_dir_stats_delta(&[parent], 0, -align4k(0), -1)
                .await?;
        }
        let _: () = conn.hdel(Self::dir_key(parent), name).await?;
        let _: () = conn.hdel(DIR_QUOTA_KEY, ino.as_u64().to_string()).await?;
        let _: () = conn.hdel(DIR_STATS_KEY, ino.as_u64().to_string()).await?;
        let _: () = conn.del(Self::dir_key(ino)).await?;
        self.delete_inode_data(ino).await?;
        let mut pattr = self.read_attr_internal(parent).await?;
        pattr.nlink = pattr.nlink.saturating_sub(1).max(2);
        self.write_attr_internal(parent, &pattr).await?;
        let _ = self.incr_counter(USED_INODES_KEY, -1).await?;
        let now = now_ns();
        if fmt.as_ref().is_some_and(|f| f.change_log) {
            self.record_changelog(&serde_json::json!({
                "op": "RMDIR",
                "parent": parent.as_u64(),
                "name": name,
                "inode": ino.as_u64(),
                "ts": now,
            }))
            .await?;
        }
        self.update_parent_mtime(parent, now).await?;
        Ok(())
    }

    async fn rename(
        &self,
        src_parent: Ino,
        src_name: &str,
        dst_parent: Ino,
        dst_name: &str,
    ) -> Result<(), MetaError> {
        validate_name(src_name)?;
        validate_name(dst_name)?;
        let fmt = self.read_format().await?;
        let mut conn = self.client();
        let raw: Option<Vec<u8>> = conn.hget(Self::dir_key(src_parent), src_name).await?;
        let bytes = raw.ok_or_else(|| MetaError::NotFound(src_name.to_owned()))?;
        let (src_ino, src_typ) = decode_dir_entry(&bytes)?;
        let dst_pattr = self.read_attr_internal(dst_parent).await?;
        if !dst_pattr.is_dir() {
            return Err(MetaError::NotDir(format!("inode {dst_parent}")));
        }
        if src_parent == dst_parent && src_name == dst_name {
            return Ok(());
        }
        self.remove_existing_dst(dst_parent, dst_name, &fmt).await?;
        let is_dir = src_typ == NodeType::Directory;
        if src_parent != dst_parent {
            let (moved_len, moved_aligned, moved_inodes) = if is_dir {
                let (l, a, i) = self.calc_dir_usage(src_ino).await?;
                (l, a + align4k(0), i + 1)
            } else {
                let src_attr = self.read_attr_internal(src_ino).await?;
                (src_attr.length as i64, align4k(src_attr.length), 1)
            };
            self.apply_dir_quota_delta(&[src_parent], -moved_len, -moved_inodes)
                .await?;
            self.apply_dir_quota_delta(&[dst_parent], moved_len, moved_inodes)
                .await?;
            if fmt.dir_stats {
                self.apply_dir_stats_delta(
                    &[src_parent],
                    -moved_len,
                    -moved_aligned,
                    -moved_inodes,
                )
                .await?;
                self.apply_dir_stats_delta(&[dst_parent], moved_len, moved_aligned, moved_inodes)
                    .await?;
            }
        }
        let _: () = conn.hdel(Self::dir_key(src_parent), src_name).await?;
        let _: () = conn
            .hset(
                Self::dir_key(dst_parent),
                dst_name,
                encode_dir_entry(src_ino, src_typ),
            )
            .await?;
        if is_dir && src_parent != dst_parent {
            let mut src_pattr = self.read_attr_internal(src_parent).await?;
            src_pattr.nlink = src_pattr.nlink.saturating_sub(1).max(2);
            self.write_attr_internal(src_parent, &src_pattr).await?;
            let mut dst_pattr = self.read_attr_internal(dst_parent).await?;
            dst_pattr.nlink += 1;
            self.write_attr_internal(dst_parent, &dst_pattr).await?;
        }
        let mut src_attr = self.read_attr_internal(src_ino).await?;
        if src_attr.parent.is_valid() {
            src_attr.parent = dst_parent;
            self.write_attr_internal(src_ino, &src_attr).await?;
        }
        let now = now_ns();
        if fmt.change_log {
            self.record_changelog(&serde_json::json!({
                "op": "RENAME",
                "src_parent": src_parent.as_u64(),
                "src_name": src_name,
                "dst_parent": dst_parent.as_u64(),
                "dst_name": dst_name,
                "inode": src_ino.as_u64(),
                "ts": now,
            }))
            .await?;
        }
        self.update_parent_mtime(src_parent, now).await?;
        if dst_parent != src_parent {
            self.update_parent_mtime(dst_parent, now).await?;
        }
        Ok(())
    }

    async fn readdir(&self, inode: Ino) -> Result<Vec<Entry>, MetaError> {
        let dir_attr = self.read_attr_internal(inode).await?;
        if !dir_attr.is_dir() {
            return Err(MetaError::NotDir(format!("inode {inode}")));
        }
        let mut conn = self.client();
        let raw_entries: BTreeMap<String, Vec<u8>> = conn.hgetall(Self::dir_key(inode)).await?;
        let mut out = Vec::with_capacity(raw_entries.len());
        for (name, bytes) in raw_entries {
            let (child_ino, _) = decode_dir_entry(&bytes)?;
            let attr = self.read_attr_internal(child_ino).await?;
            out.push(Entry {
                inode: child_ino,
                name,
                attr,
            });
        }
        Ok(out)
    }

    async fn new_slice(&self) -> Result<u64, MetaError> {
        Ok(self.incr_counter(Self::NEXT_CHUNK_KEY, 1).await? as u64)
    }

    async fn read_chunk(&self, inode: Ino, indx: u32) -> Result<Vec<Slice>, MetaError> {
        let mut conn = self.client();
        let blob: Option<Vec<u8>> = conn.get(Self::chunk_key(inode, indx)).await?;
        let Some(bytes) = blob else {
            return Ok(Vec::new());
        };
        let records = decode_slices(&bytes)?;
        Ok(build_slice(&records))
    }

    async fn write_chunk(
        &self,
        inode: Ino,
        indx: u32,
        pos: u32,
        slice: Slice,
        new_file_length: u64,
    ) -> Result<Attr, MetaError> {
        let fmt = self.read_format_opt().await?;
        let mut attr = self.read_attr_internal(inode).await?;
        if !attr.is_file() {
            return Err(MetaError::IsDir(format!("inode {inode}")));
        }
        let encoded = encode_slice(SliceRecord {
            pos,
            id: slice.id,
            size: slice.size,
            off: slice.off,
            len: slice.len,
        });
        let mut conn = self.client();
        let _: () = conn
            .append(Self::chunk_key(inode, indx), &encoded[..])
            .await?;
        if slice.id > 0 {
            self.update_slice_ref(slice.id, slice.size, 1).await?;
        }
        let now = now_ns();
        let old_len = attr.length;
        let final_len = old_len.max(new_file_length);
        if final_len > old_len {
            let delta = (final_len - old_len) as i64;
            let parent_dirs = self.file_parents(inode, &attr).await?;
            self.apply_dir_quota_delta(&parent_dirs, delta, 0).await?;
            if fmt.as_ref().is_some_and(|f| f.user_group_quota) {
                self.apply_ug_quota_targets(
                    &[QuotaTarget::User(attr.uid), QuotaTarget::Group(attr.gid)],
                    delta,
                    0,
                )
                .await?;
            }
            if fmt.as_ref().is_some_and(|f| f.dir_stats) {
                self.apply_dir_stats_delta(
                    &parent_dirs,
                    delta,
                    align4k(final_len) - align4k(old_len),
                    0,
                )
                .await?;
            }
            let _ = self.incr_counter(USED_SPACE_KEY, delta).await?;
        }
        attr.length = final_len;
        attr.mtime = now / 1_000_000_000;
        attr.mtime_nsec = (now % 1_000_000_000) as u32;
        attr.ctime = attr.mtime;
        attr.ctime_nsec = attr.mtime_nsec;
        self.write_attr_internal(inode, &attr).await?;
        if fmt.as_ref().is_some_and(|f| f.change_log) {
            self.record_changelog(&serde_json::json!({
                "op": "WRITE",
                "inode": inode.as_u64(),
                "indx": indx,
                "pos": pos,
                "len": slice.len,
                "length": final_len,
                "ts": now,
            }))
            .await?;
        }
        Ok(attr)
    }

    async fn truncate(&self, inode: Ino, length: u64) -> Result<Attr, MetaError> {
        let fmt = self.read_format_opt().await?;
        let mut attr = self.read_attr_internal(inode).await?;
        if !attr.is_file() {
            return Err(MetaError::IsDir(format!("inode {inode}")));
        }
        let old_len = attr.length;
        if length < old_len {
            let first_drop_indx = length.div_ceil(CHUNK_SIZE) as u32;
            let mut conn = self.client();
            let pattern = format!("c{}_*", inode.as_u64());
            for key in scan_keys(&mut conn, &pattern).await? {
                if let Some(indx_str) = key.strip_prefix(&format!("c{}_", inode.as_u64()))
                    && let Ok(indx) = indx_str.parse::<u32>()
                    && indx >= first_drop_indx
                {
                    if let Some(bytes) = conn.get::<_, Option<Vec<u8>>>(&key).await? {
                        for rec in decode_slices(&bytes)? {
                            if rec.id > 0 {
                                self.update_slice_ref(rec.id, rec.size, -1).await?;
                            }
                        }
                    }
                    let _: () = conn.del(&key).await?;
                }
            }
        }
        let delta = length as i64 - old_len as i64;
        let delta_space = align4k(length) - align4k(old_len);
        if delta != 0 || (fmt.as_ref().is_some_and(|f| f.dir_stats) && delta_space != 0) {
            let parent_dirs = self.file_parents(inode, &attr).await?;
            if delta != 0 {
                self.apply_dir_quota_delta(&parent_dirs, delta, 0).await?;
                if fmt.as_ref().is_some_and(|f| f.user_group_quota) {
                    self.apply_ug_quota_targets(
                        &[QuotaTarget::User(attr.uid), QuotaTarget::Group(attr.gid)],
                        delta,
                        0,
                    )
                    .await?;
                }
                let _ = self.incr_counter(USED_SPACE_KEY, delta).await?;
            }
            if fmt.as_ref().is_some_and(|f| f.dir_stats) {
                self.apply_dir_stats_delta(&parent_dirs, delta, delta_space, 0)
                    .await?;
            }
        }
        let now = now_ns();
        attr.length = length;
        attr.mtime = now / 1_000_000_000;
        attr.mtime_nsec = (now % 1_000_000_000) as u32;
        attr.ctime = attr.mtime;
        attr.ctime_nsec = attr.mtime_nsec;
        self.write_attr_internal(inode, &attr).await?;
        Ok(attr)
    }

    async fn set_xattr(&self, inode: Ino, name: &str, value: &[u8]) -> Result<(), MetaError> {
        let _ = self.read_attr_internal(inode).await?;
        let mut conn = self.client();
        let _: () = conn.hset(Self::xattr_key(inode), name, value).await?;
        Ok(())
    }

    async fn get_xattr(&self, inode: Ino, name: &str) -> Result<Vec<u8>, MetaError> {
        let mut conn = self.client();
        let val: Option<Vec<u8>> = conn.hget(Self::xattr_key(inode), name).await?;
        val.ok_or_else(|| MetaError::NotFound(name.to_owned()))
    }

    async fn list_xattr(&self, inode: Ino) -> Result<Vec<String>, MetaError> {
        let mut conn = self.client();
        let mut names: Vec<String> = conn.hkeys(Self::xattr_key(inode)).await?;
        names.sort();
        Ok(names)
    }

    async fn remove_xattr(&self, inode: Ino, name: &str) -> Result<(), MetaError> {
        let mut conn = self.client();
        let removed: usize = conn.hdel(Self::xattr_key(inode), name).await?;
        if removed == 0 {
            return Err(MetaError::NotFound(name.to_owned()));
        }
        Ok(())
    }

    async fn list_orphan_slices(&self) -> Result<Vec<(u64, u32)>, MetaError> {
        let mut conn = self.client();
        let all: BTreeMap<String, String> = conn.hgetall(SLICE_REFS_KEY).await?;
        let mut out = Vec::new();
        for (id_str, val) in all {
            let (size, refs) = parse_slice_ref(&val)?;
            if refs <= 0
                && let Ok(id) = id_str.parse::<u64>()
            {
                out.push((id, size));
            }
        }
        out.sort_by_key(|&(id, _)| id);
        Ok(out)
    }

    async fn purge_orphan_slice(&self, slice_id: u64) -> Result<(), MetaError> {
        let mut conn = self.client();
        let field = slice_id.to_string();
        if let Some(raw) = conn
            .hget::<_, _, Option<String>>(SLICE_REFS_KEY, &field)
            .await?
        {
            let (_, refs) = parse_slice_ref(&raw)?;
            if refs <= 0 {
                let _: () = conn.hdel(SLICE_REFS_KEY, &field).await?;
            }
        }
        Ok(())
    }

    async fn flock(&self, inode: Ino, owner: u64, ltype: FlockType) -> Result<(), MetaError> {
        let mut conn = self.client();
        let key = flock_key(inode);
        let self_field = format!("0:{owner}");
        if ltype == FlockType::Unlock {
            let _: () = conn.hdel(&key, &self_field).await?;
            return Ok(());
        }
        let _ = self.read_attr_internal(inode).await?;
        let all: BTreeMap<String, u8> = conn.hgetall(&key).await?;
        for (field, raw_typ) in all {
            if field == self_field {
                continue;
            }
            let other_typ = FlockType::try_from(raw_typ)?;
            if ltype == FlockType::Exclusive || other_typ == FlockType::Exclusive {
                return Err(MetaError::WouldBlock(format!(
                    "flock conflict on inode {inode}"
                )));
            }
        }
        let _: () = conn.hset(&key, &self_field, ltype as u8).await?;
        Ok(())
    }

    async fn get_lk(
        &self,
        inode: Ino,
        owner: u64,
        lock: PlockRecord,
    ) -> Result<PlockRecord, MetaError> {
        if lock.typ == FlockType::Unlock {
            return Ok(PlockRecord {
                typ: FlockType::Unlock,
                pid: 0,
                start: 0,
                end: 0,
            });
        }
        if lock.start > lock.end {
            return Err(MetaError::InvalidInput(format!(
                "invalid lock range: {}..{}",
                lock.start, lock.end
            )));
        }
        let _ = self.read_attr_internal(inode).await?;
        let mut conn = self.client();
        let all: BTreeMap<String, Vec<u8>> = conn.hgetall(plock_key(inode)).await?;
        let self_field = format!("0:{owner}");
        for (field, blob) in all {
            if field == self_field {
                continue;
            }
            let sid_zero = field.starts_with("0:");
            for rec in decode_plocks(&blob)? {
                if (lock.typ == FlockType::Exclusive || rec.typ == FlockType::Exclusive)
                    && lock.end >= rec.start
                    && lock.start <= rec.end
                {
                    return Ok(PlockRecord {
                        typ: rec.typ,
                        pid: if sid_zero { rec.pid } else { 0 },
                        start: rec.start,
                        end: rec.end,
                    });
                }
            }
        }
        Ok(PlockRecord {
            typ: FlockType::Unlock,
            pid: 0,
            start: 0,
            end: 0,
        })
    }

    async fn set_lk(&self, inode: Ino, owner: u64, lock: PlockRecord) -> Result<(), MetaError> {
        if lock.start > lock.end {
            return Err(MetaError::InvalidInput(format!(
                "invalid lock range: {}..{}",
                lock.start, lock.end
            )));
        }
        let _ = self.read_attr_internal(inode).await?;
        let mut conn = self.client();
        let key = plock_key(inode);
        let self_field = format!("0:{owner}");
        if lock.typ == FlockType::Unlock {
            if let Some(blob) = conn
                .hget::<_, _, Option<Vec<u8>>>(&key, &self_field)
                .await?
            {
                let updated = update_plocks(decode_plocks(&blob)?, lock);
                if updated.is_empty() {
                    let _: () = conn.hdel(&key, &self_field).await?;
                } else {
                    let _: () = conn
                        .hset(&key, &self_field, encode_plocks(&updated))
                        .await?;
                }
            }
            return Ok(());
        }
        let all: BTreeMap<String, Vec<u8>> = conn.hgetall(&key).await?;
        let mut self_records = Vec::new();
        for (field, blob) in all {
            let recs = decode_plocks(&blob)?;
            if field == self_field {
                self_records = recs;
                continue;
            }
            for rec in recs {
                if (lock.typ == FlockType::Exclusive || rec.typ == FlockType::Exclusive)
                    && lock.end >= rec.start
                    && lock.start <= rec.end
                {
                    return Err(MetaError::WouldBlock(format!(
                        "plock conflict on inode {inode} range {}..{}",
                        lock.start, lock.end
                    )));
                }
            }
        }
        let updated = update_plocks(self_records, lock);
        let _: () = conn
            .hset(&key, &self_field, encode_plocks(&updated))
            .await?;
        Ok(())
    }

    async fn set_quota(
        &self,
        inode: Ino,
        max_space: i64,
        max_inodes: i64,
    ) -> Result<Quota, MetaError> {
        if inode.is_trash() {
            return Err(MetaError::InvalidInput(
                "cannot set quota on trash inode".to_owned(),
            ));
        }
        let attr = self.read_attr_internal(inode).await?;
        if !attr.is_dir() {
            return Err(MetaError::NotDir(format!("inode {inode}")));
        }
        let mut conn = self.client();
        let field = inode.as_u64().to_string();
        let existing: Option<String> = conn.hget(DIR_QUOTA_KEY, &field).await?;
        let (used_space, used_inodes) = if let Some(json) = existing {
            let q: Quota = serde_json::from_str(&json)?;
            (q.used_space, q.used_inodes)
        } else {
            let (s, _, i) = self.calc_dir_usage(inode).await?;
            (s, i)
        };
        let quota = Quota {
            max_space,
            max_inodes,
            used_space,
            used_inodes,
        };
        let _: () = conn
            .hset(DIR_QUOTA_KEY, &field, serde_json::to_string(&quota)?)
            .await?;
        Ok(quota)
    }

    async fn get_quota(&self, inode: Ino) -> Result<Quota, MetaError> {
        let mut conn = self.client();
        let raw: Option<String> = conn.hget(DIR_QUOTA_KEY, inode.as_u64().to_string()).await?;
        let json = raw.ok_or_else(|| MetaError::NotFound(format!("quota for inode {inode}")))?;
        Ok(serde_json::from_str(&json)?)
    }

    async fn del_quota(&self, inode: Ino) -> Result<(), MetaError> {
        let mut conn = self.client();
        let removed: usize = conn.hdel(DIR_QUOTA_KEY, inode.as_u64().to_string()).await?;
        if removed == 0 {
            return Err(MetaError::NotFound(format!("quota for inode {inode}")));
        }
        Ok(())
    }

    async fn list_quotas(&self) -> Result<Vec<(Ino, Quota)>, MetaError> {
        let mut conn = self.client();
        let all: BTreeMap<String, String> = conn.hgetall(DIR_QUOTA_KEY).await?;
        let mut out = Vec::with_capacity(all.len());
        for (k, v) in all {
            let ino: u64 = k
                .parse()
                .map_err(|_| MetaError::InvalidInput(format!("corrupt quota key: {k}")))?;
            out.push((Ino(ino), serde_json::from_str(&v)?));
        }
        out.sort_by_key(|(ino, _)| ino.as_u64());
        Ok(out)
    }

    async fn move_to_trash(&self, parent: Ino, name: &str) -> Result<String, MetaError> {
        validate_name(name)?;
        if parent.is_trash() {
            return Err(MetaError::InvalidInput(
                "cannot move trash entry into trash".to_owned(),
            ));
        }
        let fmt = self.read_format().await?;
        if fmt.trash_days <= 0 {
            return Err(MetaError::InvalidInput(
                "trash is disabled (trash_days <= 0)".to_owned(),
            ));
        }
        let now = now_ns();
        self.ensure_trash_node(now).await?;
        let mut conn = self.client();
        let raw: Option<Vec<u8>> = conn.hget(Self::dir_key(parent), name).await?;
        let bytes = raw.ok_or_else(|| MetaError::NotFound(name.to_owned()))?;
        let (ino, typ) = decode_dir_entry(&bytes)?;
        let is_dir = typ == NodeType::Directory;
        let trash_name = make_trash_entry_name(parent, ino, name);

        let (moved_len, moved_aligned, moved_inodes) = if is_dir {
            let (l, a, i) = self.calc_dir_usage(ino).await?;
            (l, a + align4k(0), i + 1)
        } else {
            let attr = self.read_attr_internal(ino).await?;
            (attr.length as i64, align4k(attr.length), 1)
        };
        self.apply_dir_quota_delta(&[parent], -moved_len, -moved_inodes)
            .await?;
        if fmt.dir_stats {
            self.apply_dir_stats_delta(&[parent], -moved_len, -moved_aligned, -moved_inodes)
                .await?;
        }
        let _: () = conn.hdel(Self::dir_key(parent), name).await?;
        let _: () = conn
            .hset(
                Self::dir_key(TRASH_INODE),
                &trash_name,
                encode_dir_entry(ino, typ),
            )
            .await?;
        let mut attr = self.read_attr_internal(ino).await?;
        if attr.parent.is_valid() {
            attr.parent = TRASH_INODE;
            self.write_attr_internal(ino, &attr).await?;
        }
        if is_dir {
            let mut pattr = self.read_attr_internal(parent).await?;
            pattr.nlink = pattr.nlink.saturating_sub(1).max(2);
            self.write_attr_internal(parent, &pattr).await?;
            let mut tattr = self.read_attr_internal(TRASH_INODE).await?;
            tattr.nlink += 1;
            self.write_attr_internal(TRASH_INODE, &tattr).await?;
        }
        self.update_parent_mtime(parent, now).await?;
        self.update_parent_mtime(TRASH_INODE, now).await?;
        Ok(trash_name)
    }

    async fn empty_trash(&self) -> Result<u64, MetaError> {
        if self.read_attr_internal(TRASH_INODE).await.is_err() {
            return Ok(0);
        }
        let deleted = self.purge_trash_recursive(TRASH_INODE).await?;
        let now = now_ns();
        if let Ok(mut tattr) = self.read_attr_internal(TRASH_INODE).await {
            tattr.nlink = 2;
            tattr.mtime = now / 1_000_000_000;
            tattr.mtime_nsec = (now % 1_000_000_000) as u32;
            tattr.ctime = tattr.mtime;
            tattr.ctime_nsec = tattr.mtime_nsec;
            self.write_attr_internal(TRASH_INODE, &tattr).await?;
        }
        Ok(deleted)
    }

    async fn restore_from_trash(&self, trash_name: &str) -> Result<(Ino, Attr), MetaError> {
        validate_name(trash_name)?;
        let fmt = self.read_format_opt().await?;
        let mut conn = self.client();
        let raw: Option<Vec<u8>> = conn.hget(Self::dir_key(TRASH_INODE), trash_name).await?;
        let bytes = raw.ok_or_else(|| MetaError::NotFound(trash_name.to_owned()))?;
        let (ino, typ) = decode_dir_entry(&bytes)?;
        let (orig_parent, _orig_ino, orig_name) = parse_trash_entry_name(trash_name)?;
        let mut pattr = self.read_attr_internal(orig_parent).await?;
        if !pattr.is_dir() {
            return Err(MetaError::NotDir(format!("inode {orig_parent}")));
        }
        if conn.hexists(Self::dir_key(orig_parent), orig_name).await? {
            return Err(MetaError::AlreadyExists(orig_name.to_owned()));
        }
        let is_dir = typ == NodeType::Directory;
        let (restored_len, restored_aligned, restored_inodes) = if is_dir {
            let (l, a, i) = self.calc_dir_usage(ino).await?;
            (l, a + align4k(0), i + 1)
        } else {
            let attr = self.read_attr_internal(ino).await?;
            (attr.length as i64, align4k(attr.length), 1)
        };
        self.apply_dir_quota_delta(&[orig_parent], restored_len, restored_inodes)
            .await?;
        if fmt.as_ref().is_some_and(|f| f.dir_stats) {
            self.apply_dir_stats_delta(
                &[orig_parent],
                restored_len,
                restored_aligned,
                restored_inodes,
            )
            .await?;
        }
        let _: () = conn.hdel(Self::dir_key(TRASH_INODE), trash_name).await?;
        let _: () = conn
            .hset(
                Self::dir_key(orig_parent),
                orig_name,
                encode_dir_entry(ino, typ),
            )
            .await?;
        let mut attr = self.read_attr_internal(ino).await?;
        if attr.parent.is_valid() {
            attr.parent = orig_parent;
            self.write_attr_internal(ino, &attr).await?;
        }
        if is_dir {
            let mut tattr = self.read_attr_internal(TRASH_INODE).await?;
            tattr.nlink = tattr.nlink.saturating_sub(1).max(2);
            self.write_attr_internal(TRASH_INODE, &tattr).await?;
            pattr.nlink += 1;
            self.write_attr_internal(orig_parent, &pattr).await?;
        }
        let now = now_ns();
        self.update_parent_mtime(TRASH_INODE, now).await?;
        self.update_parent_mtime(orig_parent, now).await?;
        Ok((ino, attr))
    }

    async fn dump_json(&self) -> Result<String, MetaError> {
        let format = self.read_format().await?;
        let mut counters = BTreeMap::new();
        for name in [
            Self::NEXT_INODE_KEY,
            Self::NEXT_CHUNK_KEY,
            USED_SPACE_KEY,
            USED_INODES_KEY,
        ] {
            counters.insert(name.to_owned(), self.get_counter(name).await?);
        }
        let quotas = self
            .list_quotas()
            .await?
            .into_iter()
            .map(|(ino, q)| (ino.as_u64(), q))
            .collect();
        let orphan_slices = self
            .list_orphan_slices()
            .await?
            .into_iter()
            .map(|(id, size)| DumpedOrphanSlice { id, size })
            .collect();
        let tree = self.dump_entry_recursive(ROOT_INODE).await?;
        let trash = if self.read_attr_internal(TRASH_INODE).await.is_ok() {
            Some(self.dump_entry_recursive(TRASH_INODE).await?)
        } else {
            None
        };
        let dumped = DumpedMeta {
            format,
            counters,
            quotas,
            orphan_slices,
            tree,
            trash,
        };
        Ok(serde_json::to_string_pretty(&dumped)?)
    }

    async fn load_json(&self, json: &str) -> Result<(), MetaError> {
        let dumped: DumpedMeta = serde_json::from_str(json)?;
        let mut conn = self.client();
        let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await?;
        let _: () = conn
            .hset(
                Self::SETTING_KEY,
                "format",
                serde_json::to_string(&dumped.format)?,
            )
            .await?;
        for (k, v) in &dumped.counters {
            let _: () = conn.set(k, *v).await?;
        }
        let mut seen = HashSet::new();
        self.load_entry_recursive(&dumped.tree, &mut seen).await?;
        if let Some(trash) = &dumped.trash {
            self.load_entry_recursive(trash, &mut seen).await?;
        }
        for orphan in &dumped.orphan_slices {
            let field = orphan.id.to_string();
            if !conn.hexists(SLICE_REFS_KEY, &field).await? {
                let _: () = conn
                    .hset(SLICE_REFS_KEY, &field, format!("{}:0", orphan.size))
                    .await?;
            }
        }
        for (ino, quota) in &dumped.quotas {
            let _: () = conn
                .hset(
                    DIR_QUOTA_KEY,
                    ino.to_string(),
                    serde_json::to_string(quota)?,
                )
                .await?;
        }
        Ok(())
    }

    async fn fsck(&self) -> Result<FsckReport, MetaError> {
        let _ = self.read_format().await?;
        let mut conn = self.client();
        let all_inos: Vec<u64> = conn.smembers(ALL_INODES_KEY).await?;
        let mut nodes = BTreeMap::new();
        for ino_u64 in all_inos {
            if let Ok(attr) = self.read_attr_internal(Ino(ino_u64)).await {
                nodes.insert(ino_u64, (attr.typ, attr.nlink));
            }
        }
        let mut edges = Vec::new();
        for (&parent, &(typ, _)) in &nodes {
            if typ == NodeType::Directory {
                let raw: BTreeMap<String, Vec<u8>> =
                    conn.hgetall(Self::dir_key(Ino(parent))).await?;
                for (name, bytes) in raw {
                    let (child, child_typ) = decode_dir_entry(&bytes)?;
                    edges.push((parent, name, child.as_u64(), child_typ));
                }
            }
        }
        let mut report = FsckReport {
            checked_nodes: nodes.len(),
            checked_edges: edges.len(),
            ..FsckReport::default()
        };
        let mut incoming_edges: BTreeMap<u64, u32> = BTreeMap::new();
        let mut child_dir_count: BTreeMap<u64, u32> = BTreeMap::new();
        let mut missing = BTreeSet::new();
        for (parent, name, child, edge_typ) in &edges {
            if !nodes
                .get(parent)
                .is_some_and(|(typ, _)| *typ == NodeType::Directory)
            {
                report.orphaned_edges.push(format!("{parent}:{name}"));
            }
            if let Some((child_typ, _)) = nodes.get(child) {
                *incoming_edges.entry(*child).or_insert(0) += 1;
                if *child_typ == NodeType::Directory {
                    *child_dir_count.entry(*parent).or_insert(0) += 1;
                }
            } else {
                missing.insert(*child);
                if *edge_typ == NodeType::Directory {
                    *child_dir_count.entry(*parent).or_insert(0) += 1;
                }
            }
        }
        report.missing_nodes = missing.into_iter().collect();
        for (&ino, &(typ, stored_nlink)) in &nodes {
            let links = incoming_edges.get(&ino).copied().unwrap_or(0);
            if ino != ROOT_INODE.as_u64() && ino != TRASH_INODE.as_u64() && links == 0 {
                report.unreferenced_nodes.push(ino);
            }
            let expected_nlink = if typ == NodeType::Directory {
                2 + child_dir_count.get(&ino).copied().unwrap_or(0)
            } else {
                links
            };
            if stored_nlink != expected_nlink {
                report.nlink_mismatches.push(NlinkMismatch {
                    inode: Ino(ino),
                    stored_nlink,
                    expected_nlink,
                });
            }
        }
        Ok(report)
    }

    async fn compact_chunk(
        &self,
        inode: Ino,
        indx: u32,
        old_count: usize,
        new_slice: Slice,
    ) -> Result<(), MetaError> {
        if old_count == 0 {
            return Err(MetaError::InvalidInput(
                "old_count must be at least 1".to_owned(),
            ));
        }
        let attr = self.read_attr_internal(inode).await?;
        if !attr.is_file() {
            return Err(MetaError::IsDir(format!("inode {inode}")));
        }
        let mut conn = self.client();
        let key = Self::chunk_key(inode, indx);
        let blob: Vec<u8> = conn
            .get::<_, Option<Vec<u8>>>(&key)
            .await?
            .ok_or_else(|| MetaError::NotFound(format!("chunk {inode}:{indx} not found")))?;
        let records = decode_slices(&blob)?;
        if records.len() < old_count {
            return Err(MetaError::InvalidInput(format!(
                "chunk {inode}:{indx} has {} slices, expected at least {old_count}",
                records.len()
            )));
        }
        let old_records = &records[..old_count];
        let (start_pos, _, _) = juicefs_core::compact_chunk(old_records);
        let compacted_rec = SliceRecord {
            pos: start_pos,
            id: new_slice.id,
            size: new_slice.size,
            off: new_slice.off,
            len: new_slice.len,
        };
        let tail_start = old_count * SLICE_BYTES;
        let mut new_blob = Vec::with_capacity(SLICE_BYTES + (blob.len() - tail_start));
        new_blob.extend_from_slice(&encode_slice(compacted_rec));
        new_blob.extend_from_slice(&blob[tail_start..]);
        let _: () = conn.set(&key, new_blob).await?;
        if new_slice.id > 0 {
            self.update_slice_ref(new_slice.id, new_slice.size, 1)
                .await?;
        }
        for rec in old_records {
            if rec.id > 0 {
                self.update_slice_ref(rec.id, rec.size, -1).await?;
            }
        }
        Ok(())
    }

    async fn update_format(&self, format: &Format) -> Result<(), MetaError> {
        let mut conn = self.client();
        if !conn.hexists(Self::SETTING_KEY, "format").await? {
            return Err(MetaError::NotFormatted);
        }
        let _: () = conn
            .hset(Self::SETTING_KEY, "format", serde_json::to_string(format)?)
            .await?;
        Ok(())
    }

    async fn clone_entry(
        &self,
        src_parent: Ino,
        src_name: &str,
        dst_parent: Ino,
        dst_name: &str,
    ) -> Result<(Ino, Attr), MetaError> {
        validate_name(src_name)?;
        validate_name(dst_name)?;
        let mut conn = self.client();
        let raw: Option<Vec<u8>> = conn.hget(Self::dir_key(src_parent), src_name).await?;
        let bytes = raw.ok_or_else(|| MetaError::NotFound(src_name.to_owned()))?;
        let (src_ino, _) = decode_dir_entry(&bytes)?;
        let dst_pattr = self.read_attr_internal(dst_parent).await?;
        if !dst_pattr.is_dir() {
            return Err(MetaError::NotDir(format!("inode {dst_parent}")));
        }
        if conn.hexists(Self::dir_key(dst_parent), dst_name).await? {
            return Err(MetaError::AlreadyExists(dst_name.to_owned()));
        }
        let now = now_ns();
        let res = self
            .clone_recursive(src_ino, dst_parent, dst_name, now)
            .await?;
        self.update_parent_mtime(dst_parent, now).await?;
        Ok(res)
    }

    async fn get_dir_stat(&self, inode: Ino) -> Result<DirStat, MetaError> {
        let attr = self.read_attr_internal(inode).await?;
        if !attr.is_dir() {
            return Err(MetaError::NotDir(format!("inode {inode}")));
        }
        let mut conn = self.client();
        let raw: Option<String> = conn.hget(DIR_STATS_KEY, inode.as_u64().to_string()).await?;
        match raw {
            Some(json) => Ok(serde_json::from_str(&json)?),
            None => Ok(DirStat::default()),
        }
    }

    async fn set_ug_quota(
        &self,
        target: QuotaTarget,
        max_space: i64,
        max_inodes: i64,
    ) -> Result<Quota, MetaError> {
        let mut conn = self.client();
        let field = ug_field(target);
        let existing: Option<String> = conn.hget(UG_QUOTA_KEY, &field).await?;
        let (used_space, used_inodes) = if let Some(json) = existing {
            let q: Quota = serde_json::from_str(&json)?;
            (q.used_space, q.used_inodes)
        } else {
            let all_inos: Vec<u64> = conn.smembers(ALL_INODES_KEY).await?;
            let mut space: i64 = 0;
            let mut inodes: i64 = 0;
            for ino_u64 in all_inos {
                if ino_u64 <= ROOT_INODE.as_u64() || ino_u64 >= TRASH_INODE.as_u64() {
                    continue;
                }
                if let Ok(attr) = self.read_attr_internal(Ino(ino_u64)).await {
                    let matches = match target {
                        QuotaTarget::User(uid) => attr.uid == uid,
                        QuotaTarget::Group(gid) => attr.gid == gid,
                    };
                    if matches {
                        inodes += 1;
                        if attr.is_file() {
                            space += attr.length as i64;
                        }
                    }
                }
            }
            (space, inodes)
        };
        let quota = Quota {
            max_space,
            max_inodes,
            used_space,
            used_inodes,
        };
        let _: () = conn
            .hset(UG_QUOTA_KEY, &field, serde_json::to_string(&quota)?)
            .await?;
        Ok(quota)
    }

    async fn get_ug_quota(&self, target: QuotaTarget) -> Result<Quota, MetaError> {
        let mut conn = self.client();
        let raw: Option<String> = conn.hget(UG_QUOTA_KEY, ug_field(target)).await?;
        let json = raw.ok_or_else(|| MetaError::NotFound(format!("ug quota for {target:?}")))?;
        Ok(serde_json::from_str(&json)?)
    }

    async fn del_ug_quota(&self, target: QuotaTarget) -> Result<(), MetaError> {
        let mut conn = self.client();
        let removed: usize = conn.hdel(UG_QUOTA_KEY, ug_field(target)).await?;
        if removed == 0 {
            return Err(MetaError::NotFound(format!("ug quota for {target:?}")));
        }
        Ok(())
    }

    async fn list_ug_quotas(&self) -> Result<Vec<(QuotaTarget, Quota)>, MetaError> {
        let mut conn = self.client();
        let all: BTreeMap<String, String> = conn.hgetall(UG_QUOTA_KEY).await?;
        let mut out = Vec::with_capacity(all.len());
        for (k, v) in all {
            out.push((parse_ug_field(&k)?, serde_json::from_str(&v)?));
        }
        out.sort_by_key(|(target, _)| *target);
        Ok(out)
    }

    async fn read_changelog(
        &self,
        after_id: i64,
        limit: usize,
    ) -> Result<Vec<ChangeLogEntry>, MetaError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.client();
        let min_score = format!("({after_id}");
        let members: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(CHANGELOG_KEY)
            .arg(min_score)
            .arg("+inf")
            .arg("LIMIT")
            .arg(0usize)
            .arg(limit)
            .query_async(&mut conn)
            .await?;
        let mut out = Vec::with_capacity(members.len());
        for raw in members {
            out.push(serde_json::from_str(&raw)?);
        }
        Ok(out)
    }

    async fn new_session(
        &self,
        mount_point: &str,
        ttl_secs: i64,
    ) -> Result<SessionInfo, MetaError> {
        let mut conn = self.client();
        let sid = self.incr_counter(NEXT_SESSION_KEY, 1).await? as u64;
        let info = SessionInfo {
            sid,
            expire: now_secs() + ttl_secs,
            mount_point: mount_point.to_owned(),
        };
        let _: () = conn
            .hset(SESSIONS_KEY, sid.to_string(), serde_json::to_string(&info)?)
            .await?;
        Ok(info)
    }

    async fn heartbeat(&self, sid: u64, ttl_secs: i64) -> Result<(), MetaError> {
        let mut conn = self.client();
        let field = sid.to_string();
        let raw: Option<String> = conn.hget(SESSIONS_KEY, &field).await?;
        let json = raw.ok_or_else(|| MetaError::NotFound(format!("session {sid}")))?;
        let mut info: SessionInfo = serde_json::from_str(&json)?;
        info.expire = now_secs() + ttl_secs;
        let _: () = conn
            .hset(SESSIONS_KEY, &field, serde_json::to_string(&info)?)
            .await?;
        Ok(())
    }

    async fn close_session(&self, sid: u64) -> Result<(), MetaError> {
        let mut conn = self.client();
        let removed: usize = conn.hdel(SESSIONS_KEY, sid.to_string()).await?;
        if removed == 0 {
            return Err(MetaError::NotFound(format!("session {sid}")));
        }
        let prefix = format!("{sid}:");
        for pat in ["flock:*", "plock:*"] {
            for key in scan_keys(&mut conn, pat).await? {
                let fields: Vec<String> = conn.hkeys(&key).await?;
                for f in fields {
                    if f.starts_with(&prefix) {
                        let _: () = conn.hdel(&key, &f).await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn list_sessions(&self) -> Result<Vec<SessionInfo>, MetaError> {
        let mut conn = self.client();
        let all: BTreeMap<String, String> = conn.hgetall(SESSIONS_KEY).await?;
        let mut out: Vec<SessionInfo> = Vec::with_capacity(all.len());
        for v in all.into_values() {
            out.push(serde_json::from_str(&v)?);
        }
        out.sort_by_key(|s| s.sid);
        Ok(out)
    }

    async fn cleanup_stale_sessions(&self) -> Result<u64, MetaError> {
        let now = now_secs();
        let sessions = self.list_sessions().await?;
        let mut removed = 0u64;
        for session in sessions {
            if session.expire <= now {
                self.close_session(session.sid).await?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}

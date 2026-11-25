//! `SQLite` metadata engine compatible with `JuiceFS` SQL schema conventions.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use juicefs_core::{
    Attr, CHUNK_SIZE, Entry, Format, Ino, MAX_SYMLINK, NodeType, ROOT_INODE, SLICE_BYTES,
    SetAttrFlags, Slice, SliceRecord, StatFs, TRASH_INODE, build_slice, decode_slices,
    encode_slice,
};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::util::{
    align4k, decode_plocks, decode_session_mount_point, encode_plocks, encode_session_info,
    make_trash_entry_name, now_ns, now_secs, parse_trash_entry_name, split_ns, update_plocks,
    validate_name,
};
use crate::{
    ChangeLogEntry, DirStat, FlockType, FsckReport, Meta, MetaError, NlinkMismatch, PlockRecord,
    Quota, QuotaTarget, SessionInfo,
};

const SCHEMA_SQL: &str = r"
CREATE TABLE IF NOT EXISTS jfs_setting (
    name TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jfs_counter (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS jfs_edge (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent INTEGER NOT NULL,
    name BLOB NOT NULL,
    inode INTEGER NOT NULL,
    type INTEGER NOT NULL,
    UNIQUE(parent, name)
);
CREATE INDEX IF NOT EXISTS idx_jfs_edge_inode ON jfs_edge(inode);
CREATE TABLE IF NOT EXISTS jfs_node (
    inode INTEGER PRIMARY KEY,
    type INTEGER NOT NULL,
    flags INTEGER NOT NULL,
    mode INTEGER NOT NULL,
    uid INTEGER NOT NULL,
    gid INTEGER NOT NULL,
    atime INTEGER NOT NULL,
    mtime INTEGER NOT NULL,
    ctime INTEGER NOT NULL,
    atimensec INTEGER NOT NULL DEFAULT 0,
    mtimensec INTEGER NOT NULL DEFAULT 0,
    ctimensec INTEGER NOT NULL DEFAULT 0,
    nlink INTEGER NOT NULL,
    length INTEGER NOT NULL,
    rdev INTEGER NOT NULL DEFAULT 0,
    parent INTEGER NOT NULL DEFAULT 0,
    access_acl_id INTEGER NOT NULL DEFAULT 0,
    default_acl_id INTEGER NOT NULL DEFAULT 0,
    tier_id INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS jfs_chunk (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inode INTEGER NOT NULL,
    indx INTEGER NOT NULL,
    slices BLOB NOT NULL,
    UNIQUE(inode, indx)
);
CREATE TABLE IF NOT EXISTS jfs_slice_ref (
    id INTEGER PRIMARY KEY,
    size INTEGER NOT NULL,
    refs INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jfs_slice_ref_refs ON jfs_slice_ref(refs);
CREATE TABLE IF NOT EXISTS jfs_symlink (
    inode INTEGER PRIMARY KEY,
    target BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS jfs_xattr (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inode INTEGER NOT NULL,
    name TEXT NOT NULL,
    value BLOB NOT NULL,
    UNIQUE(inode, name)
);
CREATE TABLE IF NOT EXISTS jfs_flock (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inode INTEGER NOT NULL,
    sid INTEGER NOT NULL DEFAULT 0,
    owner INTEGER NOT NULL,
    ltype INTEGER NOT NULL,
    UNIQUE(inode, sid, owner)
);
CREATE TABLE IF NOT EXISTS jfs_plock (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inode INTEGER NOT NULL,
    sid INTEGER NOT NULL DEFAULT 0,
    owner INTEGER NOT NULL,
    records BLOB NOT NULL,
    UNIQUE(inode, sid, owner)
);
CREATE TABLE IF NOT EXISTS jfs_dir_quota (
    inode INTEGER PRIMARY KEY,
    max_space INTEGER NOT NULL,
    max_inodes INTEGER NOT NULL,
    used_space INTEGER NOT NULL,
    used_inodes INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS jfs_dirstats (
    inode INTEGER PRIMARY KEY,
    data_length INTEGER NOT NULL DEFAULT 0,
    used_space INTEGER NOT NULL DEFAULT 0,
    used_inodes INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS jfs_ug_quota (
    qtype INTEGER NOT NULL,
    qkey INTEGER NOT NULL,
    max_space INTEGER NOT NULL,
    max_inodes INTEGER NOT NULL,
    used_space INTEGER NOT NULL,
    used_inodes INTEGER NOT NULL,
    PRIMARY KEY(qtype, qkey)
);
CREATE TABLE IF NOT EXISTS jfs_changelog (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entry TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jfs_session2 (
    sid INTEGER PRIMARY KEY,
    expire INTEGER NOT NULL,
    info BLOB
);
";

/// SQLite-backed [`Meta`] engine.
#[derive(Debug, Clone)]
pub struct SqliteMeta {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteMeta {
    /// Opens or creates a SQLite metadata database at `path` (or `:memory:`).
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MetaError> {
        let path_ref = path.as_ref();
        if path_ref != Path::new(":memory:")
            && let Some(parent) = path_ref.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|e| MetaError::InvalidInput(format!("cannot create meta dir: {e}")))?;
        }
        let conn = Connection::open(path_ref)?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;",
        )?;
        conn.execute_batch(SCHEMA_SQL)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    async fn run<F, R>(&self, f: F) -> Result<R, MetaError>
    where
        F: FnOnce(&mut Connection) -> Result<R, MetaError> + Send + 'static,
        R: Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let mut guard = conn.lock();
            f(&mut guard)
        })
        .await
        .map_err(|e| MetaError::Task(e.to_string()))?
    }
}

fn next_counter(tx: &Transaction<'_>, name: &str, delta: i64) -> Result<i64, MetaError> {
    tx.execute(
        "INSERT INTO jfs_counter (name, value) VALUES (?1, ?2)
         ON CONFLICT(name) DO UPDATE SET value = value + ?2",
        params![name, delta],
    )?;
    let val: i64 = tx.query_row(
        "SELECT value FROM jfs_counter WHERE name = ?1",
        params![name],
        |r| r.get(0),
    )?;
    Ok(val)
}

fn get_counter(conn: &Connection, name: &str) -> Result<i64, MetaError> {
    let val: Option<i64> = conn
        .query_row(
            "SELECT value FROM jfs_counter WHERE name = ?1",
            params![name],
            |r| r.get(0),
        )
        .optional()?;
    Ok(val.unwrap_or(0))
}

fn read_attr(conn: &Connection, inode: Ino) -> Result<Attr, MetaError> {
    conn.query_row(
        "SELECT type, flags, mode, uid, gid, atime, mtime, ctime,
                atimensec, mtimensec, ctimensec, nlink, length, rdev, parent, tier_id
         FROM jfs_node WHERE inode = ?1",
        params![inode.as_u64() as i64],
        |row| {
            let typ_raw: u8 = row.get(0)?;
            let atime_us: i64 = row.get(5)?;
            let mtime_us: i64 = row.get(6)?;
            let ctime_us: i64 = row.get(7)?;
            let atime_sub: i64 = row.get(8)?;
            let mtime_sub: i64 = row.get(9)?;
            let ctime_sub: i64 = row.get(10)?;
            Ok((
                typ_raw,
                Attr {
                    flags: row.get(1)?,
                    typ: NodeType::File,
                    mode: row.get(2)?,
                    uid: row.get(3)?,
                    gid: row.get(4)?,
                    atime: atime_us / 1_000_000,
                    mtime: mtime_us / 1_000_000,
                    ctime: ctime_us / 1_000_000,
                    atime_nsec: ((atime_us % 1_000_000) * 1_000 + atime_sub) as u32,
                    mtime_nsec: ((mtime_us % 1_000_000) * 1_000 + mtime_sub) as u32,
                    ctime_nsec: ((ctime_us % 1_000_000) * 1_000 + ctime_sub) as u32,
                    nlink: row.get(11)?,
                    length: row.get::<_, i64>(12)? as u64,
                    rdev: row.get(13)?,
                    parent: Ino(row.get::<_, i64>(14)? as u64),
                    tier: row.get(15)?,
                },
            ))
        },
    )
    .optional()?
    .ok_or_else(|| MetaError::NotFound(format!("inode {inode}")))
    .and_then(|(typ_raw, mut attr)| {
        attr.typ = NodeType::try_from(typ_raw)?;
        Ok(attr)
    })
}

pub(crate) fn insert_node(tx: &Transaction<'_>, inode: Ino, attr: &Attr) -> Result<(), MetaError> {
    let atime_us = attr.atime * 1_000_000 + i64::from(attr.atime_nsec) / 1_000;
    let atime_sub = i64::from(attr.atime_nsec) % 1_000;
    let mtime_us = attr.mtime * 1_000_000 + i64::from(attr.mtime_nsec) / 1_000;
    let mtime_sub = i64::from(attr.mtime_nsec) % 1_000;
    let ctime_us = attr.ctime * 1_000_000 + i64::from(attr.ctime_nsec) / 1_000;
    let ctime_sub = i64::from(attr.ctime_nsec) % 1_000;

    tx.execute(
        "INSERT INTO jfs_node (
            inode, type, flags, mode, uid, gid, atime, mtime, ctime,
            atimensec, mtimensec, ctimensec, nlink, length, rdev, parent, tier_id
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
        params![
            inode.as_u64() as i64,
            attr.typ as u8,
            attr.flags,
            attr.mode,
            attr.uid,
            attr.gid,
            atime_us,
            mtime_us,
            ctime_us,
            atime_sub,
            mtime_sub,
            ctime_sub,
            attr.nlink,
            attr.length as i64,
            attr.rdev,
            attr.parent.as_u64() as i64,
            attr.tier,
        ],
    )?;
    Ok(())
}

fn update_parent_mtime(tx: &Transaction<'_>, parent: Ino, ns: i64) -> Result<(), MetaError> {
    let (us, sub) = split_ns(ns);
    tx.execute(
        "UPDATE jfs_node SET mtime = ?1, mtimensec = ?2, ctime = ?1, ctimensec = ?2 WHERE inode = ?3",
        params![us, sub, parent.as_u64() as i64],
    )?;
    Ok(())
}

fn decrement_chunk_refs(tx: &Transaction<'_>, inode: Ino) -> Result<(), MetaError> {
    let mut stmt = tx.prepare("SELECT slices FROM jfs_chunk WHERE inode = ?1")?;
    let rows = stmt.query_map(params![inode.as_u64() as i64], |r| r.get::<_, Vec<u8>>(0))?;
    let mut blobs = Vec::new();
    for blob in rows {
        blobs.push(blob?);
    }
    drop(stmt);

    for blob in blobs {
        for rec in decode_slices(&blob)? {
            if rec.id > 0 {
                tx.execute(
                    "UPDATE jfs_slice_ref SET refs = refs - 1 WHERE id = ?1",
                    params![rec.id as i64],
                )?;
            }
        }
    }
    tx.execute(
        "DELETE FROM jfs_chunk WHERE inode = ?1",
        params![inode.as_u64() as i64],
    )?;
    Ok(())
}

fn delete_inode_auxiliary(tx: &Transaction<'_>, ino_raw: i64) -> Result<(), MetaError> {
    tx.execute("DELETE FROM jfs_symlink WHERE inode = ?1", params![ino_raw])?;
    tx.execute("DELETE FROM jfs_xattr WHERE inode = ?1", params![ino_raw])?;
    tx.execute("DELETE FROM jfs_flock WHERE inode = ?1", params![ino_raw])?;
    tx.execute("DELETE FROM jfs_plock WHERE inode = ?1", params![ino_raw])?;
    tx.execute("DELETE FROM jfs_node WHERE inode = ?1", params![ino_raw])?;
    Ok(())
}

fn calc_dir_descendant_usage(conn: &Connection, dir_ino: Ino) -> Result<(i64, i64), MetaError> {
    let mut queue = vec![dir_ino];
    let mut visited = HashSet::new();
    visited.insert(dir_ino.as_u64());
    let mut total_space: i64 = 0;
    let mut total_inodes: i64 = 0;

    while let Some(cur) = queue.pop() {
        let mut stmt = conn.prepare("SELECT inode, type FROM jfs_edge WHERE parent = ?1")?;
        let rows = stmt.query_map(params![cur.as_u64() as i64], |r| {
            Ok((r.get::<_, i64>(0)? as u64, r.get::<_, u8>(1)?))
        })?;
        let mut children = Vec::new();
        for row in rows {
            children.push(row?);
        }
        drop(stmt);

        for (child_u64, typ_raw) in children {
            total_inodes += 1;
            let typ = NodeType::try_from(typ_raw)?;
            if typ == NodeType::Directory {
                if visited.insert(child_u64) {
                    queue.push(Ino(child_u64));
                }
            } else if typ == NodeType::File {
                let len: Option<i64> = conn
                    .query_row(
                        "SELECT length FROM jfs_node WHERE inode = ?1",
                        params![child_u64 as i64],
                        |r| r.get(0),
                    )
                    .optional()?;
                total_space += len.unwrap_or(0);
            }
        }
    }

    Ok((total_space, total_inodes))
}

fn ancestor_quota_inodes(
    conn: &Connection,
    start_dir: Ino,
) -> Result<Vec<(Ino, Quota)>, MetaError> {
    let mut result = Vec::new();
    let mut visited = HashSet::new();
    let mut cur = start_dir;

    while cur.is_valid() && !cur.is_trash() && visited.insert(cur.as_u64()) {
        if let Some(q) = conn
            .query_row(
                "SELECT max_space, max_inodes, used_space, used_inodes FROM jfs_dir_quota WHERE inode = ?1",
                params![cur.as_u64() as i64],
                |r| {
                    Ok(Quota {
                        max_space: r.get(0)?,
                        max_inodes: r.get(1)?,
                        used_space: r.get(2)?,
                        used_inodes: r.get(3)?,
                    })
                },
            )
            .optional()?
        {
            result.push((cur, q));
        }
        if cur == ROOT_INODE {
            break;
        }
        let parent_opt: Option<i64> = conn
            .query_row(
                "SELECT parent FROM jfs_node WHERE inode = ?1",
                params![cur.as_u64() as i64],
                |r| r.get(0),
            )
            .optional()?;
        let Some(parent_raw) = parent_opt else {
            break;
        };
        let parent = Ino(parent_raw as u64);
        if parent == cur || !parent.is_valid() {
            break;
        }
        cur = parent;
    }

    Ok(result)
}

fn apply_quota_delta_dirs(
    tx: &Transaction<'_>,
    dirs: &[Ino],
    delta_space: i64,
    delta_inodes: i64,
) -> Result<(), MetaError> {
    if delta_space == 0 && delta_inodes == 0 {
        return Ok(());
    }
    let mut quotas_by_ino: BTreeMap<u64, Quota> = BTreeMap::new();
    for &dir in dirs {
        for (q_ino, q) in ancestor_quota_inodes(tx, dir)? {
            quotas_by_ino.insert(q_ino.as_u64(), q);
        }
    }
    if quotas_by_ino.is_empty() {
        return Ok(());
    }

    for (q_ino, q) in &quotas_by_ino {
        if delta_space > 0 && q.max_space > 0 && q.used_space + delta_space > q.max_space {
            return Err(MetaError::QuotaExceeded(format!(
                "directory inode {q_ino} space quota exceeded ({} + {} > {})",
                q.used_space, delta_space, q.max_space
            )));
        }
        if delta_inodes > 0 && q.max_inodes > 0 && q.used_inodes + delta_inodes > q.max_inodes {
            return Err(MetaError::QuotaExceeded(format!(
                "directory inode {q_ino} inode quota exceeded ({} + {} > {})",
                q.used_inodes, delta_inodes, q.max_inodes
            )));
        }
    }

    for (q_ino, q) in quotas_by_ino {
        let new_space = (q.used_space + delta_space).max(0);
        let new_inodes = (q.used_inodes + delta_inodes).max(0);
        tx.execute(
            "UPDATE jfs_dir_quota SET used_space = ?1, used_inodes = ?2 WHERE inode = ?3",
            params![new_space, new_inodes, q_ino as i64],
        )?;
    }
    Ok(())
}

fn apply_quota_delta(
    tx: &Transaction<'_>,
    dir: Ino,
    delta_space: i64,
    delta_inodes: i64,
) -> Result<(), MetaError> {
    apply_quota_delta_dirs(tx, &[dir], delta_space, delta_inodes)
}

fn file_parent_dirs(conn: &Connection, inode: Ino, attr: &Attr) -> Result<Vec<Ino>, MetaError> {
    if attr.parent.is_valid() {
        return Ok(vec![attr.parent]);
    }
    let mut stmt = conn.prepare("SELECT DISTINCT parent FROM jfs_edge WHERE inode = ?1")?;
    let rows = stmt.query_map(params![inode.as_u64() as i64], |r| r.get::<_, i64>(0))?;
    let mut dirs = Vec::new();
    for row in rows {
        dirs.push(Ino(row? as u64));
    }
    Ok(dirs)
}

fn ensure_trash_node(tx: &Transaction<'_>, now: i64) -> Result<(), MetaError> {
    let exists: Option<i64> = tx
        .query_row(
            "SELECT inode FROM jfs_node WHERE inode = ?1",
            params![TRASH_INODE.as_u64() as i64],
            |r| r.get(0),
        )
        .optional()?;
    if exists.is_none() {
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
        insert_node(tx, TRASH_INODE, &trash_attr)?;
    }
    Ok(())
}

fn load_format_sync(conn: &Connection) -> Result<Format, MetaError> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT value FROM jfs_setting WHERE name = 'format'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    let json = raw.ok_or(MetaError::NotFormatted)?;
    Ok(serde_json::from_str(&json)?)
}

#[derive(Debug, Clone, Copy, Default)]
struct FormatFlags {
    dir_stats: bool,
    user_group_quota: bool,
    change_log: bool,
}

fn load_format_flags(conn: &Connection) -> Result<FormatFlags, MetaError> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT value FROM jfs_setting WHERE name = 'format'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    let Some(json) = raw else {
        return Ok(FormatFlags::default());
    };
    let fmt: Format = serde_json::from_str(&json)?;
    Ok(FormatFlags {
        dir_stats: fmt.dir_stats,
        user_group_quota: fmt.user_group_quota,
        change_log: fmt.change_log,
    })
}

fn ancestor_dir_inodes(conn: &Connection, dirs: &[Ino]) -> Result<Vec<Ino>, MetaError> {
    let mut visited = BTreeSet::new();
    for &start_dir in dirs {
        let mut cur = start_dir;
        while cur.is_valid() && !cur.is_trash() && visited.insert(cur.as_u64()) {
            if cur == ROOT_INODE {
                break;
            }
            let parent_opt: Option<i64> = conn
                .query_row(
                    "SELECT parent FROM jfs_node WHERE inode = ?1",
                    params![cur.as_u64() as i64],
                    |r| r.get(0),
                )
                .optional()?;
            let Some(parent_raw) = parent_opt else {
                break;
            };
            let parent = Ino(parent_raw as u64);
            if parent == cur || !parent.is_valid() {
                break;
            }
            cur = parent;
        }
    }
    Ok(visited.into_iter().map(Ino).collect())
}

fn apply_dir_stats_delta(
    tx: &Transaction<'_>,
    dirs: &[Ino],
    delta_len: i64,
    delta_space: i64,
    delta_inodes: i64,
) -> Result<(), MetaError> {
    if delta_len == 0 && delta_space == 0 && delta_inodes == 0 {
        return Ok(());
    }
    for anc in ancestor_dir_inodes(tx, dirs)? {
        let existing: Option<(i64, i64, i64)> = tx
            .query_row(
                "SELECT data_length, used_space, used_inodes FROM jfs_dirstats WHERE inode = ?1",
                params![anc.as_u64() as i64],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let (cur_len, cur_space, cur_inodes) = existing.unwrap_or((0, 0, 0));
        let new_len = (cur_len + delta_len).max(0);
        let new_space = (cur_space + delta_space).max(0);
        let new_inodes = (cur_inodes + delta_inodes).max(0);
        tx.execute(
            "INSERT INTO jfs_dirstats (inode, data_length, used_space, used_inodes)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(inode) DO UPDATE SET
                data_length = ?2, used_space = ?3, used_inodes = ?4",
            params![anc.as_u64() as i64, new_len, new_space, new_inodes],
        )?;
    }
    Ok(())
}

fn calc_dir_descendant_stats(
    conn: &Connection,
    dir_ino: Ino,
) -> Result<(i64, i64, i64), MetaError> {
    let mut queue = vec![dir_ino];
    let mut visited = HashSet::new();
    visited.insert(dir_ino.as_u64());
    let mut total_len: i64 = 0;
    let mut total_space: i64 = align4k(0);
    let mut total_inodes: i64 = 1;

    while let Some(cur) = queue.pop() {
        let mut stmt = conn.prepare("SELECT inode, type FROM jfs_edge WHERE parent = ?1")?;
        let rows = stmt.query_map(params![cur.as_u64() as i64], |r| {
            Ok((r.get::<_, i64>(0)? as u64, r.get::<_, u8>(1)?))
        })?;
        let mut children = Vec::new();
        for row in rows {
            children.push(row?);
        }
        drop(stmt);

        for (child_u64, typ_raw) in children {
            total_inodes += 1;
            let typ = NodeType::try_from(typ_raw)?;
            if typ == NodeType::Directory {
                total_space += align4k(0);
                if visited.insert(child_u64) {
                    queue.push(Ino(child_u64));
                }
            } else {
                let len: Option<i64> = conn
                    .query_row(
                        "SELECT length FROM jfs_node WHERE inode = ?1",
                        params![child_u64 as i64],
                        |r| r.get(0),
                    )
                    .optional()?;
                let len_u64 = len.unwrap_or(0).max(0) as u64;
                total_len += len_u64 as i64;
                total_space += align4k(len_u64);
            }
        }
    }

    Ok((total_len, total_space, total_inodes))
}

const fn ug_quota_key(target: QuotaTarget) -> (u8, i64) {
    match target {
        QuotaTarget::User(uid) => (1, uid as i64),
        QuotaTarget::Group(gid) => (2, gid as i64),
    }
}

fn ug_quota_from_row(qtype: u8, qkey: i64) -> Result<QuotaTarget, MetaError> {
    let key = u32::try_from(qkey)
        .map_err(|_| MetaError::InvalidInput(format!("invalid quota id: {qkey}")))?;
    match qtype {
        1 => Ok(QuotaTarget::User(key)),
        2 => Ok(QuotaTarget::Group(key)),
        other => Err(MetaError::InvalidInput(format!(
            "invalid ug quota type: {other}"
        ))),
    }
}

fn read_ug_quota_opt(conn: &Connection, target: QuotaTarget) -> Result<Option<Quota>, MetaError> {
    let (qtype, qkey) = ug_quota_key(target);
    Ok(conn
        .query_row(
            "SELECT max_space, max_inodes, used_space, used_inodes
             FROM jfs_ug_quota WHERE qtype = ?1 AND qkey = ?2",
            params![qtype, qkey],
            |r| {
                Ok(Quota {
                    max_space: r.get(0)?,
                    max_inodes: r.get(1)?,
                    used_space: r.get(2)?,
                    used_inodes: r.get(3)?,
                })
            },
        )
        .optional()?)
}

fn apply_ug_quota_target_delta(
    tx: &Transaction<'_>,
    targets: &[QuotaTarget],
    delta_space: i64,
    delta_inodes: i64,
) -> Result<(), MetaError> {
    if delta_space == 0 && delta_inodes == 0 {
        return Ok(());
    }
    let mut loaded = Vec::new();
    for &target in targets {
        if let Some(q) = read_ug_quota_opt(tx, target)? {
            if delta_space > 0 && q.max_space > 0 && q.used_space + delta_space > q.max_space {
                return Err(MetaError::QuotaExceeded(format!(
                    "{target:?} space quota exceeded ({} + {} > {})",
                    q.used_space, delta_space, q.max_space
                )));
            }
            if delta_inodes > 0 && q.max_inodes > 0 && q.used_inodes + delta_inodes > q.max_inodes {
                return Err(MetaError::QuotaExceeded(format!(
                    "{target:?} inode quota exceeded ({} + {} > {})",
                    q.used_inodes, delta_inodes, q.max_inodes
                )));
            }
            loaded.push((target, q));
        }
    }
    for (target, q) in loaded {
        let (qtype, qkey) = ug_quota_key(target);
        let new_space = (q.used_space + delta_space).max(0);
        let new_inodes = (q.used_inodes + delta_inodes).max(0);
        tx.execute(
            "UPDATE jfs_ug_quota SET used_space = ?1, used_inodes = ?2 WHERE qtype = ?3 AND qkey = ?4",
            params![new_space, new_inodes, qtype, qkey],
        )?;
    }
    Ok(())
}

fn apply_ug_quota_delta(
    tx: &Transaction<'_>,
    uid: u32,
    gid: u32,
    delta_space: i64,
    delta_inodes: i64,
) -> Result<(), MetaError> {
    apply_ug_quota_target_delta(
        tx,
        &[QuotaTarget::User(uid), QuotaTarget::Group(gid)],
        delta_space,
        delta_inodes,
    )
}

fn record_changelog(tx: &Transaction<'_>, entry: &serde_json::Value) -> Result<(), MetaError> {
    let serialized = serde_json::to_string(entry)?;
    tx.execute(
        "INSERT INTO jfs_changelog (entry) VALUES (?1)",
        params![serialized],
    )?;
    Ok(())
}

fn purge_trash_dir_recursive(tx: &Transaction<'_>, dir_ino: Ino) -> Result<u64, MetaError> {
    let mut stmt = tx.prepare("SELECT name, inode, type FROM jfs_edge WHERE parent = ?1")?;
    let rows = stmt.query_map(params![dir_ino.as_u64() as i64], |r| {
        Ok((
            r.get::<_, Vec<u8>>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, u8>(2)?,
        ))
    })?;
    let mut children = Vec::new();
    for row in rows {
        children.push(row?);
    }
    drop(stmt);

    let mut deleted: u64 = 0;
    for (name_bytes, child_raw, typ_raw) in children {
        let child_ino = Ino(child_raw as u64);
        let typ = NodeType::try_from(typ_raw)?;
        if typ == NodeType::Directory {
            deleted += purge_trash_dir_recursive(tx, child_ino)?;
            tx.execute(
                "DELETE FROM jfs_dir_quota WHERE inode = ?1",
                params![child_raw],
            )?;
            delete_inode_auxiliary(tx, child_raw)?;
            let _ = next_counter(tx, "usedInodes", -1)?;
            deleted += 1;
        } else {
            let attr = read_attr(tx, child_ino)?;
            if attr.nlink <= 1 {
                decrement_chunk_refs(tx, child_ino)?;
                delete_inode_auxiliary(tx, child_raw)?;
                let _ = next_counter(tx, "usedInodes", -1)?;
                let _ = next_counter(tx, "usedSpace", -(attr.length as i64))?;
                deleted += 1;
            } else {
                tx.execute(
                    "UPDATE jfs_node SET nlink = nlink - 1 WHERE inode = ?1",
                    params![child_raw],
                )?;
            }
        }
        tx.execute(
            "DELETE FROM jfs_edge WHERE parent = ?1 AND name = ?2",
            params![dir_ino.as_u64() as i64, name_bytes],
        )?;
    }
    Ok(deleted)
}

fn remove_existing_dst(
    tx: &Transaction<'_>,
    dst_parent: Ino,
    dst_name: &str,
    flags: FormatFlags,
) -> Result<(), MetaError> {
    let dst_row: Option<(i64, u8)> = tx
        .query_row(
            "SELECT inode, type FROM jfs_edge WHERE parent = ?1 AND name = ?2",
            params![dst_parent.as_u64() as i64, dst_name.as_bytes()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;

    let Some((dst_ino_raw, dst_typ)) = dst_row else {
        return Ok(());
    };
    if NodeType::try_from(dst_typ)? == NodeType::Directory {
        let count: i64 = tx.query_row(
            "SELECT COUNT(1) FROM jfs_edge WHERE parent = ?1",
            params![dst_ino_raw],
            |r| r.get(0),
        )?;
        if count > 0 {
            return Err(MetaError::NotEmpty(dst_name.to_owned()));
        }
        apply_quota_delta(tx, dst_parent, 0, -1)?;
        let dst_ino = Ino(dst_ino_raw as u64);
        let dst_attr = read_attr(tx, dst_ino)?;
        if flags.user_group_quota {
            apply_ug_quota_delta(tx, dst_attr.uid, dst_attr.gid, 0, -1)?;
        }
        if flags.dir_stats {
            apply_dir_stats_delta(tx, &[dst_parent], 0, -align4k(0), -1)?;
        }
        tx.execute(
            "DELETE FROM jfs_dir_quota WHERE inode = ?1",
            params![dst_ino_raw],
        )?;
        tx.execute(
            "DELETE FROM jfs_dirstats WHERE inode = ?1",
            params![dst_ino_raw],
        )?;
        delete_inode_auxiliary(tx, dst_ino_raw)?;
        tx.execute(
            "UPDATE jfs_node SET nlink = MAX(2, nlink - 1) WHERE inode = ?1",
            params![dst_parent.as_u64() as i64],
        )?;
        let _ = next_counter(tx, "usedInodes", -1)?;
    } else {
        let dst_ino = Ino(dst_ino_raw as u64);
        let dst_attr = read_attr(tx, dst_ino)?;
        apply_quota_delta(tx, dst_parent, -(dst_attr.length as i64), -1)?;
        if flags.dir_stats {
            apply_dir_stats_delta(
                tx,
                &[dst_parent],
                -(dst_attr.length as i64),
                -align4k(dst_attr.length),
                -1,
            )?;
        }
        if dst_attr.nlink <= 1 {
            if flags.user_group_quota {
                apply_ug_quota_delta(
                    tx,
                    dst_attr.uid,
                    dst_attr.gid,
                    -(dst_attr.length as i64),
                    -1,
                )?;
            }
            decrement_chunk_refs(tx, dst_ino)?;
            delete_inode_auxiliary(tx, dst_ino_raw)?;
            let _ = next_counter(tx, "usedInodes", -1)?;
            let _ = next_counter(tx, "usedSpace", -(dst_attr.length as i64))?;
        } else {
            tx.execute(
                "UPDATE jfs_node SET nlink = nlink - 1 WHERE inode = ?1",
                params![dst_ino_raw],
            )?;
        }
    }
    tx.execute(
        "DELETE FROM jfs_edge WHERE parent = ?1 AND name = ?2",
        params![dst_parent.as_u64() as i64, dst_name.as_bytes()],
    )?;
    Ok(())
}

#[async_trait]
impl Meta for SqliteMeta {
    async fn init(&self, format: &Format, force: bool) -> Result<(), MetaError> {
        let format_json = serde_json::to_string(format)?;
        let create_trash = format.trash_days > 0;
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let existing: Option<String> = tx
                .query_row(
                    "SELECT value FROM jfs_setting WHERE name = 'format'",
                    [],
                    |r| r.get(0),
                )
                .optional()?;
            if existing.is_some() && !force {
                return Err(MetaError::AlreadyExists("volume already formatted".to_owned()));
            }
            if force {
                tx.execute_batch(
                    "DELETE FROM jfs_setting;
                     DELETE FROM jfs_counter;
                     DELETE FROM jfs_edge;
                     DELETE FROM jfs_node;
                     DELETE FROM jfs_chunk;
                     DELETE FROM jfs_slice_ref;
                     DELETE FROM jfs_symlink;
                     DELETE FROM jfs_xattr;
                     DELETE FROM jfs_flock;
                     DELETE FROM jfs_plock;
                     DELETE FROM jfs_dir_quota;
                     DELETE FROM jfs_dirstats;
                     DELETE FROM jfs_ug_quota;
                     DELETE FROM jfs_changelog;
                     DELETE FROM jfs_session2;",
                )?;
            }
            tx.execute(
                "INSERT INTO jfs_setting (name, value) VALUES ('format', ?1)",
                params![format_json],
            )?;
            tx.execute(
                "INSERT INTO jfs_counter (name, value) VALUES ('nextInode', 1), ('nextChunk', 0), ('usedSpace', 0), ('usedInodes', 1)",
                [],
            )?;
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
            insert_node(&tx, ROOT_INODE, &root)?;
            if create_trash {
                ensure_trash_node(&tx, now)?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    async fn load(&self) -> Result<Format, MetaError> {
        self.run(|conn| {
            let raw: Option<String> = conn
                .query_row(
                    "SELECT value FROM jfs_setting WHERE name = 'format'",
                    [],
                    |r| r.get(0),
                )
                .optional()?;
            let json = raw.ok_or(MetaError::NotFormatted)?;
            Ok(serde_json::from_str(&json)?)
        })
        .await
    }

    async fn statfs(&self) -> Result<StatFs, MetaError> {
        self.run(|conn| {
            let raw: Option<String> = conn
                .query_row(
                    "SELECT value FROM jfs_setting WHERE name = 'format'",
                    [],
                    |r| r.get(0),
                )
                .optional()?;
            let fmt: Format = serde_json::from_str(&raw.ok_or(MetaError::NotFormatted)?)?;
            let used_space = get_counter(conn, "usedSpace")?.max(0) as u64;
            let used_inodes = get_counter(conn, "usedInodes")?.max(0) as u64;
            let total_space = if fmt.capacity > 0 {
                fmt.capacity
            } else {
                1u64 << 50
            };
            let avail_space = total_space.saturating_sub(used_space);
            let total_inodes = if fmt.inodes > 0 {
                fmt.inodes
            } else {
                10_000_000
            };
            let avail_inodes = total_inodes.saturating_sub(used_inodes);
            Ok(StatFs {
                total_space,
                avail_space,
                used_inodes,
                avail_inodes,
            })
        })
        .await
    }

    async fn lookup(&self, parent: Ino, name: &str) -> Result<(Ino, Attr), MetaError> {
        if name == "." {
            let attr = self.getattr(parent).await?;
            return Ok((parent, attr));
        }
        let name = name.to_owned();
        self.run(move |conn| {
            if name == ".." {
                let pattr = read_attr(conn, parent)?;
                let target = if pattr.parent.is_valid() {
                    pattr.parent
                } else {
                    ROOT_INODE
                };
                let tattr = read_attr(conn, target)?;
                return Ok((target, tattr));
            }
            if parent == ROOT_INODE
                && name == ".trash"
                && let Ok(tattr) = read_attr(conn, TRASH_INODE)
            {
                return Ok((TRASH_INODE, tattr));
            }
            let inode_opt: Option<i64> = conn
                .query_row(
                    "SELECT inode FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                    params![parent.as_u64() as i64, name.as_bytes()],
                    |r| r.get(0),
                )
                .optional()?;
            let ino = inode_opt
                .map(|v| Ino(v as u64))
                .ok_or_else(|| MetaError::NotFound(name.clone()))?;
            let attr = read_attr(conn, ino)?;
            Ok((ino, attr))
        })
        .await
    }

    async fn getattr(&self, inode: Ino) -> Result<Attr, MetaError> {
        self.run(move |conn| read_attr(conn, inode)).await
    }

    async fn setattr(
        &self,
        inode: Ino,
        attr: &Attr,
        mask: SetAttrFlags,
    ) -> Result<Attr, MetaError> {
        let attr = attr.clone();
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let flags = load_format_flags(&tx)?;
            let mut cur = read_attr(&tx, inode)?;
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
            if flags.user_group_quota && inode != ROOT_INODE && !inode.is_trash() {
                let space = if cur.is_file() { cur.length as i64 } else { 0 };
                if cur.uid != prior_user {
                    apply_ug_quota_target_delta(&tx, &[QuotaTarget::User(cur.uid)], space, 1)?;
                    apply_ug_quota_target_delta(&tx, &[QuotaTarget::User(prior_user)], -space, -1)?;
                }
                if cur.gid != prior_group {
                    apply_ug_quota_target_delta(&tx, &[QuotaTarget::Group(cur.gid)], space, 1)?;
                    apply_ug_quota_target_delta(
                        &tx,
                        &[QuotaTarget::Group(prior_group)],
                        -space,
                        -1,
                    )?;
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

            let atime_us = cur.atime * 1_000_000 + i64::from(cur.atime_nsec) / 1_000;
            let mtime_us = cur.mtime * 1_000_000 + i64::from(cur.mtime_nsec) / 1_000;
            let ctime_us = cur.ctime * 1_000_000 + i64::from(cur.ctime_nsec) / 1_000;

            tx.execute(
                "UPDATE jfs_node SET mode = ?1, uid = ?2, gid = ?3,
                 atime = ?4, atimensec = ?5, mtime = ?6, mtimensec = ?7, ctime = ?8, ctimensec = ?9
                 WHERE inode = ?10",
                params![
                    cur.mode,
                    cur.uid,
                    cur.gid,
                    atime_us,
                    i64::from(cur.atime_nsec) % 1_000,
                    mtime_us,
                    i64::from(cur.mtime_nsec) % 1_000,
                    ctime_us,
                    i64::from(cur.ctime_nsec) % 1_000,
                    inode.as_u64() as i64,
                ],
            )?;
            tx.commit()?;
            Ok(cur)
        })
        .await
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
        let name = name.to_owned();
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let flags = load_format_flags(&tx)?;
            let pattr = read_attr(&tx, parent)?;
            if !pattr.is_dir() {
                return Err(MetaError::NotDir(format!("inode {parent}")));
            }
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT inode FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                    params![parent.as_u64() as i64, name.as_bytes()],
                    |r| r.get(0),
                )
                .optional()?;
            if exists.is_some() {
                return Err(MetaError::AlreadyExists(name));
            }
            apply_quota_delta(&tx, parent, 0, 1)?;
            if flags.user_group_quota {
                apply_ug_quota_delta(&tx, uid, gid, 0, 1)?;
            }
            let ino = Ino(next_counter(&tx, "nextInode", 1)? as u64);
            let _ = next_counter(&tx, "usedInodes", 1)?;
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
            insert_node(&tx, ino, &attr)?;
            tx.execute(
                "INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?1, ?2, ?3, ?4)",
                params![
                    parent.as_u64() as i64,
                    name.as_bytes(),
                    ino.as_u64() as i64,
                    typ as u8
                ],
            )?;
            if flags.dir_stats {
                apply_dir_stats_delta(&tx, &[parent], 0, align4k(0), 1)?;
            }
            if flags.change_log {
                record_changelog(
                    &tx,
                    &serde_json::json!({
                        "op": "CREATE",
                        "parent": parent.as_u64(),
                        "name": name,
                        "inode": ino.as_u64(),
                        "type": typ as u8,
                        "ts": now,
                    }),
                )?;
            }
            update_parent_mtime(&tx, parent, now)?;
            tx.commit()?;
            Ok((ino, attr))
        })
        .await
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
        let name = name.to_owned();
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let flags = load_format_flags(&tx)?;
            let pattr = read_attr(&tx, parent)?;
            if !pattr.is_dir() {
                return Err(MetaError::NotDir(format!("inode {parent}")));
            }
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT inode FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                    params![parent.as_u64() as i64, name.as_bytes()],
                    |r| r.get(0),
                )
                .optional()?;
            if exists.is_some() {
                return Err(MetaError::AlreadyExists(name));
            }
            apply_quota_delta(&tx, parent, 0, 1)?;
            if flags.user_group_quota {
                apply_ug_quota_delta(&tx, uid, gid, 0, 1)?;
            }
            let ino = Ino(next_counter(&tx, "nextInode", 1)? as u64);
            let _ = next_counter(&tx, "usedInodes", 1)?;
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
            insert_node(&tx, ino, &attr)?;
            tx.execute(
                "INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?1, ?2, ?3, ?4)",
                params![
                    parent.as_u64() as i64,
                    name.as_bytes(),
                    ino.as_u64() as i64,
                    NodeType::Directory as u8
                ],
            )?;
            tx.execute(
                "UPDATE jfs_node SET nlink = nlink + 1 WHERE inode = ?1",
                params![parent.as_u64() as i64],
            )?;
            if flags.dir_stats {
                apply_dir_stats_delta(&tx, &[parent], 0, align4k(0), 1)?;
            }
            if flags.change_log {
                record_changelog(
                    &tx,
                    &serde_json::json!({
                        "op": "MKDIR",
                        "parent": parent.as_u64(),
                        "name": name,
                        "inode": ino.as_u64(),
                        "ts": now,
                    }),
                )?;
            }
            update_parent_mtime(&tx, parent, now)?;
            tx.commit()?;
            Ok((ino, attr))
        })
        .await
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
        let name = name.to_owned();
        let target = target.to_owned();
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let flags = load_format_flags(&tx)?;
            let pattr = read_attr(&tx, parent)?;
            if !pattr.is_dir() {
                return Err(MetaError::NotDir(format!("inode {parent}")));
            }
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT inode FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                    params![parent.as_u64() as i64, name.as_bytes()],
                    |r| r.get(0),
                )
                .optional()?;
            if exists.is_some() {
                return Err(MetaError::AlreadyExists(name));
            }
            apply_quota_delta(&tx, parent, 0, 1)?;
            if flags.user_group_quota {
                apply_ug_quota_delta(&tx, uid, gid, 0, 1)?;
            }
            let ino = Ino(next_counter(&tx, "nextInode", 1)? as u64);
            let _ = next_counter(&tx, "usedInodes", 1)?;
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
            insert_node(&tx, ino, &attr)?;
            tx.execute(
                "INSERT INTO jfs_symlink (inode, target) VALUES (?1, ?2)",
                params![ino.as_u64() as i64, target.as_bytes()],
            )?;
            tx.execute(
                "INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?1, ?2, ?3, ?4)",
                params![
                    parent.as_u64() as i64,
                    name.as_bytes(),
                    ino.as_u64() as i64,
                    NodeType::Symlink as u8
                ],
            )?;
            if flags.dir_stats {
                apply_dir_stats_delta(
                    &tx,
                    &[parent],
                    target.len() as i64,
                    align4k(target.len() as u64),
                    1,
                )?;
            }
            update_parent_mtime(&tx, parent, now)?;
            tx.commit()?;
            Ok((ino, attr))
        })
        .await
    }

    async fn readlink(&self, inode: Ino) -> Result<String, MetaError> {
        self.run(move |conn| {
            let target: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT target FROM jfs_symlink WHERE inode = ?1",
                    params![inode.as_u64() as i64],
                    |r| r.get(0),
                )
                .optional()?;
            let bytes = target.ok_or_else(|| MetaError::NotFound(format!("symlink {inode}")))?;
            Ok(String::from_utf8_lossy(&bytes).into_owned())
        })
        .await
    }

    async fn link(&self, inode: Ino, new_parent: Ino, new_name: &str) -> Result<Attr, MetaError> {
        validate_name(new_name)?;
        let new_name = new_name.to_owned();
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let flags = load_format_flags(&tx)?;
            let pattr = read_attr(&tx, new_parent)?;
            if !pattr.is_dir() {
                return Err(MetaError::NotDir(format!("inode {new_parent}")));
            }
            let mut attr = read_attr(&tx, inode)?;
            if attr.is_dir() {
                return Err(MetaError::IsDir(format!("inode {inode}")));
            }
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT inode FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                    params![new_parent.as_u64() as i64, new_name.as_bytes()],
                    |r| r.get(0),
                )
                .optional()?;
            if exists.is_some() {
                return Err(MetaError::AlreadyExists(new_name));
            }
            tx.execute(
                "INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?1, ?2, ?3, ?4)",
                params![
                    new_parent.as_u64() as i64,
                    new_name.as_bytes(),
                    inode.as_u64() as i64,
                    attr.typ as u8
                ],
            )?;
            apply_quota_delta(&tx, new_parent, attr.length as i64, 1)?;
            if flags.dir_stats {
                apply_dir_stats_delta(
                    &tx,
                    &[new_parent],
                    attr.length as i64,
                    align4k(attr.length),
                    1,
                )?;
            }
            attr.nlink += 1;
            let now = now_ns();
            let (us, sub) = split_ns(now);
            tx.execute(
                "UPDATE jfs_node SET nlink = nlink + 1, ctime = ?1, ctimensec = ?2, parent = 0 WHERE inode = ?3",
                params![us, sub, inode.as_u64() as i64],
            )?;
            update_parent_mtime(&tx, new_parent, now)?;
            tx.commit()?;
            Ok(attr)
        })
        .await
    }

    async fn unlink(&self, parent: Ino, name: &str) -> Result<(), MetaError> {
        validate_name(name)?;
        let name = name.to_owned();
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let flags = load_format_flags(&tx)?;
            let row: Option<(i64, u8)> = tx
                .query_row(
                    "SELECT inode, type FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                    params![parent.as_u64() as i64, name.as_bytes()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let (ino_raw, typ_raw) = row.ok_or_else(|| MetaError::NotFound(name.clone()))?;
            let ino = Ino(ino_raw as u64);
            if NodeType::try_from(typ_raw)? == NodeType::Directory {
                return Err(MetaError::IsDir(name));
            }
            tx.execute(
                "DELETE FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                params![parent.as_u64() as i64, name.as_bytes()],
            )?;
            let attr = read_attr(&tx, ino)?;
            apply_quota_delta(&tx, parent, -(attr.length as i64), -1)?;
            if flags.dir_stats {
                apply_dir_stats_delta(
                    &tx,
                    &[parent],
                    -(attr.length as i64),
                    -align4k(attr.length),
                    -1,
                )?;
            }
            let now = now_ns();
            if attr.nlink <= 1 {
                if flags.user_group_quota {
                    apply_ug_quota_delta(&tx, attr.uid, attr.gid, -(attr.length as i64), -1)?;
                }
                decrement_chunk_refs(&tx, ino)?;
                delete_inode_auxiliary(&tx, ino_raw)?;
                let _ = next_counter(&tx, "usedInodes", -1)?;
                let _ = next_counter(&tx, "usedSpace", -(attr.length as i64))?;
            } else {
                let (us, sub) = split_ns(now);
                tx.execute(
                    "UPDATE jfs_node SET nlink = nlink - 1, ctime = ?1, ctimensec = ?2 WHERE inode = ?3",
                    params![us, sub, ino_raw],
                )?;
            }
            if flags.change_log {
                record_changelog(
                    &tx,
                    &serde_json::json!({
                        "op": "UNLINK",
                        "parent": parent.as_u64(),
                        "name": name,
                        "inode": ino.as_u64(),
                        "ts": now,
                    }),
                )?;
            }
            update_parent_mtime(&tx, parent, now)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    async fn rmdir(&self, parent: Ino, name: &str) -> Result<(), MetaError> {
        validate_name(name)?;
        let name = name.to_owned();
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let flags = load_format_flags(&tx)?;
            let row: Option<(i64, u8)> = tx
                .query_row(
                    "SELECT inode, type FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                    params![parent.as_u64() as i64, name.as_bytes()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let (ino_raw, typ_raw) = row.ok_or_else(|| MetaError::NotFound(name.clone()))?;
            if NodeType::try_from(typ_raw)? != NodeType::Directory {
                return Err(MetaError::NotDir(name));
            }
            let child_count: i64 = tx.query_row(
                "SELECT COUNT(1) FROM jfs_edge WHERE parent = ?1",
                params![ino_raw],
                |r| r.get(0),
            )?;
            if child_count > 0 {
                return Err(MetaError::NotEmpty(name));
            }
            let dir_attr = read_attr(&tx, Ino(ino_raw as u64))?;
            tx.execute(
                "DELETE FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                params![parent.as_u64() as i64, name.as_bytes()],
            )?;
            apply_quota_delta(&tx, parent, 0, -1)?;
            if flags.user_group_quota {
                apply_ug_quota_delta(&tx, dir_attr.uid, dir_attr.gid, 0, -1)?;
            }
            if flags.dir_stats {
                apply_dir_stats_delta(&tx, &[parent], 0, -align4k(0), -1)?;
            }
            tx.execute(
                "DELETE FROM jfs_dir_quota WHERE inode = ?1",
                params![ino_raw],
            )?;
            tx.execute(
                "DELETE FROM jfs_dirstats WHERE inode = ?1",
                params![ino_raw],
            )?;
            delete_inode_auxiliary(&tx, ino_raw)?;
            tx.execute(
                "UPDATE jfs_node SET nlink = MAX(2, nlink - 1) WHERE inode = ?1",
                params![parent.as_u64() as i64],
            )?;
            let _ = next_counter(&tx, "usedInodes", -1)?;
            let now = now_ns();
            if flags.change_log {
                record_changelog(
                    &tx,
                    &serde_json::json!({
                        "op": "RMDIR",
                        "parent": parent.as_u64(),
                        "name": name,
                        "inode": ino_raw as u64,
                        "ts": now,
                    }),
                )?;
            }
            update_parent_mtime(&tx, parent, now)?;
            tx.commit()?;
            Ok(())
        })
        .await
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
        let src_name = src_name.to_owned();
        let dst_name = dst_name.to_owned();
        self.run(move |conn| rename_sync(conn, src_parent, &src_name, dst_parent, &dst_name))
            .await
    }

    async fn readdir(&self, inode: Ino) -> Result<Vec<Entry>, MetaError> {
        self.run(move |conn| {
            let dir_attr = read_attr(conn, inode)?;
            if !dir_attr.is_dir() {
                return Err(MetaError::NotDir(format!("inode {inode}")));
            }
            let mut stmt = conn.prepare(
                "SELECT e.name, e.inode FROM jfs_edge e WHERE e.parent = ?1 ORDER BY e.name",
            )?;
            let rows = stmt.query_map(params![inode.as_u64() as i64], |r| {
                let name_bytes: Vec<u8> = r.get(0)?;
                let child_ino: i64 = r.get(1)?;
                Ok((
                    String::from_utf8_lossy(&name_bytes).into_owned(),
                    Ino(child_ino as u64),
                ))
            })?;
            let mut pairs = Vec::new();
            for row in rows {
                pairs.push(row?);
            }
            drop(stmt);

            let mut entries = Vec::with_capacity(pairs.len());
            for (name, child_ino) in pairs {
                let attr = read_attr(conn, child_ino)?;
                entries.push(Entry {
                    inode: child_ino,
                    name,
                    attr,
                });
            }
            Ok(entries)
        })
        .await
    }

    async fn new_slice(&self) -> Result<u64, MetaError> {
        self.run(|conn| {
            let tx = conn.transaction()?;
            let id = next_counter(&tx, "nextChunk", 1)?;
            tx.commit()?;
            Ok(id as u64)
        })
        .await
    }

    async fn read_chunk(&self, inode: Ino, indx: u32) -> Result<Vec<Slice>, MetaError> {
        self.run(move |conn| {
            let blob: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT slices FROM jfs_chunk WHERE inode = ?1 AND indx = ?2",
                    params![inode.as_u64() as i64, i64::from(indx)],
                    |r| r.get(0),
                )
                .optional()?;
            let Some(bytes) = blob else {
                return Ok(Vec::new());
            };
            let records = decode_slices(&bytes)?;
            Ok(build_slice(&records))
        })
        .await
    }

    async fn write_chunk(
        &self,
        inode: Ino,
        indx: u32,
        pos: u32,
        slice: Slice,
        new_file_length: u64,
    ) -> Result<Attr, MetaError> {
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let flags = load_format_flags(&tx)?;
            let mut attr = read_attr(&tx, inode)?;
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
            let existing: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT slices FROM jfs_chunk WHERE inode = ?1 AND indx = ?2",
                    params![inode.as_u64() as i64, i64::from(indx)],
                    |r| r.get(0),
                )
                .optional()?;
            match existing {
                Some(mut buf) => {
                    buf.extend_from_slice(&encoded);
                    tx.execute(
                        "UPDATE jfs_chunk SET slices = ?1 WHERE inode = ?2 AND indx = ?3",
                        params![buf, inode.as_u64() as i64, i64::from(indx)],
                    )?;
                }
                None => {
                    tx.execute(
                        "INSERT INTO jfs_chunk (inode, indx, slices) VALUES (?1, ?2, ?3)",
                        params![inode.as_u64() as i64, i64::from(indx), &encoded[..]],
                    )?;
                }
            }
            if slice.id > 0 {
                tx.execute(
                    "INSERT INTO jfs_slice_ref (id, size, refs) VALUES (?1, ?2, 1)
                     ON CONFLICT(id) DO UPDATE SET refs = refs + 1",
                    params![slice.id as i64, i64::from(slice.size)],
                )?;
            }
            let now = now_ns();
            let (us, sub) = split_ns(now);
            let old_len = attr.length;
            let final_len = old_len.max(new_file_length);
            if final_len > old_len {
                let delta = (final_len - old_len) as i64;
                let parent_dirs = file_parent_dirs(&tx, inode, &attr)?;
                apply_quota_delta_dirs(&tx, &parent_dirs, delta, 0)?;
                if flags.user_group_quota {
                    apply_ug_quota_delta(&tx, attr.uid, attr.gid, delta, 0)?;
                }
                if flags.dir_stats {
                    apply_dir_stats_delta(
                        &tx,
                        &parent_dirs,
                        delta,
                        align4k(final_len) - align4k(old_len),
                        0,
                    )?;
                }
                let _ = next_counter(&tx, "usedSpace", delta)?;
            }
            attr.length = final_len;
            attr.mtime = now / 1_000_000_000;
            attr.mtime_nsec = (now % 1_000_000_000) as u32;
            attr.ctime = attr.mtime;
            attr.ctime_nsec = attr.mtime_nsec;

            tx.execute(
                "UPDATE jfs_node SET length = ?1, mtime = ?2, mtimensec = ?3, ctime = ?2, ctimensec = ?3 WHERE inode = ?4",
                params![final_len as i64, us, sub, inode.as_u64() as i64],
            )?;
            if flags.change_log {
                record_changelog(
                    &tx,
                    &serde_json::json!({
                        "op": "WRITE",
                        "inode": inode.as_u64(),
                        "indx": indx,
                        "pos": pos,
                        "len": slice.len,
                        "length": final_len,
                        "ts": now,
                    }),
                )?;
            }
            tx.commit()?;
            Ok(attr)
        })
        .await
    }

    async fn truncate(&self, inode: Ino, length: u64) -> Result<Attr, MetaError> {
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let flags = load_format_flags(&tx)?;
            let mut attr = read_attr(&tx, inode)?;
            if !attr.is_file() {
                return Err(MetaError::IsDir(format!("inode {inode}")));
            }
            let old_len = attr.length;
            if length < old_len {
                let first_drop_indx = length.div_ceil(CHUNK_SIZE) as u32;
                let mut stmt = tx.prepare(
                    "SELECT indx, slices FROM jfs_chunk WHERE inode = ?1 AND indx >= ?2",
                )?;
                let rows = stmt.query_map(
                    params![inode.as_u64() as i64, i64::from(first_drop_indx)],
                    |r| Ok((r.get::<_, u32>(0)?, r.get::<_, Vec<u8>>(1)?)),
                )?;
                let mut dropped = Vec::new();
                for row in rows {
                    dropped.push(row?);
                }
                drop(stmt);
                for (indx, blob) in dropped {
                    for rec in decode_slices(&blob)? {
                        if rec.id > 0 {
                            tx.execute(
                                "UPDATE jfs_slice_ref SET refs = refs - 1 WHERE id = ?1",
                                params![rec.id as i64],
                            )?;
                        }
                    }
                    tx.execute(
                        "DELETE FROM jfs_chunk WHERE inode = ?1 AND indx = ?2",
                        params![inode.as_u64() as i64, i64::from(indx)],
                    )?;
                }
            }
            let delta = length as i64 - old_len as i64;
            let delta_space = align4k(length) - align4k(old_len);
            if delta != 0 || (flags.dir_stats && delta_space != 0) {
                let parent_dirs = file_parent_dirs(&tx, inode, &attr)?;
                if delta != 0 {
                    apply_quota_delta_dirs(&tx, &parent_dirs, delta, 0)?;
                    if flags.user_group_quota {
                        apply_ug_quota_delta(&tx, attr.uid, attr.gid, delta, 0)?;
                    }
                    let _ = next_counter(&tx, "usedSpace", delta)?;
                }
                if flags.dir_stats {
                    apply_dir_stats_delta(&tx, &parent_dirs, delta, delta_space, 0)?;
                }
            }
            let now = now_ns();
            let (us, sub) = split_ns(now);
            attr.length = length;
            attr.mtime = now / 1_000_000_000;
            attr.mtime_nsec = (now % 1_000_000_000) as u32;
            attr.ctime = attr.mtime;
            attr.ctime_nsec = attr.mtime_nsec;
            tx.execute(
                "UPDATE jfs_node SET length = ?1, mtime = ?2, mtimensec = ?3, ctime = ?2, ctimensec = ?3 WHERE inode = ?4",
                params![length as i64, us, sub, inode.as_u64() as i64],
            )?;
            tx.commit()?;
            Ok(attr)
        })
        .await
    }

    async fn set_xattr(&self, inode: Ino, name: &str, value: &[u8]) -> Result<(), MetaError> {
        let name = name.to_owned();
        let value = value.to_vec();
        self.run(move |conn| {
            let _ = read_attr(conn, inode)?;
            conn.execute(
                "INSERT INTO jfs_xattr (inode, name, value) VALUES (?1, ?2, ?3)
                 ON CONFLICT(inode, name) DO UPDATE SET value = ?3",
                params![inode.as_u64() as i64, name, value],
            )?;
            Ok(())
        })
        .await
    }

    async fn get_xattr(&self, inode: Ino, name: &str) -> Result<Vec<u8>, MetaError> {
        let name = name.to_owned();
        self.run(move |conn| {
            let val: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT value FROM jfs_xattr WHERE inode = ?1 AND name = ?2",
                    params![inode.as_u64() as i64, name],
                    |r| r.get(0),
                )
                .optional()?;
            val.ok_or_else(|| MetaError::NotFound(name))
        })
        .await
    }

    async fn list_xattr(&self, inode: Ino) -> Result<Vec<String>, MetaError> {
        self.run(move |conn| {
            let mut stmt =
                conn.prepare("SELECT name FROM jfs_xattr WHERE inode = ?1 ORDER BY name")?;
            let rows = stmt.query_map(params![inode.as_u64() as i64], |r| r.get::<_, String>(0))?;
            let mut names = Vec::new();
            for row in rows {
                names.push(row?);
            }
            Ok(names)
        })
        .await
    }

    async fn remove_xattr(&self, inode: Ino, name: &str) -> Result<(), MetaError> {
        let name = name.to_owned();
        self.run(move |conn| {
            let changed = conn.execute(
                "DELETE FROM jfs_xattr WHERE inode = ?1 AND name = ?2",
                params![inode.as_u64() as i64, name],
            )?;
            if changed == 0 {
                return Err(MetaError::NotFound(name));
            }
            Ok(())
        })
        .await
    }

    async fn list_orphan_slices(&self) -> Result<Vec<(u64, u32)>, MetaError> {
        self.run(|conn| {
            let mut stmt = conn.prepare("SELECT id, size FROM jfs_slice_ref WHERE refs <= 0")?;
            let rows = stmt.query_map([], |r| {
                Ok((r.get::<_, i64>(0)? as u64, r.get::<_, u32>(1)?))
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .await
    }

    async fn purge_orphan_slice(&self, slice_id: u64) -> Result<(), MetaError> {
        self.run(move |conn| {
            conn.execute(
                "DELETE FROM jfs_slice_ref WHERE id = ?1 AND refs <= 0",
                params![slice_id as i64],
            )?;
            Ok(())
        })
        .await
    }

    async fn flock(&self, inode: Ino, owner: u64, ltype: FlockType) -> Result<(), MetaError> {
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let ino_raw = inode.as_u64() as i64;
            let owner_raw = owner as i64;

            if ltype == FlockType::Unlock {
                tx.execute(
                    "DELETE FROM jfs_flock WHERE inode = ?1 AND sid = 0 AND owner = ?2",
                    params![ino_raw, owner_raw],
                )?;
                tx.commit()?;
                return Ok(());
            }

            let _ = read_attr(&tx, inode)?;
            let mut stmt =
                tx.prepare("SELECT sid, owner, ltype FROM jfs_flock WHERE inode = ?1")?;
            let rows = stmt.query_map(params![ino_raw], |r| {
                Ok((
                    r.get::<_, i64>(0)? as u64,
                    r.get::<_, i64>(1)? as u64,
                    r.get::<_, u8>(2)?,
                ))
            })?;
            let mut existing_self = false;
            for row in rows {
                let (sid, row_owner, raw_typ) = row?;
                if sid == 0 && row_owner == owner {
                    existing_self = true;
                    continue;
                }
                let other_typ = FlockType::try_from(raw_typ)?;
                if ltype == FlockType::Exclusive || other_typ == FlockType::Exclusive {
                    return Err(MetaError::WouldBlock(format!(
                        "flock conflict on inode {inode}"
                    )));
                }
            }
            drop(stmt);

            if existing_self {
                tx.execute(
                    "UPDATE jfs_flock SET ltype = ?1 WHERE inode = ?2 AND sid = 0 AND owner = ?3",
                    params![ltype as u8, ino_raw, owner_raw],
                )?;
            } else {
                tx.execute(
                    "INSERT INTO jfs_flock (inode, sid, owner, ltype) VALUES (?1, 0, ?2, ?3)",
                    params![ino_raw, owner_raw, ltype as u8],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
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
        self.run(move |conn| {
            let _ = read_attr(conn, inode)?;
            let mut stmt =
                conn.prepare("SELECT sid, owner, records FROM jfs_plock WHERE inode = ?1")?;
            let rows = stmt.query_map(params![inode.as_u64() as i64], |r| {
                Ok((
                    r.get::<_, i64>(0)? as u64,
                    r.get::<_, i64>(1)? as u64,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })?;
            for row in rows {
                let (sid, row_owner, blob) = row?;
                if sid == 0 && row_owner == owner {
                    continue;
                }
                for rec in decode_plocks(&blob)? {
                    if (lock.typ == FlockType::Exclusive || rec.typ == FlockType::Exclusive)
                        && lock.end >= rec.start
                        && lock.start <= rec.end
                    {
                        return Ok(PlockRecord {
                            typ: rec.typ,
                            pid: if sid == 0 { rec.pid } else { 0 },
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
        })
        .await
    }

    async fn set_lk(&self, inode: Ino, owner: u64, lock: PlockRecord) -> Result<(), MetaError> {
        if lock.start > lock.end {
            return Err(MetaError::InvalidInput(format!(
                "invalid lock range: {}..{}",
                lock.start, lock.end
            )));
        }
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let _ = read_attr(&tx, inode)?;
            let ino_raw = inode.as_u64() as i64;
            let owner_raw = owner as i64;

            if lock.typ == FlockType::Unlock {
                let existing: Option<Vec<u8>> = tx
                    .query_row(
                        "SELECT records FROM jfs_plock WHERE inode = ?1 AND sid = 0 AND owner = ?2",
                        params![ino_raw, owner_raw],
                        |r| r.get(0),
                    )
                    .optional()?;
                if let Some(blob) = existing {
                    let updated = update_plocks(decode_plocks(&blob)?, lock);
                    if updated.is_empty() {
                        tx.execute(
                            "DELETE FROM jfs_plock WHERE inode = ?1 AND sid = 0 AND owner = ?2",
                            params![ino_raw, owner_raw],
                        )?;
                    } else {
                        tx.execute(
                            "UPDATE jfs_plock SET records = ?1 WHERE inode = ?2 AND sid = 0 AND owner = ?3",
                            params![encode_plocks(&updated), ino_raw, owner_raw],
                        )?;
                    }
                }
                tx.commit()?;
                return Ok(());
            }

            let mut stmt =
                tx.prepare("SELECT sid, owner, records FROM jfs_plock WHERE inode = ?1")?;
            let rows = stmt.query_map(params![ino_raw], |r| {
                Ok((
                    r.get::<_, i64>(0)? as u64,
                    r.get::<_, i64>(1)? as u64,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })?;
            let mut self_records = Vec::new();
            let mut has_self_row = false;
            for row in rows {
                let (sid, row_owner, blob) = row?;
                let recs = decode_plocks(&blob)?;
                if sid == 0 && row_owner == owner {
                    has_self_row = true;
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
            drop(stmt);

            let updated = update_plocks(self_records, lock);
            let encoded = encode_plocks(&updated);
            if has_self_row {
                tx.execute(
                    "UPDATE jfs_plock SET records = ?1 WHERE inode = ?2 AND sid = 0 AND owner = ?3",
                    params![encoded, ino_raw, owner_raw],
                )?;
            } else {
                tx.execute(
                    "INSERT INTO jfs_plock (inode, sid, owner, records) VALUES (?1, 0, ?2, ?3)",
                    params![ino_raw, owner_raw, encoded],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    async fn set_quota(
        &self,
        inode: Ino,
        max_space: i64,
        max_inodes: i64,
    ) -> Result<Quota, MetaError> {
        self.run(move |conn| {
            if inode.is_trash() {
                return Err(MetaError::InvalidInput(
                    "cannot set quota on trash inode".to_owned(),
                ));
            }
            let tx = conn.transaction()?;
            let attr = read_attr(&tx, inode)?;
            if !attr.is_dir() {
                return Err(MetaError::NotDir(format!("inode {inode}")));
            }

            let existing: Option<(i64, i64)> = tx
                .query_row(
                    "SELECT used_space, used_inodes FROM jfs_dir_quota WHERE inode = ?1",
                    params![inode.as_u64() as i64],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;

            let (used_space, used_inodes) = match existing {
                Some(pair) => pair,
                None => calc_dir_descendant_usage(&tx, inode)?,
            };

            tx.execute(
                "INSERT INTO jfs_dir_quota (inode, max_space, max_inodes, used_space, used_inodes)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(inode) DO UPDATE SET max_space = ?2, max_inodes = ?3",
                params![
                    inode.as_u64() as i64,
                    max_space,
                    max_inodes,
                    used_space,
                    used_inodes
                ],
            )?;
            tx.commit()?;
            Ok(Quota {
                max_space,
                max_inodes,
                used_space,
                used_inodes,
            })
        })
        .await
    }

    async fn get_quota(&self, inode: Ino) -> Result<Quota, MetaError> {
        self.run(move |conn| {
            let quota: Option<Quota> = conn
                .query_row(
                    "SELECT max_space, max_inodes, used_space, used_inodes
                     FROM jfs_dir_quota WHERE inode = ?1",
                    params![inode.as_u64() as i64],
                    |r| {
                        Ok(Quota {
                            max_space: r.get(0)?,
                            max_inodes: r.get(1)?,
                            used_space: r.get(2)?,
                            used_inodes: r.get(3)?,
                        })
                    },
                )
                .optional()?;
            quota.ok_or_else(|| MetaError::NotFound(format!("quota for inode {inode}")))
        })
        .await
    }

    async fn del_quota(&self, inode: Ino) -> Result<(), MetaError> {
        self.run(move |conn| {
            let changed = conn.execute(
                "DELETE FROM jfs_dir_quota WHERE inode = ?1",
                params![inode.as_u64() as i64],
            )?;
            if changed == 0 {
                return Err(MetaError::NotFound(format!("quota for inode {inode}")));
            }
            Ok(())
        })
        .await
    }

    async fn list_quotas(&self) -> Result<Vec<(Ino, Quota)>, MetaError> {
        self.run(|conn| {
            let mut stmt = conn.prepare(
                "SELECT inode, max_space, max_inodes, used_space, used_inodes
                 FROM jfs_dir_quota ORDER BY inode",
            )?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    Ino(r.get::<_, i64>(0)? as u64),
                    Quota {
                        max_space: r.get(1)?,
                        max_inodes: r.get(2)?,
                        used_space: r.get(3)?,
                        used_inodes: r.get(4)?,
                    },
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .await
    }

    async fn move_to_trash(&self, parent: Ino, name: &str) -> Result<String, MetaError> {
        validate_name(name)?;
        if parent.is_trash() {
            return Err(MetaError::InvalidInput(
                "cannot move trash entry into trash".to_owned(),
            ));
        }
        let name = name.to_owned();
        self.run(move |conn| {
            let fmt = load_format_sync(conn)?;
            if fmt.trash_days <= 0 {
                return Err(MetaError::InvalidInput(
                    "trash is disabled (trash_days <= 0)".to_owned(),
                ));
            }
            let tx = conn.transaction()?;
            let now = now_ns();
            ensure_trash_node(&tx, now)?;

            let row: Option<(i64, u8)> = tx
                .query_row(
                    "SELECT inode, type FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                    params![parent.as_u64() as i64, name.as_bytes()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let (ino_raw, typ_raw) = row.ok_or_else(|| MetaError::NotFound(name.clone()))?;
            let ino = Ino(ino_raw as u64);
            let is_dir = NodeType::try_from(typ_raw)? == NodeType::Directory;
            let trash_name = make_trash_entry_name(parent, ino, &name);

            let (moved_space, moved_inodes) = if is_dir {
                let (s, i) = calc_dir_descendant_usage(&tx, ino)?;
                (s, i + 1)
            } else {
                let attr = read_attr(&tx, ino)?;
                (attr.length as i64, 1)
            };
            apply_quota_delta(&tx, parent, -moved_space, -moved_inodes)?;
            if fmt.dir_stats {
                let (stat_len, stat_space, stat_inodes) = if is_dir {
                    calc_dir_descendant_stats(&tx, ino)?
                } else {
                    let attr = read_attr(&tx, ino)?;
                    (attr.length as i64, align4k(attr.length), 1)
                };
                apply_dir_stats_delta(&tx, &[parent], -stat_len, -stat_space, -stat_inodes)?;
            }

            tx.execute(
                "DELETE FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                params![parent.as_u64() as i64, name.as_bytes()],
            )?;
            tx.execute(
                "INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?1, ?2, ?3, ?4)",
                params![
                    TRASH_INODE.as_u64() as i64,
                    trash_name.as_bytes(),
                    ino_raw,
                    typ_raw
                ],
            )?;
            tx.execute(
                "UPDATE jfs_node SET parent = ?1 WHERE inode = ?2 AND parent != 0",
                params![TRASH_INODE.as_u64() as i64, ino_raw],
            )?;
            if is_dir {
                tx.execute(
                    "UPDATE jfs_node SET nlink = MAX(2, nlink - 1) WHERE inode = ?1",
                    params![parent.as_u64() as i64],
                )?;
                tx.execute(
                    "UPDATE jfs_node SET nlink = nlink + 1 WHERE inode = ?1",
                    params![TRASH_INODE.as_u64() as i64],
                )?;
            }
            update_parent_mtime(&tx, parent, now)?;
            update_parent_mtime(&tx, TRASH_INODE, now)?;
            tx.commit()?;
            Ok(trash_name)
        })
        .await
    }

    async fn empty_trash(&self) -> Result<u64, MetaError> {
        self.run(|conn| {
            let tx = conn.transaction()?;
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT inode FROM jfs_node WHERE inode = ?1",
                    params![TRASH_INODE.as_u64() as i64],
                    |r| r.get(0),
                )
                .optional()?;
            if exists.is_none() {
                return Ok(0);
            }
            let deleted = purge_trash_dir_recursive(&tx, TRASH_INODE)?;
            let now = now_ns();
            let (us, sub) = split_ns(now);
            tx.execute(
                "UPDATE jfs_node SET nlink = 2, mtime = ?1, mtimensec = ?2, ctime = ?1, ctimensec = ?2
                 WHERE inode = ?3",
                params![us, sub, TRASH_INODE.as_u64() as i64],
            )?;
            tx.commit()?;
            Ok(deleted)
        })
        .await
    }

    async fn restore_from_trash(&self, trash_name: &str) -> Result<(Ino, Attr), MetaError> {
        validate_name(trash_name)?;
        let trash_name = trash_name.to_owned();
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let row: Option<(i64, u8)> = tx
                .query_row(
                    "SELECT inode, type FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                    params![TRASH_INODE.as_u64() as i64, trash_name.as_bytes()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let (ino_raw, typ_raw) = row.ok_or_else(|| MetaError::NotFound(trash_name.clone()))?;
            let ino = Ino(ino_raw as u64);

            let (orig_parent, _orig_ino, orig_name) = parse_trash_entry_name(&trash_name)?;
            let pattr = read_attr(&tx, orig_parent)?;
            if !pattr.is_dir() {
                return Err(MetaError::NotDir(format!("inode {orig_parent}")));
            }
            let conflict: Option<i64> = tx
                .query_row(
                    "SELECT inode FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                    params![orig_parent.as_u64() as i64, orig_name.as_bytes()],
                    |r| r.get(0),
                )
                .optional()?;
            if conflict.is_some() {
                return Err(MetaError::AlreadyExists(orig_name.to_owned()));
            }

            let is_dir = NodeType::try_from(typ_raw)? == NodeType::Directory;
            let (restored_space, restored_inodes) = if is_dir {
                let (s, i) = calc_dir_descendant_usage(&tx, ino)?;
                (s, i + 1)
            } else {
                let attr = read_attr(&tx, ino)?;
                (attr.length as i64, 1)
            };
            apply_quota_delta(&tx, orig_parent, restored_space, restored_inodes)?;
            let flags = load_format_flags(&tx)?;
            if flags.dir_stats {
                let (stat_len, stat_space, stat_inodes) = if is_dir {
                    calc_dir_descendant_stats(&tx, ino)?
                } else {
                    let attr = read_attr(&tx, ino)?;
                    (attr.length as i64, align4k(attr.length), 1)
                };
                apply_dir_stats_delta(&tx, &[orig_parent], stat_len, stat_space, stat_inodes)?;
            }

            tx.execute(
                "DELETE FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                params![TRASH_INODE.as_u64() as i64, trash_name.as_bytes()],
            )?;
            tx.execute(
                "INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?1, ?2, ?3, ?4)",
                params![
                    orig_parent.as_u64() as i64,
                    orig_name.as_bytes(),
                    ino_raw,
                    typ_raw
                ],
            )?;
            tx.execute(
                "UPDATE jfs_node SET parent = ?1 WHERE inode = ?2 AND parent != 0",
                params![orig_parent.as_u64() as i64, ino_raw],
            )?;
            if is_dir {
                tx.execute(
                    "UPDATE jfs_node SET nlink = MAX(2, nlink - 1) WHERE inode = ?1",
                    params![TRASH_INODE.as_u64() as i64],
                )?;
                tx.execute(
                    "UPDATE jfs_node SET nlink = nlink + 1 WHERE inode = ?1",
                    params![orig_parent.as_u64() as i64],
                )?;
            }
            let now = now_ns();
            update_parent_mtime(&tx, TRASH_INODE, now)?;
            update_parent_mtime(&tx, orig_parent, now)?;
            let attr = read_attr(&tx, ino)?;
            tx.commit()?;
            Ok((ino, attr))
        })
        .await
    }

    async fn dump_json(&self) -> Result<String, MetaError> {
        self.run(|conn| crate::dump::dump_to_json(conn)).await
    }

    async fn load_json(&self, json: &str) -> Result<(), MetaError> {
        let json = json.to_owned();
        self.run(move |conn| crate::dump::load_from_json(conn, &json))
            .await
    }

    async fn fsck(&self) -> Result<FsckReport, MetaError> {
        self.run(|conn| fsck_sync(conn)).await
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
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let attr = read_attr(&tx, inode)?;
            if !attr.is_file() {
                return Err(MetaError::IsDir(format!("inode {inode}")));
            }
            let blob: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT slices FROM jfs_chunk WHERE inode = ?1 AND indx = ?2",
                    params![inode.as_u64() as i64, i64::from(indx)],
                    |r| r.get(0),
                )
                .optional()?;
            let blob =
                blob.ok_or_else(|| MetaError::NotFound(format!("chunk {inode}:{indx} not found")))?;
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

            tx.execute(
                "UPDATE jfs_chunk SET slices = ?1 WHERE inode = ?2 AND indx = ?3",
                params![new_blob, inode.as_u64() as i64, i64::from(indx)],
            )?;

            if new_slice.id > 0 {
                tx.execute(
                    "INSERT INTO jfs_slice_ref (id, size, refs) VALUES (?1, ?2, 1)
                     ON CONFLICT(id) DO UPDATE SET refs = refs + 1",
                    params![new_slice.id as i64, i64::from(new_slice.size)],
                )?;
            }
            for rec in old_records {
                if rec.id > 0 {
                    tx.execute(
                        "UPDATE jfs_slice_ref SET refs = refs - 1 WHERE id = ?1",
                        params![rec.id as i64],
                    )?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    async fn update_format(&self, format: &Format) -> Result<(), MetaError> {
        let format_json = serde_json::to_string(format)?;
        self.run(move |conn| {
            let changed = conn.execute(
                "UPDATE jfs_setting SET value = ?1 WHERE name = 'format'",
                params![format_json],
            )?;
            if changed == 0 {
                return Err(MetaError::NotFormatted);
            }
            Ok(())
        })
        .await
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
        let src_name = src_name.to_owned();
        let dst_name = dst_name.to_owned();
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let src_ino_raw: Option<i64> = tx
                .query_row(
                    "SELECT inode FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                    params![src_parent.as_u64() as i64, src_name.as_bytes()],
                    |r| r.get(0),
                )
                .optional()?;
            let src_ino =
                Ino(src_ino_raw.ok_or_else(|| MetaError::NotFound(src_name.clone()))? as u64);
            let dst_parent_attr = read_attr(&tx, dst_parent)?;
            if !dst_parent_attr.is_dir() {
                return Err(MetaError::NotDir(format!("inode {dst_parent}")));
            }
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT inode FROM jfs_edge WHERE parent = ?1 AND name = ?2",
                    params![dst_parent.as_u64() as i64, dst_name.as_bytes()],
                    |r| r.get(0),
                )
                .optional()?;
            if exists.is_some() {
                return Err(MetaError::AlreadyExists(dst_name));
            }
            let now = now_ns();
            let result = clone_node_recursive(&tx, src_ino, dst_parent, &dst_name, now)?;
            update_parent_mtime(&tx, dst_parent, now)?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }

    async fn get_dir_stat(&self, inode: Ino) -> Result<DirStat, MetaError> {
        self.run(move |conn| {
            let attr = read_attr(conn, inode)?;
            if !attr.is_dir() {
                return Err(MetaError::NotDir(format!("inode {inode}")));
            }
            let stat: Option<DirStat> = conn
                .query_row(
                    "SELECT data_length, used_space, used_inodes FROM jfs_dirstats WHERE inode = ?1",
                    params![inode.as_u64() as i64],
                    |r| {
                        Ok(DirStat {
                            data_length: r.get(0)?,
                            used_space: r.get(1)?,
                            used_inodes: r.get(2)?,
                        })
                    },
                )
                .optional()?;
            Ok(stat.unwrap_or_default())
        })
        .await
    }

    async fn set_ug_quota(
        &self,
        target: QuotaTarget,
        max_space: i64,
        max_inodes: i64,
    ) -> Result<Quota, MetaError> {
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let (qtype, qkey) = ug_quota_key(target);
            let existing: Option<(i64, i64)> = tx
                .query_row(
                    "SELECT used_space, used_inodes FROM jfs_ug_quota WHERE qtype = ?1 AND qkey = ?2",
                    params![qtype, qkey],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let (used_space, used_inodes) = if let Some(pair) = existing {
                pair
            } else {
                let sql = match target {
                    QuotaTarget::User(_) => {
                        "SELECT COALESCE(SUM(CASE WHEN type = 1 THEN length ELSE 0 END), 0), COUNT(1)
                         FROM jfs_node WHERE uid = ?1 AND inode > 1 AND inode < ?2"
                    }
                    QuotaTarget::Group(_) => {
                        "SELECT COALESCE(SUM(CASE WHEN type = 1 THEN length ELSE 0 END), 0), COUNT(1)
                         FROM jfs_node WHERE gid = ?1 AND inode > 1 AND inode < ?2"
                    }
                };
                tx.query_row(sql, params![qkey, TRASH_INODE.as_u64() as i64], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
                })?
            };
            tx.execute(
                "INSERT INTO jfs_ug_quota (qtype, qkey, max_space, max_inodes, used_space, used_inodes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(qtype, qkey) DO UPDATE SET max_space = ?3, max_inodes = ?4",
                params![qtype, qkey, max_space, max_inodes, used_space, used_inodes],
            )?;
            tx.commit()?;
            Ok(Quota {
                max_space,
                max_inodes,
                used_space,
                used_inodes,
            })
        })
        .await
    }

    async fn get_ug_quota(&self, target: QuotaTarget) -> Result<Quota, MetaError> {
        self.run(move |conn| {
            read_ug_quota_opt(conn, target)?
                .ok_or_else(|| MetaError::NotFound(format!("ug quota for {target:?}")))
        })
        .await
    }

    async fn del_ug_quota(&self, target: QuotaTarget) -> Result<(), MetaError> {
        self.run(move |conn| {
            let (qtype, qkey) = ug_quota_key(target);
            let changed = conn.execute(
                "DELETE FROM jfs_ug_quota WHERE qtype = ?1 AND qkey = ?2",
                params![qtype, qkey],
            )?;
            if changed == 0 {
                return Err(MetaError::NotFound(format!("ug quota for {target:?}")));
            }
            Ok(())
        })
        .await
    }

    async fn list_ug_quotas(&self) -> Result<Vec<(QuotaTarget, Quota)>, MetaError> {
        self.run(|conn| {
            let mut stmt = conn.prepare(
                "SELECT qtype, qkey, max_space, max_inodes, used_space, used_inodes
                 FROM jfs_ug_quota ORDER BY qtype, qkey",
            )?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, u8>(0)?,
                    r.get::<_, i64>(1)?,
                    Quota {
                        max_space: r.get(2)?,
                        max_inodes: r.get(3)?,
                        used_space: r.get(4)?,
                        used_inodes: r.get(5)?,
                    },
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (qtype, qkey, quota) = row?;
                out.push((ug_quota_from_row(qtype, qkey)?, quota));
            }
            Ok(out)
        })
        .await
    }

    async fn read_changelog(
        &self,
        after_id: i64,
        limit: usize,
    ) -> Result<Vec<ChangeLogEntry>, MetaError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
        self.run(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT id, entry FROM jfs_changelog WHERE id > ?1 ORDER BY id ASC LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![after_id, limit_i64], |r| {
                Ok(ChangeLogEntry {
                    id: r.get(0)?,
                    entry: r.get(1)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .await
    }

    async fn new_session(
        &self,
        mount_point: &str,
        ttl_secs: i64,
    ) -> Result<SessionInfo, MetaError> {
        let mount_point = mount_point.to_owned();
        let info = encode_session_info(&mount_point)?;
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let sid = next_counter(&tx, "nextSession", 1)? as u64;
            let expire = now_secs() + ttl_secs;
            tx.execute(
                "INSERT INTO jfs_session2 (sid, expire, info) VALUES (?1, ?2, ?3)",
                params![sid as i64, expire, info],
            )?;
            tx.commit()?;
            Ok(SessionInfo {
                sid,
                expire,
                mount_point,
            })
        })
        .await
    }

    async fn heartbeat(&self, sid: u64, ttl_secs: i64) -> Result<(), MetaError> {
        self.run(move |conn| {
            let expire = now_secs() + ttl_secs;
            let changed = conn.execute(
                "UPDATE jfs_session2 SET expire = ?1 WHERE sid = ?2",
                params![expire, sid as i64],
            )?;
            if changed == 0 {
                return Err(MetaError::NotFound(format!("session {sid}")));
            }
            Ok(())
        })
        .await
    }

    async fn close_session(&self, sid: u64) -> Result<(), MetaError> {
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let sid_raw = sid as i64;
            let changed =
                tx.execute("DELETE FROM jfs_session2 WHERE sid = ?1", params![sid_raw])?;
            if changed == 0 {
                return Err(MetaError::NotFound(format!("session {sid}")));
            }
            tx.execute("DELETE FROM jfs_flock WHERE sid = ?1", params![sid_raw])?;
            tx.execute("DELETE FROM jfs_plock WHERE sid = ?1", params![sid_raw])?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    async fn list_sessions(&self) -> Result<Vec<SessionInfo>, MetaError> {
        self.run(|conn| {
            let mut stmt =
                conn.prepare("SELECT sid, expire, info FROM jfs_session2 ORDER BY sid ASC")?;
            let rows = stmt.query_map([], |r| {
                let sid = r.get::<_, i64>(0)? as u64;
                let expire = r.get::<_, i64>(1)?;
                let info: Option<Vec<u8>> = r.get(2)?;
                Ok(SessionInfo {
                    sid,
                    expire,
                    mount_point: decode_session_mount_point(info.as_deref().unwrap_or(&[])),
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .await
    }

    async fn cleanup_stale_sessions(&self) -> Result<u64, MetaError> {
        self.run(|conn| {
            let tx = conn.transaction()?;
            let now = now_secs();
            let mut stmt = tx.prepare("SELECT sid FROM jfs_session2 WHERE expire <= ?1")?;
            let rows = stmt.query_map(params![now], |r| r.get::<_, i64>(0))?;
            let mut stale_sids = Vec::new();
            for row in rows {
                stale_sids.push(row?);
            }
            drop(stmt);

            for sid_raw in &stale_sids {
                tx.execute("DELETE FROM jfs_flock WHERE sid = ?1", params![sid_raw])?;
                tx.execute("DELETE FROM jfs_plock WHERE sid = ?1", params![sid_raw])?;
            }
            let deleted =
                tx.execute("DELETE FROM jfs_session2 WHERE expire <= ?1", params![now])?;
            tx.commit()?;
            Ok(deleted as u64)
        })
        .await
    }
}

fn rename_sync(
    conn: &mut Connection,
    src_parent: Ino,
    src_name: &str,
    dst_parent: Ino,
    dst_name: &str,
) -> Result<(), MetaError> {
    let tx = conn.transaction()?;
    let flags = load_format_flags(&tx)?;
    let src_row: Option<(i64, u8)> = tx
        .query_row(
            "SELECT inode, type FROM jfs_edge WHERE parent = ?1 AND name = ?2",
            params![src_parent.as_u64() as i64, src_name.as_bytes()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let (src_ino_raw, src_typ) = src_row.ok_or_else(|| MetaError::NotFound(src_name.to_owned()))?;
    let dst_parent_attr = read_attr(&tx, dst_parent)?;
    if !dst_parent_attr.is_dir() {
        return Err(MetaError::NotDir(format!("inode {dst_parent}")));
    }

    if src_parent == dst_parent && src_name == dst_name {
        return Ok(());
    }

    remove_existing_dst(&tx, dst_parent, dst_name, flags)?;

    let is_dir = NodeType::try_from(src_typ)? == NodeType::Directory;
    if src_parent != dst_parent {
        let src_ino = Ino(src_ino_raw as u64);
        let (moved_space, moved_inodes) = if is_dir {
            let (s, i) = calc_dir_descendant_usage(&tx, src_ino)?;
            (s, i + 1)
        } else {
            let src_attr = read_attr(&tx, src_ino)?;
            (src_attr.length as i64, 1)
        };
        apply_quota_delta(&tx, src_parent, -moved_space, -moved_inodes)?;
        apply_quota_delta(&tx, dst_parent, moved_space, moved_inodes)?;

        if flags.dir_stats {
            let (stat_len, stat_space, stat_inodes) = if is_dir {
                calc_dir_descendant_stats(&tx, src_ino)?
            } else {
                let src_attr = read_attr(&tx, src_ino)?;
                (src_attr.length as i64, align4k(src_attr.length), 1)
            };
            apply_dir_stats_delta(&tx, &[src_parent], -stat_len, -stat_space, -stat_inodes)?;
            apply_dir_stats_delta(&tx, &[dst_parent], stat_len, stat_space, stat_inodes)?;
        }
    }

    tx.execute(
        "DELETE FROM jfs_edge WHERE parent = ?1 AND name = ?2",
        params![src_parent.as_u64() as i64, src_name.as_bytes()],
    )?;
    tx.execute(
        "INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?1, ?2, ?3, ?4)",
        params![
            dst_parent.as_u64() as i64,
            dst_name.as_bytes(),
            src_ino_raw,
            src_typ
        ],
    )?;
    if is_dir && src_parent != dst_parent {
        tx.execute(
            "UPDATE jfs_node SET nlink = MAX(2, nlink - 1) WHERE inode = ?1",
            params![src_parent.as_u64() as i64],
        )?;
        tx.execute(
            "UPDATE jfs_node SET nlink = nlink + 1 WHERE inode = ?1",
            params![dst_parent.as_u64() as i64],
        )?;
    }
    tx.execute(
        "UPDATE jfs_node SET parent = ?1 WHERE inode = ?2 AND parent != 0",
        params![dst_parent.as_u64() as i64, src_ino_raw],
    )?;
    let now = now_ns();
    if flags.change_log {
        record_changelog(
            &tx,
            &serde_json::json!({
                "op": "RENAME",
                "src_parent": src_parent.as_u64(),
                "src_name": src_name,
                "dst_parent": dst_parent.as_u64(),
                "dst_name": dst_name,
                "inode": src_ino_raw as u64,
                "ts": now,
            }),
        )?;
    }
    update_parent_mtime(&tx, src_parent, now)?;
    if dst_parent != src_parent {
        update_parent_mtime(&tx, dst_parent, now)?;
    }
    tx.commit()?;
    Ok(())
}

fn clone_node_recursive(
    tx: &Transaction<'_>,
    src_ino: Ino,
    dst_parent: Ino,
    dst_name: &str,
    now: i64,
) -> Result<(Ino, Attr), MetaError> {
    let mut attr = read_attr(tx, src_ino)?;
    let new_ino = Ino(next_counter(tx, "nextInode", 1)? as u64);
    let _ = next_counter(tx, "usedInodes", 1)?;

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

    insert_node(tx, new_ino, &attr)?;
    tx.execute(
        "INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?1, ?2, ?3, ?4)",
        params![
            dst_parent.as_u64() as i64,
            dst_name.as_bytes(),
            new_ino.as_u64() as i64,
            attr.typ as u8
        ],
    )?;

    if attr.is_file() {
        if attr.length > 0 {
            let _ = next_counter(tx, "usedSpace", attr.length as i64)?;
        }
        let mut stmt = tx.prepare("SELECT indx, slices FROM jfs_chunk WHERE inode = ?1")?;
        let rows = stmt.query_map(params![src_ino.as_u64() as i64], |r| {
            Ok((r.get::<_, u32>(0)?, r.get::<_, Vec<u8>>(1)?))
        })?;
        let mut chunks = Vec::new();
        for row in rows {
            chunks.push(row?);
        }
        drop(stmt);

        for (indx, blob) in chunks {
            tx.execute(
                "INSERT INTO jfs_chunk (inode, indx, slices) VALUES (?1, ?2, ?3)",
                params![new_ino.as_u64() as i64, i64::from(indx), &blob],
            )?;
            for rec in decode_slices(&blob)? {
                if rec.id > 0 {
                    tx.execute(
                        "UPDATE jfs_slice_ref SET refs = refs + 1 WHERE id = ?1",
                        params![rec.id as i64],
                    )?;
                }
            }
        }
    } else if attr.is_symlink() {
        let target: Option<Vec<u8>> = tx
            .query_row(
                "SELECT target FROM jfs_symlink WHERE inode = ?1",
                params![src_ino.as_u64() as i64],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(t) = target {
            tx.execute(
                "INSERT INTO jfs_symlink (inode, target) VALUES (?1, ?2)",
                params![new_ino.as_u64() as i64, t],
            )?;
        }
    } else if attr.is_dir() {
        tx.execute(
            "UPDATE jfs_node SET nlink = nlink + 1 WHERE inode = ?1",
            params![dst_parent.as_u64() as i64],
        )?;
        let mut stmt =
            tx.prepare("SELECT name, inode FROM jfs_edge WHERE parent = ?1 ORDER BY name")?;
        let rows = stmt.query_map(params![src_ino.as_u64() as i64], |r| {
            let nb: Vec<u8> = r.get(0)?;
            let child_ino: i64 = r.get(1)?;
            Ok((
                String::from_utf8_lossy(&nb).into_owned(),
                Ino(child_ino as u64),
            ))
        })?;
        let mut children = Vec::new();
        for row in rows {
            children.push(row?);
        }
        drop(stmt);

        for (cname, cino) in children {
            clone_node_recursive(tx, cino, new_ino, &cname, now)?;
        }
    }

    copy_xattrs(tx, src_ino, new_ino)?;

    Ok((new_ino, attr))
}

fn copy_xattrs(tx: &Transaction<'_>, src_ino: Ino, dst_ino: Ino) -> Result<(), MetaError> {
    let mut xstmt = tx.prepare("SELECT name, value FROM jfs_xattr WHERE inode = ?1")?;
    let xrows = xstmt.query_map(params![src_ino.as_u64() as i64], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
    })?;
    let mut xattrs = Vec::new();
    for row in xrows {
        xattrs.push(row?);
    }
    drop(xstmt);
    for (xname, xval) in xattrs {
        tx.execute(
            "INSERT INTO jfs_xattr (inode, name, value) VALUES (?1, ?2, ?3)",
            params![dst_ino.as_u64() as i64, xname, xval],
        )?;
    }
    Ok(())
}

fn fsck_sync(conn: &Connection) -> Result<FsckReport, MetaError> {
    let _ = load_format_sync(conn)?;

    let mut nodes: BTreeMap<u64, (NodeType, u32)> = BTreeMap::new();
    let mut nstmt = conn.prepare("SELECT inode, type, nlink FROM jfs_node ORDER BY inode")?;
    let nrows = nstmt.query_map([], |r| {
        Ok((
            r.get::<_, i64>(0)? as u64,
            r.get::<_, u8>(1)?,
            r.get::<_, u32>(2)?,
        ))
    })?;
    for row in nrows {
        let (ino, typ_raw, nlink) = row?;
        nodes.insert(ino, (NodeType::try_from(typ_raw)?, nlink));
    }
    drop(nstmt);

    let mut estmt =
        conn.prepare("SELECT parent, name, inode, type FROM jfs_edge ORDER BY parent, name")?;
    let erows = estmt.query_map([], |r| {
        Ok((
            r.get::<_, i64>(0)? as u64,
            r.get::<_, Vec<u8>>(1)?,
            r.get::<_, i64>(2)? as u64,
            r.get::<_, u8>(3)?,
        ))
    })?;
    let mut edges = Vec::new();
    for row in erows {
        edges.push(row?);
    }
    drop(estmt);

    let mut report = FsckReport {
        checked_nodes: nodes.len(),
        checked_edges: edges.len(),
        ..FsckReport::default()
    };

    let mut incoming_edges: BTreeMap<u64, u32> = BTreeMap::new();
    let mut child_dir_count: BTreeMap<u64, u32> = BTreeMap::new();
    let mut missing_set = std::collections::BTreeSet::new();

    for (parent, name_bytes, child, edge_typ_raw) in &edges {
        let parent_valid = nodes
            .get(parent)
            .is_some_and(|(typ, _)| *typ == NodeType::Directory);
        if !parent_valid {
            report
                .orphaned_edges
                .push(format!("{parent}:{}", String::from_utf8_lossy(name_bytes)));
        }

        if let Some((child_typ, _)) = nodes.get(child) {
            *incoming_edges.entry(*child).or_insert(0) += 1;
            if *child_typ == NodeType::Directory {
                *child_dir_count.entry(*parent).or_insert(0) += 1;
            }
        } else {
            missing_set.insert(*child);
            if matches!(NodeType::try_from(*edge_typ_raw), Ok(NodeType::Directory)) {
                *child_dir_count.entry(*parent).or_insert(0) += 1;
            }
        }
    }
    report.missing_nodes = missing_set.into_iter().collect();

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_meta_lifecycle() {
        let meta = SqliteMeta::open(":memory:").expect("open in-memory sqlite");
        let fmt = Format::new("testvol", "mem", "");
        meta.init(&fmt, false).await.expect("init");
        let loaded = meta.load().await.expect("load");
        assert_eq!(loaded.name, "testvol");

        let (dir_ino, _) = meta
            .mkdir(ROOT_INODE, "docs", 0o755, 1000, 1000)
            .await
            .expect("mkdir");
        let (file_ino, _) = meta
            .mknod(dir_ino, "hello.txt", NodeType::File, 0o644, 1000, 1000)
            .await
            .expect("mknod");

        let sid = meta.new_slice().await.expect("new_slice");
        let attr = meta
            .write_chunk(
                file_ino,
                0,
                0,
                Slice {
                    id: sid,
                    size: 11,
                    off: 0,
                    len: 11,
                },
                11,
            )
            .await
            .expect("write_chunk");
        assert_eq!(attr.length, 11);

        let entries = meta.readdir(dir_ino).await.expect("readdir");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "hello.txt");

        meta.unlink(dir_ino, "hello.txt").await.expect("unlink");
        let orphans = meta.list_orphan_slices().await.expect("orphans");
        assert_eq!(orphans, vec![(sid, 11)]);
        meta.purge_orphan_slice(sid).await.expect("purge");
    }
}

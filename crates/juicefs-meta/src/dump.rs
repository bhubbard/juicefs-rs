//! JSON metadata dump and load helpers modeled after `pkg/meta/dump.go`.

use std::collections::{BTreeMap, HashSet};

use juicefs_core::{
    Attr, Format, Ino, NodeType, ROOT_INODE, SliceRecord, TRASH_INODE, decode_slices, encode_slice,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};

use crate::{MetaError, Quota};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DumpedXattr {
    name: String,
    value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DumpedChunk {
    indx: u32,
    slices: Vec<SliceRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DumpedOrphanSlice {
    id: u64,
    size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DumpedEntry {
    inode: Ino,
    attr: Attr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    symlink: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    xattrs: Vec<DumpedXattr>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    chunks: Vec<DumpedChunk>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    entries: BTreeMap<String, DumpedEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DumpedMeta {
    format: Format,
    counters: BTreeMap<String, i64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    quotas: BTreeMap<u64, Quota>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    orphan_slices: Vec<DumpedOrphanSlice>,
    tree: DumpedEntry,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trash: Option<DumpedEntry>,
}

fn read_node_attr(conn: &Connection, inode: Ino) -> Result<Option<Attr>, MetaError> {
    let row = conn
        .query_row(
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
        .optional()?;
    let Some((typ_raw, mut attr)) = row else {
        return Ok(None);
    };
    attr.typ = NodeType::try_from(typ_raw)?;
    Ok(Some(attr))
}

fn dump_entry(conn: &Connection, inode: Ino) -> Result<DumpedEntry, MetaError> {
    let attr = read_node_attr(conn, inode)?
        .ok_or_else(|| MetaError::NotFound(format!("inode {inode}")))?;

    let symlink = if attr.is_symlink() {
        conn.query_row(
            "SELECT target FROM jfs_symlink WHERE inode = ?1",
            params![inode.as_u64() as i64],
            |r| r.get::<_, Vec<u8>>(0),
        )
        .optional()?
        .map(|b| String::from_utf8_lossy(&b).into_owned())
    } else {
        None
    };

    let mut xattr_stmt =
        conn.prepare("SELECT name, value FROM jfs_xattr WHERE inode = ?1 ORDER BY name")?;
    let xattr_rows = xattr_stmt.query_map(params![inode.as_u64() as i64], |r| {
        Ok(DumpedXattr {
            name: r.get(0)?,
            value: r.get(1)?,
        })
    })?;
    let mut xattrs = Vec::new();
    for row in xattr_rows {
        xattrs.push(row?);
    }
    drop(xattr_stmt);

    let mut chunks = Vec::new();
    if attr.is_file() {
        let mut chunk_stmt =
            conn.prepare("SELECT indx, slices FROM jfs_chunk WHERE inode = ?1 ORDER BY indx")?;
        let chunk_rows = chunk_stmt.query_map(params![inode.as_u64() as i64], |r| {
            Ok((r.get::<_, u32>(0)?, r.get::<_, Vec<u8>>(1)?))
        })?;
        for row in chunk_rows {
            let (indx, blob) = row?;
            chunks.push(DumpedChunk {
                indx,
                slices: decode_slices(&blob)?,
            });
        }
        drop(chunk_stmt);
    }

    let mut entries = BTreeMap::new();
    if attr.is_dir() {
        let mut edge_stmt =
            conn.prepare("SELECT name, inode FROM jfs_edge WHERE parent = ?1 ORDER BY name")?;
        let edge_rows = edge_stmt.query_map(params![inode.as_u64() as i64], |r| {
            let name_bytes: Vec<u8> = r.get(0)?;
            let child_ino: i64 = r.get(1)?;
            Ok((
                String::from_utf8_lossy(&name_bytes).into_owned(),
                Ino(child_ino as u64),
            ))
        })?;
        let mut children = Vec::new();
        for row in edge_rows {
            children.push(row?);
        }
        drop(edge_stmt);

        for (name, child_ino) in children {
            entries.insert(name, dump_entry(conn, child_ino)?);
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

pub(crate) fn dump_to_json(conn: &Connection) -> Result<String, MetaError> {
    let raw_fmt: Option<String> = conn
        .query_row(
            "SELECT value FROM jfs_setting WHERE name = 'format'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    let format: Format = serde_json::from_str(&raw_fmt.ok_or(MetaError::NotFormatted)?)?;

    let mut counters = BTreeMap::new();
    let mut cstmt = conn.prepare("SELECT name, value FROM jfs_counter ORDER BY name")?;
    let crows = cstmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
    for row in crows {
        let (k, v) = row?;
        counters.insert(k, v);
    }
    drop(cstmt);

    let mut quotas = BTreeMap::new();
    let mut qstmt = conn.prepare(
        "SELECT inode, max_space, max_inodes, used_space, used_inodes FROM jfs_dir_quota ORDER BY inode",
    )?;
    let qrows = qstmt.query_map([], |r| {
        Ok((
            r.get::<_, i64>(0)? as u64,
            Quota {
                max_space: r.get(1)?,
                max_inodes: r.get(2)?,
                used_space: r.get(3)?,
                used_inodes: r.get(4)?,
            },
        ))
    })?;
    for row in qrows {
        let (ino, q) = row?;
        quotas.insert(ino, q);
    }
    drop(qstmt);

    let mut orphan_slices = Vec::new();
    let mut sstmt =
        conn.prepare("SELECT id, size FROM jfs_slice_ref WHERE refs <= 0 ORDER BY id")?;
    let srows = sstmt.query_map([], |r| {
        Ok(DumpedOrphanSlice {
            id: r.get::<_, i64>(0)? as u64,
            size: r.get(1)?,
        })
    })?;
    for row in srows {
        orphan_slices.push(row?);
    }
    drop(sstmt);

    let tree = dump_entry(conn, ROOT_INODE)?;
    let trash = if read_node_attr(conn, TRASH_INODE)?.is_some() {
        Some(dump_entry(conn, TRASH_INODE)?)
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

fn load_entry(
    tx: &Transaction<'_>,
    entry: &DumpedEntry,
    seen_inodes: &mut HashSet<u64>,
) -> Result<(), MetaError> {
    let ino = entry.inode;
    if seen_inodes.insert(ino.as_u64()) {
        crate::sqlite::insert_node(tx, ino, &entry.attr)?;

        if let Some(target) = &entry.symlink {
            tx.execute(
                "INSERT INTO jfs_symlink (inode, target) VALUES (?1, ?2)",
                params![ino.as_u64() as i64, target.as_bytes()],
            )?;
        }

        for xattr in &entry.xattrs {
            tx.execute(
                "INSERT INTO jfs_xattr (inode, name, value) VALUES (?1, ?2, ?3)",
                params![ino.as_u64() as i64, &xattr.name, &xattr.value],
            )?;
        }

        for chunk in &entry.chunks {
            let mut blob = Vec::with_capacity(chunk.slices.len() * juicefs_core::SLICE_BYTES);
            for rec in &chunk.slices {
                blob.extend_from_slice(&encode_slice(*rec));
                if rec.id > 0 {
                    tx.execute(
                        "INSERT INTO jfs_slice_ref (id, size, refs) VALUES (?1, ?2, 1)
                         ON CONFLICT(id) DO UPDATE SET refs = refs + 1",
                        params![rec.id as i64, i64::from(rec.size)],
                    )?;
                }
            }
            tx.execute(
                "INSERT INTO jfs_chunk (inode, indx, slices) VALUES (?1, ?2, ?3)",
                params![ino.as_u64() as i64, i64::from(chunk.indx), blob],
            )?;
        }
    }

    for (name, child) in &entry.entries {
        tx.execute(
            "INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?1, ?2, ?3, ?4)",
            params![
                ino.as_u64() as i64,
                name.as_bytes(),
                child.inode.as_u64() as i64,
                child.attr.typ as u8,
            ],
        )?;
        load_entry(tx, child, seen_inodes)?;
    }

    Ok(())
}

pub(crate) fn load_from_json(conn: &mut Connection, json: &str) -> Result<(), MetaError> {
    let dumped: DumpedMeta = serde_json::from_str(json)?;
    let format_json = serde_json::to_string(&dumped.format)?;

    let tx = conn.transaction()?;
    tx.execute_batch(
        "DELETE FROM jfs_setting;
         DELETE FROM jfs_counter;
         DELETE FROM jfs_edge;
         DELETE FROM jfs_node;
         DELETE FROM jfs_chunk;
         DELETE FROM jfs_slice_ref;
         DELETE FROM jfs_symlink;
         DELETE FROM jfs_xattr;
         DELETE FROM jfs_dir_quota;
         DELETE FROM jfs_flock;
         DELETE FROM jfs_plock;",
    )?;

    tx.execute(
        "INSERT INTO jfs_setting (name, value) VALUES ('format', ?1)",
        params![format_json],
    )?;

    for (name, value) in &dumped.counters {
        tx.execute(
            "INSERT INTO jfs_counter (name, value) VALUES (?1, ?2)",
            params![name, value],
        )?;
    }

    let mut seen_inodes = HashSet::new();
    load_entry(&tx, &dumped.tree, &mut seen_inodes)?;
    if let Some(trash) = &dumped.trash {
        load_entry(&tx, trash, &mut seen_inodes)?;
    }

    for orphan in &dumped.orphan_slices {
        tx.execute(
            "INSERT INTO jfs_slice_ref (id, size, refs) VALUES (?1, ?2, 0)
             ON CONFLICT(id) DO NOTHING",
            params![orphan.id as i64, i64::from(orphan.size)],
        )?;
    }

    for (ino, quota) in &dumped.quotas {
        tx.execute(
            "INSERT INTO jfs_dir_quota (inode, max_space, max_inodes, used_space, used_inodes)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                *ino as i64,
                quota.max_space,
                quota.max_inodes,
                quota.used_space,
                quota.used_inodes,
            ],
        )?;
    }

    tx.commit()?;
    Ok(())
}

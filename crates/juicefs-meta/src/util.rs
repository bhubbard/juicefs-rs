//! Shared pure helper functions across metadata backends.

use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "redis")]
use juicefs_core::NodeType;
use juicefs_core::{Ino, MAX_NAME};

use crate::{FlockType, MetaError, PlockRecord};

pub(crate) fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}

pub(crate) fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

pub(crate) const fn split_ns(ns: i64) -> (i64, i64) {
    (ns / 1_000, ns % 1_000)
}

pub(crate) const fn align4k(len: u64) -> i64 {
    if len == 0 {
        4096
    } else {
        ((((len - 1) >> 12) + 1) << 12) as i64
    }
}

pub(crate) fn validate_name(name: &str) -> Result<(), MetaError> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.len() > MAX_NAME
    {
        return Err(MetaError::InvalidInput(format!(
            "invalid entry name: {name:?}"
        )));
    }
    Ok(())
}

pub(crate) fn make_trash_entry_name(parent: Ino, inode: Ino, name: &str) -> String {
    let full = format!("{}-{}-{name}", parent.as_u64(), inode.as_u64());
    if full.len() <= MAX_NAME {
        return full;
    }
    let mut end = MAX_NAME;
    while !full.is_char_boundary(end) {
        end -= 1;
    }
    full[..end].to_owned()
}

pub(crate) fn parse_trash_entry_name(trash_name: &str) -> Result<(Ino, Ino, &str), MetaError> {
    let mut parts = trash_name.splitn(3, '-');
    let parent_str = parts
        .next()
        .ok_or_else(|| MetaError::InvalidInput(format!("invalid trash entry: {trash_name}")))?;
    let ino_str = parts
        .next()
        .ok_or_else(|| MetaError::InvalidInput(format!("invalid trash entry: {trash_name}")))?;
    let orig_name = parts
        .next()
        .ok_or_else(|| MetaError::InvalidInput(format!("invalid trash entry: {trash_name}")))?;
    let orig_parent: u64 = parent_str
        .parse()
        .map_err(|_| MetaError::InvalidInput(format!("invalid trash parent in: {trash_name}")))?;
    let orig_ino: u64 = ino_str
        .parse()
        .map_err(|_| MetaError::InvalidInput(format!("invalid trash inode in: {trash_name}")))?;
    validate_name(orig_name)?;
    Ok((Ino(orig_parent), Ino(orig_ino), orig_name))
}

pub(crate) fn encode_plocks(records: &[PlockRecord]) -> Vec<u8> {
    let mut out = Vec::with_capacity(records.len() * 24);
    for rec in records {
        out.extend_from_slice(&u32::from(rec.typ as u8).to_be_bytes());
        out.extend_from_slice(&rec.pid.to_be_bytes());
        out.extend_from_slice(&rec.start.to_be_bytes());
        out.extend_from_slice(&rec.end.to_be_bytes());
    }
    out
}

pub(crate) fn decode_plocks(bytes: &[u8]) -> Result<Vec<PlockRecord>, MetaError> {
    let (chunks, rem) = bytes.as_chunks::<24>();
    if !rem.is_empty() {
        return Err(MetaError::InvalidInput(format!(
            "corrupt plock record length: {}",
            bytes.len()
        )));
    }
    let mut out = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let typ_raw = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        let typ = FlockType::try_from(
            u8::try_from(typ_raw)
                .map_err(|_| MetaError::InvalidInput(format!("invalid plock type: {typ_raw}")))?,
        )?;
        let pid = u32::from_be_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        let start = u64::from_be_bytes([
            chunk[8], chunk[9], chunk[10], chunk[11], chunk[12], chunk[13], chunk[14], chunk[15],
        ]);
        let end = u64::from_be_bytes([
            chunk[16], chunk[17], chunk[18], chunk[19], chunk[20], chunk[21], chunk[22], chunk[23],
        ]);
        out.push(PlockRecord {
            typ,
            pid,
            start,
            end,
        });
    }
    Ok(out)
}

pub(crate) fn update_plocks(mut ls: Vec<PlockRecord>, nl: PlockRecord) -> Vec<PlockRecord> {
    if nl.start > nl.end {
        return ls;
    }
    let mut rem = Some(nl);
    let orig_len = ls.len();
    for i in 0..orig_len {
        let Some(mut cur_nl) = rem else {
            break;
        };
        let mut l = ls[i];
        if cur_nl.start < l.start && cur_nl.end >= l.start {
            let mut prefix = cur_nl;
            prefix.end = l.start - 1;
            ls.push(prefix);
            cur_nl.start = l.start;
        }
        if cur_nl.start > l.start && cur_nl.start <= l.end {
            let mut before = l;
            before.end = cur_nl.start - 1;
            ls.push(before);
            ls[i].start = cur_nl.start;
            l.start = cur_nl.start;
        }
        if cur_nl.start == l.start {
            ls[i].typ = cur_nl.typ;
            ls[i].pid = cur_nl.pid;
            if l.end > cur_nl.end {
                let mut after = l;
                after.start = cur_nl.end + 1;
                ls[i].end = cur_nl.end;
                ls.push(after);
            }
            rem = ls[i]
                .end
                .checked_add(1)
                .filter(|&s| s <= cur_nl.end)
                .map(|s| {
                    cur_nl.start = s;
                    cur_nl
                });
        } else {
            rem = Some(cur_nl);
        }
    }
    if let Some(tail) = rem
        && tail.start <= tail.end
    {
        ls.push(tail);
    }

    ls.sort_by_key(|r| r.start);
    ls.retain(|r| r.typ != FlockType::Unlock && r.start <= r.end);

    let mut merged: Vec<PlockRecord> = Vec::with_capacity(ls.len());
    for rec in ls {
        if let Some(last) = merged.last_mut()
            && last.typ == rec.typ
            && last.pid == rec.pid
            && last.end.checked_add(1) == Some(rec.start)
        {
            last.end = rec.end;
        } else {
            merged.push(rec);
        }
    }
    merged
}

pub(crate) fn encode_session_info(mount_point: &str) -> Result<Vec<u8>, MetaError> {
    Ok(serde_json::to_vec(&serde_json::json!({
        "mount_point": mount_point,
    }))?)
}

pub(crate) fn decode_session_mount_point(info: &[u8]) -> String {
    if info.is_empty() {
        return String::new();
    }
    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(info)
        && let Some(mp) = val.get("mount_point").and_then(|v| v.as_str())
    {
        return mp.to_owned();
    }
    String::from_utf8_lossy(info).into_owned()
}

#[cfg(feature = "redis")]
pub(crate) fn encode_dir_entry(inode: Ino, typ: NodeType) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(typ as u8);
    out.extend_from_slice(&inode.as_u64().to_be_bytes());
    out
}

#[cfg(feature = "redis")]
pub(crate) fn decode_dir_entry(bytes: &[u8]) -> Result<(Ino, NodeType), MetaError> {
    if bytes.len() != 9 {
        return Err(MetaError::InvalidInput(format!(
            "corrupt directory entry length: {}",
            bytes.len()
        )));
    }
    let typ = NodeType::try_from(bytes[0])?;
    let ino = u64::from_be_bytes([
        bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
    ]);
    Ok((Ino(ino), typ))
}

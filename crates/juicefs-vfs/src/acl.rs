//! POSIX access control mode checks and extended ACL encoding/evaluation.

use bytes::{Buf, BufMut, BytesMut};
use juicefs_core::Attr;

use crate::error::VfsError;

/// Sentinel permission value indicating an unset ACL tag (`0xFFFF`).
pub const ACL_UNSET: u16 = 0xFFFF;

/// Extended attribute name for POSIX access ACLs.
pub const POSIX_ACL_ACCESS_XATTR: &str = "system.posix_acl_access";

/// Extended attribute name for POSIX default directory ACLs.
pub const POSIX_ACL_DEFAULT_XATTR: &str = "system.posix_acl_default";

const ACL_HEADER_BYTES: usize = 16;
const ACL_ENTRY_BYTES: usize = 6;

/// POSIX access permission bitflags (`R_OK = 4`, `W_OK = 2`, `X_OK = 1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AccessMode;

impl AccessMode {
    /// Existence check (`F_OK`).
    pub const EXISTS: u8 = 0;
    /// Read permission bit (`R_OK = 4`).
    pub const READ: u8 = 4;
    /// Write permission bit (`W_OK = 2`).
    pub const WRITE: u8 = 2;
    /// Execute or directory search permission bit (`X_OK = 1`).
    pub const EXEC: u8 = 1;
}

/// A single named-user or named-group ACL entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AclEntry {
    /// User ID or group ID.
    pub id: u32,
    /// Permission bits (`READ = 4`, `WRITE = 2`, `EXEC = 1`).
    pub perm: u16,
}

/// POSIX ACL rule representation modeled after `JuiceFS` `pkg/acl.Rule`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessControlList {
    /// Owner permission bits (or [`ACL_UNSET`]).
    pub owner: u16,
    /// Owning group permission bits (or [`ACL_UNSET`]).
    pub group: u16,
    /// ACL mask applied to named users, owning group, and named groups (or [`ACL_UNSET`]).
    pub mask: u16,
    /// Other permission bits (or [`ACL_UNSET`]).
    pub other: u16,
    /// Named user ACL entries.
    pub named_users: Vec<AclEntry>,
    /// Named group ACL entries.
    pub named_groups: Vec<AclEntry>,
}

impl Default for AccessControlList {
    fn default() -> Self {
        Self::empty()
    }
}

impl AccessControlList {
    /// Creates an empty [`AccessControlList`] with all base entries unset (`0xFFFF`).
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            owner: ACL_UNSET,
            group: ACL_UNSET,
            mask: ACL_UNSET,
            other: ACL_UNSET,
            named_users: Vec::new(),
            named_groups: Vec::new(),
        }
    }

    /// Creates a minimal [`AccessControlList`] initialized from POSIX `mode` bits.
    #[must_use]
    pub const fn from_mode(mode: u16) -> Self {
        Self {
            owner: (mode >> 6) & 0o7,
            group: (mode >> 3) & 0o7,
            mask: ACL_UNSET,
            other: mode & 0o7,
            named_users: Vec::new(),
            named_groups: Vec::new(),
        }
    }

    /// Returns `true` if no entries or base permissions are set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.named_users.is_empty()
            && self.named_groups.is_empty()
            && (self.owner & self.group & self.other & self.mask) == ACL_UNSET
    }

    /// Returns `true` if this ACL is equivalent to standard POSIX mode bits.
    #[must_use]
    pub fn is_minimal(&self) -> bool {
        self.named_users.is_empty() && self.named_groups.is_empty() && self.mask == ACL_UNSET
    }

    /// Computes the effective POSIX permission bits (`0o777`) represented by this ACL.
    #[must_use]
    pub fn mode(&self) -> u16 {
        let group_bits = if self.is_minimal() {
            self.group
        } else {
            self.mask
        };
        ((self.owner & 0o7) << 6) | ((group_bits & 0o7) << 3) | (self.other & 0o7)
    }

    /// Updates base ACL permissions from POSIX `mode` bits.
    pub fn set_mode(&mut self, mode: u16) {
        self.owner = (self.owner & 0xFFF8) | ((mode >> 6) & 0o7);
        if self.is_minimal() {
            self.group = (self.group & 0xFFF8) | ((mode >> 3) & 0o7);
        } else {
            self.mask = (self.mask & 0xFFF8) | ((mode >> 3) & 0o7);
        }
        self.other = (self.other & 0xFFF8) | (mode & 0o7);
    }

    /// Fills any unset base permission fields from `mode`.
    pub const fn inherit_perms(&mut self, mode: u16) {
        if self.owner == ACL_UNSET {
            self.owner = (mode >> 6) & 0o7;
        }
        if self.group == ACL_UNSET {
            self.group = (mode >> 3) & 0o7;
        }
        if self.other == ACL_UNSET {
            self.other = mode & 0o7;
        }
    }

    /// Derives a child node's access ACL from a parent's default ACL and creation `mode`.
    #[must_use]
    pub fn child_access_acl(&self, mode: u16) -> Self {
        Self {
            owner: ((mode >> 6) & 0o7) & self.owner,
            group: self.group,
            mask: ((mode >> 3) & 0o7) & self.mask,
            other: (mode & 0o7) & self.other,
            named_users: self.named_users.clone(),
            named_groups: self.named_groups.clone(),
        }
    }

    /// Serializes this ACL rule into the big-endian `JuiceFS` binary format.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let cap =
            ACL_HEADER_BYTES + (self.named_users.len() + self.named_groups.len()) * ACL_ENTRY_BYTES;
        let mut buf = BytesMut::with_capacity(cap);
        buf.put_u16(self.owner);
        buf.put_u16(self.group);
        buf.put_u16(self.mask);
        buf.put_u16(self.other);
        buf.put_u32(self.named_users.len() as u32);
        for entry in &self.named_users {
            buf.put_u32(entry.id);
            buf.put_u16(entry.perm);
        }
        buf.put_u32(self.named_groups.len() as u32);
        for entry in &self.named_groups {
            buf.put_u32(entry.id);
            buf.put_u16(entry.perm);
        }
        buf.to_vec()
    }

    /// Decodes an [`AccessControlList`] from the big-endian `JuiceFS` binary format.
    pub fn decode(buf: &[u8]) -> Result<Self, VfsError> {
        if buf.len() < ACL_HEADER_BYTES {
            return Err(VfsError::InvalidAcl(format!(
                "buffer too short: expected at least {ACL_HEADER_BYTES} bytes, got {}",
                buf.len()
            )));
        }
        let mut cursor = buf;
        let owner = cursor.get_u16();
        let group = cursor.get_u16();
        let mask = cursor.get_u16();
        let other = cursor.get_u16();
        let user_count = cursor.get_u32() as usize;

        let expected_after_users = user_count
            .checked_mul(ACL_ENTRY_BYTES)
            .and_then(|n| n.checked_add(4))
            .ok_or_else(|| VfsError::InvalidAcl("named user count overflow".to_owned()))?;
        if cursor.remaining() < expected_after_users {
            return Err(VfsError::InvalidAcl(format!(
                "truncated named user entries: need {expected_after_users} bytes, got {}",
                cursor.remaining()
            )));
        }

        let mut named_users = Vec::with_capacity(user_count);
        for _ in 0..user_count {
            let id = cursor.get_u32();
            let perm = cursor.get_u16();
            named_users.push(AclEntry { id, perm });
        }

        let group_count = cursor.get_u32() as usize;
        let expected_groups = group_count
            .checked_mul(ACL_ENTRY_BYTES)
            .ok_or_else(|| VfsError::InvalidAcl("named group count overflow".to_owned()))?;
        if cursor.remaining() != expected_groups {
            return Err(VfsError::InvalidAcl(format!(
                "invalid named group payload size: expected {expected_groups} bytes, got {}",
                cursor.remaining()
            )));
        }

        let mut named_groups = Vec::with_capacity(group_count);
        for _ in 0..group_count {
            let id = cursor.get_u32();
            let perm = cursor.get_u16();
            named_groups.push(AclEntry { id, perm });
        }

        Ok(Self {
            owner,
            group,
            mask,
            other,
            named_users,
            named_groups,
        })
    }

    /// Evaluates whether `(uid, gids)` satisfies `req_mask` against a node owned by `(file_owner, owning_group)`.
    #[must_use]
    pub fn can_access(
        &self,
        uid: u32,
        gids: &[u32],
        file_owner: u32,
        owning_group: u32,
        req_mask: u8,
    ) -> bool {
        let req = req_mask & 0o7;
        if uid == file_owner {
            return ((self.owner & 0o7) as u8 & req) == req;
        }
        for entry in &self.named_users {
            if uid == entry.id {
                return (((entry.perm & self.mask & 0o7) as u8) & req) == req;
            }
        }

        let mut group_matched = false;
        for &gid in gids {
            if gid == owning_group {
                if (((self.group & self.mask & 0o7) as u8) & req) == req {
                    return true;
                }
                group_matched = true;
            }
        }
        for &gid in gids {
            for entry in &self.named_groups {
                if gid == entry.id {
                    if (((entry.perm & self.mask & 0o7) as u8) & req) == req {
                        return true;
                    }
                    group_matched = true;
                }
            }
        }
        if group_matched {
            return false;
        }

        ((self.other & 0o7) as u8 & req) == req
    }
}

/// Checks POSIX `owner/group/other` mode bits on `attr` for `(uid, gid)`.
///
/// Root (`uid == 0`) bypasses read and write checks. For execute checks on non-directories,
/// at least one execute bit (`0o111`) must be set.
#[must_use]
pub const fn check_posix_mode(attr: &Attr, uid: u32, gid: u32, mask: u8) -> bool {
    let req = mask & 0o7;
    if uid == 0 {
        if (req & AccessMode::EXEC) != 0 && !attr.is_dir() && (attr.mode & 0o111) == 0 {
            return false;
        }
        return true;
    }
    let effective = if uid == attr.uid {
        ((attr.mode >> 6) & 0o7) as u8
    } else if gid == attr.gid {
        ((attr.mode >> 3) & 0o7) as u8
    } else {
        (attr.mode & 0o7) as u8
    };
    (effective & req) == req
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acl_encode_decode_roundtrip() {
        let acl = AccessControlList {
            owner: 0o7,
            group: 0o5,
            mask: 0o6,
            other: 0o1,
            named_users: vec![
                AclEntry { id: 1001, perm: 6 },
                AclEntry { id: 1002, perm: 4 },
            ],
            named_groups: vec![AclEntry { id: 2001, perm: 7 }],
        };
        let encoded = acl.encode();
        let decoded = AccessControlList::decode(&encoded).expect("decode");
        assert_eq!(decoded, acl);
    }

    #[test]
    fn acl_decode_rejects_truncated() {
        assert!(AccessControlList::decode(&[0u8; 4]).is_err());
        let mut bad = AccessControlList::empty().encode();
        bad.pop();
        assert!(AccessControlList::decode(&bad).is_err());
    }
}

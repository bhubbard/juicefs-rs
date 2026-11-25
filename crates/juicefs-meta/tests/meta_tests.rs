//! Ported tests from `pkg/meta/base_test.go`, `pkg/meta/dump_test.go`, and `pkg/meta/interface_test.go`.

use juicefs_core::{
    CHUNK_SIZE, Format, Ino, NodeType, ROOT_INODE, SetAttrFlags, Slice, TRASH_INODE,
};
#[cfg(feature = "redis")]
use juicefs_meta::RedisMeta;
use juicefs_meta::{
    DirStat, FlockType, Meta, MetaError, PlockRecord, Quota, QuotaTarget, SqliteMeta,
    open_any_meta, open_meta,
};
use rusqlite::Connection;
use tempfile::tempdir;

const fn full_slice(id: u64, len: u32) -> Slice {
    Slice {
        id,
        size: len,
        off: 0,
        len,
    }
}

const fn plock(typ: FlockType, pid: u32, start: u64, end: u64) -> PlockRecord {
    PlockRecord {
        typ,
        pid,
        start,
        end,
    }
}

async fn setup_meta() -> SqliteMeta {
    let meta = open_meta("sqlite3://:memory:").expect("open in-memory meta");
    let mut fmt = Format::new("testvol", "mem", "");
    fmt.capacity = 1 << 30;
    fmt.inodes = 1000;
    meta.init(&fmt, false).await.expect("init");
    meta
}

#[tokio::test]
async fn test_init_force_and_not_formatted() {
    let meta = SqliteMeta::open(":memory:").expect("open");
    assert!(matches!(meta.load().await, Err(MetaError::NotFormatted)));

    let fmt = Format::new("v1", "mem", "");
    meta.init(&fmt, false).await.expect("first init");
    assert!(matches!(
        meta.init(&fmt, false).await,
        Err(MetaError::AlreadyExists(_))
    ));

    let fmt2 = Format::new("v2", "mem", "");
    meta.init(&fmt2, true).await.expect("force re-init");
    assert_eq!(meta.load().await.expect("load").name, "v2");
}

#[tokio::test]
async fn test_dir_file_symlink_hardlink_rename() {
    let meta = setup_meta().await;

    // mkdir & lookup (. and ..)
    let (d1, d1_attr) = meta
        .mkdir(ROOT_INODE, "d1", 0o755, 1000, 1000)
        .await
        .expect("mkdir d1");
    assert!(d1_attr.is_dir());
    assert_eq!(d1_attr.nlink, 2);
    let (dot, _) = meta.lookup(d1, ".").await.expect("lookup .");
    assert_eq!(dot, d1);
    let (dotdot, _) = meta.lookup(d1, "..").await.expect("lookup ..");
    assert_eq!(dotdot, ROOT_INODE);

    // mknod & hardlink
    let (f1, _) = meta
        .mknod(d1, "f1", NodeType::File, 0o644, 1000, 1000)
        .await
        .expect("mknod f1");
    let linked = meta.link(f1, ROOT_INODE, "f1_link").await.expect("link");
    assert_eq!(linked.nlink, 2);

    // unlink one link, inode still exists
    meta.unlink(d1, "f1").await.expect("unlink f1");
    assert_eq!(meta.getattr(f1).await.expect("getattr").nlink, 1);

    // symlink & readlink
    let (sym, sym_attr) = meta
        .symlink(d1, "sym", "/f1_link", 1000, 1000)
        .await
        .expect("symlink");
    assert!(sym_attr.is_symlink());
    assert_eq!(meta.readlink(sym).await.expect("readlink"), "/f1_link");

    // rmdir non-empty fails
    assert!(matches!(
        meta.rmdir(ROOT_INODE, "d1").await,
        Err(MetaError::NotEmpty(_))
    ));

    // rename symlink to root, then rmdir d1
    meta.rename(d1, "sym", ROOT_INODE, "sym_moved")
        .await
        .expect("rename");
    meta.rmdir(ROOT_INODE, "d1").await.expect("rmdir d1");

    // setattr
    let mut upd = meta.getattr(f1).await.expect("getattr");
    upd.mode = 0o600;
    upd.uid = 2000;
    let updated = meta
        .setattr(
            f1,
            &upd,
            SetAttrFlags::MODE
                .union(SetAttrFlags::UID)
                .union(SetAttrFlags::MTIME_NOW),
        )
        .await
        .expect("setattr");
    assert_eq!(updated.mode, 0o600);
    assert_eq!(updated.uid, 2000);

    // cleanup
    meta.unlink(ROOT_INODE, "f1_link")
        .await
        .expect("unlink link");
    meta.unlink(ROOT_INODE, "sym_moved")
        .await
        .expect("unlink sym");
}

#[tokio::test]
async fn test_xattrs_and_truncate_slices() {
    let meta = setup_meta().await;
    let (f, _) = meta
        .mknod(ROOT_INODE, "blob", NodeType::File, 0o644, 0, 0)
        .await
        .expect("mknod");

    meta.set_xattr(f, "user.author", b"juicefs-rs")
        .await
        .expect("set_xattr");
    meta.set_xattr(f, "user.env", b"test")
        .await
        .expect("set_xattr 2");
    assert_eq!(
        meta.get_xattr(f, "user.author").await.expect("get_xattr"),
        b"juicefs-rs"
    );
    assert_eq!(
        meta.list_xattr(f).await.expect("list_xattr"),
        vec!["user.author", "user.env"]
    );
    meta.remove_xattr(f, "user.env")
        .await
        .expect("remove_xattr");
    assert!(matches!(
        meta.get_xattr(f, "user.env").await,
        Err(MetaError::NotFound(_))
    ));

    // Write slices in chunk 0 and chunk 1
    let s0 = meta.new_slice().await.expect("s0");
    let s1 = meta.new_slice().await.expect("s1");
    meta.write_chunk(
        f,
        0,
        0,
        Slice {
            id: s0,
            size: 100,
            off: 0,
            len: 100,
        },
        100,
    )
    .await
    .expect("write chunk 0");
    meta.write_chunk(
        f,
        1,
        0,
        Slice {
            id: s1,
            size: 50,
            off: 0,
            len: 50,
        },
        CHUNK_SIZE + 50,
    )
    .await
    .expect("write chunk 1");

    // Truncate back to chunk 0 -> chunk 1 slice becomes orphan
    let attr = meta.truncate(f, 100).await.expect("truncate");
    assert_eq!(attr.length, 100);
    let orphans = meta.list_orphan_slices().await.expect("orphans");
    assert_eq!(orphans, vec![(s1, 50)]);
    meta.purge_orphan_slice(s1).await.expect("purge");

    let st = meta.statfs().await.expect("statfs");
    assert_eq!(st.used_inodes, 2); // root + blob
    assert_eq!(st.total_space - st.avail_space, 100);
}

#[tokio::test]
async fn test_flock_contention_upgrade_downgrade_and_release() {
    let meta = setup_meta().await;
    let (ino, _) = meta
        .mknod(ROOT_INODE, "lockfile", NodeType::File, 0o644, 0, 0)
        .await
        .expect("mknod");

    // Multiple owners can hold Shared locks simultaneously
    meta.flock(ino, 1, FlockType::Shared)
        .await
        .expect("owner 1 shared");
    meta.flock(ino, 2, FlockType::Shared)
        .await
        .expect("owner 2 shared");

    // Upgrade by owner 1 conflicts with owner 2's Shared lock
    assert!(matches!(
        meta.flock(ino, 1, FlockType::Exclusive).await,
        Err(MetaError::WouldBlock(_))
    ));

    // Once owner 2 unlocks, owner 1 can upgrade to Exclusive
    meta.flock(ino, 2, FlockType::Unlock)
        .await
        .expect("owner 2 unlock");
    meta.flock(ino, 1, FlockType::Exclusive)
        .await
        .expect("owner 1 upgrade to exclusive");

    // Both Shared and Exclusive from owner 2 fail while owner 1 holds Exclusive
    assert!(matches!(
        meta.flock(ino, 2, FlockType::Shared).await,
        Err(MetaError::WouldBlock(_))
    ));
    assert!(matches!(
        meta.flock(ino, 2, FlockType::Exclusive).await,
        Err(MetaError::WouldBlock(_))
    ));

    // Downgrade owner 1 to Shared allows owner 2 Shared again
    meta.flock(ino, 1, FlockType::Shared)
        .await
        .expect("owner 1 downgrade");
    meta.flock(ino, 2, FlockType::Shared)
        .await
        .expect("owner 2 shared after downgrade");

    // Unlock both
    meta.flock(ino, 1, FlockType::Unlock)
        .await
        .expect("owner 1 unlock");
    meta.flock(ino, 2, FlockType::Unlock)
        .await
        .expect("owner 2 unlock");
    meta.flock(ino, 2, FlockType::Exclusive)
        .await
        .expect("owner 2 exclusive after all unlocked");
}

#[tokio::test]
async fn test_plock_byte_range_contention_and_get_lk() {
    let meta = setup_meta().await;
    let (ino, _) = meta
        .mknod(ROOT_INODE, "plockfile", NodeType::File, 0o644, 0, 0)
        .await
        .expect("mknod");

    // Shared ranges from different owners can overlap
    meta.set_lk(ino, 1, plock(FlockType::Shared, 100, 0, 0xFFFF))
        .await
        .expect("owner 1 shared");
    meta.set_lk(ino, 2, plock(FlockType::Shared, 200, 0, 0x2_FFFF))
        .await
        .expect("owner 2 shared");

    // Owner 2 Exclusive overlapping owner 1 fails
    assert!(matches!(
        meta.set_lk(ino, 2, plock(FlockType::Exclusive, 200, 0, 0xFFFF))
            .await,
        Err(MetaError::WouldBlock(_))
    ));

    // Non-overlapping Exclusive range succeeds
    meta.set_lk(ino, 2, plock(FlockType::Exclusive, 200, 0x1_0000, 0x2_0000))
        .await
        .expect("owner 2 non-overlapping exclusive");

    let conflict = meta
        .get_lk(ino, 1, plock(FlockType::Exclusive, 100, 0x1_5000, 0x1_8000))
        .await
        .expect("get_lk conflict");
    assert_eq!(
        conflict,
        plock(FlockType::Exclusive, 200, 0x1_0000, 0x2_0000)
    );
}

#[tokio::test]
async fn test_plock_split_merge_and_max_boundary() {
    let meta = setup_meta().await;
    let (ino, _) = meta
        .mknod(ROOT_INODE, "plocksplit", NodeType::File, 0o644, 0, 0)
        .await
        .expect("mknod");

    // Owner 2 acquires [0, 100], then unlocks middle [20, 40] -> splits into [0, 19] and [41, 100]
    meta.set_lk(ino, 2, plock(FlockType::Exclusive, 20, 0, 100))
        .await
        .expect("owner 2 exclusive 0..100");
    meta.set_lk(ino, 2, plock(FlockType::Unlock, 20, 20, 40))
        .await
        .expect("owner 2 unlock 20..40");

    let gap = meta
        .get_lk(ino, 1, plock(FlockType::Exclusive, 1, 20, 40))
        .await
        .expect("get_lk gap");
    assert_eq!(gap.typ, FlockType::Unlock);

    let conflict = meta
        .get_lk(ino, 1, plock(FlockType::Exclusive, 1, 30, 60))
        .await
        .expect("get_lk conflict");
    assert_eq!(conflict, plock(FlockType::Exclusive, 20, 41, 100));

    // Adjacent lock merge and u64::MAX end boundary
    meta.set_lk(ino, 2, plock(FlockType::Exclusive, 20, 20, 40))
        .await
        .expect("re-lock gap");
    let merged = meta
        .get_lk(ino, 1, plock(FlockType::Shared, 1, 5, 5))
        .await
        .expect("get_lk merged");
    assert_eq!(merged, plock(FlockType::Exclusive, 20, 0, 100));

    meta.set_lk(
        ino,
        2,
        plock(FlockType::Exclusive, 20, u64::MAX - 10, u64::MAX),
    )
    .await
    .expect("u64::MAX lock");
    meta.set_lk(ino, 2, plock(FlockType::Unlock, 20, 0, u64::MAX))
        .await
        .expect("unlock all to u64::MAX");

    let clear = meta
        .get_lk(ino, 1, plock(FlockType::Exclusive, 1, 0, u64::MAX))
        .await
        .expect("get_lk clear");
    assert_eq!(clear.typ, FlockType::Unlock);
}

#[tokio::test]
async fn test_directory_quota_crud_and_enforcement() {
    let meta = setup_meta().await;
    let (qdir, _) = meta
        .mkdir(ROOT_INODE, "qdir", 0o755, 0, 0)
        .await
        .expect("mkdir qdir");
    let (sub, _) = meta
        .mkdir(qdir, "sub", 0o755, 0, 0)
        .await
        .expect("mkdir sub");
    let (pre_file, _) = meta
        .mknod(sub, "pre.bin", NodeType::File, 0o644, 0, 0)
        .await
        .expect("mknod pre");
    let sid = meta.new_slice().await.expect("new_slice");
    meta.write_chunk(
        pre_file,
        0,
        0,
        Slice {
            id: sid,
            size: 40,
            off: 0,
            len: 40,
        },
        40,
    )
    .await
    .expect("write pre");

    // Non-dir cannot have quota
    assert!(matches!(
        meta.set_quota(pre_file, 100, 10).await,
        Err(MetaError::NotDir(_))
    ));
    assert!(matches!(
        meta.get_quota(qdir).await,
        Err(MetaError::NotFound(_))
    ));

    // Set quota on qdir -> captures existing usage (sub + pre.bin = 2 inodes, 40 bytes)
    let created = meta.set_quota(qdir, 100, 3).await.expect("set_quota");
    assert_eq!(
        created,
        Quota {
            max_space: 100,
            max_inodes: 3,
            used_space: 40,
            used_inodes: 2,
        }
    );

    // Nested quota on sub
    let sub_q = meta.set_quota(sub, 80, 2).await.expect("set_quota sub");
    assert_eq!(sub_q.used_space, 40);
    assert_eq!(sub_q.used_inodes, 1);

    let list = meta.list_quotas().await.expect("list_quotas");
    assert_eq!(list.len(), 2);

    // Adding 1 more file in sub succeeds (qdir: 3/3 inodes, sub: 2/2 inodes)
    let (f2, _) = meta
        .mknod(sub, "f2.bin", NodeType::File, 0o644, 0, 0)
        .await
        .expect("mknod f2");

    // Exceeding inode quota fails
    assert!(matches!(
        meta.mknod(sub, "f3.bin", NodeType::File, 0o644, 0, 0).await,
        Err(MetaError::QuotaExceeded(_))
    ));

    // Writing 30 bytes succeeds (40 + 30 = 70 <= 80), writing 20 more exceeds sub's 80-byte quota
    let sid2 = meta.new_slice().await.expect("sid2");
    meta.write_chunk(
        f2,
        0,
        0,
        Slice {
            id: sid2,
            size: 30,
            off: 0,
            len: 30,
        },
        30,
    )
    .await
    .expect("write 30");
    let sid3 = meta.new_slice().await.expect("sid3");
    assert!(matches!(
        meta.write_chunk(
            f2,
            0,
            30,
            Slice {
                id: sid3,
                size: 20,
                off: 0,
                len: 20,
            },
            50,
        )
        .await,
        Err(MetaError::QuotaExceeded(_))
    ));

    // Unlink f2 decrements used_space and used_inodes
    meta.unlink(sub, "f2.bin").await.expect("unlink f2");
    let q_after = meta.get_quota(qdir).await.expect("get_quota");
    assert_eq!(q_after.used_space, 40);
    assert_eq!(q_after.used_inodes, 2);

    // Delete quota
    meta.del_quota(sub).await.expect("del_quota sub");
    meta.del_quota(qdir).await.expect("del_quota qdir");
    assert_eq!(meta.list_quotas().await.expect("list empty"), vec![]);
}

#[tokio::test]
async fn test_trash_move_restore_and_empty() {
    let meta = setup_meta().await;
    let (trash_ino, trash_attr) = meta
        .lookup(ROOT_INODE, ".trash")
        .await
        .expect("lookup .trash");
    assert_eq!(trash_ino, TRASH_INODE);
    assert!(trash_attr.is_dir());

    let (dir, _) = meta
        .mkdir(ROOT_INODE, "work", 0o755, 0, 0)
        .await
        .expect("mkdir work");
    let (file, _) = meta
        .mknod(dir, "report.txt", NodeType::File, 0o644, 0, 0)
        .await
        .expect("mknod report");
    let sid = meta.new_slice().await.expect("new_slice");
    meta.write_chunk(
        file,
        0,
        0,
        Slice {
            id: sid,
            size: 64,
            off: 0,
            len: 64,
        },
        64,
    )
    .await
    .expect("write chunk");

    // Move non-empty directory into trash
    let trash_name = meta
        .move_to_trash(ROOT_INODE, "work")
        .await
        .expect("move_to_trash");
    assert!(matches!(
        meta.lookup(ROOT_INODE, "work").await,
        Err(MetaError::NotFound(_))
    ));
    let trash_entries = meta.readdir(TRASH_INODE).await.expect("readdir trash");
    assert_eq!(trash_entries.len(), 1);
    assert_eq!(trash_entries[0].name, trash_name);

    // Restore from trash
    let (restored_ino, restored_attr) = meta
        .restore_from_trash(&trash_name)
        .await
        .expect("restore_from_trash");
    assert_eq!(restored_ino, dir);
    assert!(restored_attr.is_dir());
    let (found_file, _) = meta.lookup(dir, "report.txt").await.expect("lookup child");
    assert_eq!(found_file, file);

    // Move to trash again and empty trash -> permanently deletes and orphans slice
    let _ = meta
        .move_to_trash(ROOT_INODE, "work")
        .await
        .expect("move_to_trash 2");
    let purged = meta.empty_trash().await.expect("empty_trash");
    assert_eq!(purged, 2); // work + report.txt
    assert_eq!(meta.readdir(TRASH_INODE).await.expect("empty"), vec![]);
    assert_eq!(
        meta.list_orphan_slices().await.expect("orphans"),
        vec![(sid, 64)]
    );

    // When trash_days == 0, move_to_trash returns InvalidInput
    let mut no_trash_fmt = Format::new("notrash", "mem", "");
    no_trash_fmt.trash_days = 0;
    let no_trash = SqliteMeta::open(":memory:").expect("open");
    no_trash.init(&no_trash_fmt, false).await.expect("init");
    no_trash
        .mknod(ROOT_INODE, "tmp", NodeType::File, 0o644, 0, 0)
        .await
        .expect("mknod");
    assert!(matches!(
        no_trash.move_to_trash(ROOT_INODE, "tmp").await,
        Err(MetaError::InvalidInput(_))
    ));
}

async fn create_written_file(meta: &SqliteMeta, parent: Ino, name: &str, len: u32) -> (Ino, u64) {
    let (ino, _) = meta
        .mknod(parent, name, NodeType::File, 0o644, 1000, 1000)
        .await
        .expect("mknod");
    let sid = meta.new_slice().await.expect("new_slice");
    meta.write_chunk(ino, 0, 0, full_slice(sid, len), u64::from(len))
        .await
        .expect("write_chunk");
    (ino, sid)
}

#[tokio::test]
async fn test_dump_json_load_json_roundtrip() {
    let src = setup_meta().await;
    let (dir, _) = src
        .mkdir(ROOT_INODE, "docs", 0o755, 1000, 1000)
        .await
        .expect("mkdir");
    src.set_quota(dir, 10_000, 100).await.expect("set_quota");
    let (f1, _) = create_written_file(&src, dir, "a.bin", 128).await;
    src.set_xattr(f1, "user.k", b"\x00\xffv")
        .await
        .expect("set_xattr");
    src.link(f1, ROOT_INODE, "a_hardlink")
        .await
        .expect("hardlink");
    src.symlink(ROOT_INODE, "a_sym", "/docs/a.bin", 1000, 1000)
        .await
        .expect("symlink");

    // Put one file into trash and create one orphan slice
    create_written_file(&src, ROOT_INODE, "deleted.txt", 16).await;
    let trash_name = src
        .move_to_trash(ROOT_INODE, "deleted.txt")
        .await
        .expect("move_to_trash");

    let (_, s_orphan) = create_written_file(&src, ROOT_INODE, "orphan_src", 32).await;
    src.unlink(ROOT_INODE, "orphan_src")
        .await
        .expect("unlink orphan");

    let json = src.dump_json().await.expect("dump_json");
    let dst = SqliteMeta::open(":memory:").expect("open dst");
    dst.load_json(&json).await.expect("load_json");

    assert_eq!(
        dst.load().await.expect("load").name,
        src.load().await.expect("src load").name
    );
    assert_eq!(
        dst.statfs().await.expect("dst statfs"),
        src.statfs().await.expect("src statfs")
    );
    assert_eq!(
        dst.read_chunk(f1, 0).await.expect("dst read_chunk"),
        src.read_chunk(f1, 0).await.expect("src read_chunk")
    );
    assert_eq!(
        dst.get_xattr(f1, "user.k").await.expect("dst xattr"),
        b"\x00\xffv"
    );
    assert_eq!(dst.getattr(f1).await.expect("dst hardlink").nlink, 2);
    let (sym_ino, _) = dst.lookup(ROOT_INODE, "a_sym").await.expect("sym lookup");
    assert_eq!(
        dst.readlink(sym_ino).await.expect("readlink"),
        "/docs/a.bin"
    );
    assert_eq!(
        dst.get_quota(dir).await.expect("dst quota"),
        src.get_quota(dir).await.expect("src quota")
    );
    assert_eq!(
        dst.readdir(TRASH_INODE).await.expect("dst trash")[0].name,
        trash_name
    );
    assert_eq!(
        dst.list_orphan_slices().await.expect("dst orphans"),
        vec![(s_orphan, 32)]
    );
    assert!(dst.fsck().await.expect("dst fsck").is_clean());
}

#[tokio::test]
async fn test_fsck_clean_and_corrupted_state() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("fsck.db");
    let meta = SqliteMeta::open(&db_path).expect("open file sqlite");
    let fmt = Format::new("fsckvol", "mem", "");
    meta.init(&fmt, false).await.expect("init");

    let (d1, _) = meta
        .mkdir(ROOT_INODE, "d1", 0o755, 0, 0)
        .await
        .expect("mkdir d1");
    let (f1, _) = meta
        .mknod(d1, "f1", NodeType::File, 0o644, 0, 0)
        .await
        .expect("mknod f1");
    meta.link(f1, ROOT_INODE, "f1_hard")
        .await
        .expect("hardlink");
    let (_f2, _) = meta
        .mknod(d1, "trashed", NodeType::File, 0o644, 0, 0)
        .await
        .expect("mknod trashed");
    meta.move_to_trash(d1, "trashed")
        .await
        .expect("move_to_trash");

    let clean_report = meta.fsck().await.expect("clean fsck");
    assert!(
        clean_report.is_clean(),
        "expected clean fsck, got {clean_report:?}"
    );

    // Inject corruptions directly via raw SQLite connection
    let raw = Connection::open(&db_path).expect("open raw");
    // 1. Corrupt nlink on f1 (2 -> 9)
    raw.execute(
        "UPDATE jfs_node SET nlink = 9 WHERE inode = ?1",
        rusqlite::params![f1.as_u64() as i64],
    )
    .expect("corrupt nlink");
    // 2. Orphaned edge whose parent (9999) does not exist
    raw.execute(
        "INSERT INTO jfs_edge (parent, name, inode, type) VALUES (9999, X'67686F7374', ?1, 1)",
        rusqlite::params![f1.as_u64() as i64],
    )
    .expect("insert orphan edge");
    // 3. Edge pointing to missing node (8888)
    raw.execute(
        "INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?1, X'6D697373', 8888, 1)",
        rusqlite::params![d1.as_u64() as i64],
    )
    .expect("insert missing node edge");
    // 4. Unreferenced node (7777) with no incoming edge
    raw.execute(
        "INSERT INTO jfs_node (inode, type, flags, mode, uid, gid, atime, mtime, ctime, nlink, length)
         VALUES (7777, 1, 0, 420, 0, 0, 0, 0, 0, 1, 0)",
        [],
    )
    .expect("insert unreferenced node");
    drop(raw);

    let bad = meta.fsck().await.expect("corrupted fsck");
    assert!(!bad.is_clean());
    assert_eq!(bad.orphaned_edges, vec!["9999:ghost"]);
    assert_eq!(bad.missing_nodes, vec![8888]);
    assert_eq!(bad.unreferenced_nodes, vec![7777]);
    assert!(
        bad.nlink_mismatches
            .iter()
            .any(|m| m.inode == f1 && m.stored_nlink == 9 && m.expected_nlink == 3),
        "expected f1 nlink mismatch in {:?}",
        bad.nlink_mismatches
    );
}

#[tokio::test]
async fn test_compact_chunk_atomic_with_concurrent_append() {
    let meta = setup_meta().await;
    let (f, _) = meta
        .mknod(ROOT_INODE, "compact.bin", NodeType::File, 0o644, 0, 0)
        .await
        .expect("mknod");

    let s1 = meta.new_slice().await.expect("s1");
    let s2 = meta.new_slice().await.expect("s2");
    let s3 = meta.new_slice().await.expect("s3");
    let s4 = meta.new_slice().await.expect("s4");

    // Initial 3 overlapping writes + concurrent 4th write appended before compaction commits
    for (pos, sid, len) in [
        (100, s1, 100),
        (150, s2, 100),
        (200, s3, 100),
        (300, s4, 50),
    ] {
        meta.write_chunk(f, 0, pos, full_slice(sid, len), u64::from(pos + len))
            .await
            .expect("write_chunk");
    }

    // Invalid old_count checks
    let s_comp = meta.new_slice().await.expect("s_comp");
    let compacted_slice = full_slice(s_comp, 200);
    assert!(matches!(
        meta.compact_chunk(f, 0, 0, compacted_slice).await,
        Err(MetaError::InvalidInput(_))
    ));
    assert!(matches!(
        meta.compact_chunk(f, 0, 10, compacted_slice).await,
        Err(MetaError::InvalidInput(_))
    ));

    // Atomically compact the first 3 slices while preserving s4
    meta.compact_chunk(f, 0, 3, compacted_slice)
        .await
        .expect("compact_chunk");

    let resolved = meta.read_chunk(f, 0).await.expect("read_chunk");
    assert_eq!(
        resolved,
        vec![full_slice(0, 100), compacted_slice, full_slice(s4, 50)]
    );

    // s1, s2, s3 should now be orphans; s_comp and s4 remain referenced
    let mut orphans = meta.list_orphan_slices().await.expect("orphans");
    orphans.sort_by_key(|&(id, _)| id);
    assert_eq!(orphans, vec![(s1, 100), (s2, 100), (s3, 100)]);
}

const fn dir_stat(data_length: i64, used_space: i64, used_inodes: i64) -> DirStat {
    DirStat {
        data_length,
        used_space,
        used_inodes,
    }
}

#[tokio::test]
async fn test_dir_stats_accumulation_and_decrement() {
    let meta = open_meta("sqlite3://:memory:").expect("open");
    let mut fmt = Format::new("dirstatvol", "mem", "");
    fmt.dir_stats = true;
    meta.init(&fmt, false).await.expect("init");

    assert_eq!(
        meta.get_dir_stat(ROOT_INODE).await.expect("root"),
        dir_stat(0, 0, 0)
    );
    assert!(matches!(
        meta.get_dir_stat(Ino(999_999)).await,
        Err(MetaError::NotFound(_))
    ));

    let (parent, _) = meta
        .mkdir(ROOT_INODE, "parent", 0o755, 0, 0)
        .await
        .expect("mkdir parent");
    let (child, _) = meta
        .mkdir(parent, "child", 0o755, 0, 0)
        .await
        .expect("mkdir child");
    assert_eq!(
        meta.get_dir_stat(parent).await.expect("p1"),
        dir_stat(0, 4096, 1)
    );
    assert_eq!(
        meta.get_dir_stat(ROOT_INODE).await.expect("r1"),
        dir_stat(0, 8192, 2)
    );

    let (file, _) = meta
        .mknod(child, "data.bin", NodeType::File, 0o644, 0, 0)
        .await
        .expect("mknod");
    assert!(matches!(
        meta.get_dir_stat(file).await,
        Err(MetaError::NotDir(_))
    ));
    assert_eq!(
        meta.get_dir_stat(child).await.expect("c1"),
        dir_stat(0, 4096, 1)
    );

    let sid = meta.new_slice().await.expect("new_slice");
    meta.write_chunk(file, 0, 0, full_slice(sid, 5000), 5000)
        .await
        .expect("write_chunk");
    assert_eq!(
        meta.get_dir_stat(child).await.expect("c2"),
        dir_stat(5000, 8192, 1)
    );
    assert_eq!(
        meta.get_dir_stat(parent).await.expect("p2"),
        dir_stat(5000, 12288, 2)
    );
    assert_eq!(
        meta.get_dir_stat(ROOT_INODE).await.expect("r2"),
        dir_stat(5000, 16384, 3)
    );

    meta.truncate(file, 100).await.expect("truncate");
    assert_eq!(
        meta.get_dir_stat(child).await.expect("c3"),
        dir_stat(100, 4096, 1)
    );

    meta.unlink(child, "data.bin").await.expect("unlink");
    assert_eq!(
        meta.get_dir_stat(child).await.expect("c4"),
        dir_stat(0, 0, 0)
    );
    assert_eq!(
        meta.get_dir_stat(parent).await.expect("p3"),
        dir_stat(0, 4096, 1)
    );

    meta.rmdir(parent, "child").await.expect("rmdir child");
    assert_eq!(
        meta.get_dir_stat(parent).await.expect("p4"),
        dir_stat(0, 0, 0)
    );
    meta.rmdir(ROOT_INODE, "parent")
        .await
        .expect("rmdir parent");
    assert_eq!(
        meta.get_dir_stat(ROOT_INODE).await.expect("r3"),
        dir_stat(0, 0, 0)
    );
}

const fn quota(max_space: i64, max_inodes: i64, used_space: i64, used_inodes: i64) -> Quota {
    Quota {
        max_space,
        max_inodes,
        used_space,
        used_inodes,
    }
}

#[tokio::test]
async fn test_user_group_quota_crud_and_enforcement() {
    let meta = open_meta("sqlite3://:memory:").expect("open");
    let mut fmt = Format::new("ugquotavol", "mem", "");
    fmt.user_group_quota = true;
    meta.init(&fmt, false).await.expect("init");

    let (pre, _) = meta
        .mknod(ROOT_INODE, "pre.bin", NodeType::File, 0o644, 1000, 2000)
        .await
        .expect("mknod pre");
    let sid1 = meta.new_slice().await.expect("sid1");
    meta.write_chunk(pre, 0, 0, full_slice(sid1, 50), 50)
        .await
        .expect("write pre");

    assert!(matches!(
        meta.get_ug_quota(QuotaTarget::User(1000)).await,
        Err(MetaError::NotFound(_))
    ));

    let uq = meta
        .set_ug_quota(QuotaTarget::User(1000), 200, 3)
        .await
        .expect("set user quota");
    assert_eq!(uq, quota(200, 3, 50, 1));
    let gq = meta
        .set_ug_quota(QuotaTarget::Group(2000), 500, 2)
        .await
        .expect("set group quota");
    assert_eq!(gq, quota(500, 2, 50, 1));

    let uq_upd = meta
        .set_ug_quota(QuotaTarget::User(1000), 150, 3)
        .await
        .expect("update user quota");
    assert_eq!(uq_upd.max_space, 150);
    assert_eq!(uq_upd.used_space, 50);
    assert_eq!(
        meta.list_ug_quotas().await.expect("list_ug_quotas"),
        vec![
            (QuotaTarget::User(1000), uq_upd),
            (QuotaTarget::Group(2000), gq)
        ]
    );

    let (f2, _) = meta
        .mknod(ROOT_INODE, "f2.bin", NodeType::File, 0o644, 1000, 2000)
        .await
        .expect("mknod f2");
    assert!(matches!(
        meta.mknod(ROOT_INODE, "f3.bin", NodeType::File, 0o644, 1000, 2000)
            .await,
        Err(MetaError::QuotaExceeded(_))
    ));

    let sid2 = meta.new_slice().await.expect("sid2");
    assert!(matches!(
        meta.write_chunk(f2, 0, 0, full_slice(sid2, 120), 120).await,
        Err(MetaError::QuotaExceeded(_))
    ));

    meta.write_chunk(f2, 0, 0, full_slice(sid2, 80), 80)
        .await
        .expect("write 80");
    assert_eq!(
        meta.get_ug_quota(QuotaTarget::User(1000))
            .await
            .expect("u")
            .used_space,
        130
    );

    meta.truncate(f2, 20).await.expect("truncate f2");
    assert_eq!(
        meta.get_ug_quota(QuotaTarget::Group(2000))
            .await
            .expect("g")
            .used_space,
        70
    );

    meta.unlink(ROOT_INODE, "f2.bin").await.expect("unlink f2");
    let after_unlink = meta
        .get_ug_quota(QuotaTarget::User(1000))
        .await
        .expect("after unlink");
    assert_eq!((after_unlink.used_space, after_unlink.used_inodes), (50, 1));

    meta.del_ug_quota(QuotaTarget::User(1000))
        .await
        .expect("del user quota");
    meta.del_ug_quota(QuotaTarget::Group(2000))
        .await
        .expect("del group quota");
    assert!(matches!(
        meta.del_ug_quota(QuotaTarget::User(1000)).await,
        Err(MetaError::NotFound(_))
    ));
    assert_eq!(meta.list_ug_quotas().await.expect("empty"), vec![]);
}

#[tokio::test]
async fn test_changelog_recording_and_pagination() {
    // Disabled by default
    let disabled = setup_meta().await;
    disabled
        .mknod(ROOT_INODE, "ignored.txt", NodeType::File, 0o644, 0, 0)
        .await
        .expect("mknod disabled");
    assert_eq!(
        disabled
            .read_changelog(0, 100)
            .await
            .expect("read disabled"),
        vec![]
    );

    // Enabled
    let meta = open_meta("sqlite3://:memory:").expect("open");
    let mut fmt = Format::new("changelogvol", "mem", "");
    fmt.change_log = true;
    meta.init(&fmt, false).await.expect("init");

    let (dir, _) = meta
        .mkdir(ROOT_INODE, "logs", 0o755, 0, 0)
        .await
        .expect("mkdir");
    let (file, _) = meta
        .mknod(dir, "app.log", NodeType::File, 0o644, 0, 0)
        .await
        .expect("mknod");
    let sid = meta.new_slice().await.expect("new_slice");
    meta.write_chunk(file, 0, 0, full_slice(sid, 16), 16)
        .await
        .expect("write_chunk");
    meta.rename(dir, "app.log", dir, "app.log.1")
        .await
        .expect("rename");
    meta.unlink(dir, "app.log.1").await.expect("unlink");
    meta.rmdir(ROOT_INODE, "logs").await.expect("rmdir");

    assert_eq!(meta.read_changelog(0, 0).await.expect("limit 0"), vec![]);

    let p1 = meta.read_changelog(0, 2).await.expect("page 1");
    assert_eq!(p1.len(), 2);
    assert!(p1[0].entry.contains("MKDIR"));
    assert!(p1[1].entry.contains("CREATE"));

    let p2 = meta.read_changelog(p1[1].id, 2).await.expect("page 2");
    assert_eq!(p2.len(), 2);
    assert!(p2[0].entry.contains("WRITE"));
    assert!(p2[1].entry.contains("RENAME"));

    let p3 = meta.read_changelog(p2[1].id, 2).await.expect("page 3");
    assert_eq!(p3.len(), 2);
    assert!(p3[0].entry.contains("UNLINK"));
    assert!(p3[1].entry.contains("RMDIR"));

    let p4 = meta.read_changelog(p3[1].id, 2).await.expect("page 4");
    assert_eq!(p4, vec![]);

    let all = meta.read_changelog(0, 100).await.expect("all");
    assert_eq!(all.len(), 6);
    assert!(all.windows(2).all(|w| w[0].id < w[1].id));
}

#[tokio::test]
async fn test_session_lifecycle_and_stale_cleanup() {
    let meta = setup_meta().await;

    let s1 = meta.new_session("/mnt/active", 60).await.expect("s1");
    let s2 = meta.new_session("/mnt/expired-zero", 0).await.expect("s2");
    let s3 = meta.new_session("/mnt/expired-neg", -10).await.expect("s3");
    assert!(s1.sid < s2.sid && s2.sid < s3.sid);
    assert_eq!(s1.mount_point, "/mnt/active");

    let sessions = meta.list_sessions().await.expect("list_sessions");
    assert_eq!(sessions.len(), 3);
    assert_eq!(sessions[0].sid, s1.sid);
    assert_eq!(sessions[1].sid, s2.sid);
    assert_eq!(sessions[2].sid, s3.sid);

    meta.heartbeat(s1.sid, 120).await.expect("heartbeat");
    assert!(matches!(
        meta.heartbeat(999_999, 60).await,
        Err(MetaError::NotFound(_))
    ));
    let refreshed = meta.list_sessions().await.expect("list after heartbeat");
    assert!(refreshed[0].expire >= s1.expire);

    let cleaned = meta
        .cleanup_stale_sessions()
        .await
        .expect("cleanup_stale_sessions");
    assert_eq!(cleaned, 2);

    let remaining = meta.list_sessions().await.expect("remaining sessions");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].sid, s1.sid);

    meta.close_session(s1.sid).await.expect("close_session");
    assert_eq!(meta.list_sessions().await.expect("empty sessions"), vec![]);
    assert!(matches!(
        meta.close_session(s1.sid).await,
        Err(MetaError::NotFound(_))
    ));
}

#[tokio::test]
async fn test_open_any_meta_and_redis_key_url_validation() {
    let meta = open_any_meta("sqlite3://:memory:")
        .await
        .expect("open_any_meta sqlite");
    let fmt = Format::new("anyvol", "mem", "");
    meta.init(&fmt, false).await.expect("init");
    assert_eq!(meta.load().await.expect("load").name, "anyvol");

    assert!(matches!(
        open_any_meta("http://unsupported.local").await,
        Err(MetaError::InvalidInput(_))
    ));

    #[cfg(feature = "redis")]
    {
        assert_eq!(RedisMeta::SETTING_KEY, "setting");
        assert_eq!(RedisMeta::NEXT_INODE_KEY, "nextInode");
        assert_eq!(RedisMeta::NEXT_CHUNK_KEY, "nextChunk");
        assert_eq!(RedisMeta::inode_key(Ino(42)), "i42");
        assert_eq!(RedisMeta::dir_key(ROOT_INODE), "d1");
        assert_eq!(RedisMeta::chunk_key(Ino(7), 3), "c7_3");
        assert_eq!(RedisMeta::symlink_key(Ino(9)), "sym9");
        assert_eq!(RedisMeta::xattr_key(Ino(11)), "x11");

        RedisMeta::validate_url("redis://127.0.0.1:6379/0").expect("valid redis url");
        RedisMeta::validate_url("rediss://user:pass@redis.example.com:6380/1")
            .expect("valid rediss url");

        for bad in [
            "sqlite3://:memory:",
            "redis://",
            "redis:///0",
            "redis://:6379",
        ] {
            assert!(
                matches!(
                    RedisMeta::validate_url(bad),
                    Err(MetaError::InvalidInput(_))
                ),
                "expected InvalidInput for {bad}"
            );
        }

        assert!(matches!(
            open_any_meta("redis://").await,
            Err(MetaError::InvalidInput(_))
        ));
    }

    #[cfg(not(feature = "redis"))]
    {
        assert!(matches!(
            open_any_meta("redis://127.0.0.1:6379/0").await,
            Err(MetaError::InvalidInput(_))
        ));
    }
}

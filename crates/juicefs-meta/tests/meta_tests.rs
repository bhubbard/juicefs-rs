//! Ported tests from `pkg/meta/base_test.go`, `pkg/meta/dump_test.go`, and `pkg/meta/interface_test.go`.

use juicefs_core::{
    CHUNK_SIZE, Format, Ino, NodeType, ROOT_INODE, SetAttrFlags, Slice, TRASH_INODE,
};
use juicefs_meta::{FlockType, Meta, MetaError, PlockRecord, Quota, SqliteMeta, open_meta};
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

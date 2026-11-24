//! Ported tests from `pkg/vfs/vfs_test.go`.

use std::sync::Arc;

use juicefs_core::{CHUNK_SIZE, ChunkConfig, ClientConfig, Format, SetAttrFlags};
use juicefs_meta::{Meta, MetaError, SqliteMeta};
use juicefs_vfs::{
    AccessControlList, AccessMode, AclEntry, MAX_SYMLINK_DEPTH, Vfs, VfsError, VolumeStats,
};
use tempfile::tempdir;

async fn setup_vfs(compress: &str, block_size: usize) -> (Vfs, tempfile::TempDir) {
    let dir = tempdir().expect("tempdir");
    let meta_path = dir.path().join("meta.db");
    let data_path = dir.path().join("data");
    let meta = Arc::new(SqliteMeta::open(&meta_path).expect("sqlite"));
    let mut fmt = Format::new("vfstest", "file", data_path.to_string_lossy().as_ref());
    fmt.block_size = block_size;
    fmt.compression = compress.to_owned();
    meta.init(&fmt, false).await.expect("init");

    let chunk_cfg = ChunkConfig {
        cache_dir: Some(dir.path().join("cache")),
        ..Default::default()
    };
    let vfs = Vfs::connect(meta, chunk_cfg, ClientConfig::default())
        .await
        .expect("connect");
    (vfs, dir)
}

#[tokio::test]
async fn test_vfs_basic_tree_and_rename() {
    let (vfs, _dir) = setup_vfs("none", 4096).await;

    vfs.mkdir_p("/dir1/sub", 0o755).await.expect("mkdir_p");
    vfs.write_file("/dir1/sub/a.txt", b"alpha")
        .await
        .expect("write a");
    vfs.write_file("/dir1/sub/b.txt", b"beta")
        .await
        .expect("write b");

    let entries = vfs.readdir("/dir1/sub").await.expect("readdir");
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, vec!["a.txt", "b.txt"]);

    vfs.rename("/dir1/sub/a.txt", "/dir1/a_moved.txt")
        .await
        .expect("rename");
    assert_eq!(
        vfs.read_file("/dir1/a_moved.txt")
            .await
            .expect("read moved"),
        b"alpha"
    );
    assert!(matches!(
        vfs.resolve_path("/dir1/sub/a.txt").await,
        Err(VfsError::Meta(juicefs_meta::MetaError::NotFound(_)))
    ));

    vfs.rm_rf("/dir1").await.expect("rm_rf");
    assert_eq!(vfs.readdir("/").await.expect("root readdir").len(), 0);
}

#[tokio::test]
async fn test_vfs_io_multi_chunk_and_sparse_holes() {
    let (vfs, _dir) = setup_vfs("zstd", 1024).await;
    let (ino, _) = vfs.create_file("/multi.bin", 0o644).await.expect("create");

    // Write near end of chunk 0 and across into chunk 1
    let cross_offset = CHUNK_SIZE - 10;
    let payload = b"0123456789ABCDEFGHIJ"; // 10 bytes in chunk 0, 10 bytes in chunk 1
    let written = vfs
        .write_at(ino, cross_offset, payload)
        .await
        .expect("cross-chunk write");
    assert_eq!(written, payload.len());

    let mut read_buf = vec![0u8; 30];
    // Read 5 bytes before cross_offset (sparse zeros) + 20 bytes written + 5 bytes past EOF
    let n = vfs
        .read_at(ino, cross_offset - 5, &mut read_buf)
        .await
        .expect("cross-chunk read");
    assert_eq!(n, 25);
    assert_eq!(&read_buf[0..5], &[0u8; 5]);
    assert_eq!(&read_buf[5..25], payload);
}

#[tokio::test]
async fn test_vfs_xattrs_and_gc() {
    let (vfs, _dir) = setup_vfs("lz4", 512).await;
    let (ino, _) = vfs.create_file("/tagged", 0o644).await.expect("create");
    vfs.meta()
        .set_xattr(ino, "user.tag", b"v1")
        .await
        .expect("set_xattr");
    assert_eq!(
        vfs.meta()
            .get_xattr(ino, "user.tag")
            .await
            .expect("get_xattr"),
        b"v1"
    );

    vfs.write_at(ino, 0, b"payload-1").await.expect("write 1");
    vfs.write_at(ino, 0, b"payload-2").await.expect("overwrite");
    vfs.unlink("/tagged").await.expect("unlink");

    let collected = vfs.gc_orphans().await.expect("gc");
    assert_eq!(collected, 2);
}

#[tokio::test]
async fn test_vfs_access_permissions_and_root_bypass() {
    let (vfs, _dir) = setup_vfs("none", 4096).await;
    let (ino, mut attr) = vfs.create_file("/secret.txt", 0o640).await.expect("create");

    // Set owner uid=1000, gid=2000, mode=0o640 (rw- r-- ---)
    attr.uid = 1000;
    attr.gid = 2000;
    attr.mode = 0o640;
    let mask = SetAttrFlags::UID
        .union(SetAttrFlags::GID)
        .union(SetAttrFlags::MODE);
    vfs.meta().setattr(ino, &attr, mask).await.expect("setattr");

    // Owner: R+W allowed, EXEC denied
    vfs.access(ino, 1000, 9999, AccessMode::READ | AccessMode::WRITE)
        .await
        .expect("owner rw");
    assert!(matches!(
        vfs.access(ino, 1000, 9999, AccessMode::EXEC).await,
        Err(VfsError::PermissionDenied(_))
    ));

    // Group: READ allowed, WRITE denied
    vfs.access(ino, 1001, 2000, AccessMode::READ)
        .await
        .expect("group read");
    assert!(matches!(
        vfs.access(ino, 1001, 2000, AccessMode::WRITE).await,
        Err(VfsError::PermissionDenied(_))
    ));

    // Other: READ/WRITE/EXEC all denied
    assert!(matches!(
        vfs.access(ino, 1002, 2001, AccessMode::READ).await,
        Err(VfsError::PermissionDenied(_))
    ));

    // Root (uid=0): bypasses R/W even on 0o000 file, but EXEC on non-dir requires 0o111
    attr.mode = 0o000;
    vfs.meta()
        .setattr(ino, &attr, SetAttrFlags::MODE)
        .await
        .expect("chmod 000");
    vfs.access(ino, 0, 0, AccessMode::READ | AccessMode::WRITE)
        .await
        .expect("root bypasses R/W");
    assert!(matches!(
        vfs.access(ino, 0, 0, AccessMode::EXEC).await,
        Err(VfsError::PermissionDenied(_))
    ));

    // With an executable bit set, root gets EXEC too
    attr.mode = 0o700;
    vfs.meta()
        .setattr(ino, &attr, SetAttrFlags::MODE)
        .await
        .expect("chmod 700");
    vfs.access(
        ino,
        0,
        0,
        AccessMode::READ | AccessMode::WRITE | AccessMode::EXEC,
    )
    .await
    .expect("root rwx on executable file");
}

#[tokio::test]
async fn test_acl_encode_decode_and_vfs_eval() {
    let acl = AccessControlList {
        owner: 0o7,
        group: 0o5,
        mask: 0o6,
        other: 0o0,
        named_users: vec![
            AclEntry { id: 500, perm: 0o7 }, // masked to 0o6 (rw-)
            AclEntry { id: 501, perm: 0o4 },
        ],
        named_groups: vec![AclEntry { id: 600, perm: 0o6 }],
    };

    let encoded = acl.encode();
    let decoded = AccessControlList::decode(&encoded).expect("decode acl");
    assert_eq!(decoded, acl);

    assert!(decoded.can_access(
        500,
        &[999],
        1000,
        2000,
        AccessMode::READ | AccessMode::WRITE
    ));
    assert!(!decoded.can_access(500, &[999], 1000, 2000, AccessMode::EXEC));
    assert!(decoded.can_access(999, &[600], 1000, 2000, AccessMode::WRITE));
    assert!(!decoded.can_access(999, &[999], 1000, 2000, AccessMode::READ));

    // Persist ACL on a VFS file and evaluate through Vfs::access
    let (vfs, _dir) = setup_vfs("none", 4096).await;
    let (ino, mut attr) = vfs.create_file("/acl_file", 0o600).await.expect("create");
    attr.uid = 1000;
    attr.gid = 2000;
    vfs.meta()
        .setattr(ino, &attr, SetAttrFlags::UID.union(SetAttrFlags::GID))
        .await
        .expect("chown");
    vfs.set_acl(ino, &acl).await.expect("set_acl");
    assert_eq!(vfs.get_acl(ino).await.expect("get_acl"), acl);

    vfs.access(ino, 500, 999, AccessMode::READ | AccessMode::WRITE)
        .await
        .expect("named user rw via acl");
    assert!(matches!(
        vfs.access(ino, 500, 999, AccessMode::EXEC).await,
        Err(VfsError::PermissionDenied(_))
    ));
}

#[tokio::test]
async fn test_read_internal_stats_and_config() {
    let (vfs, _dir) = setup_vfs("lz4", 4096).await;
    vfs.write_file("/hello.txt", b"hello internal")
        .await
        .expect("write");

    let stats_bytes = vfs.read_internal(".stats").await.expect("read .stats");
    let stats: VolumeStats = serde_json::from_slice(&stats_bytes).expect("parse stats json");
    assert_eq!(stats.volume, "vfstest");
    assert_eq!(stats.compression, "lz4");
    assert_eq!(stats.block_size, 4096);
    assert!(stats.used_inodes >= 1);
    assert!(stats.used_space >= 14);

    let config_bytes = vfs.read_internal(".config").await.expect("read .config");
    let cfg: Format = serde_json::from_slice(&config_bytes).expect("parse format json");
    assert_eq!(cfg.name, "vfstest");
    assert_eq!(cfg.uuid, vfs.format().uuid);
    assert_eq!(cfg.storage, "file");
    assert_eq!(cfg.block_size, 4096);

    assert!(matches!(
        vfs.read_internal(".nonexistent").await,
        Err(VfsError::Meta(MetaError::NotFound(_)))
    ));
}

#[tokio::test]
async fn test_compact_file_overlapping_writes() {
    let (vfs, _dir) = setup_vfs("lz4", 1024).await;
    let (ino, _) = vfs.create_file("/frag.bin", 0o644).await.expect("create");

    // Multiple overlapping and sparse writes inside chunk 0
    vfs.write_at(ino, 0, b"aaaaaaaaaaaaaaaa").await.expect("w1");
    vfs.write_at(ino, 4, b"BBBB").await.expect("w2");
    vfs.write_at(ino, 12, b"CCCC").await.expect("w3");
    vfs.write_at(ino, 24, b"TAIL").await.expect("w4");

    let before_slices = vfs.meta().read_chunk(ino, 0).await.expect("read_chunk");
    assert!(
        before_slices.len() > 1,
        "expected fragmented chunk, got {} slices",
        before_slices.len()
    );

    let expected = vfs.read_file("/frag.bin").await.expect("read before");
    assert_eq!(&expected[0..16], b"aaaaBBBBaaaaCCCC");
    assert_eq!(&expected[16..24], &[0u8; 8]);
    assert_eq!(&expected[24..28], b"TAIL");

    let compacted = vfs.compact_file("/frag.bin").await.expect("compact");
    assert_eq!(compacted, 1);

    let after_slices = vfs.meta().read_chunk(ino, 0).await.expect("read_chunk");
    assert_eq!(after_slices.len(), 1);

    let actual = vfs.read_file("/frag.bin").await.expect("read after");
    assert_eq!(actual, expected);

    // Compacting an already compacted file is a no-op
    assert_eq!(vfs.compact_file("/frag.bin").await.expect("recompact"), 0);
}

#[tokio::test]
async fn test_symlinks_and_loop_detection() {
    let (vfs, _dir) = setup_vfs("none", 4096).await;
    vfs.mkdir_p("/dir/sub", 0o755).await.expect("mkdir_p");
    let file_attr = vfs
        .write_file("/dir/sub/target.txt", b"symlink payload")
        .await
        .expect("write");

    // Relative symlink within same directory
    vfs.symlink("/dir/sub/rel_link", "target.txt")
        .await
        .expect("rel symlink");
    assert_eq!(
        vfs.readlink("/dir/sub/rel_link").await.expect("readlink"),
        "target.txt"
    );

    let (resolved_ino, resolved_attr) = vfs
        .resolve_path_follow("/dir/sub/rel_link")
        .await
        .expect("resolve rel");
    assert_eq!(resolved_attr.length, file_attr.length);

    let mut buf = vec![0u8; resolved_attr.length as usize];
    vfs.read_at(resolved_ino, 0, &mut buf)
        .await
        .expect("read_at target");
    assert_eq!(&buf, b"symlink payload");

    // Directory symlink + relative ".." traversal
    vfs.symlink("/dir_alias", "/dir")
        .await
        .expect("dir symlink");
    vfs.symlink("/dir/up_link", "../dir/sub/rel_link")
        .await
        .expect("up symlink");
    let (up_ino, _) = vfs
        .resolve_path_follow("/dir_alias/up_link")
        .await
        .expect("resolve up_link");
    assert_eq!(up_ino, resolved_ino);

    // Mutual symlink cycle -> TooManySymlinks
    vfs.symlink("/loop_a", "/loop_b").await.expect("loop_a");
    vfs.symlink("/loop_b", "/loop_a").await.expect("loop_b");
    assert!(matches!(
        vfs.resolve_path_follow("/loop_a").await,
        Err(VfsError::TooManySymlinks)
    ));

    // Chain exceeding MAX_SYMLINK_DEPTH (40) -> TooManySymlinks
    vfs.mkdir_p("/chain", 0o755).await.expect("mkdir chain");
    vfs.write_file("/chain/end", b"ok")
        .await
        .expect("write end");
    let mut prev = "/chain/end".to_owned();
    for i in 0..=MAX_SYMLINK_DEPTH {
        let link = format!("/chain/l{i}");
        vfs.symlink(&link, &prev).await.expect("chain symlink");
        prev = link;
    }
    // 40 hops from l39 works
    let l39 = format!("/chain/l{}", MAX_SYMLINK_DEPTH - 1);
    let (_, end_attr) = vfs.resolve_path_follow(&l39).await.expect("40 hops ok");
    assert_eq!(end_attr.length, 2);
    // 41 hops from l40 fails with TooManySymlinks
    assert!(matches!(
        vfs.resolve_path_follow(&prev).await,
        Err(VfsError::TooManySymlinks)
    ));
}

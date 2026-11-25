//! Integration tests for `juicefs-fuse` POSIX lifecycle and errno mapping.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use juicefs_core::{
    Attr, ChunkConfig, ClientConfig, CoreError, Format, Ino, MAX_NAME, NodeType, ROOT_INODE,
    SetAttrFlags,
};
use juicefs_fuse::{FuseError, JuiceFsFuse, MountConfig, unmount};
use juicefs_meta::{FlockType, Meta, MetaError, PlockRecord, SqliteMeta};
use juicefs_vfs::{AccessMode, Vfs, VfsError};
use tempfile::{TempDir, tempdir};

async fn setup_fuse(read_only: bool) -> (JuiceFsFuse, TempDir) {
    let dir = tempdir().expect("create tempdir");
    let meta_path = dir.path().join("meta.db");
    let data_path = dir.path().join("data");
    let mountpoint = dir.path().join("mnt");

    let meta = Arc::new(SqliteMeta::open(&meta_path).expect("open sqlite meta"));
    let mut fmt = Format::new("fusetest", "file", data_path.to_string_lossy().as_ref());
    fmt.block_size = 4096;
    "lz4".clone_into(&mut fmt.compression);
    fmt.capacity = 100 * 1024 * 1024;
    fmt.inodes = 10_000;
    meta.init(&fmt, false).await.expect("init volume format");

    let chunk_cfg = ChunkConfig {
        cache_dir: Some(dir.path().join("cache")),
        ..Default::default()
    };
    let vfs = Vfs::connect(meta, chunk_cfg, ClientConfig::default())
        .await
        .expect("connect vfs");

    let config = MountConfig::new(&mountpoint)
        .with_allow_other(false)
        .with_read_only(read_only)
        .with_entry_ttl(Duration::from_secs(3))
        .with_attr_ttl(Duration::from_secs(5));

    (JuiceFsFuse::new(vfs, config), dir)
}

#[tokio::test]
async fn test_full_posix_lifecycle() {
    let (fs, dir) = setup_fuse(false).await;
    assert_eq!(fs.config().mountpoint, dir.path().join("mnt"));
    assert_eq!(fs.config().entry_ttl, Duration::from_secs(3));
    assert_eq!(fs.config().attr_ttl, Duration::from_secs(5));

    // mkdir + lookup + create + write + flush + fsync + read + setattr
    let work = fs
        .mkdir(ROOT_INODE, "work", 0o755, 1000, 1000)
        .await
        .expect("mkdir");
    let work_ino = Ino(work.ino);
    let looked = fs.lookup(ROOT_INODE, "work").await.expect("lookup");
    assert_eq!(looked.ino, work.ino);

    let flags = libc::O_CREAT | libc::O_EXCL | libc::O_RDWR;
    let (file_e, fh) = fs
        .create(work_ino, "hello.txt", 0o644, flags, 1000, 2000)
        .await
        .expect("create");
    let file_ino = Ino(file_e.ino);
    assert_eq!(fs.open_handle_count(), 1);
    let written = fs
        .write(file_ino, fh, 0, b"hello fuse world")
        .await
        .expect("write");
    assert_eq!(written, 16);
    assert!(fs.get_handle(fh).expect("handle").dirty);
    fs.fsync(file_ino, fh, true).await.expect("fsync");
    assert!(!fs.get_handle(fh).expect("handle").dirty);
    fs.flush(file_ino, fh, 0).await.expect("flush");
    let read_buf = fs.read(file_ino, fh, 0, 64).await.expect("read");
    assert_eq!(read_buf, b"hello fuse world");

    let patch = Attr {
        mode: 0o640,
        uid: 1001,
        gid: 2001,
        length: 5,
        ..Attr::default()
    };
    let mask = SetAttrFlags::MODE
        .union(SetAttrFlags::UID)
        .union(SetAttrFlags::GID)
        .union(SetAttrFlags::SIZE);
    let updated = fs.setattr(file_ino, &patch, mask).await.expect("setattr");
    assert_eq!((updated.size, updated.mode & 0o7777), (5, 0o640));
    let trunc_buf = fs.read(file_ino, fh, 0, 32).await.expect("read trunc");
    assert_eq!(trunc_buf, b"hello");

    // readdir + rename + link + symlink/readlink + statfs + release + unlink + rmdir
    let entries = fs.readdir(work_ino, 0).await.expect("readdir");
    assert_eq!((entries.len(), entries[0].name.as_str()), (1, "hello.txt"));

    fs.rename(work_ino, "hello.txt", work_ino, "renamed.txt", 0)
        .await
        .expect("rename");
    let linked = fs.link(file_ino, work_ino, "hard.txt").await.expect("link");
    assert_eq!(linked.attr.nlink, 2);

    let sym = fs
        .symlink(work_ino, "sym.txt", "renamed.txt", 1000, 1000)
        .await
        .expect("symlink");
    let target = fs.readlink(Ino(sym.ino)).await.expect("readlink");
    assert_eq!(target, "renamed.txt");

    fs.setxattr(file_ino, "user.tag", b"v1", libc::XATTR_CREATE)
        .await
        .expect("setxattr");
    assert_eq!(
        fs.getxattr(file_ino, "user.tag").await.expect("getxattr"),
        b"v1"
    );
    fs.flock(file_ino, fh, 1, FlockType::Exclusive)
        .await
        .expect("flock");
    fs.access(file_ino, 1001, 9999, AccessMode::READ | AccessMode::WRITE)
        .await
        .expect("access");
    assert_eq!(fs.statfs().await.expect("statfs").namelen, MAX_NAME as u32);

    fs.release(file_ino, fh).await.expect("release");
    assert_eq!(fs.open_handle_count(), 0);
    let err = fs
        .read(file_ino, fh, 0, 4)
        .await
        .expect_err("read after release");
    assert_eq!(err.errno(), libc::EBADF);

    fs.unlink(work_ino, "sym.txt").await.expect("unlink sym");
    fs.unlink(work_ino, "hard.txt").await.expect("unlink hard");
    fs.unlink(work_ino, "renamed.txt")
        .await
        .expect("unlink renamed");
    fs.rmdir(ROOT_INODE, "work").await.expect("rmdir work");
    assert_eq!(fs.readdir(ROOT_INODE, 0).await.expect("root").len(), 0);
}

#[tokio::test]
async fn test_dirs_mknod_rename_links_and_handles() {
    let (fs, _) = setup_fuse(false).await;
    let dir = fs
        .mkdir(ROOT_INODE, "d", 0o755, 1000, 1000)
        .await
        .expect("mkdir");
    let dir_ino = Ino(dir.ino);
    assert_eq!(
        dir.attr.mode & u32::from(libc::S_IFMT),
        u32::from(libc::S_IFDIR)
    );

    let sub = fs
        .mkdir(dir_ino, "sub", 0o750, 1000, 1000)
        .await
        .expect("mkdir sub");
    let sub_ino = Ino(sub.ino);

    let (file_e, fh) = fs
        .create(
            dir_ino,
            "a.bin",
            0o644,
            libc::O_CREAT | libc::O_RDWR,
            1000,
            1000,
        )
        .await
        .expect("create a.bin");
    let file_ino = Ino(file_e.ino);
    fs.write(file_ino, fh, 0, b"0123456789")
        .await
        .expect("write");

    let extend = Attr {
        length: 14,
        ..Attr::default()
    };
    let ext_attr = fs
        .setattr(file_ino, &extend, SetAttrFlags::SIZE)
        .await
        .expect("extend");
    assert_eq!(ext_attr.size, 14);
    assert_eq!(
        fs.read(file_ino, fh, 8, 16).await.expect("read tail"),
        b"89\0\0\0\0"
    );

    let fifo = fs
        .mknod(
            dir_ino,
            "pipe",
            u32::from(libc::S_IFIFO) | 0o600,
            1000,
            1000,
            0,
        )
        .await
        .expect("mknod fifo");
    assert_eq!(
        fifo.attr.mode & u32::from(libc::S_IFMT),
        u32::from(libc::S_IFIFO)
    );

    let dir_fh = fs.opendir(dir_ino, libc::O_RDONLY).await.expect("opendir");
    let entries = fs.readdir(dir_ino, 0).await.expect("readdir");
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, vec!["a.bin", "pipe", "sub"]);
    assert_eq!(entries[0].typ, NodeType::File);
    assert_eq!(entries[1].typ, NodeType::Fifo);
    assert_eq!(entries[2].typ, NodeType::Directory);

    let page2 = fs
        .readdir(dir_ino, entries[0].offset)
        .await
        .expect("readdir page 2");
    let page2_names: Vec<&str> = page2.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(page2_names, vec!["pipe", "sub"]);
    fs.releasedir(dir_ino, dir_fh).await.expect("releasedir");

    // RENAME_NOREPLACE fails with EEXIST if target exists
    fs.rename(dir_ino, "a.bin", sub_ino, "b.bin", 0)
        .await
        .expect("rename");
    let noreplace_err = fs
        .rename(dir_ino, "pipe", sub_ino, "b.bin", 1)
        .await
        .expect_err("RENAME_NOREPLACE");
    assert_eq!(noreplace_err.errno(), libc::EEXIST);

    // Re-open truncated
    let fh_trunc = fs
        .open(file_ino, libc::O_RDWR | libc::O_TRUNC)
        .await
        .expect("open O_TRUNC");
    assert_eq!(fs.getattr(file_ino).await.expect("getattr").size, 0);

    fs.release(file_ino, fh).await.expect("release fh");
    fs.release(file_ino, fh_trunc)
        .await
        .expect("release fh_trunc");
}

#[tokio::test]
async fn test_xattr_and_statfs() {
    let (fs, _) = setup_fuse(false).await;
    let (file_e, fh) = fs
        .create(
            ROOT_INODE,
            "xattr.txt",
            0o640,
            libc::O_CREAT | libc::O_RDWR,
            1000,
            2000,
        )
        .await
        .expect("create");
    let file_ino = Ino(file_e.ino);

    fs.setxattr(file_ino, "user.a", b"one", libc::XATTR_CREATE)
        .await
        .expect("create xattr");
    assert_eq!(
        fs.setxattr(file_ino, "user.a", b"two", libc::XATTR_CREATE)
            .await
            .expect_err("dup xattr")
            .errno(),
        libc::EEXIST
    );
    fs.setxattr(file_ino, "user.a", b"two", libc::XATTR_REPLACE)
        .await
        .expect("replace xattr");
    assert_eq!(
        fs.setxattr(file_ino, "user.missing", b"x", libc::XATTR_REPLACE)
            .await
            .expect_err("replace missing")
            .errno(),
        libc::ENOENT
    );
    fs.setxattr(file_ino, "user.b", b"2", 0)
        .await
        .expect("set user.b");
    assert_eq!(
        fs.listxattr(file_ino).await.expect("listxattr"),
        vec!["user.a", "user.b"]
    );
    fs.removexattr(file_ino, "user.b")
        .await
        .expect("removexattr");
    assert_eq!(
        fs.getxattr(file_ino, "user.a").await.expect("getxattr"),
        b"two"
    );

    let st = fs.statfs().await.expect("statfs");
    assert_eq!((st.bsize, st.frsize), (4096, 4096));
    assert_eq!(st.blocks, (100 * 1024 * 1024) / 4096);
    assert!(st.files > 0 && st.ffree < st.files);

    fs.release(file_ino, fh).await.expect("release");
}

#[tokio::test]
async fn test_flock_plock_and_access() {
    let (fs, _) = setup_fuse(false).await;
    let (file_e, fh1) = fs
        .create(
            ROOT_INODE,
            "locked.txt",
            0o640,
            libc::O_CREAT | libc::O_RDWR,
            1000,
            2000,
        )
        .await
        .expect("create");
    let file_ino = Ino(file_e.ino);
    let fh2 = fs.open(file_ino, libc::O_RDONLY).await.expect("open fh2");

    fs.flock(file_ino, fh1, 100, FlockType::Shared)
        .await
        .expect("shared 1");
    fs.flock(file_ino, fh2, 200, FlockType::Shared)
        .await
        .expect("shared 2");
    assert_eq!(
        fs.flock(file_ino, fh2, 200, FlockType::Exclusive)
            .await
            .expect_err("flock EAGAIN")
            .errno(),
        libc::EAGAIN
    );
    fs.release(file_ino, fh1)
        .await
        .expect("release fh1 unlocks owner 100");
    fs.flock(file_ino, fh2, 200, FlockType::Exclusive)
        .await
        .expect("upgrade after fh1 release");

    let fh3 = fs.open(file_ino, libc::O_RDWR).await.expect("open fh3");
    let rec = PlockRecord {
        typ: FlockType::Exclusive,
        pid: 99,
        start: 0,
        end: 64,
    };
    fs.setlk(file_ino, fh3, 10, rec).await.expect("setlk");
    let conflict = fs.getlk(file_ino, fh2, 20, rec).await.expect("getlk");
    assert_eq!((conflict.typ, conflict.pid), (FlockType::Exclusive, 99));
    fs.flush(file_ino, fh3, 10)
        .await
        .expect("flush clears plock");
    assert_eq!(
        fs.getlk(file_ino, fh2, 20, rec)
            .await
            .expect("getlk cleared")
            .typ,
        FlockType::Unlock
    );

    fs.access(file_ino, 1000, 9999, AccessMode::READ | AccessMode::WRITE)
        .await
        .expect("owner rw");
    fs.access(file_ino, 1001, 2000, AccessMode::READ)
        .await
        .expect("group r");
    assert_eq!(
        fs.access(file_ino, 1001, 2000, AccessMode::WRITE)
            .await
            .expect_err("group w denied")
            .errno(),
        libc::EACCES
    );

    fs.release(file_ino, fh2).await.expect("release fh2");
    fs.release(file_ino, fh3).await.expect("release fh3");
}

#[tokio::test]
async fn test_posix_errno_mapping_live_and_direct() {
    let direct_cases: &[(FuseError, libc::c_int)] = &[
        (MetaError::NotFound("missing".into()).into(), libc::ENOENT),
        (MetaError::AlreadyExists("dup".into()).into(), libc::EEXIST),
        (MetaError::NotDir("file".into()).into(), libc::ENOTDIR),
        (MetaError::IsDir("dir".into()).into(), libc::EISDIR),
        (MetaError::NotEmpty("dir".into()).into(), libc::ENOTEMPTY),
        (
            VfsError::PermissionDenied("denied".into()).into(),
            libc::EACCES,
        ),
        (MetaError::InvalidInput("bad".into()).into(), libc::EINVAL),
        (MetaError::WouldBlock("busy".into()).into(), libc::EAGAIN),
        (
            MetaError::Core(CoreError::Compression("zstd".into())).into(),
            libc::EIO,
        ),
    ];
    for (err, expected) in direct_cases {
        assert_eq!(err.errno(), *expected, "unexpected errno for {err}");
    }

    let (fs, _) = setup_fuse(false).await;
    let dir_e = fs
        .mkdir(ROOT_INODE, "d", 0o755, 0, 0)
        .await
        .expect("mkdir d");
    let dir_ino = Ino(dir_e.ino);
    let (file_e, fh) = fs
        .create(
            dir_ino,
            "f",
            0o600,
            libc::O_CREAT | libc::O_RDWR,
            1000,
            1000,
        )
        .await
        .expect("create f");
    let file_ino = Ino(file_e.ino);

    assert_eq!(
        fs.lookup(ROOT_INODE, "nope")
            .await
            .expect_err("ENOENT")
            .errno(),
        libc::ENOENT
    );
    assert_eq!(
        fs.mkdir(ROOT_INODE, "d", 0o755, 0, 0)
            .await
            .expect_err("EEXIST")
            .errno(),
        libc::EEXIST
    );
    assert_eq!(
        fs.readdir(file_ino, 0).await.expect_err("ENOTDIR").errno(),
        libc::ENOTDIR
    );
    assert_eq!(
        fs.open(dir_ino, libc::O_RDWR)
            .await
            .expect_err("EISDIR")
            .errno(),
        libc::EISDIR
    );
    assert_eq!(
        fs.rmdir(ROOT_INODE, "d")
            .await
            .expect_err("ENOTEMPTY")
            .errno(),
        libc::ENOTEMPTY
    );
    assert_eq!(
        fs.access(file_ino, 2000, 2000, AccessMode::READ)
            .await
            .expect_err("EACCES")
            .errno(),
        libc::EACCES
    );
    assert_eq!(
        fs.readlink(file_ino).await.expect_err("EINVAL").errno(),
        libc::EINVAL
    );

    let fh_other = fs
        .open(file_ino, libc::O_RDONLY)
        .await
        .expect("open fh_other");
    fs.flock(file_ino, fh, 1, FlockType::Exclusive)
        .await
        .expect("lock 1");
    assert_eq!(
        fs.flock(file_ino, fh_other, 2, FlockType::Exclusive)
            .await
            .expect_err("EAGAIN")
            .errno(),
        libc::EAGAIN
    );

    fs.release(file_ino, fh).await.expect("release fh");
    fs.release(file_ino, fh_other)
        .await
        .expect("release fh_other");
}

#[tokio::test]
async fn test_read_only_mount_and_unmount_validation() {
    let (fs, _) = setup_fuse(true).await;

    assert_eq!(
        fs.mkdir(ROOT_INODE, "ro", 0o755, 0, 0)
            .await
            .expect_err("mkdir ro")
            .errno(),
        libc::EROFS
    );
    assert_eq!(
        fs.create(
            ROOT_INODE,
            "ro.txt",
            0o644,
            libc::O_CREAT | libc::O_RDWR,
            0,
            0
        )
        .await
        .expect_err("create ro")
        .errno(),
        libc::EROFS
    );
    let ro_fh = fs
        .open(ROOT_INODE, libc::O_RDONLY)
        .await
        .expect("open root read-only");
    fs.release(ROOT_INODE, ro_fh)
        .await
        .expect("release root fh");
    assert_eq!(
        fs.access(ROOT_INODE, 0, 0, AccessMode::WRITE)
            .await
            .expect_err("access W_OK ro")
            .errno(),
        libc::EROFS
    );
    assert_eq!(
        fs.access(Ino(999_999), 0, 0, AccessMode::WRITE)
            .await
            .expect_err("missing inode")
            .errno(),
        libc::ENOENT
    );

    assert_eq!(
        unmount(Path::new(""))
            .expect_err("empty mountpoint")
            .errno(),
        libc::EINVAL
    );
    assert!(unmount(Path::new("/nonexistent/juicefs-rs-test-mnt")).is_err());
}

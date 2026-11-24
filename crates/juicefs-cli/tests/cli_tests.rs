//! End-to-end CLI integration tests for the `juicefs` binary.

use std::process::Command;

use tempfile::tempdir;

fn jfs_bin() -> &'static str {
    env!("CARGO_BIN_EXE_juicefs")
}

#[test]
fn cli_full_volume_lifecycle() {
    let dir = tempdir().expect("tempdir");
    let meta = format!("sqlite3://{}", dir.path().join("meta.db").display());
    let data = dir.path().join("data").to_string_lossy().into_owned();
    let src_file = dir.path().join("input.txt");
    let dst_file = dir.path().join("output.txt");
    let dump_file = dir.path().join("dump.json");
    std::fs::write(&src_file, b"juicefs-cli-integration").expect("write src");

    let run = |args: &[&str]| {
        let out = Command::new(jfs_bin()).args(args).output().expect("exec");
        assert!(
            out.status.success(),
            "command `juicefs {}` failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    };

    run(&[
        "format",
        &meta,
        "clivol",
        "--storage",
        "file",
        "--bucket",
        &data,
        "--compress",
        "zstd",
    ]);
    run(&["mkdir", "-p", &meta, "/work/sub"]);
    run(&[
        "put",
        &meta,
        src_file.to_str().unwrap(),
        "/work/sub/test.txt",
    ]);
    let ls_out = run(&["ls", &meta, "/work/sub"]);
    assert!(ls_out.contains("test.txt"));

    let cat_out = run(&["cat", &meta, "/work/sub/test.txt"]);
    assert_eq!(cat_out, "juicefs-cli-integration");

    run(&[
        "get",
        &meta,
        "/work/sub/test.txt",
        dst_file.to_str().unwrap(),
    ]);
    assert_eq!(
        std::fs::read(&dst_file).expect("read dst"),
        b"juicefs-cli-integration"
    );

    run(&["quota", "set", &meta, "/work", "--capacity", "1048576"]);
    let q_out = run(&["quota", "get", &meta, "/work"]);
    assert!(q_out.contains("1048576"));

    run(&["fsck", &meta]);
    run(&["dump", &meta, "-o", dump_file.to_str().unwrap()]);
    assert!(dump_file.exists());

    run(&["rm", "-r", &meta, "/work"]);
    run(&["gc", &meta]);
}

#[test]
fn cli_sync_command() {
    let dir = tempdir().expect("tempdir");
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    std::fs::create_dir_all(&src).expect("mkdir src");
    std::fs::write(src.join("a.txt"), b"alpha").expect("write a");
    std::fs::write(src.join("b.txt"), b"beta").expect("write b");

    let out = Command::new(jfs_bin())
        .args([
            "sync",
            &format!("file://{}", src.display()),
            &format!("file://{}", dst.display()),
        ])
        .output()
        .expect("exec sync");
    assert!(
        out.status.success(),
        "sync failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(std::fs::read(dst.join("a.txt")).unwrap(), b"alpha");
    assert_eq!(std::fs::read(dst.join("b.txt")).unwrap(), b"beta");
}

//! Criterion benchmarks for `juicefs-core` compression/slices and `juicefs-vfs` I/O & metadata.
#![allow(missing_docs)]

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use juicefs_core::{ChunkConfig, ClientConfig, CompressionAlgo, Format, SliceRecord, build_slice};
use juicefs_meta::{Meta, SqliteMeta};
use juicefs_vfs::Vfs;
use tempfile::tempdir;
use tokio::runtime::Runtime;

fn bench_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression_1mb");
    let payload = vec![b'A'; 1 << 20];
    group.throughput(Throughput::Bytes(payload.len() as u64));

    for algo in [CompressionAlgo::Lz4, CompressionAlgo::Zstd] {
        group.bench_with_input(
            BenchmarkId::new("compress", algo.as_str()),
            &payload,
            |b, data| {
                b.iter(|| algo.compress(data).expect("compress"));
            },
        );
        let compressed = algo.compress(&payload).expect("compress");
        group.bench_with_input(
            BenchmarkId::new("decompress", algo.as_str()),
            &compressed,
            |b, data| {
                b.iter(|| algo.decompress(data, payload.len()).expect("decompress"));
            },
        );
    }
    group.finish();
}

fn bench_slice_resolution(c: &mut Criterion) {
    let records: Vec<SliceRecord> = (0..100)
        .map(|i| SliceRecord {
            pos: i * 4096,
            id: u64::from(i) + 1,
            size: 8192,
            off: 0,
            len: 8192,
        })
        .collect();

    c.bench_function("build_slice_100_overlapping", |b| {
        b.iter(|| build_slice(&records));
    });
}

fn bench_vfs_io_and_meta(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let dir = tempdir().expect("tempdir");
    let meta_path = dir.path().join("bench_meta.db");
    let data_path = dir.path().join("bench_data");

    let vfs = rt.block_on(async {
        let meta = Arc::new(SqliteMeta::open(&meta_path).expect("sqlite"));
        let mut fmt = Format::new("benchvol", "file", data_path.to_string_lossy().as_ref());
        "lz4".clone_into(&mut fmt.compression);
        meta.init(&fmt, false).await.expect("init");
        Vfs::connect(meta, ChunkConfig::default(), ClientConfig::default())
            .await
            .expect("connect")
    });

    let payload_64k = vec![42u8; 64 * 1024];
    let mut group = c.benchmark_group("vfs_io");
    group.throughput(Throughput::Bytes(payload_64k.len() as u64));

    group.bench_function("write_64k_file_lz4", |b| {
        let mut idx = 0u64;
        b.to_async(&rt).iter(|| {
            idx += 1;
            let path = format!("/f_{idx}");
            let vfs = &vfs;
            let data = &payload_64k;
            async move {
                vfs.write_file(&path, data).await.expect("write_file");
            }
        });
    });

    rt.block_on(async {
        vfs.write_file("/read_target", &payload_64k)
            .await
            .expect("seed read_target");
    });

    group.bench_function("read_64k_file_cached", |b| {
        b.to_async(&rt).iter(|| {
            let vfs = &vfs;
            async move {
                let data = vfs.read_file("/read_target").await.expect("read_file");
                assert_eq!(data.len(), 64 * 1024);
            }
        });
    });
    group.finish();

    c.bench_function("meta_create_and_unlink", |b| {
        let mut idx = 0u64;
        b.to_async(&rt).iter(|| {
            idx += 1;
            let path = format!("/m_{idx}");
            let vfs = &vfs;
            async move {
                vfs.create_file(&path, 0o644).await.expect("create");
                vfs.unlink(&path).await.expect("unlink");
            }
        });
    });
}

criterion_group!(
    benches,
    bench_compression,
    bench_slice_resolution,
    bench_vfs_io_and_meta
);
criterion_main!(benches);

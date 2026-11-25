# juicefs-rs `v0.2.0`

A pure-Rust port of the [JuiceFS](https://github.com/juicedata/juicefs) distributed POSIX filesystem architecture — zero Go runtime, zero C/`libfuse` build dependencies, and native support for both macOS and Linux.

## Workspace Crates (11 Crates)

| Crate | Purpose |
|---|---|
| [`juicefs-core`](crates/juicefs-core) | POSIX types (`Ino`, `Attr`, `NodeType`, `Format`, `TierConfig`), `CompressionAlgo` (`none`, `lz4`, `zstd`), 24-byte big-endian `SliceRecord` encoding, `build_slice` / `compact_chunk` interval algebra |
| [`juicefs-object`](crates/juicefs-object) | Async `ObjectStorage` trait with `MemoryStore`, `FileStore` (atomic & `inplace`), `S3Store` (`s3`/`oss`/`cos`/`minio`/`b2`/`obs`/`bos`/`tos`), `GcsStore` (`gs://`), `AzureStore` (`azblob://`/`wasb://`), `PrefixedStore`, `EncryptedStore` (AES-256-GCM), `ChunkedEncryptedStore` (64 KiB AEAD), `ShardedStore` (FNV-1a), multipart uploads, and `restore` |
| [`juicefs-chunk`](crates/juicefs-chunk) | `CachedChunkStore` with `moka` byte-weighted `MemoryBlockCache`, `DiskBlockCache` (`TwoRandom`/`Lru`), `SingleFlight` deduplication, `Prefetcher`, `TokenBucket` rate limiting, and async `WritebackQueue` |
| [`juicefs-meta`](crates/juicefs-meta) | Async `Meta` trait, `SqliteMeta` (`rusqlite` WAL) & feature-gated `RedisMeta` (`redis://`), POSIX tree ops, `flock`/`plock`, directory & user/group quotas, `.trash` soft-delete/restore, `dirStats`, `changelog`, `session` heartbeats, `clone_entry`, `dump_json`/`load_json`, `fsck`, and atomic `compact_chunk` |
| [`juicefs-vfs`](crates/juicefs-vfs) | High-level `Vfs` with multi-chunk I/O, sparse holes, POSIX `access`/ACLs, `.stats`/`.config` virtual files, `compact_file`, symlink loop guard, `summary`, `warmup`, `set_tier`, and `gc_orphans` |
| [`juicefs-fuse`](crates/juicefs-fuse) | Pure-Rust `JuiceFsFuse` POSIX FUSE adapter, `HandleTable`, `MountConfig`, and `unmount()` |
| [`juicefs-sync`](crates/juicefs-sync) | Concurrent `Semaphore`-bounded `sync_objects` engine (`--threads`, `--update`, `--delete-dst`, `--dry-run`) |
| [`juicefs-gateway`](crates/juicefs-gateway) | S3-compatible HTTP gateway (`GatewayServer`) over `Vfs` (`ListObjectsV2` XML, `GET` with `Range`, `HEAD`, `PUT`, `DELETE`) |
| [`juicefs-webdav`](crates/juicefs-webdav) | WebDAV HTTP server (`WebDavServer`) over `Vfs` (`OPTIONS`, `PROPFIND`, `GET`, `HEAD`, `PUT`, `MKCOL`, `DELETE`, `MOVE`, `COPY`, `/metrics`) |
| [`juicefs-metric`](crates/juicefs-metric) | Lock-free atomic `Metrics` counters and Prometheus/OpenMetrics text exporter |
| [`juicefs-cli`](crates/juicefs-cli) | `juicefs` CLI binary (`format`, `status`, `config`, `destroy`, `ls`, `mkdir`, `put`, `get`, `cat`, `stat`, `info`, `summary`, `rm`, `mv`, `clone`, `restore`, `warmup`, `compact`, `gc`, `dump`, `load`, `fsck`, `quota`, `tier`, `changelog`, `sessions`, `bench`, `objbench`, `sync`, `gateway`, `webdav`, `umount`) |

## Quickstart

```bash
# Build release binary
cargo build --release -p juicefs-cli

# Format a volume (SQLite metadata + local file storage + zstd compression)
./target/release/juicefs format sqlite3:///tmp/jfs.db myvol \
  --storage file --bucket /tmp/jfs-data --compress zstd

# Basic file operations
./target/release/juicefs mkdir -p sqlite3:///tmp/jfs.db /docs
./target/release/juicefs put sqlite3:///tmp/jfs.db ./Cargo.toml /docs/Cargo.toml
./target/release/juicefs ls sqlite3:///tmp/jfs.db /docs
./target/release/juicefs cat sqlite3:///tmp/jfs.db /docs/Cargo.toml

# Start S3 Gateway or WebDAV server
./target/release/juicefs gateway sqlite3:///tmp/jfs.db --listen 127.0.0.1:9000
./target/release/juicefs webdav sqlite3:///tmp/jfs.db --listen 127.0.0.1:9007
```

## Benchmarks

Measured on Apple Silicon (macOS, `--release` profile) against `/opt/homebrew/bin/juicefs` (`1.4.2+2026-10-22.46af7afa9f81`).

### Head-to-Head vs `juicefs-go` (`./scripts/bench_compare.sh`)

| Operation | `juicefs-rs` | `juicefs-go` | Notes |
|---|---:|---:|---|
| `format` (`sqlite3` + `file` + `lz4`) | **0.038 s** | 0.085 s | **2.2× faster** (creates bucket + single-tx WAL schema/init) |
| `status` | **0.036 s** | 0.075 s | **2.1× faster** |
| `fsck` | **0.036 s** | 0.078 s | **2.2× faster** (metadata consistency check on empty volume) |
| `sync` (`100 × 128 KiB`, `--threads 8`) | **0.046 s** | 0.081 s | **1.8× faster** |
| `vfs put 16 MiB` | **0.070 s** (~229 MiB/s) | — | End-to-end CLI upload |
| `vfs get 16 MiB` | **0.052 s** (~308 MiB/s) | — | End-to-end CLI download |

### Criterion Microbenchmarks (`cargo bench -p juicefs-vfs --bench vfs_bench`)

| Benchmark | Mean Time | Throughput |
|---|---:|---:|
| `compression_1mb/compress/lz4` | 58.96 µs | 16.57 GiB/s |
| `compression_1mb/decompress/lz4` | 30.23 µs | 32.31 GiB/s |
| `compression_1mb/compress/zstd` | 107.75 µs | 9.06 GiB/s |
| `compression_1mb/decompress/zstd` | 35.40 µs | 27.59 GiB/s |
| `build_slice_100_overlapping` | 7.76 µs | — |
| `vfs_io/write_64k_file_lz4` (`writeback: false`) | 4.05 ms | 15.42 MiB/s |
| `vfs_io/read_64k_file_cached` (`moka`) | 63.20 µs | **988.90 MiB/s** |
| `meta_create_and_unlink` | 185.67 µs | — |

## Testing & Verification

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo llvm-cov --workspace --summary-only
```

- **93 tests** passing across all 11 workspace crates (`0` failures, `0` clippy warnings).
- **82.96% line coverage** (`cargo-llvm-cov`) with automated CI in [.github/workflows/ci.yml](.github/workflows/ci.yml).

# juicefs-rs

A Rust port of the core [JuiceFS](https://github.com/juicedata/juicefs) distributed POSIX filesystem architecture.

## Workspace Crates

- `crates/juicefs-core` — Core POSIX domain types (`Ino`, `Attr`, `NodeType`, `Format`), `CompressionAlgo` (`none`, `lz4`, `zstd`), 24-byte big-endian `SliceRecord` wire encoding, and `build_slice` / `compact_chunk` interval algebra.
- `crates/juicefs-object` — Async `ObjectStorage` trait with `MemoryStore`, `FileStore`, `S3Store` (`object_store`), `PrefixedStore`, `EncryptedStore` (AES-256-GCM via `ring`), `ShardedStore` (FNV-1a), and multipart uploads.
- `crates/juicefs-chunk` — `CachedChunkStore` with `SingleFlight` deduplication, `Prefetcher`, `MemoryBlockCache`, `DiskBlockCache` (`TwoRandom` / `Lru` eviction), and `TokenBucket` rate limiting.
- `crates/juicefs-meta` — Async `Meta` trait and `SqliteMeta` (`rusqlite` WAL) with POSIX tree ops, `flock`/`plock`, directory quotas, `.trash` soft-delete/restore, `dump_json`/`load_json`, `fsck`, and atomic `compact_chunk`.
- `crates/juicefs-vfs` — High-level `Vfs` with multi-chunk I/O, sparse holes, POSIX `access`/ACLs, `.stats`/`.config` virtual files, `compact_file`, and symlink-aware path resolution.
- `crates/juicefs-sync` — Concurrent `Semaphore`-bounded `sync_objects` engine.
- `crates/juicefs-gateway` — S3-compatible HTTP gateway (`GatewayServer`) backed by `Vfs`.
- `crates/juicefs-cli` — `juicefs` CLI binary (`format`, `status`, `ls`, `mkdir`, `put`, `get`, `cat`, `stat`, `rm`, `mv`, `compact`, `gc`, `dump`, `load`, `fsck`, `quota`, `sync`, `gateway`).

## Verification

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

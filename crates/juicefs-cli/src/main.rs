//! `juicefs` CLI binary for formatting, inspecting, and managing `JuiceFS` volumes.

use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use juicefs_core::{ChunkConfig, ClientConfig, Format, NodeType};
use juicefs_gateway::GatewayServer;
use juicefs_meta::{Meta, open_meta};
use juicefs_object::{FileStore, MemoryStore, ObjectStorage, S3Store};
use juicefs_sync::{SyncConfig, sync_objects};
use juicefs_vfs::Vfs;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "juicefs", version, about = "Rust implementation of JuiceFS")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Format a new `JuiceFS` volume
    Format {
        /// Metadata URL (e.g. `<sqlite3:///tmp/jfs.db>`)
        meta_url: String,
        /// Volume name
        name: String,
        /// Object storage backend (file, s3, mem)
        #[arg(long, default_value = "file")]
        storage: String,
        /// Bucket URL or local directory path
        #[arg(long, default_value = "/tmp/jfs-data")]
        bucket: String,
        /// Block size in KiB
        #[arg(long, default_value_t = 4096)]
        block_size: usize,
        /// Compression algorithm (none, lz4, zstd)
        #[arg(long, default_value = "none")]
        compress: String,
        /// Enable hash prefix for object keys
        #[arg(long)]
        hash_prefix: bool,
        /// Overwrite existing volume metadata
        #[arg(long)]
        force: bool,
    },
    /// Show volume format and capacity statistics
    Status {
        /// Metadata URL
        meta_url: String,
    },
    /// List directory contents
    Ls {
        /// Metadata URL
        meta_url: String,
        /// Path inside `JuiceFS`
        #[arg(default_value = "/")]
        path: String,
    },
    /// Create a directory
    Mkdir {
        /// Metadata URL
        meta_url: String,
        /// Directory path inside `JuiceFS`
        path: String,
        /// Create parent directories as needed
        #[arg(short, long)]
        parents: bool,
    },
    /// Upload a local file into `JuiceFS`
    Put {
        /// Metadata URL
        meta_url: String,
        /// Local source file path
        src: PathBuf,
        /// Destination path inside `JuiceFS`
        dst: String,
    },
    /// Download a file from `JuiceFS` to local disk
    Get {
        /// Metadata URL
        meta_url: String,
        /// Source path inside `JuiceFS`
        src: String,
        /// Local destination file path
        dst: PathBuf,
    },
    /// Print file contents to stdout
    Cat {
        /// Metadata URL
        meta_url: String,
        /// Path inside `JuiceFS`
        path: String,
    },
    /// Show inode and attribute details for a path
    Stat {
        /// Metadata URL
        meta_url: String,
        /// Path inside `JuiceFS`
        path: String,
    },
    /// Remove a file or directory
    Rm {
        /// Metadata URL
        meta_url: String,
        /// Path inside `JuiceFS`
        path: String,
        /// Remove directories and their contents recursively
        #[arg(short, long)]
        recursive: bool,
    },
    /// Move or rename a file or directory
    Mv {
        /// Metadata URL
        meta_url: String,
        /// Source path
        src: String,
        /// Destination path
        dst: String,
    },
    /// Compact fragmented chunks of a file
    Compact {
        /// Metadata URL
        meta_url: String,
        /// File path inside `JuiceFS`
        path: String,
    },
    /// Garbage-collect unreferenced slices from object storage
    Gc {
        /// Metadata URL
        meta_url: String,
    },
    /// Dump volume metadata as JSON
    Dump {
        /// Metadata URL
        meta_url: String,
        /// Optional output file path (prints to stdout if omitted)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// Load volume metadata from a JSON dump file
    Load {
        /// Metadata URL
        meta_url: String,
        /// Input JSON dump file path
        input: PathBuf,
    },
    /// Check metadata consistency
    Fsck {
        /// Metadata URL
        meta_url: String,
    },
    /// Manage directory quotas
    Quota {
        #[command(subcommand)]
        action: QuotaAction,
    },
    /// Synchronize objects between two storage endpoints (`file://`, `mem://`, `s3://`)
    Sync {
        /// Source URI
        src: String,
        /// Destination URI
        dst: String,
        /// Concurrent worker count
        #[arg(long, default_value_t = 10)]
        threads: usize,
        /// Delete destination keys missing from source
        #[arg(long)]
        delete_dst: bool,
        /// Skip keys when destination has same size and newer/equal mtime
        #[arg(long)]
        update: bool,
        /// Dry-run without modifying destination
        #[arg(long)]
        dry_run: bool,
    },
    /// Start an S3-compatible HTTP gateway backed by a `JuiceFS` volume
    Gateway {
        /// Metadata URL
        meta_url: String,
        /// Socket address to listen on
        #[arg(long, default_value = "127.0.0.1:9000")]
        listen: SocketAddr,
    },
}

#[derive(Debug, Subcommand)]
enum QuotaAction {
    /// Set space/inode quota on a directory
    Set {
        /// Metadata URL
        meta_url: String,
        /// Directory path inside `JuiceFS`
        path: String,
        /// Maximum space in bytes (`0` = unlimited)
        #[arg(long, default_value_t = 0)]
        capacity: i64,
        /// Maximum inodes (`0` = unlimited)
        #[arg(long, default_value_t = 0)]
        inodes: i64,
    },
    /// Get quota on a directory
    Get {
        /// Metadata URL
        meta_url: String,
        /// Directory path inside `JuiceFS`
        path: String,
    },
    /// Delete quota on a directory
    Del {
        /// Metadata URL
        meta_url: String,
        /// Directory path inside `JuiceFS`
        path: String,
    },
    /// List all directory quotas
    List {
        /// Metadata URL
        meta_url: String,
    },
}

async fn connect_vfs(meta_url: &str) -> Result<Vfs> {
    let meta = Arc::new(open_meta(meta_url).context("opening metadata store")?);
    Vfs::connect(meta, ChunkConfig::default(), ClientConfig::default())
        .await
        .context("connecting to JuiceFS volume")
}

fn open_sync_store(uri: &str) -> Result<Arc<dyn ObjectStorage>> {
    if uri == "mem://" {
        return Ok(Arc::new(MemoryStore::new()));
    }
    if uri.starts_with("s3://") {
        let fmt = Format::new("", "s3", uri);
        return Ok(Arc::new(S3Store::from_format(&fmt)?));
    }
    let path = uri.strip_prefix("file://").unwrap_or(uri);
    Ok(Arc::new(FileStore::new(path)?))
}

async fn cmd_format(meta_url: &str, format: &Format, force: bool) -> Result<()> {
    let meta = open_meta(meta_url).context("opening metadata store")?;
    meta.init(format, force)
        .await
        .context("formatting volume")?;
    println!("Formatted volume '{}' (UUID: {})", format.name, format.uuid);
    Ok(())
}

async fn cmd_status(meta_url: &str) -> Result<()> {
    let vfs = connect_vfs(meta_url).await?;
    let st = vfs.statfs().await?;
    let fmt = vfs.format();
    println!("Volume:      {} ({})", fmt.name, fmt.uuid);
    println!("Storage:     {} -> {}", fmt.storage, fmt.bucket);
    println!("BlockSize:   {} bytes", fmt.block_size);
    println!("Compression: {}", fmt.compression);
    println!("Used Inodes: {}", st.used_inodes);
    println!(
        "Used Space:  {} / {} bytes",
        st.total_space - st.avail_space,
        st.total_space
    );
    Ok(())
}

async fn cmd_ls(meta_url: &str, path: &str) -> Result<()> {
    let vfs = connect_vfs(meta_url).await?;
    for entry in vfs.readdir(path).await? {
        let kind = match entry.attr.typ {
            NodeType::Directory => 'd',
            NodeType::Symlink => 'l',
            _ => '-',
        };
        println!(
            "{kind}{:03o} {:>10} {:>6} {}",
            entry.attr.mode, entry.attr.length, entry.inode, entry.name
        );
    }
    Ok(())
}

async fn cmd_quota(action: QuotaAction) -> Result<()> {
    match action {
        QuotaAction::Set {
            meta_url,
            path,
            capacity,
            inodes,
        } => {
            let vfs = connect_vfs(&meta_url).await?;
            let (ino, _) = vfs.resolve_path(&path).await?;
            vfs.meta().set_quota(ino, capacity, inodes).await?;
            println!("Quota set on {path} (inode {ino})");
        }
        QuotaAction::Get { meta_url, path } => {
            let vfs = connect_vfs(&meta_url).await?;
            let (ino, _) = vfs.resolve_path(&path).await?;
            let q = vfs.meta().get_quota(ino).await?;
            println!(
                "Inode {ino}: space={}/{} inodes={}/{}",
                q.used_space, q.max_space, q.used_inodes, q.max_inodes
            );
        }
        QuotaAction::Del { meta_url, path } => {
            let vfs = connect_vfs(&meta_url).await?;
            let (ino, _) = vfs.resolve_path(&path).await?;
            vfs.meta().del_quota(ino).await?;
            println!("Quota deleted on {path} (inode {ino})");
        }
        QuotaAction::List { meta_url } => {
            let meta = open_meta(&meta_url).context("opening metadata store")?;
            for (ino, q) in meta.list_quotas().await? {
                println!(
                    "Inode {ino}: space={}/{} inodes={}/{}",
                    q.used_space, q.max_space, q.used_inodes, q.max_inodes
                );
            }
        }
    }
    Ok(())
}

async fn dispatch_io(command: Commands) -> Result<()> {
    match command {
        Commands::Put { meta_url, src, dst } => {
            let vfs = connect_vfs(&meta_url).await?;
            let data = tokio::fs::read(&src)
                .await
                .with_context(|| format!("reading {}", src.display()))?;
            let attr = vfs.write_file(&dst, &data).await?;
            println!("Uploaded {} bytes to {dst}", attr.length);
        }
        Commands::Get { meta_url, src, dst } => {
            let vfs = connect_vfs(&meta_url).await?;
            let data = vfs.read_file(&src).await?;
            tokio::fs::write(&dst, &data)
                .await
                .with_context(|| format!("writing {}", dst.display()))?;
            println!("Downloaded {} bytes to {}", data.len(), dst.display());
        }
        Commands::Cat { meta_url, path } => {
            let vfs = connect_vfs(&meta_url).await?;
            let data = vfs.read_file(&path).await?;
            std::io::stdout().write_all(&data)?;
        }
        Commands::Stat { meta_url, path } => {
            let vfs = connect_vfs(&meta_url).await?;
            let (ino, attr) = vfs.resolve_path(&path).await?;
            println!("Path:   {path}");
            println!("Inode:  {ino}");
            println!("Type:   {:?}", attr.typ);
            println!("Mode:   {:04o}", attr.mode);
            println!("Size:   {} bytes", attr.length);
            println!("Nlink:  {}", attr.nlink);
        }
        Commands::Rm {
            meta_url,
            path,
            recursive,
        } => {
            let vfs = connect_vfs(&meta_url).await?;
            if recursive {
                vfs.rm_rf(&path).await?;
            } else {
                let (_, attr) = vfs.resolve_path(&path).await?;
                if attr.is_dir() {
                    vfs.rmdir(&path).await?;
                } else {
                    vfs.unlink(&path).await?;
                }
            }
        }
        Commands::Mv { meta_url, src, dst } => {
            let vfs = connect_vfs(&meta_url).await?;
            vfs.rename(&src, &dst).await?;
        }
        Commands::Compact { meta_url, path } => {
            let vfs = connect_vfs(&meta_url).await?;
            let compacted = vfs.compact_file(&path).await?;
            println!("Compacted {compacted} chunk(s) for {path}");
        }
        Commands::Gc { meta_url } => {
            let vfs = connect_vfs(&meta_url).await?;
            let removed = vfs.gc_orphans().await?;
            println!("Garbage collected {removed} orphan slice(s)");
        }
        _ => unreachable!(),
    }
    Ok(())
}

async fn dispatch(command: Commands) -> Result<()> {
    match command {
        Commands::Format {
            meta_url,
            name,
            storage,
            bucket,
            block_size,
            compress,
            hash_prefix,
            force,
        } => {
            let mut fmt = Format::new(&name, &storage, &bucket);
            fmt.block_size = block_size * 1024;
            fmt.compression = compress;
            fmt.hash_prefix = hash_prefix;
            cmd_format(&meta_url, &fmt, force).await?;
        }
        Commands::Status { meta_url } => cmd_status(&meta_url).await?,
        Commands::Ls { meta_url, path } => cmd_ls(&meta_url, &path).await?,
        Commands::Mkdir {
            meta_url,
            path,
            parents,
        } => {
            let vfs = connect_vfs(&meta_url).await?;
            if parents {
                vfs.mkdir_p(&path, 0o755).await?;
            } else {
                vfs.mkdir(&path, 0o755).await?;
            }
        }
        Commands::Dump { meta_url, output } => {
            let meta = open_meta(&meta_url).context("opening metadata store")?;
            let json = meta.dump_json().await?;
            match output {
                Some(path) => tokio::fs::write(&path, json).await?,
                None => println!("{json}"),
            }
        }
        Commands::Load { meta_url, input } => {
            let meta = open_meta(&meta_url).context("opening metadata store")?;
            let json = tokio::fs::read_to_string(&input).await?;
            meta.load_json(&json).await?;
            println!("Loaded metadata from {}", input.display());
        }
        Commands::Fsck { meta_url } => {
            let meta = open_meta(&meta_url).context("opening metadata store")?;
            let report = meta.fsck().await?;
            println!(
                "Fsck complete: orphaned_edges={}, missing_nodes={}, unreferenced_nodes={}, nlink_mismatches={}",
                report.orphaned_edges.len(),
                report.missing_nodes.len(),
                report.unreferenced_nodes.len(),
                report.nlink_mismatches.len()
            );
            anyhow::ensure!(report.is_clean(), "metadata inconsistencies detected");
        }
        Commands::Quota { action } => cmd_quota(action).await?,
        Commands::Sync {
            src,
            dst,
            threads,
            delete_dst,
            update,
            dry_run,
        } => {
            let src_store = open_sync_store(&src)?;
            let dst_store = open_sync_store(&dst)?;
            let stats = sync_objects(
                src_store,
                dst_store,
                SyncConfig {
                    concurrency: threads,
                    delete_dst,
                    update,
                    dry_run,
                    prefix: String::new(),
                },
            )
            .await?;
            println!(
                "Sync complete: copied={}, skipped={}, deleted={}, failed={}, bytes={}",
                stats.copied, stats.skipped, stats.deleted, stats.failed, stats.bytes_copied
            );
        }
        Commands::Gateway { meta_url, listen } => {
            let vfs = connect_vfs(&meta_url).await?;
            let server = GatewayServer::bind(listen, vfs).await?;
            println!("S3 Gateway listening on http://{}", server.local_addr());
            tokio::signal::ctrl_c().await?;
            server.shutdown().await;
        }
        other => dispatch_io(other).await?,
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    dispatch(cli.command).await
}

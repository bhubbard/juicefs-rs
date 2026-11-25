//! Lightweight S3-compatible HTTP gateway backed by [`juicefs_vfs::Vfs`].
//!
//! Supports:
//! - `GET /<bucket>?list-type=2&prefix=...` (`ListObjectsV2` XML)
//! - `GET /<bucket>/<key>` (with optional HTTP `Range` support)
//! - `HEAD /<bucket>/<key>`
//! - `PUT /<bucket>/<key>`
//! - `DELETE /<bucket>/<key>`

pub mod error;
pub mod s3_xml;

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http::header::{
    ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, LAST_MODIFIED, RANGE,
};
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use juicefs_core::{Attr, Ino};
use juicefs_meta::MetaError;
use juicefs_vfs::{Vfs, VfsError};
use md5::{Digest, Md5};
use percent_encoding::percent_decode_str;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

pub use error::GatewayError;
use s3_xml::{ListObjectsV2Result, ObjectEntry, render_error, render_list_objects_v2};

const XATTR_ETAG: &str = "user.s3.etag";
const XATTR_CONTENT_TYPE: &str = "user.s3.content_type";
const DEFAULT_MAX_KEYS: usize = 1000;

type BoxBody = Full<Bytes>;

/// Handle used to gracefully stop a running [`GatewayServer`].
#[derive(Debug)]
pub struct ShutdownHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl ShutdownHandle {
    /// Signals the gateway server to stop accepting connections and awaits task completion.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ShutdownHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Running S3-compatible HTTP gateway bound to a local TCP socket.
#[derive(Debug)]
pub struct GatewayServer {
    local_addr: SocketAddr,
    handle: ShutdownHandle,
}

impl GatewayServer {
    /// Binds an S3 HTTP gateway to `addr` backed by `vfs`.
    pub async fn bind(
        addr: impl ToSocketAddrs,
        vfs: impl Into<Arc<Vfs>>,
    ) -> Result<Self, GatewayError> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let vfs = vfs.into();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        let task = tokio::spawn(async move {
            let builder = auto::Builder::new(TokioExecutor::new());
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => {
                        debug!(%local_addr, "gateway shutting down");
                        break;
                    }
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((stream, peer)) => {
                                let vfs = Arc::clone(&vfs);
                                let builder = builder.clone();
                                tokio::spawn(async move {
                                    let io = TokioIo::new(stream);
                                    let svc = service_fn(move |req| {
                                        let vfs = Arc::clone(&vfs);
                                        async move {
                                            Ok::<_, Infallible>(handle_request(vfs, req).await)
                                        }
                                    });
                                    if let Err(err) = builder.serve_connection(io, svc).await {
                                        debug!(%peer, error = %err, "connection closed with error");
                                    }
                                });
                            }
                            Err(err) => {
                                warn!(error = %err, "failed to accept connection");
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            local_addr,
            handle: ShutdownHandle {
                shutdown_tx: Some(shutdown_tx),
                task: Some(task),
            },
        })
    }

    /// Returns the local socket address the server is bound to.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Splits this server into its bound [`SocketAddr`] and [`ShutdownHandle`].
    #[must_use]
    pub fn into_parts(self) -> (SocketAddr, ShutdownHandle) {
        (self.local_addr, self.handle)
    }

    /// Gracefully shuts down the server and waits for the accept loop to exit.
    pub async fn shutdown(self) {
        self.handle.shutdown().await;
    }
}

enum RouteTarget {
    Root,
    Bucket { bucket: String },
    Object { bucket: String, key: String },
}

fn parse_route(raw_path: &str) -> Result<RouteTarget, String> {
    let decoded = percent_decode_str(raw_path)
        .decode_utf8()
        .map_err(|_| "invalid UTF-8 in URI path".to_owned())?;
    let trimmed = decoded.trim_start_matches('/');
    if trimmed.is_empty() {
        return Ok(RouteTarget::Root);
    }

    if let Some((bucket, key)) = trimmed.split_once('/') {
        validate_segment(bucket, "bucket")?;
        if key.is_empty() {
            return Ok(RouteTarget::Bucket {
                bucket: bucket.to_owned(),
            });
        }
        validate_key(key)?;
        Ok(RouteTarget::Object {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
        })
    } else {
        validate_segment(trimmed, "bucket")?;
        Ok(RouteTarget::Bucket {
            bucket: trimmed.to_owned(),
        })
    }
}

fn validate_segment(seg: &str, kind: &str) -> Result<(), String> {
    if seg.is_empty() || seg == "." || seg == ".." || seg.contains('\0') {
        return Err(format!("invalid {kind} name: {seg:?}"));
    }
    Ok(())
}

fn validate_key(key: &str) -> Result<(), String> {
    if key.contains('\0') {
        return Err("key contains NUL byte".to_owned());
    }
    for part in key.split('/') {
        if part == ".." {
            return Err("path traversal is not allowed in object key".to_owned());
        }
    }
    Ok(())
}

fn object_vfs_path(bucket: &str, key: &str) -> String {
    let normalized = key.trim_start_matches('/');
    format!("/{bucket}/{normalized}")
}

fn parent_vfs_path(full_path: &str) -> &str {
    match full_path.rfind('/') {
        Some(0) | None => "/",
        Some(idx) => &full_path[..idx],
    }
}

async fn handle_request(vfs: Arc<Vfs>, req: Request<Incoming>) -> Response<BoxBody> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let raw_path = uri.path().to_owned();

    let target = match parse_route(&raw_path) {
        Ok(t) => t,
        Err(msg) => {
            return xml_error_response(
                StatusCode::BAD_REQUEST,
                "InvalidURI",
                &msg,
                &raw_path,
                method == Method::HEAD,
            );
        }
    };

    match (method.clone(), target) {
        (Method::GET, RouteTarget::Bucket { bucket }) => {
            handle_list_objects_v2(&vfs, &bucket, uri.query().unwrap_or(""), &raw_path).await
        }
        (Method::PUT, RouteTarget::Bucket { bucket }) => {
            handle_create_bucket(&vfs, &bucket, &raw_path).await
        }
        (Method::HEAD, RouteTarget::Bucket { bucket }) => handle_head_bucket(&vfs, &bucket).await,
        (Method::GET, RouteTarget::Object { bucket, key }) => {
            let range = req
                .headers()
                .get(RANGE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            handle_get_object(&vfs, &bucket, &key, range.as_deref(), &raw_path).await
        }
        (Method::HEAD, RouteTarget::Object { bucket, key }) => {
            handle_head_object(&vfs, &bucket, &key).await
        }
        (Method::PUT, RouteTarget::Object { bucket, key }) => {
            let content_type = req
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let body = match req.into_body().collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(err) => {
                    return xml_error_response(
                        StatusCode::BAD_REQUEST,
                        "IncompleteBody",
                        &format!("failed to read request body: {err}"),
                        &raw_path,
                        false,
                    );
                }
            };
            handle_put_object(
                &vfs,
                &bucket,
                &key,
                body,
                content_type.as_deref(),
                &raw_path,
            )
            .await
        }
        (Method::DELETE, RouteTarget::Object { bucket, key }) => {
            handle_delete_object(&vfs, &bucket, &key, &raw_path).await
        }
        (Method::GET, RouteTarget::Root) => xml_error_response(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "bucket name is required",
            &raw_path,
            false,
        ),
        _ => xml_error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "the specified method is not allowed against this resource",
            &raw_path,
            method == Method::HEAD,
        ),
    }
}

async fn handle_create_bucket(vfs: &Vfs, bucket: &str, resource: &str) -> Response<BoxBody> {
    let bucket_path = format!("/{bucket}");
    match vfs.mkdir_p(&bucket_path, 0o755).await {
        Ok(_) => empty_response(StatusCode::OK),
        Err(err) => vfs_error_response(&err, resource, false),
    }
}

async fn handle_head_bucket(vfs: &Vfs, bucket: &str) -> Response<BoxBody> {
    let bucket_path = format!("/{bucket}");
    match vfs.resolve_path(&bucket_path).await {
        Ok((_, attr)) if attr.is_dir() => empty_response(StatusCode::OK),
        Ok(_) | Err(VfsError::Meta(MetaError::NotFound(_))) => {
            empty_response(StatusCode::NOT_FOUND)
        }
        Err(_) => empty_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

struct ListQuery {
    prefix: String,
    delimiter: Option<String>,
    continuation_token: Option<String>,
    start_after: Option<String>,
    max_keys: usize,
}

fn parse_list_query(query: &str) -> Result<ListQuery, String> {
    let mut prefix = String::new();
    let mut delimiter = None;
    let mut continuation_token = None;
    let mut start_after = None;
    let mut max_keys = DEFAULT_MAX_KEYS;

    for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
        match k.as_ref() {
            "list-type" => {
                if v != "2" {
                    return Err(format!("unsupported list-type: {v}"));
                }
            }
            "prefix" => prefix = v.into_owned(),
            "delimiter" => {
                if !v.is_empty() {
                    delimiter = Some(v.into_owned());
                }
            }
            "continuation-token" => {
                if !v.is_empty() {
                    continuation_token = Some(v.into_owned());
                }
            }
            "start-after" => {
                if !v.is_empty() {
                    start_after = Some(v.into_owned());
                }
            }
            "max-keys" => {
                max_keys = v
                    .parse::<usize>()
                    .map_err(|_| format!("invalid max-keys: {v}"))?;
            }
            _ => {}
        }
    }

    Ok(ListQuery {
        prefix,
        delimiter,
        continuation_token,
        start_after,
        max_keys,
    })
}

async fn handle_list_objects_v2(
    vfs: &Vfs,
    bucket: &str,
    query: &str,
    resource: &str,
) -> Response<BoxBody> {
    let params = match parse_list_query(query) {
        Ok(p) => p,
        Err(msg) => {
            return xml_error_response(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                &msg,
                resource,
                false,
            );
        }
    };

    let bucket_path = format!("/{bucket}");
    let collected = match collect_bucket_objects(vfs, &bucket_path, &params.prefix).await {
        Ok(items) => items,
        Err(err) => return vfs_error_response(&err, resource, false),
    };

    let marker = params
        .continuation_token
        .as_deref()
        .or(params.start_after.as_deref());

    let mut contents: Vec<ObjectEntry> = Vec::new();
    let mut common_prefixes_set: BTreeSet<String> = BTreeSet::new();
    let mut is_truncated = false;
    let mut last_key_or_prefix: Option<String> = None;

    for (key, ino, attr) in collected {
        if let Some(m) = marker
            && key.as_str() <= m
        {
            continue;
        }

        if let Some(delim) = params.delimiter.as_deref() {
            let suffix = &key[params.prefix.len()..];
            if let Some(pos) = suffix.find(delim) {
                let cp = format!("{}{}", params.prefix, &suffix[..=pos]);
                if common_prefixes_set.contains(&cp) {
                    continue;
                }
                if contents.len() + common_prefixes_set.len() >= params.max_keys {
                    is_truncated = true;
                    break;
                }
                last_key_or_prefix = Some(cp.clone());
                common_prefixes_set.insert(cp);
                continue;
            }
        }

        if contents.len() + common_prefixes_set.len() >= params.max_keys {
            is_truncated = true;
            break;
        }

        let etag = resolve_etag(vfs, ino, &attr).await;
        last_key_or_prefix = Some(key.clone());
        contents.push(ObjectEntry {
            key,
            last_modified: attr_mtime(&attr),
            etag,
            size: attr.length,
        });
    }

    let common_prefixes: Vec<String> = common_prefixes_set.into_iter().collect();
    let next_token = if is_truncated {
        last_key_or_prefix.as_deref()
    } else {
        None
    };

    let xml = render_list_objects_v2(&ListObjectsV2Result {
        bucket,
        prefix: &params.prefix,
        delimiter: params.delimiter.as_deref(),
        continuation_token: params.continuation_token.as_deref(),
        next_continuation_token: next_token,
        max_keys: params.max_keys,
        is_truncated,
        contents: &contents,
        common_prefixes: &common_prefixes,
    });

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .header(CONTENT_LENGTH, xml.len())
        .body(Full::new(Bytes::from(xml)))
        .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR))
}

async fn collect_bucket_objects(
    vfs: &Vfs,
    bucket_path: &str,
    prefix: &str,
) -> Result<Vec<(String, Ino, Attr)>, VfsError> {
    let (bucket_ino, bucket_attr) = match vfs.resolve_path(bucket_path).await {
        Ok(res) => res,
        Err(VfsError::Meta(MetaError::NotFound(_))) => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };

    if !bucket_attr.is_dir() {
        return Err(MetaError::NotDir(bucket_path.to_owned()).into());
    }

    let mut stack = vec![(bucket_ino, String::new())];
    let mut results = Vec::new();

    while let Some((dir_ino, rel_dir)) = stack.pop() {
        let entries = vfs.meta().readdir(dir_ino).await?;
        for entry in entries {
            if entry.name == "." || entry.name == ".." {
                continue;
            }
            let rel_key = if rel_dir.is_empty() {
                entry.name
            } else {
                format!("{rel_dir}/{}", entry.name)
            };
            if entry.attr.is_dir() {
                if prefix.is_empty()
                    || rel_key.starts_with(prefix)
                    || prefix.starts_with(&format!("{rel_key}/"))
                    || prefix == rel_key
                {
                    stack.push((entry.inode, rel_key));
                }
            } else if entry.attr.is_file() && rel_key.starts_with(prefix) {
                results.push((rel_key, entry.inode, entry.attr));
            }
        }
    }

    results.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(results)
}

async fn handle_head_object(vfs: &Vfs, bucket: &str, key: &str) -> Response<BoxBody> {
    let full_path = object_vfs_path(bucket, key);
    let (ino, attr) = match vfs.resolve_path(&full_path).await {
        Ok(res) => res,
        Err(VfsError::Meta(MetaError::NotFound(_))) => {
            return empty_response(StatusCode::NOT_FOUND);
        }
        Err(_) => return empty_response(StatusCode::INTERNAL_SERVER_ERROR),
    };

    if !attr.is_file() {
        return empty_response(StatusCode::NOT_FOUND);
    }

    let etag = resolve_etag(vfs, ino, &attr).await;
    let content_type = resolve_content_type(vfs, ino).await;
    let last_mod = format_http_date(attr_mtime(&attr));

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_LENGTH, attr.length)
        .header(CONTENT_TYPE, content_type)
        .header(ETAG, etag)
        .header(LAST_MODIFIED, last_mod)
        .header(ACCEPT_RANGES, "bytes")
        .body(Full::new(Bytes::new()))
        .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR))
}

async fn handle_get_object(
    vfs: &Vfs,
    bucket: &str,
    key: &str,
    range_header: Option<&str>,
    resource: &str,
) -> Response<BoxBody> {
    let full_path = object_vfs_path(bucket, key);
    let (ino, attr) = match vfs.resolve_path(&full_path).await {
        Ok(res) => res,
        Err(VfsError::Meta(MetaError::NotFound(_))) => {
            return xml_error_response(
                StatusCode::NOT_FOUND,
                "NoSuchKey",
                "the specified key does not exist",
                resource,
                false,
            );
        }
        Err(err) => return vfs_error_response(&err, resource, false),
    };

    if !attr.is_file() {
        return xml_error_response(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "the specified key does not exist",
            resource,
            false,
        );
    }

    let etag = resolve_etag(vfs, ino, &attr).await;
    let content_type = resolve_content_type(vfs, ino).await;
    let last_mod = format_http_date(attr_mtime(&attr));

    if let Some(range_spec) = range_header {
        match parse_byte_range(range_spec, attr.length) {
            Ok((start, end)) => {
                let len = (end - start + 1) as usize;
                let mut buf = vec![0u8; len];
                let n = match vfs.read_at(ino, start, &mut buf).await {
                    Ok(n) => n,
                    Err(err) => return vfs_error_response(&err, resource, false),
                };
                buf.truncate(n);
                let content_range = format!("bytes {start}-{end}/{}", attr.length);
                return Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(CONTENT_LENGTH, buf.len())
                    .header(CONTENT_RANGE, content_range)
                    .header(CONTENT_TYPE, content_type)
                    .header(ETAG, etag)
                    .header(LAST_MODIFIED, last_mod)
                    .header(ACCEPT_RANGES, "bytes")
                    .body(Full::new(Bytes::from(buf)))
                    .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR));
            }
            Err(()) => {
                return xml_error_response(
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "InvalidRange",
                    "the requested range is not satisfiable",
                    resource,
                    false,
                );
            }
        }
    }

    let mut buf = vec![0u8; attr.length as usize];
    if !buf.is_empty() {
        let n = match vfs.read_at(ino, 0, &mut buf).await {
            Ok(n) => n,
            Err(err) => return vfs_error_response(&err, resource, false),
        };
        buf.truncate(n);
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_LENGTH, buf.len())
        .header(CONTENT_TYPE, content_type)
        .header(ETAG, etag)
        .header(LAST_MODIFIED, last_mod)
        .header(ACCEPT_RANGES, "bytes")
        .body(Full::new(Bytes::from(buf)))
        .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR))
}

async fn handle_put_object(
    vfs: &Vfs,
    bucket: &str,
    key: &str,
    body: Bytes,
    content_type: Option<&str>,
    resource: &str,
) -> Response<BoxBody> {
    let full_path = object_vfs_path(bucket, key);
    let parent = parent_vfs_path(&full_path);

    if let Err(err) = vfs.mkdir_p(parent, 0o755).await {
        return vfs_error_response(&err, resource, false);
    }

    let etag = compute_etag(&body);
    if let Err(err) = vfs.write_file(&full_path, &body).await {
        return vfs_error_response(&err, resource, false);
    }

    if let Ok((ino, _)) = vfs.resolve_path(&full_path).await {
        let _ = vfs.meta().set_xattr(ino, XATTR_ETAG, etag.as_bytes()).await;
        if let Some(ct) = content_type {
            let _ = vfs
                .meta()
                .set_xattr(ino, XATTR_CONTENT_TYPE, ct.as_bytes())
                .await;
        }
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(ETAG, etag)
        .header(CONTENT_LENGTH, 0)
        .body(Full::new(Bytes::new()))
        .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR))
}

async fn handle_delete_object(
    vfs: &Vfs,
    bucket: &str,
    key: &str,
    resource: &str,
) -> Response<BoxBody> {
    let full_path = object_vfs_path(bucket, key);
    match vfs.unlink(&full_path).await {
        Ok(()) => {
            prune_empty_parents(vfs, bucket, parent_vfs_path(&full_path)).await;
            empty_response(StatusCode::NO_CONTENT)
        }
        Err(VfsError::Meta(MetaError::NotFound(_) | MetaError::IsDir(_))) => {
            empty_response(StatusCode::NO_CONTENT)
        }
        Err(err) => vfs_error_response(&err, resource, false),
    }
}

async fn prune_empty_parents(vfs: &Vfs, bucket: &str, start_dir: &str) {
    let bucket_root = format!("/{bucket}");
    let mut cur = start_dir.to_owned();
    while cur != bucket_root && cur.starts_with(&format!("{bucket_root}/")) {
        if let Ok((ino, _)) = vfs.resolve_path(&cur).await
            && let Ok(entries) = vfs.meta().readdir(ino).await
            && entries.is_empty()
            && vfs.rmdir(&cur).await.is_ok()
        {
            cur = parent_vfs_path(&cur).to_owned();
            continue;
        }
        break;
    }
}

fn parse_byte_range(header: &str, total_len: u64) -> Result<(u64, u64), ()> {
    if total_len == 0 {
        return Err(());
    }
    let spec = header.strip_prefix("bytes=").ok_or(())?;
    if spec.contains(',') {
        return Err(());
    }
    let (start_str, end_str) = spec.split_once('-').ok_or(())?;
    if start_str.is_empty() {
        let suffix_len: u64 = end_str.parse().map_err(|_| ())?;
        if suffix_len == 0 {
            return Err(());
        }
        let start = total_len.saturating_sub(suffix_len);
        Ok((start, total_len - 1))
    } else {
        let start: u64 = start_str.parse().map_err(|_| ())?;
        if start >= total_len {
            return Err(());
        }
        let end = if end_str.is_empty() {
            total_len - 1
        } else {
            let parsed: u64 = end_str.parse().map_err(|_| ())?;
            if parsed < start {
                return Err(());
            }
            parsed.min(total_len - 1)
        };
        Ok((start, end))
    }
}

fn compute_etag(data: &[u8]) -> String {
    let digest = Md5::digest(data);
    let mut out = String::with_capacity(34);
    out.push('"');
    for b in digest {
        let _ = write!(out, "{b:02x}");
    }
    out.push('"');
    out
}

async fn resolve_etag(vfs: &Vfs, ino: Ino, attr: &Attr) -> String {
    if let Ok(raw) = vfs.meta().get_xattr(ino, XATTR_ETAG).await
        && let Ok(s) = String::from_utf8(raw)
        && !s.is_empty()
    {
        return s;
    }
    format!("\"{:016x}-{}\"", ino.as_u64(), attr.length)
}

async fn resolve_content_type(vfs: &Vfs, ino: Ino) -> String {
    if let Ok(raw) = vfs.meta().get_xattr(ino, XATTR_CONTENT_TYPE).await
        && let Ok(s) = String::from_utf8(raw)
        && !s.is_empty()
    {
        return s;
    }
    "application/octet-stream".to_owned()
}

fn attr_mtime(attr: &Attr) -> SystemTime {
    let Ok(secs) = u64::try_from(attr.mtime) else {
        return UNIX_EPOCH;
    };
    UNIX_EPOCH
        .checked_add(Duration::new(secs, attr.mtime_nsec))
        .unwrap_or(UNIX_EPOCH)
}

fn format_http_date(ts: SystemTime) -> String {
    const MAX_HTTP_DATE_SECS: u64 = 253_402_300_800; // 9999-12-31T23:59:59Z + 1s
    let clamped = match ts.duration_since(UNIX_EPOCH) {
        Ok(dur) if dur.as_secs() < MAX_HTTP_DATE_SECS => ts,
        Ok(_) => UNIX_EPOCH + Duration::from_secs(MAX_HTTP_DATE_SECS - 1),
        Err(_) => UNIX_EPOCH,
    };
    httpdate::fmt_http_date(clamped)
}

fn empty_response(status: StatusCode) -> Response<BoxBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_LENGTH, 0)
        .body(Full::new(Bytes::new()))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
}

fn xml_error_response(
    status: StatusCode,
    code: &str,
    message: &str,
    resource: &str,
    head_only: bool,
) -> Response<BoxBody> {
    if head_only {
        return empty_response(status);
    }
    let body = render_error(code, message, resource);
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/xml")
        .header(CONTENT_LENGTH, body.len())
        .body(Full::new(Bytes::from(body)))
        .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR))
}

fn vfs_error_response(err: &VfsError, resource: &str, head_only: bool) -> Response<BoxBody> {
    match err {
        VfsError::Meta(MetaError::NotFound(_)) => xml_error_response(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "the specified key does not exist",
            resource,
            head_only,
        ),
        VfsError::Meta(MetaError::IsDir(_) | MetaError::NotDir(_) | MetaError::InvalidInput(_))
        | VfsError::InvalidPath(_) => xml_error_response(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            &err.to_string(),
            resource,
            head_only,
        ),
        _ => xml_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &err.to_string(),
            resource,
            head_only,
        ),
    }
}

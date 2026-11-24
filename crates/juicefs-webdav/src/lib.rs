//! `WebDAV` HTTP server backed by [`juicefs_vfs::Vfs`].
//!
//! Supports:
//! - `OPTIONS` (`DAV: 1, 2`, `Allow: OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, PROPFIND, MOVE, COPY`)
//! - `PROPFIND` (`207 Multi-Status` XML with `Depth: 0` and `Depth: 1` default)
//! - `GET` (including `Range` `206 Partial Content`) and `HEAD`
//! - `PUT` (auto-creating missing parent directories) -> `201 Created` / `204 No Content`
//! - `MKCOL` -> `201 Created`
//! - `DELETE` -> `204 No Content`
//! - `MOVE` and `COPY` (reading `Destination` and `Overwrite` headers) -> `201 Created` / `204 No Content`
//! - `GET /metrics` -> Prometheus text exposition metrics from [`Vfs::statfs`]

pub mod error;
pub mod xml;

use std::convert::Infallible;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use http::header::{
    ACCEPT_RANGES, ALLOW, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, LAST_MODIFIED, RANGE,
};
use http::{Request, Response, StatusCode};
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
use url::Url;

pub use error::WebDavError;
use xml::{DavResource, escape_xml, format_http_date, render_multistatus};

const DAV_CAPABILITIES: &str = "1, 2";
const ALLOWED_METHODS: &str = "OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, PROPFIND, MOVE, COPY";
const XATTR_ETAG: &str = "user.webdav.etag";
const XATTR_CONTENT_TYPE: &str = "user.webdav.content_type";

type BoxBody = Full<Bytes>;

/// Handle used to gracefully stop a running [`WebDavServer`].
#[derive(Debug)]
pub struct ShutdownHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl ShutdownHandle {
    /// Signals the `WebDAV` server to stop accepting connections and awaits task completion.
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

/// Running `WebDAV` HTTP server bound to a local TCP socket.
#[derive(Debug)]
pub struct WebDavServer {
    local_addr: SocketAddr,
    handle: ShutdownHandle,
}

impl WebDavServer {
    /// Binds a `WebDAV` HTTP server to `addr` backed by `vfs`.
    pub async fn bind(
        addr: impl ToSocketAddrs,
        vfs: impl Into<Arc<Vfs>>,
    ) -> Result<Self, WebDavError> {
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
                        debug!(%local_addr, "webdav server shutting down");
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PropfindDepth {
    Zero,
    One,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyDepth {
    Zero,
    Infinity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferOp {
    Move,
    Copy,
}

fn normalize_dav_path(raw_path: &str) -> Result<String, &'static str> {
    let decoded = percent_decode_str(raw_path)
        .decode_utf8()
        .map_err(|_| "invalid UTF-8 in URI path")?;
    if decoded.contains('\0') {
        return Err("path contains NUL byte");
    }
    let mut parts = Vec::new();
    for seg in decoded.split('/') {
        if seg.is_empty() || seg == "." {
            continue;
        }
        if seg == ".." {
            return Err("path traversal is not allowed");
        }
        parts.push(seg);
    }
    if parts.is_empty() {
        Ok("/".to_owned())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn parent_vfs_path(full_path: &str) -> &str {
    match full_path.rfind('/') {
        Some(0) | None => "/",
        Some(idx) => &full_path[..idx],
    }
}

fn display_name_for_path(vfs_path: &str) -> String {
    if vfs_path == "/" {
        return String::new();
    }
    vfs_path.rsplit('/').next().unwrap_or("").to_owned()
}

fn parse_destination_path(header_val: &str) -> Result<String, &'static str> {
    let raw_path = if header_val.starts_with('/') {
        header_val
            .split(['?', '#'])
            .next()
            .ok_or("invalid Destination path")?
            .to_owned()
    } else {
        let parsed = Url::parse(header_val).map_err(|_| "invalid Destination URL")?;
        parsed.path().to_owned()
    };
    normalize_dav_path(&raw_path)
}

fn parse_propfind_depth(headers: &http::HeaderMap) -> Result<PropfindDepth, StatusCode> {
    let Some(val) = headers.get("depth") else {
        return Ok(PropfindDepth::One);
    };
    let s = val.to_str().map_err(|_| StatusCode::BAD_REQUEST)?.trim();
    match s {
        "0" => Ok(PropfindDepth::Zero),
        "1" => Ok(PropfindDepth::One),
        "infinity" => Err(StatusCode::FORBIDDEN),
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

fn parse_copy_depth(headers: &http::HeaderMap) -> Result<CopyDepth, StatusCode> {
    let Some(val) = headers.get("depth") else {
        return Ok(CopyDepth::Infinity);
    };
    let s = val.to_str().map_err(|_| StatusCode::BAD_REQUEST)?.trim();
    match s {
        "0" => Ok(CopyDepth::Zero),
        "infinity" => Ok(CopyDepth::Infinity),
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

fn parse_overwrite(headers: &http::HeaderMap) -> Result<bool, StatusCode> {
    let Some(val) = headers.get("overwrite") else {
        return Ok(true);
    };
    let s = val.to_str().map_err(|_| StatusCode::BAD_REQUEST)?.trim();
    match s {
        "T" => Ok(true),
        "F" => Ok(false),
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

async fn handle_request(vfs: Arc<Vfs>, req: Request<Incoming>) -> Response<BoxBody> {
    let method = req.method().as_str().to_owned();
    let raw_path = req.uri().path().to_owned();

    if method == "OPTIONS" {
        return handle_options();
    }

    let Ok(vfs_path) = normalize_dav_path(&raw_path) else {
        return text_response(StatusCode::BAD_REQUEST, "invalid request path");
    };

    if method == "GET" && vfs_path == "/metrics" {
        return handle_metrics(&vfs).await;
    }

    match method.as_str() {
        "PROPFIND" => {
            let depth = match parse_propfind_depth(req.headers()) {
                Ok(d) => d,
                Err(status) => return empty_response(status),
            };
            if req.into_body().collect().await.is_err() {
                return empty_response(StatusCode::BAD_REQUEST);
            }
            handle_propfind(&vfs, &vfs_path, depth).await
        }
        "GET" => {
            let range = req
                .headers()
                .get(RANGE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            handle_get_or_head(&vfs, &vfs_path, range.as_deref(), false).await
        }
        "HEAD" => handle_get_or_head(&vfs, &vfs_path, None, true).await,
        "PUT" => {
            let content_type = req
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let body = match req.into_body().collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(_) => return empty_response(StatusCode::BAD_REQUEST),
            };
            handle_put(&vfs, &vfs_path, body, content_type.as_deref()).await
        }
        "MKCOL" => {
            let body = match req.into_body().collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(_) => return empty_response(StatusCode::BAD_REQUEST),
            };
            if !body.is_empty() {
                return empty_response(StatusCode::UNSUPPORTED_MEDIA_TYPE);
            }
            handle_mkcol(&vfs, &vfs_path).await
        }
        "DELETE" => handle_delete(&vfs, &vfs_path).await,
        "MOVE" => handle_transfer(&vfs, &vfs_path, req.headers(), TransferOp::Move).await,
        "COPY" => handle_transfer(&vfs, &vfs_path, req.headers(), TransferOp::Copy).await,
        _ => empty_response(StatusCode::METHOD_NOT_ALLOWED),
    }
}

fn handle_options() -> Response<BoxBody> {
    Response::builder()
        .status(StatusCode::OK)
        .header("DAV", DAV_CAPABILITIES)
        .header(ALLOW, ALLOWED_METHODS)
        .header(CONTENT_LENGTH, 0)
        .body(Full::new(Bytes::new()))
        .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR))
}

async fn handle_metrics(vfs: &Vfs) -> Response<BoxBody> {
    let Ok(stats) = vfs.statfs().await else {
        return empty_response(StatusCode::INTERNAL_SERVER_ERROR);
    };
    let used_space = stats.total_space.saturating_sub(stats.avail_space);

    let mut body = String::with_capacity(512);
    let _ = writeln!(
        body,
        "# HELP juicefs_total_space_bytes Total filesystem space in bytes."
    );
    let _ = writeln!(body, "# TYPE juicefs_total_space_bytes gauge");
    let _ = writeln!(body, "juicefs_total_space_bytes {}", stats.total_space);
    let _ = writeln!(
        body,
        "# HELP juicefs_avail_space_bytes Available filesystem space in bytes."
    );
    let _ = writeln!(body, "# TYPE juicefs_avail_space_bytes gauge");
    let _ = writeln!(body, "juicefs_avail_space_bytes {}", stats.avail_space);
    let _ = writeln!(
        body,
        "# HELP juicefs_used_space_bytes Used filesystem space in bytes."
    );
    let _ = writeln!(body, "# TYPE juicefs_used_space_bytes gauge");
    let _ = writeln!(body, "juicefs_used_space_bytes {used_space}");
    let _ = writeln!(body, "# HELP juicefs_used_inodes Number of used inodes.");
    let _ = writeln!(body, "# TYPE juicefs_used_inodes gauge");
    let _ = writeln!(body, "juicefs_used_inodes {}", stats.used_inodes);
    let _ = writeln!(
        body,
        "# HELP juicefs_avail_inodes Number of available inodes."
    );
    let _ = writeln!(body, "# TYPE juicefs_avail_inodes gauge");
    let _ = writeln!(body, "juicefs_avail_inodes {}", stats.avail_inodes);

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")
        .header(CONTENT_LENGTH, body.len())
        .body(Full::new(Bytes::from(body)))
        .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR))
}

async fn build_dav_resource(vfs: &Vfs, vfs_path: &str, ino: Ino, attr: &Attr) -> DavResource {
    let is_dir = attr.is_dir();
    let (etag, content_type) = if is_dir {
        (String::new(), String::new())
    } else {
        (
            resolve_etag(vfs, ino, attr).await,
            resolve_content_type(vfs, ino).await,
        )
    };
    DavResource {
        vfs_path: vfs_path.to_owned(),
        display_name: display_name_for_path(vfs_path),
        is_dir,
        size: attr.length,
        content_type,
        etag,
        creation_date: attr_ctime(attr),
        last_modified: attr_mtime(attr),
    }
}

async fn handle_propfind(vfs: &Vfs, vfs_path: &str, depth: PropfindDepth) -> Response<BoxBody> {
    let (ino, attr) = match vfs.resolve_path(vfs_path).await {
        Ok(res) => res,
        Err(err) => return map_vfs_error(&err),
    };

    let mut resources = Vec::new();
    resources.push(build_dav_resource(vfs, vfs_path, ino, &attr).await);

    if depth == PropfindDepth::One && attr.is_dir() {
        let entries = match vfs.meta().readdir(ino).await {
            Ok(e) => e,
            Err(err) => return map_meta_error(&err),
        };
        for entry in entries {
            if entry.name == "." || entry.name == ".." {
                continue;
            }
            let child_path = if vfs_path == "/" {
                format!("/{}", entry.name)
            } else {
                format!("{vfs_path}/{}", entry.name)
            };
            resources.push(build_dav_resource(vfs, &child_path, entry.inode, &entry.attr).await);
        }
    }

    let xml = render_multistatus(&resources);
    Response::builder()
        .status(StatusCode::MULTI_STATUS)
        .header(CONTENT_TYPE, "application/xml; charset=utf-8")
        .header(CONTENT_LENGTH, xml.len())
        .body(Full::new(Bytes::from(xml)))
        .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR))
}

async fn handle_get_dir(vfs: &Vfs, ino: Ino, attr: &Attr, head_only: bool) -> Response<BoxBody> {
    let last_mod = format_http_date(attr_mtime(attr));
    if head_only {
        return Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/html; charset=utf-8")
            .header(CONTENT_LENGTH, 0)
            .header(LAST_MODIFIED, last_mod)
            .body(Full::new(Bytes::new()))
            .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR));
    }

    let entries = match vfs.meta().readdir(ino).await {
        Ok(e) => e,
        Err(err) => return map_meta_error(&err),
    };
    let mut html = String::from("<html><body><ul>");
    for entry in entries {
        if entry.name == "." || entry.name == ".." {
            continue;
        }
        html.push_str("<li>");
        escape_xml(&entry.name, &mut html);
        if entry.attr.is_dir() {
            html.push('/');
        }
        html.push_str("</li>");
    }
    html.push_str("</ul></body></html>");

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/html; charset=utf-8")
        .header(CONTENT_LENGTH, html.len())
        .header(LAST_MODIFIED, last_mod)
        .body(Full::new(Bytes::from(html)))
        .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR))
}

async fn handle_get_or_head(
    vfs: &Vfs,
    vfs_path: &str,
    range_header: Option<&str>,
    head_only: bool,
) -> Response<BoxBody> {
    let (ino, attr) = match vfs.resolve_path(vfs_path).await {
        Ok(res) => res,
        Err(err) => return map_vfs_error(&err),
    };

    if attr.is_dir() {
        return handle_get_dir(vfs, ino, &attr, head_only).await;
    }

    let etag = resolve_etag(vfs, ino, &attr).await;
    let content_type = resolve_content_type(vfs, ino).await;
    let last_mod = format_http_date(attr_mtime(&attr));

    if head_only {
        return Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, attr.length)
            .header(CONTENT_TYPE, content_type)
            .header(ETAG, etag)
            .header(LAST_MODIFIED, last_mod)
            .header(ACCEPT_RANGES, "bytes")
            .body(Full::new(Bytes::new()))
            .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR));
    }

    if let Some(range_spec) = range_header {
        let Ok((start, end)) = parse_byte_range(range_spec, attr.length) else {
            return Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(CONTENT_RANGE, format!("bytes */{}", attr.length))
                .header(CONTENT_LENGTH, 0)
                .body(Full::new(Bytes::new()))
                .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR));
        };

        let len = (end - start + 1) as usize;
        let mut buf = vec![0u8; len];
        let n = match vfs.read_at(ino, start, &mut buf).await {
            Ok(n) => n,
            Err(err) => return map_vfs_error(&err),
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

    let mut buf = vec![0u8; attr.length as usize];
    if !buf.is_empty() {
        let n = match vfs.read_at(ino, 0, &mut buf).await {
            Ok(n) => n,
            Err(err) => return map_vfs_error(&err),
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

async fn handle_put(
    vfs: &Vfs,
    vfs_path: &str,
    body: Bytes,
    content_type: Option<&str>,
) -> Response<BoxBody> {
    if vfs_path == "/" {
        return empty_response(StatusCode::METHOD_NOT_ALLOWED);
    }

    let existed = match vfs.resolve_path(vfs_path).await {
        Ok((_, attr)) => {
            if attr.is_dir() {
                return empty_response(StatusCode::METHOD_NOT_ALLOWED);
            }
            true
        }
        Err(VfsError::Meta(MetaError::NotFound(_))) => false,
        Err(err) => return map_vfs_error(&err),
    };

    let parent = parent_vfs_path(vfs_path);
    if parent != "/"
        && let Err(err) = vfs.mkdir_p(parent, 0o755).await
    {
        return match err {
            VfsError::Meta(MetaError::NotDir(_)) => empty_response(StatusCode::CONFLICT),
            other => map_vfs_error(&other),
        };
    }

    let etag = compute_etag(&body);
    if let Err(err) = vfs.write_file(vfs_path, &body).await {
        return map_vfs_error(&err);
    }

    if let Ok((ino, _)) = vfs.resolve_path(vfs_path).await {
        let _ = vfs.meta().set_xattr(ino, XATTR_ETAG, etag.as_bytes()).await;
        if let Some(ct) = content_type {
            let _ = vfs
                .meta()
                .set_xattr(ino, XATTR_CONTENT_TYPE, ct.as_bytes())
                .await;
        }
    }

    let status = if existed {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::CREATED
    };
    Response::builder()
        .status(status)
        .header(ETAG, etag)
        .header(CONTENT_LENGTH, 0)
        .body(Full::new(Bytes::new()))
        .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR))
}

async fn handle_mkcol(vfs: &Vfs, vfs_path: &str) -> Response<BoxBody> {
    if vfs_path == "/" {
        return empty_response(StatusCode::METHOD_NOT_ALLOWED);
    }

    match vfs.mkdir(vfs_path, 0o755).await {
        Ok(_) => empty_response(StatusCode::CREATED),
        Err(VfsError::Meta(MetaError::AlreadyExists(_))) => {
            empty_response(StatusCode::METHOD_NOT_ALLOWED)
        }
        Err(VfsError::Meta(MetaError::NotFound(_) | MetaError::NotDir(_))) => {
            empty_response(StatusCode::CONFLICT)
        }
        Err(err) => map_vfs_error(&err),
    }
}

async fn handle_delete(vfs: &Vfs, vfs_path: &str) -> Response<BoxBody> {
    if vfs_path == "/" {
        return empty_response(StatusCode::FORBIDDEN);
    }

    match vfs.resolve_path(vfs_path).await {
        Ok(_) => {}
        Err(err) => return map_vfs_error(&err),
    }

    match vfs.rm_rf(vfs_path).await {
        Ok(()) => empty_response(StatusCode::NO_CONTENT),
        Err(err) => map_vfs_error(&err),
    }
}

async fn handle_transfer(
    vfs: &Vfs,
    src_path: &str,
    headers: &http::HeaderMap,
    op: TransferOp,
) -> Response<BoxBody> {
    if src_path == "/" {
        return empty_response(StatusCode::FORBIDDEN);
    }

    let Some(dest_hdr) = headers.get("destination").and_then(|v| v.to_str().ok()) else {
        return empty_response(StatusCode::BAD_REQUEST);
    };
    let Ok(dst_path) = parse_destination_path(dest_hdr) else {
        return empty_response(StatusCode::BAD_REQUEST);
    };
    if dst_path == "/" {
        return empty_response(StatusCode::FORBIDDEN);
    }
    if src_path == dst_path || dst_path.starts_with(&format!("{src_path}/")) {
        return empty_response(StatusCode::FORBIDDEN);
    }

    let overwrite = match parse_overwrite(headers) {
        Ok(o) => o,
        Err(status) => return empty_response(status),
    };
    let copy_depth = if op == TransferOp::Copy {
        match parse_copy_depth(headers) {
            Ok(d) => d,
            Err(status) => return empty_response(status),
        }
    } else {
        CopyDepth::Infinity
    };

    let (_, src_attr) = match vfs.resolve_path(src_path).await {
        Ok(res) => res,
        Err(err) => return map_vfs_error(&err),
    };

    let dst_parent = parent_vfs_path(&dst_path);
    if dst_parent != "/" {
        match vfs.resolve_path(dst_parent).await {
            Ok((_, attr)) if attr.is_dir() => {}
            Ok(_) | Err(VfsError::Meta(MetaError::NotFound(_) | MetaError::NotDir(_))) => {
                return empty_response(StatusCode::CONFLICT);
            }
            Err(err) => return map_vfs_error(&err),
        }
    }

    let dst_existed = match vfs.resolve_path(&dst_path).await {
        Ok(_) => true,
        Err(VfsError::Meta(MetaError::NotFound(_))) => false,
        Err(err) => return map_vfs_error(&err),
    };

    if dst_existed {
        if !overwrite {
            return empty_response(StatusCode::PRECONDITION_FAILED);
        }
        if let Err(err) = vfs.rm_rf(&dst_path).await {
            return map_vfs_error(&err);
        }
    }

    let result = match op {
        TransferOp::Move => vfs.rename(src_path, &dst_path).await,
        TransferOp::Copy => {
            if src_attr.is_dir() && copy_depth == CopyDepth::Zero {
                vfs.mkdir(&dst_path, src_attr.mode).await.map(|_| ())
            } else {
                vfs.clone_path(src_path, &dst_path).await.map(|_| ())
            }
        }
    };

    match result {
        Ok(()) => empty_response(if dst_existed {
            StatusCode::NO_CONTENT
        } else {
            StatusCode::CREATED
        }),
        Err(err) => map_vfs_error(&err),
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

fn attr_mtime(attr: &Attr) -> DateTime<Utc> {
    DateTime::from_timestamp(attr.mtime, attr.mtime_nsec).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

fn attr_ctime(attr: &Attr) -> DateTime<Utc> {
    DateTime::from_timestamp(attr.ctime, attr.ctime_nsec).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

fn empty_response(status: StatusCode) -> Response<BoxBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_LENGTH, 0)
        .body(Full::new(Bytes::new()))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
}

fn text_response(status: StatusCode, msg: &str) -> Response<BoxBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(CONTENT_LENGTH, msg.len())
        .body(Full::new(Bytes::from(msg.to_owned())))
        .unwrap_or_else(|_| empty_response(StatusCode::INTERNAL_SERVER_ERROR))
}

fn map_meta_error(err: &MetaError) -> Response<BoxBody> {
    match err {
        MetaError::NotFound(_) => empty_response(StatusCode::NOT_FOUND),
        MetaError::AlreadyExists(_) => empty_response(StatusCode::CONFLICT),
        MetaError::IsDir(_) | MetaError::NotDir(_) | MetaError::InvalidInput(_) => {
            empty_response(StatusCode::BAD_REQUEST)
        }
        _ => empty_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

fn map_vfs_error(err: &VfsError) -> Response<BoxBody> {
    match err {
        VfsError::Meta(meta_err) => map_meta_error(meta_err),
        VfsError::InvalidPath(_) => empty_response(StatusCode::BAD_REQUEST),
        VfsError::PermissionDenied(_) => empty_response(StatusCode::FORBIDDEN),
        _ => empty_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

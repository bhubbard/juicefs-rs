//! Integration tests for `juicefs-webdav`.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http::header::{
    ALLOW, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, HOST, LAST_MODIFIED, RANGE,
};
use http::{Method, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use juicefs_core::{ChunkConfig, ClientConfig, Format};
use juicefs_meta::{Meta, SqliteMeta};
use juicefs_vfs::Vfs;
use juicefs_webdav::WebDavServer;
use quick_xml::Reader;
use quick_xml::events::Event;
use tokio::net::TcpStream;

async fn setup_vfs() -> Vfs {
    let meta = Arc::new(SqliteMeta::open(":memory:").expect("open sqlite meta"));
    let mut fmt = Format::new("davtest", "mem", "");
    fmt.block_size = 4096;
    "none".clone_into(&mut fmt.compression);
    meta.init(&fmt, false).await.expect("init meta");
    Vfs::connect(meta, ChunkConfig::default(), ClientConfig::default())
        .await
        .expect("connect vfs")
}

async fn send_http(
    addr: SocketAddr,
    method: &str,
    path_and_query: &str,
    headers: &[(&str, &str)],
    body: Bytes,
) -> (StatusCode, http::HeaderMap, Bytes) {
    let stream = TcpStream::connect(addr).await.expect("connect to webdav");
    let io = TokioIo::new(stream);
    let (mut sender, conn) = http1::handshake(io).await.expect("http1 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let method = Method::from_bytes(method.as_bytes()).expect("valid HTTP method");
    let mut builder = Request::builder()
        .method(method)
        .uri(path_and_query)
        .header(HOST, addr.to_string());
    for &(name, val) in headers {
        builder = builder.header(name, val);
    }
    let req = builder.body(Full::new(body)).expect("build request");
    let resp = sender.send_request(req).await.expect("send request");
    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let resp_body = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    (status, resp_headers, resp_body)
}

#[derive(Debug, Default)]
struct DavEntry {
    href: String,
    display_name: String,
    is_collection: bool,
    content_length: Option<u64>,
    status: String,
}

fn parse_multistatus(xml: &[u8]) -> Vec<DavEntry> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut entries = Vec::new();
    let mut current: Option<DavEntry> = None;
    let mut stack: Vec<String> = Vec::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                if name.ends_with("response") {
                    current = Some(DavEntry::default());
                } else if name.ends_with("collection")
                    && let Some(entry) = current.as_mut()
                {
                    entry.is_collection = true;
                }
                stack.push(name);
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                if name.ends_with("collection")
                    && let Some(entry) = current.as_mut()
                {
                    entry.is_collection = true;
                }
            }
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                if name.ends_with("response")
                    && let Some(entry) = current.take()
                {
                    entries.push(entry);
                }
                stack.pop();
            }
            Ok(Event::Text(e)) => {
                let text = String::from_utf8_lossy(e.as_ref()).into_owned();
                if let (Some(entry), Some(tag)) = (current.as_mut(), stack.last()) {
                    if tag.ends_with("href") {
                        entry.href = text;
                    } else if tag.ends_with("displayname") {
                        entry.display_name = text;
                    } else if tag.ends_with("getcontentlength") {
                        entry.content_length = Some(text.parse().expect("valid content length"));
                    } else if tag.ends_with("status") {
                        entry.status = text;
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(err) => panic!("XML parse error: {err}"),
            _ => {}
        }
    }
    entries
}

#[tokio::test]
async fn options_mkcol_put_get_head_and_range() {
    let vfs = setup_vfs().await;
    let server = WebDavServer::bind("127.0.0.1:0", vfs)
        .await
        .expect("bind webdav");
    let addr = server.local_addr();

    // OPTIONS
    let (status, headers, body) = send_http(addr, "OPTIONS", "/", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    assert_eq!(headers.get("dav").unwrap(), "1, 2");
    assert_eq!(
        headers.get(ALLOW).unwrap(),
        "OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, PROPFIND, MOVE, COPY"
    );

    // MKCOL
    let (status, _, _) = send_http(addr, "MKCOL", "/docs", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::CREATED);

    // MKCOL again -> 405 Method Not Allowed
    let (status, _, _) = send_http(addr, "MKCOL", "/docs", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);

    // MKCOL with missing parent -> 409 Conflict
    let (status, _, _) = send_http(addr, "MKCOL", "/missing/sub", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::CONFLICT);

    // PUT new file (auto-creating nested missing parents) -> 201 Created
    let payload = Bytes::from_static(b"0123456789abcdef");
    let (status, headers, _) = send_http(
        addr,
        "PUT",
        "/docs/nested/readme.txt",
        &[("content-type", "text/plain")],
        payload.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let etag1 = headers.get(ETAG).unwrap().to_str().unwrap().to_owned();

    // PUT overwrite existing file -> 204 No Content
    let (status, _, _) = send_http(
        addr,
        "PUT",
        "/docs/nested/readme.txt",
        &[("content-type", "text/plain")],
        payload.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // HEAD
    let (status, headers, body) =
        send_http(addr, "HEAD", "/docs/nested/readme.txt", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    assert_eq!(headers.get(CONTENT_LENGTH).unwrap(), "16");
    assert_eq!(headers.get(CONTENT_TYPE).unwrap(), "text/plain");
    assert_eq!(headers.get(ETAG).unwrap().to_str().unwrap(), etag1);
    assert!(headers.contains_key(LAST_MODIFIED));

    // GET full content
    let (status, headers, body) =
        send_http(addr, "GET", "/docs/nested/readme.txt", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, payload);
    assert_eq!(headers.get(ETAG).unwrap().to_str().unwrap(), etag1);

    // GET with Range -> 206 Partial Content
    let (status, headers, body) = send_http(
        addr,
        "GET",
        "/docs/nested/readme.txt",
        &[(RANGE.as_str(), "bytes=4-9")],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(headers.get(CONTENT_RANGE).unwrap(), "bytes 4-9/16");
    assert_eq!(body, Bytes::from_static(b"456789"));

    // GET with unsatisfiable Range -> 416
    let (status, _, _) = send_http(
        addr,
        "GET",
        "/docs/nested/readme.txt",
        &[(RANGE.as_str(), "bytes=99-120")],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);

    server.shutdown().await;
}

#[tokio::test]
async fn propfind_depth_zero_and_one() {
    let vfs = setup_vfs().await;
    let server = WebDavServer::bind("127.0.0.1:0", vfs)
        .await
        .expect("bind webdav");
    let addr = server.local_addr();

    let (status, _, _) = send_http(
        addr,
        "PUT",
        "/docs/nested/readme.txt",
        &[("content-type", "text/plain")],
        Bytes::from_static(b"0123456789abcdef"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _, _) = send_http(
        addr,
        "PUT",
        "/docs/notes.md",
        &[],
        Bytes::from_static(b"hello world!"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // PROPFIND Depth: 0
    let (status, headers, body) = send_http(
        addr,
        "PROPFIND",
        "/docs",
        &[("depth", "0")],
        Bytes::from_static(
            b"<?xml version=\"1.0\"?><D:propfind xmlns:D=\"DAV:\"><D:allprop/></D:propfind>",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::MULTI_STATUS);
    assert!(
        headers
            .get(CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("application/xml")
    );
    let body_str = std::str::from_utf8(&body).unwrap();
    assert!(body_str.contains("<D:multistatus xmlns:D=\"DAV:\">"));
    let depth0 = parse_multistatus(&body);
    assert_eq!(depth0.len(), 1);
    assert_eq!(depth0[0].href, "/docs/");
    assert!(depth0[0].is_collection);
    assert_eq!(depth0[0].status, "HTTP/1.1 200 OK");

    // PROPFIND Depth: 1 (explicit and default)
    for depth_hdr in [vec![("depth", "1")], vec![]] {
        let (status, _, body) =
            send_http(addr, "PROPFIND", "/docs", &depth_hdr, Bytes::new()).await;
        assert_eq!(status, StatusCode::MULTI_STATUS);
        let entries = parse_multistatus(&body);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].href, "/docs/");
        assert!(entries[0].is_collection);

        let child_hrefs: Vec<&str> = entries.iter().map(|e| e.href.as_str()).collect();
        assert!(child_hrefs.contains(&"/docs/nested/"));
        assert!(child_hrefs.contains(&"/docs/notes.md"));

        let notes = entries
            .iter()
            .find(|e| e.href == "/docs/notes.md")
            .expect("notes.md entry");
        assert!(!notes.is_collection);
        assert_eq!(notes.content_length, Some(12));
    }

    // PROPFIND on file with Depth: 0
    let (status, _, body) = send_http(
        addr,
        "PROPFIND",
        "/docs/notes.md",
        &[("depth", "0")],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::MULTI_STATUS);
    let file_entries = parse_multistatus(&body);
    assert_eq!(file_entries.len(), 1);
    assert_eq!(file_entries[0].href, "/docs/notes.md");
    assert_eq!(file_entries[0].content_length, Some(12));

    server.shutdown().await;
}

#[tokio::test]
async fn move_copy_delete_and_metrics() {
    let vfs = setup_vfs().await;
    let server = WebDavServer::bind("127.0.0.1:0", vfs)
        .await
        .expect("bind webdav");
    let addr = server.local_addr();

    // Seed source file
    let (status, _, _) = send_http(
        addr,
        "PUT",
        "/src/alpha.txt",
        &[("content-type", "text/plain")],
        Bytes::from_static(b"alpha-content"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // COPY to new destination -> 201 Created
    let dst_url = format!("http://{addr}/copy/alpha-copy.txt");
    let (status, _, _) = send_http(addr, "MKCOL", "/copy", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _, _) = send_http(
        addr,
        "COPY",
        "/src/alpha.txt",
        &[("destination", &dst_url)],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Verify both source and copy exist with identical payload
    let (status, _, src_body) = send_http(addr, "GET", "/src/alpha.txt", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(src_body, Bytes::from_static(b"alpha-content"));

    let (status, _, copy_body) =
        send_http(addr, "GET", "/copy/alpha-copy.txt", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(copy_body, Bytes::from_static(b"alpha-content"));

    // COPY with Overwrite: F when destination exists -> 412 Precondition Failed
    let (status, _, _) = send_http(
        addr,
        "COPY",
        "/src/alpha.txt",
        &[("destination", &dst_url), ("overwrite", "F")],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED);

    // MOVE file to new path -> 201 Created
    let moved_url = format!("http://{addr}/copy/alpha-moved.txt");
    let (status, _, _) = send_http(
        addr,
        "MOVE",
        "/src/alpha.txt",
        &[("destination", &moved_url)],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Old source is gone; moved file has original bytes
    let (status, _, _) = send_http(addr, "GET", "/src/alpha.txt", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _, moved_body) =
        send_http(addr, "GET", "/copy/alpha-moved.txt", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(moved_body, Bytes::from_static(b"alpha-content"));

    // MOVE overwriting existing file -> 204 No Content
    let (status, _, _) = send_http(
        addr,
        "MOVE",
        "/copy/alpha-moved.txt",
        &[("destination", "/copy/alpha-copy.txt")],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _, _) = send_http(addr, "GET", "/copy/alpha-moved.txt", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // DELETE collection recursively -> 204 No Content
    let (status, _, _) = send_http(addr, "DELETE", "/copy", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _, _) = send_http(addr, "PROPFIND", "/copy", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // DELETE missing resource -> 404 Not Found
    let (status, _, _) = send_http(addr, "DELETE", "/copy/missing", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // GET /metrics -> Prometheus text stats from Vfs::statfs()
    let (status, headers, metrics_body) =
        send_http(addr, "GET", "/metrics", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        headers
            .get(CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/plain")
    );
    let metrics_text = std::str::from_utf8(&metrics_body).unwrap();
    assert!(metrics_text.contains("juicefs_total_space_bytes "));
    assert!(metrics_text.contains("juicefs_avail_space_bytes "));
    assert!(metrics_text.contains("juicefs_used_space_bytes "));
    assert!(metrics_text.contains("juicefs_used_inodes "));
    assert!(metrics_text.contains("juicefs_avail_inodes "));

    server.shutdown().await;
    assert!(TcpStream::connect(addr).await.is_err());
}

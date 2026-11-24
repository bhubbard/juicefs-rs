//! Integration tests for `juicefs-gateway`.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http::header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, HOST, LAST_MODIFIED, RANGE};
use http::{Method, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use juicefs_core::{ChunkConfig, ClientConfig, Format};
use juicefs_gateway::GatewayServer;
use juicefs_meta::{Meta, SqliteMeta};
use juicefs_vfs::Vfs;
use quick_xml::Reader;
use quick_xml::events::Event;
use tokio::net::TcpStream;

async fn setup_vfs() -> Vfs {
    let meta = Arc::new(SqliteMeta::open(":memory:").expect("open sqlite meta"));
    let mut fmt = Format::new("gwtest", "mem", "");
    fmt.block_size = 4096;
    "none".clone_into(&mut fmt.compression);
    meta.init(&fmt, false).await.expect("init meta");
    Vfs::connect(meta, ChunkConfig::default(), ClientConfig::default())
        .await
        .expect("connect vfs")
}

async fn send_http(
    addr: SocketAddr,
    method: Method,
    path_and_query: &str,
    headers: &[(&str, &str)],
    body: Bytes,
) -> (StatusCode, http::HeaderMap, Bytes) {
    let stream = TcpStream::connect(addr).await.expect("connect to gateway");
    let io = TokioIo::new(stream);
    let (mut sender, conn) = http1::handshake(io).await.expect("http1 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });

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
struct ParsedListV2 {
    name: String,
    prefix: String,
    key_count: usize,
    is_truncated: bool,
    next_continuation_token: Option<String>,
    keys: Vec<String>,
    sizes: Vec<u64>,
    common_prefixes: Vec<String>,
}

fn parse_list_xml(xml: &[u8]) -> ParsedListV2 {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut out = ParsedListV2::default();
    let mut path_stack: Vec<String> = Vec::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                path_stack.push(tag);
            }
            Ok(Event::End(_)) => {
                path_stack.pop();
            }
            Ok(Event::Text(e)) => {
                let text = String::from_utf8_lossy(e.as_ref()).into_owned();
                let path = path_stack.join("/");
                match path.as_str() {
                    "ListBucketResult/Name" => out.name = text,
                    "ListBucketResult/Prefix" => out.prefix = text,
                    "ListBucketResult/KeyCount" => out.key_count = text.parse().unwrap(),
                    "ListBucketResult/IsTruncated" => out.is_truncated = text == "true",
                    "ListBucketResult/NextContinuationToken" => {
                        out.next_continuation_token = Some(text);
                    }
                    "ListBucketResult/Contents/Key" => out.keys.push(text),
                    "ListBucketResult/Contents/Size" => out.sizes.push(text.parse().unwrap()),
                    "ListBucketResult/CommonPrefixes/Prefix" => out.common_prefixes.push(text),
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(err) => panic!("XML parse error: {err}"),
            _ => {}
        }
    }
    out
}

#[tokio::test]
async fn put_head_get_delete_lifecycle() {
    let vfs = setup_vfs().await;
    let server = GatewayServer::bind("127.0.0.1:0", vfs)
        .await
        .expect("bind gateway");
    let addr = server.local_addr();

    // PUT object
    let payload = Bytes::from_static(b"hello juicefs gateway!");
    let (status, headers, body) = send_http(
        addr,
        Method::PUT,
        "/mybucket/greetings/hello.txt",
        &[("content-type", "text/plain")],
        payload.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    let put_etag = headers
        .get(ETAG)
        .expect("PUT ETag")
        .to_str()
        .unwrap()
        .to_owned();

    // HEAD object
    let (status, headers, body) = send_http(
        addr,
        Method::HEAD,
        "/mybucket/greetings/hello.txt",
        &[],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    assert_eq!(headers.get(CONTENT_LENGTH).unwrap(), "22");
    assert_eq!(headers.get(CONTENT_TYPE).unwrap(), "text/plain");
    assert_eq!(headers.get(ETAG).unwrap().to_str().unwrap(), put_etag);
    assert!(headers.contains_key(LAST_MODIFIED));

    // GET object
    let (status, headers, body) = send_http(
        addr,
        Method::GET,
        "/mybucket/greetings/hello.txt",
        &[],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, payload);
    assert_eq!(headers.get(ETAG).unwrap().to_str().unwrap(), put_etag);

    // DELETE object
    let (status, _, body) = send_http(
        addr,
        Method::DELETE,
        "/mybucket/greetings/hello.txt",
        &[],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_empty());

    // HEAD & GET after DELETE return 404
    let (status, _, body) = send_http(
        addr,
        Method::HEAD,
        "/mybucket/greetings/hello.txt",
        &[],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.is_empty());

    let (status, _, body) = send_http(
        addr,
        Method::GET,
        "/mybucket/greetings/hello.txt",
        &[],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let body_str = std::str::from_utf8(&body).unwrap();
    assert!(body_str.contains("<Code>NoSuchKey</Code>"));

    // Idempotent DELETE
    let (status, _, _) = send_http(
        addr,
        Method::DELETE,
        "/mybucket/greetings/hello.txt",
        &[],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    server.shutdown().await;
}

#[tokio::test]
async fn list_objects_v2_prefix_delimiter_and_pagination() {
    let vfs = setup_vfs().await;
    let server = GatewayServer::bind("127.0.0.1:0", vfs)
        .await
        .expect("bind gateway");
    let addr = server.local_addr();

    for (key, data) in [
        ("docs/a.md", b"alpha".as_slice()),
        ("docs/b.md", b"bravo-2".as_slice()),
        ("docs/sub/c.md", b"charlie".as_slice()),
        ("images/logo.png", b"png!".as_slice()),
    ] {
        let (status, _, _) = send_http(
            addr,
            Method::PUT,
            &format!("/testbucket/{key}"),
            &[],
            Bytes::copy_from_slice(data),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    // Prefix listing without delimiter
    let (status, headers, body) = send_http(
        addr,
        Method::GET,
        "/testbucket?list-type=2&prefix=docs%2F",
        &[],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get(CONTENT_TYPE).unwrap(), "application/xml");
    let parsed = parse_list_xml(&body);
    assert_eq!(parsed.name, "testbucket");
    assert_eq!(parsed.prefix, "docs/");
    assert_eq!(parsed.key_count, 3);
    assert!(!parsed.is_truncated);
    assert_eq!(parsed.keys, vec!["docs/a.md", "docs/b.md", "docs/sub/c.md"]);
    assert_eq!(parsed.sizes, vec![5, 7, 7]);

    // Delimiter listing
    let (status, _, body) = send_http(
        addr,
        Method::GET,
        "/testbucket?list-type=2&prefix=docs%2F&delimiter=%2F",
        &[],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let parsed = parse_list_xml(&body);
    assert_eq!(parsed.keys, vec!["docs/a.md", "docs/b.md"]);
    assert_eq!(parsed.common_prefixes, vec!["docs/sub/"]);

    // Pagination with max-keys=2
    let (status, _, page1_body) = send_http(
        addr,
        Method::GET,
        "/testbucket?list-type=2&max-keys=2",
        &[],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let page1 = parse_list_xml(&page1_body);
    assert!(page1.is_truncated);
    assert_eq!(page1.keys, vec!["docs/a.md", "docs/b.md"]);
    let token = page1.next_continuation_token.expect("continuation token");

    let (status, _, page2_body) = send_http(
        addr,
        Method::GET,
        &format!("/testbucket?list-type=2&max-keys=2&continuation-token={token}"),
        &[],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let page2 = parse_list_xml(&page2_body);
    assert!(!page2.is_truncated);
    assert_eq!(page2.keys, vec!["docs/sub/c.md", "images/logo.png"]);

    server.shutdown().await;
}

#[tokio::test]
async fn get_object_range_and_overwrite_and_validation() {
    let vfs = setup_vfs().await;
    let server = GatewayServer::bind("127.0.0.1:0", vfs)
        .await
        .expect("bind gateway");
    let addr = server.local_addr();

    // Empty bucket listing works cleanly
    let (status, _, body) = send_http(
        addr,
        Method::GET,
        "/freshbucket?list-type=2",
        &[],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let empty_list = parse_list_xml(&body);
    assert_eq!(empty_list.key_count, 0);
    assert!(!empty_list.is_truncated);

    // Put and byte-range read
    let (status, _, _) = send_http(
        addr,
        Method::PUT,
        "/bkt/range.bin",
        &[],
        Bytes::from_static(b"0123456789"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, headers, body) = send_http(
        addr,
        Method::GET,
        "/bkt/range.bin",
        &[(RANGE.as_str(), "bytes=2-5")],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(headers.get(CONTENT_RANGE).unwrap(), "bytes 2-5/10");
    assert_eq!(body, Bytes::from_static(b"2345"));

    let (status, _, _) = send_http(
        addr,
        Method::GET,
        "/bkt/range.bin",
        &[(RANGE.as_str(), "bytes=50-60")],
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);

    // Overwrite updates payload and ETag
    let (status, h1, _) = send_http(
        addr,
        Method::PUT,
        "/bkt/range.bin",
        &[],
        Bytes::from_static(b"updated-payload"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, h2, body) =
        send_http(addr, Method::GET, "/bkt/range.bin", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from_static(b"updated-payload"));
    assert_eq!(h1.get(ETAG), h2.get(ETAG));

    // Reject path traversal
    let (status, _, _) = send_http(addr, Method::GET, "/bkt/../escape", &[], Bytes::new()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    server.shutdown().await;
    assert!(TcpStream::connect(addr).await.is_err());
}

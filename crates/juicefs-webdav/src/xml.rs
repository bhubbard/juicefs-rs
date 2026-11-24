//! `WebDAV` XML serialization helpers for `PROPFIND` multi-status responses.

use std::fmt::Write as _;

use chrono::{DateTime, Utc};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};

const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}');

/// Single resource entry rendered inside a `207 Multi-Status` XML response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DavResource {
    /// Normalized VFS path (e.g. `"/"`, `"/docs"`, `"/docs/readme.txt"`).
    pub vfs_path: String,
    /// Display name for the resource (empty for root).
    pub display_name: String,
    /// Whether this resource is a collection (directory).
    pub is_dir: bool,
    /// File length in bytes (ignored for directories).
    pub size: u64,
    /// MIME content type for regular files.
    pub content_type: String,
    /// Entity tag for regular files.
    pub etag: String,
    /// Creation timestamp.
    pub creation_date: DateTime<Utc>,
    /// Last modification timestamp.
    pub last_modified: DateTime<Utc>,
}

/// Escapes special XML characters in text nodes.
pub fn escape_xml(input: &str, out: &mut String) {
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
}

/// Encodes a normalized VFS path into a valid `WebDAV` `<D:href>` URI path.
#[must_use]
pub fn encode_dav_href(vfs_path: &str, is_dir: bool) -> String {
    if vfs_path == "/" {
        return "/".to_owned();
    }
    let mut href = String::with_capacity(vfs_path.len() + 4);
    for seg in vfs_path.split('/').filter(|s| !s.is_empty()) {
        href.push('/');
        for chunk in utf8_percent_encode(seg, PATH_SEGMENT_ENCODE_SET) {
            href.push_str(chunk);
        }
    }
    if is_dir && !href.ends_with('/') {
        href.push('/');
    }
    href
}

/// Formats an HTTP-date string (`IMF-fixdate`) for `<D:getlastmodified>`.
#[must_use]
pub fn format_http_date(dt: DateTime<Utc>) -> String {
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

/// Renders a `WebDAV` `<D:multistatus xmlns:D="DAV:">` XML response body.
#[must_use]
pub fn render_multistatus(resources: &[DavResource]) -> String {
    let mut xml = String::with_capacity(256 + resources.len() * 384);
    xml.push_str("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
    xml.push_str("<D:multistatus xmlns:D=\"DAV:\">");

    for res in resources {
        let href = encode_dav_href(&res.vfs_path, res.is_dir);
        xml.push_str("<D:response><D:href>");
        escape_xml(&href, &mut xml);
        xml.push_str("</D:href><D:propstat><D:prop>");

        xml.push_str("<D:displayname>");
        escape_xml(&res.display_name, &mut xml);
        xml.push_str("</D:displayname>");

        if res.is_dir {
            xml.push_str("<D:resourcetype><D:collection/></D:resourcetype>");
        } else {
            xml.push_str("<D:resourcetype/>");
            let _ = write!(xml, "<D:getcontentlength>{}</D:getcontentlength>", res.size);
            xml.push_str("<D:getcontenttype>");
            escape_xml(&res.content_type, &mut xml);
            xml.push_str("</D:getcontenttype>");
            xml.push_str("<D:getetag>");
            escape_xml(&res.etag, &mut xml);
            xml.push_str("</D:getetag>");
        }

        let created = res.creation_date.format("%Y-%m-%dT%H:%M:%SZ");
        let _ = write!(xml, "<D:creationdate>{created}</D:creationdate>");

        let modified = format_http_date(res.last_modified);
        let _ = write!(xml, "<D:getlastmodified>{modified}</D:getlastmodified>");

        xml.push_str("</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>");
    }

    xml.push_str("</D:multistatus>");
    xml
}

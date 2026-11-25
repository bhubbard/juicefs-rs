//! `WebDAV` XML serialization helpers for `PROPFIND` multi-status responses.

use std::fmt::Write as _;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    pub creation_date: SystemTime,
    /// Last modification timestamp.
    pub last_modified: SystemTime,
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

/// Converts days since the Unix epoch (`1970-01-01`) to `(year, month, day)`.
///
/// Implements Howard Hinnant's `civil_from_days` algorithm.
fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = y + i64::from(m <= 2);
    (y, m as u32, d as u32)
}

/// Formats a [`SystemTime`] as an ISO-8601 UTC timestamp (`YYYY-MM-DDTHH:MM:SS.000Z`).
#[must_use]
pub fn format_iso8601_utc(ts: SystemTime) -> String {
    let dur = ts.duration_since(UNIX_EPOCH).unwrap_or_default();
    let total_secs = dur.as_secs();
    let millis = dur.subsec_millis();
    let days = (total_secs / 86_400) as i64;
    let secs_of_day = total_secs % 86_400;
    let hour = secs_of_day / 3_600;
    let minute = (secs_of_day % 3_600) / 60;
    let second = secs_of_day % 60;
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

/// Formats an HTTP-date string (`IMF-fixdate`) for `<D:getlastmodified>`.
#[must_use]
pub fn format_http_date(ts: SystemTime) -> String {
    const MAX_HTTP_DATE_SECS: u64 = 253_402_300_800; // 9999-12-31T23:59:59Z + 1s
    let clamped = match ts.duration_since(UNIX_EPOCH) {
        Ok(dur) if dur.as_secs() < MAX_HTTP_DATE_SECS => ts,
        Ok(_) => UNIX_EPOCH + Duration::from_secs(MAX_HTTP_DATE_SECS - 1),
        Err(_) => UNIX_EPOCH,
    };
    httpdate::fmt_http_date(clamped)
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

        let created = format_iso8601_utc(res.creation_date);
        let _ = write!(xml, "<D:creationdate>{created}</D:creationdate>");

        let modified = format_http_date(res.last_modified);
        let _ = write!(xml, "<D:getlastmodified>{modified}</D:getlastmodified>");

        xml.push_str("</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>");
    }

    xml.push_str("</D:multistatus>");
    xml
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso8601_utc_and_http_date_formatting() {
        assert_eq!(format_iso8601_utc(UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            format_http_date(UNIX_EPOCH),
            "Thu, 01 Jan 1970 00:00:00 GMT"
        );

        let leap_2000 = UNIX_EPOCH + Duration::new(951_868_799, 456_000_000);
        assert_eq!(format_iso8601_utc(leap_2000), "2000-02-29T23:59:59.456Z");

        let nov_2023 = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        assert_eq!(format_iso8601_utc(nov_2023), "2023-11-14T22:13:20.000Z");
        assert_eq!(format_http_date(nov_2023), "Tue, 14 Nov 2023 22:13:20 GMT");
    }
}

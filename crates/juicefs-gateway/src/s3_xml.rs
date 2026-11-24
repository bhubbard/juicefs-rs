//! S3 XML serialization helpers for `ListObjectsV2` and error responses.

use std::fmt::Write as _;

use chrono::{DateTime, Utc};

/// Single object entry in an S3 `ListObjectsV2` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectEntry {
    /// Object key relative to the bucket root.
    pub key: String,
    /// Last modification timestamp.
    pub last_modified: DateTime<Utc>,
    /// Entity tag (quoted or unquoted; formatted as quoted in XML).
    pub etag: String,
    /// Object payload size in bytes.
    pub size: u64,
}

/// Parameters and results for rendering an S3 `ListObjectsV2` XML body.
#[derive(Debug, Clone)]
pub struct ListObjectsV2Result<'a> {
    /// Bucket name.
    pub bucket: &'a str,
    /// Prefix filter used in the listing.
    pub prefix: &'a str,
    /// Optional delimiter used to group common prefixes.
    pub delimiter: Option<&'a str>,
    /// Optional continuation token provided by the caller.
    pub continuation_token: Option<&'a str>,
    /// Optional token to fetch the next page when `is_truncated` is `true`.
    pub next_continuation_token: Option<&'a str>,
    /// Maximum keys requested.
    pub max_keys: usize,
    /// Whether additional keys were omitted due to `max_keys`.
    pub is_truncated: bool,
    /// Listed object entries.
    pub contents: &'a [ObjectEntry],
    /// Rolled-up common prefixes when `delimiter` is set.
    pub common_prefixes: &'a [String],
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

/// Renders an S3 `ListBucketResult` XML document (`ListObjectsV2`).
#[must_use]
pub fn render_list_objects_v2(result: &ListObjectsV2Result<'_>) -> String {
    let mut xml = String::with_capacity(512 + result.contents.len() * 192);
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");

    xml.push_str("<Name>");
    escape_xml(result.bucket, &mut xml);
    xml.push_str("</Name>");

    xml.push_str("<Prefix>");
    escape_xml(result.prefix, &mut xml);
    xml.push_str("</Prefix>");

    if let Some(delim) = result.delimiter {
        xml.push_str("<Delimiter>");
        escape_xml(delim, &mut xml);
        xml.push_str("</Delimiter>");
    }

    if let Some(token) = result.continuation_token {
        xml.push_str("<ContinuationToken>");
        escape_xml(token, &mut xml);
        xml.push_str("</ContinuationToken>");
    }

    let key_count = result.contents.len() + result.common_prefixes.len();
    let _ = write!(xml, "<KeyCount>{key_count}</KeyCount>");
    let _ = write!(xml, "<MaxKeys>{}</MaxKeys>", result.max_keys);
    let _ = write!(
        xml,
        "<IsTruncated>{}</IsTruncated>",
        if result.is_truncated { "true" } else { "false" }
    );

    if let Some(next_token) = result.next_continuation_token {
        xml.push_str("<NextContinuationToken>");
        escape_xml(next_token, &mut xml);
        xml.push_str("</NextContinuationToken>");
    }

    for obj in result.contents {
        xml.push_str("<Contents>");
        xml.push_str("<Key>");
        escape_xml(&obj.key, &mut xml);
        xml.push_str("</Key>");

        let ts = obj.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let _ = write!(xml, "<LastModified>{ts}</LastModified>");

        xml.push_str("<ETag>");
        if obj.etag.starts_with('"') {
            escape_xml(&obj.etag, &mut xml);
        } else {
            xml.push('"');
            escape_xml(&obj.etag, &mut xml);
            xml.push('"');
        }
        xml.push_str("</ETag>");

        let _ = write!(xml, "<Size>{}</Size>", obj.size);
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        xml.push_str("</Contents>");
    }

    for cp in result.common_prefixes {
        xml.push_str("<CommonPrefixes><Prefix>");
        escape_xml(cp, &mut xml);
        xml.push_str("</Prefix></CommonPrefixes>");
    }

    xml.push_str("</ListBucketResult>");
    xml
}

/// Renders an S3 XML `<Error>` payload.
#[must_use]
pub fn render_error(code: &str, message: &str, resource: &str) -> String {
    let mut xml = String::with_capacity(256);
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<Error><Code>");
    escape_xml(code, &mut xml);
    xml.push_str("</Code><Message>");
    escape_xml(message, &mut xml);
    xml.push_str("</Message><Resource>");
    escape_xml(resource, &mut xml);
    xml.push_str("</Resource></Error>");
    xml
}

//! Streamed file responses with one byte range: nothing is read into memory, and the file is read
//! only from a path the node chose. Errors carry stable codes, never a path.

use std::path::Path;

use axum::body::Body;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

use crate::error::ApiError;

const CHUNK: usize = 64 * 1024;

/// One range of a `Range: bytes=` header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteRange {
    /// `start-` or `start-end` (inclusive).
    From { start: u64, end: Option<u64> },
    /// `-n`: the last `n` bytes.
    Suffix(u64),
}

impl ByteRange {
    /// Whether it reads from the first byte (a client that holds nothing yet asks this).
    pub fn starts_at_zero(&self) -> bool {
        matches!(self, ByteRange::From { start: 0, .. })
    }

    /// `(first, last)` inclusive within a file of `size` bytes; `None`: not satisfiable.
    pub fn resolve(&self, size: u64) -> Option<(u64, u64)> {
        let last = size.checked_sub(1)?;
        match *self {
            ByteRange::From { start, end } if start <= last => Some((start, end.map_or(last, |e| e.min(last)))),
            ByteRange::From { .. } | ByteRange::Suffix(0) => None,
            ByteRange::Suffix(n) => Some((size.saturating_sub(n), last)),
        }
    }
}

fn number(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// `416 range_not_satisfiable`, with the size when it is known.
pub fn range_not_satisfiable(size: Option<u64>) -> Response {
    let mut resp = ApiError::new("range_not_satisfiable", "this range cannot be served").into_response();
    if let Some(v) = size.and_then(|n| HeaderValue::from_str(&format!("bytes */{n}")).ok()) {
        resp.headers_mut().insert(header::CONTENT_RANGE, v);
    }
    resp
}

/// The `Range` header. A unit other than `bytes` is ignored, as RFC 9110 says. Malformed values and
/// several ranges at once are refused (`Err`): this answer is one contiguous stream, and a full
/// body nobody asked for is the expensive way to say so.
pub fn parse_range(headers: &HeaderMap) -> Result<Option<ByteRange>, Response> {
    let Some(raw) = headers.get(header::RANGE) else {
        return Ok(None);
    };
    let refuse = || range_not_satisfiable(None);
    let raw = raw.to_str().map_err(|_| refuse())?.trim();
    let Some((unit, spec)) = raw.split_once('=') else {
        return Err(refuse());
    };
    if !unit.trim().eq_ignore_ascii_case("bytes") {
        return Ok(None);
    }
    let Some((a, b)) = spec.trim().split_once('-') else {
        return Err(refuse());
    };
    if spec.contains(',') {
        return Err(refuse());
    }
    let range = match (a, b) {
        ("", n) => ByteRange::Suffix(number(n).ok_or_else(refuse)?),
        (s, "") => ByteRange::From {
            start: number(s).ok_or_else(refuse)?,
            end: None,
        },
        (s, e) => {
            let (start, end) = (number(s).ok_or_else(refuse)?, number(e).ok_or_else(refuse)?);
            if end < start {
                return Err(refuse());
            }
            ByteRange::From { start, end: Some(end) }
        }
    };
    Ok(Some(range))
}

/// Stream the `size`-byte file at `path` (the whole file, or `range` as a `206`). `keep` lives as
/// long as the body: pass what keeps the file in place, so a temp file goes when the client is
/// done or disconnects.
pub async fn file_response(
    path: &Path,
    size: u64,
    content_type: &'static str,
    etag: Option<&str>,
    range: Option<ByteRange>,
    keep: impl Send + Sync + 'static,
) -> Result<Response, ApiError> {
    let (status, first, len, content_range) = match range {
        None => (StatusCode::OK, 0, size, None),
        Some(r) => match r.resolve(size) {
            None => return Ok(range_not_satisfiable(Some(size))),
            Some((first, last)) => (
                StatusCode::PARTIAL_CONTENT,
                first,
                last - first + 1,
                Some(format!("bytes {first}-{last}/{size}")),
            ),
        },
    };
    let gone = |_| ApiError::new("internal", "internal");
    let mut file = tokio::fs::File::open(path).await.map_err(gone)?;
    if first > 0 {
        file.seek(std::io::SeekFrom::Start(first)).await.map_err(gone)?;
    }
    let stream = ReaderStream::with_capacity(file.take(len), CHUNK).map(move |chunk| {
        let _ = &keep;
        chunk
    });
    let mut resp = Body::from_stream(stream).into_response();
    *resp.status_mut() = status;
    let h = resp.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    h.insert(header::CONTENT_LENGTH, HeaderValue::from(len));
    if let Some(v) = content_range.and_then(|v| HeaderValue::from_str(&v).ok()) {
        h.insert(header::CONTENT_RANGE, v);
    }
    if let Some(v) = etag.and_then(|e| HeaderValue::from_str(&format!("\"{e}\"")).ok()) {
        h.insert(header::ETAG, v);
    }
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(v: &str) -> Result<Option<ByteRange>, u16> {
        let mut h = HeaderMap::new();
        h.insert(header::RANGE, HeaderValue::from_str(v).expect("header"));
        parse_range(&h).map_err(|r| r.status().as_u16())
    }

    #[test]
    fn valid_ranges_parse() {
        let from = |start, end| Ok(Some(ByteRange::From { start, end }));
        assert_eq!(parse("bytes=0-9"), from(0, Some(9)));
        assert_eq!(parse("bytes=5-"), from(5, None));
        assert_eq!(parse("bytes=-3"), Ok(Some(ByteRange::Suffix(3))));
        assert_eq!(parse("BYTES=1-1"), from(1, Some(1)));
        assert_eq!(parse(" bytes=7-7 "), from(7, Some(7)));
        assert_eq!(parse_range(&HeaderMap::new()).ok(), Some(None));
    }

    #[test]
    fn another_unit_is_ignored() {
        assert_eq!(parse("items=0-1"), Ok(None));
    }

    #[test]
    fn malformed_and_multi_ranges_are_refused() {
        for bad in [
            "bytes",
            "bytes=",
            "bytes=-",
            "bytes=abc",
            "bytes=a-b",
            "bytes=5-2",
            "bytes=+1-2",
            "bytes=-+1",
            "bytes=1-2-3",
            "bytes=0-1,3-4",
            "bytes=0-1, 3-4",
            "bytes=99999999999999999999999-",
        ] {
            assert_eq!(parse(bad), Err(416), "{bad}");
        }
    }

    #[test]
    fn resolve_clamps_and_refuses_out_of_bounds() {
        let from = |start, end| ByteRange::From { start, end };
        assert_eq!(from(0, None).resolve(10), Some((0, 9)));
        assert_eq!(from(9, Some(100)).resolve(10), Some((9, 9)));
        assert_eq!(from(10, None).resolve(10), None);
        assert_eq!(ByteRange::Suffix(3).resolve(10), Some((7, 9)));
        assert_eq!(ByteRange::Suffix(30).resolve(10), Some((0, 9)));
        assert_eq!(ByteRange::Suffix(0).resolve(10), None);
        assert_eq!(from(0, None).resolve(0), None);
    }
}

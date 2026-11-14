//! Response message framing (RFC 9112 section 6.3) and connection persistence (section 9.3).

use crate::error::FramingError;
use crate::parse::ResponseHead;
use http::header::{CONNECTION, CONTENT_LENGTH, TRANSFER_ENCODING};
use http::{HeaderMap, Method, Version};

/// How the response body is delimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyKind {
    /// No body at all (HEAD, 1xx, 204, 304, 2xx to CONNECT).
    Empty,
    /// Exactly this many bytes (may be 0).
    Length(u64),
    /// Chunked transfer coding.
    Chunked,
    /// Body runs until the server closes the connection.
    CloseDelimited,
}

/// Result of [`response_body_kind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Framing {
    /// Body delimiting.
    pub kind: BodyKind,
    /// The connection must not be reused afterwards (close-delimited body, or
    /// Transfer-Encoding together with Content-Length, or TE on HTTP/1.0).
    pub must_close: bool,
}

/// Split a comma separated header value into trimmed, non-empty-checked items.
pub(crate) fn list_items(v: &[u8]) -> impl DoubleEndedIterator<Item = &[u8]> {
    v.split(|&b| b == b',').map(|s| s.trim_ascii())
}

/// True when the final transfer coding across all values is `chunked`.
pub(crate) fn final_coding_is_chunked(headers: &HeaderMap) -> bool {
    let last = headers
        .get_all(TRANSFER_ENCODING)
        .iter()
        .next_back()
        .and_then(|v| list_items(v.as_bytes()).rfind(|i| !i.is_empty()));
    last.is_some_and(|c| c.eq_ignore_ascii_case(b"chunked"))
}

/// Parse all Content-Length values (each may be a comma list). `Ok(None)` if absent.
pub(crate) fn content_length(headers: &HeaderMap) -> Result<Option<u64>, FramingError> {
    let mut found: Option<u64> = None;
    for v in headers.get_all(CONTENT_LENGTH) {
        for item in list_items(v.as_bytes()) {
            if item.is_empty() || !item.iter().all(u8::is_ascii_digit) {
                return Err(FramingError::InvalidContentLength);
            }
            let mut n: u64 = 0;
            for &d in item {
                n = n
                    .checked_mul(10)
                    .and_then(|n| n.checked_add(u64::from(d - b'0')))
                    .ok_or(FramingError::InvalidContentLength)?;
            }
            match found {
                None => found = Some(n),
                Some(p) if p == n => {}
                Some(_) => return Err(FramingError::ConflictingContentLength),
            }
        }
    }
    Ok(found)
}

/// Determine how to read the response body. Zero allocations.
///
/// Rules, in order: HEAD request, 1xx, 204, 304 and 2xx-to-CONNECT are [`BodyKind::Empty`]
/// (Content-Length / Transfer-Encoding are not inspected). Otherwise Transfer-Encoding wins:
/// final coding `chunked` gives [`BodyKind::Chunked`], anything else close-delimited; if
/// Content-Length is also present, `must_close` is set. Otherwise Content-Length (identical
/// repeats allowed, differing or invalid is an error), otherwise close-delimited.
///
/// A 101 response yields `Empty`; the caller must treat the connection as switched.
pub fn response_body_kind(
    request_method: &Method,
    head: &ResponseHead,
) -> Result<Framing, FramingError> {
    let s = head.status;
    if *request_method == Method::HEAD
        || s.is_informational()
        || s.as_u16() == 204
        || s.as_u16() == 304
        || (*request_method == Method::CONNECT && s.is_success())
    {
        return Ok(Framing {
            kind: BodyKind::Empty,
            must_close: false,
        });
    }
    let h = &head.headers;
    if h.contains_key(TRANSFER_ENCODING) {
        let chunked = final_coding_is_chunked(h);
        let must_close =
            !chunked || h.contains_key(CONTENT_LENGTH) || head.version == Version::HTTP_10;
        return Ok(Framing {
            kind: if chunked {
                BodyKind::Chunked
            } else {
                BodyKind::CloseDelimited
            },
            must_close,
        });
    }
    Ok(match content_length(h)? {
        Some(n) => Framing {
            kind: BodyKind::Length(n),
            must_close: false,
        },
        None => Framing {
            kind: BodyKind::CloseDelimited,
            must_close: true,
        },
    })
}

/// Whether the connection may be kept alive according to the response head alone
/// (HTTP/1.1: unless `Connection: close`; HTTP/1.0: only with `keep-alive`).
/// Combine with [`Framing::must_close`] and the request's own `Connection` header.
pub fn keep_alive(head: &ResponseHead) -> bool {
    let mut close = false;
    let mut ka = false;
    for v in head.headers.get_all(CONNECTION) {
        for t in list_items(v.as_bytes()) {
            close |= t.eq_ignore_ascii_case(b"close");
            ka |= t.eq_ignore_ascii_case(b"keep-alive");
        }
    }
    if close {
        return false;
    }
    match head.version {
        Version::HTTP_10 => ka,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, StatusCode};

    fn head(status: u16, hs: &[(&'static str, &'static str)]) -> ResponseHead {
        let mut headers = HeaderMap::new();
        for (k, v) in hs {
            headers.append(
                http::HeaderName::from_static(k),
                HeaderValue::from_static(v),
            );
        }
        ResponseHead {
            version: Version::HTTP_11,
            status: StatusCode::from_u16(status).unwrap(),
            reason: None,
            headers,
        }
    }
    fn k(m: &Method, h: &ResponseHead) -> Result<Framing, FramingError> {
        response_body_kind(m, h)
    }

    #[test]
    fn empty_cases() {
        let cl = [("content-length", "5")];
        for s in [100u16, 101, 204, 304] {
            assert_eq!(
                k(&Method::GET, &head(s, &cl)).unwrap().kind,
                BodyKind::Empty
            );
        }
        assert_eq!(
            k(&Method::HEAD, &head(200, &cl)).unwrap().kind,
            BodyKind::Empty
        );
        assert_eq!(
            k(&Method::CONNECT, &head(200, &[])).unwrap().kind,
            BodyKind::Empty
        );
        // non-2xx CONNECT response has a normal body
        assert_eq!(
            k(&Method::CONNECT, &head(407, &cl)).unwrap().kind,
            BodyKind::Length(5)
        );
    }

    #[test]
    fn te_rules() {
        let f = k(
            &Method::GET,
            &head(200, &[("transfer-encoding", "gzip, chunked")]),
        )
        .unwrap();
        assert_eq!(
            f,
            Framing {
                kind: BodyKind::Chunked,
                must_close: false
            }
        );
        let f = k(
            &Method::GET,
            &head(200, &[("transfer-encoding", "chunked, gzip")]),
        )
        .unwrap();
        assert_eq!(
            f,
            Framing {
                kind: BodyKind::CloseDelimited,
                must_close: true
            }
        );
        let f = k(
            &Method::GET,
            &head(
                200,
                &[
                    ("transfer-encoding", "gzip"),
                    ("transfer-encoding", "ChunKed"),
                ],
            ),
        )
        .unwrap();
        assert_eq!(f.kind, BodyKind::Chunked);
        let f = k(
            &Method::GET,
            &head(
                200,
                &[
                    ("transfer-encoding", "chunked"),
                    ("content-length", "bogus"),
                ],
            ),
        )
        .unwrap();
        assert_eq!(
            f,
            Framing {
                kind: BodyKind::Chunked,
                must_close: true
            }
        );
    }

    #[test]
    fn content_length_rules() {
        let ok = |hs: &[(&'static str, &'static str)]| k(&Method::GET, &head(200, hs));
        assert_eq!(
            ok(&[("content-length", "42")]).unwrap().kind,
            BodyKind::Length(42)
        );
        assert_eq!(
            ok(&[("content-length", "7"), ("content-length", "7")])
                .unwrap()
                .kind,
            BodyKind::Length(7)
        );
        assert_eq!(
            ok(&[("content-length", "7, 7")]).unwrap().kind,
            BodyKind::Length(7)
        );
        assert_eq!(
            ok(&[("content-length", "7"), ("content-length", "8")]),
            Err(FramingError::ConflictingContentLength)
        );
        for bad in [
            "",
            "-1",
            "+1",
            "1 2",
            "0x10",
            "abc",
            "99999999999999999999",
            "1,",
        ] {
            assert_eq!(
                ok(&[("content-length", bad)]),
                Err(FramingError::InvalidContentLength),
                "{bad:?}"
            );
        }
        assert_eq!(
            ok(&[]).unwrap(),
            Framing {
                kind: BodyKind::CloseDelimited,
                must_close: true
            }
        );
    }

    #[test]
    fn keep_alive_rules() {
        assert!(keep_alive(&head(200, &[])));
        assert!(!keep_alive(&head(200, &[("connection", "close")])));
        assert!(!keep_alive(&head(
            200,
            &[("connection", "keep-alive, Close")]
        )));
        let mut h = head(200, &[]);
        h.version = Version::HTTP_10;
        assert!(!keep_alive(&h));
        h.headers
            .insert(CONNECTION, HeaderValue::from_static("Keep-Alive"));
        assert!(keep_alive(&h));
    }
}

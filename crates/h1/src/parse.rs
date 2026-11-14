//! Response head parsing.

use crate::error::ParseError;
use bytes::{Bytes, BytesMut};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Version};
use std::mem::MaybeUninit;

const STACK_HEADERS: usize = 128;

/// Parser limits and policy.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Maximum head size (status line + headers + final CRLF). Default 64 KiB.
    pub max_head_bytes: usize,
    /// Maximum number of header lines. Default 128 (stack scratch; more uses the heap).
    pub max_headers: usize,
    /// Accept obsolete line folding by replacing each fold with SP (RFC 9112 section 5.2).
    /// Default `false`: folded responses are rejected with [`ParseError::ObsFold`].
    pub allow_obs_fold: bool,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_head_bytes: 64 * 1024,
            max_headers: STACK_HEADERS,
            allow_obs_fold: false,
        }
    }
}

/// Capacity added to a head arena when it runs out.
const ARENA_CHUNK: usize = 8 * 1024;

/// A parsed response head. Header values are zero-copy slices of one shared `Bytes`.
#[derive(Debug, Clone)]
pub struct ResponseHead {
    /// `HTTP/1.0` or `HTTP/1.1`.
    pub version: Version,
    /// Status code (1xx included).
    pub status: StatusCode,
    /// Reason phrase, if non-empty.
    pub reason: Option<Bytes>,
    /// Headers; repeated fields are appended in order.
    pub headers: HeaderMap,
}

/// Outcome of a parse attempt.
#[derive(Debug)]
pub enum ParseStatus<T> {
    /// Parsed; the head occupied `buf[..head_len]`. Bytes after belong to the body / next message.
    Complete {
        /// The value.
        value: T,
        /// Bytes consumed.
        head_len: usize,
    },
    /// Need more bytes; call again with the longer buffer (re-parses from the start).
    Partial,
}

fn off(base: &[u8], sub: &[u8]) -> usize {
    (sub.as_ptr() as usize).wrapping_sub(base.as_ptr() as usize)
}

/// Parse a response head from a contiguous buffer.
///
/// 1xx responses (including 101) are returned like any other head: the caller loops,
/// discarding `buf[..head_len]` and parsing again until a final (non-1xx) status, except
/// that 101 ends the HTTP exchange.
///
/// Allocations on `Complete`: one exact-size copy of the head, one `Bytes` shared-state box
/// created by the first slice, two for `HeaderMap`, one per non-standard header name, plus
/// extra-value growth for repeated headers. `Partial` and errors do not allocate
/// (unless `max_headers > 128`, which allocates scratch space).
pub fn parse_response_head(
    buf: &[u8],
    limits: &Limits,
) -> Result<ParseStatus<ResponseHead>, ParseError> {
    parse_response_head_in(buf, limits, &mut BytesMut::new())
}

/// Like [`parse_response_head`], copying the head into `arena` instead of a fresh allocation: the copy is
/// `split().freeze()` off `arena`, so heads parsed with the same arena share chunk allocations (the arena
/// grows in chunks; its contents on entry are discarded and it is empty on a successful return).
pub fn parse_response_head_in(
    buf: &[u8],
    limits: &Limits,
    arena: &mut BytesMut,
) -> Result<ParseStatus<ResponseHead>, ParseError> {
    // A previous call that failed after copying leaves its bytes here.
    arena.clear();
    let max = limits.max_head_bytes;
    let view = &buf[..buf.len().min(max)];

    let mut stack = [const { MaybeUninit::<httparse::Header<'_>>::uninit() }; STACK_HEADERS];
    let mut heap: Vec<MaybeUninit<httparse::Header<'_>>>;
    let slots: &mut [MaybeUninit<httparse::Header<'_>>] = if limits.max_headers <= STACK_HEADERS {
        &mut stack[..limits.max_headers]
    } else {
        heap = Vec::new();
        heap.resize_with(limits.max_headers, MaybeUninit::uninit);
        &mut heap
    };

    let mut resp = httparse::Response::new(&mut []);
    let mut cfg = httparse::ParserConfig::default();
    cfg.allow_obsolete_multiline_headers_in_responses(limits.allow_obs_fold);
    let head_len = match cfg.parse_response_with_uninit_headers(&mut resp, view, slots) {
        Ok(httparse::Status::Complete(n)) => n,
        Ok(httparse::Status::Partial) => {
            return if buf.len() >= max {
                Err(ParseError::TooLarge)
            } else {
                Ok(ParseStatus::Partial)
            };
        }
        Err(e) => {
            return Err(match e {
                httparse::Error::HeaderName => {
                    // httparse reports a folded line as a bad header name.
                    if view
                        .windows(2)
                        .any(|w| w[0] == b'\n' && (w[1] == b' ' || w[1] == b'\t'))
                    {
                        ParseError::ObsFold
                    } else {
                        ParseError::InvalidHeaderName
                    }
                }
                httparse::Error::HeaderValue => ParseError::InvalidHeaderValue,
                httparse::Error::NewLine => ParseError::InvalidNewLine,
                httparse::Error::Status => ParseError::InvalidStatus,
                httparse::Error::Token => ParseError::Syntax,
                httparse::Error::TooManyHeaders => ParseError::TooManyHeaders,
                httparse::Error::Version => ParseError::InvalidVersion,
            });
        }
    };

    let version = match resp.version {
        Some(0) => Version::HTTP_10,
        Some(1) => Version::HTTP_11,
        _ => return Err(ParseError::InvalidVersion),
    };
    let status = resp
        .code
        .and_then(|c| StatusCode::from_u16(c).ok())
        .ok_or(ParseError::InvalidStatus)?;
    let reason = resp.reason.filter(|r| !r.is_empty()).map(str::as_bytes);
    let hs = &*resp.headers;

    // The single copy of the head.
    if arena.capacity() < head_len {
        arena.reserve(head_len.max(ARENA_CHUNK));
    }
    arena.extend_from_slice(&buf[..head_len]);
    let data = &mut arena[..];

    if limits.allow_obs_fold {
        // httparse reports a folded value as one span that still contains CRLF + whitespace;
        // replace the line breaks with SP in our private copy (RFC 9112 section 5.2).
        for h in hs.iter() {
            let s = off(buf, h.value);
            for b in &mut data[s..s + h.value.len()] {
                if *b == b'\r' || *b == b'\n' {
                    *b = b' ';
                }
            }
        }
    }

    let shared = arena.split().freeze();
    let reason = reason.map(|r| {
        let s = off(buf, r);
        shared.slice(s..s + r.len())
    });
    let mut headers = HeaderMap::with_capacity(hs.len());
    for h in hs.iter() {
        let name =
            HeaderName::from_bytes(h.name.as_bytes()).map_err(|_| ParseError::InvalidHeaderName)?;
        let s = off(buf, h.value);
        let value = shared.slice(s..s + h.value.len());
        if value.iter().any(|&b| (b < 0x20 && b != b'\t') || b == 0x7f) {
            return Err(ParseError::InvalidHeaderValue);
        }
        // SAFETY: every byte was just checked to be HTAB, SP..~ or >= 0x80, which is exactly the
        // HeaderValue invariant.
        let value = unsafe { HeaderValue::from_maybe_shared_unchecked(value) };
        headers.append(name, value);
    }

    Ok(ParseStatus::Complete {
        value: ResponseHead {
            version,
            status,
            reason,
            headers,
        },
        head_len,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(s: &[u8]) -> (ResponseHead, usize) {
        match parse_response_head(s, &Limits::default()).unwrap() {
            ParseStatus::Complete { value, head_len } => (value, head_len),
            ParseStatus::Partial => panic!("partial"),
        }
    }

    #[test]
    fn basic() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-A: 1\r\nx-a: 2\r\nContent-Length: 5\r\n\r\nhello";
        let (h, n) = parse_ok(raw);
        assert_eq!(&raw[n..], b"hello");
        assert_eq!(h.version, Version::HTTP_11);
        assert_eq!(h.status, 200);
        assert_eq!(h.reason.as_deref(), Some(&b"OK"[..]));
        assert_eq!(h.headers["content-type"], "text/plain");
        let xa: Vec<_> = h.headers.get_all("x-a").iter().collect();
        assert_eq!(xa, ["1", "2"]);
    }

    #[test]
    fn empty_reason_and_http10() {
        let (h, _) = parse_ok(b"HTTP/1.0 204 \r\n\r\n");
        assert_eq!(h.version, Version::HTTP_10);
        assert!(h.reason.is_none());
        let (h, _) = parse_ok(b"HTTP/1.1 204\r\n\r\n");
        assert!(h.reason.is_none());
    }

    #[test]
    fn split_at_every_byte() {
        let raw = b"HTTP/1.1 200 OK\r\nA: b\r\nCustom-Hdr: yes\r\n\r\nBODY";
        let head_len = raw.len() - 4;
        for i in 0..head_len {
            assert!(matches!(
                parse_response_head(&raw[..i], &Limits::default()),
                Ok(ParseStatus::Partial)
            ));
        }
        let (h, n) = parse_ok(&raw[..head_len + 2]);
        assert_eq!(n, head_len);
        assert_eq!(h.headers["custom-hdr"], "yes");
    }

    #[test]
    fn informational_then_final() {
        let raw = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
        let (h1, n1) = parse_ok(raw);
        assert_eq!(h1.status, 100);
        let (h2, n2) = parse_ok(&raw[n1..]);
        assert_eq!(h2.status, 200);
        assert_eq!(n1 + n2, raw.len());
        let (h, _) = parse_ok(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n");
        assert_eq!(h.status, 101);
    }

    #[test]
    fn errors() {
        let l = Limits::default();
        let e = |s: &[u8]| parse_response_head(s, &l).unwrap_err();
        assert_eq!(e(b"HTTP/2.0 200 OK\r\n\r\n"), ParseError::InvalidVersion);
        assert_eq!(e(b"HTTP/1.1 abc OK\r\n\r\n"), ParseError::InvalidStatus);
        assert_eq!(e(b"HTTP/1.1 99 OK\r\n\r\n"), ParseError::InvalidStatus);
        assert_eq!(
            e(b"HTTP/1.1 200 OK\r\nBad Name: x\r\n\r\n"),
            ParseError::InvalidHeaderName
        );
        assert_eq!(
            e(b"HTTP/1.1 200 OK\r\nA: b\x00c\r\n\r\n"),
            ParseError::InvalidHeaderValue
        );
        assert_eq!(
            e(b"HTTP/1.1 200 OK\r\nA: b\rc\r\n\r\n"),
            ParseError::InvalidHeaderValue
        );
        assert_eq!(
            e(b"HTTP/1.1 200 OK\r\nA : b\r\n\r\n"),
            ParseError::InvalidHeaderName
        );
    }

    #[test]
    fn size_limits() {
        let l = Limits {
            max_head_bytes: 32,
            ..Limits::default()
        };
        let raw = b"HTTP/1.1 200 OK\r\nA: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\r\n\r\n";
        assert_eq!(
            parse_response_head(raw, &l).unwrap_err(),
            ParseError::TooLarge
        );
        assert!(matches!(
            parse_response_head(&raw[..20], &l),
            Ok(ParseStatus::Partial)
        ));
        let l = Limits {
            max_headers: 2,
            ..Limits::default()
        };
        let raw = b"HTTP/1.1 200 OK\r\nA: 1\r\nB: 2\r\nC: 3\r\n\r\n";
        assert_eq!(
            parse_response_head(raw, &l).unwrap_err(),
            ParseError::TooManyHeaders
        );
        let l = Limits {
            max_headers: 300,
            ..Limits::default()
        };
        let mut big = b"HTTP/1.1 200 OK\r\n".to_vec();
        for i in 0..200 {
            big.extend_from_slice(format!("h{i}: v\r\n").as_bytes());
        }
        big.extend_from_slice(b"\r\n");
        match parse_response_head(&big, &l).unwrap() {
            ParseStatus::Complete { value, .. } => assert_eq!(value.headers.len(), 200),
            _ => panic!(),
        }
    }

    #[test]
    fn obs_fold() {
        let raw = b"HTTP/1.1 200 OK\r\nA: one\r\n two\r\n\tthree\r\nB: x\r\n\r\n";
        assert_eq!(
            parse_response_head(raw, &Limits::default()).unwrap_err(),
            ParseError::ObsFold
        );
        let l = Limits {
            allow_obs_fold: true,
            ..Limits::default()
        };
        match parse_response_head(raw, &l).unwrap() {
            ParseStatus::Complete { value, head_len } => {
                assert_eq!(head_len, raw.len());
                let a = value.headers["a"].to_str().unwrap();
                assert!(!a.contains(['\r', '\n']));
                assert_eq!(
                    a.split_ascii_whitespace().collect::<Vec<_>>(),
                    ["one", "two", "three"]
                );
                assert_eq!(value.headers["b"], "x");
                assert_eq!(value.headers.len(), 2);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn values_do_not_pin_input() {
        let mut big = vec![0u8; 1 << 16];
        let raw = b"HTTP/1.1 200 OK\r\nX: y\r\n\r\n";
        big[..raw.len()].copy_from_slice(raw);
        let (h, _) = parse_ok(&big);
        drop(big);
        assert_eq!(h.headers["x"], "y");
    }
}

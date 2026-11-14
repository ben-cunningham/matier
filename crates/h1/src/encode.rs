//! Request head and chunked-body framing encoders.

use crate::error::EncodeError;
use crate::framing::{content_length, final_coding_is_chunked};
use http::header::{CONTENT_LENGTH, HOST, TRANSFER_ENCODING};
use http::{HeaderMap, Method, Uri};

/// Declared request body framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyLength {
    /// No body. `POST`/`PUT`/`PATCH` get `Content-Length: 0`.
    None,
    /// Exactly this many bytes.
    Known(u64),
    /// Chunked transfer coding.
    Chunked,
}

/// Encoder options.
#[derive(Debug, Clone, Copy, Default)]
pub struct EncodeOptions {
    /// Write `Content-Type` instead of `content-type`. Default `false`.
    pub title_case_headers: bool,
    /// Use absolute-form request target (`GET http://host/path`), for proxies.
    pub absolute_form: bool,
}

/// `CRLF` that terminates every chunk's data.
pub const CHUNK_END: &[u8] = b"\r\n";

fn push_u64(dst: &mut Vec<u8>, mut n: u64) {
    let mut b = [0u8; 20];
    let mut i = b.len();
    loop {
        i -= 1;
        b[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    dst.extend_from_slice(&b[i..]);
}

fn push_name(dst: &mut Vec<u8>, name: &str, title: bool) {
    if !title {
        dst.extend_from_slice(name.as_bytes());
        return;
    }
    let mut up = true;
    for &b in name.as_bytes() {
        dst.push(if up { b.to_ascii_uppercase() } else { b });
        up = b == b'-';
    }
}

fn bad_value(v: &[u8]) -> bool {
    v.iter().any(|&b| b == b'\r' || b == b'\n' || b == 0)
}

fn strip_userinfo(a: &str) -> &str {
    a.rsplit_once('@').map_or(a, |(_, h)| h)
}

/// Header size above which nothing special is needed; just an estimate for `reserve`.
fn estimate(method: &Method, uri: &Uri, headers: &HeaderMap) -> usize {
    let mut n = method.as_str().len() + uri.path().len() + 128;
    n += uri.query().map_or(0, str::len);
    n += uri.authority().map_or(0, |a| a.as_str().len() * 2 + 16);
    for (k, v) in headers {
        n += k.as_str().len() + v.len() + 4;
    }
    n
}

/// Append an HTTP/1.1 request head to `dst`.
///
/// Writes `METHOD target HTTP/1.1`, `Host` (from the URI authority, userinfo stripped, unless
/// `headers` has one), the headers in map order, a framing header per `body`, and the blank
/// line. The target is origin-form (`/` if the path is empty), absolute-form when
/// [`EncodeOptions::absolute_form`] is set, and authority-form for `CONNECT`.
///
/// Framing headers already in `headers` are kept if consistent with `body` and rejected with
/// [`EncodeError::ConflictingFraming`] otherwise. Values containing CR, LF or NUL are rejected.
/// On error `dst` is restored to its original length.
///
/// Allocations: none if `dst` already has the (estimated) capacity; otherwise one `reserve`.
pub fn encode_request_head(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: BodyLength,
    opts: &EncodeOptions,
    dst: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let start = dst.len();
    dst.reserve(estimate(method, uri, headers));
    let r = encode_inner(method, uri, headers, body, opts, dst);
    if r.is_err() {
        dst.truncate(start);
    }
    r
}

fn encode_inner(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: BodyLength,
    opts: &EncodeOptions,
    dst: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let title = opts.title_case_headers;
    dst.extend_from_slice(method.as_str().as_bytes());
    dst.push(b' ');
    let authority = uri.authority().map(|a| strip_userinfo(a.as_str()));
    if *method == Method::CONNECT {
        dst.extend_from_slice(authority.ok_or(EncodeError::MissingAuthority)?.as_bytes());
    } else if opts.absolute_form {
        let scheme = uri.scheme_str().ok_or(EncodeError::MissingScheme)?;
        let auth = authority.ok_or(EncodeError::MissingAuthority)?;
        dst.extend_from_slice(scheme.as_bytes());
        dst.extend_from_slice(b"://");
        dst.extend_from_slice(auth.as_bytes());
        dst.extend_from_slice(uri.path_and_query().map_or("/", |p| p.as_str()).as_bytes());
    } else {
        dst.extend_from_slice(uri.path_and_query().map_or("/", |p| p.as_str()).as_bytes());
    }
    dst.extend_from_slice(b" HTTP/1.1\r\n");

    if !headers.contains_key(HOST) {
        push_name(dst, "host", title);
        dst.extend_from_slice(b": ");
        dst.extend_from_slice(authority.ok_or(EncodeError::MissingAuthority)?.as_bytes());
        dst.extend_from_slice(b"\r\n");
    }
    for (k, v) in headers {
        if bad_value(v.as_bytes()) {
            return Err(EncodeError::InvalidHeaderValue);
        }
        push_name(dst, k.as_str(), title);
        dst.extend_from_slice(b": ");
        dst.extend_from_slice(v.as_bytes());
        dst.extend_from_slice(b"\r\n");
    }

    let has_cl = headers.contains_key(CONTENT_LENGTH);
    let has_te = headers.contains_key(TRANSFER_ENCODING);
    let cl = if has_cl {
        Some(
            content_length(headers)
                .ok()
                .flatten()
                .ok_or(EncodeError::ConflictingFraming)?,
        )
    } else {
        None
    };
    let conflict = Err(EncodeError::ConflictingFraming);
    match body {
        BodyLength::None => {
            if has_te || cl.is_some_and(|n| n != 0) {
                return conflict;
            }
            if !has_cl && matches!(*method, Method::POST | Method::PUT | Method::PATCH) {
                push_name(dst, "content-length", title);
                dst.extend_from_slice(b": 0\r\n");
            }
        }
        BodyLength::Known(n) => {
            if has_te || cl.is_some_and(|c| c != n) {
                return conflict;
            }
            if !has_cl {
                push_name(dst, "content-length", title);
                dst.extend_from_slice(b": ");
                push_u64(dst, n);
                dst.extend_from_slice(b"\r\n");
            }
        }
        BodyLength::Chunked => {
            if has_cl || (has_te && !final_coding_is_chunked(headers)) {
                return conflict;
            }
            if !has_te {
                push_name(dst, "transfer-encoding", title);
                dst.extend_from_slice(b": chunked\r\n");
            }
        }
    }
    dst.extend_from_slice(b"\r\n");
    Ok(())
}

/// Format a chunk-size line (`hex(len)` + CRLF) into a stack buffer; no allocation.
/// Write it, then the data, then [`CHUNK_END`], e.g. as three iovecs.
pub fn chunk_header(len: usize, buf: &mut [u8; 18]) -> &[u8] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    buf[16] = b'\r';
    buf[17] = b'\n';
    let mut i = 16;
    let mut n = len as u64;
    loop {
        i -= 1;
        buf[i] = HEX[(n & 0xf) as usize];
        n >>= 4;
        if n == 0 {
            break;
        }
    }
    &buf[i..]
}

/// Append the terminating `0\r\n`, optional trailer fields, and final CRLF.
/// On error (a trailer value containing CR/LF/NUL) `dst` is restored.
pub fn encode_last_chunk(
    trailers: Option<&HeaderMap>,
    dst: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let start = dst.len();
    dst.extend_from_slice(b"0\r\n");
    if let Some(t) = trailers {
        for (k, v) in t {
            if bad_value(v.as_bytes()) {
                dst.truncate(start);
                return Err(EncodeError::InvalidHeaderValue);
            }
            dst.extend_from_slice(k.as_str().as_bytes());
            dst.extend_from_slice(b": ");
            dst.extend_from_slice(v.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
    }
    dst.extend_from_slice(b"\r\n");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderName, HeaderValue};

    fn enc(
        m: &Method,
        u: &str,
        hs: &[(&str, &str)],
        body: BodyLength,
        o: &EncodeOptions,
    ) -> Result<String, EncodeError> {
        let mut h = HeaderMap::new();
        for (k, v) in hs {
            h.append(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        let mut d = Vec::new();
        encode_request_head(m, &u.parse().unwrap(), &h, body, o, &mut d)?;
        Ok(String::from_utf8(d).unwrap())
    }
    const D: EncodeOptions = EncodeOptions {
        title_case_headers: false,
        absolute_form: false,
    };

    #[test]
    fn origin_form() {
        let s = enc(
            &Method::GET,
            "http://example.com/a/b?x=1",
            &[("accept", "*/*")],
            BodyLength::None,
            &D,
        )
        .unwrap();
        assert_eq!(
            s,
            "GET /a/b?x=1 HTTP/1.1\r\nhost: example.com\r\naccept: */*\r\n\r\n"
        );
        let s = enc(
            &Method::GET,
            "http://u:p@example.com:8080",
            &[],
            BodyLength::None,
            &D,
        )
        .unwrap();
        assert_eq!(s, "GET / HTTP/1.1\r\nhost: example.com:8080\r\n\r\n");
    }

    #[test]
    fn explicit_host_not_duplicated() {
        let s = enc(
            &Method::GET,
            "/x",
            &[("host", "h.test")],
            BodyLength::None,
            &D,
        )
        .unwrap();
        assert_eq!(s, "GET /x HTTP/1.1\r\nhost: h.test\r\n\r\n");
        assert_eq!(
            enc(&Method::GET, "/x", &[], BodyLength::None, &D),
            Err(EncodeError::MissingAuthority)
        );
    }

    #[test]
    fn title_case() {
        let o = EncodeOptions {
            title_case_headers: true,
            ..D
        };
        let s = enc(
            &Method::POST,
            "http://a/",
            &[("x-custom-hdr", "1"), ("content-type", "t")],
            BodyLength::Known(12),
            &o,
        )
        .unwrap();
        assert_eq!(
            s,
            "POST / HTTP/1.1\r\nHost: a\r\nX-Custom-Hdr: 1\r\nContent-Type: t\r\nContent-Length: 12\r\n\r\n"
        );
    }

    #[test]
    fn connect_authority_form() {
        let s = enc(
            &Method::CONNECT,
            "example.com:443",
            &[],
            BodyLength::None,
            &D,
        )
        .unwrap();
        assert_eq!(
            s,
            "CONNECT example.com:443 HTTP/1.1\r\nhost: example.com:443\r\n\r\n"
        );
        assert_eq!(
            enc(&Method::CONNECT, "/path", &[], BodyLength::None, &D),
            Err(EncodeError::MissingAuthority)
        );
    }

    #[test]
    fn absolute_form() {
        let o = EncodeOptions {
            absolute_form: true,
            ..D
        };
        let s = enc(
            &Method::GET,
            "http://example.com/p?q=1",
            &[],
            BodyLength::None,
            &o,
        )
        .unwrap();
        assert_eq!(
            s,
            "GET http://example.com/p?q=1 HTTP/1.1\r\nhost: example.com\r\n\r\n"
        );
        let s = enc(
            &Method::GET,
            "https://example.com",
            &[],
            BodyLength::None,
            &o,
        )
        .unwrap();
        assert!(s.starts_with("GET https://example.com/ HTTP/1.1\r\n"));
        assert_eq!(
            enc(&Method::GET, "/p", &[("host", "a")], BodyLength::None, &o),
            Err(EncodeError::MissingScheme)
        );
    }

    #[test]
    fn framing_headers() {
        let c = |m, hs: &[(&str, &str)], b| enc(&m, "http://a/", hs, b, &D);
        let s = c(Method::POST, &[], BodyLength::None).unwrap();
        assert!(s.contains("content-length: 0\r\n"));
        let s = c(Method::GET, &[], BodyLength::None).unwrap();
        assert!(!s.contains("content-length"));
        let s = c(Method::POST, &[], BodyLength::Chunked).unwrap();
        assert!(s.ends_with("transfer-encoding: chunked\r\n\r\n"));
        // consistent existing headers are not duplicated
        let s = c(
            Method::POST,
            &[("content-length", "5")],
            BodyLength::Known(5),
        )
        .unwrap();
        assert_eq!(s.matches("content-length").count(), 1);
        let s = c(
            Method::POST,
            &[("transfer-encoding", "gzip, chunked")],
            BodyLength::Chunked,
        )
        .unwrap();
        assert_eq!(s.matches("transfer-encoding").count(), 1);
        // conflicts
        for (hs, b) in [
            (&[("content-length", "5")][..], BodyLength::Known(6)),
            (&[("content-length", "5")][..], BodyLength::Chunked),
            (&[("content-length", "x")][..], BodyLength::Known(5)),
            (&[("content-length", "5")][..], BodyLength::None),
            (
                &[("transfer-encoding", "chunked")][..],
                BodyLength::Known(1),
            ),
            (&[("transfer-encoding", "gzip")][..], BodyLength::Chunked),
        ] {
            assert_eq!(
                c(Method::POST, hs, b),
                Err(EncodeError::ConflictingFraming),
                "{hs:?}"
            );
        }
    }

    #[test]
    fn injection_check() {
        // `HeaderValue` already forbids these; the encoder re-checks as defense in depth.
        assert!(bad_value(b"a\r\nEvil: 1"));
        assert!(bad_value(b"a\0"));
        assert!(!bad_value(b"a b\tc"));
    }

    #[test]
    fn no_realloc_when_reserved() {
        let mut d = Vec::with_capacity(4096);
        let p = d.as_ptr();
        let h = HeaderMap::new();
        encode_request_head(
            &Method::GET,
            &"http://a/".parse().unwrap(),
            &h,
            BodyLength::None,
            &D,
            &mut d,
        )
        .unwrap();
        assert_eq!(p, d.as_ptr());
    }

    #[test]
    fn chunk_framing() {
        let mut b = [0u8; 18];
        assert_eq!(chunk_header(0, &mut b), b"0\r\n");
        assert_eq!(chunk_header(255, &mut b), b"ff\r\n");
        assert_eq!(chunk_header(0x1234abc, &mut b), b"1234abc\r\n");
        assert_eq!(chunk_header(usize::MAX, &mut b), b"ffffffffffffffff\r\n");
        let mut d = Vec::new();
        encode_last_chunk(None, &mut d).unwrap();
        assert_eq!(d, b"0\r\n\r\n");
        let mut t = HeaderMap::new();
        t.insert("x-sum", HeaderValue::from_static("abc"));
        d.clear();
        encode_last_chunk(Some(&t), &mut d).unwrap();
        assert_eq!(d, b"0\r\nx-sum: abc\r\n\r\n");
    }
}

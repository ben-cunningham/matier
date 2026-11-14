//! Verifies the documented allocation counts with a counting global allocator.
//! Single test function so no other thread allocates concurrently.

use http::{HeaderMap, HeaderName, HeaderValue, Method};
use matier_h1::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

struct Counting;
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        unsafe { System.realloc(p, l, n) }
    }
}
#[global_allocator]
static A: Counting = Counting;

fn count<R>(f: impl FnOnce() -> R) -> (usize, R) {
    let b = ALLOCS.load(Relaxed);
    let r = f();
    (ALLOCS.load(Relaxed) - b, r)
}

#[test]
fn allocation_counts() {
    let uri: http::Uri = "http://example.com/x?y=1".parse().unwrap();
    let mut h = HeaderMap::new();
    h.insert(
        HeaderName::from_static("accept"),
        HeaderValue::from_static("*/*"),
    );
    let mut dst = Vec::with_capacity(1024);
    let o = EncodeOptions::default();
    let (n, r) =
        count(|| encode_request_head(&Method::GET, &uri, &h, BodyLength::None, &o, &mut dst));
    r.unwrap();
    assert_eq!(n, 0, "encode with capacity");

    let mut b = [0u8; 18];
    let (n, _) = count(|| chunk_header(4096, &mut b).len());
    assert_eq!(n, 0);

    let raw =
        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\nServer: x\r\n\r\n";
    let l = Limits::default();
    let (n, r) = count(|| parse_response_head(&raw[..20], &l));
    assert!(matches!(r, Ok(ParseStatus::Partial)));
    assert_eq!(n, 0, "partial");
    let (n, r) = count(|| parse_response_head(raw, &l));
    let ParseStatus::Complete { value, .. } = r.unwrap() else {
        panic!()
    };
    println!("parse (3 std headers + reason): {n} allocations");
    assert_eq!(n, 4, "head copy + Bytes shared + HeaderMap x2");

    let (n, f) = count(|| response_body_kind(&Method::GET, &value));
    f.unwrap();
    assert_eq!(n, 0);
    assert_eq!(count(|| keep_alive(&value)).0, 0);

    let wire = b"5\r\nhello\r\n0\r\n\r\n";
    let (n, _) = count(|| {
        let mut d = ChunkedDecoder::new();
        let mut p = 0;
        while !d.is_done() {
            p += d.decode(&wire[p..]).unwrap().consumed;
        }
    });
    assert_eq!(n, 0);
}

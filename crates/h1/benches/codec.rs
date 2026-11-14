use criterion::{Criterion, criterion_group, criterion_main};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Uri};
use matier_h1::{BodyLength, EncodeOptions, Limits, encode_request_head, parse_response_head};
use std::hint::black_box;

const RESP: &[u8] = b"HTTP/1.1 200 OK\r\n\
Date: Sat, 14 Nov 2026 12:00:00 GMT\r\n\
Content-Type: application/json; charset=utf-8\r\n\
Content-Length: 1234\r\n\
Connection: keep-alive\r\n\
Server: nginx/1.25.3\r\n\
Cache-Control: max-age=3600, public\r\n\
ETag: \"33a64df551425fcc55e4d42a148795d9f25f89d4\"\r\n\
Last-Modified: Fri, 13 Nov 2026 08:30:00 GMT\r\n\
Vary: Accept-Encoding\r\n\
Strict-Transport-Security: max-age=63072000; includeSubDomains\r\n\
X-Request-Id: 7f3c2a1e-9b4d-4e8a-a1c2-0d5e6f7a8b9c\r\n\
X-Content-Type-Options: nosniff\r\n\r\n";

fn bench(c: &mut Criterion) {
    let limits = Limits::default();
    c.bench_function("parse_response_head/12_headers", |b| {
        b.iter(|| black_box(parse_response_head(black_box(RESP), &limits).unwrap()))
    });

    let uri: Uri = "https://api.example.com/v1/items?limit=50&cursor=abc"
        .parse()
        .unwrap();
    let mut headers = HeaderMap::new();
    for (k, v) in [
        ("accept", "application/json"),
        ("accept-encoding", "gzip, br"),
        ("user-agent", "matier/0.0"),
        (
            "authorization",
            "Bearer abcdefghijklmnopqrstuvwxyz0123456789",
        ),
        ("x-request-id", "7f3c2a1e-9b4d-4e8a-a1c2-0d5e6f7a8b9c"),
    ] {
        headers.insert(HeaderName::from_static(k), HeaderValue::from_static(v));
    }
    let opts = EncodeOptions::default();
    let mut dst = Vec::with_capacity(1024);
    c.bench_function("encode_request_head/5_headers", |b| {
        b.iter(|| {
            dst.clear();
            encode_request_head(
                &Method::GET,
                &uri,
                &headers,
                BodyLength::None,
                &opts,
                &mut dst,
            )
            .unwrap();
            black_box(dst.len())
        })
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);

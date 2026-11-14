use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use http::{HeaderMap, HeaderValue, Method, Uri};
use matier_h2::frame::*;
use matier_h2::hpack::{Decoder, Encoder, huffman};
use matier_h2::{Reason, hpack};
use std::hint::black_box;

fn mixed_frames() -> Vec<u8> {
    let mut enc = Encoder::new(4096);
    let mut block = Vec::new();
    enc.encode_field(b":status", b"200", false, &mut block);
    enc.encode_field(b"content-type", b"text/html", false, &mut block);
    let mut v = Vec::new();
    for i in 0..100u32 {
        let sid = 1 + 2 * (i % 10);
        match i % 5 {
            0 => encode_headers(&mut v, sid, false, &block, 16_384),
            1 | 2 => {
                v.extend_from_slice(&data_frame_header(sid, 1000, false));
                v.extend_from_slice(&[0xab; 1000]);
            }
            3 => encode_window_update(&mut v, sid, 65_535),
            _ => match i % 3 {
                0 => encode_ping(&mut v, false, [i as u8; 8]),
                1 => encode_settings(&mut v, &[(4, 1 << 20), (5, 32_768)]),
                _ => encode_rst_stream(&mut v, sid, Reason::CANCEL),
            },
        }
    }
    v
}

fn request_headers() -> (Method, Uri, HeaderMap) {
    let mut h = HeaderMap::new();
    h.insert("user-agent", HeaderValue::from_static("matier/0.1"));
    h.insert(
        "accept",
        HeaderValue::from_static("text/html,application/xhtml+xml,*/*;q=0.8"),
    );
    h.insert("accept-encoding", HeaderValue::from_static("gzip, deflate"));
    h.insert(
        "accept-language",
        HeaderValue::from_static("en-US,en;q=0.9"),
    );
    h.insert("cache-control", HeaderValue::from_static("no-cache"));
    h.insert(
        "cookie",
        HeaderValue::from_static("session=0123456789abcdef0123456789abcdef; theme=dark"),
    );
    h.insert(
        "x-request-id",
        HeaderValue::from_static("8f14e45f-ceea-4672-b0a1-1c2b3d4e5f60"),
    );
    (
        Method::GET,
        "https://api.example.com/v1/items?page=3".parse().unwrap(),
        h,
    )
}

fn response_fields() -> Vec<(&'static str, &'static str)> {
    vec![
        (":status", "200"),
        ("content-type", "application/json; charset=utf-8"),
        ("content-length", "1234"),
        ("date", "Mon, 21 Oct 2013 20:13:22 GMT"),
        ("server", "matier-bench"),
        ("cache-control", "private, max-age=0"),
        ("etag", "\"33a64df551425fcc55e4d42a148795d9f25f89d4\""),
        ("vary", "accept-encoding"),
        ("x-request-id", "8f14e45f-ceea-4672-b0a1-1c2b3d4e5f60"),
        ("strict-transport-security", "max-age=31536000"),
    ]
}

fn benches(c: &mut Criterion) {
    let frames = mixed_frames();
    let mut g = c.benchmark_group("frame");
    g.throughput(Throughput::Bytes(frames.len() as u64));
    g.bench_function("decode_100_mixed", |b| {
        b.iter(|| {
            let mut buf = black_box(&frames[..]);
            let mut n = 0;
            while let Some((f, used)) = decode_frame(buf, 16_384).unwrap() {
                black_box(&f);
                buf = &buf[used..];
                n += 1;
            }
            assert_eq!(n, 100);
        })
    });
    g.finish();

    let mut g = c.benchmark_group("hpack");
    let (m, u, h) = request_headers();
    let mut enc = Encoder::new(4096);
    let mut dec = Decoder::new(4096);
    let mut out = Vec::with_capacity(1024);
    enc.encode_request(&m, &u, &h, &mut out);
    dec.decode(&Bytes::copy_from_slice(&out), |_, _, _| {})
        .unwrap();
    g.bench_function("request_10_headers_encode_warm", |b| {
        b.iter(|| {
            out.clear();
            enc.encode_request(&m, &u, black_box(&h), &mut out);
            black_box(out.len())
        })
    });
    let block = Bytes::copy_from_slice(&out);
    g.bench_function("request_decode_warm", |b| {
        b.iter(|| {
            let mut n = 0;
            dec.decode(black_box(&block), |_, _, _| n += 1).unwrap();
            n
        })
    });
    g.bench_function("request_roundtrip_warm", |b| {
        b.iter(|| {
            out.clear();
            enc.encode_request(&m, &u, &h, &mut out);
            let blk = Bytes::copy_from_slice(&out);
            let mut n = 0;
            dec.decode(&blk, |_, _, _| n += 1).unwrap();
            n
        })
    });
    let mut sl = Encoder::stateless();
    g.bench_function("request_encode_stateless", |b| {
        b.iter(|| {
            out.clear();
            sl.encode_request(&m, &u, black_box(&h), &mut out);
            black_box(out.len())
        })
    });

    let resp = response_fields();
    let mut renc = Encoder::new(4096);
    let mut rdec = Decoder::new(4096);
    let mut rout = Vec::with_capacity(1024);
    let encode_resp = |renc: &mut Encoder, rout: &mut Vec<u8>| {
        rout.clear();
        for (n, v) in &resp {
            renc.encode_field(n.as_bytes(), v.as_bytes(), false, rout);
        }
    };
    encode_resp(&mut renc, &mut rout);
    rdec.decode(&Bytes::copy_from_slice(&rout), |_, _, _| {})
        .unwrap();
    g.bench_function("response_10_headers_roundtrip_warm", |b| {
        b.iter(|| {
            encode_resp(&mut renc, &mut rout);
            let blk = Bytes::copy_from_slice(&rout);
            let mut n = 0;
            rdec.decode(&blk, |_, _, _| n += 1).unwrap();
            n
        })
    });
    let rblock = Bytes::copy_from_slice(&rout);
    g.bench_function("response_decode_head_warm", |b| {
        b.iter(|| {
            rdec.decode_head(black_box(&rblock), hpack::BlockKind::Response, 1)
                .unwrap()
        })
    });
    g.finish();

    let text: Vec<u8> = b"The quick brown fox jumps over the lazy dog; text/html, application/json; charset=utf-8. "
        .iter()
        .cycle()
        .take(1024)
        .copied()
        .collect();
    let mut enc = Vec::new();
    huffman::encode(&text, &mut enc);
    let mut g = c.benchmark_group("huffman");
    g.throughput(Throughput::Bytes(1024));
    let mut o = Vec::with_capacity(2048);
    g.bench_function("decode_1KiB", |b| {
        b.iter(|| {
            o.clear();
            huffman::decode(black_box(&enc), &mut o, usize::MAX).unwrap();
            o.len()
        })
    });
    g.bench_function("encode_1KiB", |b| {
        b.iter(|| {
            o.clear();
            huffman::encode(black_box(&text), &mut o);
            o.len()
        })
    });
    g.finish();
}

criterion_group!(h2, benches);
criterion_main!(h2);

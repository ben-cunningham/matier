use http::{HeaderMap, HeaderName, HeaderValue, Method};
use matier_h1::*;
use proptest::prelude::*;

/// Test-only response encoder.
fn encode_response(status: u16, headers: &[(String, String)]) -> Vec<u8> {
    let mut v = format!("HTTP/1.1 {status} X\r\n").into_bytes();
    for (k, val) in headers {
        v.extend_from_slice(format!("{k}: {val}\r\n").as_bytes());
    }
    v.extend_from_slice(b"\r\n");
    v
}

fn header_strategy() -> impl Strategy<Value = Vec<(String, String)>> {
    prop::collection::vec(
        ("[A-Za-z][A-Za-z0-9-]{0,15}", "[!-~]([ -~]{0,30}[!-~])?"),
        0..40,
    )
}

proptest! {
    #[test]
    fn head_round_trip(hs in header_strategy(), status in 200u16..600) {
        let raw = encode_response(status, &hs);
        let mut buf = raw.clone();
        buf.extend_from_slice(b"BODY");
        let ParseStatus::Complete { value, head_len } =
            parse_response_head(&buf, &Limits::default()).unwrap() else { panic!() };
        prop_assert_eq!(head_len, raw.len());
        prop_assert_eq!(value.status.as_u16(), status);
        // Expected: grouping by lowercase name preserves per-name order.
        let mut expect = HeaderMap::new();
        for (k, v) in &hs {
            expect.append(HeaderName::from_bytes(k.as_bytes()).unwrap(), HeaderValue::from_str(v).unwrap());
        }
        prop_assert_eq!(&value.headers, &expect);
        // Any strict prefix is Partial.
        for cut in [0, raw.len() / 2, raw.len() - 1] {
            prop_assert!(matches!(parse_response_head(&raw[..cut], &Limits::default()), Ok(ParseStatus::Partial)));
        }
    }

    #[test]
    fn request_encode_then_parse_as_headers(hs in header_strategy()) {
        let mut map = HeaderMap::new();
        for (k, v) in &hs {
            let n = HeaderName::from_bytes(k.as_bytes()).unwrap();
            if n == http::header::CONTENT_LENGTH || n == http::header::TRANSFER_ENCODING || n == http::header::HOST { continue; }
            map.append(n, HeaderValue::from_str(v).unwrap());
        }
        let mut dst = Vec::new();
        encode_request_head(&Method::POST, &"http://h.test/p".parse().unwrap(), &map, BodyLength::Known(3), &EncodeOptions::default(), &mut dst).unwrap();
        // Reuse the response parser by swapping the request line for a status line.
        let line_end = dst.windows(2).position(|w| w == b"\r\n").unwrap();
        let mut resp = b"HTTP/1.1 200 OK".to_vec();
        resp.extend_from_slice(&dst[line_end..]);
        let ParseStatus::Complete { value, .. } = parse_response_head(&resp, &Limits::default()).unwrap() else { panic!() };
        prop_assert_eq!(value.headers["host"].as_bytes(), b"h.test");
        prop_assert_eq!(value.headers["content-length"].as_bytes(), b"3");
        for (k, _) in map.iter() {
            prop_assert_eq!(value.headers.get_all(k).iter().collect::<Vec<_>>(), map.get_all(k).iter().collect::<Vec<_>>());
        }
    }

    #[test]
    fn chunked_round_trip(
        chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 1..200), 0..12),
        step in 1usize..64,
        trailer in any::<bool>(),
    ) {
        let mut wire = Vec::new();
        let mut hb = [0u8; 18];
        for c in &chunks {
            wire.extend_from_slice(chunk_header(c.len(), &mut hb));
            wire.extend_from_slice(c);
            wire.extend_from_slice(CHUNK_END);
        }
        let mut t = HeaderMap::new();
        t.insert("x-trailer", HeaderValue::from_static("v"));
        encode_last_chunk(trailer.then_some(&t), &mut wire).unwrap();
        let total = wire.len();
        wire.extend_from_slice(b"TAIL");

        let mut d = ChunkedDecoder::new().collect_trailers();
        let mut out = Vec::new();
        let (mut pos, mut fed) = (0usize, 0usize);
        while !d.is_done() {
            fed = (fed + step).min(wire.len());
            let avail = &wire[pos..fed];
            let r = d.decode(avail).unwrap();
            if let Some(rg) = r.data { out.extend_from_slice(&avail[rg]); }
            pos += r.consumed;
            prop_assert!(fed < wire.len() || r.consumed > 0 || r.done || fed == pos);
        }
        prop_assert_eq!(pos, total);
        prop_assert_eq!(out, chunks.concat());
        let tr = d.take_trailers().unwrap();
        prop_assert_eq!(tr.is_some(), trailer);
    }

    #[test]
    fn decoder_never_panics(data in prop::collection::vec(any::<u8>(), 0..300)) {
        let mut d = ChunkedDecoder::new().collect_trailers();
        let _ = d.decode(&data);
        let _ = d.take_trailers();
        let _ = parse_response_head(&data, &Limits::default());
    }
}

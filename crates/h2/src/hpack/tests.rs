use super::*;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Uri};
use proptest::prelude::*;

fn hex(s: &str) -> Bytes {
    let s: String = s.split_whitespace().collect();
    Bytes::from(
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect::<Vec<u8>>(),
    )
}

type Fields = Vec<(String, String)>;

fn decode_all(d: &mut Decoder, block: &Bytes) -> Result<Fields, DecodeError> {
    let mut v = Vec::new();
    d.decode(block, |n, val, _| {
        v.push((
            String::from_utf8_lossy(&n).into_owned(),
            String::from_utf8_lossy(&val).into_owned(),
        ))
    })?;
    Ok(v)
}

fn f(pairs: &[(&str, &str)]) -> Fields {
    pairs
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

#[test]
fn c2_single_representations() {
    let mut d = Decoder::new(4096);
    // C.2.1
    let r = decode_all(
        &mut d,
        &hex("400a637573746f6d2d6b65790d637573746f6d2d686561646572"),
    )
    .unwrap();
    assert_eq!(r, f(&[("custom-key", "custom-header")]));
    assert_eq!(d.table_size(), 55);
    // C.2.2
    let mut d = Decoder::new(4096);
    let r = decode_all(&mut d, &hex("040c2f73616d706c652f70617468")).unwrap();
    assert_eq!(r, f(&[(":path", "/sample/path")]));
    assert_eq!(d.table_size(), 0);
    // C.2.3 never indexed
    let mut d = Decoder::new(4096);
    let mut sens = false;
    d.decode(&hex("100870617373776f726406736563726574"), |n, v, s| {
        assert_eq!((&n[..], &v[..]), (&b"password"[..], &b"secret"[..]));
        sens = s;
    })
    .unwrap();
    assert!(sens);
    assert_eq!(d.table_size(), 0);
    // C.2.4
    let mut d = Decoder::new(4096);
    assert_eq!(
        decode_all(&mut d, &hex("82")).unwrap(),
        f(&[(":method", "GET")])
    );
}

fn run_requests(blocks: [&str; 3]) {
    let mut d = Decoder::new(4096);
    let r1 = decode_all(&mut d, &hex(blocks[0])).unwrap();
    assert_eq!(
        r1,
        f(&[
            (":method", "GET"),
            (":scheme", "http"),
            (":path", "/"),
            (":authority", "www.example.com")
        ])
    );
    assert_eq!(d.table_size(), 57);
    let r2 = decode_all(&mut d, &hex(blocks[1])).unwrap();
    assert_eq!(
        r2,
        f(&[
            (":method", "GET"),
            (":scheme", "http"),
            (":path", "/"),
            (":authority", "www.example.com"),
            ("cache-control", "no-cache")
        ])
    );
    assert_eq!(d.table_size(), 110);
    let r3 = decode_all(&mut d, &hex(blocks[2])).unwrap();
    assert_eq!(
        r3,
        f(&[
            (":method", "GET"),
            (":scheme", "https"),
            (":path", "/index.html"),
            (":authority", "www.example.com"),
            ("custom-key", "custom-value")
        ])
    );
    assert_eq!(d.table_size(), 164);
}

#[test]
fn c3_requests_plain() {
    run_requests([
        "828684410f7777772e6578616d706c652e636f6d",
        "828684be58086e6f2d6361636865",
        "828785bf400a637573746f6d2d6b65790c637573746f6d2d76616c7565",
    ]);
}

const C4: [&str; 3] = [
    "828684418cf1e3c2e5f23a6ba0ab90f4ff",
    "828684be5886a8eb10649cbf",
    "828785bf408825a849e95ba97d7f8925a849e95bb8e8b4bf",
];

#[test]
fn c4_requests_huffman() {
    run_requests(C4);
}

fn run_responses(blocks: [&str; 3]) {
    let mut d = Decoder::new(256);
    let loc = "https://www.example.com";
    let r1 = decode_all(&mut d, &hex(blocks[0])).unwrap();
    assert_eq!(
        r1,
        f(&[
            (":status", "302"),
            ("cache-control", "private"),
            ("date", "Mon, 21 Oct 2013 20:13:21 GMT"),
            ("location", loc)
        ])
    );
    assert_eq!(d.table_size(), 222);
    let r2 = decode_all(&mut d, &hex(blocks[1])).unwrap();
    assert_eq!(
        r2,
        f(&[
            (":status", "307"),
            ("cache-control", "private"),
            ("date", "Mon, 21 Oct 2013 20:13:21 GMT"),
            ("location", loc)
        ])
    );
    assert_eq!(d.table_size(), 222);
    let r3 = decode_all(&mut d, &hex(blocks[2])).unwrap();
    assert_eq!(
        r3,
        f(&[
            (":status", "200"),
            ("cache-control", "private"),
            ("date", "Mon, 21 Oct 2013 20:13:22 GMT"),
            ("location", loc),
            ("content-encoding", "gzip"),
            (
                "set-cookie",
                "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1"
            ),
        ])
    );
    assert_eq!(d.table_size(), 215);
}

#[test]
fn c5_responses_plain() {
    run_responses([
        "4803333032580770726976617465611d4d6f6e2c203231204f637420323031332032303a31333a323120474d546e1768747470733a2f2f7777772e6578616d706c652e636f6d",
        "4803333037c1c0bf",
        "88c1611d4d6f6e2c203231204f637420323031332032303a31333a323220474d54c05a04677a69707738666f6f3d4153444a4b48514b425a584f5157454f50495541585157454f49553b206d61782d6167653d333630303b2076657273696f6e3d31",
    ]);
}

#[test]
fn c6_responses_huffman() {
    run_responses([
        "488264025885aec3771a4b6196d07abe941054d444a8200595040b8166e082a62d1bff6e919d29ad171863c78f0b97c8e9ae82ae43d3",
        "4883640effc1c0bf",
        "88c16196d07abe941054d444a8200595040b8166e084a62d1bffc05a839bd9ab77ad94e7821dd7f2e6c7b335dfdfcd5b3960d5af27087f3672c1ab270fb5291f9587316065c003ed4ee5b1063d5007",
    ]);
}

#[test]
fn encoder_reproduces_c4() {
    let mut e = Encoder::new(4096);
    let reqs: [&[(&str, &str)]; 3] = [
        &[
            (":method", "GET"),
            (":scheme", "http"),
            (":path", "/"),
            (":authority", "www.example.com"),
        ],
        &[
            (":method", "GET"),
            (":scheme", "http"),
            (":path", "/"),
            (":authority", "www.example.com"),
            ("cache-control", "no-cache"),
        ],
        &[
            (":method", "GET"),
            (":scheme", "https"),
            (":path", "/index.html"),
            (":authority", "www.example.com"),
            ("custom-key", "custom-value"),
        ],
    ];
    for (i, r) in reqs.iter().enumerate() {
        let mut out = Vec::new();
        for (n, v) in *r {
            e.encode_field(n.as_bytes(), v.as_bytes(), false, &mut out);
        }
        assert_eq!(out, hex(C4[i]).to_vec(), "request {}", i + 1);
    }
}

#[test]
fn encoder_never_indexes_and_stateless() {
    let mut e = Encoder::new(4096);
    let mut out = Vec::new();
    e.encode_field(b"authorization", b"Bearer abc", false, &mut out);
    assert_eq!(out[0] & 0xf0, 0x10);
    assert_eq!(e.table_size(), 0);
    out.clear();
    e.encode_field(b"cookie", b"a=b", false, &mut out);
    assert_eq!(out[0] & 0xf0, 0x10);
    out.clear();
    e.encode_field(b"cookie", &[b'x'; 100], false, &mut out);
    assert_eq!(out[0] & 0xc0, 0x40);
    assert!(e.table_size() > 0);

    let mut s = Encoder::stateless();
    out.clear();
    s.encode_field(b"x-custom", b"value", false, &mut out);
    s.encode_field(b"x-custom", b"value", false, &mut out);
    assert_eq!(s.table_size(), 0);
    assert_eq!(out[0] & 0xf0, 0x00);
    let mut d = Decoder::new(4096);
    let r = decode_all(&mut d, &Bytes::from(out)).unwrap();
    assert_eq!(r.len(), 2);
    assert_eq!(d.table_len(), 0);
}

#[test]
fn table_size_update_signalling() {
    let mut e = Encoder::new(4096);
    let mut d = Decoder::new(4096);
    let mut out = Vec::new();
    e.encode_field(b"x-a", b"1", false, &mut out);
    decode_all(&mut d, &Bytes::from(std::mem::take(&mut out))).unwrap();
    // shrink to 0 then up to 100 before the next block: both updates emitted
    e.set_peer_table_size(0);
    e.set_peer_table_size(100);
    assert_eq!(e.table_size(), 0);
    e.begin_block(&mut out);
    assert_eq!(out, [0x20, 0x3f, 0x45]);
    e.encode_field(b"x-b", b"2", false, &mut out);
    let mut dec_limit = Decoder::new(100);
    // decoder with smaller limit (SETTINGS-bound) accepts <= 100
    decode_all(&mut dec_limit, &Bytes::from(out.clone())).unwrap();
    decode_all(&mut d, &Bytes::from(out)).unwrap();
    assert_eq!(d.table_capacity(), 100);
    assert_eq!(d.table_len(), 1);
}

#[test]
fn encode_request_and_validate_roundtrip() {
    let mut headers = HeaderMap::new();
    headers.insert("user-agent", HeaderValue::from_static("matier"));
    headers.insert("accept", HeaderValue::from_static("*/*"));
    headers.insert("connection", HeaderValue::from_static("close"));
    headers.insert("te", HeaderValue::from_static("gzip"));
    headers.insert("host", HeaderValue::from_static("ignored"));
    let mut a = HeaderValue::from_static("secret");
    a.set_sensitive(true);
    headers.insert("x-token", a);
    let uri: Uri = "https://example.com/a/b?q=1".parse().unwrap();
    let mut e = Encoder::new(4096);
    let mut out = Vec::with_capacity(512);
    e.encode_request(&Method::POST, &uri, &headers, &mut out);
    let mut d = Decoder::new(4096);
    let r = decode_all(&mut d, &Bytes::from(out)).unwrap();
    assert_eq!(
        &r[..4],
        &f(&[
            (":method", "POST"),
            (":scheme", "https"),
            (":authority", "example.com"),
            (":path", "/a/b?q=1")
        ])[..]
    );
    let names: Vec<_> = r.iter().map(|x| x.0.as_str()).collect();
    assert!(!names.contains(&"connection") && !names.contains(&"te") && !names.contains(&"host"));
    assert!(names.contains(&"x-token") && names.contains(&"user-agent"));
}

#[test]
fn encode_request_zero_alloc_stateless_capacity() {
    let mut e = Encoder::stateless();
    let mut out = Vec::with_capacity(1024);
    let ptr = out.as_ptr();
    let mut headers = HeaderMap::new();
    headers.insert("accept", HeaderValue::from_static("*/*"));
    e.encode_request(
        &Method::GET,
        &"https://example.com/".parse().unwrap(),
        &headers,
        &mut out,
    );
    assert_eq!(out.as_ptr(), ptr);
    assert!(out.len() < 1024);
}

#[test]
fn connect_forms() {
    let mut e = Encoder::stateless();
    let mut out = Vec::new();
    e.encode_request(
        &Method::CONNECT,
        &"example.com:443".parse().unwrap(),
        &HeaderMap::new(),
        &mut out,
    );
    let mut d = Decoder::new(4096);
    let r = decode_all(&mut d, &Bytes::from(out)).unwrap();
    assert_eq!(
        r,
        f(&[(":method", "CONNECT"), (":authority", "example.com:443")])
    );
}

// ---- decode_head validation ----

fn enc(fields: &[(&str, &str)]) -> Bytes {
    let mut e = Encoder::stateless();
    let mut out = Vec::new();
    for (n, v) in fields {
        e.encode_field(n.as_bytes(), v.as_bytes(), false, &mut out);
    }
    Bytes::from(out)
}

fn head(fields: &[(&str, &str)], kind: BlockKind) -> Result<DecodedHead, crate::H2Error> {
    Decoder::new(4096).decode_head(&enc(fields), kind, 1)
}

fn is_malformed(r: Result<DecodedHead, crate::H2Error>) -> bool {
    matches!(r, Err(crate::H2Error::Stream { id: 1, reason }) if reason == crate::Reason::PROTOCOL_ERROR)
}

#[test]
fn head_valid() {
    let h = head(
        &[
            (":status", "200"),
            ("content-type", "text/plain"),
            ("x-a", "1"),
            ("x-a", "2"),
        ],
        BlockKind::Response,
    )
    .unwrap();
    assert_eq!(h.status.unwrap().as_u16(), 200);
    assert_eq!(h.headers.get_all("x-a").iter().count(), 2);
    let t = head(&[("grpc-status", "0")], BlockKind::Trailers).unwrap();
    assert!(t.status.is_none());
    let te = head(
        &[(":status", "200"), ("te", "trailers")],
        BlockKind::Response,
    )
    .unwrap();
    assert!(te.headers.contains_key("te"));
}

#[test]
fn head_malformed() {
    let r = BlockKind::Response;
    assert!(
        is_malformed(head(&[("x-a", "1"), (":status", "200")], r)),
        "pseudo after regular"
    );
    assert!(
        is_malformed(head(&[(":status", "200"), (":bogus", "1")], r)),
        "unknown pseudo"
    );
    assert!(
        is_malformed(head(&[(":status", "200"), (":status", "200")], r)),
        "dup status"
    );
    assert!(is_malformed(head(&[("x-a", "1")], r)), "missing status");
    assert!(is_malformed(head(&[(":status", "20")], r)), "short status");
    assert!(
        is_malformed(head(&[(":status", "200"), ("X-Upper", "1")], r)),
        "uppercase"
    );
    assert!(
        is_malformed(head(&[(":status", "200"), ("x-a", " 1")], r)),
        "leading ws"
    );
    assert!(
        is_malformed(head(&[(":status", "200"), ("x-a", "1\t")], r)),
        "trailing ws"
    );
    assert!(
        is_malformed(head(&[(":status", "200"), ("x-a", "a\nb")], r)),
        "newline"
    );
    assert!(
        is_malformed(head(&[(":status", "200"), ("x a", "b")], r)),
        "bad name"
    );
    for n in [
        "connection",
        "keep-alive",
        "proxy-connection",
        "transfer-encoding",
        "upgrade",
    ] {
        assert!(
            is_malformed(head(&[(":status", "200"), (n, "x")], r)),
            "{n}"
        );
    }
    assert!(
        is_malformed(head(&[(":status", "200"), ("te", "gzip")], r)),
        "te"
    );
    assert!(
        is_malformed(head(&[(":status", "200")], BlockKind::Trailers)),
        "pseudo in trailers"
    );
}

#[test]
fn malformed_still_keeps_table_in_sync() {
    let mut e = Encoder::new(4096);
    let mut out = Vec::new();
    e.encode_field(b":status", b"200", false, &mut out);
    e.encode_field(b"connection", b"close", false, &mut out);
    e.encode_field(b"x-after", b"v", false, &mut out);
    let mut d = Decoder::new(4096);
    assert!(is_malformed(d.decode_head(
        &Bytes::from(out),
        BlockKind::Response,
        1
    )));
    assert_eq!(d.table_len(), 2); // connection + x-after inserted (status is static)
}

// ---- limits ----

#[test]
fn limit_huge_integer() {
    let mut d = Decoder::new(4096);
    // indexed field with endless continuation bytes
    let mut b = vec![0xff];
    b.extend_from_slice(&[0xff; 10]);
    b.push(0x01);
    assert_eq!(
        decode_all(&mut d, &Bytes::from(b)),
        Err(DecodeError::IntegerOverflow)
    );
    // value just over u32::MAX
    let b = Bytes::from_static(&[0xff, 0x80, 0xff, 0xff, 0xff, 0x7f]);
    assert_eq!(
        decode_all(&mut Decoder::new(4096), &b),
        Err(DecodeError::IntegerOverflow)
    );
    // zero-padded long integer
    let mut b = vec![0x7f];
    b.extend_from_slice(&[0x80; 8]);
    b.push(0);
    assert_eq!(
        decode_all(&mut Decoder::new(4096), &Bytes::from(b)),
        Err(DecodeError::IntegerOverflow)
    );
}

#[test]
fn limit_huge_string() {
    let mut d = Decoder::new(4096);
    d.set_max_string_len(1000);
    // literal w/o indexing, new name, declared length 2^31
    let b = Bytes::from_static(&[0x00, 0x7f, 0x81, 0xff, 0xff, 0xff, 0x07]);
    assert_eq!(decode_all(&mut d, &b), Err(DecodeError::StringTooLong));
    // Declared within limit but beyond the input => truncated, no allocation of that size
    let b = Bytes::from_static(&[0x00, 0x7f, 0xff, 0x01]);
    assert_eq!(decode_all(&mut d, &b), Err(DecodeError::Truncated));
    // Huffman string whose decoded size exceeds the limit
    let mut e = Vec::new();
    huffman::encode(&vec![b'a'; 900], &mut e);
    let mut b = vec![0x00];
    encode_int(&mut b, 0x80, 7, e.len());
    b.extend_from_slice(&e);
    b.extend_from_slice(&[0x01, b'v']);
    d.set_max_string_len(500);
    assert_eq!(
        decode_all(&mut d, &Bytes::from(b)),
        Err(DecodeError::StringTooLong)
    );
}

#[test]
fn limit_header_list() {
    let mut d = Decoder::new(4096);
    d.set_max_header_list_size(200);
    let mut out = Vec::new();
    let mut e = Encoder::stateless();
    for i in 0..10 {
        e.encode_field(format!("x-{i}").as_bytes(), b"0123456789", false, &mut out);
    }
    assert_eq!(
        decode_all(&mut d, &Bytes::from(out)),
        Err(DecodeError::HeaderListTooLarge)
    );
    // many indexed copies of the same entry are bounded too
    let mut d = Decoder::new(4096);
    d.set_max_header_list_size(500);
    let b = Bytes::from(vec![0x82u8; 100]); // :method GET x100 = 100 * (7+3+32)
    assert_eq!(decode_all(&mut d, &b), Err(DecodeError::HeaderListTooLarge));
}

#[test]
fn size_update_rules() {
    let mut d = Decoder::new(4096);
    // update after a field is an error
    assert_eq!(
        decode_all(&mut d, &Bytes::from_static(&[0x82, 0x20])),
        Err(DecodeError::SizeUpdateNotAtStart)
    );
    // update above limit
    let mut d = Decoder::new(100);
    let mut b = Vec::new();
    encode_int(&mut b, 0x20, 5, 101);
    assert_eq!(
        decode_all(&mut d, &Bytes::from(b)),
        Err(DecodeError::SizeUpdateTooLarge)
    );
    // multiple at start are fine, and shrink evicts
    let mut d = Decoder::new(4096);
    decode_all(
        &mut d,
        &hex("400a637573746f6d2d6b65790d637573746f6d2d686561646572"),
    )
    .unwrap();
    assert_eq!(d.table_len(), 1);
    // 0 then 4096: two updates at the start; the first one evicts.
    decode_all(&mut d, &Bytes::from_static(&[0x20, 0x3f, 0xe1, 0x1f, 0x82])).unwrap();
    assert_eq!(d.table_len(), 0);
    assert_eq!(d.table_capacity(), 4096);
    decode_all(
        &mut d,
        &hex("400a637573746f6d2d6b65790d637573746f6d2d686561646572"),
    )
    .unwrap();
    assert_eq!(d.table_len(), 1);
    decode_all(&mut d, &Bytes::from_static(&[0x20, 0x82])).unwrap();
    assert_eq!(d.table_len(), 0);
}

#[test]
fn invalid_index_and_oversized_entry() {
    let mut d = Decoder::new(4096);
    assert_eq!(
        decode_all(&mut d, &Bytes::from_static(&[0x80])),
        Err(DecodeError::InvalidIndex)
    );
    assert_eq!(
        decode_all(&mut d, &Bytes::from_static(&[0xbe])),
        Err(DecodeError::InvalidIndex)
    );
    // entry larger than the table empties it but still decodes
    let mut d = Decoder::new(64);
    decode_all(
        &mut d,
        &hex("400a637573746f6d2d6b65790d637573746f6d2d686561646572"),
    )
    .unwrap();
    assert_eq!(d.table_len(), 1);
    let big = enc_with_indexing("x", &"y".repeat(100));
    assert_eq!(decode_all(&mut d, &big).unwrap().len(), 1);
    assert_eq!(d.table_len(), 0);
}

fn enc_with_indexing(n: &str, v: &str) -> Bytes {
    let mut b = vec![0x40];
    encode_int(&mut b, 0, 7, n.len());
    b.extend_from_slice(n.as_bytes());
    encode_int(&mut b, 0, 7, v.len());
    b.extend_from_slice(v.as_bytes());
    Bytes::from(b)
}

#[test]
fn literals_are_zero_copy_slices() {
    let block = hex("040c2f73616d706c652f70617468");
    let mut d = Decoder::new(4096);
    d.decode(&block, |_, v, _| {
        let start = block.as_ptr() as usize;
        let p = v.as_ptr() as usize;
        assert!(p >= start && p + v.len() <= start + block.len());
    })
    .unwrap();
}

// ---- property tests ----

fn arb_header() -> impl Strategy<Value = (String, String, bool)> {
    let name = prop_oneof![
        Just("cookie".to_string()),
        Just("authorization".to_string()),
        Just(":path".to_string()),
        Just("content-type".to_string()),
        Just("accept-encoding".to_string()),
        "[a-z][a-z0-9-]{0,30}",
    ];
    let value = prop_oneof![
        Just("gzip, deflate".to_string()),
        Just("".to_string()),
        "[ -~]{0,60}",
        proptest::collection::vec(any::<u8>(), 0..40)
            .prop_map(|v| v.into_iter().map(|b| (b % 95 + 32) as char).collect()),
    ];
    (name, value, any::<bool>())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn huffman_roundtrip(data in proptest::collection::vec(any::<u8>(), 0..512)) {
        let mut e = Vec::new();
        huffman::encode(&data, &mut e);
        prop_assert_eq!(e.len(), huffman::encoded_len(&data));
        let mut d = Vec::new();
        huffman::decode(&e, &mut d, usize::MAX).unwrap();
        prop_assert_eq!(d, data);
    }

    #[test]
    fn huffman_decode_never_panics(data in proptest::collection::vec(any::<u8>(), 0..256)) {
        let mut d = Vec::new();
        let _ = huffman::decode(&data, &mut d, 4096);
    }

    #[test]
    fn hpack_roundtrip(
        blocks in proptest::collection::vec(proptest::collection::vec(arb_header(), 0..12), 1..8),
        table in prop_oneof![Just(0usize), Just(64), Just(256), Just(4096)],
        resize in proptest::option::of((0usize..8, 0usize..5000)),
        stateless in any::<bool>(),
    ) {
        let mut e = if stateless { Encoder::stateless() } else { Encoder::new(table) };
        let mut d = Decoder::new(table);
        d.set_table_size_limit(5000);
        for (i, block) in blocks.iter().enumerate() {
            if let Some((at, size)) = resize
                && at == i
            {
                e.set_peer_table_size(size);
            }
            let mut out = Vec::new();
            e.begin_block(&mut out);
            for (n, v, s) in block {
                e.encode_field(n.as_bytes(), v.as_bytes(), *s, &mut out);
            }
            let mut got = Vec::new();
            d.decode(&Bytes::from(out), |n, v, sens| got.push((n, v, sens))).unwrap();
            prop_assert_eq!(got.len(), block.len());
            for (g, (n, v, s)) in got.iter().zip(block) {
                prop_assert_eq!(&g.0[..], n.as_bytes());
                prop_assert_eq!(&g.1[..], v.as_bytes());
                if *s { prop_assert!(g.2); }
            }
            prop_assert_eq!(d.table_size(), e.table_size());
        }
    }

    #[test]
    fn decoder_never_panics(data in proptest::collection::vec(any::<u8>(), 0..300)) {
        let mut d = Decoder::new(4096);
        let b = Bytes::from(data);
        let _ = d.decode(&b, |_, _, _| {});
        let _ = d.decode(&b, |_, _, _| {});
    }
}

#[test]
fn authority_never_carries_userinfo() {
    let uri: Uri = "https://user:pa%40ss@example.com:8443/p".parse().unwrap();
    let mut e = Encoder::new(4096);
    let mut out = Vec::new();
    e.encode_request(&Method::GET, &uri, &HeaderMap::new(), &mut out);
    let mut d = Decoder::new(4096);
    let r = decode_all(&mut d, &Bytes::from(out)).unwrap();
    assert!(
        r.iter()
            .any(|x| x.0 == ":authority" && x.1 == "example.com:8443"),
        "{r:?}"
    );
    assert!(!r.iter().any(|x| x.1.contains('@')));
}

#[test]
fn connection_specific_fields_are_recognised() {
    use crate::hpack::is_connection_specific as c;
    assert!(c("connection", b"close") && c("transfer-encoding", b"chunked") && c("host", b"x"));
    assert!(c("te", b"gzip") && !c("te", b"trailers") && !c("x-ok", b"1"));
}

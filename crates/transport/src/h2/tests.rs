//! Actor tests against a scripted fake peer (built on the matier-h2 codec) over `tokio::io::duplex`.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, Bytes};
use http_body::Body as _;
use matier_core::{Body, BytesView, ErrorKind, NoopObserver, OriginKey, Recovery};
use matier_h2::frame::{self, Frame, FrameHeader, flags, kind};
use matier_h2::hpack::{Decoder, Encoder};
use matier_h2::{H2Error, Reason, settings};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

use super::sync::Arc;
use super::{H2Config, H2Handle};
use crate::Incoming;

type WriteLog = Arc<Mutex<Vec<(usize, usize)>>>;

/// Duplex wrapper with a real vectored write (one underlying write per call) that logs (bytes, iovecs).
struct TestIo {
    inner: DuplexStream,
    log: WriteLog,
}

impl AsyncRead for TestIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, b)
    }
}

impl AsyncWrite for TestIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, b)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let all: Vec<u8> = bufs.iter().flat_map(|b| b.iter().copied()).collect();
        match Pin::new(&mut self.inner).poll_write(cx, &all) {
            Poll::Ready(Ok(n)) => {
                self.log.lock().unwrap().push((n, bufs.len()));
                Poll::Ready(Ok(n))
            }
            o => o,
        }
    }
    fn is_write_vectored(&self) -> bool {
        true
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum F {
    Data { id: u32, end: bool, len: usize },
    Headers { id: u32, end: bool, block: Vec<u8> },
    Rst { id: u32, code: Reason },
    Settings { ack: bool, params: Vec<(u16, u32)> },
    Ping { ack: bool, data: [u8; 8] },
    GoAway { last: u32, code: Reason },
    WindowUpdate { id: u32, inc: u32 },
    Other,
}

pub(crate) struct Peer {
    io: DuplexStream,
    buf: Vec<u8>,
    enc: Encoder,
    dec: Decoder,
    pub(crate) log: WriteLog,
}

impl Peer {
    async fn read_more(&mut self) -> io::Result<usize> {
        let mut tmp = vec![0u8; 256 * 1024];
        let n = self.io.read(&mut tmp).await?;
        self.buf.extend_from_slice(&tmp[..n]);
        Ok(n)
    }

    /// Next frame, or `None` on EOF.
    pub(crate) async fn try_frame(&mut self) -> Option<F> {
        loop {
            if let Some((f, used)) = self.parse_buffered() {
                self.buf.drain(..used);
                return Some(f);
            }
            if self.read_more().await.ok()? == 0 {
                return None;
            }
        }
    }

    pub(crate) async fn frame(&mut self) -> F {
        tokio::time::timeout(Duration::from_secs(10), self.try_frame())
            .await
            .expect("timed out waiting for a frame")
            .expect("unexpected EOF")
    }

    /// Next frame that is not WINDOW_UPDATE/SETTINGS-ack noise.
    pub(crate) async fn frame_skip_noise(&mut self) -> F {
        loop {
            match self.frame().await {
                F::WindowUpdate { .. } | F::Settings { ack: true, .. } => {}
                f => return f,
            }
        }
    }

    fn parse_buffered(&self) -> Option<(F, usize)> {
        let (fr, used) = frame::decode_frame(&self.buf, 1 << 24).expect("peer: bad frame")?;
        let f = match fr {
            Frame::Data {
                stream_id,
                end_stream,
                flow_len,
                ..
            } => F::Data {
                id: stream_id,
                end: end_stream,
                len: flow_len as usize,
            },
            Frame::Headers {
                stream_id,
                end_stream,
                fragment,
                ..
            } => F::Headers {
                id: stream_id,
                end: end_stream,
                block: fragment.to_vec(),
            },
            Frame::RstStream { stream_id, code } => F::Rst {
                id: stream_id,
                code,
            },
            Frame::Settings { ack, params } => F::Settings {
                ack,
                params: params.collect(),
            },
            Frame::Ping { ack, data } => F::Ping { ack, data },
            Frame::GoAway {
                last_stream_id,
                code,
                ..
            } => F::GoAway {
                last: last_stream_id,
                code,
            },
            Frame::WindowUpdate {
                stream_id,
                increment,
            } => F::WindowUpdate {
                id: stream_id,
                inc: increment,
            },
            _ => F::Other,
        };
        Some((f, used))
    }

    /// Decode a request HEADERS block into (pseudo + regular) fields.
    pub(crate) fn decode_request(&mut self, block: &[u8]) -> Vec<(String, String)> {
        let mut out = Vec::new();
        self.dec
            .decode(&Bytes::copy_from_slice(block), |n, v, _| {
                out.push((
                    String::from_utf8_lossy(&n).into_owned(),
                    String::from_utf8_lossy(&v).into_owned(),
                ));
            })
            .expect("peer hpack");
        out
    }

    /// Read until a request HEADERS arrives; returns (stream id, end_stream, fields).
    pub(crate) async fn request(&mut self) -> (u32, bool, Vec<(String, String)>) {
        loop {
            if let F::Headers { id, end, block } = self.frame().await {
                let f = self.decode_request(&block);
                return (id, end, f);
            }
        }
    }

    pub(crate) async fn send_raw(&mut self, b: &[u8]) {
        self.io.write_all(b).await.unwrap();
    }

    pub(crate) async fn send_settings(&mut self, params: &[(u16, u32)]) {
        let mut v = Vec::new();
        frame::encode_settings(&mut v, params);
        self.send_raw(&v).await;
    }

    pub(crate) async fn send_headers(
        &mut self,
        id: u32,
        status: u16,
        extra: &[(&str, &str)],
        end: bool,
    ) {
        let mut block = Vec::new();
        self.enc.begin_block(&mut block);
        self.enc
            .encode_field(b":status", status.to_string().as_bytes(), false, &mut block);
        for (n, v) in extra {
            self.enc
                .encode_field(n.as_bytes(), v.as_bytes(), false, &mut block);
        }
        let mut v = Vec::new();
        frame::encode_headers(&mut v, id, end, &block, 16_384);
        self.send_raw(&v).await;
    }

    pub(crate) async fn send_trailers(&mut self, id: u32, extra: &[(&str, &str)]) {
        let mut block = Vec::new();
        self.enc.begin_block(&mut block);
        for (n, v) in extra {
            self.enc
                .encode_field(n.as_bytes(), v.as_bytes(), false, &mut block);
        }
        let mut v = Vec::new();
        frame::encode_headers(&mut v, id, true, &block, 16_384);
        self.send_raw(&v).await;
    }

    pub(crate) async fn send_data(&mut self, id: u32, data: &[u8], end: bool) {
        let mut v = frame::data_frame_header(id, data.len() as u32, end).to_vec();
        v.extend_from_slice(data);
        self.send_raw(&v).await;
    }

    pub(crate) async fn send_rst(&mut self, id: u32, code: Reason) {
        let mut v = Vec::new();
        frame::encode_rst_stream(&mut v, id, code);
        self.send_raw(&v).await;
    }

    pub(crate) async fn send_goaway(&mut self, last: u32, code: Reason) {
        let mut v = Vec::new();
        frame::encode_goaway(&mut v, last, code, b"");
        self.send_raw(&v).await;
    }

    pub(crate) async fn send_window_update(&mut self, id: u32, inc: u32) {
        let mut v = Vec::new();
        frame::encode_window_update(&mut v, id, inc);
        self.send_raw(&v).await;
    }

    pub(crate) async fn respond(&mut self, id: u32, body: &[u8]) {
        self.send_headers(
            id,
            200,
            &[("content-length", &body.len().to_string())],
            body.is_empty(),
        )
        .await;
        if !body.is_empty() {
            self.send_data(id, body, true).await;
        }
    }
}

pub(crate) fn origin() -> OriginKey {
    OriginKey::from_uri(&"http://example.com/".parse().unwrap()).unwrap()
}

pub(crate) fn get(path: &str) -> http::Request<Body> {
    http::Request::builder()
        .uri(format!("http://example.com{path}"))
        .body(Body::empty())
        .unwrap()
}

pub(crate) fn post(path: &str, body: Body) -> http::Request<Body> {
    http::Request::builder()
        .method("POST")
        .uri(format!("http://example.com{path}"))
        .body(body)
        .unwrap()
}

/// Perform the handshake against a scripted peer sending `peer_settings`.
pub(crate) async fn connect(cfg: H2Config, peer_settings: &[(u16, u32)]) -> (H2Handle, Peer) {
    connect_buf(8 << 20, cfg, peer_settings).await
}

/// [`connect`] over a duplex with `cap` bytes of buffer: our writes block once the peer stops reading.
async fn connect_buf(cap: usize, cfg: H2Config, peer_settings: &[(u16, u32)]) -> (H2Handle, Peer) {
    let (a, b) = tokio::io::duplex(cap);
    let log: WriteLog = Arc::new(Mutex::new(Vec::new()));
    let io = TestIo {
        inner: a,
        log: Arc::clone(&log),
    };
    let mut peer = Peer {
        io: b,
        buf: Vec::new(),
        enc: Encoder::new(4096),
        dec: Decoder::new(4096),
        log,
    };
    let server = async {
        // preface
        let mut pre = [0u8; 24];
        peer.io.read_exact(&mut pre).await.unwrap();
        assert_eq!(&pre, frame::CONNECTION_PREFACE);
        let s = peer.frame().await;
        assert!(matches!(s, F::Settings { ack: false, .. }), "{s:?}");
        peer.send_settings(peer_settings).await;
    };
    let client = super::actor::handshake_io(io, origin(), cfg, Arc::new(NoopObserver));
    let (h, ()) = tokio::join!(client, server);
    (h.expect("handshake"), peer)
}

pub(crate) async fn collect_body(
    resp: http::Response<Incoming>,
) -> Result<Vec<u8>, matier_core::Error> {
    let (_, mut body) = resp.into_parts();
    let mut out = Vec::new();
    while let Some(f) = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        if let Ok(mut d) = f?.into_data() {
            while d.has_remaining() {
                let c = d.chunk().to_vec();
                d.advance(c.len());
                out.extend_from_slice(&c);
            }
        }
    }
    Ok(out)
}

fn field<'a>(f: &'a [(String, String)], n: &str) -> Option<&'a str> {
    f.iter().find(|(k, _)| k == n).map(|(_, v)| v.as_str())
}

// ---------------------------------------------------------------------------------------------------------

#[tokio::test]
async fn handshake_exchanges_settings_and_acks() {
    let (h, mut peer) = connect(
        H2Config::default(),
        &[(settings::MAX_CONCURRENT_STREAMS, 7)],
    )
    .await;
    assert_eq!(h.max_concurrent_streams(), 7);
    assert!(h.is_accepting());
    // The client's connection WINDOW_UPDATE and the ack of our SETTINGS follow.
    let mut saw_ack = false;
    let mut saw_wu = false;
    for _ in 0..2 {
        match peer.frame().await {
            F::Settings { ack: true, .. } => saw_ack = true,
            F::WindowUpdate { id: 0, inc } => {
                assert_eq!(inc, H2Config::default().initial_conn_window - 65_535);
                saw_wu = true;
            }
            f => panic!("unexpected {f:?}"),
        }
    }
    assert!(saw_ack || saw_wu);
}

#[tokio::test]
async fn single_get() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let slot = h.try_reserve().unwrap();
    assert_eq!(h.in_flight(), 1);
    let fut = h.send(slot, get("/hello?x=1"));
    let server = async {
        let (id, end, f) = peer.request().await;
        assert_eq!(id, 1);
        assert!(end, "GET with empty body must END_STREAM on HEADERS");
        assert_eq!(field(&f, ":method"), Some("GET"));
        assert_eq!(field(&f, ":path"), Some("/hello?x=1"));
        assert_eq!(field(&f, ":authority"), Some("example.com"));
        peer.respond(id, b"world").await;
    };
    let (resp, ()) = tokio::join!(fut, server);
    let resp = resp.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.version(), http::Version::HTTP_2);
    let info = resp.body().connection_info().unwrap();
    assert_eq!(
        resp.extensions()
            .get::<matier_core::ConnectionInfo>()
            .copied(),
        None,
        "the extension is opt-in"
    );
    assert_eq!(info.protocol, matier_core::Protocol::Http2);
    assert_eq!(collect_body(resp).await.unwrap(), b"world");
    // Slot freed once the stream completed.
    for _ in 0..50 {
        if h.in_flight() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(h.in_flight(), 0);
}

#[tokio::test]
async fn post_full_body_goes_out_with_headers_in_one_write() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    // Let the handshake-time frames settle so the log only contains request writes.
    let body = vec![7u8; 4096];
    let before = peer.log.lock().unwrap().len();
    let fut = h.send(
        h.try_reserve().unwrap(),
        post("/up", Body::full(matier_core::buf::copy_from_slice(&body))),
    );
    let server = async {
        let (id, end, f) = peer.request().await;
        assert!(!end);
        assert_eq!(field(&f, ":method"), Some("POST"));
        let mut got = 0;
        loop {
            match peer.frame_skip_noise().await {
                F::Data { id: i, end, len } => {
                    assert_eq!(i, id);
                    got += len;
                    if end {
                        break;
                    }
                }
                f => panic!("unexpected {f:?}"),
            }
        }
        assert_eq!(got, 4096);
        peer.respond(id, b"ok").await;
    };
    let (resp, ()) = tokio::join!(fut, server);
    resp.unwrap();
    let log = peer.log.lock().unwrap()[before..].to_vec();
    // HEADERS + DATA header + 4 KiB payload: one write, several iovecs (payload is not copied).
    let big = log
        .iter()
        .find(|(b, _)| *b >= 4096 + 9 + 9)
        .expect("one write carrying HEADERS and DATA");
    assert!(big.1 >= 2, "expected vectored write, got {log:?}");
}

#[tokio::test]
async fn small_post_payload_is_coalesced_into_arena() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let before = peer.log.lock().unwrap().len();
    let fut = h.send(
        h.try_reserve().unwrap(),
        post("/up", Body::from_static(b"tiny")),
    );
    let server = async {
        let (id, _, _) = peer.request().await;
        loop {
            if let F::Data { end: true, len, .. } = peer.frame_skip_noise().await {
                assert_eq!(len, 4);
                break;
            }
        }
        peer.respond(id, b"").await;
    };
    let (resp, ()) = tokio::join!(fut, server);
    resp.unwrap();
    let log = peer.log.lock().unwrap()[before..].to_vec();
    assert!(log.iter().any(|(_, iov)| *iov == 1), "{log:?}");
}

async fn run_concurrent(n: usize) {
    let cfg = H2Config::default();
    let (h, mut peer) = connect(cfg, &[]).await;
    let server = async {
        let mut seen = 0;
        while seen < n {
            let (id, _, f) = peer.request().await;
            let path = field(&f, ":path").unwrap().to_owned();
            peer.respond(id, path.as_bytes()).await;
            seen += 1;
        }
    };
    let clients = async {
        let futs: Vec<_> = (0..n)
            .map(|i| {
                let h = h.clone();
                tokio::spawn(async move {
                    let slot = h.try_reserve().unwrap();
                    let resp = h.send(slot, get(&format!("/r{i}"))).await.unwrap();
                    let b = collect_body(resp).await.unwrap();
                    assert_eq!(b, format!("/r{i}").as_bytes());
                })
            })
            .collect();
        for f in futs {
            f.await.unwrap();
        }
    };
    tokio::join!(server, clients);
    assert_eq!(h.in_flight(), 0);
}

#[tokio::test]
async fn hundred_concurrent_streams() {
    run_concurrent(100).await;
}

#[tokio::test]
async fn two_thousand_concurrent_streams() {
    run_concurrent(2000).await;
}

#[tokio::test]
async fn max_concurrent_streams_enforced_by_try_reserve() {
    let (h, _peer) = connect(
        H2Config::default(),
        &[(settings::MAX_CONCURRENT_STREAMS, 2)],
    )
    .await;
    let a = h.try_reserve().unwrap();
    let b = h.try_reserve().unwrap();
    assert!(h.try_reserve().is_none());
    assert_eq!(h.in_flight(), 2);
    drop(a);
    assert_eq!(h.in_flight(), 1);
    let c = h.try_reserve().unwrap();
    assert!(h.try_reserve().is_none());
    drop((b, c));
    assert_eq!(h.in_flight(), 0);
}

#[tokio::test]
async fn slot_released_notifies_waiters() {
    let (h, _peer) = connect(
        H2Config::default(),
        &[(settings::MAX_CONCURRENT_STREAMS, 1)],
    )
    .await;
    let a = h.try_reserve().unwrap();
    let h2 = h.clone();
    let waiter = tokio::spawn(async move {
        loop {
            let n = h2.slot_released();
            tokio::pin!(n);
            n.as_mut().enable();
            if let Some(s) = h2.try_reserve() {
                return s;
            }
            n.await;
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(a);
    let s = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
    drop(s);
}

#[tokio::test]
async fn flow_control_stall_and_resume() {
    // Peer window of 1000 bytes per stream; 5000-byte upload must stall, then resume on WINDOW_UPDATE.
    let (h, mut peer) = connect(
        H2Config::default(),
        &[(settings::INITIAL_WINDOW_SIZE, 1000)],
    )
    .await;
    let body = vec![1u8; 5000];
    let fut = h.send(
        h.try_reserve().unwrap(),
        post("/s", Body::full(matier_core::buf::copy_from_slice(&body))),
    );
    let server = async {
        let (id, _, _) = peer.request().await;
        let mut got = 0usize;
        // Receive the first window's worth, then verify nothing more arrives until we update.
        while got < 1000 {
            if let F::Data { len, .. } = peer.frame_skip_noise().await {
                got += len;
            }
        }
        assert_eq!(got, 1000);
        let quiet = tokio::time::timeout(Duration::from_millis(100), peer.try_frame()).await;
        assert!(
            quiet.is_err(),
            "sender must stall at window 0, got {quiet:?}"
        );
        for _ in 0..4 {
            peer.send_window_update(id, 1000).await;
        }
        let mut end = false;
        while !end {
            if let F::Data { len, end: e, .. } = peer.frame_skip_noise().await {
                got += len;
                end = e;
            }
        }
        assert_eq!(got, 5000);
        peer.respond(id, b"done").await;
    };
    let (resp, ()) = tokio::join!(fut, server);
    resp.unwrap();
}

fn full_body(len: usize) -> Body {
    Body::full(matier_core::buf::copy_from_slice(&vec![5u8; len]))
}

/// The next `n` DATA frames as (stream id, length, END_STREAM).
async fn data_frames(peer: &mut Peer, n: usize) -> Vec<(u32, usize, bool)> {
    let mut seen = Vec::new();
    while seen.len() < n {
        if let F::Data { id, len, end } = peer.frame_skip_noise().await {
            seen.push((id, len, end));
        }
    }
    seen
}

/// Responds to `ids`, then checks every response future completes.
async fn respond_all(peer: &mut Peer, ids: &[u32], futs: Vec<super::ResponseFuture>) {
    let server = async {
        for &id in ids {
            peer.respond(id, b"").await;
        }
    };
    let clients = async {
        for f in futs {
            f.await.unwrap();
        }
    };
    tokio::join!(server, clients);
}

#[tokio::test]
async fn fifo_send_finishes_each_body_before_the_next() {
    // Three 20,000-byte bodies at the peer's default 16 KiB frames: 16,384 bytes, then 3,616 with END_STREAM.
    for (cfg, order) in [
        (H2Config::default(), [1, 3, 5, 1, 3, 5]),
        (
            H2Config {
                fifo_send: true,
                ..H2Config::default()
            },
            [1, 1, 3, 3, 5, 5],
        ),
    ] {
        let fifo = cfg.fifo_send;
        let (h, mut peer) = connect(cfg, &[]).await;
        // Queued before the actor runs, so its first tick schedules all three bodies together.
        let futs: Vec<_> = (0..3)
            .map(|_| h.send(h.try_reserve().unwrap(), post("/up", full_body(20_000))))
            .collect();
        let mut sent = [0usize; 6];
        let expected: Vec<_> = order
            .iter()
            .map(|&id| {
                let first = sent[id as usize] == 0;
                sent[id as usize] += 1;
                if first {
                    (id, 16_384, false)
                } else {
                    (id, 3_616, true)
                }
            })
            .collect();
        assert_eq!(
            data_frames(&mut peer, 6).await,
            expected,
            "fifo_send={fifo}"
        );
        respond_all(&mut peer, &[1, 3, 5], futs).await;
    }
}

#[tokio::test]
async fn fifo_send_order_survives_a_cancelled_stream_slot_reuse() {
    // X uses up the peer's 65,535-byte connection window, so A and B wait queued behind it. A is cancelled and
    // C takes over its stream slot; when credit returns, B (queued earlier) must still go before C.
    let cfg = H2Config {
        fifo_send: true,
        ..H2Config::default()
    };
    let (h, mut peer) = connect(cfg, &[]).await;
    let x = h.send(h.try_reserve().unwrap(), post("/x", full_body(65_535)));
    let a = h.send(h.try_reserve().unwrap(), post("/a", full_body(20_000)));
    let b = h.send(h.try_reserve().unwrap(), post("/b", full_body(20_000)));
    drain_data(&mut peer, 65_535).await;
    drop(a);
    let c = h.send(h.try_reserve().unwrap(), post("/c", full_body(20_000)));
    let (mut reset, mut opened) = (false, false);
    while !(reset && opened) {
        match peer.frame_skip_noise().await {
            F::Rst { id: 3, .. } => reset = true,
            F::Headers { id: 7, .. } => opened = true,
            F::Data { id, .. } => panic!("DATA on stream {id} without connection credit"),
            _ => {}
        }
    }
    peer.send_window_update(0, 1 << 20).await;
    let ids: Vec<_> = data_frames(&mut peer, 4)
        .await
        .into_iter()
        .map(|f| f.0)
        .collect();
    assert_eq!(ids, [5, 5, 7, 7]);
    respond_all(&mut peer, &[1, 5, 7], vec![x, b, c]).await;
}

async fn drain_data(peer: &mut Peer, want: usize) {
    let mut got = 0usize;
    while got < want {
        if let F::Data { len, .. } = peer.frame_skip_noise().await {
            got += len;
        }
    }
    assert_eq!(got, want);
}

async fn assert_no_data(peer: &mut Peer, why: &str) {
    let quiet = tokio::time::timeout(Duration::from_millis(150), async {
        loop {
            if let F::Data { .. } = peer.frame().await {
                return;
            }
        }
    })
    .await;
    assert!(quiet.is_err(), "{why}");
}

#[tokio::test]
async fn initial_window_decrease_goes_negative_then_recovers() {
    // Window 3000: send 3000 (window 0); shrink INITIAL_WINDOW_SIZE to 1000 => window -2000.
    // WINDOW_UPDATE 2500 => +500 available; 1000 more bytes wait for another update.
    let (h, mut peer) = connect(
        H2Config::default(),
        &[(settings::INITIAL_WINDOW_SIZE, 3000)],
    )
    .await;
    let (mut tx, body) = Body::channel(8);
    let fut = h.send(h.try_reserve().unwrap(), post("/neg", body));
    tx.send(matier_core::buf::copy_from_slice(&[9u8; 3000]))
        .await
        .unwrap();
    let server = async {
        let (id, _, _) = peer.request().await;
        drain_data(&mut peer, 3000).await;
        peer.send_settings(&[(settings::INITIAL_WINDOW_SIZE, 1000)])
            .await;
        tx.send(matier_core::buf::copy_from_slice(&[8u8; 1000]))
            .await
            .unwrap();
        assert_no_data(&mut peer, "window is negative: nothing may be sent").await;
        peer.send_window_update(id, 2500).await;
        drain_data(&mut peer, 500).await;
        assert_no_data(&mut peer, "only 500 of the remaining 1000 bytes fit").await;
        peer.send_window_update(id, 1000).await;
        drain_data(&mut peer, 500).await;
        tx.finish();
        loop {
            if let F::Data { end: true, .. } = peer.frame_skip_noise().await {
                break;
            }
        }
        peer.respond(id, b"k").await;
    };
    let (resp, ()) = tokio::join!(fut, server);
    resp.unwrap();
}

#[tokio::test]
async fn goaway_splits_unprocessed_and_retryable() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let f1 = h.send(h.try_reserve().unwrap(), get("/1"));
    let f2 = h.send(h.try_reserve().unwrap(), get("/2"));
    let f3 = h.send(h.try_reserve().unwrap(), get("/3"));
    let server = async {
        let mut ids = vec![];
        for _ in 0..3 {
            ids.push(peer.request().await.0);
        }
        assert_eq!(ids, [1, 3, 5]);
        peer.send_goaway(3, Reason::NO_ERROR).await;
        peer.respond(1, b"a").await;
        peer.respond(3, b"b").await;
    };
    let (r1, r2, r3, ()) = tokio::join!(f1, f2, f3, server);
    assert_eq!(collect_body(r1.unwrap()).await.unwrap(), b"a");
    assert_eq!(collect_body(r2.unwrap()).await.unwrap(), b"b");
    let e = r3.unwrap_err();
    assert_eq!(e.recovery(), Recovery::Unprocessed);
    assert!(!h.is_accepting());
    assert!(h.try_reserve().is_none());
    tokio::time::timeout(Duration::from_secs(5), h.closed())
        .await
        .unwrap();
}

#[tokio::test]
async fn eof_fails_streams_retryable_and_unsent_unprocessed() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let f1 = h.send(h.try_reserve().unwrap(), get("/1"));
    let server = async {
        peer.request().await;
        drop(peer);
    };
    let (r1, ()) = tokio::join!(f1, server);
    let e = r1.unwrap_err();
    assert_eq!(e.recovery(), Recovery::Retryable);
    tokio::time::timeout(Duration::from_secs(5), h.closed())
        .await
        .unwrap();
    // After close, new requests fail immediately and are safe to retry.
    let e = h
        .send(
            super::StreamSlot {
                conn: Arc::clone(&h.shared),
            },
            get("/late"),
        )
        .await
        .unwrap_err();
    assert_eq!(e.recovery(), Recovery::Unprocessed);
}

#[tokio::test]
async fn refused_stream_is_unprocessed() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let f = h.send(h.try_reserve().unwrap(), get("/r"));
    let server = async {
        let (id, _, _) = peer.request().await;
        peer.send_rst(id, Reason::REFUSED_STREAM).await;
    };
    let (r, ()) = tokio::join!(f, server);
    assert_eq!(r.unwrap_err().recovery(), Recovery::Unprocessed);
    let f = h.send(h.try_reserve().unwrap(), get("/r2"));
    let server = async {
        let (id, _, _) = peer.request().await;
        peer.send_rst(id, Reason::INTERNAL_ERROR).await;
    };
    let (r, ()) = tokio::join!(f, server);
    assert_eq!(r.unwrap_err().recovery(), Recovery::Retryable);
    assert!(h.is_accepting());
}

#[tokio::test]
async fn dropping_response_future_sends_rst_cancel() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let f = h.send(h.try_reserve().unwrap(), get("/c"));
    let (id, _, _) = peer.request().await;
    drop(f);
    match peer.frame_skip_noise().await {
        F::Rst { id: i, code } => {
            assert_eq!(i, id);
            assert_eq!(code, Reason::CANCEL);
        }
        f => panic!("unexpected {f:?}"),
    }
    for _ in 0..100 {
        if h.in_flight() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(h.in_flight(), 0);
}

#[tokio::test]
async fn dropping_body_mid_stream_sends_rst_cancel_and_releases_window() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let f = h.send(h.try_reserve().unwrap(), get("/c"));
    let (id, _, _) = peer.request().await;
    peer.send_headers(id, 200, &[], false).await;
    let resp = f.await.unwrap();
    peer.send_data(id, &[0u8; 1000], false).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(resp);
    loop {
        if let F::Rst { id: i, code } = peer.frame_skip_noise().await {
            assert_eq!((i, code), (id, Reason::CANCEL));
            break;
        }
    }
    // Late DATA on the canceled stream is ignored; the connection stays healthy.
    peer.send_data(id, &[0u8; 100], false).await;
    let f = h.send(h.try_reserve().unwrap(), get("/next"));
    let server = async {
        let (id2, _, _) = peer.request().await;
        peer.respond(id2, b"fine").await;
    };
    let (r, ()) = tokio::join!(f, server);
    assert_eq!(collect_body(r.unwrap()).await.unwrap(), b"fine");
}

/// HTTP.sys counts a reset stream against MAX_CONCURRENT_STREAMS until it has processed the RST_STREAM, and
/// aborts the connection when HEADERS reusing those slots arrive in the same read. A reset stream's slot is
/// released only once its RST_STREAM has been written and flushed.
#[tokio::test]
async fn reset_stream_keeps_its_slot_until_the_rst_is_flushed() {
    let (h, mut peer) = connect_buf(
        4096,
        H2Config::default(),
        &[(settings::MAX_CONCURRENT_STREAMS, 2)],
    )
    .await;
    let cancelled = h.send(h.try_reserve().unwrap(), get("/cancel"));
    let (id, _, _) = peer.request().await;
    // The peer stops reading: this body fills the duplex buffer and blocks our writes.
    let upload = h.send(h.try_reserve().unwrap(), post("/up", full_body(60_000)));
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(cancelled);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        h.in_flight(),
        2,
        "slot released before its RST_STREAM was written"
    );
    assert!(h.try_reserve().is_none());
    loop {
        if let F::Rst { id: i, code } = peer.frame_skip_noise().await {
            assert_eq!((i, code), (id, Reason::CANCEL));
            break;
        }
    }
    for _ in 0..100 {
        if h.in_flight() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(h.in_flight(), 1);
    drop(upload);
}

async fn expect_goaway(peer: &mut Peer, code: Reason) {
    loop {
        match peer.try_frame().await {
            Some(F::GoAway { code: c, .. }) => {
                assert_eq!(c, code);
                return;
            }
            Some(_) => {}
            None => panic!("EOF without GOAWAY"),
        }
    }
}

#[tokio::test]
async fn continuation_flood_gets_enhance_your_calm() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let f = h.send(h.try_reserve().unwrap(), get("/f"));
    let (id, _, _) = peer.request().await;
    // HEADERS without END_HEADERS followed by an endless stream of empty-ish CONTINUATIONs.
    let mut v = Vec::new();
    v.extend_from_slice(
        &FrameHeader {
            len: 1,
            kind: kind::HEADERS,
            flags: 0,
            stream_id: id,
        }
        .encode(),
    );
    v.push(0x88);
    for _ in 0..200 {
        v.extend_from_slice(
            &FrameHeader {
                len: 1,
                kind: kind::CONTINUATION,
                flags: 0,
                stream_id: id,
            }
            .encode(),
        );
        v.push(0x88);
    }
    peer.send_raw(&v).await;
    expect_goaway(&mut peer, Reason::ENHANCE_YOUR_CALM).await;
    let e = f.await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Protocol);
    tokio::time::timeout(Duration::from_secs(5), h.closed())
        .await
        .unwrap();
    assert!(!h.is_accepting());
}

#[tokio::test]
async fn ping_is_acked() {
    let (_h, mut peer) = connect(H2Config::default(), &[]).await;
    let mut v = Vec::new();
    frame::encode_ping(&mut v, false, *b"matier!!");
    peer.send_raw(&v).await;
    loop {
        if let F::Ping { ack, data } = peer.frame().await {
            assert!(ack);
            assert_eq!(&data, b"matier!!");
            break;
        }
    }
}

#[tokio::test]
async fn push_promise_is_a_connection_error() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let f = h.send(h.try_reserve().unwrap(), get("/p"));
    let (id, _, _) = peer.request().await;
    let mut v = Vec::new();
    v.extend_from_slice(
        &FrameHeader {
            len: 5,
            kind: kind::PUSH_PROMISE,
            flags: flags::END_HEADERS,
            stream_id: id,
        }
        .encode(),
    );
    v.extend_from_slice(&[0, 0, 0, 2, 0x88]);
    peer.send_raw(&v).await;
    expect_goaway(&mut peer, Reason::PROTOCOL_ERROR).await;
    assert!(f.await.is_err());
}

#[tokio::test]
async fn keepalive_ping_and_timeout() {
    let cfg = H2Config {
        keepalive_interval: Some(Duration::from_millis(50)),
        keepalive_timeout: Duration::from_millis(100),
        ..H2Config::default()
    };
    let (h, mut peer) = connect(cfg, &[]).await;
    // Answer the first ping, ignore the second: the connection must close.
    let mut answered = 0;
    let r = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match peer.try_frame().await {
                Some(F::Ping { ack: false, data }) => {
                    if answered == 0 {
                        let mut v = Vec::new();
                        frame::encode_ping(&mut v, true, data);
                        peer.send_raw(&v).await;
                    }
                    answered += 1;
                }
                Some(_) => {}
                None => return,
            }
        }
    })
    .await;
    assert!(
        r.is_ok(),
        "connection should have been closed after the unanswered ping"
    );
    assert!(answered >= 2);
    tokio::time::timeout(Duration::from_secs(5), h.closed())
        .await
        .unwrap();
}

#[tokio::test]
async fn trailers_are_delivered() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let f = h.send(h.try_reserve().unwrap(), get("/t"));
    let server = async {
        let (id, _, _) = peer.request().await;
        peer.send_headers(id, 200, &[], false).await;
        peer.send_data(id, b"body", false).await;
        peer.send_trailers(id, &[("x-checksum", "abc")]).await;
    };
    let (r, ()) = tokio::join!(f, server);
    let (_, mut body) = r.unwrap().into_parts();
    let mut data = 0;
    let mut trailers = None;
    while let Some(fr) = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        let fr = fr.unwrap();
        if let Some(d) = fr.data_ref() {
            data += d.len();
        } else if let Ok(t) = fr.into_trailers() {
            trailers = Some(t);
        }
    }
    assert_eq!(data, 4);
    assert_eq!(trailers.unwrap().get("x-checksum").unwrap(), "abc");
}

/// h2#975 scenario: the peer stops reading entirely. We must keep reading and complete other streams'
/// responses.
#[tokio::test]
async fn blocked_writer_does_not_stop_reads() {
    // Tiny duplex buffer so our writes block quickly.
    let (a, b) = tokio::io::duplex(4096);
    let log: WriteLog = Arc::new(Mutex::new(Vec::new()));
    let io = TestIo {
        inner: a,
        log: Arc::clone(&log),
    };
    let mut peer = Peer {
        io: b,
        buf: Vec::new(),
        enc: Encoder::new(4096),
        dec: Decoder::new(4096),
        log,
    };
    let client = tokio::spawn(super::actor::handshake_io(
        io,
        origin(),
        H2Config::default(),
        Arc::new(NoopObserver),
    ));
    let mut pre = [0u8; 24];
    peer.io.read_exact(&mut pre).await.unwrap();
    peer.frame().await; // client SETTINGS
    peer.send_settings(&[]).await;
    let h = client.await.unwrap().unwrap();
    // A huge upload that will fill our write path, plus many small GETs.
    let big = vec![5u8; 4 << 20];
    let big_fut = h.send(
        h.try_reserve().unwrap(),
        post("/big", Body::full(matier_core::buf::copy_from_slice(&big))),
    );
    let mut small = Vec::new();
    for i in 0..20 {
        small.push(h.send(h.try_reserve().unwrap(), get(&format!("/s{i}"))));
    }
    // Do not read from the peer socket (write side is stuck), but keep *sending* responses.
    // Stream ids: 1 = big, 3.. = small. The requests' HEADERS may not even have been flushed yet; the
    // server answers regardless (a misbehaving-but-legal pipelining peer).
    let mut resp_tasks = Vec::new();
    for f in small {
        resp_tasks.push(tokio::spawn(async move {
            collect_body(f.await.unwrap()).await.unwrap()
        }));
    }
    // Make sure the actor has processed all Opens before we respond (they are queued in the write buffer).
    tokio::time::sleep(Duration::from_millis(100)).await;
    for i in 0..20u32 {
        let id = 3 + 2 * i;
        peer.send_headers(id, 200, &[], false).await;
        peer.send_data(id, b"pong", true).await;
    }
    for t in resp_tasks {
        let b = tokio::time::timeout(Duration::from_secs(10), t)
            .await
            .expect("deadlock: reads stalled")
            .unwrap();
        assert_eq!(b, b"pong");
    }
    drop(big_fut);
}

#[tokio::test]
async fn large_response_is_zero_copy_chunks_with_window_updates() {
    let total = 8usize << 20;
    let cfg = H2Config {
        initial_stream_window: 1 << 20,
        initial_conn_window: 4 << 20,
        max_frame_size: 16_384,
        ..H2Config::default()
    };
    let (h, mut peer) = connect(cfg, &[(settings::MAX_FRAME_SIZE, 16_384)]).await;
    let f = h.send(h.try_reserve().unwrap(), get("/big"));
    let server = async {
        let (id, _, _) = peer.request().await;
        peer.send_headers(id, 200, &[], false).await;
        // Respect the client's windows: send, and wait for WINDOW_UPDATEs when exhausted.
        let mut stream_win: i64 = 1 << 20;
        let mut conn_win: i64 = 4 << 20;
        let mut sent = 0usize;
        let mut wu_stream = 0u64;
        let chunk = vec![0xabu8; 16_384];
        while sent < total {
            let n = chunk.len().min(total - sent);
            if stream_win < n as i64 || conn_win < n as i64 {
                loop {
                    match peer.frame().await {
                        F::WindowUpdate { id: 0, inc } => conn_win += i64::from(inc),
                        F::WindowUpdate { inc, .. } => {
                            wu_stream += 1;
                            stream_win += i64::from(inc)
                        }
                        _ => {}
                    }
                    if stream_win >= n as i64 && conn_win >= n as i64 {
                        break;
                    }
                }
            }
            peer.send_data(id, &chunk[..n], sent + n == total).await;
            stream_win -= n as i64;
            conn_win -= n as i64;
            sent += n;
        }
        wu_stream
    };
    let client = async {
        let resp = f.await.unwrap();
        let (_, mut body) = resp.into_parts();
        let mut got = 0usize;
        let mut chunks = 0usize;
        while let Some(fr) = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
            let d: BytesView = fr.unwrap().into_data().ok().unwrap();
            got += d.len();
            chunks += 1;
            assert!(d.len() <= 16_384);
        }
        (got, chunks)
    };
    let (wu, (got, chunks)) = tokio::join!(server, client);
    assert_eq!(got, total);
    assert!(chunks >= total / 16_384);
    assert!(
        wu > 0,
        "stream WINDOW_UPDATEs must be sent as the body is consumed"
    );
}

#[tokio::test]
async fn unused_slot_drop_decrements() {
    let (h, _p) = connect(H2Config::default(), &[]).await;
    let s = h.try_reserve().unwrap();
    assert_eq!(h.in_flight(), 1);
    drop(s);
    assert_eq!(h.in_flight(), 0);
}

#[tokio::test]
async fn hpack_errors_are_connection_errors() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let f = h.send(h.try_reserve().unwrap(), get("/h"));
    let (id, _, _) = peer.request().await;
    let mut v = Vec::new();
    v.extend_from_slice(
        &FrameHeader {
            len: 1,
            kind: kind::HEADERS,
            flags: flags::END_HEADERS,
            stream_id: id,
        }
        .encode(),
    );
    v.push(0xff); // index beyond the table
    peer.send_raw(&v).await;
    expect_goaway(&mut peer, Reason::COMPRESSION_ERROR).await;
    assert!(f.await.is_err());
    let _ = H2Error::Connection(Reason::NO_ERROR, "");
}

/// Regression: consumption below the report threshold used to be invisible to the actor, so the buffered-bytes
/// budget leaked and the connection window stalled after ~budget bytes (a hang after ~7k x 1 KiB responses).
#[tokio::test]
async fn many_small_responses_never_stall_the_connection_window() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let total = 12_000usize; // 12 MB of bodies >> the 4 MiB connection window / buffered budget
    let server = async {
        let body = vec![b'x'; 1024];
        for _ in 0..total {
            let (id, _, _) = peer.request().await;
            peer.send_headers(id, 200, &[], false).await;
            peer.send_data(id, &body, true).await;
        }
    };
    let clients = async {
        let sem = Arc::new(tokio::sync::Semaphore::new(16));
        let mut tasks = Vec::new();
        for _ in 0..total {
            let p = sem.clone().acquire_owned().await.unwrap();
            let h = h.clone();
            tasks.push(tokio::spawn(async move {
                let _p = p;
                let r = h.send(h.try_reserve().unwrap(), get("/b")).await.unwrap();
                assert_eq!(collect_body(r).await.unwrap().len(), 1024);
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
    };
    tokio::time::timeout(Duration::from_secs(60), async {
        tokio::join!(server, clients)
    })
    .await
    .expect("stalled");
}

/// Sum of connection-level WINDOW_UPDATE credit the peer receives within `quiet` of silence.
async fn conn_credit(peer: &mut Peer, quiet: Duration) -> (u64, Vec<F>) {
    let mut sum = 0u64;
    let mut other = Vec::new();
    while let Ok(Some(f)) = tokio::time::timeout(quiet, peer.try_frame()).await {
        match f {
            F::WindowUpdate { id: 0, inc } => sum += u64::from(inc),
            f => other.push(f),
        }
    }
    (sum, other)
}

fn padded_data(id: u32, data: &[u8], pad: u8, end: bool) -> Vec<u8> {
    let len = 1 + data.len() + usize::from(pad);
    let mut v = FrameHeader {
        len: len as u32,
        kind: kind::DATA,
        flags: flags::PADDED | if end { flags::END_STREAM } else { 0 },
        stream_id: id,
    }
    .encode()
    .to_vec();
    v.push(pad);
    v.extend_from_slice(data);
    v.extend(std::iter::repeat_n(0u8, usize::from(pad)));
    v
}

/// S1: a flood of padded 1-byte DATA frames to a stream nobody reads must stop earning connection credit
/// once ~budget (wire bytes + per-event overhead) is outstanding.
#[tokio::test]
async fn padded_tiny_data_flood_is_charged_and_stops_connection_credit() {
    let cfg = H2Config {
        initial_conn_window: 65_535,
        ..H2Config::default()
    };
    let (h, mut peer) = connect(cfg, &[]).await;
    let resp = h.send(h.try_reserve().unwrap(), get("/"));
    let (id, _, _) = peer.request().await;
    peer.send_headers(id, 200, &[], false).await;
    let resp = resp.await.unwrap(); // the consumer never polls the body
    let frame = padded_data(id, b"x", 200, false);
    let n = 300usize;
    let mut wire = Vec::new();
    for _ in 0..n {
        wire.extend_from_slice(&frame);
    }
    peer.send_raw(&wire).await;
    let flow_total = (n * 202) as u64;
    let (credit, other) = conn_credit(&mut peer, Duration::from_millis(200)).await;
    assert!(other.iter().all(|f| matches!(f, F::WindowUpdate { .. })));
    // 65_535 / (202 + 64) = 246 frames fit the budget; the remainder must be withheld.
    assert!(credit <= 246 * 202, "credit {credit} of {flow_total}");
    assert!(
        credit < flow_total - 5_000,
        "credit {credit} of {flow_total}"
    );
    drop(resp);
    drop(h);
}

/// Per-stream cap on unconsumed receive events: ENHANCE_YOUR_CALM on that stream.
#[tokio::test]
async fn too_many_unconsumed_events_reset_the_stream() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let resp = h.send(h.try_reserve().unwrap(), get("/"));
    let (id, _, _) = peer.request().await;
    peer.send_headers(id, 200, &[], false).await;
    let resp = resp.await.unwrap();
    let mut wire = Vec::new();
    for _ in 0..1100 {
        wire.extend_from_slice(&frame::data_frame_header(id, 100, false));
        wire.extend_from_slice(&[7u8; 100]);
    }
    peer.send_raw(&wire).await;
    let mut got = None;
    while let Ok(Some(f)) = tokio::time::timeout(Duration::from_secs(2), peer.try_frame()).await {
        if let F::Rst { id: i, code } = f {
            got = Some((i, code));
            break;
        }
    }
    assert_eq!(got, Some((id, Reason::ENHANCE_YOUR_CALM)));
    drop(resp);
}

/// P2: consumption of a *finished* stream (no stream-level credit, no per-response `Ready`) still releases
/// connection-window credit that was parked behind the buffered budget.
#[tokio::test]
async fn consumption_without_ready_still_credits_the_connection_window() {
    let cfg = H2Config {
        initial_conn_window: 65_535,
        ..H2Config::default()
    };
    let (h, mut peer) = connect(cfg, &[]).await;
    let r1 = h.send(h.try_reserve().unwrap(), get("/a"));
    let r2 = h.send(h.try_reserve().unwrap(), get("/b"));
    let (id1, _, _) = peer.request().await;
    let (id2, _, _) = peer.request().await;
    peer.send_headers(id1, 200, &[], false).await;
    peer.send_headers(id2, 200, &[], false).await;
    let big = vec![1u8; 16_384];
    for _ in 0..3 {
        peer.send_data(id1, &big, false).await;
    }
    peer.send_data(id1, &big[..10_000], true).await;
    for i in 0..100 {
        peer.send_data(id2, &big[..50], i == 99).await;
    }
    let (r1, r2) = (r1.await.unwrap(), r2.await.unwrap());
    let (before, _) = conn_credit(&mut peer, Duration::from_millis(200)).await;
    assert_eq!(before, 0, "credit must be parked behind the budget");
    assert_eq!(collect_body(r1).await.unwrap().len(), 3 * 16_384 + 10_000);
    let (after, _) = conn_credit(&mut peer, Duration::from_millis(300)).await;
    assert!(
        after > 0,
        "consumption never released the connection window"
    );
    drop(r2);
}

fn timeouts_cfg() -> H2Config {
    H2Config {
        response_head_timeout: Some(Duration::from_secs(2)),
        body_idle_timeout: Some(Duration::from_secs(3)),
        header_block_timeout: Duration::from_secs(2),
        keepalive_interval: None,
        ..H2Config::default()
    }
}

#[tokio::test(start_paused = true)]
async fn silent_peer_hits_the_response_head_timeout() {
    let (h, mut peer) = connect(timeouts_cfg(), &[]).await;
    let t0 = tokio::time::Instant::now();
    let resp = h.send(h.try_reserve().unwrap(), get("/"));
    let (id, _, _) = peer.request().await;
    let e = resp.await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Timeout, "{e:?}");
    assert_eq!(e.recovery(), Recovery::Retryable);
    assert!(t0.elapsed() >= Duration::from_secs(2));
    assert!(t0.elapsed() < Duration::from_secs(5));
    loop {
        if let F::Rst { id: i, code } = peer.frame_skip_noise().await {
            assert_eq!((i, code), (id, Reason::CANCEL));
            break;
        }
    }
    assert_eq!(h.in_flight(), 0);
}

#[tokio::test(start_paused = true)]
async fn response_timeout_extension_overrides_the_config() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let t0 = tokio::time::Instant::now();
    let mut req = get("/");
    req.extensions_mut()
        .insert(matier_core::ResponseTimeout(Duration::from_secs(1)));
    let resp = h.send(h.try_reserve().unwrap(), req);
    let _ = peer.request().await;
    let e = resp.await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Timeout, "{e:?}");
    assert!(t0.elapsed() < Duration::from_secs(5), "{:?}", t0.elapsed());
}

#[tokio::test(start_paused = true)]
async fn stalled_body_hits_the_idle_timeout() {
    let (h, mut peer) = connect(timeouts_cfg(), &[]).await;
    let resp = h.send(h.try_reserve().unwrap(), get("/"));
    let (id, _, _) = peer.request().await;
    peer.send_headers(id, 200, &[], false).await;
    peer.send_data(id, b"abc", false).await;
    let resp = resp.await.unwrap();
    let t0 = tokio::time::Instant::now();
    let e = collect_body(resp).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Timeout, "{e:?}");
    assert_eq!(e.recovery(), Recovery::Never);
    assert!(t0.elapsed() >= Duration::from_secs(3));
}

#[tokio::test(start_paused = true)]
async fn unfinished_header_block_times_out_the_connection() {
    let (h, mut peer) = connect(timeouts_cfg(), &[]).await;
    let resp = h.send(h.try_reserve().unwrap(), get("/"));
    let (id, _, _) = peer.request().await;
    // HEADERS without END_HEADERS, then silence.
    let mut block = Vec::new();
    peer.enc.begin_block(&mut block);
    peer.enc.encode_field(b":status", b"200", false, &mut block);
    let mut v = FrameHeader {
        len: block.len() as u32,
        kind: kind::HEADERS,
        flags: 0,
        stream_id: id,
    }
    .encode()
    .to_vec();
    v.extend_from_slice(&block);
    peer.send_raw(&v).await;
    let e = resp.await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Timeout, "{e:?}");
    expect_goaway(&mut peer, Reason::ENHANCE_YOUR_CALM).await;
}

async fn flood_case(wire: Vec<u8>) {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let resp = h.send(h.try_reserve().unwrap(), get("/"));
    let (id, _, _) = peer.request().await;
    peer.send_headers(id, 200, &[], false).await;
    let resp = resp.await.unwrap();
    peer.send_raw(&wire).await;
    expect_goaway(&mut peer, Reason::ENHANCE_YOUR_CALM).await;
    drop(resp);
}

#[tokio::test]
async fn settings_flood_gets_enhance_your_calm() {
    let mut wire = Vec::new();
    for _ in 0..150 {
        frame::encode_settings(&mut wire, &[(settings::ENABLE_PUSH, 0)]);
    }
    flood_case(wire).await;
}

#[tokio::test]
async fn ping_flood_gets_enhance_your_calm() {
    let mut wire = Vec::new();
    for _ in 0..150 {
        frame::encode_ping(&mut wire, false, [1; 8]);
    }
    flood_case(wire).await;
}

#[tokio::test]
async fn rst_stream_flood_gets_enhance_your_calm() {
    let mut wire = Vec::new();
    for _ in 0..300 {
        frame::encode_rst_stream(&mut wire, 1, Reason::CANCEL);
    }
    // The first one resets the stream; later ones hit a closed stream but still count.
    flood_case(wire).await;
}

#[tokio::test]
async fn window_update_flood_gets_enhance_your_calm() {
    let mut wire = Vec::new();
    for _ in 0..1200 {
        frame::encode_window_update(&mut wire, 0, 1);
    }
    flood_case(wire).await;
}

#[tokio::test]
async fn empty_data_flood_gets_enhance_your_calm() {
    let mut wire = Vec::new();
    for _ in 0..1200 {
        wire.extend_from_slice(&frame::data_frame_header(1, 0, false));
    }
    flood_case(wire).await;
}

#[tokio::test]
async fn content_length_mismatch_is_a_stream_error() {
    for (declared, sent) in [(10usize, 5usize), (5, 10)] {
        let (h, mut peer) = connect(H2Config::default(), &[]).await;
        let f = h.send(h.try_reserve().unwrap(), get("/"));
        let (id, _, _) = peer.request().await;
        peer.send_headers(id, 200, &[("content-length", &declared.to_string())], false)
            .await;
        peer.send_data(id, &vec![b'x'; sent], true).await;
        let r = f.await.unwrap();
        let e = collect_body(r).await.unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Protocol, "{e:?}");
        loop {
            if let F::Rst { id: i, code } = peer.frame_skip_noise().await {
                assert_eq!((i, code), (id, Reason::PROTOCOL_ERROR));
                break;
            }
        }
    }
}

#[tokio::test]
async fn head_and_204_may_carry_content_length_without_body() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let mut req = get("/");
    *req.method_mut() = http::Method::HEAD;
    let f = h.send(h.try_reserve().unwrap(), req);
    let (id, _, _) = peer.request().await;
    peer.send_headers(id, 200, &[("content-length", "1000")], true)
        .await;
    assert!(collect_body(f.await.unwrap()).await.unwrap().is_empty());
}

#[tokio::test]
async fn too_many_interim_responses_fail_the_stream() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    let f = h.send(h.try_reserve().unwrap(), get("/"));
    let (id, _, _) = peer.request().await;
    for _ in 0..20 {
        peer.send_headers(id, 103, &[], false).await;
    }
    let e = f.await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Protocol, "{e:?}");
}

#[tokio::test]
async fn invalid_frame_on_unknown_stream_is_not_answered_with_rst() {
    let (h, mut peer) = connect(H2Config::default(), &[]).await;
    // WINDOW_UPDATE with increment 0 on a never-opened (idle) odd stream: the connection survives.
    let mut v = FrameHeader {
        len: 4,
        kind: kind::WINDOW_UPDATE,
        flags: 0,
        stream_id: 99,
    }
    .encode()
    .to_vec();
    v.extend_from_slice(&[0, 0, 0, 0]);
    peer.send_raw(&v).await;
    let mut saw_rst = false;
    while let Ok(Some(f)) = tokio::time::timeout(Duration::from_millis(200), peer.try_frame()).await
    {
        saw_rst |= matches!(f, F::Rst { .. });
    }
    assert!(!saw_rst);
    drop(h);
}

/// Duplex end whose next read blocks the reading thread once, for `delay`, when armed: models an
/// actor tick that takes long (slow I/O, a large inbox batch, preemption) after it has stamped
/// its tick time.
struct StallingIo {
    inner: DuplexStream,
    stall: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl AsyncRead for StallingIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let ms = self.stall.swap(0, std::sync::atomic::Ordering::SeqCst);
        if ms > 0 {
            std::thread::sleep(Duration::from_millis(ms));
        }
        Pin::new(&mut self.inner).poll_read(cx, b)
    }
}

impl AsyncWrite for StallingIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, b)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// A response deadline that passes while a tick is running (after the tick stamped its time,
/// before the deadline scan) must fail the stream with `Timeout`. The scan used the stale tick
/// time, found the deadline still ahead, re-armed the timer at that (already elapsed) instant,
/// saw it fire at once and looped forever inside one poll: the worker thread spun at 100%,
/// mostly in the timer driver's lock, and the connection never made progress again.
#[test]
fn deadline_elapsing_during_a_tick_times_out_instead_of_spinning() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    rt.spawn(async move {
        let stall = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (a, b) = tokio::io::duplex(1 << 20);
        let io = StallingIo {
            inner: a,
            stall: std::sync::Arc::clone(&stall),
        };
        let mut peer = Peer {
            io: b,
            buf: Vec::new(),
            enc: Encoder::new(4096),
            dec: Decoder::new(4096),
            log: Arc::new(Mutex::new(Vec::new())),
        };
        let server = async {
            let mut pre = [0u8; 24];
            peer.io.read_exact(&mut pre).await.unwrap();
            let _ = peer.frame().await;
            peer.send_settings(&[]).await;
        };
        let client =
            super::actor::handshake_io(io, origin(), H2Config::default(), Arc::new(NoopObserver));
        let (h, ()) = tokio::join!(client, server);
        let h = h.expect("handshake");

        let mut req = get("/slow");
        req.extensions_mut()
            .insert(matier_core::ResponseTimeout(Duration::from_millis(300)));
        let fut = h.send(h.try_reserve().unwrap(), req);
        let _ = peer.frame_skip_noise().await; // our HEADERS; never answered
        // Other timers keep the runtime's timer wheel advancing with the clock, as in any busy
        // process; the stalled actor's wheel insertions then land in the past.
        tokio::spawn(async {
            for _ in 0..200 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        // Wake the actor well before the deadline; its read then blocks past the deadline.
        stall.store(400, std::sync::atomic::Ordering::SeqCst);
        peer.send_window_update(0, 1).await;
        let r = fut.await;
        let _ = tx.send(r.err().map(|e| e.kind()));
        drop(peer);
    });
    match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(kind) => assert_eq!(kind, Some(ErrorKind::Timeout)),
        Err(_) => {
            // The actor is stuck in its deadline loop: leak the runtime rather than hang on drop.
            std::mem::forget(rt);
            panic!("the response deadline never fired: the actor is spinning in its deadline loop");
        }
    }
}

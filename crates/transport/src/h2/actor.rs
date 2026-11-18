//! The connection actor: one `Future` per connection, owning the transport, HPACK, stream table, windows and
//! write queue. Each tick runs READ -> INBOX -> SCHEDULE -> WRITE.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::hash::{BuildHasherDefault, Hasher};
use std::io;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use http::HeaderMap;
use matier_core::{
    BytesBuf, BytesView, ConnectionInfo, Error, ErrorKind, Observer, OriginKey, Protocol, Recovery,
    Result,
};
use matier_h2::flow::{RecvWindow, StreamSendWindow, Window, WindowUpdatePolicy};
use matier_h2::frame::{self, Frame, FrameHeader, flags, kind};
use matier_h2::hpack::{BlockKind, Decoder, Encoder};
use matier_h2::settings::{self, Settings};
use matier_h2::{H2Error, Reason};
use slab::Slab;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::mpsc;
use tokio::time::{Instant as TInstant, Sleep};

use super::load::LoadMeter;
use super::shared::{
    CANCELED, CONSUMED_BLOCKED, Command, ConnShared, Dirty, EVENT_OVERHEAD, MAX_RECV_EVENTS,
    OpenBody, OpenStream, RecvEvent, SendEvent, StreamShared,
};
use super::sync::{Arc, Ordering};
use super::write::WriteQueue;
use super::{H2Config, H2Handle, StreamSlot};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const READ_BYTES_BUDGET: usize = 1024 * 1024;
const WRITE_BYTES_BUDGET: usize = 256 * 1024;
const DATA_BYTES_BUDGET: usize = 256 * 1024;
const CMD_BUDGET: usize = 128;
/// Do not schedule new DATA while this much is already queued for the socket.
const WRITE_HIGH_WATER: usize = 256 * 1024;
/// Stop reading (backpressure) if control frames pile up beyond this because the peer is not reading.
const READ_PAUSE_PENDING: usize = 1 << 20;
/// Stop pulling new commands beyond this much queued output.
const INBOX_PAUSE_PENDING: usize = 4 << 20;
/// Request coalescing (`H2Config::coalesce_requests`): hold queued request HEADERS only while at most this
/// much is queued for the socket (anything bigger, e.g. a request body, is written at once) ...
const COALESCE_MAX_PENDING: usize = 1024;
/// ... and more than this many streams are in flight ...
const COALESCE_MIN_IN_FLIGHT: usize = 8;
/// ... until the batch reaches `clamp(in_flight / COALESCE_BATCH_DIV, 2, COALESCE_MAX_BATCH)` requests ...
const COALESCE_MAX_BATCH: u32 = 32;
const COALESCE_BATCH_DIV: usize = 2;
/// ... or the oldest held request is `clamp(rtt / COALESCE_RTT_DIV, COALESCE_MIN_HOLD, cfg.coalesce_max_hold)`
/// old (rtt = smoothed open -> response-head time). Checked on every tick (socket readiness and further
/// requests keep ticks coming); `hold_timer` is the fallback when nothing else polls the actor (tokio
/// timers have millisecond granularity, so that bound is ~1 ms).
const COALESCE_RTT_DIV: u32 = 8;
const COALESCE_MIN_HOLD: Duration = Duration::from_micros(10);
const HOLD_TIMER_FALLBACK: Duration = Duration::from_millis(1);

const READ_BLOCK: usize = 64 * 1024;
const MIN_READ_TAIL: usize = 4096;
const MAX_CONTINUATION_FRAMES: u32 = 32;
/// Fraction of a receive window consumed before a WINDOW_UPDATE is sent (h2/hyper use one half). Consumers
/// report stream consumption at the same point, so every report can produce an update.
const WINDOW_UPDATE_RATIO: f32 = 0.5;
/// Interim (1xx) responses tolerated per request.
const MAX_INFORMATIONAL: u8 = 16;
const MAX_STREAM_ID: u32 = 0x7fff_ffff;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// The body length a response announces: `content-length` unless the response cannot carry a body (HEAD, 204,
/// 304). `Err` for malformed or conflicting values (RFC 9110 8.6).
fn declared_length(
    h: &HeaderMap,
    is_head: bool,
    status: http::StatusCode,
) -> std::result::Result<Option<u64>, ()> {
    if is_head || status == http::StatusCode::NO_CONTENT || status == http::StatusCode::NOT_MODIFIED
    {
        return Ok(None);
    }
    let mut len = None;
    for v in h.get_all(http::header::CONTENT_LENGTH) {
        let n: u64 = std::str::from_utf8(v.as_bytes())
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .ok_or(())?;
        if len.is_some_and(|l| l != n) {
            return Err(());
        }
        len = Some(n);
    }
    Ok(len)
}

fn conn_err(r: Reason, m: &'static str) -> H2Error {
    H2Error::Connection(r, m)
}

// ---------------------------------------------------------------------------------------------------------
// Identity hasher for stream ids.

#[derive(Default)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 << 8 | u64::from(b)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
    }
    fn write_u32(&mut self, i: u32) {
        self.0 = u64::from(i).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}
type IdMap = HashMap<u32, usize, BuildHasherDefault<IdHasher>>;

// ---------------------------------------------------------------------------------------------------------
// Reader: pooled read buffer, frames sliced zero-copy.

/// Receive buffer. `rbuf` holds unconsumed bytes; `cur` is a peeked view of them used for parsing;
/// `skip` bytes at the front of `rbuf` have been parsed (and advanced past in `cur`) but not yet consumed
/// from `rbuf`; they are consumed in one batch (non-DATA frames) or together with the next DATA frame.
pub(crate) struct Reader {
    rbuf: BytesBuf,
    cur: BytesView,
    skip: usize,
    /// Copy target for non-DATA frames that straddle two pooled blocks (small: <= MAX_FRAME_SIZE, control
    /// and header frames only). DATA frames never go through it: they are sliced as multi-span views.
    scratch: Vec<u8>,
}

impl Reader {
    pub(crate) fn new() -> Self {
        Self {
            rbuf: BytesBuf::new(),
            cur: BytesView::new(),
            skip: 0,
            scratch: Vec::new(),
        }
    }

    fn header(&self) -> Option<FrameHeader> {
        if self.cur.len() < 9 {
            return None;
        }
        let mut b = [0u8; 9];
        let fs = self.cur.first_slice();
        if fs.len() >= 9 {
            b.copy_from_slice(&fs[..9]);
        } else {
            self.copy_prefix(&mut b);
        }
        Some(FrameHeader::decode(&b))
    }

    fn copy_prefix(&self, out: &mut [u8]) {
        let mut n = 0;
        for (s, _) in self.cur.slices() {
            let take = s.len().min(out.len() - n);
            out[n..n + take].copy_from_slice(&s[..take]);
            n += take;
            if n == out.len() {
                break;
            }
        }
    }

    fn byte_at_9(&self) -> u8 {
        let mut b = [0u8; 10];
        self.copy_prefix(&mut b);
        b[9]
    }

    /// Makes the next `total` bytes contiguous. Returns true if they are in `scratch`.
    fn contiguous(&mut self, total: usize) -> bool {
        if self.cur.first_slice().len() >= total {
            return false;
        }
        self.scratch.clear();
        self.scratch.resize(total, 0);
        let mut tmp = std::mem::take(&mut self.scratch);
        self.copy_prefix(&mut tmp);
        self.scratch = tmp;
        true
    }

    fn advance(&mut self, total: usize) {
        self.cur.advance(total);
        self.skip += total;
    }

    fn flush_skip(&mut self) {
        if self.skip > 0 {
            drop(self.rbuf.consume(self.skip));
            self.skip = 0;
        }
    }

    /// Slice a DATA frame's payload out of the buffer without copying.
    fn take_data(&mut self, total: usize, head: usize, data_len: usize) -> BytesView {
        let skip = std::mem::take(&mut self.skip);
        let mut v = self.rbuf.consume(skip + total);
        v.advance(skip + head);
        if head + data_len < total {
            v = v.range(..data_len);
        }
        self.cur.advance(total);
        v
    }

    /// Reads more bytes into the first unfilled block.
    ///
    /// Never reserves more than one block, even when the current frame needs more: extra capacity reserved
    /// here was dropped unused by the re-wrap below once the first block filled (pool churn for every frame
    /// larger than a block). A vectored `readv`
    /// over several blocks was measured and rejected (1 MiB bodies: -20% req/s, +25% CPU/req).
    pub(crate) fn poll_fill<T: AsyncRead + Unpin>(
        &mut self,
        io: &mut T,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<usize>> {
        self.flush_skip();
        if self.rbuf.first_unfilled_slice().len() < MIN_READ_TAIL {
            let rest = self.rbuf.consume_all();
            self.rbuf = BytesBuf::from(rest);
            let mem = matier_core::buf::memory();
            self.rbuf.reserve(READ_BLOCK, &mem);
        }
        let n = {
            let dst = self.rbuf.first_unfilled_slice();
            let mut rb = ReadBuf::uninit(dst);
            match Pin::new(io).poll_read(cx, &mut rb) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => rb.filled().len(),
            }
        };
        if n > 0 {
            // SAFETY: tokio's `ReadBuf::filled()` reports exactly the bytes `poll_read` initialized at the
            // start of `first_unfilled_slice()`, which is the span `advance` extends over.
            unsafe { self.rbuf.advance(n) };
            self.cur = self.rbuf.peek();
        }
        Poll::Ready(Ok(n))
    }
}

// ---------------------------------------------------------------------------------------------------------
// Flood limits.

/// Leaky bucket in milli-units: each event adds its cost, the level drains at `rate` units per second, and
/// exceeding `burst` units is a flood.
struct Leaky {
    level: u64,
    last: Option<TInstant>,
    burst: u64,
    rate: u64,
}

impl Leaky {
    const fn new(burst: u32, rate: u32) -> Self {
        Self {
            level: 0,
            last: None,
            burst: burst as u64 * 1000,
            rate: rate as u64,
        }
    }

    /// Records one event of `cost` units after crediting `extra` units; false when the bucket overflows.
    fn hit(&mut self, now: TInstant, cost: u32, extra: u64) -> bool {
        if let Some(last) = self.last {
            let ms =
                u64::try_from(now.saturating_duration_since(last).as_millis()).unwrap_or(u64::MAX);
            self.level = self
                .level
                .saturating_sub(ms.saturating_mul(self.rate))
                .saturating_sub(extra.saturating_mul(1000));
        }
        self.last = Some(now);
        self.level += u64::from(cost) * 1000;
        self.level <= self.burst
    }
}

/// Per-connection flood limits for frames whose cost to us far exceeds their cost to the peer.
struct Floods {
    settings: Leaky,
    ping: Leaky,
    rst: Leaky,
    window_update: Leaky,
    tiny_data: Leaky,
    /// `written_total` at the last WINDOW_UPDATE (legit updates track our outbound bytes).
    wu_written: u64,
}

impl Floods {
    fn new() -> Self {
        Self {
            settings: Leaky::new(100, 10),
            ping: Leaky::new(100, 10),
            rst: Leaky::new(200, 100),
            window_update: Leaky::new(1000, 1000),
            tiny_data: Leaky::new(1000, 1000),
            wu_written: 0,
        }
    }
}

/// DATA payloads shorter than this (without END_STREAM) count against the tiny-DATA bucket.
const TINY_DATA: usize = 16;

// ---------------------------------------------------------------------------------------------------------
// Stream table entries.

enum RecvState {
    AwaitHead,
    Body,
    Done,
}

#[derive(Default)]
struct StreamSend {
    chunks: VecDeque<BytesView>,
    ended: bool,
    trailers: Option<Box<HeaderMap>>,
}

#[allow(clippy::large_enum_variant, reason = "BytesView inlines 8 spans")]
enum SendState {
    None,
    Full(BytesView),
    Stream(Box<StreamSend>),
}

struct StreamEntry {
    id: u32,
    shared: Arc<StreamShared>,
    /// Released when the entry is dropped.
    _slot: StreamSlot,
    send_win: StreamSendWindow,
    recv_win: RecvWindow,
    send: SendState,
    recv: RecvState,
    local_closed: bool,
    remote_closed: bool,
    in_ready: bool,
    /// Absolute write offset just past this stream's HEADERS.
    hdr_end: u64,
    stall_since: Option<Instant>,
    /// Last request/response progress on this stream (tick time).
    activity: TInstant,
    /// When the request was opened (tick time); feeds `Core::rtt_ewma_ns`.
    opened_at: TInstant,
    /// Absolute response-head deadline (from a `ResponseTimeout` extension).
    head_abs: Option<TInstant>,
    /// Max time without progress while waiting for the response head.
    head_idle: Option<Duration>,
    /// Max time without progress between body events.
    body_idle: Option<Duration>,
    /// The request was HEAD (a `content-length` response header does not announce a body).
    is_head: bool,
    /// Declared response body length (`content-length`), when one applies.
    expect_len: Option<u64>,
    /// Response body bytes received so far.
    recv_len: u64,
    /// Interim (1xx) responses seen.
    info_count: u8,
}

impl StreamEntry {
    fn has_data(&self) -> bool {
        match &self.send {
            SendState::None => false,
            SendState::Full(v) => !v.is_empty(),
            SendState::Stream(s) => !s.chunks.is_empty(),
        }
    }
}

struct HeaderAsm {
    started: TInstant,
    stream_id: u32,
    end_stream: bool,
    frames: u32,
    buf: Vec<u8>,
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum ConnState {
    Open,
    /// A fatal error was handled; flush what we can and exit.
    Closing,
}

// ---------------------------------------------------------------------------------------------------------
// Core: everything except the transport, reader and inbox.

pub(crate) struct Core {
    origin: OriginKey,
    obs: std::sync::Arc<dyn Observer>,
    obs_on: bool,
    shared: Arc<ConnShared>,
    cfg: H2Config,
    enc: Encoder,
    dec: Decoder,
    next_stream_id: u32,
    streams: Slab<StreamEntry>,
    by_id: IdMap,
    conn_send: Window,
    conn_recv: RecvWindow,
    /// Bytes received on the connection window and not yet credited back (gated by the buffered budget).
    conn_unreleased: u64,
    initial_send_window: u32,
    peer: Settings,
    local: Settings,
    got_settings: bool,
    first_no_prio: bool,
    wq: WriteQueue,
    /// Streams with DATA to send, as (slab key, stream id). An entry can outlive its stream, whose key a
    /// newer stream may reuse, so the id is checked before the entry is used.
    send_ready: VecDeque<(usize, u32)>,
    asm: Option<HeaderAsm>,
    asm_buf: Vec<u8>,
    hdr_scratch: BytesMut,
    /// Flow-controlled bytes plus per-event overhead delivered to stream queues; the application's matching
    /// total is `ConnShared::consumed`, and `delivered - consumed` is the buffered amount.
    delivered: u64,
    buffered_budget: u64,
    /// `CONSUMED_BLOCKED` is currently set in `ConnShared::consumed`.
    recv_blocked: bool,
    /// Streams owed a wake, delivered in batches (see `StreamShared::owe_wake`).
    dirty: Dirty,
    goaway: Option<u32>,
    state: ConnState,
    policy: WindowUpdatePolicy,
    marks: VecDeque<(u64, Instant)>,
    meter: LoadMeter,
    /// Smoothed request-open -> response-head time (ns); bounds the request-coalescing hold.
    rtt_ewma_ns: u64,
    ping_acked: bool,
    ping_seq: u64,
    ping_out: Option<[u8; 8]>,
    conn_stall: Option<Instant>,
    goaway_queued: bool,
    floods: Floods,
    /// Tick time (one clock read per actor poll).
    now: TInstant,
    /// Earliest instant at which the timeout scan must run next (`None`: nothing to watch).
    timer_at: Option<TInstant>,
    /// `timer_at` moved earlier; the actor must re-arm its single timer.
    timer_dirty: bool,
}

impl Core {
    fn new(
        origin: OriginKey,
        obs: std::sync::Arc<dyn Observer>,
        shared: Arc<ConnShared>,
        cfg: H2Config,
        local: Settings,
    ) -> Self {
        let mut dec = Decoder::new(local.header_table_size as usize);
        dec.set_max_header_list_size(cfg.max_header_list_size as usize);
        let conn_target = cfg.initial_conn_window.clamp(65_535, settings::MAX_WINDOW);
        Self {
            origin,
            obs_on: obs.enabled(),
            obs,
            shared,
            enc: Encoder::new(4096),
            dec,
            next_stream_id: 1,
            streams: Slab::new(),
            by_id: IdMap::default(),
            conn_send: Window::new(65_535),
            conn_recv: RecvWindow::new(conn_target),
            conn_unreleased: 0,
            initial_send_window: 65_535,
            peer: Settings::default(),
            local,
            got_settings: false,
            first_no_prio: false,
            wq: WriteQueue::new(),
            send_ready: VecDeque::new(),
            asm: None,
            asm_buf: Vec::new(),
            hdr_scratch: BytesMut::new(),
            delivered: 0,
            recv_blocked: false,
            dirty: Vec::new(),
            buffered_budget: u64::from(conn_target),
            goaway: None,
            state: ConnState::Open,
            policy: WindowUpdatePolicy {
                threshold_ratio: WINDOW_UPDATE_RATIO,
            },
            marks: VecDeque::new(),
            meter: LoadMeter::new(Instant::now()),
            rtt_ewma_ns: 0,
            ping_acked: false,
            ping_seq: 0,
            ping_out: None,
            conn_stall: None,
            goaway_queued: false,
            floods: Floods::new(),
            now: TInstant::now(),
            timer_at: None,
            timer_dirty: false,
            cfg,
        }
    }

    // ---- control frames (queued in order; they go out ahead of any not-yet-scheduled DATA) ----------

    fn ctrl(&mut self, f: impl FnOnce(&mut Vec<u8>)) {
        let (a, s) = self.wq.begin();
        f(a);
        self.wq.commit(s);
    }

    fn ctrl_rst(&mut self, id: u32, code: Reason, sent: bool) {
        self.ctrl(|a| frame::encode_rst_stream(a, id, code));
        if self.obs_on {
            self.obs.h2_rst_stream(&self.origin, code.0, sent);
        }
    }

    fn ctrl_window_update(&mut self, id: u32, inc: u32) {
        self.ctrl(|a| frame::encode_window_update(a, id, inc));
    }

    fn ctrl_goaway(&mut self, code: Reason, msg: &str) {
        if self.goaway_queued {
            return;
        }
        self.goaway_queued = true;
        self.ctrl(|a| frame::encode_goaway(a, 0, code, msg.as_bytes()));
        if self.obs_on {
            self.obs.h2_goaway(&self.origin, code.0);
        }
    }

    fn accepting(&self) -> bool {
        self.shared.accepting.load(Ordering::Acquire)
    }

    fn stop_accepting(&self) {
        self.shared.accepting.store(false, Ordering::Release);
    }

    // ---- errors ---------------------------------------------------------------------------------------

    fn err_for(written: u64, hdr_end: u64, kind: ErrorKind, src: impl Into<BoxError>) -> Error {
        if hdr_end <= written {
            Error::retryable(kind, src)
        } else {
            Error::unprocessed(kind, src)
        }
    }

    fn reset_recovery(code: Reason) -> Recovery {
        match code {
            Reason::REFUSED_STREAM => Recovery::Unprocessed,
            Reason::NO_ERROR
            | Reason::CANCEL
            | Reason::INTERNAL_ERROR
            | Reason::STREAM_CLOSED
            | Reason::CONNECT_ERROR => Recovery::Retryable,
            _ => Recovery::Never,
        }
    }

    /// Remove stream `k`, failing the consumer with `err` unless the response already completed.
    fn remove_with_error(&mut self, k: usize, err: Error) {
        let st = self.streams.remove(k);
        self.by_id.remove(&st.id);
        if matches!(st.recv, RecvState::Done) {
            st.shared.close_send();
        } else {
            StreamShared::fail(&st.shared, err, &mut self.dirty);
        }
    }

    /// Ask for a timeout scan no later than `d`.
    fn want_timer(&mut self, d: TInstant) {
        if self.timer_at.is_none_or(|t| d < t) {
            self.timer_at = Some(d);
            self.timer_dirty = true;
        }
    }

    /// The timer fired: fail streams (and a header block) that exceeded their deadlines and compute the next
    /// scan time. O(streams), but only runs when the earliest known deadline passes.
    fn scan_timeouts(&mut self) -> std::result::Result<(), H2Error> {
        let now = self.now;
        let mut next: Option<TInstant> = None;
        let mut expired: Vec<(usize, bool)> = Vec::new();
        for (k, st) in &self.streams {
            let d = match st.recv {
                RecvState::AwaitHead => {
                    let idle = st.head_idle.map(|d| st.activity + d);
                    match (idle, st.head_abs) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    }
                }
                RecvState::Body => st.body_idle.map(|d| st.activity + d),
                RecvState::Done => None,
            };
            if let Some(d) = d {
                if d <= now {
                    expired.push((k, matches!(st.recv, RecvState::AwaitHead)));
                } else {
                    next = Some(next.map_or(d, |n| n.min(d)));
                }
            }
        }
        if let Some(a) = &self.asm {
            let d = a.started + self.cfg.header_block_timeout;
            if d <= now {
                return Err(conn_err(
                    Reason::ENHANCE_YOUR_CALM,
                    "header block (CONTINUATION) timed out",
                ));
            }
            next = Some(next.map_or(d, |n| n.min(d)));
        }
        for (k, head) in expired {
            self.stream_timeout(k, head);
        }
        if let Some(n) = next {
            self.timer_at = Some(n.max(now + Duration::from_millis(1)));
            self.timer_dirty = true;
        }
        Ok(())
    }

    fn stream_timeout(&mut self, k: usize, head: bool) {
        let (id, hdr_end) = {
            let st = &self.streams[k];
            (st.id, st.hdr_end)
        };
        self.ctrl_rst(id, Reason::CANCEL, true);
        let err = if head {
            Self::err_for(
                self.wq.written_total(),
                hdr_end,
                ErrorKind::Timeout,
                "timed out waiting for the response head",
            )
        } else {
            Error::with_source(
                ErrorKind::Timeout,
                Recovery::Never,
                "response body idle timeout",
            )
        };
        self.remove_with_error(k, err);
    }

    /// Deliver every wake owed by this tick (one per stream, however many events it received).
    fn drain_dirty(&mut self) {
        for s in self.dirty.drain(..) {
            s.wake_now();
        }
    }

    fn remove_ok(&mut self, k: usize) {
        let st = self.streams.remove(k);
        self.by_id.remove(&st.id);
        st.shared.close_send();
    }

    fn maybe_remove(&mut self, k: usize) {
        let st = &self.streams[k];
        if st.local_closed && st.remote_closed {
            self.remove_ok(k);
        }
    }

    /// Stream-level error: RST_STREAM(reason) and fail the consumer.
    fn stream_error(&mut self, k: usize, reason: Reason, why: &'static str) {
        let id = self.streams[k].id;
        self.ctrl_rst(id, reason, true);
        let err = Error::with_source(
            ErrorKind::Protocol,
            Recovery::Never,
            format!("stream {id} error {reason}: {why}"),
        );
        self.remove_with_error(k, err);
    }

    /// A connection-level failure: queue GOAWAY, fail every stream, stop accepting.
    fn fail_connection(&mut self, e: &H2Error) {
        let (code, msg) = match e {
            H2Error::Connection(r, m) => (*r, *m),
            H2Error::Stream { reason, .. } => (*reason, "stream error treated as connection error"),
        };
        self.ctrl_goaway(code, msg);
        self.state = ConnState::Closing;
        self.fail_all(ErrorKind::Protocol, &e.to_string());
    }

    fn fail_all(&mut self, kind: ErrorKind, msg: &str) {
        self.stop_accepting();
        let written = self.wq.written_total();
        self.send_ready.clear();
        self.by_id.clear();
        for st in self.streams.drain() {
            if matches!(st.recv, RecvState::Done) {
                st.shared.close_send();
            } else {
                StreamShared::fail(
                    &st.shared,
                    Self::err_for(written, st.hdr_end, kind, msg.to_owned()),
                    &mut self.dirty,
                );
            }
        }
    }

    fn is_idle_id(&self, id: u32) -> bool {
        id.is_multiple_of(2) || id >= self.next_stream_id
    }

    // ---- inbox ------------------------------------------------------------------------------------------

    fn open(&mut self, o: OpenStream) {
        let OpenStream {
            shared,
            parts,
            body,
            enqueued_at,
            slot,
        } = o;
        if shared.flags() & CANCELED != 0 {
            shared.close_send();
            return; // never sent: slot released by drop
        }
        if !self.accepting()
            || self.goaway.is_some()
            || self.state != ConnState::Open
            || self.next_stream_id > MAX_STREAM_ID
        {
            if self.next_stream_id > MAX_STREAM_ID {
                self.stop_accepting();
            }
            shared.publish_head(Err(Error::unprocessed(
                ErrorKind::Closed,
                "HTTP/2 connection is not accepting new streams",
            )));
            shared.close_send();
            return;
        }
        let id = self.next_stream_id;
        self.next_stream_id += 2;
        shared.set_id(id);
        let end_stream = matches!(body, OpenBody::Empty);
        let max_frame = self.peer.max_frame_size as usize;
        self.encode_block(id, end_stream, max_frame, |enc, out| {
            enc.encode_request(&parts.method, &parts.uri, &parts.headers, out);
        });
        let hdr_end = self.wq.enqueued_total();
        if let Some(t) = enqueued_at {
            self.marks.push_back((hdr_end, t));
        }
        self.shared.streams_opened.store(
            self.shared
                .streams_opened
                .load(Ordering::Relaxed)
                .wrapping_add(1),
            Ordering::Relaxed,
        );
        let send = match body {
            OpenBody::Empty => SendState::None,
            OpenBody::Full(v) => SendState::Full(v),
            OpenBody::Streaming => SendState::Stream(Box::default()),
        };
        let now = self.now;
        let head_abs = parts
            .extensions
            .get::<matier_core::ResponseTimeout>()
            .map(|t| now + t.0);
        let head_idle = if head_abs.is_some() {
            None
        } else {
            self.cfg.response_head_timeout
        };
        let body_idle = parts
            .extensions
            .get::<matier_core::BodyTimeout>()
            .map(|t| t.0)
            .or(self.cfg.body_idle_timeout);
        if let Some(d) = head_abs.or_else(|| head_idle.map(|d| now + d)) {
            self.want_timer(d);
        }
        let entry = StreamEntry {
            id,
            shared,
            _slot: slot,
            send_win: StreamSendWindow::new(),
            recv_win: RecvWindow::new(self.cfg.initial_stream_window),
            send,
            recv: RecvState::AwaitHead,
            local_closed: end_stream,
            remote_closed: false,
            in_ready: false,
            hdr_end,
            stall_since: None,
            activity: now,
            opened_at: now,
            head_abs,
            head_idle,
            body_idle,
            is_head: parts.method == http::Method::HEAD,
            expect_len: None,
            recv_len: 0,
            info_count: 0,
        };
        let has_full = matches!(entry.send, SendState::Full(_));
        let k = self.streams.insert(entry);
        self.by_id.insert(id, k);
        if has_full {
            self.rearm(k);
        }
    }

    /// Encode a header block (HEADERS + CONTINUATIONs) for `id` directly into the arena.
    fn encode_block(
        &mut self,
        id: u32,
        end_stream: bool,
        max_frame: usize,
        f: impl FnOnce(&mut Encoder, &mut Vec<u8>),
    ) {
        let (arena, start) = self.wq.begin();
        arena.extend_from_slice(&[0u8; 9]);
        f(&mut self.enc, arena);
        let blen = arena.len() - start - 9;
        if blen <= max_frame {
            let mut fl = flags::END_HEADERS;
            if end_stream {
                fl |= flags::END_STREAM;
            }
            arena[start..start + 9].copy_from_slice(
                &FrameHeader {
                    len: blen as u32,
                    kind: kind::HEADERS,
                    flags: fl,
                    stream_id: id,
                }
                .encode(),
            );
        } else {
            let block = arena[start + 9..].to_vec();
            arena.truncate(start);
            frame::encode_headers(arena, id, end_stream, &block, max_frame as u32);
        }
        self.wq.commit(start);
    }

    fn on_ready(&mut self, shared: &Arc<StreamShared>) {
        let f = shared.begin_ready();
        let consumed = shared.take_consumed();
        // Consumption may have unblocked connection-window credit (no stream needed for that).
        self.release_conn();
        let Some(&k) = self.by_id.get(&shared.id()) else {
            return;
        };
        if f & CANCELED != 0 {
            self.cancel_stream(k);
            return;
        }
        let st = &mut self.streams[k];
        if consumed > 0 && !matches!(st.recv, RecvState::Done) {
            st.recv_win.release(consumed);
            if let Some(inc) = st.recv_win.poll_update(&self.policy) {
                let id = st.id;
                self.ctrl_window_update(id, inc);
            }
        }
        self.drain_send_events(k);
    }

    fn cancel_stream(&mut self, k: usize) {
        let st = self.streams.remove(k);
        self.by_id.remove(&st.id);
        if !(st.local_closed && st.remote_closed) {
            self.ctrl_rst(st.id, Reason::CANCEL, true);
        }
        st.shared.drain_recv();
        st.shared.close_send();
        // The queue may have held the bytes that kept connection-window credit parked.
        self.release_conn();
    }

    fn drain_send_events(&mut self, k: usize) {
        let st = &mut self.streams[k];
        let SendState::Stream(s) = &mut st.send else {
            return;
        };
        let mut abort = None;
        while let Some(ev) = st.shared.pop_send() {
            match ev {
                SendEvent::Data(d) => s.chunks.push_back(*d),
                SendEvent::Trailers(t) => s.trailers = Some(t),
                SendEvent::End => s.ended = true,
                SendEvent::Abort(e) => {
                    abort = Some(*e);
                    break;
                }
            }
        }
        if let Some(e) = abort {
            let id = st.id;
            self.ctrl_rst(id, Reason::CANCEL, true);
            let e = Error::with_source(ErrorKind::BodyRead, Recovery::Never, e);
            self.remove_with_error(k, e);
            return;
        }
        let ended = s.ended;
        let has_data = !s.chunks.is_empty();
        let closed = st.local_closed;
        if has_data {
            self.rearm(k);
        } else if ended && !closed {
            self.finish_send(k);
        }
    }

    /// Put stream `k` on the send list if it has data and window.
    fn rearm(&mut self, k: usize) {
        self.rearm_at(k, false);
    }

    /// [`Self::rearm`], at the head of the send list when `front` is set (`H2Config::fifo_send`).
    fn rearm_at(&mut self, k: usize, front: bool) {
        let initial = self.initial_send_window;
        let st = &mut self.streams[k];
        if st.in_ready || !st.has_data() {
            return;
        }
        if st.send_win.available(initial) == 0 {
            if self.obs_on && st.stall_since.is_none() {
                st.stall_since = Some(Instant::now());
            }
            return;
        }
        if let Some(t) = st.stall_since.take() {
            self.obs.flow_control_stall(&self.origin, t.elapsed());
        }
        st.in_ready = true;
        let entry = (k, st.id);
        if front {
            self.send_ready.push_front(entry);
        } else {
            self.send_ready.push_back(entry);
        }
    }

    /// Nothing more than END (and trailers) remains to be sent on stream `k`.
    fn finish_send(&mut self, k: usize) {
        let st = &mut self.streams[k];
        let id = st.id;
        let trailers = match &mut st.send {
            SendState::Stream(s) => s.trailers.take(),
            _ => None,
        };
        st.send = SendState::None;
        st.local_closed = true;
        match trailers {
            Some(t) => {
                let max_frame = self.peer.max_frame_size as usize;
                self.encode_block(id, true, max_frame, |enc, out| {
                    enc.begin_block(out);
                    for (n, v) in t.iter() {
                        if matier_h2::hpack::is_connection_specific(n.as_str(), v.as_bytes())
                            || n.as_str().starts_with(':')
                        {
                            continue;
                        }
                        enc.encode_field(
                            n.as_str().as_bytes(),
                            v.as_bytes(),
                            v.is_sensitive(),
                            out,
                        );
                    }
                });
            }
            None => {
                let h = frame::data_frame_header(id, 0, true);
                self.ctrl(|a| a.extend_from_slice(&h));
            }
        }
        self.maybe_remove(k);
    }

    // ---- scheduling -------------------------------------------------------------------------------------

    /// Emit DATA frames round-robin. Returns true if sendable work remains (the caller should yield and retry).
    fn schedule(&mut self) -> bool {
        let mut budget = DATA_BYTES_BUDGET;
        while budget > 0 && self.wq.pending() < WRITE_HIGH_WATER {
            if self.conn_send.available() == 0 {
                if !self.send_ready.is_empty() && self.obs_on && self.conn_stall.is_none() {
                    self.conn_stall = Some(Instant::now());
                }
                return false;
            }
            let Some((k, id)) = self.send_ready.pop_front() else {
                return false;
            };
            if !matches!(self.streams.get(k), Some(st) if st.id == id) {
                // Charged like an empty emit so a backlog of stale entries is spread across ticks.
                budget -= 1;
                continue;
            }
            self.streams[k].in_ready = false;
            let n = self.emit_data(k, budget);
            budget = budget.saturating_sub(n.max(1));
        }
        self.can_schedule()
    }

    /// One DATA frame for stream `k`; returns payload bytes emitted.
    fn emit_data(&mut self, k: usize, budget: usize) -> usize {
        let initial = self.initial_send_window;
        let max_frame = self.peer.max_frame_size;
        let now = self.now;
        let st = &mut self.streams[k];
        let win = st.send_win.available(initial);
        if win == 0 {
            if self.obs_on && st.stall_since.is_none() {
                st.stall_since = Some(Instant::now());
            }
            return 0;
        }
        st.activity = now;
        let cap = win
            .min(self.conn_send.available())
            .min(max_frame)
            .min(u32::try_from(budget).unwrap_or(u32::MAX)) as usize;
        let id = st.id;
        let (chunk, last_chunk, ended_all) = match &mut st.send {
            SendState::None => return 0,
            SendState::Full(v) => {
                let n = cap.min(v.len());
                let chunk = if n == v.len() {
                    std::mem::take(v)
                } else {
                    let c = v.range(..n);
                    v.advance(n);
                    c
                };
                let done = v.is_empty();
                (chunk, done, done)
            }
            SendState::Stream(s) => {
                let Some(front) = s.chunks.front_mut() else {
                    return 0;
                };
                let n = cap.min(front.len());
                let chunk = if n == front.len() {
                    s.chunks.pop_front().expect("front exists")
                } else {
                    let c = front.range(..n);
                    front.advance(n);
                    c
                };
                let drained = s.chunks.is_empty();
                (chunk, drained, drained && s.ended && s.trailers.is_none())
            }
        };
        let n = chunk.len();
        // Windows were checked above, so these cannot fail.
        let _ = st.send_win.consume(n as u32, initial);
        let _ = self.conn_send.consume(n as u32);
        let hdr = frame::data_frame_header(id, n as u32, ended_all);
        if let SendState::Stream(_) = st.send {
            st.shared.add_credit(n);
        }
        self.wq.push_frame_with_payload(hdr, chunk);
        let st = &mut self.streams[k];
        if ended_all {
            st.send = SendState::None;
            st.local_closed = true;
            self.maybe_remove(k);
        } else if last_chunk {
            // Stream body: more may come (or END/trailers are pending).
            let pending_end = matches!(&st.send, SendState::Stream(s) if s.ended);
            if pending_end {
                self.finish_send(k);
            }
        } else {
            // More of this body is ready: round-robin moves to the next stream, FIFO stays on this one.
            let front = self.cfg.fifo_send;
            self.rearm_at(k, front);
        }
        n
    }

    // ---- frames -----------------------------------------------------------------------------------------

    fn on_frame(&mut self, f: Frame<'_>) -> std::result::Result<(), H2Error> {
        if let Some(a) = &self.asm {
            match &f {
                Frame::Continuation { stream_id, .. } if *stream_id == a.stream_id => {}
                _ => {
                    return Err(conn_err(
                        Reason::PROTOCOL_ERROR,
                        "expected CONTINUATION for open header block",
                    ));
                }
            }
        }
        match f {
            Frame::Data { .. } => Err(conn_err(Reason::INTERNAL_ERROR, "DATA on slow path")),
            Frame::Headers {
                stream_id,
                end_stream,
                end_headers,
                fragment,
                ..
            } => {
                if stream_id.is_multiple_of(2) {
                    return Err(conn_err(Reason::PROTOCOL_ERROR, "HEADERS on even stream"));
                }
                if end_headers {
                    let block = self.hdr_bytes(fragment);
                    self.on_block(stream_id, end_stream, &block)
                } else {
                    let mut buf = std::mem::take(&mut self.asm_buf);
                    buf.clear();
                    buf.extend_from_slice(fragment);
                    let now = self.now;
                    self.want_timer(now + self.cfg.header_block_timeout);
                    self.asm = Some(HeaderAsm {
                        started: now,
                        stream_id,
                        end_stream,
                        frames: 1,
                        buf,
                    });
                    Ok(())
                }
            }
            Frame::Continuation {
                stream_id,
                end_headers,
                fragment,
            } => {
                let Some(a) = self.asm.as_mut() else {
                    return Err(conn_err(Reason::PROTOCOL_ERROR, "unexpected CONTINUATION"));
                };
                a.frames += 1;
                let cap = (self.cfg.max_header_list_size as usize).max(16 * 1024) * 2;
                if a.frames > MAX_CONTINUATION_FRAMES || a.buf.len() + fragment.len() > cap {
                    return Err(conn_err(
                        Reason::ENHANCE_YOUR_CALM,
                        "header block too large (CONTINUATION flood)",
                    ));
                }
                a.buf.extend_from_slice(fragment);
                if end_headers {
                    let a = self.asm.take().expect("checked");
                    let block = self.hdr_bytes(&a.buf);
                    let r = self.on_block(stream_id, a.end_stream, &block);
                    self.asm_buf = a.buf;
                    r
                } else {
                    Ok(())
                }
            }
            Frame::Priority { .. } | Frame::PriorityUpdate { .. } | Frame::Unknown { .. } => Ok(()),
            Frame::RstStream { stream_id, code } => {
                self.flood(|f| &mut f.rst, 1, 0, "RST_STREAM flood")?;
                self.on_rst(stream_id, code)
            }
            Frame::Settings { ack, params } => {
                if ack {
                    Ok(())
                } else {
                    self.flood(|f| &mut f.settings, 1, 0, "SETTINGS flood")?;
                    self.on_settings(params)
                }
            }
            Frame::PushPromise { .. } => Err(conn_err(
                Reason::PROTOCOL_ERROR,
                "PUSH_PROMISE received but push is disabled",
            )),
            Frame::Ping { ack, data } => {
                if ack {
                    if self.ping_out == Some(data) {
                        self.ping_out = None;
                        self.ping_acked = true;
                    }
                } else {
                    self.flood(|f| &mut f.ping, 1, 0, "PING flood")?;
                    self.ctrl(|a| frame::encode_ping(a, true, data));
                }
                Ok(())
            }
            Frame::GoAway {
                last_stream_id,
                code,
                ..
            } => {
                self.on_goaway(last_stream_id, code);
                Ok(())
            }
            Frame::WindowUpdate {
                stream_id,
                increment,
            } => {
                let written = self.wq.written_total();
                // Every 16 KiB we have written legitimately earns the peer one update.
                let credit = written.saturating_sub(self.floods.wu_written) / 16_384;
                if credit > 0 {
                    self.floods.wu_written = written;
                }
                self.flood(|f| &mut f.window_update, 1, credit, "WINDOW_UPDATE flood")?;
                self.on_window_update(stream_id, increment)
            }
        }
    }

    /// Charges one event to a flood bucket; a flood is a connection error (ENHANCE_YOUR_CALM).
    fn flood(
        &mut self,
        pick: impl FnOnce(&mut Floods) -> &mut Leaky,
        cost: u32,
        extra: u64,
        why: &'static str,
    ) -> std::result::Result<(), H2Error> {
        let now = self.now;
        if pick(&mut self.floods).hit(now, cost, extra) {
            Ok(())
        } else {
            Err(conn_err(Reason::ENHANCE_YOUR_CALM, why))
        }
    }

    fn hdr_bytes(&mut self, frag: &[u8]) -> Bytes {
        if self.hdr_scratch.capacity() < frag.len() {
            self.hdr_scratch.reserve(frag.len().max(8192));
        }
        self.hdr_scratch.extend_from_slice(frag);
        self.hdr_scratch.split().freeze()
    }

    fn on_block(
        &mut self,
        id: u32,
        end_stream: bool,
        block: &Bytes,
    ) -> std::result::Result<(), H2Error> {
        let Some(&k) = self.by_id.get(&id) else {
            if self.is_idle_id(id) {
                return Err(conn_err(Reason::PROTOCOL_ERROR, "HEADERS on idle stream"));
            }
            // Closed/reset stream: keep HPACK in sync and drop it.
            return self
                .dec
                .decode(block, |_, _, _| {})
                .map_err(|e| e.into_h2());
        };
        let now = self.now;
        let st = &mut self.streams[k];
        st.activity = now;
        match st.recv {
            RecvState::AwaitHead => {
                let head = match self.dec.decode_head(block, BlockKind::Response, id) {
                    Ok(h) => h,
                    Err(H2Error::Stream { reason, .. }) => {
                        self.stream_error(k, reason, "malformed response headers");
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                };
                let status = head.status.expect("response head has :status");
                if status.is_informational() {
                    if end_stream {
                        self.stream_error(k, Reason::PROTOCOL_ERROR, "1xx with END_STREAM");
                    } else {
                        st.info_count += 1;
                        if st.info_count > MAX_INFORMATIONAL {
                            self.stream_error(k, Reason::PROTOCOL_ERROR, "too many 1xx responses");
                        }
                    }
                    return Ok(());
                }
                match declared_length(&head.headers, st.is_head, status) {
                    Ok(l) => st.expect_len = l,
                    Err(()) => {
                        self.stream_error(k, Reason::PROTOCOL_ERROR, "invalid content-length");
                        return Ok(());
                    }
                }
                let (mut parts, ()) = http::Response::new(()).into_parts();
                parts.status = status;
                parts.version = http::Version::HTTP_2;
                parts.headers = head.headers;
                if self.cfg.connection_info {
                    parts.extensions.insert(ConnectionInfo {
                        protocol: Protocol::Http2,
                        reused: id > 1,
                        conn_id: self.shared.conn_id,
                    });
                }
                st.recv = RecvState::Body;
                let rtt = now.saturating_duration_since(st.opened_at).as_nanos() as u64;
                if let Some(d) = st.body_idle {
                    self.want_timer(now + d);
                }
                self.rtt_ewma_ns = if self.rtt_ewma_ns == 0 {
                    rtt
                } else {
                    self.rtt_ewma_ns - self.rtt_ewma_ns / 16 + rtt / 16
                };
                let st = &self.streams[k];
                StreamShared::publish_head_deferred(&st.shared, Ok(parts), &mut self.dirty);
                if end_stream {
                    self.remote_end(k, true);
                }
                Ok(())
            }
            RecvState::Body => match self.dec.decode_head(block, BlockKind::Trailers, id) {
                Ok(h) => {
                    if !end_stream {
                        self.stream_error(k, Reason::PROTOCOL_ERROR, "trailers without END_STREAM");
                        return Ok(());
                    }
                    let st = &self.streams[k];
                    if !h.headers.is_empty() {
                        StreamShared::push_recv(
                            &st.shared,
                            RecvEvent::Trailers(Box::new(h.headers)),
                            &mut self.dirty,
                        );
                    }
                    self.remote_end(k, true);
                    Ok(())
                }
                Err(H2Error::Stream { reason, .. }) => {
                    self.stream_error(k, reason, "malformed trailers");
                    Ok(())
                }
                Err(e) => Err(e),
            },
            RecvState::Done => {
                self.dec
                    .decode(block, |_, _, _| {})
                    .map_err(|e| e.into_h2())?;
                self.stream_error(k, Reason::STREAM_CLOSED, "HEADERS after END_STREAM");
                Ok(())
            }
        }
    }

    /// The server finished its response on stream `k`. `push_end`: queue a separate `End` event (false when
    /// END_STREAM was folded into the last DATA event).
    fn remote_end(&mut self, k: usize, push_end: bool) {
        let st = &mut self.streams[k];
        if st.expect_len.is_some_and(|l| l != st.recv_len) {
            self.stream_error(
                k,
                Reason::PROTOCOL_ERROR,
                "response body shorter than content-length",
            );
            return;
        }
        if push_end {
            StreamShared::push_recv(&st.shared, RecvEvent::End, &mut self.dirty);
        }
        st.recv = RecvState::Done;
        st.remote_closed = true;
        if st.local_closed {
            self.remove_ok(k);
        } else {
            // Response complete before the request body was: tell the server to stop (RFC 9113 8.1).
            let id = st.id;
            self.ctrl_rst(id, Reason::NO_ERROR, true);
            self.remove_ok(k);
        }
    }

    pub(crate) fn on_data(
        &mut self,
        id: u32,
        end_stream: bool,
        flow_len: u32,
        data: BytesView,
    ) -> std::result::Result<(), H2Error> {
        self.conn_recv
            .on_data(flow_len)
            .map_err(|e| e.into_error(0))?;
        self.conn_unreleased += u64::from(flow_len);
        let Some(&k) = self.by_id.get(&id) else {
            if self.is_idle_id(id) {
                return Err(conn_err(Reason::PROTOCOL_ERROR, "DATA on idle stream"));
            }
            return Ok(()); // closed stream: bytes already credited to the connection window
        };
        let now = self.now;
        let st = &mut self.streams[k];
        st.activity = now;
        match st.recv {
            RecvState::AwaitHead => {
                self.stream_error(k, Reason::PROTOCOL_ERROR, "DATA before response headers");
                return Ok(());
            }
            RecvState::Done => {
                self.stream_error(k, Reason::STREAM_CLOSED, "DATA after END_STREAM");
                return Ok(());
            }
            RecvState::Body => {}
        }
        if st.recv_win.on_data(flow_len).is_err() {
            self.stream_error(k, Reason::FLOW_CONTROL_ERROR, "stream window exceeded");
            return Ok(());
        }
        let n = data.len();
        let pad = flow_len as usize - n;
        let st = &mut self.streams[k];
        st.recv_len += n as u64;
        if st.expect_len.is_some_and(|l| st.recv_len > l) {
            self.stream_error(
                k,
                Reason::PROTOCOL_ERROR,
                "response body longer than content-length",
            );
            return Ok(());
        }
        // Checked before the (END-folded) last event is queued, so the consumer never sees a clean end.
        if end_stream && st.expect_len.is_some_and(|l| l != st.recv_len) {
            self.stream_error(
                k,
                Reason::PROTOCOL_ERROR,
                "response body shorter than content-length",
            );
            return Ok(());
        }
        if n < TINY_DATA && !end_stream {
            self.flood(|f| &mut f.tiny_data, 1, 0, "tiny DATA flood")?;
        }
        let st = &mut self.streams[k];
        if st.shared.pending_events() >= MAX_RECV_EVENTS {
            self.stream_error(
                k,
                Reason::ENHANCE_YOUR_CALM,
                "too many unconsumed receive events",
            );
            return Ok(());
        }
        if n == 0 {
            // Nothing is buffered for the application: return the (padding-only) credit right away.
            if flow_len > 0 && !end_stream {
                st.recv_win.release(flow_len);
                if let Some(inc) = st.recv_win.poll_update(&self.policy) {
                    self.ctrl_window_update(id, inc);
                }
            }
            if end_stream {
                self.remote_end(k, true);
            }
            return Ok(());
        }
        // Tiny or padded payloads are copied out of the read block so a flood of them cannot pin 64 KiB
        // blocks one byte at a time. Everything else stays a zero-copy slice.
        let data = if n < 16 || (pad > 0 && n < 256) {
            matier_core::buf::from_bytes(Bytes::from(data.to_vec()))
        } else {
            data
        };
        self.delivered += u64::from(flow_len) + EVENT_OVERHEAD;
        StreamShared::push_recv(
            &st.shared,
            RecvEvent::Data {
                view: data,
                flow: flow_len,
                end: end_stream,
            },
            &mut self.dirty,
        );
        if end_stream {
            self.remote_end(k, false);
        }
        Ok(())
    }

    fn on_rst(&mut self, id: u32, code: Reason) -> std::result::Result<(), H2Error> {
        if self.is_idle_id(id) {
            return Err(conn_err(
                Reason::PROTOCOL_ERROR,
                "RST_STREAM on idle stream",
            ));
        }
        if self.obs_on {
            self.obs.h2_rst_stream(&self.origin, code.0, false);
        }
        if let Some(&k) = self.by_id.get(&id) {
            let err = Error::with_source(
                ErrorKind::Protocol,
                Self::reset_recovery(code),
                H2Error::Stream { id, reason: code },
            );
            self.remove_with_error(k, err);
        }
        Ok(())
    }

    fn on_settings(
        &mut self,
        params: matier_h2::frame::SettingsIter<'_>,
    ) -> std::result::Result<(), H2Error> {
        let old_max = self.peer.max_concurrent_streams;
        let changes = self.peer.apply_all(params)?;
        if self.got_settings {
            if self.peer.no_rfc7540_priorities != self.first_no_prio {
                return Err(conn_err(
                    Reason::PROTOCOL_ERROR,
                    "SETTINGS_NO_RFC7540_PRIORITIES changed after first SETTINGS",
                ));
            }
        } else {
            self.got_settings = true;
            self.first_no_prio = self.peer.no_rfc7540_priorities;
        }
        if let Some(n) = changes.header_table_size {
            self.enc.set_peer_table_size(n as usize);
        }
        if let Some((old, new)) = changes.initial_window_size {
            self.initial_send_window = new;
            if new > old {
                // Overflow check across all live streams, then re-arm those that gained window.
                for (_, st) in &self.streams {
                    st.send_win
                        .check(new)
                        .map_err(|e| e.into_error(st.id))
                        .map_err(|_| {
                            conn_err(
                                Reason::FLOW_CONTROL_ERROR,
                                "INITIAL_WINDOW_SIZE overflows a stream window",
                            )
                        })?;
                }
                for k in 0..self.streams.capacity() {
                    if self.streams.contains(k) {
                        self.rearm(k);
                    }
                }
            }
        }
        if self.peer.max_concurrent_streams != old_max {
            let v = self
                .peer
                .max_concurrent_streams
                .map_or(usize::MAX, |v| v as usize);
            self.shared.max_streams.store(v, Ordering::Release);
            self.shared.slot_released.notify_waiters();
        }
        self.ctrl(frame::encode_settings_ack);
        Ok(())
    }

    fn on_window_update(&mut self, id: u32, inc: u32) -> std::result::Result<(), H2Error> {
        if id == 0 {
            self.conn_send.increase(inc).map_err(|e| e.into_error(0))?;
            if let Some(t) = self.conn_stall.take() {
                self.obs.flow_control_stall(&self.origin, t.elapsed());
            }
            return Ok(());
        }
        let Some(&k) = self.by_id.get(&id) else {
            if self.is_idle_id(id) {
                return Err(conn_err(
                    Reason::PROTOCOL_ERROR,
                    "WINDOW_UPDATE on idle stream",
                ));
            }
            return Ok(());
        };
        let initial = self.initial_send_window;
        if self.streams[k].send_win.increase(inc, initial).is_err() {
            self.stream_error(k, Reason::FLOW_CONTROL_ERROR, "stream window overflow");
            return Ok(());
        }
        self.rearm(k);
        Ok(())
    }

    fn on_goaway(&mut self, last: u32, code: Reason) {
        if self.obs_on {
            self.obs.h2_goaway(&self.origin, code.0);
        }
        self.stop_accepting();
        let last = self.goaway.map_or(last, |g| g.min(last));
        self.goaway = Some(last);
        let doomed: Vec<usize> = self
            .streams
            .iter()
            .filter(|(_, s)| s.id > last)
            .map(|(k, _)| k)
            .collect();
        for k in doomed {
            let e = Error::unprocessed(
                ErrorKind::Closed,
                format!("GOAWAY ({code}): stream was not processed by the server"),
            );
            self.remove_with_error(k, e);
        }
    }

    fn buffered_given(&self, consumed_word: u64) -> u64 {
        self.delivered
            .saturating_sub(consumed_word & !CONSUMED_BLOCKED)
    }

    /// Credit the connection window once the buffered bytes are within budget. When they are not, publish
    /// `recv_blocked` (set state) and re-check (then look), so a consumer that raced past us still triggers a
    /// `Ready`.
    fn release_conn(&mut self) {
        if self.conn_unreleased > 0 {
            let mut word = self.shared.consumed.load(Ordering::Acquire);
            let mut ok = self.buffered_given(word) <= self.buffered_budget;
            if !ok && !self.recv_blocked {
                self.recv_blocked = true;
                word = self
                    .shared
                    .consumed
                    .fetch_or(CONSUMED_BLOCKED, Ordering::AcqRel);
                ok = self.buffered_given(word) <= self.buffered_budget;
            }
            if ok {
                if self.recv_blocked {
                    self.recv_blocked = false;
                    self.shared
                        .consumed
                        .fetch_and(!CONSUMED_BLOCKED, Ordering::AcqRel);
                }
                let n = std::mem::take(&mut self.conn_unreleased).min(u64::from(u32::MAX)) as u32;
                self.conn_recv.release(n);
            }
        }
        if let Some(inc) = self.conn_recv.poll_update(&self.policy) {
            self.ctrl_window_update(0, inc);
        }
    }

    fn marks_flushed(&mut self) {
        if self.marks.is_empty() {
            return;
        }
        let w = self.wq.written_total();
        let now = Instant::now();
        while let Some(&(off, t)) = self.marks.front() {
            if off > w {
                break;
            }
            self.marks.pop_front();
            let d = now.saturating_duration_since(t);
            self.meter.record_dispatch(d);
            if self.obs_on {
                self.obs.request_dispatched(&self.origin, d);
            }
        }
    }

    fn can_schedule(&self) -> bool {
        !self.send_ready.is_empty()
            && self.conn_send.available() > 0
            && self.wq.pending() < WRITE_HIGH_WATER
    }
}

// ---------------------------------------------------------------------------------------------------------
// Actor.

struct Keepalive {
    sleep: Pin<Box<Sleep>>,
    last_read: tokio::time::Instant,
    outstanding: bool,
}

pub(crate) struct Actor<T> {
    io: T,
    rd: Reader,
    core: Core,
    rx: mpsc::UnboundedReceiver<Command>,
    eof: bool,
    inbox_closed: bool,
    keepalive: Option<Keepalive>,
    /// Requests opened in the current tick (input to the small-write deferral).
    opened_this_tick: u32,
    /// Opened requests whose HEADERS are queued but held back (request coalescing).
    held: u32,
    hold_since: Instant,
    hold_timer: Pin<Box<Sleep>>,
    hold_armed: bool,
    /// The one timer for all stream/header-block deadlines (see `Core::scan_timeouts`).
    timer: Pin<Box<Sleep>>,
    torn_down: bool,
}

enum Fail {
    Proto(H2Error),
    Io(io::Error),
}

impl From<H2Error> for Fail {
    fn from(e: H2Error) -> Self {
        Fail::Proto(e)
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Actor<T> {
    fn read_phase(&mut self, cx: &mut Context<'_>) -> std::result::Result<bool, Fail> {
        let budget_frames = self.core.cfg.read_budget_frames.max(1);
        let max = self.core.local.max_frame_size;
        let mut frames = 0usize;
        let mut bytes = 0usize;
        let mut yielded = false;
        loop {
            if frames >= budget_frames || bytes >= READ_BYTES_BUDGET {
                yielded = true;
                break;
            }
            if self.core.wq.pending() > READ_PAUSE_PENDING {
                break; // backpressure: the peer is not reading; resume when a write completes
            }
            if let Some(h) = self.rd.header() {
                if h.len > max {
                    return Err(conn_err(
                        Reason::FRAME_SIZE_ERROR,
                        "frame exceeds SETTINGS_MAX_FRAME_SIZE",
                    )
                    .into());
                }
                let total = 9 + h.len as usize;
                if self.rd.cur.len() >= total {
                    frames += 1;
                    bytes += total;
                    self.process_frame(h, total)?;
                    continue;
                }
            }
            match self.rd.poll_fill(&mut self.io, cx) {
                Poll::Pending => break,
                Poll::Ready(Err(e)) => return Err(Fail::Io(e)),
                Poll::Ready(Ok(0)) => {
                    self.eof = true;
                    break;
                }
                Poll::Ready(Ok(n)) => {
                    if self.core.obs_on {
                        self.core.obs.bytes_read(&self.core.origin, n);
                    }
                    if let Some(k) = &mut self.keepalive {
                        k.last_read = self.core.now; // tick time: ample precision for a keepalive interval
                    }
                }
            }
        }
        self.rd.flush_skip();
        self.core.release_conn(); // credit the connection window (gated by the buffered-bytes budget)
        self.core.drain_dirty();
        Ok(yielded)
    }

    fn process_frame(&mut self, h: FrameHeader, total: usize) -> std::result::Result<(), Fail> {
        if h.kind == kind::DATA {
            if h.stream_id == 0 {
                return Err(conn_err(Reason::PROTOCOL_ERROR, "DATA on stream 0").into());
            }
            if self.core.asm.is_some() {
                return Err(conn_err(Reason::PROTOCOL_ERROR, "DATA inside header block").into());
            }
            let mut head = 9usize;
            let mut pad = 0usize;
            if h.flags & flags::PADDED != 0 {
                if h.len == 0 {
                    return Err(conn_err(Reason::FRAME_SIZE_ERROR, "missing pad length").into());
                }
                pad = usize::from(self.rd.byte_at_9());
                if pad >= h.len as usize {
                    return Err(conn_err(Reason::PROTOCOL_ERROR, "padding exceeds payload").into());
                }
                head = 10;
            }
            let data_len = h.len as usize - (head - 9) - pad;
            let v = self.rd.take_data(total, head, data_len);
            self.core
                .on_data(h.stream_id, h.flags & flags::END_STREAM != 0, h.len, v)?;
            return Ok(());
        }
        let scratch = self.rd.contiguous(total);
        let bytes: &[u8] = if scratch {
            &self.rd.scratch[..total]
        } else {
            &self.rd.cur.first_slice()[..total]
        };
        match frame::decode_frame(bytes, self.core.local.max_frame_size) {
            Ok(Some((f, _))) => self.core.on_frame(f)?,
            Ok(None) => {
                return Err(conn_err(
                    Reason::INTERNAL_ERROR,
                    "incomplete frame after length check",
                )
                .into());
            }
            Err(H2Error::Stream { id, reason }) => {
                if h.kind == kind::HEADERS {
                    // HPACK block is skipped: the table would desync, so this is fatal.
                    return Err(
                        conn_err(Reason::PROTOCOL_ERROR, "HEADERS with self-dependency").into(),
                    );
                }
                if let Some(&k) = self.core.by_id.get(&id) {
                    self.core.stream_error(k, reason, "invalid frame");
                }
                // Unknown streams (idle or long closed) are ignored: an RST_STREAM for an idle stream is
                // itself a protocol violation (RFC 9113 5.1).
            }
            Err(e) => return Err(e.into()),
        }
        self.rd.advance(total);
        Ok(())
    }

    /// Drains queued commands without registering the inbox waker (see `arm_inbox`). Returns true if the
    /// budget was exhausted with commands left.
    fn inbox_phase(&mut self) -> bool {
        let mut n = 0;
        while n < CMD_BUDGET {
            if self.core.wq.pending() > INBOX_PAUSE_PENDING {
                return false;
            }
            match self.rx.try_recv() {
                Ok(c) => self.handle_cmd(c),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.inbox_closed = true;
                    return false;
                }
                Err(mpsc::error::TryRecvError::Empty) => return false,
            }
            n += 1;
        }
        true
    }

    fn handle_cmd(&mut self, c: Command) {
        match c {
            Command::Open(o) => {
                self.opened_this_tick += 1;
                self.core.open(o);
            }
            Command::Ready(s) => self.core.on_ready(&s),
        }
    }

    /// Registers the inbox waker (register, then check). A command that raced in is handled now; the caller
    /// re-runs the tick for it.
    fn arm_inbox(&mut self, cx: &mut Context<'_>) -> bool {
        if self.inbox_closed {
            return false;
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(c)) => {
                self.handle_cmd(c);
                true
            }
            Poll::Ready(None) => {
                self.inbox_closed = true;
                false
            }
            Poll::Pending => false,
        }
    }

    fn write_phase(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        let mut budget = WRITE_BYTES_BUDGET;
        let mut more = false;
        loop {
            match self.core.wq.poll_write(&mut self.io, cx) {
                Poll::Pending => {
                    self.core.marks_flushed();
                    return Ok(false);
                }
                Poll::Ready(Err(e)) => return Err(e),
                Poll::Ready(Ok(None)) => break,
                Poll::Ready(Ok(Some(w))) => {
                    if self.core.obs_on {
                        self.core
                            .obs
                            .write_batch(&self.core.origin, w.bytes, w.iovecs);
                    }
                    budget = budget.saturating_sub(w.bytes);
                    if budget == 0 && !self.core.wq.is_empty() {
                        more = true;
                        break;
                    }
                }
            }
        }
        if self.core.wq.is_empty()
            && let Poll::Ready(Err(e)) = self.core.wq.poll_flush(&mut self.io, cx)
        {
            return Err(e);
        }
        self.core.marks_flushed();
        Ok(more)
    }

    fn poll_keepalive(&mut self, cx: &mut Context<'_>) -> std::result::Result<(), Fail> {
        let Some(k) = self.keepalive.as_mut() else {
            return Ok(());
        };
        let interval = self.core.cfg.keepalive_interval.expect("keepalive on");
        let timeout = self.core.cfg.keepalive_timeout;
        if self.core.ping_acked {
            self.core.ping_acked = false;
            k.outstanding = false;
            k.sleep
                .as_mut()
                .reset(tokio::time::Instant::now() + interval);
        }
        while k.sleep.as_mut().poll(cx).is_ready() {
            let now = tokio::time::Instant::now();
            if k.outstanding {
                return Err(Fail::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "HTTP/2 keepalive ping timed out",
                )));
            }
            if now.duration_since(k.last_read) >= interval {
                self.core.ping_seq += 1;
                let data = self.core.ping_seq.to_be_bytes();
                self.core.ping_out = Some(data);
                self.core.ctrl(|a| frame::encode_ping(a, false, data));
                k.outstanding = true;
                k.sleep.as_mut().reset(now + timeout);
            } else {
                k.sleep.as_mut().reset(k.last_read + interval);
            }
        }
        Ok(())
    }

    /// Arms/polls the single deadline timer and runs the deadline scan when it fires.
    fn poll_deadlines(&mut self, cx: &mut Context<'_>) {
        loop {
            if self.core.timer_dirty {
                self.core.timer_dirty = false;
                if let Some(t) = self.core.timer_at {
                    self.timer.as_mut().reset(t);
                }
            }
            if self.core.timer_at.is_none() {
                return;
            }
            if self.timer.as_mut().poll(cx).is_pending() {
                return;
            }
            self.core.timer_at = None;
            if let Err(e) = self.core.scan_timeouts() {
                self.core.ctrl_goaway(Reason::ENHANCE_YOUR_CALM, "timeout");
                self.core.state = ConnState::Closing;
                self.core
                    .fail_all(ErrorKind::Timeout, &format!("HTTP/2 {e}"));
                return;
            }
        }
    }
}

impl<T> Actor<T> {
    /// Fail everything, stop accepting and wake waiters. Idempotent.
    fn teardown(&mut self, kind: ErrorKind, msg: &str) {
        self.core.fail_all(kind, msg);
        if self.torn_down {
            return;
        }
        self.torn_down = true;
        self.core.stop_accepting();
        self.rx.close();
        while let Ok(c) = self.rx.try_recv() {
            if let Command::Open(o) = c {
                o.shared
                    .publish_head(Err(Error::unprocessed(ErrorKind::Closed, msg.to_owned())));
                o.shared.close_send();
            }
        }
        self.core.shared.closed.store(true, Ordering::Release);
        self.core.shared.close_notify.notify_waiters();
        self.core.shared.slot_released.notify_waiters();
    }
}

impl<T> Drop for Actor<T> {
    fn drop(&mut self) {
        // Task dropped (runtime shutdown): unblock every waiter.
        if !self.torn_down {
            self.teardown(ErrorKind::Closed, "HTTP/2 connection task dropped");
            self.core.drain_dirty();
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Future for Actor<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let r = this.poll_tick(cx);
        // Every exit path (teardown, fail_connection, Closing, Pending) delivers the wakes it owes.
        this.core.drain_dirty();
        r
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Actor<T> {
    fn poll_tick(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let this = self;
        let t0 = Instant::now();
        this.core.now = TInstant::now();
        let mut again = false;

        if this.core.state == ConnState::Open {
            // 1. READ
            if !this.eof {
                match this.read_phase(cx) {
                    Ok(y) => again |= y,
                    Err(Fail::Proto(e)) => this.core.fail_connection(&e),
                    Err(Fail::Io(e)) => {
                        this.teardown(ErrorKind::Io, &format!("read error: {e}"));
                        return Poll::Ready(());
                    }
                }
            }
            if this.eof && this.core.state == ConnState::Open {
                this.teardown(ErrorKind::Io, "connection closed by peer");
                return Poll::Ready(());
            }
        }
        if this.core.state == ConnState::Open {
            // 2. INBOX
            again |= this.inbox_phase();
            this.poll_deadlines(cx);
            // keepalive
            if let Err(Fail::Io(e)) = this.poll_keepalive(cx) {
                this.teardown(ErrorKind::Io, &e.to_string());
                return Poll::Ready(());
            }
            // 3. SCHEDULE
            again |= this.core.schedule();
            // Graceful drain: every handle is gone and nothing is in flight.
            if this.inbox_closed && this.core.streams.is_empty() {
                this.core.stop_accepting();
                this.core.ctrl_goaway(Reason::NO_ERROR, "");
                this.core.state = ConnState::Closing;
            } else if this.core.goaway.is_some() && this.core.streams.is_empty() {
                this.core.state = ConnState::Closing;
            }
        }

        // 4. WRITE, with request coalescing (see the COALESCE_* constants): HEADERS of newly opened requests
        // are held back while the batch is small, for a bounded time, when many streams are in flight and
        // nothing bigger is queued. One `writev` then carries the whole batch and the server receives (and
        // answers) the requests together, which is what keeps every later read and wake batched too.
        let pending = this.core.wq.pending();
        let in_flight = this.core.shared.in_flight.load(Ordering::Relaxed);
        if this.opened_this_tick > 0 {
            if this.held == 0 {
                this.hold_since = t0;
            }
            this.held += this.opened_this_tick;
        }
        this.opened_this_tick = 0;
        let mut hold = this.core.cfg.coalesce_requests
            && this.core.state == ConnState::Open
            && this.held > 0
            && pending <= COALESCE_MAX_PENDING
            && in_flight > COALESCE_MIN_IN_FLIGHT
            && this.held < coalesce_batch(in_flight)
            && t0.saturating_duration_since(this.hold_since)
                < coalesce_hold(this.core.rtt_ewma_ns, this.core.cfg.coalesce_max_hold);
        if hold {
            if !this.hold_armed {
                this.hold_armed = true;
                this.hold_timer
                    .as_mut()
                    .reset(TInstant::now() + HOLD_TIMER_FALLBACK);
            }
            if this.hold_timer.as_mut().poll(cx).is_ready() {
                hold = false;
            }
        }
        if !hold {
            this.held = 0;
            this.hold_armed = false;
            match this.write_phase(cx) {
                Ok(y) => again |= y,
                Err(e) => {
                    this.teardown(ErrorKind::Io, &format!("write error: {e}"));
                    return Poll::Ready(());
                }
            }
        }
        again |= this.core.can_schedule() && this.core.state == ConnState::Open;
        // The inbox was drained without a waker; register it now (register, then check: a command that raced
        // in is handled and the tick re-runs).
        if this.core.state == ConnState::Open && this.arm_inbox(cx) {
            again = true;
        }

        let t1 = Instant::now();
        let inbox = this.rx.len();
        if this.core.obs_on {
            this.core
                .obs
                .actor_tick(&this.core.origin, t1.saturating_duration_since(t0), inbox);
        }
        if this.core.meter.record_tick(t0, t1, inbox) {
            let sh = &this.core.shared;
            let now_ms = u32::try_from(t1.saturating_duration_since(sh.epoch).as_millis())
                .unwrap_or(u32::MAX);
            this.core.meter.publish(&sh.load, now_ms);
        }

        if this.core.state == ConnState::Closing {
            // Best effort: one flush attempt, then leave regardless of a stuck peer.
            if this.core.wq.is_empty() {
                let _ = Pin::new(&mut this.io).poll_shutdown(cx);
            }
            this.teardown(ErrorKind::Closed, "HTTP/2 connection closed");
            return Poll::Ready(());
        }
        if again {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

// ---------------------------------------------------------------------------------------------------------
// Handshake.

fn hs_err(e: impl Into<BoxError>) -> Error {
    Error::unprocessed(ErrorKind::Io, e)
}

pub(crate) async fn handshake_io<T>(
    mut io: T,
    origin: OriginKey,
    cfg: H2Config,
    obs: std::sync::Arc<dyn Observer>,
) -> Result<H2Handle>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    cfg.validate()?;
    let mut local = Settings::client_default();
    local.initial_window_size = cfg.initial_stream_window;
    local.max_frame_size = cfg.max_frame_size;
    local.max_header_list_size = Some(cfg.max_header_list_size);

    let mut out = Vec::with_capacity(160);
    out.extend_from_slice(frame::CONNECTION_PREFACE);
    local.encode(&mut out);
    let conn_inc = cfg.initial_conn_window.saturating_sub(65_535);
    if conn_inc > 0 {
        frame::encode_window_update(&mut out, 0, conn_inc);
    }
    let deadline = cfg.handshake_timeout;
    let mut rd = Reader::new();
    let hs = async {
        io.write_all(&out).await.map_err(hs_err)?;
        io.flush().await.map_err(hs_err)?;
        // Peek the peer's first SETTINGS (left in the buffer; the actor processes it normally).
        let mut peer = Settings::default();
        loop {
            if let Some(h) = rd.header() {
                if h.kind != kind::SETTINGS || h.flags & flags::ACK != 0 {
                    return Err(Error::unprocessed(
                        ErrorKind::Protocol,
                        "peer's first frame was not SETTINGS",
                    ));
                }
                if h.len > local.max_frame_size {
                    return Err(Error::unprocessed(
                        ErrorKind::Protocol,
                        "oversized SETTINGS",
                    ));
                }
                let total = 9 + h.len as usize;
                if rd.cur.len() >= total {
                    let scratch = rd.contiguous(total);
                    let bytes: &[u8] = if scratch {
                        &rd.scratch[..total]
                    } else {
                        &rd.cur.first_slice()[..total]
                    };
                    return match frame::decode_frame(bytes, local.max_frame_size) {
                        Ok(Some((Frame::Settings { params, .. }, _))) => {
                            peer.apply_all(params)
                                .map_err(|e| Error::unprocessed(ErrorKind::Protocol, e))?;
                            Ok(peer)
                        }
                        Ok(_) => Err(Error::unprocessed(ErrorKind::Protocol, "bad SETTINGS")),
                        Err(e) => Err(Error::unprocessed(ErrorKind::Protocol, e)),
                    };
                }
            }
            let n = std::future::poll_fn(|cx| rd.poll_fill(&mut io, cx))
                .await
                .map_err(hs_err)?;
            if n == 0 {
                return Err(Error::unprocessed(
                    ErrorKind::Closed,
                    "connection closed during HTTP/2 handshake",
                ));
            }
        }
    };
    let peer = match tokio::time::timeout(deadline, hs).await {
        Ok(r) => r?,
        Err(_) => return Err(Error::timeout().with_recovery(Recovery::Unprocessed)),
    };

    let max_streams = peer
        .max_concurrent_streams
        .map_or(usize::MAX, |v| v as usize);
    let conn_id = NEXT_CONN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let shared = Arc::new(ConnShared::new(
        origin.clone(),
        conn_id,
        max_streams,
        ((cfg.initial_stream_window as f32 * WINDOW_UPDATE_RATIO) as u32).max(1),
        obs.enabled(),
    ));
    let (tx, rx) = mpsc::unbounded_channel();
    let keepalive = cfg.keepalive_interval.map(|i| Keepalive {
        sleep: Box::pin(tokio::time::sleep(i)),
        last_read: tokio::time::Instant::now(),
        outstanding: false,
    });
    let core = Core::new(origin, obs, Arc::clone(&shared), cfg, local);
    let actor = Actor {
        io,
        rd,
        core,
        rx,
        eof: false,
        inbox_closed: false,
        keepalive,
        opened_this_tick: 0,
        held: 0,
        hold_since: Instant::now(),
        hold_timer: Box::pin(tokio::time::sleep(Duration::from_secs(86_400))),
        hold_armed: false,
        timer: Box::pin(tokio::time::sleep(Duration::from_secs(86_400))),
        torn_down: false,
    };
    // The actor bounds its own per-tick work (frames, bytes, commands). tokio's cooperative budget would
    // only make a busy actor read `Pending` from its inbox/socket as "empty"/"not ready" and re-wake.
    drop(tokio::spawn(tokio::task::unconstrained(actor)));
    Ok(H2Handle::new(shared, tx))
}

/// Request-coalescing batch target for the current load (see `COALESCE_BATCH_DIV`).
fn coalesce_batch(in_flight: usize) -> u32 {
    u32::try_from(in_flight / COALESCE_BATCH_DIV)
        .unwrap_or(u32::MAX)
        .clamp(2, COALESCE_MAX_BATCH)
}

/// Request-coalescing hold bound: a small fraction of the connection's current round trip.
fn coalesce_hold(rtt_ewma_ns: u64, max: Duration) -> Duration {
    Duration::from_nanos(rtt_ewma_ns / u64::from(COALESCE_RTT_DIV))
        .clamp(COALESCE_MIN_HOLD, max.max(COALESCE_MIN_HOLD))
}

//! HTTP/2 client connection actor.
//!
//! One task per connection owns the socket, HPACK state, stream table, flow-control windows and the write
//! queue. Callers talk to it through an unbounded inbox and per-stream atomics; there is no connection-wide
//! lock (hyperium/h2#531 is fixed by construction). See `docs/design/h2-actor.md`.

mod actor;
mod load;
#[cfg(all(test, matier_loom))]
mod loom_tests;
mod shared;
mod sync;
#[cfg(all(test, not(matier_loom)))]
mod tests;
mod write;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use matier_core::{Body, BytesView, Error, ErrorKind, Observer, OriginKey, Result};

pub use self::load::LoadSnapshot;
use self::shared::{
    CmdTx, Command, ConnShared, OpenBody, OpenStream, RecvEvent, SendEvent, StreamShared,
};
use self::sync::Arc;
use crate::{Incoming, Io};

/// Bytes of request body a streaming upload may have buffered in the actor per stream.
const SEND_BUFFER: i64 = 256 * 1024;

/// HTTP/2 connection configuration.
#[derive(Clone, Debug)]
pub struct H2Config {
    /// SETTINGS_INITIAL_WINDOW_SIZE we advertise (per-stream receive window).
    pub initial_stream_window: u32,
    /// Connection receive window we open with a WINDOW_UPDATE at the start (also the buffered-bytes budget).
    pub initial_conn_window: u32,
    /// SETTINGS_MAX_FRAME_SIZE we advertise (16 KiB ..= 16 MiB - 1).
    pub max_frame_size: u32,
    /// SETTINGS_MAX_HEADER_LIST_SIZE we advertise; also the HPACK decoder cap.
    pub max_header_list_size: u32,
    /// Send a PING after this much read inactivity. `None` disables keepalive.
    pub keepalive_interval: Option<Duration>,
    /// Close the connection when a keepalive PING is not acknowledged within this long.
    pub keepalive_timeout: Duration,
    /// Frames processed per tick before yielding.
    pub read_budget_frames: usize,
    /// Deadline for the preface/SETTINGS exchange.
    pub handshake_timeout: Duration,
    /// Attach `ConnectionInfo` to responses (costs a few allocations per response; the pool may do it instead).
    pub connection_info: bool,
}

impl Default for H2Config {
    fn default() -> Self {
        Self {
            initial_stream_window: 1 << 20,
            initial_conn_window: 4 << 20,
            max_frame_size: 16 * 1024,
            max_header_list_size: 64 * 1024,
            keepalive_interval: None,
            keepalive_timeout: Duration::from_secs(20),
            read_budget_frames: 64,
            handshake_timeout: Duration::from_secs(10),
            connection_info: true,
        }
    }
}

/// Performs the preface/SETTINGS exchange, then spawns the connection actor (`tokio::spawn`).
///
/// # Errors
/// I/O failure, timeout or a protocol violation during the handshake; all are `Unprocessed`.
pub async fn handshake(
    io: Io,
    origin: OriginKey,
    cfg: H2Config,
    obs: std::sync::Arc<dyn Observer>,
) -> Result<H2Handle> {
    actor::handshake_io(io, origin, cfg, obs).await
}

/// Cheap-to-clone handle to a connection actor. All state the pool needs is atomic.
#[derive(Clone)]
pub struct H2Handle {
    shared: Arc<ConnShared>,
    tx: CmdTx,
}

impl fmt::Debug for H2Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("H2Handle")
            .field("conn_id", &self.shared.conn_id)
            .field("in_flight", &self.in_flight())
            .finish_non_exhaustive()
    }
}

/// A reserved stream slot; released on drop (or when the stream it was used for ends).
#[must_use = "a slot holds one of the connection's concurrent-stream permits"]
pub struct StreamSlot {
    conn: Arc<ConnShared>,
}

impl fmt::Debug for StreamSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StreamSlot")
    }
}

impl Drop for StreamSlot {
    fn drop(&mut self) {
        self.conn.release_slot();
    }
}

impl H2Handle {
    pub(crate) fn new(shared: Arc<ConnShared>, tx: CmdTx) -> Self {
        Self { shared, tx }
    }

    /// Streams reserved or open.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.shared.in_flight.load(sync::Ordering::Acquire)
    }

    /// Peer SETTINGS_MAX_CONCURRENT_STREAMS (`usize::MAX` if the peer set none).
    #[must_use]
    pub fn max_concurrent_streams(&self) -> usize {
        self.shared.max_streams.load(sync::Ordering::Acquire)
    }

    /// False after GOAWAY, a connection error or close.
    #[must_use]
    pub fn is_accepting(&self) -> bool {
        self.shared.accepting.load(sync::Ordering::Acquire)
    }

    /// The actor's published load signals (busy EWMA, dispatch-delay EWMA, inbox depth). Values older than
    /// a few windows (the actor stopped ticking, i.e. the connection is idle) read as zero. One relaxed
    /// load of the cache line unless something non-zero was published, in which case one clock read.
    #[must_use]
    pub fn load(&self) -> LoadSnapshot {
        let raw = self.shared.load.raw();
        if raw == LoadSnapshot::default() {
            return raw;
        }
        if self.shared.load.is_stale(self.now_ms()) {
            return LoadSnapshot::default();
        }
        raw
    }

    /// Like [`Self::load`] without the staleness check (no clock read): cheap, but may be old if the actor
    /// went idle.
    #[must_use]
    pub fn load_unchecked(&self) -> LoadSnapshot {
        self.shared.load.raw()
    }

    fn now_ms(&self) -> u32 {
        u32::try_from(self.shared.epoch.elapsed().as_millis()).unwrap_or(u32::MAX)
    }

    /// Number of streams this connection's actor has opened so far (wraps).
    #[must_use]
    pub fn streams_opened(&self) -> u32 {
        self.shared.streams_opened.load(sync::Ordering::Relaxed)
    }

    /// Reserve a stream slot (CAS on `in_flight < max_concurrent_streams && accepting`).
    #[must_use]
    pub fn try_reserve(&self) -> Option<StreamSlot> {
        self.shared.try_reserve().then(|| StreamSlot {
            conn: Arc::clone(&self.shared),
        })
    }

    /// Resolves when a slot is released or the peer raises MAX_CONCURRENT_STREAMS. Pool waiters should create
    /// (and `enable()`) this *before* calling [`Self::try_reserve`] to avoid a lost wakeup.
    pub fn slot_released(&self) -> tokio::sync::futures::Notified<'_> {
        self.shared.slot_released.notified()
    }

    /// Connection id (unique per process).
    #[must_use]
    pub fn conn_id(&self) -> u64 {
        self.shared.conn_id
    }

    /// The origin this connection serves.
    #[must_use]
    pub fn origin(&self) -> &OriginKey {
        &self.shared.origin
    }

    /// Hands the request to the actor immediately (the request is moved into the inbox command, so the
    /// returned future is small) and returns a future for the response head.
    ///
    /// Dropping the future before completion cancels the request (RST_STREAM(CANCEL) if it was sent).
    pub fn send(&self, slot: StreamSlot, req: http::Request<Body>) -> ResponseFuture {
        let (parts, body) = req.into_parts();
        let streaming = matches!(body, Body::Stream(_));
        let threshold = self.shared.stream_threshold;
        let shared = Arc::new(StreamShared::new(
            self.tx.clone(),
            threshold,
            if streaming { SEND_BUFFER } else { 0 },
        ));
        let (open_body, pump_body) = match body {
            Body::Empty => (OpenBody::Empty, None),
            Body::Full(v) => (OpenBody::Full(v), None),
            b @ Body::Stream(_) => (OpenBody::Streaming, Some(b)),
        };
        let cmd = Command::Open(OpenStream {
            shared: Arc::clone(&shared),
            parts,
            body: open_body,
            enqueued_at: (self.shared.obs_on || load::sample_now()).then(Instant::now),
            slot,
        });
        if self.tx.send(cmd).is_err() {
            return ResponseFuture {
                shared: None,
                err: Some(Box::new(Error::unprocessed(
                    ErrorKind::Closed,
                    "HTTP/2 connection closed",
                ))),
            };
        }
        if let Some(b) = pump_body {
            tokio::spawn(Pump {
                shared: Arc::clone(&shared),
                body: b,
                state: PumpState::NeedCredit,
                finished: false,
            });
        }
        ResponseFuture {
            shared: Some(shared),
            err: None,
        }
    }

    /// Resolves when the actor has exited.
    pub fn closed(&self) -> impl Future<Output = ()> + Send + 'static {
        let s = Arc::clone(&self.shared);
        async move {
            loop {
                let n = s.close_notify.notified();
                tokio::pin!(n);
                n.as_mut().enable();
                if s.closed.load(sync::Ordering::Acquire) {
                    return;
                }
                n.await;
            }
        }
    }
}

/// Future for the response head of one request. Small by design (two pointers).
#[must_use = "futures do nothing unless polled; dropping cancels the request"]
pub struct ResponseFuture {
    shared: Option<Arc<StreamShared>>,
    err: Option<Box<Error>>,
}

impl Future for ResponseFuture {
    type Output = Result<http::Response<Incoming>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(e) = self.err.take() {
            return Poll::Ready(Err(*e));
        }
        let Some(sh) = self.shared.as_ref() else {
            return Poll::Ready(Err(Error::new(
                ErrorKind::Closed,
                matier_core::Recovery::Never,
            )));
        };
        match sh.poll_head(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => {
                self.shared = None;
                Poll::Ready(Err(e))
            }
            Poll::Ready(Ok(parts)) => {
                let shared = self.shared.take().expect("checked above");
                Poll::Ready(Ok(http::Response::from_parts(
                    parts,
                    Incoming::H2(RecvStream {
                        shared,
                        done: false,
                    }),
                )))
            }
        }
    }
}

impl Drop for ResponseFuture {
    fn drop(&mut self) {
        if let Some(sh) = self.shared.take() {
            StreamShared::cancel(&sh);
        }
    }
}

/// Response body of one HTTP/2 stream.
pub struct RecvStream {
    shared: Arc<StreamShared>,
    done: bool,
}

impl fmt::Debug for RecvStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecvStream")
            .field("done", &self.done)
            .finish()
    }
}

impl http_body::Body for RecvStream {
    type Data = BytesView;
    type Error = Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<http_body::Frame<BytesView>, Error>>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.shared.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(RecvEvent::Data(d)) => {
                StreamShared::consumed(&self.shared, u32::try_from(d.len()).unwrap_or(u32::MAX));
                Poll::Ready(Some(Ok(http_body::Frame::data(*d))))
            }
            Poll::Ready(RecvEvent::Trailers(t)) => {
                Poll::Ready(Some(Ok(http_body::Frame::trailers(*t))))
            }
            Poll::Ready(RecvEvent::End) => {
                self.done = true;
                StreamShared::flush_consumed(&self.shared);
                Poll::Ready(None)
            }
            Poll::Ready(RecvEvent::Error(e)) => {
                self.done = true;
                StreamShared::flush_consumed(&self.shared);
                Poll::Ready(Some(Err(*e)))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }
}

impl Drop for RecvStream {
    fn drop(&mut self) {
        if !self.done {
            StreamShared::cancel(&self.shared);
        }
    }
}

enum PumpState {
    NeedCredit,
    Reading,
}

/// Request-body pump for streaming uploads: a small manual future, one per such request.
struct Pump {
    shared: Arc<StreamShared>,
    body: Body,
    state: PumpState,
    finished: bool,
}

impl Future for Pump {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        loop {
            match this.state {
                PumpState::NeedCredit => match this.shared.poll_credit(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(false) => {
                        this.finished = true;
                        return Poll::Ready(());
                    }
                    Poll::Ready(true) => this.state = PumpState::Reading,
                },
                PumpState::Reading => {
                    match http_body::Body::poll_frame(Pin::new(&mut this.body), cx) {
                        Poll::Pending => {
                            // Also wake if the actor closes the stream while the producer is idle.
                            if !this.shared.poll_credit_or_closed(cx) {
                                this.finished = true;
                                return Poll::Ready(());
                            }
                            return Poll::Pending;
                        }
                        Poll::Ready(None) => {
                            this.finished = true;
                            StreamShared::push_send(&this.shared, SendEvent::End);
                            return Poll::Ready(());
                        }
                        Poll::Ready(Some(Err(e))) => {
                            this.finished = true;
                            StreamShared::push_send(&this.shared, SendEvent::Abort(Box::new(e)));
                            return Poll::Ready(());
                        }
                        Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                            Ok(d) => {
                                if d.is_empty() {
                                    continue;
                                }
                                this.shared.take_credit(d.len());
                                StreamShared::push_send(&this.shared, SendEvent::Data(Box::new(d)));
                                this.state = PumpState::NeedCredit;
                            }
                            Err(frame) => {
                                if let Ok(t) = frame.into_trailers() {
                                    StreamShared::push_send(
                                        &this.shared,
                                        SendEvent::Trailers(Box::new(t)),
                                    );
                                }
                            }
                        },
                    }
                }
            }
        }
    }
}

impl Drop for Pump {
    fn drop(&mut self) {
        if !self.finished {
            // Task dropped (runtime shutdown) mid-body: never let it look like a complete body.
            StreamShared::push_send(&self.shared, SendEvent::Abort(Box::new(Error::canceled())));
        }
    }
}

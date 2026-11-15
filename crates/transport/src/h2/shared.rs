//! Cross-task shared state: per-connection atomics (`ConnShared`) and per-stream state (`StreamShared`).
//!
//! Every protocol here is "register waker, then check state" on the consumer side and "set state, then
//! wake" on the producer side. They are covered by loom models in `loom_tests.rs`.
//! The only mutex is the one-shot head slot: written once by the actor, taken once by the consumer,
//! never held across an await.

use std::task::{Context, Poll};
use std::time::Instant;

use super::load::LoadCell;

use http::HeaderMap;
use matier_core::{BytesView, Error, OriginKey};
use tokio::sync::Notify;

use super::StreamSlot;
use super::ring::RecvQueue;
use super::sync::{
    Arc, AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, AtomicWaker, EventQueue, Mutex,
    Ordering, lock,
};

/// A `Ready` command is already queued for this stream (dedup flag).
pub(crate) const QUEUED: u32 = 1;
/// The consumer dropped the response future / body before completion.
pub(crate) const CANCELED: u32 = 2;
/// The head slot holds a value.
pub(crate) const HEAD_READY: u32 = 4;
/// The actor will take no more request-body data (stream reset or finished).
pub(crate) const SEND_CLOSED: u32 = 8;
/// The actor has dropped this stream from its table: no more stream-level window credit is needed.
pub(crate) const RECV_DONE: u32 = 16;

/// Fixed per-event cost charged against the connection's buffered-bytes budget on top of the DATA frame's
/// flow-controlled length (event slot, queue and bookkeeping memory; also covers pinned read blocks).
pub(crate) const EVENT_OVERHEAD: u64 = 64;
/// Bit in `ConnShared::consumed` set by the actor while it holds connection-window credit behind the
/// buffered budget (so a consumer's `fetch_add` learns, from the previous value, that it must send a `Ready`).
/// One word, so the handshake is decided by the modification order and needs no fences.
pub(crate) const CONSUMED_BLOCKED: u64 = 1 << 63;
/// Cap on unconsumed receive events per stream; beyond it the stream is reset with ENHANCE_YOUR_CALM.
pub(crate) const MAX_RECV_EVENTS: usize = 1024;

/// Streams with undelivered wakeups, drained by the actor once per phase (wake batching).
pub(crate) type Dirty = Vec<Arc<StreamShared>>;

/// Request body disposition handed to the actor.
#[allow(
    clippy::large_enum_variant,
    reason = "BytesView inlines 8 spans; moved once into the inbox"
)]
pub(crate) enum OpenBody {
    Empty,
    Full(BytesView),
    Streaming,
}

pub(crate) struct OpenStream {
    pub(crate) shared: Arc<StreamShared>,
    pub(crate) parts: http::request::Parts,
    pub(crate) body: OpenBody,
    pub(crate) enqueued_at: Option<Instant>,
    /// Released when the stream is removed (or when this command is dropped unprocessed).
    pub(crate) slot: StreamSlot,
}

/// The actor's single inbox message type.
#[allow(
    clippy::large_enum_variant,
    reason = "BytesView inlines 8 spans; boxing costs an allocation per request"
)]
pub(crate) enum Command {
    Open(OpenStream),
    /// "This stream has work": consumption credit, cancel, or request-body data. Deduped by `QUEUED`.
    Ready(Arc<StreamShared>),
}

pub(crate) type CmdTx = tokio::sync::mpsc::UnboundedSender<Command>;

#[allow(
    clippy::large_enum_variant,
    reason = "lives inline in the stream's ring; boxing would cost an allocation per DATA chunk"
)]
pub(crate) enum RecvEvent {
    /// A DATA payload. `flow` is the frame's flow-controlled length (payload + padding); `end` folds
    /// END_STREAM into the event (one push, one pop).
    Data {
        view: BytesView,
        flow: u32,
        end: bool,
    },
    Trailers(Box<HeaderMap>),
    End,
    Error(Box<Error>),
}

pub(crate) enum SendEvent {
    Data(Box<BytesView>),
    Trailers(Box<HeaderMap>),
    End,
    Abort(Box<Error>),
}

pub(crate) struct StreamShared {
    flags: AtomicU32,
    /// Stream id, set by the actor when the stream is opened (0 before).
    id: AtomicU32,
    head: Mutex<Option<Result<http::response::Parts, Error>>>,
    head_waker: AtomicWaker,
    recv: RecvQueue<RecvEvent>,
    recv_waker: AtomicWaker,
    /// Actor-only: this stream is already on the actor's `dirty` list (a wake is owed).
    wake_pending: AtomicBool,
    pub(crate) conn: Arc<ConnShared>,
    /// Bytes the consumer has consumed but the actor has not yet credited back to the stream window.
    recv_consumed: AtomicU32,
    /// Send a `Ready` once `recv_consumed` reaches this.
    threshold: u32,
    send: EventQueue<SendEvent>,
    send_credit: AtomicI64,
    send_waker: AtomicWaker,
    tx: CmdTx,
}

impl StreamShared {
    pub(crate) fn new(tx: CmdTx, conn: Arc<ConnShared>, threshold: u32, send_credit: i64) -> Self {
        Self {
            flags: AtomicU32::new(0),
            id: AtomicU32::new(0),
            head: Mutex::new(None),
            head_waker: AtomicWaker::new(),
            recv: RecvQueue::new(),
            recv_waker: AtomicWaker::new(),
            wake_pending: AtomicBool::new(false),
            conn,
            recv_consumed: AtomicU32::new(0),
            threshold: threshold.max(1),
            send: EventQueue::new(),
            send_credit: AtomicI64::new(send_credit),
            send_waker: AtomicWaker::new(),
            tx,
        }
    }

    pub(crate) fn flags(&self) -> u32 {
        self.flags.load(Ordering::Acquire)
    }

    pub(crate) fn id(&self) -> u32 {
        self.id.load(Ordering::Relaxed)
    }

    pub(crate) fn set_id(&self, id: u32) {
        self.id.store(id, Ordering::Relaxed);
    }

    // ---- "mark ready" (any task -> actor) ------------------------------------------------------

    /// State must already be published. Sends one `Ready` unless one is already queued: the actor clears
    /// `QUEUED` *before* reading state, so a producer that observes `QUEUED` set is guaranteed to be seen.
    pub(crate) fn mark_ready(this: &Arc<Self>) {
        if this.flags.fetch_or(QUEUED, Ordering::AcqRel) & QUEUED == 0 {
            // An error means the actor is gone: nothing to tell. (QUEUED stays set; nobody will read it.)
            let _ = this.tx.send(Command::Ready(Arc::clone(this)));
        }
    }

    /// Actor: clear `QUEUED` and return the flags as of now. Call before reading any other state.
    pub(crate) fn begin_ready(&self) -> u32 {
        self.flags.fetch_and(!QUEUED, Ordering::AcqRel) & !QUEUED
    }

    // ---- head slot ---------------------------------------------------------------------------

    /// Actor: publish the response head (or terminal error), then wake the consumer immediately.
    pub(crate) fn publish_head(&self, r: Result<http::response::Parts, Error>) {
        self.set_head(r);
        self.head_waker.wake();
    }

    fn set_head(&self, r: Result<http::response::Parts, Error>) {
        *lock(&self.head) = Some(r);
        self.flags.fetch_or(HEAD_READY, Ordering::Release);
    }

    /// Actor: publish the head and owe the consumer a wake, delivered by [`Self::wake_now`] when the actor
    /// drains `dirty` (set state, then wake).
    pub(crate) fn publish_head_deferred(
        this: &Arc<Self>,
        r: Result<http::response::Parts, Error>,
        dirty: &mut Dirty,
    ) {
        this.set_head(r);
        Self::owe_wake(this, dirty);
    }

    /// Actor: schedule one wake for this stream (deduplicated until the next drain).
    fn owe_wake(this: &Arc<Self>, dirty: &mut Dirty) {
        // Only the actor touches `wake_pending`; plain load/store suffice.
        if !this.wake_pending.load(Ordering::Relaxed) {
            this.wake_pending.store(true, Ordering::Relaxed);
            dirty.push(Arc::clone(this));
        }
    }

    /// Actor: deliver the wake owed by `owe_wake`. The flag is cleared *before* waking so that a push that
    /// follows re-registers the stream; every push happens-before the wake that covers it.
    pub(crate) fn wake_now(&self) {
        self.wake_pending.store(false, Ordering::Relaxed);
        self.head_waker.wake();
        self.recv_waker.wake();
    }

    pub(crate) fn head_published(&self) -> bool {
        self.flags() & HEAD_READY != 0
    }

    /// Consumer: register the waker first, then check the slot.
    pub(crate) fn poll_head(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<http::response::Parts, Error>> {
        if let Some(r) = self.try_take_head() {
            return Poll::Ready(r);
        }
        self.head_waker.register(cx.waker());
        match self.try_take_head() {
            Some(r) => Poll::Ready(r),
            None => Poll::Pending,
        }
    }

    fn try_take_head(&self) -> Option<Result<http::response::Parts, Error>> {
        if self.flags.load(Ordering::Acquire) & HEAD_READY == 0 {
            return None;
        }
        lock(&self.head).take()
    }

    // ---- receive side --------------------------------------------------------------------------

    /// Actor: queue an event and owe the consumer a wake (see [`Self::wake_now`]).
    pub(crate) fn push_recv(this: &Arc<Self>, ev: RecvEvent, dirty: &mut Dirty) {
        // SAFETY: the actor is the only producer.
        unsafe { this.recv.push(ev) };
        Self::owe_wake(this, dirty);
    }

    /// Unconsumed receive events (approximate while the consumer is running).
    pub(crate) fn pending_events(&self) -> usize {
        self.recv.len()
    }

    fn pop(&self) -> Option<RecvEvent> {
        // SAFETY: single consumer at a time. The body reader is the consumer; on cancel it drains and then
        // hands over to the actor through the command channel (which orders the two), after which only the
        // actor pops.
        unsafe { self.recv.pop() }
    }

    /// Consumer: pop the next event, registering the waker before the final check.
    pub(crate) fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<RecvEvent> {
        if let Some(ev) = self.pop() {
            return Poll::Ready(ev);
        }
        self.recv_waker.register(cx.waker());
        match self.pop() {
            Some(ev) => Poll::Ready(ev),
            None => Poll::Pending,
        }
    }

    /// Drops every queued event, accounting DATA as consumed (it still counts against the connection's
    /// buffered-bytes budget). Caller must be the sole consumer.
    pub(crate) fn drain_recv(&self) {
        while let Some(ev) = self.pop() {
            if let RecvEvent::Data { flow, .. } = ev {
                self.account_consumed(flow);
            }
        }
    }

    /// Accounts a drained/consumed DATA event to the connection's buffered-bytes counter (no command).
    /// Returns whether the actor had published `CONSUMED_BLOCKED`.
    pub(crate) fn account_consumed(&self, flow: u32) -> bool {
        let prev = self
            .conn
            .consumed
            .fetch_add(u64::from(flow) + EVENT_OVERHEAD, Ordering::AcqRel);
        prev & CONSUMED_BLOCKED != 0
    }

    /// Consumer: a DATA event with flow-controlled length `flow` was handed to the application.
    ///
    /// Connection-level accounting is two atomics and no command. A `Ready` is sent only when this stream is
    /// still open and its stream-window credit crossed the threshold, or when the actor has parked
    /// connection-window credit behind the buffered budget (`recv_blocked`).
    pub(crate) fn consumed(this: &Arc<Self>, flow: u32) {
        // The actor sets `CONSUMED_BLOCKED` with an RMW on the same word and then re-reads the counter from
        // that RMW's result: in the word's modification order either our add precedes it (the actor sees the
        // consumption) or follows it (we see the bit here).
        let mut wake = this.account_consumed(flow);
        if flow > 0 && this.flags.load(Ordering::Acquire) & RECV_DONE == 0 {
            let prev = this.recv_consumed.fetch_add(flow, Ordering::AcqRel);
            wake |= prev.saturating_add(flow) >= this.threshold;
        }
        if wake {
            Self::mark_ready(this);
        }
    }

    /// Actor: take the accumulated consumption.
    pub(crate) fn take_consumed(&self) -> u32 {
        self.recv_consumed.swap(0, Ordering::AcqRel)
    }

    /// Consumer dropped before completion: report everything still queued as consumed and tell the actor.
    pub(crate) fn cancel(this: &Arc<Self>) {
        this.drain_recv();
        this.flags.fetch_or(CANCELED, Ordering::AcqRel);
        Self::mark_ready(this);
    }

    // ---- request body pump -----------------------------------------------------------------------

    /// Pump: wait until the actor allows more request-body bytes. `Ready(false)` = stream closed.
    pub(crate) fn poll_credit(&self, cx: &mut Context<'_>) -> Poll<bool> {
        if let Some(r) = self.credit_state() {
            return Poll::Ready(r);
        }
        self.send_waker.register(cx.waker());
        match self.credit_state() {
            Some(r) => Poll::Ready(r),
            None => Poll::Pending,
        }
    }

    /// Pump idle on the producer: register for a close notification. Returns false if already closed.
    pub(crate) fn poll_credit_or_closed(&self, cx: &mut Context<'_>) -> bool {
        self.send_waker.register(cx.waker());
        self.flags() & (SEND_CLOSED | CANCELED) == 0
    }

    fn credit_state(&self) -> Option<bool> {
        if self.flags() & (SEND_CLOSED | CANCELED) != 0 {
            return Some(false);
        }
        (self.send_credit.load(Ordering::Acquire) > 0).then_some(true)
    }

    pub(crate) fn take_credit(&self, n: usize) {
        self.send_credit
            .fetch_sub(i64::try_from(n).unwrap_or(i64::MAX), Ordering::AcqRel);
    }

    /// Pump: queue a request-body event and tell the actor.
    pub(crate) fn push_send(this: &Arc<Self>, ev: SendEvent) {
        this.send.push(ev);
        Self::mark_ready(this);
    }

    pub(crate) fn pop_send(&self) -> Option<SendEvent> {
        self.send.pop()
    }

    /// Actor: `n` request-body bytes were framed; let the pump produce more.
    pub(crate) fn add_credit(&self, n: usize) {
        self.send_credit
            .fetch_add(i64::try_from(n).unwrap_or(i64::MAX), Ordering::AcqRel);
        self.send_waker.wake();
    }

    /// Actor: no more request-body data will be accepted and the stream has left the actor's table.
    pub(crate) fn close_send(&self) {
        self.flags
            .fetch_or(SEND_CLOSED | RECV_DONE, Ordering::AcqRel);
        self.send_waker.wake();
    }

    /// Actor: terminate the stream towards the consumer with `err`.
    pub(crate) fn fail(this: &Arc<Self>, err: Error, dirty: &mut Dirty) {
        if this.head_published() {
            Self::push_recv(this, RecvEvent::Error(Box::new(err)), dirty);
        } else {
            Self::publish_head_deferred(this, Err(err), dirty);
        }
        this.close_send();
    }
}

impl Drop for StreamShared {
    fn drop(&mut self) {
        self.drain_recv();
    }
}

/// Pads (and aligns) a hot atomic to its own pair of cache lines so unrelated writers do not false-share.
#[repr(align(128))]
pub(crate) struct Aligned<T>(pub(crate) T);

impl<T> std::ops::Deref for Aligned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

/// Per-connection state read by the pool without any lock. Field groups by writer: read-mostly first, then
/// the request-path counter (any task), then the consumer-written byte counter, then actor-written state.
pub(crate) struct ConnShared {
    // ---- read-mostly -------------------------------------------------------------------------
    pub(crate) max_streams: AtomicUsize,
    pub(crate) accepting: AtomicBool,
    pub(crate) closed: AtomicBool,
    pub(crate) close_notify: Notify,
    pub(crate) origin: OriginKey,
    pub(crate) conn_id: u64,
    /// Per-stream consumption (bytes) at which the consumer asks the actor for a WINDOW_UPDATE.
    pub(crate) stream_threshold: u32,
    pub(crate) obs_on: bool,
    /// Reference point for `load`'s millisecond stamp.
    pub(crate) epoch: Instant,
    // ---- written by every request task (reserve / release) -----------------------------------
    pub(crate) in_flight: Aligned<AtomicUsize>,
    pub(crate) slot_released: Notify,
    // ---- written by body consumers -----------------------------------------------------------
    /// Total flow-controlled bytes (plus per-event overhead) the application has consumed or dropped, with
    /// `CONSUMED_BLOCKED` in the top bit. The actor keeps the matching `delivered` total;
    /// `buffered = delivered - (consumed & !CONSUMED_BLOCKED)`.
    pub(crate) consumed: Aligned<AtomicU64>,
    // ---- written by the actor ----------------------------------------------------------------
    /// Autoscaling signals published by the actor (own cache line).
    pub(crate) load: LoadCell,
    /// Streams the actor has opened so far (actor-written; the pool uses it to detect idleness).
    pub(crate) streams_opened: Aligned<AtomicU32>,
}

impl ConnShared {
    pub(crate) fn new(
        origin: OriginKey,
        conn_id: u64,
        max_streams: usize,
        stream_threshold: u32,
        obs_on: bool,
    ) -> Self {
        Self {
            in_flight: Aligned(AtomicUsize::new(0)),
            max_streams: AtomicUsize::new(max_streams),
            accepting: AtomicBool::new(true),
            closed: AtomicBool::new(false),
            slot_released: Notify::new(),
            close_notify: Notify::new(),
            origin,
            conn_id,
            stream_threshold,
            obs_on,
            load: LoadCell::new(),
            epoch: Instant::now(),
            streams_opened: Aligned(AtomicU32::new(0)),
            consumed: Aligned(AtomicU64::new(0)),
        }
    }

    pub(crate) fn try_reserve(&self) -> bool {
        if !self.accepting.load(Ordering::Acquire) {
            return false;
        }
        let max = self.max_streams.load(Ordering::Acquire);
        let mut cur = self.in_flight.load(Ordering::Relaxed);
        loop {
            if cur >= max {
                return false;
            }
            match self.in_flight.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
        // Re-check after the CAS so a concurrent close can not leave a live reservation unnoticed.
        if !self.accepting.load(Ordering::Acquire) {
            self.release_slot();
            return false;
        }
        true
    }

    pub(crate) fn release_slot(&self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.slot_released.notify_one();
    }
}

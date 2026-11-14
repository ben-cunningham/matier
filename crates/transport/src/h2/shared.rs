//! Cross-task shared state: per-connection atomics (`ConnShared`) and per-stream state (`StreamShared`).
//!
//! Every protocol here is "register waker, then check state" on the consumer side and "set state, then
//! wake" on the producer side. They are covered by loom models in `loom_tests.rs`.
//! The only mutex is the one-shot head slot: written once by the actor, taken once by the consumer,
//! never held across an await.

use std::task::{Context, Poll};
use std::time::Instant;

use http::HeaderMap;
use matier_core::{BytesView, Error, OriginKey};
use tokio::sync::Notify;

use super::StreamSlot;
use super::sync::{
    Arc, AtomicBool, AtomicI64, AtomicU32, AtomicUsize, AtomicWaker, EventQueue, Mutex, Ordering,
    lock,
};

/// A `Ready` command is already queued for this stream (dedup flag).
pub(crate) const QUEUED: u32 = 1;
/// The consumer dropped the response future / body before completion.
pub(crate) const CANCELED: u32 = 2;
/// The head slot holds a value.
pub(crate) const HEAD_READY: u32 = 4;
/// The actor will take no more request-body data (stream reset or finished).
pub(crate) const SEND_CLOSED: u32 = 8;

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

pub(crate) fn send_cmd(tx: &CmdTx, c: Command) -> bool {
    tx.send(c).is_ok()
}

pub(crate) enum RecvEvent {
    Data(Box<BytesView>),
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
    recv: EventQueue<RecvEvent>,
    recv_waker: AtomicWaker,
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
    pub(crate) fn new(tx: CmdTx, threshold: u32, send_credit: i64) -> Self {
        Self {
            flags: AtomicU32::new(0),
            id: AtomicU32::new(0),
            head: Mutex::new(None),
            head_waker: AtomicWaker::new(),
            recv: EventQueue::new(),
            recv_waker: AtomicWaker::new(),
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
        if this.flags.fetch_or(QUEUED, Ordering::AcqRel) & QUEUED == 0
            && !send_cmd(&this.tx, Command::Ready(Arc::clone(this)))
        {
            // Actor gone: nothing to tell. (Leave QUEUED set; nobody will read it.)
        }
    }

    /// Actor: clear `QUEUED` and return the flags as of now. Call before reading any other state.
    pub(crate) fn begin_ready(&self) -> u32 {
        self.flags.fetch_and(!QUEUED, Ordering::AcqRel) & !QUEUED
    }

    // ---- head slot ---------------------------------------------------------------------------

    /// Actor: publish the response head (or terminal error), then wake the consumer.
    pub(crate) fn publish_head(&self, r: Result<http::response::Parts, Error>) {
        *lock(&self.head) = Some(r);
        self.flags.fetch_or(HEAD_READY, Ordering::Release);
        self.head_waker.wake();
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

    pub(crate) fn push_recv(&self, ev: RecvEvent) {
        self.recv.push(ev);
        self.recv_waker.wake();
    }

    /// Consumer: pop the next event, registering the waker before the final check.
    pub(crate) fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<RecvEvent> {
        if let Some(ev) = self.recv.pop() {
            return Poll::Ready(ev);
        }
        self.recv_waker.register(cx.waker());
        match self.recv.pop() {
            Some(ev) => Poll::Ready(ev),
            None => Poll::Pending,
        }
    }

    pub(crate) fn pop_recv(&self) -> Option<RecvEvent> {
        self.recv.pop()
    }

    /// Consumer: `n` body bytes were handed to the application.
    pub(crate) fn consumed(this: &Arc<Self>, n: u32) {
        if n == 0 {
            return;
        }
        let prev = this.recv_consumed.fetch_add(n, Ordering::AcqRel);
        if prev.saturating_add(n) >= this.threshold {
            Self::mark_ready(this);
        }
    }

    /// Consumer finished the body: report any consumption below the threshold so the actor's buffered-bytes
    /// accounting (which gates connection-window replenishment) does not leak.
    pub(crate) fn flush_consumed(this: &Arc<Self>) {
        if this.recv_consumed.load(Ordering::Acquire) > 0 {
            Self::mark_ready(this);
        }
    }

    /// Actor: take the accumulated consumption.
    pub(crate) fn take_consumed(&self) -> u32 {
        self.recv_consumed.swap(0, Ordering::AcqRel)
    }

    /// Consumer dropped before completion: report everything still queued as consumed and tell the actor.
    pub(crate) fn cancel(this: &Arc<Self>) {
        let mut n: u32 = 0;
        while let Some(ev) = this.recv.pop() {
            if let RecvEvent::Data(d) = ev {
                n = n.saturating_add(u32::try_from(d.len()).unwrap_or(u32::MAX));
            }
        }
        if n > 0 {
            this.recv_consumed.fetch_add(n, Ordering::AcqRel);
        }
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

    /// Actor: no more request-body data will be accepted.
    pub(crate) fn close_send(&self) {
        self.flags.fetch_or(SEND_CLOSED, Ordering::AcqRel);
        self.send_waker.wake();
    }

    /// Actor: terminate the stream towards the consumer with `err`.
    pub(crate) fn fail(&self, err: Error) {
        if self.head_published() {
            self.push_recv(RecvEvent::Error(Box::new(err)));
        } else {
            self.publish_head(Err(err));
        }
        self.close_send();
    }
}

/// Per-connection state read by the pool without any lock.
pub(crate) struct ConnShared {
    pub(crate) in_flight: AtomicUsize,
    pub(crate) max_streams: AtomicUsize,
    pub(crate) accepting: AtomicBool,
    pub(crate) closed: AtomicBool,
    pub(crate) slot_released: Notify,
    pub(crate) close_notify: Notify,
    pub(crate) origin: OriginKey,
    pub(crate) conn_id: u64,
    /// Per-stream consumption (bytes) at which the consumer asks the actor for a WINDOW_UPDATE.
    pub(crate) stream_threshold: u32,
    pub(crate) obs_on: bool,
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
            in_flight: AtomicUsize::new(0),
            max_streams: AtomicUsize::new(max_streams),
            accepting: AtomicBool::new(true),
            closed: AtomicBool::new(false),
            slot_released: Notify::new(),
            close_notify: Notify::new(),
            origin,
            conn_id,
            stream_threshold,
            obs_on,
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

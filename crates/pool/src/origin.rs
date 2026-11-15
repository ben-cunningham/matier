use std::cell::Cell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::Poll;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use matier_core::{Error, ErrorKind, OriginKey, Protocol, Result, ScaleReason};
use matier_transport::h1::{H1Conn, Release};
use matier_transport::h2::{self, H2Handle, StreamSlot};
use matier_transport::{Connected, connect};
use parking_lot::Mutex;
use tokio::sync::{Notify, oneshot};

use crate::scale;
use crate::waiters::Waiters;
use crate::{OriginStats, Shared, dial_error};

const P_UNKNOWN: u8 = 0;
const P_H1: u8 = 1;
const P_H2: u8 = 2;

pub(crate) enum Lease {
    H1(H1Conn),
    H2(H2Handle, StreamSlot),
}

enum Handoff {
    Conn(H1Conn),
    /// A connection slot was freed: the receiver may dial (the slot stays counted in `total`).
    Dial,
}

struct H1State {
    idle: Vec<H1Conn>,
    waiters: Waiters<oneshot::Sender<Handoff>>,
    /// Connections in use + idle + dialing.
    total: usize,
}

pub(crate) struct H2Entry {
    id: u64,
    pub(crate) handle: H2Handle,
    /// Not selectable for new streams (scale-down in progress); retired once it has no streams.
    draining: AtomicBool,
    opened: Instant,
}

impl H2Entry {
    fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }
}

/// "No scale-up yet" marker for the `last_*` timestamps.
const NEVER: u64 = u64::MAX;

/// Reaper-owned bookkeeping.
struct Reap {
    /// Since when a connection has advertised MAX_CONCURRENT_STREAMS = 0.
    zero: HashMap<u64, Instant>,
    /// Per connection: `streams_opened` at the last tick and since when it has had no streams.
    idle: HashMap<u64, (u32, Option<Instant>)>,
    /// Since when every selectable connection has been under-used.
    low_since: Option<Instant>,
    /// Last lifetime-replacement dial. After a failed one the next waits [`REPLACE_DIAL_INTERVAL`], so an
    /// unreachable origin is not redialed every tick.
    last_replace: Option<Instant>,
}

/// Spacing of replacement dials for connections past `max_connection_lifetime` after a failed one.
const REPLACE_DIAL_INTERVAL: Duration = Duration::from_secs(5);

struct H2Pool {
    handles: ArcSwap<Vec<Arc<H2Entry>>>,
    notify: Notify,
    dialing: AtomicUsize,
    waiting: AtomicUsize,
    next_id: AtomicU64,
    /// Connections in the snapshot (mirrors `handles.len()`; lets the hot path avoid loading the snapshot).
    live: AtomicUsize,
    /// Connections marked draining.
    draining: AtomicUsize,
    /// Nanoseconds since `OriginPool::epoch` of the last scale-up dial / of the last scale-up (or `NEVER`).
    last_scale: AtomicU64,
    last_up: AtomicU64,
    reaper_running: AtomicBool,
    fail_gen: AtomicU64,
    fail: Mutex<Option<(ErrorKind, String)>>,
}

pub(crate) struct OriginPool {
    key: OriginKey,
    shared: Arc<Shared>,
    proto: AtomicU8,
    me: Weak<OriginPool>,
    epoch: Instant,
    /// Effective HTTP/2 connection ceiling (see [`crate::scale::h2_ceiling`]).
    h2_max: usize,
    h1: Mutex<H1State>,
    discover: tokio::sync::Mutex<()>,
    h2: H2Pool,
}

enum Pick {
    Conn(H1Conn),
    Dial,
    Wait(Waiter),
}

enum Dialed {
    H1(H1Conn),
    H2,
}

/// A queued HTTP/1 waiter. Dropping it cancels the wait in O(1) and re-homes anything that was
/// handed to it concurrently.
struct Waiter {
    op: Arc<OriginPool>,
    key: usize,
    id: u64,
    rx: Option<oneshot::Receiver<Handoff>>,
}

impl Waiter {
    async fn wait(&mut self) -> Result<Handoff> {
        let rx = self.rx.as_mut().expect("polled after completion");
        let r = rx.await;
        self.rx = None;
        r.map_err(|_| Error::closed())
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        let Some(mut rx) = self.rx.take() else { return };
        let removed = {
            let mut st = self.op.h1.lock();
            let r = st.waiters.remove(self.key, self.id);
            let n = st.waiters.len();
            drop(st);
            if r.is_some() && self.op.shared.obs.enabled() {
                self.op.shared.obs.pool_waiters(&self.op.key, n);
            }
            r
        };
        if removed.is_none() {
            // Already served: the value is in the channel (the sender sent under the lock).
            rx.close();
            match rx.try_recv() {
                Ok(Handoff::Conn(c)) => self.op.release_conn(c),
                Ok(Handoff::Dial) => self.op.release_slot(),
                Err(_) => {}
            }
        }
    }
}

struct CountGuard<'a>(&'a AtomicUsize);
impl<'a> CountGuard<'a> {
    fn new(c: &'a AtomicUsize) -> Self {
        c.fetch_add(1, Ordering::AcqRel);
        Self(c)
    }
}
impl Drop for CountGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

thread_local! {
    static RNG: Cell<u64> = const { Cell::new(0) };
}

fn rand() -> u64 {
    RNG.with(|c| {
        let mut x = c.get();
        if x == 0 {
            let seed = std::ptr::from_ref(c) as u64 ^ 0x9E37_79B9_7F4A_7C15;
            x = seed | 1;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        c.set(x);
        x
    })
}

impl OriginPool {
    pub(crate) fn new(key: OriginKey, shared: Arc<Shared>) -> Arc<Self> {
        let h2_max = crate::scale::h2_ceiling(
            shared.cfg.h2_min_conns_per_origin,
            shared.cfg.h2_max_conns_per_origin,
        );
        Arc::new_cyclic(|me| Self {
            key,
            shared,
            proto: AtomicU8::new(P_UNKNOWN),
            me: me.clone(),
            epoch: Instant::now(),
            h2_max,
            h1: Mutex::new(H1State {
                idle: Vec::new(),
                waiters: Waiters::new(),
                total: 0,
            }),
            discover: tokio::sync::Mutex::new(()),
            h2: H2Pool {
                handles: ArcSwap::from_pointee(Vec::new()),
                notify: Notify::new(),
                dialing: AtomicUsize::new(0),
                waiting: AtomicUsize::new(0),
                next_id: AtomicU64::new(1),
                live: AtomicUsize::new(0),
                draining: AtomicUsize::new(0),
                last_scale: AtomicU64::new(NEVER),
                last_up: AtomicU64::new(NEVER),
                reaper_running: AtomicBool::new(false),
                fail_gen: AtomicU64::new(0),
                fail: Mutex::new(None),
            },
        })
    }

    pub(crate) fn stats(&self) -> OriginStats {
        let st = self.h1.lock();
        OriginStats {
            protocol: match self.proto.load(Ordering::Acquire) {
                P_H1 => Some(Protocol::Http1),
                P_H2 => Some(Protocol::Http2),
                _ => None,
            },
            h1_total: st.total,
            h1_idle: st.idle.len(),
            h1_waiters: st.waiters.len(),
            h2_conns: self.h2.handles.load().len(),
            h2_draining: self.h2.draining.load(Ordering::Acquire),
        }
    }

    // ---- HTTP/1 --------------------------------------------------------------------------

    fn expired(&self, c: &H1Conn) -> bool {
        c.last_used().elapsed() > self.shared.cfg.idle_timeout || self.too_old(c)
    }

    fn too_old(&self, c: &H1Conn) -> bool {
        self.shared
            .cfg
            .max_connection_lifetime
            .is_some_and(|max| c.created_at().elapsed() > max)
    }

    /// Pops the freshest usable idle connection (probing liveness), discarding dead ones.
    fn pop_idle(&self) -> Option<H1Conn> {
        loop {
            let mut c = self.h1.lock().idle.pop()?;
            if self.expired(&c) || !c.is_reusable_now() {
                drop(c); // `closed()` frees the slot
                continue;
            }
            return Some(c);
        }
    }

    /// Allocation-free checkout when something is immediately available.
    pub(crate) fn try_fast(&self, fresh: bool) -> Option<Lease> {
        match self.proto.load(Ordering::Acquire) {
            P_H1 if !fresh => self.pop_idle().map(Lease::H1),
            P_H2 => self.h2_pick().map(|(h, s)| Lease::H2(h, s)),
            _ => None,
        }
    }

    fn h1_pick(self: &Arc<Self>, fresh: bool) -> Pick {
        loop {
            let mut st = self.h1.lock();
            if !fresh && let Some(mut c) = st.idle.pop() {
                drop(st);
                if self.expired(&c) || !c.is_reusable_now() {
                    drop(c);
                    continue;
                }
                return Pick::Conn(c);
            }
            if st.total < self.shared.cfg.max_conns_per_origin {
                st.total += 1;
                return Pick::Dial;
            }
            let (tx, rx) = oneshot::channel();
            let (key, id) = st.waiters.push_back(tx);
            let n = st.waiters.len();
            drop(st);
            if self.shared.obs.enabled() {
                self.shared.obs.pool_waiters(&self.key, n);
            }
            return Pick::Wait(Waiter {
                op: Arc::clone(self),
                key,
                id,
                rx: Some(rx),
            });
        }
    }

    /// Returns a connection to the pool: oldest waiter first, else the idle stack.
    fn release_conn(&self, mut conn: H1Conn) {
        if self.too_old(&conn) {
            drop(conn); // `closed()` frees the slot, or hands it to a waiter to dial
            return;
        }
        let mut stale: Vec<H1Conn> = Vec::new();
        {
            let mut st = self.h1.lock();
            while let Some(tx) = st.waiters.pop_front() {
                match tx.send(Handoff::Conn(conn)) {
                    Ok(()) => return,
                    Err(Handoff::Conn(c)) => conn = c,
                    Err(Handoff::Dial) => unreachable!(),
                }
            }
            if st.idle.len() < self.shared.cfg.max_idle_per_origin {
                let timeout = self.shared.cfg.idle_timeout;
                let k = st
                    .idle
                    .partition_point(|c| c.last_used().elapsed() > timeout);
                if k > 0 {
                    stale.extend(st.idle.drain(..k));
                }
                st.idle.push(conn);
                drop(st);
                drop(stale);
                return;
            }
        }
        drop(conn); // over max_idle: close (outside the lock)
    }

    /// A connection slot is free: hand it to the oldest waiter (who dials) or give it back.
    fn release_slot(&self) {
        let mut st = self.h1.lock();
        while let Some(tx) = st.waiters.pop_front() {
            if tx.send(Handoff::Dial).is_ok() {
                return;
            }
        }
        st.total = st.total.saturating_sub(1);
    }

    async fn dial_reserved(self: &Arc<Self>) -> Result<Dialed> {
        let sh = &self.shared;
        let connected = match connect(&self.key, &sh.connect, &*sh.obs).await {
            Ok(c) => c,
            Err(e) => {
                self.release_slot();
                return Err(e);
            }
        };
        let Connected { io, protocol } = connected;
        match protocol {
            Protocol::Http1 => {
                let mut c = H1Conn::new_shared(io, self.key.clone(), Arc::clone(&sh.h1));
                c.set_owner(self.me.clone() as Weak<dyn Release>);
                sh.obs.connection_opened(&self.key, Protocol::Http1);
                self.proto.store(P_H1, Ordering::Release);
                Ok(Dialed::H1(c))
            }
            Protocol::Http2 => {
                let r =
                    h2::handshake(io, self.key.clone(), sh.h2.clone(), Arc::clone(&sh.obs)).await;
                self.release_slot();
                let handle = r?;
                self.proto.store(P_H2, Ordering::Release);
                self.install_h2(handle, ScaleReason::Min);
                self.h2_ensure_min();
                Ok(Dialed::H2)
            }
        }
    }

    // ---- slow path -------------------------------------------------------------------------

    pub(crate) fn acquire_slow<'a>(
        self: &'a Arc<Self>,
        fresh: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Lease>> + Send + 'a>> {
        Box::pin(self.acquire_slow_inner(fresh))
    }

    async fn acquire_slow_inner(self: &Arc<Self>, fresh: bool) -> Result<Lease> {
        let started = Instant::now();
        let deadline = self.shared.cfg.checkout_timeout.map(|d| started + d);
        let mut waited = false;
        loop {
            match self.proto.load(Ordering::Acquire) {
                P_H1 => match self.h1_pick(fresh) {
                    Pick::Conn(c) => return Ok(Lease::H1(c)),
                    Pick::Dial => {
                        if let Dialed::H1(c) = self.dial_reserved().await? {
                            return Ok(Lease::H1(c));
                        }
                    }
                    Pick::Wait(mut w) => {
                        waited = true;
                        let h = match deadline {
                            Some(d) => tokio::time::timeout_at(d.into(), w.wait())
                                .await
                                .map_err(|_| Error::pool_timeout())??,
                            None => w.wait().await?,
                        };
                        match h {
                            Handoff::Conn(c) => {
                                self.observe_wait(started);
                                return Ok(Lease::H1(c));
                            }
                            Handoff::Dial => {
                                if let Dialed::H1(c) = self.dial_reserved().await? {
                                    self.observe_wait(started);
                                    return Ok(Lease::H1(c));
                                }
                            }
                        }
                    }
                },
                P_H2 => {
                    let r = self.h2_acquire_slow(deadline).await?;
                    if waited || self.shared.obs.enabled() {
                        self.observe_wait(started);
                    }
                    return Ok(Lease::H2(r.0, r.1));
                }
                _ => {
                    if let Some(l) = self.discover().await? {
                        return Ok(l);
                    }
                }
            }
        }
    }

    fn observe_wait(&self, started: Instant) {
        if self.shared.obs.enabled() {
            self.shared.obs.request_queued(&self.key, started.elapsed());
        }
    }

    /// First connection to an origin: only one task dials; the rest wait for the verdict.
    async fn discover(self: &Arc<Self>) -> Result<Option<Lease>> {
        let _g = self.discover.lock().await;
        if self.proto.load(Ordering::Acquire) != P_UNKNOWN {
            return Ok(None);
        }
        self.h1.lock().total += 1;
        match self.dial_reserved().await? {
            Dialed::H1(c) => Ok(Some(Lease::H1(c))),
            Dialed::H2 => Ok(None),
        }
    }

    // ---- HTTP/2 ----------------------------------------------------------------------------

    fn h2_limit(&self, h: &H2Handle) -> usize {
        h.max_concurrent_streams()
            .min(self.shared.cfg.h2_target_streams_per_conn)
    }

    fn h2_try<'g>(&self, e: &'g H2Entry) -> Option<(&'g H2Handle, StreamSlot)> {
        let h = &e.handle;
        if e.is_draining() || !h.is_accepting() || h.in_flight() >= self.h2_limit(h) {
            return None;
        }
        // Zero-capacity connections never count (`in_flight >= limit` covers it; stated for clarity).
        h.try_reserve().map(|s| (h, s))
    }

    /// Power-of-two-choices pick, then the autoscaling check on the chosen connection.
    fn h2_pick(&self) -> Option<(H2Handle, StreamSlot)> {
        let snap = self.h2.handles.load();
        let (h, s) = self.h2_select(&snap)?;
        self.h2_scale_check(h);
        Some((h.clone(), s))
    }

    /// Hot path: pick a connection and reserve a stream without cloning the handle (the caller sends the
    /// request while `snap` is borrowed).
    pub(crate) fn h2_pick_in<'g>(
        &self,
        snap: &'g [Arc<H2Entry>],
    ) -> Option<(&'g H2Handle, StreamSlot)> {
        let (h, s) = self.h2_select(snap)?;
        self.h2_scale_check(h);
        Some((h, s))
    }

    pub(crate) fn h2_snapshot(&self) -> arc_swap::Guard<Arc<Vec<Arc<H2Entry>>>> {
        self.h2.handles.load()
    }

    pub(crate) fn is_h2(&self) -> bool {
        self.proto.load(Ordering::Acquire) == P_H2
    }

    fn h2_select<'g>(&self, snap: &'g [Arc<H2Entry>]) -> Option<(&'g H2Handle, StreamSlot)> {
        let n = snap.len();
        match n {
            0 => return None,
            1 => return self.h2_try(&snap[0]),
            _ => {}
        }
        let r = rand();
        let i = (r as usize) % n;
        let mut j = ((r >> 32) as usize) % n;
        if j == i {
            j = (j + 1) % n;
        }
        let (a, b) = (&snap[i], &snap[j]);
        let (first, second) = if a.handle.in_flight() <= b.handle.in_flight() {
            (a, b)
        } else {
            (b, a)
        };
        if let Some(x) = self.h2_try(first).or_else(|| self.h2_try(second)) {
            return Some(x);
        }
        snap.iter().find_map(|e| self.h2_try(e))
    }

    async fn h2_acquire_slow(
        self: &Arc<Self>,
        deadline: Option<Instant>,
    ) -> Result<(H2Handle, StreamSlot)> {
        let _w = CountGuard::new(&self.h2.waiting);
        let gen0 = self.h2.fail_gen.load(Ordering::Acquire);
        loop {
            let notified = self.h2.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            // Per-connection slot-release notifications (registered before the re-check: no lost wakeup).
            let snap = self.h2.handles.load_full();
            let mut slot_waits: Vec<Pin<Box<tokio::sync::futures::Notified<'_>>>> = snap
                .iter()
                .map(|e| Box::pin(e.handle.slot_released()))
                .collect();
            for w in &mut slot_waits {
                w.as_mut().enable();
            }
            if let Some(x) = self.h2_pick() {
                return Ok(x);
            }
            if self.h2.fail_gen.load(Ordering::Acquire) != gen0 {
                let f = self.h2.fail.lock().clone();
                let (kind, msg) = f.unwrap_or((ErrorKind::Connect, "dial failed".into()));
                return Err(dial_error(kind, msg));
            }
            self.h2_maybe_dial();
            let timeout = async {
                match deadline {
                    Some(d) => tokio::time::sleep_until(d.into()).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                () = &mut notified => {}
                () = std::future::poll_fn(|cx| {
                    for w in &mut slot_waits {
                        if w.as_mut().poll(cx).is_ready() {
                            return Poll::Ready(());
                        }
                    }
                    Poll::Pending
                }) => {}
                () = timeout => return Err(Error::pool_timeout()),
            }
        }
    }

    fn h2_live(&self) -> usize {
        self.h2
            .handles
            .load()
            .iter()
            .filter(|e| e.handle.is_accepting())
            .count()
    }

    fn h2_maybe_dial(self: &Arc<Self>) {
        let live = self.h2_live();
        let dialing = self.h2.dialing.load(Ordering::Acquire);
        let waiting = self.h2.waiting.load(Ordering::Acquire);
        let want = waiting
            .div_ceil(self.shared.cfg.h2_target_streams_per_conn.max(1))
            .max(1);
        if self.h2_undrain_one() {
            return;
        }
        // Connections that advertise zero stream capacity are not capacity: do not dial up to the maximum
        // on their account (at most one extra connection in total, retired again by the reaper if it is no better).
        let zero = self
            .h2
            .handles
            .load()
            .iter()
            .filter(|e| e.handle.is_accepting() && e.handle.max_concurrent_streams() == 0)
            .count();
        if zero > 0 && live + dialing >= 2 {
            return;
        }
        if live + dialing < self.h2_max() && dialing < want {
            self.h2_spawn_dial(ScaleReason::Waiters);
        }
    }

    fn h2_max(&self) -> usize {
        self.h2_max
    }

    fn h2_min(&self) -> usize {
        self.shared.cfg.h2_min_conns_per_origin.min(self.h2_max())
    }

    fn h2_ensure_min(self: &Arc<Self>) {
        let min = self.h2_min();
        loop {
            let have = self.h2_live() + self.h2.dialing.load(Ordering::Acquire);
            if have >= min {
                return;
            }
            self.h2_spawn_dial(ScaleReason::Min);
        }
    }

    fn now_ns(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(NEVER - 1)
    }

    /// Autoscaling check, run after a request picked `h` (and reserved a slot on it). Hot path: a handful of
    /// relaxed loads unless the origin could still grow *and* `h` looks saturated; then one clock read.
    /// The request itself never waits: at most one background dial is started.
    fn h2_scale_check(&self, h: &H2Handle) {
        let h2 = &self.h2;
        if h2.dialing.load(Ordering::Relaxed) != 0 {
            return;
        }
        let live = h2.live.load(Ordering::Relaxed);
        if live >= self.h2_max() && h2.draining.load(Ordering::Relaxed) == 0 {
            return;
        }
        let limit = self.h2_limit(h);
        let th = &self.shared.cfg.h2_scale;
        if !th.is_saturated(h.load_unchecked(), h.in_flight(), limit) {
            return;
        }
        let now = self.now_ns();
        let last = h2.last_scale.load(Ordering::Relaxed);
        if last != NEVER && now.saturating_sub(last) < th.cooldown.as_nanos() as u64 {
            return;
        }
        // Re-check with the staleness-aware signals (an actor that stopped ticking reads as idle).
        if !th.is_saturated(h.load(), h.in_flight(), limit) {
            return;
        }
        if h2
            .last_scale
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return; // another request won the right to scale
        }
        h2.last_up.store(now, Ordering::Relaxed);
        let Some(me) = self.me.upgrade() else { return };
        if me.h2_undrain_one() {
            return;
        }
        if live < me.h2_max() {
            me.h2_spawn_dial(ScaleReason::Saturated);
        }
    }

    /// Makes the newest draining connection selectable again (cheaper than dialing). True if one was.
    fn h2_undrain_one(&self) -> bool {
        if self.h2.draining.load(Ordering::Acquire) == 0 {
            return false;
        }
        let snap = self.h2.handles.load();
        for e in snap.iter().rev() {
            if e.draining.swap(false, Ordering::AcqRel) {
                self.h2.draining.fetch_sub(1, Ordering::AcqRel);
                self.h2.notify.notify_waiters();
                let n = snap.iter().filter(|e| !e.is_draining()).count();
                self.shared
                    .obs
                    .pool_scaled(&self.key, n, ScaleReason::Saturated);
                return true;
            }
        }
        false
    }

    fn h2_spawn_dial(self: &Arc<Self>, reason: ScaleReason) {
        self.h2.dialing.fetch_add(1, Ordering::AcqRel);
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let r = me.h2_dial(reason).await;
            // A lifetime replacement is not on behalf of any waiter: the old connection still serves them.
            if let Err(e) = r
                && reason != ScaleReason::Replace
            {
                *me.h2.fail.lock() = Some((e.kind(), e.to_string()));
                me.h2.fail_gen.fetch_add(1, Ordering::AcqRel);
            }
            me.h2.dialing.fetch_sub(1, Ordering::AcqRel);
            me.h2.notify.notify_waiters();
        });
    }

    async fn h2_dial(self: &Arc<Self>, reason: ScaleReason) -> Result<()> {
        let sh = &self.shared;
        let Connected { io, protocol } = connect(&self.key, &sh.connect, &*sh.obs).await?;
        if protocol != Protocol::Http2 {
            return Err(Error::protocol("origin no longer negotiates HTTP/2"));
        }
        let handle =
            h2::handshake(io, self.key.clone(), sh.h2.clone(), Arc::clone(&sh.obs)).await?;
        self.install_h2(handle, reason);
        Ok(())
    }

    fn h2_publish_counts(&self) {
        let snap = self.h2.handles.load();
        self.h2.live.store(snap.len(), Ordering::Release);
        let d = snap.iter().filter(|e| e.is_draining()).count();
        self.h2.draining.store(d, Ordering::Release);
    }

    fn install_h2(self: &Arc<Self>, handle: H2Handle, reason: ScaleReason) {
        let id = self.h2.next_id.fetch_add(1, Ordering::Relaxed);
        let entry = Arc::new(H2Entry {
            id,
            handle,
            draining: AtomicBool::new(false),
            opened: Instant::now(),
        });
        self.h2.handles.rcu(|cur| {
            let mut v = Vec::clone(cur);
            v.push(Arc::clone(&entry));
            v
        });
        self.h2_publish_counts();
        self.shared
            .obs
            .connection_opened(&self.key, Protocol::Http2);
        self.shared
            .obs
            .pool_scaled(&self.key, self.h2.live.load(Ordering::Acquire), reason);
        self.h2.notify.notify_waiters();
        let me = Arc::downgrade(self);
        let opened = entry.opened;
        // Only the close notification is captured: holding an `H2Handle` here would keep the actor's inbox
        // open and defeat the graceful close of retired connections.
        let closed = entry.handle.closed();
        drop(entry);
        tokio::spawn(async move {
            closed.await;
            let Some(me) = me.upgrade() else { return };
            me.h2_remove(id);
            me.shared
                .obs
                .connection_closed(&me.key, Protocol::Http2, opened.elapsed());
            me.h2.notify.notify_waiters();
            me.h2_ensure_min();
        });
        self.h2_ensure_reaper();
    }

    fn h2_remove(&self, id: u64) {
        self.h2.handles.rcu(|cur| {
            cur.iter()
                .filter(|e| e.id != id)
                .cloned()
                .collect::<Vec<_>>()
        });
        self.h2_publish_counts();
    }

    // ---- scale-down --------------------------------------------------------------------------

    fn h2_ensure_reaper(self: &Arc<Self>) {
        if self.h2.reaper_running.swap(true, Ordering::AcqRel) {
            return;
        }
        let me = Arc::downgrade(self);
        let tick = scale::reap_tick(self.shared.cfg.idle_timeout);
        tokio::spawn(async move {
            let mut st = Reap {
                zero: HashMap::new(),
                idle: HashMap::new(),
                low_since: None,
                last_replace: None,
            };
            loop {
                tokio::time::sleep(tick).await;
                let Some(op) = me.upgrade() else { return };
                if !op.h2_reap(&mut st) {
                    op.h2.reaper_running.store(false, Ordering::Release);
                    // A connection may have been installed between the check and the store.
                    if op.h2.live.load(Ordering::Acquire) > 0
                        && !op.h2.reaper_running.swap(true, Ordering::AcqRel)
                    {
                        continue;
                    }
                    return;
                }
            }
        });
    }

    /// One maintenance tick. Returns false when the origin has no connections left.
    ///
    /// * A connection with no open stream (and none opened since the last tick) for `idle_timeout` is retired,
    ///   newest first, down to `h2_min_conns_per_origin`. Retired = removed from the snapshot; when the last
    ///   in-flight request lets go of its handle the actor sends GOAWAY(NO_ERROR) and exits.
    /// * A draining connection is retired as soon as it has no streams.
    /// * When every selectable connection has been under-used for [`scale::DRAIN_AFTER_LOW`] (and nothing was
    ///   scaled up recently) the newest one is marked draining.
    /// * One connection older than `max_connection_lifetime` is retired per tick, once enough younger ones
    ///   remain; otherwise its replacement is dialed first.
    fn h2_reap(self: &Arc<Self>, st: &mut Reap) -> bool {
        let snap = self.h2.handles.load_full();
        if snap.is_empty() {
            return false;
        }
        let now = Instant::now();
        let min = self.h2_min().max(1);
        st.idle.retain(|id, _| snap.iter().any(|e| e.id == *id));
        for e in snap.iter() {
            let opened = e.handle.streams_opened();
            let s = st.idle.entry(e.id).or_insert((opened, None));
            if e.handle.in_flight() == 0 && s.0 == opened {
                s.1.get_or_insert(now);
            } else {
                s.1 = None;
            }
            s.0 = opened;
        }

        // Zero-capacity connections (peer MAX_CONCURRENT_STREAMS = 0) are retired after a grace period.
        st.zero.retain(|id, _| snap.iter().any(|e| e.id == *id));
        let mut zero_retired = false;
        for e in snap.iter() {
            if e.handle.max_concurrent_streams() == 0 {
                let since = *st.zero.entry(e.id).or_insert(now);
                if now.duration_since(since) >= self.shared.cfg.h2_zero_capacity_drain {
                    self.h2_remove(e.id);
                    zero_retired = true;
                }
            } else {
                st.zero.remove(&e.id);
            }
        }
        if zero_retired || self.h2_retire_expired(&snap, now, min, &mut st.last_replace) {
            return true;
        }
        let mut n = snap.len();
        let mut retired = false;
        for e in snap.iter().rev() {
            if n <= min {
                break;
            }
            let idle = st.idle[&e.id]
                .1
                .is_some_and(|t| now.duration_since(t) >= self.shared.cfg.idle_timeout);
            let drained = e.is_draining() && e.handle.in_flight() == 0;
            if drained || idle {
                self.h2_remove(e.id);
                n -= 1;
                retired = true;
                if !drained {
                    self.shared.obs.pool_scaled(&self.key, n, ScaleReason::Idle);
                }
            }
        }
        if retired {
            st.low_since = None;
            return true;
        }

        let sel: Vec<&Arc<H2Entry>> = snap
            .iter()
            .filter(|e| !e.is_draining() && e.handle.is_accepting())
            .collect();
        if sel.len() <= min {
            st.low_since = None;
            return true;
        }
        let busy: Vec<u32> = sel
            .iter()
            .map(|e| {
                if e.handle.in_flight() == 0 {
                    0
                } else {
                    e.handle.load().busy_permille
                }
            })
            .collect();
        let last_up = self.h2.last_up.load(Ordering::Relaxed);
        let held_off = last_up != NEVER
            && self.now_ns().saturating_sub(last_up)
                < scale::DRAIN_HOLDOFF_AFTER_SCALE_UP.as_nanos() as u64;
        if held_off || !scale::can_drain(&busy) {
            st.low_since = None;
            return true;
        }
        let since = *st.low_since.get_or_insert(now);
        if now.duration_since(since) >= scale::DRAIN_AFTER_LOW
            && let Some(e) = sel.last()
        {
            e.draining.store(true, Ordering::Release);
            self.h2_publish_counts();
            st.low_since = None;
            self.shared
                .obs
                .pool_scaled(&self.key, sel.len() - 1, ScaleReason::Underused);
        }
        true
    }

    /// Retires the oldest connection past `max_connection_lifetime` when at least `min` other selectable
    /// connections remain; otherwise restores capacity first (un-drains one, or dials a replacement that is
    /// retired against on a later tick, so the count exceeds the ceiling by at most one). True if one was
    /// retired.
    fn h2_retire_expired(
        self: &Arc<Self>,
        snap: &[Arc<H2Entry>],
        now: Instant,
        min: usize,
        last_replace: &mut Option<Instant>,
    ) -> bool {
        let Some(max) = self.shared.cfg.max_connection_lifetime else {
            return false;
        };
        let Some(old) = snap.iter().find(|e| now.duration_since(e.opened) >= max) else {
            return false;
        };
        let others = snap
            .iter()
            .filter(|e| e.id != old.id && !e.is_draining() && e.handle.is_accepting())
            .count();
        if others < min {
            if self.h2_undrain_one() {
                return false;
            }
            // Back off only when the previous replacement dial failed (no connection opened since).
            let due = last_replace.is_none_or(|t| {
                now.duration_since(t) >= REPLACE_DIAL_INTERVAL || snap.iter().any(|e| e.opened > t)
            });
            if due && self.h2.dialing.load(Ordering::Acquire) == 0 {
                *last_replace = Some(now);
                self.h2_spawn_dial(ScaleReason::Replace);
            }
            return false;
        }
        self.h2_remove(old.id);
        self.shared.obs.pool_scaled(
            &self.key,
            self.h2.live.load(Ordering::Acquire),
            ScaleReason::Expired,
        );
        true
    }
}

impl Release for OriginPool {
    fn release(&self, conn: H1Conn) {
        self.release_conn(conn);
    }

    fn closed(&self, _conn_id: u64, lifetime: Duration) {
        self.shared
            .obs
            .connection_closed(&self.key, Protocol::Http1, lifetime);
        self.release_slot();
    }
}

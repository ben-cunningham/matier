use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::Poll;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use matier_core::{Error, ErrorKind, OriginKey, Protocol, Result};
use matier_transport::h1::{H1Conn, Release};
use matier_transport::h2::{self, H2Handle, StreamSlot};
use matier_transport::{Connected, connect};
use parking_lot::Mutex;
use tokio::sync::{Notify, oneshot};

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

struct H2Entry {
    id: u64,
    handle: H2Handle,
}

struct H2Pool {
    handles: ArcSwap<Vec<Arc<H2Entry>>>,
    notify: Notify,
    dialing: AtomicUsize,
    waiting: AtomicUsize,
    next_id: AtomicU64,
    fail_gen: AtomicU64,
    fail: Mutex<Option<(ErrorKind, String)>>,
}

pub(crate) struct OriginPool {
    key: OriginKey,
    shared: Arc<Shared>,
    proto: AtomicU8,
    me: Weak<OriginPool>,
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
        Arc::new_cyclic(|me| Self {
            key,
            shared,
            proto: AtomicU8::new(P_UNKNOWN),
            me: me.clone(),
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
        }
    }

    // ---- HTTP/1 --------------------------------------------------------------------------

    fn expired(&self, c: &H1Conn) -> bool {
        c.last_used().elapsed() > self.shared.cfg.idle_timeout
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
        let t0 = sh.obs.enabled().then(Instant::now);
        let connected = match connect(&self.key, &sh.connect, &*sh.obs).await {
            Ok(c) => c,
            Err(e) => {
                self.release_slot();
                return Err(e);
            }
        };
        let _ = t0;
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
                self.install_h2(handle);
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

    fn h2_try(&self, e: &H2Entry) -> Option<(H2Handle, StreamSlot)> {
        let h = &e.handle;
        if !h.is_accepting() || h.in_flight() >= self.h2_limit(h) {
            return None;
        }
        h.try_reserve().map(|s| (h.clone(), s))
    }

    fn h2_pick(&self) -> Option<(H2Handle, StreamSlot)> {
        let snap = self.h2.handles.load();
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
        if live + dialing < self.shared.cfg.max_conns_per_origin && dialing < want {
            self.h2_spawn_dial();
        }
    }

    fn h2_ensure_min(self: &Arc<Self>) {
        let min = self
            .shared
            .cfg
            .h2_min_conns_per_origin
            .min(self.shared.cfg.max_conns_per_origin);
        loop {
            let have = self.h2_live() + self.h2.dialing.load(Ordering::Acquire);
            if have >= min {
                return;
            }
            self.h2_spawn_dial();
        }
    }

    fn h2_spawn_dial(self: &Arc<Self>) {
        self.h2.dialing.fetch_add(1, Ordering::AcqRel);
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let r = me.h2_dial().await;
            if let Err(e) = r {
                *me.h2.fail.lock() = Some((e.kind(), e.to_string()));
                me.h2.fail_gen.fetch_add(1, Ordering::AcqRel);
            }
            me.h2.dialing.fetch_sub(1, Ordering::AcqRel);
            me.h2.notify.notify_waiters();
        });
    }

    async fn h2_dial(self: &Arc<Self>) -> Result<()> {
        let sh = &self.shared;
        let Connected { io, protocol } = connect(&self.key, &sh.connect, &*sh.obs).await?;
        if protocol != Protocol::Http2 {
            return Err(Error::protocol("origin no longer negotiates HTTP/2"));
        }
        let handle =
            h2::handshake(io, self.key.clone(), sh.h2.clone(), Arc::clone(&sh.obs)).await?;
        self.install_h2(handle);
        Ok(())
    }

    fn install_h2(self: &Arc<Self>, handle: H2Handle) {
        let id = self.h2.next_id.fetch_add(1, Ordering::Relaxed);
        let entry = Arc::new(H2Entry { id, handle });
        self.h2.handles.rcu(|cur| {
            let mut v = Vec::clone(cur);
            v.push(Arc::clone(&entry));
            v
        });
        self.shared
            .obs
            .connection_opened(&self.key, Protocol::Http2);
        self.h2.notify.notify_waiters();
        let me = Arc::downgrade(self);
        let opened = Instant::now();
        let handle = entry.handle.clone();
        tokio::spawn(async move {
            handle.closed().await;
            let Some(me) = me.upgrade() else { return };
            me.h2.handles.rcu(|cur| {
                cur.iter()
                    .filter(|e| e.id != id)
                    .cloned()
                    .collect::<Vec<_>>()
            });
            me.shared
                .obs
                .connection_closed(&me.key, Protocol::Http2, opened.elapsed());
            me.h2.notify.notify_waiters();
            me.h2_ensure_min();
        });
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

//! `matier-pool`: per-origin connection pooling for Matier (PLAN §6).
//!
//! * **Origin map**: a [`papaya`] lock-free hash map `OriginKey -> Arc<OriginPool>`. Lookups are
//!   read-mostly (one insert per origin ever), papaya reads take no lock and never write shared
//!   cache lines beyond the epoch guard, and the lookup is allocation-free (borrowed key, FxHash).
//! * **HTTP/1**: LIFO idle stack behind a short `parking_lot::Mutex` (push/pop only, never held
//!   across an await), a connection count, and a FIFO waiter queue (slab-backed linked list, O(1)
//!   cancel) with direct handoff of released connections (or of a freed dial slot) to the oldest
//!   waiter. The liveness probe runs at checkout; the idle timeout is checked lazily.
//! * **HTTP/2**: an `arc_swap` snapshot of handles, power-of-two-choices on `in_flight`,
//!   `try_reserve`, coalesced background dials, and zero-config load-based autoscaling ([`scale`]).
//!
//! The protocol of an origin is learned from the first connection (ALPN / prior knowledge) and
//! cached; concurrent first requests wait for that one connection instead of dialing in parallel.

mod origin;
pub mod scale;
mod waiters;

use std::borrow::Borrow;
use std::future::Future;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::Either;
use matier_core::{Body, Error, ErrorKind, Observer, OriginKey, Protocol, Result, Scheme};
use matier_transport::h1::{H1Config, Release};
use matier_transport::h2::H2Config;
use matier_transport::{ConnectConfig, Incoming};
use rustc_hash::FxHasher;

use origin::{Lease, OriginPool};

/// Pool limits and timeouts.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Idle HTTP/1 connections kept per origin (default 32).
    pub max_idle_per_origin: usize,
    /// Idle HTTP/1 connections older than this are discarded at checkout (default 90 s).
    pub idle_timeout: Duration,
    /// HTTP/1: maximum connections (in use + idle + dialing) per origin (default 256).
    pub max_conns_per_origin: usize,
    /// Streams per HTTP/2 connection before preferring another/new connection (default 128).
    pub h2_target_streams_per_conn: usize,
    /// HTTP/2 connections kept per origin: dialed eagerly once the origin is known to speak h2 and never
    /// scaled below (default 1). Leave it alone: the pool adds connections under load by itself.
    pub h2_min_conns_per_origin: usize,
    /// HTTP/2 autoscaling ceiling per origin (default: `std::thread::available_parallelism()`; one
    /// connection is one actor task, so more connections than cores cannot add parallelism). Set equal to
    /// `h2_min_conns_per_origin` to pin the connection count. See [`scale`] for the policy.
    pub h2_max_conns_per_origin: usize,
    /// HTTP/2 scale-up thresholds (defaults: the constants in [`scale`]).
    pub h2_scale: scale::ScaleThresholds,
    /// Maximum time a request waits for a connection or stream slot (default 30 s; `None` waits forever).
    /// Failure is `PoolTimeout` with `Recovery::Unprocessed`.
    pub checkout_timeout: Option<Duration>,
    /// An HTTP/2 connection whose peer advertises `SETTINGS_MAX_CONCURRENT_STREAMS = 0` is retired after
    /// this long (default 10 s). Such a connection never counts as capacity and never triggers scale-up.
    pub h2_zero_capacity_drain: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_idle_per_origin: 32,
            idle_timeout: Duration::from_secs(90),
            max_conns_per_origin: 256,
            h2_target_streams_per_conn: 128,
            h2_min_conns_per_origin: 1,
            h2_max_conns_per_origin: scale::default_h2_max_conns(),
            h2_scale: scale::ScaleThresholds::default(),
            checkout_timeout: Some(Duration::from_secs(30)),
            h2_zero_capacity_drain: Duration::from_secs(10),
        }
    }
}

pub(crate) struct Shared {
    pub(crate) cfg: PoolConfig,
    pub(crate) connect: ConnectConfig,
    pub(crate) h1: Arc<H1Config>,
    pub(crate) h2: H2Config,
    pub(crate) obs: Arc<dyn Observer>,
}

/// Snapshot of one origin's pool, for diagnostics and tests.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OriginStats {
    /// Learned protocol (`None` until the first connection).
    pub protocol: Option<Protocol>,
    /// HTTP/1: connections in use + idle + dialing.
    pub h1_total: usize,
    /// HTTP/1: idle connections.
    pub h1_idle: usize,
    /// HTTP/1: queued waiters.
    pub h1_waiters: usize,
    /// HTTP/2: live connections (including draining ones).
    pub h2_conns: usize,
    /// HTTP/2: connections marked draining (not selectable; closing once idle).
    pub h2_draining: usize,
}

// --- allocation-free map lookup ---------------------------------------------------------------

trait KeyParts {
    fn parts(&self) -> (Scheme, &str, u16);
}
struct Key(OriginKey);
impl KeyParts for Key {
    fn parts(&self) -> (Scheme, &str, u16) {
        (self.0.scheme, &self.0.host, self.0.port)
    }
}
struct Probe<'a>(Scheme, &'a str, u16);
impl KeyParts for Probe<'_> {
    fn parts(&self) -> (Scheme, &str, u16) {
        (self.0, self.1, self.2)
    }
}
impl<'a> Borrow<dyn KeyParts + 'a> for Key {
    fn borrow(&self) -> &(dyn KeyParts + 'a) {
        self
    }
}
impl Hash for dyn KeyParts + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (s, h, p) = self.parts();
        (s as u8).hash(state);
        h.hash(state);
        p.hash(state);
    }
}
impl PartialEq for dyn KeyParts + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}
impl Eq for dyn KeyParts + '_ {}
impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        <dyn KeyParts as Hash>::hash(self, state);
    }
}
impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}
impl Eq for Key {}

struct PoolInner {
    map: papaya::HashMap<Key, Arc<OriginPool>, BuildHasherDefault<FxHasher>>,
    shared: Arc<Shared>,
}

type MapGuard<'g> = papaya::HashMapRef<
    'g,
    Key,
    Arc<OriginPool>,
    BuildHasherDefault<FxHasher>,
    papaya::LocalGuard<'g>,
>;

impl PoolInner {
    /// Allocation-free lookup of an existing origin; `None` for anything unusual or unknown.
    fn lookup<'g>(&self, map: &'g MapGuard<'g>, uri: &http::Uri) -> Option<&'g Arc<OriginPool>> {
        let (scheme, host) = (uri.scheme_str()?, uri.host()?);
        let sch = if scheme.eq_ignore_ascii_case("http") {
            Scheme::Http
        } else if scheme.eq_ignore_ascii_case("https") {
            Scheme::Https
        } else {
            return None;
        };
        if host.bytes().any(|b| b.is_ascii_uppercase()) {
            return None;
        }
        let port = uri.port_u16().unwrap_or_else(|| sch.default_port());
        let probe = Probe(sch, host, port);
        map.get(&probe as &dyn KeyParts)
    }

    fn origin_pool(&self, uri: &http::Uri) -> Result<Arc<OriginPool>> {
        let map = self.map.pin();
        // Fast path: no allocation. Falls back to the full (allocating) parse for anything unusual.
        if let (Some(scheme), Some(host)) = (uri.scheme_str(), uri.host()) {
            let sch = if scheme.eq_ignore_ascii_case("http") {
                Some(Scheme::Http)
            } else if scheme.eq_ignore_ascii_case("https") {
                Some(Scheme::Https)
            } else {
                None
            };
            if let Some(sch) = sch
                && !host.bytes().any(|b| b.is_ascii_uppercase())
            {
                let port = uri.port_u16().unwrap_or_else(|| sch.default_port());
                let probe = Probe(sch, host, port);
                if let Some(p) = map.get(&probe as &dyn KeyParts) {
                    return Ok(Arc::clone(p));
                }
            }
        }
        let key = Key(OriginKey::from_uri(uri)?);
        if let Some(p) = map.get(&key as &dyn KeyParts) {
            return Ok(Arc::clone(p));
        }
        let created = OriginPool::new(key.0.clone(), Arc::clone(&self.shared));
        let p = map.get_or_insert(key, created);
        Ok(Arc::clone(p))
    }
}

/// The connection pool. Cheap to clone.
#[derive(Clone)]
pub struct Pool {
    inner: Arc<PoolInner>,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool").finish_non_exhaustive()
    }
}

impl Pool {
    /// Creates an empty pool.
    #[must_use]
    pub fn new(
        cfg: PoolConfig,
        connect: ConnectConfig,
        h1: H1Config,
        h2: H2Config,
        obs: Arc<dyn Observer>,
    ) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                map: papaya::HashMap::with_hasher(BuildHasherDefault::default()),
                shared: Arc::new(Shared {
                    cfg,
                    connect,
                    h1: Arc::new(h1),
                    h2,
                    obs,
                }),
            }),
        }
    }

    /// Sends `req` on a pooled connection (dialing if needed) and returns the response once its
    /// head is available. The returned body returns the connection to the pool when it completes.
    ///
    /// A request that fails on a reused HTTP/1 connection before any response byte (stale
    /// keep-alive race) or with `Recovery::Unprocessed` is retried once on a fresh connection if
    /// its body is replayable (`Empty`/`Full`) and the method is idempotent (or the error is
    /// `Unprocessed`).
    ///
    /// # Errors
    /// Any transport/protocol error, `PoolTimeout` after `checkout_timeout`, `InvalidRequest` for
    /// an unusable URI.
    pub fn send(
        &self,
        req: http::Request<Body>,
    ) -> impl Future<Output = Result<http::Response<Incoming>>> + Send + use<> {
        // HTTP/2 fastest path: borrow the origin and the connection from the map/snapshot guards (no
        // `Arc<OriginPool>` or handle clone) and hand the request straight to the actor.
        {
            let map = self.inner.map.pin();
            if let Some(op) = self.inner.lookup(&map, req.uri())
                && op.is_h2()
            {
                let snap = op.h2_snapshot();
                if let Some((handle, slot)) = op.h2_pick_in(&snap) {
                    return Either::Right(Either::Left(handle.send(slot, req)));
                }
            }
        }
        let op = match self.inner.origin_pool(req.uri()) {
            Ok(op) => op,
            Err(e) => return Either::Left(std::future::ready(Err(e))),
        };
        // HTTP/2 hot path: the lease is taken synchronously and the request is moved into the actor's
        // inbox immediately; the returned `ResponseFuture` is a small `'static` future (no Box).
        match op.try_fast(false) {
            Some(Lease::H2(handle, slot)) => Either::Right(Either::Left(handle.send(slot, req))),
            lease => Either::Right(Either::Right(send_on(op, req, lease))),
        }
    }

    /// Diagnostics for the origin of `uri`.
    #[must_use]
    pub fn stats(&self, uri: &http::Uri) -> Option<OriginStats> {
        let p = self.inner.origin_pool(uri).ok()?;
        Some(p.stats())
    }
}

#[allow(
    clippy::manual_async_fn,
    reason = "async fn would store `req` twice (future size budget)"
)]
/// General path (HTTP/1, slow acquisition, retries). An `async move` block rather than an
/// `async fn` so `req` is stored once in the future (async-fn parameters are copied into locals).
fn send_on(
    op: Arc<OriginPool>,
    mut req: http::Request<Body>,
    mut lease: Option<Lease>,
) -> impl Future<Output = Result<http::Response<Incoming>>> + Send {
    async move {
        let replayable = !matches!(req.body(), Body::Stream(_));
        let idempotent = matier_transport::h1::is_idempotent(req.method());
        let mut fresh = false;
        let mut retried = false;
        loop {
            let l = match lease.take() {
                Some(l) => l,
                None => match op.try_fast(fresh) {
                    Some(l) => l,
                    None => op.acquire_slow(fresh).await?,
                },
            };
            let result = match l {
                Lease::H1(conn) => {
                    conn.send_mut(&mut req, Arc::clone(&op) as Arc<dyn Release>)
                        .await
                }
                // HTTP/2 consumes the request, so it cannot be retried here (needs a clone).
                Lease::H2(handle, slot) => {
                    return handle.send(slot, req).await;
                }
            };
            match result {
                Ok(resp) => return Ok(resp),
                Err(e) if !retried && replayable => {
                    let stale = matier_transport::h1::is_stale_keepalive(&e);
                    let unprocessed = e.recovery() == matier_core::Recovery::Unprocessed;
                    if unprocessed || (stale && idempotent) {
                        retried = true;
                        fresh = stale;
                        continue;
                    }
                    return Err(e);
                }
                Err(e) => return Err(e),
            }
        }
    }
}

pub(crate) fn dial_error(kind: ErrorKind, msg: String) -> Error {
    Error::with_source(kind, matier_core::Recovery::Unprocessed, msg)
}

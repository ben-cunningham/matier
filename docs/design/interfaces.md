# Matier — crate interfaces (v0 contract)

This is the contract the implementation agents build against. The rationale is in [../PLAN.md](../PLAN.md).
Names may be refined during implementation, but changes to cross-crate types must be reflected here.

```
matier (client, layers) ──► matier-pool ──► matier-transport ──► matier-h1 / matier-h2 (sans-IO)
        └──────────────────────────┴──────────────┴──► matier-core (shared types)
```

## matier-core

```rust
// Re-exports so downstream crates and users depend on one place.
pub use layered::Service;                        // fn execute(&self, In) -> impl Future<Output = Out> + Send
pub use bytesbuf::{BytesView, BytesBuf};         // pinned exact version in workspace

pub mod buf {
    /// Body chunk type everywhere.
    pub type Chunk = bytesbuf::BytesView;
    /// Per-connection memory: pooled 1/4/16/64 KiB blocks.
    pub fn memory() -> impl bytesbuf::mem::Memory;   // thin wrapper over a per-thread GlobalPool
    pub fn from_bytes(b: bytes::Bytes) -> Chunk;      // zero-copy
    pub fn from_static(b: &'static [u8]) -> Chunk;
}

/// Request body.
pub enum Body {
    Empty,
    Full(BytesView),             // fast path: cloneable (for retries), sent as iovecs, zero-copy
    Stream(BodyStream),          // from Body::channel()
}
impl Body {
    pub fn empty() -> Self; pub fn full(impl Into<BytesView>) -> Self; pub fn from_static(&'static [u8]) -> Self;
    pub fn channel(buffer: usize) -> (BodySender, Body);   // bounded SPSC; backpressure = credit
    pub fn size_hint(&self) -> Option<u64>;                // exact length when known
    pub fn try_clone(&self) -> Option<Body>;               // Some for Empty/Full
}
impl http_body::Body for Body { type Data = BytesView; type Error = Error; }

pub struct BodySender { /* .. */ }
impl BodySender {
    pub async fn send(&mut self, chunk: BytesView) -> Result<(), Error>;
    pub fn finish(self);                                    // end of stream
    pub fn abort(self, err: Error);
}

/// One error type with a recovery classification (from fetch/seatbelt's RecoveryInfo idea).
pub struct Error { kind: ErrorKind, recovery: Recovery, source: Option<Box<dyn std::error::Error + Send + Sync>> }
pub enum ErrorKind { Connect, Tls, Dns, Timeout, Protocol, Io, Canceled, BodyWrite, BodyRead, PoolTimeout, InvalidRequest, Closed }
pub enum Recovery {
    Never,
    Retryable,              // may have been processed; retry only idempotent requests
    Unprocessed,            // guaranteed not processed (GOAWAY above last_stream_id, REFUSED_STREAM, connect failure): always safe
    Unavailable(Option<std::time::Duration>),
}
impl Error { pub fn kind(&self) -> ErrorKind; pub fn recovery(&self) -> Recovery; pub fn label(&self) -> &'static str /* for error.type */; }
pub type Result<T> = std::result::Result<T, Error>;

/// Connection identity. Precomputed telemetry attributes hang off the pool entry for this key.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct OriginKey { pub scheme: Scheme, pub host: Arc<str>, pub port: u16 }
pub enum Scheme { Http, Https }
impl OriginKey { pub fn from_uri(&http::Uri) -> Result<Self>; }

pub enum Protocol { Http1, Http2 }

/// Instrumentation hooks. All methods default to no-ops; `matier-otel` implements them.
/// Called on hot paths: implementations must not block or allocate per call where avoidable.
pub trait Observer: Send + Sync + 'static {
    fn enabled(&self) -> bool { false }                         // callers skip timestamping when false
    fn request_queued(&self, _o: &OriginKey, _wait: Duration) {}  // pool checkout + stream-slot wait
    fn request_dispatched(&self, _o: &OriginKey, _delay: Duration) {} // H2: enqueue → HEADERS written
    fn connect_phase(&self, _o: &OriginKey, _phase: ConnectPhase, _d: Duration, _ok: bool) {}
    fn connection_opened(&self, _o: &OriginKey, _p: Protocol) {}
    fn connection_closed(&self, _o: &OriginKey, _p: Protocol, _lifetime: Duration) {}
    fn write_batch(&self, _o: &OriginKey, _bytes: usize, _iovecs: usize) {}
    fn bytes_read(&self, _o: &OriginKey, _n: usize) {}
    fn actor_tick(&self, _o: &OriginKey, _busy: Duration, _queue_depth: usize) {}
    fn flow_control_stall(&self, _o: &OriginKey, _d: Duration) {}
    fn h2_goaway(&self, _o: &OriginKey, _code: u32) {}
    fn h2_rst_stream(&self, _o: &OriginKey, _code: u32, _sent: bool) {}
    fn pool_waiters(&self, _o: &OriginKey, _n: usize) {}
}
pub enum ConnectPhase { Dns, Tcp, Tls }
pub struct NoopObserver;   // the default
```

Per-request options (in `http::Extensions`): `ResponseTimeout(Duration)`, `BodyTimeout(Duration)`,
`Priority { urgency: u8, incremental: bool }` (RFC 9218), `TelemetryAttributes(Vec<KeyValueLike>)` (defined in matier-otel),
`UriTemplate(&'static str)`. Response-side: `Incoming::connection_info() -> Option<ConnectionInfo { protocol, reused: bool, conn_id: u64 }>` (always); the same value is inserted into `http::Extensions` only when `connection_info` is enabled on `H1Config`/`H2Config`/`ClientBuilder` (off by default: three allocations per response; the otel layers read the extension).

### Implementation notes (matier-core, as built)

Deviations from the sketch above:
- `bytesbuf` is pinned `=0.14.0` with feature `bytes-compat` (gives `BytesView: bytes::Buf` incl. `chunks_vectored`, `BytesBuf: BufMut`). `layered` pinned `=0.3.9`. No `Chunk` newtype is needed: `buf::Chunk = BytesView`.
- `buf::memory()` returns the concrete `GlobalPool` (a clone of a `thread_local!` pool), not `impl Memory`. Extra facade fns: `copy_from_slice`, `to_bytes`, `io_slices(&view, &mut [IoSlice]) -> usize`.
- `buf::from_static` goes through `Bytes::from_static` (bytesbuf has no `From<&'static [u8]>`): zero-copy but one small allocation, like `from_bytes`.
- `Body::full` maps an empty view to `Body::Empty`. `Body: Default (Empty)`. `Body` is ~272 bytes because `BytesView` inlines 8 spans (clippy `large_enum_variant` allowed); pass `Request<Body>` by move, not by clone.
- `Body::channel` uses `tokio::sync::mpsc` (v0). `BodySender::send` skips empty chunks and returns `Canceled` if the receiver is gone. Dropping a `BodySender` without `finish()` yields a `Canceled` error to the reader (a dying producer can never look like a complete body). `abort` delivers already-queued chunks first, then the error. Added `BodySender::is_closed`.
- `Error` has a private layout; constructors: `new(kind, recovery)` (no allocation), `with_source`, `connect`, `dns`, `tls` (Never), `io` (Retryable), `protocol` (Never), `invalid_request`, `timeout`, `canceled`, `closed`, `pool_timeout`, `unprocessed(kind, src)`, `retryable(kind, src)`, `unavailable(kind, retry_after)`, `with_recovery`. `Display` prints one level only; walk `source()`. `ErrorKind::label()` is also public. Labels: `connect tls dns timeout protocol io canceled body_write body_read pool_timeout invalid_request closed`.
- `OriginKey.host` is lowercased and keeps IPv6 brackets (`[::1]`) so it can go straight into a `Host` header; use `connect_host()` (brackets stripped) for DNS/SNI. Also `is_tls()`, `Display` (`https://host:443`). `Scheme::{default_port, as_str}`, `Protocol`, `ConnectPhase::as_str`.
- Extension types: `ResponseTimeout`, `BodyTimeout`, `Priority::{new, default}` (default urgency 3), `UriTemplate`, `ConnectionInfo`. `TelemetryAttributes` lives in matier-otel.


## matier-transport

```rust
pub struct ConnectConfig {
    pub connect_timeout: Duration, pub nodelay: bool /* true */, pub keepalive: Option<Duration>,
    pub tls: Option<TlsConfig>, pub http2_prior_knowledge: bool, pub alpn_h2: bool /* true */,
}
pub struct TlsConfig { pub rustls: Arc<rustls::ClientConfig> }   // ALPN set by transport from config
impl TlsConfig {
    pub fn with_platform_verifier() -> Result<Self>;   // default: rustls-platform-verifier (Windows cert store, macOS Keychain, Linux roots)
    pub fn with_webpki_roots() -> Self;                 // feature `webpki-roots`
    pub fn with_root_pem(pem: &[u8]) -> Result<Self>;
}
// Portability: everything in this crate must build for x86_64-pc-windows-msvc. Use socket2 for socket options;
// no libc outside cfg(unix). Vectored writes go through tokio's poll_write_vectored (WSASend on Windows).

/// TCP or TLS stream. Implements AsyncRead + AsyncWrite with real poll_write_vectored (is_write_vectored = true).
pub enum Io { Tcp(TcpStream), Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>) }

pub struct Connected { pub io: Io, pub protocol: Protocol /* from ALPN or prior knowledge */ }
pub async fn connect(origin: &OriginKey, cfg: &ConnectConfig, obs: &dyn Observer) -> Result<Connected>;
// DNS: tokio::net::lookup_host for v0 (Resolver trait + Happy Eyeballs in Phase 4).

/// Response body produced by either protocol. Enum, not a Box<dyn>, to avoid an allocation per response.
pub enum Incoming { Empty, H1(h1::RecvBody), H2(h2::RecvStream) }
impl http_body::Body for Incoming { type Data = BytesView; type Error = Error; }

pub mod h1 {
    /// Caller-driven HTTP/1.1 connection: no background task, no channel. Owned by the request in flight.
    pub struct H1Conn { /* io, read buf (pooled), write buf (reused), id, created_at, last_used */ }
    impl H1Conn {
        pub fn new(io: Io, origin: OriginKey, cfg: H1Config) -> Self;
        /// Writes head (+ body via writev), reads the response head. The returned body owns the connection and
        /// hands it to `on_release` when the body completes cleanly (keep-alive) — that's how it returns to the pool.
        pub async fn send(self, req: http::Request<Body>, on_release: impl ReleaseFn) -> Result<http::Response<Incoming>>;
        pub fn is_reusable_now(&mut self) -> bool;   // non-blocking liveness probe (no unexpected bytes / EOF)
    }
    pub trait ReleaseFn: FnOnce(H1Conn) + Send + 'static {}
}

pub mod h2 {
    pub struct H2Config { pub initial_stream_window: u32, pub initial_conn_window: u32, pub max_frame_size: u32,
                          pub max_header_list_size: u32, pub keepalive_interval: Option<Duration>, pub read_budget_frames: usize, ... }
    /// Spawns the connection actor (tokio::spawn) after the preface/SETTINGS exchange.
    pub async fn handshake(io: Io, origin: OriginKey, cfg: H2Config, obs: Arc<dyn Observer>) -> Result<H2Handle>;

    /// Cheap to clone. All state the pool needs is atomic — no lock to pick a connection.
    #[derive(Clone)] pub struct H2Handle { /* Arc<ConnShared> */ }
    impl H2Handle {
        pub fn in_flight(&self) -> usize;
        pub fn max_concurrent_streams(&self) -> usize;           // from peer SETTINGS
        pub fn is_accepting(&self) -> bool;                       // false after GOAWAY/close
        pub fn try_reserve(&self) -> Option<StreamSlot>;          // CAS on in_flight < max; slot released on drop
        pub async fn send(&self, slot: StreamSlot, req: http::Request<Body>) -> Result<http::Response<Incoming>>;
        pub fn closed(&self) -> impl Future<Output = ()>;         // resolves when the actor exits
    }
    pub struct RecvStream { /* Arc<StreamShared> */ }   // SPSC of BytesView + window release on consume
}
```

### Implementation notes (matier-transport connect/TLS, as built)

- Features: `aws-lc-rs` (default) or `ring` selects the rustls provider (`tls::default_provider()`); `webpki-roots` (off) enables `TlsConfig::with_webpki_roots`. matier-transport declares its own `rustls`/`tokio-rustls` deps (no provider features) instead of the workspace ones so `ring` is selectable.
- `TlsConfig` additionally holds a private http/1.1-only sibling config (shares session cache + verifier) used when `ConnectConfig::alpn_h2 == false`; build via the constructors or `TlsConfig::from_rustls(ClientConfig)` (overwrites ALPN). Default rustls session resumption (256-entry in-memory cache, tickets) and rustls' default PQ hybrid key exchange (aws-lc-rs only) are kept.
- `ConnectConfig: Default` (10 s timeout, nodelay, keepalive 60 s, no TLS). `connect_timeout` applies separately to DNS, each TCP attempt and the TLS handshake. `https` without `tls` -> `InvalidRequest`. IP-literal hosts skip DNS (no `Dns` phase reported).
- Error classification: DNS/TCP failures and timeouts -> `Unprocessed` (kinds `Dns` / `Connect` / `Timeout`); TLS handshake I/O failure -> `Unprocessed`/`Tls`; rustls-level failure (bad cert etc.) -> `Never`/`Tls`.
- `Io` also has `tcp() -> &TcpStream` and `alpn_protocol()`. `is_write_vectored()` is always true.
- `Incoming`, `h1::*` are implemented (see next section); `h2::*` is owned by the h2 actor work.

### Implementation notes (matier-transport h1 + `Incoming`, as built)

- `Incoming { Empty, H1(h1::RecvBody), H2(h2::RecvStream) }` implements `http_body::Body<Data = BytesView, Error = Error>` with `size_hint`/`is_end_stream` delegation (`Empty` is exact 0).
- `H1Conn` is a `Box<Inner>` (8 bytes to move). `H1Conn::new(io, origin, cfg)` / `new_shared(io, origin, Arc<H1Config>)`; accessors `id()`, `origin()`, `created_at()`, `last_used()`, `requests_sent()`; `is_reusable_now()` (noop-waker `poll_read`; tokio's cached readiness makes it syscall-free in the common case).
- Send API (deviation: three entry points, all returning `impl Future + Send`, none an `async fn`; see "future sizes" below):
  - `send(self, req, on_release: impl ReleaseFn)` (contract signature; boxes the closure),
  - `send_mut(self, &mut req, Arc<dyn Release>)`: borrows the request so that on failure with a replayable body (`Empty`/`Full`) the pool can send it again; allocation-free release (`Release::release(&self, H1Conn)` is implemented by the pool's `OriginPool`),
  - `Release::closed(&self, conn_id, lifetime)` + `H1Conn::set_owner(Weak<dyn Release>)`: called from `Drop` of the connection so the pool's connection count stays exact when a connection is dropped mid-body, on error, or on `Connection: close` (the pool cannot observe those otherwise).
- Write path: head encoded synchronously into the reused `Vec` before the first poll. `Full` bodies <= 1 KiB are appended to the head (one `write`); larger ones are written with one `poll_write_vectored` over `[head, body spans...]` (up to 16 iovecs per call, partial writes tracked by offset, body never copied or cloned). `Stream` bodies: head first, then per chunk `[chunk_header, spans, CRLF]` vectored (chunked) or raw spans (exact `Content-Length`); the streaming loop is `Box::pin`ned (cold path, keeps the hot future small). Response trailers and request-side `Expect: 100-continue` waiting are not implemented (1xx are skipped).
- Read path: a pooled `BytesBuf` (from `buf::memory()`, grabbed once per connection; read size adapts 4 KiB .. `H1Config::max_buffer`). The head is parsed from `rbuf.peek().first_slice()` (copied into a scratch `Vec` only if it spans blocks), then `consume(head_len)`. Bodies are `rbuf.consume(n)` (Length) or `consume(consumed)` + `range(data)` (chunked): zero-copy `BytesView`s of the read blocks.
- `RecvBody` releases the connection to `on_release` eagerly when the final byte is seen (Length hits 0 / chunked decoder `done`), only if keep-alive, no `must_close` and no extra buffered bytes. Dropping a body early closes the connection (v0; no drain of small remainders).
- `Empty`/`Length(0)` responses release before `send` returns and yield `Incoming::Empty`.
- Stale keep-alive: a write error, or EOF/reset before any response byte, on a *reused* connection is `Io`/`Retryable` with a `StaleKeepAlive` source; `h1::is_stale_keepalive(&Error)` detects it. `h1::is_idempotent(&Method)` is public.
- `H1Config { limits, title_case_headers, max_buffer }` (+ `Default`).
- **Future sizes**: `async fn` parameters are stored twice in the generated future (upvar + local), which doubled `Request<Body>` (504 B). Hot-path futures are therefore plain `fn`s returning combinators (`futures_util::future::Either`) or `async move` blocks that use captured values in place. Measured by `crates/matier/tests/future_sizes.rs` (budget 1024 B): `H1Conn::send_mut` 344, `H1Conn::send` 616, `Pool::send` / `Client::send` / `Service::execute` 928.



### Implementation notes (matier-transport h2 actor, as built)

- `H2Config` additionally has `keepalive_timeout`, `handshake_timeout`, `response_head_timeout` (30 s), `body_idle_timeout` (60 s), `header_block_timeout` (10 s), `coalesce_small_writes`, `connection_info` (off) and a default `keepalive_interval` of 30 s (see h2-actor.md "As built"). `H1Config` has `response_head_timeout` (30 s), `body_idle_timeout` (60 s), `connection_info`. `ResponseTimeout`/`BodyTimeout` request extensions override them. `handshake(io, origin, cfg, obs: Arc<dyn Observer>)` is as specified; internally generic over any `AsyncRead + AsyncWrite` for tests.
- `H2Handle::send(&self, slot, req) -> ResponseFuture` is a plain `fn` (not `async fn`): the request is moved into the actor inbox immediately and the returned `'static + Send` future is 16 bytes. Dropping it cancels the request. `ResponseFuture: Future<Output = Result<Response<Incoming>>>`.
- `H2Handle::slot_released(&self) -> tokio::sync::futures::Notified<'_>`: resolves when a stream slot is released, the peer raises MAX_CONCURRENT_STREAMS, or the connection closes. Waiters must create it and `enable()` it *before* calling `try_reserve()` (the pool does this per connection).
- Also `conn_id()`, `origin()`; `StreamSlot` releases its permit on drop or when the stream it was sent on ends; `H2Handle: Debug`. `RecvStream` is `http_body::Body<Data = BytesView, Error = Error>`; trailers arrive as a trailers frame.

## matier-pool

```rust
pub struct PoolConfig {
    pub max_idle_per_origin: usize, pub idle_timeout: Duration, pub max_conns_per_origin: usize,
    pub h2_target_streams_per_conn: usize /* default min(peer max, 128) */, pub h2_min_conns_per_origin: usize /* 1 */,
    pub checkout_timeout: Option<Duration>,
}
pub struct Pool { /* origin map: read-mostly concurrent map OriginKey -> Arc<OriginPool> */ }
impl Pool {
    pub fn new(cfg: PoolConfig, connect: ConnectConfig, h1: H1Config, h2: H2Config, obs: Arc<dyn Observer>) -> Self;
    pub async fn send(&self, req: http::Request<Body>) -> Result<http::Response<Incoming>>;
}
```
Behaviour (see PLAN §6):
- Protocol per origin is learned from the first connection and cached.
- **h1:** LIFO idle stack; O(1)-cancel waiter queue with direct handoff; checkout runs the liveness probe.
- **h2:** RCU snapshot of handles; power-of-two-choices on `in_flight` among handles below the target; `try_reserve`; otherwise a coalesced dial if below max, otherwise wait.
- A request that fails with `Recovery::Unprocessed` on a connection that turned out dead is retried **once** inside the pool, on a different connection.

### Implementation notes (matier-pool, as built)

- Origin map: `papaya::HashMap<Key, Arc<OriginPool>, FxHasher>` (lock-free reads, one insert per origin). Lookup is allocation-free (borrowed `(scheme, host, port)` key through a `dyn` `Borrow`) unless the host has upper-case letters.
- H1: LIFO `Vec` idle stack + slab-backed doubly-linked FIFO waiter list (O(1) enqueue/cancel/pop, ids guard against slab-key reuse) + connection count, all in one `parking_lot::Mutex` (deviation: `total` is a plain field in the mutex, not an `AtomicUsize`, so reserve/enqueue/pop are atomic with each other). The lock is never held across `.await` or while dropping a connection.
- Direct handoff: a released connection (or a freed slot, `Handoff::Dial`) goes to the oldest live waiter through a `oneshot`. A cancelled waiter (drop) unlinks itself and re-homes anything that raced into its channel. Connection slots are released in `Release::closed`, so every drop path (error, mid-body drop, over `max_idle`, expired, probe failure) is accounted.
- Checkout: idle pop -> idle-timeout check -> liveness probe; else reserve a slot and dial; else wait (optionally `checkout_timeout` -> `PoolTimeout`). The slow path is boxed so `Pool::send` stays small; the fast path allocates nothing.
- Protocol discovery: the first request to an unknown origin holds a per-origin async mutex while it dials; others wait for the verdict (no dial storm against an h2 server). Result cached in an `AtomicU8`.
- H2: `ArcSwap<Vec<Arc<Entry>>>` snapshot, power-of-two-choices on `in_flight()` among `is_accepting()` handles below `min(max_concurrent_streams, h2_target_streams_per_conn)`, `try_reserve`. If none: wait; `h2_maybe_dial` starts a background (spawned, cancellation-proof) dial when `live + dialing < max_conns` and `dialing < max(1, ceil(waiting / target))`. Dial failures are recorded (kind + message) and returned to the waiters that started waiting before the failure. A watcher task per handle (`handle.closed()`) rebuilds the snapshot and re-establishes `h2_min_conns_per_origin`. Because `H2Handle` has no slot-release notification, h2 waiters also poll every 500 us (TODO: replace by a notification from the actor).
- H2 requests are sent via a boxed future (`handle.send(slot, req)` borrows the handle and owns the request) and are **not** retried inside the pool: `H2Handle::send` consumes the request, so an `Unprocessed` retry needs the caller (or a retry layer) to clone it.
- Retry (H1): once, on a fresh connection, if the body is `Empty`/`Full` and (`Recovery::Unprocessed`, or stale-keepalive on an idempotent method).
- `PoolConfig` defaults: `max_idle_per_origin` 32, `idle_timeout` 90 s, `max_conns_per_origin` 256, `h2_target_streams_per_conn` 128, `h2_min_conns_per_origin` 1, `checkout_timeout` Some(30 s), `h2_zero_capacity_drain` 10 s. The HTTP/2 hot path borrows the origin and connection from the map/snapshot guards (no `Arc` clone of either). A peer that advertises `MAX_CONCURRENT_STREAMS = 0` never counts as capacity: at most one extra dial, retired after `h2_zero_capacity_drain`. `Pool::stats(&Uri) -> Option<OriginStats>` was added for diagnostics/tests. The observer gets `connection_opened/closed`, `request_queued` and `pool_waiters`.

## matier

```rust
#[derive(Clone)] pub struct Client { /* Arc<stack> */ }
impl Client { pub fn builder() -> ClientBuilder; pub fn new() -> Self;
              pub async fn send(&self, req: http::Request<Body>) -> Result<http::Response<Incoming>>; }
impl Service<http::Request<Body>> for Client { type Out = Result<http::Response<Incoming>>; }
pub struct ClientBuilder { /* pool, connect, h1, h2 configs; observer; layers */ }
```
v0 stack: `Dispatch` (validation, scheme policy) → `Pool`. Timeout, retry, metrics and decompression layers come in Phase 5.

### Implementation notes (matier, as built)

- `Client` (Clone; wraps `Pool`), `ClientBuilder { pool(), connect(), h1(), h2(), tls(), http2_prior_knowledge(), observer(), build() }`. `Client::send` is a plain `fn` returning `Either<Ready<Err>, Pool::send future>` (validation in `Dispatch` happens before the pool: absolute `http`/`https` URI with a host, else `InvalidRequest`). `impl Service<Request<Body>> for Client` delegates to `send`. Core types are re-exported.
- Bench subject `matier` (feature `matier`, default-on): `--pool-size` maps to `h2_min_conns_per_origin`; h1 benchmarks set `alpn_h2 = false`.


### matier-pool autoscaling (as built)
- `PoolConfig { h2_min_conns_per_origin: 1, h2_max_conns_per_origin: available_parallelism(), h2_target_streams_per_conn: 128 (hard cap), .. }`. The effective autoscaling ceiling is also capped at the tokio runtime's worker count (`scale::h2_ceiling`; an explicit `h2_min` above it wins): extra actors cannot run in parallel and only shrink each connection's write batches (1 KiB c=256 on 5 workers: 10 conns 350k req/s vs 5 conns 375k).
- `H2Handle::load() -> LoadSnapshot { busy_permille, dispatch_delay, inbox }`. Values older than 8 windows read as zero.
- `H2Handle::streams_opened()`.
- Thresholds and the pure decision functions are in `matier_pool::scale` (`ScaleThresholds`, `is_saturated`, `can_drain`).
- `Observer::pool_scaled(origin, conns, ScaleReason)`, where `ScaleReason` is `Saturated | Waiters | Min | Idle | Underused`. `matier-otel` records `matier.pool.scale` events.
- Bench: the `matier` subject is zero-config by default. `--matier-min-conns` / `--matier-max-conns` (matrix keys `matier_min_conns` / `matier_max_conns`) pin it, e.g. `matier_max_conns = 1` for single-connection runs.

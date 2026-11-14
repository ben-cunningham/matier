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
`UriTemplate(&'static str)`. Response-side extension: `ConnectionInfo { protocol, reused: bool, conn_id: u64 }`.

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

## matier

```rust
#[derive(Clone)] pub struct Client { /* Arc<stack> */ }
impl Client { pub fn builder() -> ClientBuilder; pub fn new() -> Self;
              pub async fn send(&self, req: http::Request<Body>) -> Result<http::Response<Incoming>>; }
impl Service<http::Request<Body>> for Client { type Out = Result<http::Response<Incoming>>; }
pub struct ClientBuilder { /* pool, connect, h1, h2 configs; observer; layers */ }
```
v0 stack: `Dispatch` (validation, scheme policy) → `Pool`. Timeout, retry, metrics and decompression layers come in Phase 5.

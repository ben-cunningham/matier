//! Matier: a high-performance HTTP/1.1 + HTTP/2 client for tokio.
//!
//! ```no_run
//! use http_body_util::BodyExt;
//! use matier::{Body, Client};
//!
//! # async fn demo() -> matier::Result<()> {
//! let client = Client::new();
//! let req = http::Request::get("http://127.0.0.1:8080/hello").body(Body::empty()).unwrap();
//! let resp = client.send(req).await?;
//! println!("status = {}", resp.status());
//! let mut body = resp.into_body();
//! let mut len = 0;
//! while let Some(frame) = body.frame().await {
//!     if let Some(data) = frame?.data_ref() {
//!         len += data.len(); // `BytesView` chunks are slices of the connection's read buffer
//!     }
//! }
//! println!("{len} body bytes");
//! # Ok(()) }
//! ```
//!
//! The v0 stack is `Dispatch` (request validation) followed by the connection [`Pool`]. Timeout,
//! retry, metrics and decompression layers come later; compose them with [`Service`].

mod dispatch;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::Either;
pub use matier_core::{
    Body, BodySender, BodyTimeout, BytesView, ConnectionInfo, Error, ErrorKind, NoopObserver,
    Observer, OriginKey, Priority, Protocol, Recovery, ResponseTimeout, Result, ScaleReason,
    Scheme, Service, UriTemplate,
};
pub use matier_pool::{OriginStats, Pool, PoolConfig};
pub use matier_transport::h1::H1Config;
pub use matier_transport::h2::H2Config;
pub use matier_transport::{ConnectConfig, Incoming, TlsConfig};

/// An HTTP client. Cheap to clone (shares one connection pool).
#[derive(Clone, Debug)]
pub struct Client {
    pool: Pool,
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl Client {
    /// A client with default settings. `https` trusts the operating system's roots (via the platform
    /// verifier); if that cannot be initialised, `https` requests fail with `InvalidRequest` until a
    /// [`TlsConfig`] is supplied through [`ClientBuilder::tls`].
    ///
    /// # Panics
    /// Never in practice; use [`Client::builder`] to handle configuration errors.
    #[must_use]
    pub fn new() -> Self {
        Self::builder()
            .build()
            .expect("default client configuration is valid")
    }

    /// Starts configuring a client.
    #[must_use]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Sends `req` and resolves with the response once its head is received. The body streams.
    ///
    /// The request URI must be absolute (`http`/`https` scheme and authority).
    pub fn send(
        &self,
        req: http::Request<Body>,
    ) -> impl Future<Output = Result<http::Response<Incoming>>> + Send + use<> {
        match dispatch::validate(&req) {
            Ok(()) => Either::Left(self.pool.send(req)),
            Err(e) => Either::Right(std::future::ready(Err(e))),
        }
    }

    /// The underlying pool (diagnostics).
    #[must_use]
    pub fn pool(&self) -> &Pool {
        &self.pool
    }
}

impl Service<http::Request<Body>> for Client {
    type Out = Result<http::Response<Incoming>>;

    fn execute(&self, input: http::Request<Body>) -> impl Future<Output = Self::Out> + Send {
        self.send(input)
    }
}

/// Builder for [`Client`].
///
/// Every setting has a method below. [`pool`](Self::pool), [`connect`](Self::connect), [`h1`](Self::h1) and
/// [`h2`](Self::h2) replace a whole section at once (including values set by earlier methods), so call them
/// first.
///
/// ```
/// use std::time::Duration;
///
/// let client = matier::Client::builder()
///     .connect_timeout(Duration::from_secs(5))
///     .max_connection_lifetime(Some(Duration::from_secs(300)))
///     .http2_keep_alive_interval(None)
///     .build()?;
/// # Ok::<(), matier::Error>(())
/// ```
pub struct ClientBuilder {
    pool: PoolConfig,
    connect: ConnectConfig,
    h1: H1Config,
    h2: H2Config,
    observer: Arc<dyn Observer>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            pool: PoolConfig::default(),
            connect: ConnectConfig::default(),
            h1: H1Config::default(),
            h2: H2Config::default(),
            observer: Arc::new(NoopObserver),
        }
    }
}

impl ClientBuilder {
    /// Pool limits and timeouts (replaces the whole section).
    #[must_use]
    pub fn pool(mut self, cfg: PoolConfig) -> Self {
        self.pool = cfg;
        self
    }

    /// TCP/TLS connect settings (replaces any earlier `tls`/`http2_prior_knowledge`).
    #[must_use]
    pub fn connect(mut self, cfg: ConnectConfig) -> Self {
        self.connect = cfg;
        self
    }

    /// HTTP/1 settings (replaces the whole section).
    #[must_use]
    pub fn h1(mut self, cfg: H1Config) -> Self {
        self.h1 = cfg;
        self
    }

    /// HTTP/2 settings (replaces the whole section).
    #[must_use]
    pub fn h2(mut self, cfg: H2Config) -> Self {
        self.h2 = cfg;
        self
    }

    /// TLS configuration for `https` origins (default: the platform verifier, see [`Client::new`]).
    #[must_use]
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.connect.tls = Some(tls);
        self
    }

    /// Speak HTTP/2 (h2c) on plaintext connections without upgrade.
    #[must_use]
    pub fn http2_prior_knowledge(mut self) -> Self {
        self.connect.http2_prior_knowledge = true;
        self
    }

    /// Attach [`ConnectionInfo`] to every response's `http::Extensions` (default off: three allocations per
    /// response). Prefer [`Incoming::connection_info`]. Turn this on when a layer that only sees
    /// `http::Response<B>` needs it, e.g. the OpenTelemetry metrics layer's `network.protocol.name`.
    #[must_use]
    pub fn connection_info(mut self, on: bool) -> Self {
        self.h1.connection_info = on;
        self.h2.connection_info = on;
        self
    }

    // ---- connections ----------------------------------------------------------------------------------------

    /// Timeout for DNS, for each TCP connect attempt and for the TLS handshake (default 10 s).
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect.connect_timeout = timeout;
        self
    }

    /// `TCP_NODELAY`, i.e. Nagle's algorithm off (default `true`). Matier writes each request in one call, so
    /// this rarely matters, but leaving Nagle on can delay small writes by an ACK round trip.
    #[must_use]
    pub fn tcp_nodelay(mut self, on: bool) -> Self {
        self.connect.nodelay = on;
        self
    }

    /// TCP keepalive idle time before the OS probes a silent connection (default 60 s; `None`: OS default,
    /// usually off).
    #[must_use]
    pub fn tcp_keepalive(mut self, idle: Option<Duration>) -> Self {
        self.connect.keepalive = idle;
        self
    }

    /// Socket send buffer size, `SO_SNDBUF` (default: the OS default, which auto-tunes).
    #[must_use]
    pub fn tcp_send_buffer_size(mut self, bytes: Option<u32>) -> Self {
        self.connect.send_buffer_size = bytes;
        self
    }

    /// Socket receive buffer size, `SO_RCVBUF` (default: the OS default, which auto-tunes).
    #[must_use]
    pub fn tcp_recv_buffer_size(mut self, bytes: Option<u32>) -> Self {
        self.connect.recv_buffer_size = bytes;
        self
    }

    /// Local IP address to connect from (default: chosen by the OS). Origins that resolve only to the other
    /// address family fail to connect.
    #[must_use]
    pub fn local_address(mut self, addr: Option<std::net::IpAddr>) -> Self {
        self.connect.local_address = addr;
        self
    }

    /// Only offer HTTP/1.1 in TLS ALPN, and use HTTP/1.1 for plaintext origins. A later
    /// [`http2_prior_knowledge`](Self::http2_prior_knowledge) turns h2c back on for plaintext origins.
    #[must_use]
    pub fn http1_only(mut self) -> Self {
        self.connect.alpn_h2 = false;
        self.connect.http2_prior_knowledge = false;
        self
    }

    // ---- pool ------------------------------------------------------------------------------------------------

    /// How long an unused connection is kept (default 90 s).
    #[must_use]
    pub fn pool_idle_timeout(mut self, timeout: Duration) -> Self {
        self.pool.idle_timeout = timeout;
        self
    }

    /// Idle HTTP/1.1 connections kept per origin (default 256).
    #[must_use]
    pub fn pool_max_idle_per_origin(mut self, n: usize) -> Self {
        self.pool.max_idle_per_origin = n;
        self
    }

    /// HTTP/1.1 connections per origin, in use and idle (default 256). Requests beyond it wait for a connection.
    #[must_use]
    pub fn http1_max_connections_per_origin(mut self, n: usize) -> Self {
        self.pool.max_conns_per_origin = n;
        self
    }

    /// Retire connections once they are this old (default `None`: no limit). Use it to follow DNS and
    /// load-balancer changes. See [`PoolConfig::max_connection_lifetime`].
    #[must_use]
    pub fn max_connection_lifetime(mut self, lifetime: Option<Duration>) -> Self {
        self.pool.max_connection_lifetime = lifetime;
        self
    }

    /// How long a request may wait for a connection or stream (default 30 s; `None` waits forever).
    #[must_use]
    pub fn pool_checkout_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.pool.checkout_timeout = timeout;
        self
    }

    // ---- timeouts ----------------------------------------------------------------------------------------------

    /// Time allowed for the response head, HTTP/1.1 and HTTP/2 (default 30 s; `None` disables). A
    /// [`ResponseTimeout`] request extension overrides it per request.
    #[must_use]
    pub fn response_head_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.h1.response_head_timeout = timeout;
        self.h2.response_head_timeout = timeout;
        self
    }

    /// Longest pause allowed between response body chunks (default 60 s; `None` disables). A [`BodyTimeout`]
    /// request extension overrides it per request.
    #[must_use]
    pub fn body_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.h1.body_idle_timeout = timeout;
        self.h2.body_idle_timeout = timeout;
        self
    }

    // ---- HTTP/1.1 ----------------------------------------------------------------------------------------------

    /// Largest response head (status line and headers) accepted (default 64 KiB).
    #[must_use]
    pub fn http1_max_response_head_size(mut self, bytes: usize) -> Self {
        self.h1.limits.max_head_bytes = bytes;
        self
    }

    /// Largest single socket read for HTTP/1.1 (default 64 KiB; the read size adapts up to it).
    #[must_use]
    pub fn http1_max_buf_size(mut self, bytes: usize) -> Self {
        self.h1.max_buffer = bytes;
        self
    }

    /// Accept obsolete folded (multi-line) response headers by unfolding them (default `false`: rejected).
    #[must_use]
    pub fn http1_allow_obsolete_multiline_headers_in_responses(mut self, on: bool) -> Self {
        self.h1.limits.allow_obs_fold = on;
        self
    }

    /// Write header names in title case, e.g. `Content-Type` (default `false`).
    #[must_use]
    pub fn http1_title_case_headers(mut self, on: bool) -> Self {
        self.h1.title_case_headers = on;
        self
    }

    // ---- HTTP/2 ------------------------------------------------------------------------------------------------

    /// Per-stream receive window (default 4 MiB). Caps one response's throughput at about window / RTT.
    #[must_use]
    pub fn http2_initial_stream_window_size(mut self, bytes: u32) -> Self {
        self.h2.initial_stream_window = bytes;
        self
    }

    /// Connection receive window, shared by all streams (default 16 MiB). It is a budget, not an allocation:
    /// a connection only holds this much when its consumers read slower than the server sends.
    #[must_use]
    pub fn http2_initial_connection_window_size(mut self, bytes: u32) -> Self {
        self.h2.initial_conn_window = bytes;
        self
    }

    /// Largest frame the server may send (default 256 KiB; the protocol allows 16 KiB to 16 MiB - 1).
    /// Larger frames mean fewer frames per response body and less CPU per byte. Some servers (nginx, Envoy)
    /// send small frames regardless.
    #[must_use]
    pub fn http2_max_frame_size(mut self, bytes: u32) -> Self {
        self.h2.max_frame_size = bytes;
        self
    }

    /// Largest response header list accepted (default 64 KiB).
    #[must_use]
    pub fn http2_max_header_list_size(mut self, bytes: u32) -> Self {
        self.h2.max_header_list_size = bytes;
        self
    }

    /// Send a PING after this much read inactivity, whether or not requests are open (default 30 s; `None`
    /// disables).
    #[must_use]
    pub fn http2_keep_alive_interval(mut self, interval: Option<Duration>) -> Self {
        self.h2.keepalive_interval = interval;
        self
    }

    /// Close the connection when a keepalive PING is not answered within this long (default 20 s).
    #[must_use]
    pub fn http2_keep_alive_timeout(mut self, timeout: Duration) -> Self {
        self.h2.keepalive_timeout = timeout;
        self
    }

    /// Client-side cap on streams per connection; requests beyond it use or open another connection (default
    /// 128, or the server's `MAX_CONCURRENT_STREAMS` when lower). Not sent to the server.
    #[must_use]
    pub fn http2_max_streams_per_connection(mut self, n: usize) -> Self {
        self.pool.h2_target_streams_per_conn = n;
        self
    }

    /// Bounds for HTTP/2 connections per origin. The pool scales between them by load (defaults: 1, and the
    /// lower of the CPU count and the tokio runtime's worker count). Equal values pin the count; a `min` above
    /// `max` wins.
    #[must_use]
    pub fn http2_connections_per_origin(mut self, min: usize, max: usize) -> Self {
        self.pool.h2_min_conns_per_origin = min;
        self.pool.h2_max_conns_per_origin = max;
        self
    }

    /// Batch requests submitted together into one write when many are in flight (default `true`). See
    /// [`H2Config::coalesce_requests`].
    #[must_use]
    pub fn http2_request_coalescing(mut self, on: bool) -> Self {
        self.h2.coalesce_requests = on;
        self
    }

    // ---- telemetry ---------------------------------------------------------------------------------------------

    /// Instrumentation hooks.
    #[must_use]
    pub fn observer(mut self, obs: Arc<dyn Observer>) -> Self {
        self.observer = obs;
        self
    }

    /// Builds the client.
    ///
    /// # Errors
    /// `InvalidRequest` for zero `max_conns_per_origin` / `h2_max_conns_per_origin`, or HTTP/2 settings out of
    /// protocol range.
    pub fn build(self) -> Result<Client> {
        self.h2.validate()?;
        if self.pool.max_conns_per_origin == 0 {
            return Err(Error::invalid_request("max_conns_per_origin must be > 0"));
        }
        if self.pool.h2_max_conns_per_origin == 0 {
            return Err(Error::invalid_request(
                "h2_max_conns_per_origin must be > 0",
            ));
        }
        let mut connect = self.connect;
        if connect.tls.is_none() {
            // Secure by default: trust the operating system's roots. If they cannot be loaded (e.g. a
            // minimal container) `https` requests fail with `InvalidRequest` until `tls(..)` is set.
            connect.tls = TlsConfig::with_platform_verifier().ok();
        }
        Ok(Client {
            pool: Pool::new(self.pool, connect, self.h1, self.h2, self.observer),
        })
    }
}

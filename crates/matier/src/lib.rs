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

use futures_util::future::Either;
pub use matier_core::{
    Body, BodySender, BytesView, ConnectionInfo, Error, ErrorKind, NoopObserver, Observer,
    OriginKey, Priority, Protocol, Recovery, Result, ScaleReason, Scheme, Service,
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
    /// A client with default settings (platform TLS roots are loaded lazily on first `https`).
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
    /// Pool limits and timeouts.
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

    /// HTTP/1 codec settings.
    #[must_use]
    pub fn h1(mut self, cfg: H1Config) -> Self {
        self.h1 = cfg;
        self
    }

    /// HTTP/2 settings.
    #[must_use]
    pub fn h2(mut self, cfg: H2Config) -> Self {
        self.h2 = cfg;
        self
    }

    /// TLS configuration for `https` origins.
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

    /// Instrumentation hooks.
    #[must_use]
    pub fn observer(mut self, obs: Arc<dyn Observer>) -> Self {
        self.observer = obs;
        self
    }

    /// Builds the client.
    ///
    /// # Errors
    /// `InvalidRequest` for inconsistent limits (zero `max_conns_per_origin` / `h2_max_conns_per_origin`).
    pub fn build(self) -> Result<Client> {
        if self.pool.max_conns_per_origin == 0 {
            return Err(Error::invalid_request("max_conns_per_origin must be > 0"));
        }
        if self.pool.h2_max_conns_per_origin == 0 {
            return Err(Error::invalid_request(
                "h2_max_conns_per_origin must be > 0",
            ));
        }
        Ok(Client {
            pool: Pool::new(self.pool, self.connect, self.h1, self.h2, self.observer),
        })
    }
}

//! Client abstraction: anything that can issue an HTTP request and fully drain the response.

use std::{future::Future, path::PathBuf};

use anyhow::{Result, bail};
use bytes::Bytes;

use crate::scenario::{PoolSelection, Protocol};

#[cfg(feature = "fetch")]
pub mod fetch_client;
#[cfg(feature = "hyper-legacy")]
pub mod hyper_legacy;
#[cfg(feature = "hyper-pooled")]
pub mod hyper_pooled;
#[cfg(feature = "matier")]
pub mod matier_client;
#[cfg(feature = "reqwest")]
pub mod reqwest_client;

/// A request to issue.
#[derive(Clone, Debug)]
pub struct BenchRequest {
    pub method: http::Method,
    pub uri: http::Uri,
    pub body: Bytes,
}

/// The outcome of a request whose body was fully consumed.
#[derive(Clone, Copy, Debug)]
pub struct BenchResponse {
    pub status: u16,
    pub body_len: u64,
}

/// A client under test.
///
/// Implementations MUST read the whole response body (counting bytes, not buffering when
/// avoidable) before the future resolves, so latency covers the full transfer.
pub trait Subject: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn request(&self, req: BenchRequest) -> impl Future<Output = Result<BenchResponse>> + Send;
}

/// Options shared by all subject factories.
#[derive(Clone, Debug)]
pub struct SubjectConfig {
    /// Subject name: `hyper-legacy`, `hyper-pooled`, `reqwest`, `fetch`, `fetch-standard`, `fetch-pooled`, `matier`.
    pub name: String,
    pub protocol: Protocol,
    pub tls: bool,
    /// PEM file with the server certificate to trust (required with `tls`).
    pub ca_cert: Option<PathBuf>,
    /// Number of clients for `hyper-pooled`.
    pub pool_size: usize,
    /// Pool selection for `fetch-pooled`.
    pub pool_selection: PoolSelection,
    /// `matier` only: explicit `h2_min_conns_per_origin`. `None` = the zero-config autoscaling pool.
    pub matier_min_conns: Option<usize>,
    /// `matier`: explicit `h2_max_conns_per_origin` (e.g. 1 to force a single HTTP/2 connection).
    pub matier_max_conns: Option<usize>,
}

/// Enum dispatch over the compiled-in subjects.
pub enum AnySubject {
    #[cfg(feature = "hyper-legacy")]
    HyperLegacy(Box<hyper_legacy::HyperLegacy>),
    #[cfg(feature = "hyper-pooled")]
    HyperPooled(hyper_pooled::HyperPooled),
    #[cfg(feature = "reqwest")]
    Reqwest(reqwest_client::ReqwestSubject),
    #[cfg(feature = "fetch")]
    Fetch(fetch_client::FetchSubject),
    #[cfg(feature = "matier")]
    Matier(matier_client::MatierSubject),
}

/// Builds the subject named by `cfg.name`. Must be called inside a tokio runtime.
pub fn build_subject(cfg: &SubjectConfig) -> Result<AnySubject> {
    Ok(match cfg.name.as_str() {
        #[cfg(feature = "hyper-legacy")]
        "hyper-legacy" => AnySubject::HyperLegacy(Box::new(hyper_legacy::HyperLegacy::new(cfg)?)),
        #[cfg(feature = "hyper-pooled")]
        "hyper-pooled" => AnySubject::HyperPooled(hyper_pooled::HyperPooled::new(cfg)?),
        #[cfg(feature = "reqwest")]
        "reqwest" => AnySubject::Reqwest(reqwest_client::ReqwestSubject::new(cfg)?),
        #[cfg(feature = "fetch")]
        "fetch" | "fetch-standard" | "fetch-pooled" => {
            AnySubject::Fetch(fetch_client::FetchSubject::new(cfg)?)
        }
        #[cfg(feature = "matier")]
        "matier" => AnySubject::Matier(matier_client::MatierSubject::new(cfg)?),
        other => bail!(
            "unknown or disabled subject {other:?} (available: {})",
            available().join(", ")
        ),
    })
}

/// Names of the subjects compiled in.
pub fn available() -> Vec<&'static str> {
    vec![
        #[cfg(feature = "hyper-legacy")]
        "hyper-legacy",
        #[cfg(feature = "hyper-pooled")]
        "hyper-pooled",
        #[cfg(feature = "reqwest")]
        "reqwest",
        #[cfg(feature = "fetch")]
        "fetch",
        #[cfg(feature = "fetch")]
        "fetch-standard",
        #[cfg(feature = "fetch")]
        "fetch-pooled",
        #[cfg(feature = "matier")]
        "matier",
    ]
}

impl Subject for AnySubject {
    fn name(&self) -> &str {
        match self {
            #[cfg(feature = "hyper-legacy")]
            Self::HyperLegacy(s) => s.name(),
            #[cfg(feature = "hyper-pooled")]
            Self::HyperPooled(s) => s.name(),
            #[cfg(feature = "reqwest")]
            Self::Reqwest(s) => s.name(),
            #[cfg(feature = "fetch")]
            Self::Fetch(s) => s.name(),
            #[cfg(feature = "matier")]
            Self::Matier(s) => s.name(),
        }
    }

    async fn request(&self, req: BenchRequest) -> Result<BenchResponse> {
        match self {
            #[cfg(feature = "hyper-legacy")]
            Self::HyperLegacy(s) => s.request(req).await,
            #[cfg(feature = "hyper-pooled")]
            Self::HyperPooled(s) => s.request(req).await,
            #[cfg(feature = "reqwest")]
            Self::Reqwest(s) => s.request(req).await,
            #[cfg(feature = "fetch")]
            Self::Fetch(s) => s.request(req).await,
            #[cfg(feature = "matier")]
            Self::Matier(s) => s.request(req).await,
        }
    }
}

/// HTTP/2 tuning applied identically to every subject so comparisons are fair.
pub const H2_STREAM_WINDOW: u32 = 1 << 20;
pub const H2_CONN_WINDOW: u32 = 16 << 20;
pub const H2_MAX_FRAME: u32 = 256 * 1024;

/// Reads a PEM bundle into DER certificates.
#[cfg(any(feature = "hyper-legacy", feature = "reqwest", feature = "fetch"))]
pub(crate) fn read_ca(cfg: &SubjectConfig) -> Result<Vec<u8>> {
    match &cfg.ca_cert {
        Some(p) => {
            Ok(std::fs::read(p).map_err(|e| anyhow::anyhow!("reading --ca-cert {p:?}: {e}"))?)
        }
        None => bail!("TLS requires --ca-cert (the server's self-signed certificate)"),
    }
}

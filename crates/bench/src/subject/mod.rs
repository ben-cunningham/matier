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
    /// Subject name: `hyper-legacy`, `hyper-pooled`, `reqwest`, `fetch`, `fetch-standard`, `fetch-pooled`,
    /// `fetch-matier`, `fetch-matier-standard`, `matier`.
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
        "fetch" | "fetch-standard" | "fetch-pooled" | "fetch-matier" | "fetch-matier-standard" => {
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
        #[cfg(feature = "fetch")]
        "fetch-matier",
        #[cfg(feature = "fetch")]
        "fetch-matier-standard",
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

/// HTTP/2 tuning for the hyper, reqwest and fetch subjects, equal to Matier's defaults (fetch accepts only the
/// stream window). Their own defaults use 16 KiB frames, which halve large-body throughput.
pub const H2_STREAM_WINDOW: u32 = 4 << 20;
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

/// Diagnostic per-request phase timing (`BENCH_PHASES=1`): send → response head, head → body end, and
/// data frames per response, averaged and printed every 100k requests. Off by default (one env read, cached).
pub mod phase {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    use std::time::Instant;

    static ON: OnceLock<bool> = OnceLock::new();
    static N: AtomicU64 = AtomicU64::new(0);
    static HEAD_NS: AtomicU64 = AtomicU64::new(0);
    static BODY_NS: AtomicU64 = AtomicU64::new(0);
    static FRAMES: AtomicU64 = AtomicU64::new(0);

    pub fn enabled() -> bool {
        *ON.get_or_init(|| std::env::var_os("BENCH_PHASES").is_some())
    }

    pub fn record(start: Instant, head: Instant, end: Instant, frames: u64) {
        HEAD_NS.fetch_add((head - start).as_nanos() as u64, Relaxed);
        BODY_NS.fetch_add((end - head).as_nanos() as u64, Relaxed);
        FRAMES.fetch_add(frames, Relaxed);
        let n = N.fetch_add(1, Relaxed) + 1;
        if n.is_multiple_of(100_000) {
            let (h, b, f) = (
                HEAD_NS.swap(0, Relaxed),
                BODY_NS.swap(0, Relaxed),
                FRAMES.swap(0, Relaxed),
            );
            eprintln!(
                "[phase] send->head {:>6.0}us  head->end {:>6.0}us  frames/resp {:.2}",
                h as f64 / 100_000.0 / 1e3,
                b as f64 / 100_000.0 / 1e3,
                f as f64 / 100_000.0
            );
        }
    }
}

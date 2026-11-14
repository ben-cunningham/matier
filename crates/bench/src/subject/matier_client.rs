//! `matier` subject: the Matier client (HTTP/1.1, h2c prior knowledge, or TLS + ALPN).
//!
//! The default is the zero-config pool: one h2 connection to start, load-based autoscaling up to
//! `available_parallelism()` and back down when idle. `--pool-size` is ignored here (it configures the
//! hyper/fetch "pooled" workarounds); only an explicit `--matier-min-conns N` pins
//! `h2_min_conns_per_origin`. For h1 the pool grows to the concurrency of the run.
//!
//! `MATIER_BENCH_TRACE_POOL=1` prints every pool scaling decision (time, connection count, reason) to stderr.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Result, anyhow};
use matier::{
    Body, Client, ConnectConfig, H1Config, H2Config, Observer, OriginKey, PoolConfig, ScaleReason,
    TlsConfig,
};

use super::{
    BenchRequest, BenchResponse, H2_CONN_WINDOW, H2_MAX_FRAME, H2_STREAM_WINDOW, Subject,
    SubjectConfig, read_ca,
};
use crate::scenario::Protocol;

/// Prints pool scaling events (`MATIER_BENCH_TRACE_POOL=1`).
struct PoolTrace(Instant);

static RD_N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static RD_B: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static WR_N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static WR_B: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
impl Observer for PoolTrace {
    fn enabled(&self) -> bool {
        true
    }
    fn bytes_read(&self, _o: &OriginKey, n: usize) {
        RD_N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        RD_B.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
    }
    fn write_batch(&self, _o: &OriginKey, bytes: usize, _i: usize) {
        WR_N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        WR_B.fetch_add(bytes as u64, std::sync::atomic::Ordering::Relaxed);
    }
    fn actor_tick(&self, _o: &OriginKey, _b: std::time::Duration, _q: usize) {
        static T: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let t = T.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if t.is_multiple_of(200_000) {
            let (rn, rb, wn, wb) = (
                RD_N.load(std::sync::atomic::Ordering::Relaxed),
                RD_B.load(std::sync::atomic::Ordering::Relaxed),
                WR_N.load(std::sync::atomic::Ordering::Relaxed),
                WR_B.load(std::sync::atomic::Ordering::Relaxed),
            );
            eprintln!(
                "[io] ticks={t} reads={rn} avg_read={} writes={wn} avg_write={}",
                rb / rn.max(1),
                wb / wn.max(1)
            );
        }
    }
    fn pool_scaled(&self, _o: &OriginKey, conns: usize, reason: ScaleReason) {
        eprintln!(
            "[pool] t={:>7.3}s conns={conns} reason={}",
            self.0.elapsed().as_secs_f64(),
            reason.as_str()
        );
    }
}

/// A Matier client (one pool).
pub struct MatierSubject {
    client: Client,
}

impl MatierSubject {
    pub fn new(cfg: &SubjectConfig) -> Result<Self> {
        let mut builder = Client::builder()
            .pool(PoolConfig {
                h2_min_conns_per_origin: cfg.matier_min_conns.unwrap_or(1).max(1),
                // h2_max_conns_per_origin stays at its default (available_parallelism); a pinned minimum
                // above it raises it so the pin is honoured.
                h2_max_conns_per_origin: cfg
                    .matier_max_conns
                    .unwrap_or(PoolConfig::default().h2_max_conns_per_origin)
                    .max(cfg.matier_min_conns.unwrap_or(1)),
                max_conns_per_origin: 4096,
                max_idle_per_origin: 4096,
                ..PoolConfig::default()
            })
            .connect(ConnectConfig {
                // h2 over TLS is negotiated through ALPN; h1 benchmarks must not offer h2.
                alpn_h2: cfg.protocol == Protocol::H2,
                http2_prior_knowledge: cfg.protocol == Protocol::H2 && !cfg.tls,
                ..ConnectConfig::default()
            })
            .h1(H1Config::default())
            .h2(H2Config {
                initial_stream_window: H2_STREAM_WINDOW,
                initial_conn_window: H2_CONN_WINDOW,
                max_frame_size: H2_MAX_FRAME,
                ..H2Config::default()
            });
        if std::env::var_os("MATIER_BENCH_TRACE_POOL").is_some() {
            builder = builder.observer(Arc::new(PoolTrace(Instant::now())));
        }
        if cfg.tls {
            let pem = read_ca(cfg)?;
            builder = builder.tls(TlsConfig::with_root_pem(&pem).map_err(|e| anyhow!("{e}"))?);
        }
        Ok(Self {
            client: builder.build().map_err(|e| anyhow!("{e}"))?,
        })
    }
}

impl Subject for MatierSubject {
    fn name(&self) -> &str {
        "matier"
    }

    async fn request(&self, req: BenchRequest) -> Result<BenchResponse> {
        let body = if req.body.is_empty() {
            Body::empty()
        } else {
            Body::from(req.body)
        };
        let req = http::Request::builder()
            .method(req.method)
            .uri(req.uri)
            .body(body)?;
        let resp = self.client.send(req).await.map_err(|e| anyhow!("{e:?}"))?;
        let status = resp.status().as_u16();
        let mut body = resp.into_body();
        let mut body_len = 0u64;
        while let Some(frame) = std::future::poll_fn(|cx| {
            http_body::Body::poll_frame(std::pin::Pin::new(&mut body), cx)
        })
        .await
        {
            if let Some(data) = frame.map_err(|e| anyhow!("{e:?}"))?.data_ref() {
                body_len += data.len() as u64;
            }
        }
        Ok(BenchResponse { status, body_len })
    }
}

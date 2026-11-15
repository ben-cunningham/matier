//! `matier` subject: the Matier client (HTTP/1.1, h2c prior knowledge, or TLS + ALPN).
//!
//! Matier runs with its library defaults (the other subjects get the tuned HTTP/2 settings in `mod.rs`).
//! The default is the zero-config pool: one h2 connection to start, load-based autoscaling up to
//! `available_parallelism()` and back down when idle. `--pool-size` is ignored here (it configures the
//! hyper/fetch "pooled" workarounds); only an explicit `--matier-min-conns N` pins
//! `h2_min_conns_per_origin`. For h1 the pool grows to the concurrency of the run.
//!
//! `MATIER_BENCH_NO_COALESCE=1` turns request coalescing off (`H2Config::coalesce_requests`) for A/B runs.
//! `MATIER_BENCH_TRACE_POOL=1` prints every pool scaling decision (time, connection count, reason) to stderr.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Result, anyhow};
use matier::{Body, Client, Observer, OriginKey, PoolConfig, ScaleReason, TlsConfig};

use super::{BenchRequest, BenchResponse, Subject, SubjectConfig, read_ca};
use crate::scenario::Protocol;

/// Prints pool scaling events (`MATIER_BENCH_TRACE_POOL=1`).
struct PoolTrace(Instant);

impl Observer for PoolTrace {
    fn enabled(&self) -> bool {
        true
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
        // Matier runs with its defaults; only the protocol choice and the optional connection pin are set.
        let mut builder = Client::builder();
        if cfg.protocol == Protocol::H1 {
            builder = builder.http1_only(); // h1 benchmarks must not negotiate h2
        } else if !cfg.tls {
            builder = builder.http2_prior_knowledge();
        }
        if cfg.matier_min_conns.is_some() || cfg.matier_max_conns.is_some() {
            let min = cfg.matier_min_conns.unwrap_or(1).max(1);
            let max = cfg
                .matier_max_conns
                .unwrap_or(PoolConfig::default().h2_max_conns_per_origin)
                .max(min);
            builder = builder.http2_connections_per_origin(min, max);
        }
        if std::env::var_os("MATIER_BENCH_NO_COALESCE").is_some() {
            builder = builder.http2_request_coalescing(false);
        }
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
        let phases = super::phase::enabled();
        let t0 = phases.then(std::time::Instant::now);
        let resp = self.client.send(req).await.map_err(|e| anyhow!("{e:?}"))?;
        let t_head = phases.then(std::time::Instant::now);
        let mut frames = 0u64;
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
                frames += 1;
            }
        }
        if let (Some(t0), Some(t_head)) = (t0, t_head) {
            super::phase::record(t0, t_head, std::time::Instant::now(), frames);
        }
        Ok(BenchResponse { status, body_len })
    }
}

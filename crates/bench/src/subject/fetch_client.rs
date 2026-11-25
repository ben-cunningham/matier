//! Microsoft oxidizer `fetch` subjects.
//!
//! - `fetch`: Minimal pipeline (Dispatch -> hyper transport only), single pool.
//! - `fetch-standard`: default Standard pipeline (timeouts, retry, breaker, metrics, logging).
//! - `fetch-pooled`: Minimal pipeline with `multiple_pools = (pool_size, selection)`.
//! - `fetch-matier`, `fetch-matier-standard`: the Minimal / Standard pipeline over the Matier transport
//!   (`matier-fetch`), with the same Matier client as the `matier` subject. fetch's transport tuning does not
//!   apply to it.
//!
//! Tuning that `fetch` exposes is matched to the other subjects (1 MiB initial h2 stream
//! window); the connection window and max frame size cannot be set through `fetch`.

use anyhow::Result;
use fetch::{
    HttpClient,
    options::{ConnectionPoolOptions, Http2Options, PoolSelection as FetchSelection},
    tls::TlsOptions,
};
use http_body_util::BodyExt;

use super::{BenchRequest, BenchResponse, H2_STREAM_WINDOW, Subject, SubjectConfig, read_ca};
use crate::scenario::{PoolSelection, Protocol};

/// Requests a pool absorbs before `saturating` selection moves on (fetch's documented default).
const SATURATING_REQUESTS_PER_CLIENT: u32 = 100;

pub struct FetchSubject {
    client: HttpClient,
    name: &'static str,
}

impl FetchSubject {
    pub fn new(cfg: &SubjectConfig) -> Result<Self> {
        let name = match cfg.name.as_str() {
            "fetch" => "fetch",
            "fetch-standard" => "fetch-standard",
            "fetch-matier" => "fetch-matier",
            "fetch-matier-standard" => "fetch-matier-standard",
            _ => "fetch-pooled",
        };
        if let Some(pipeline) = name.strip_prefix("fetch-matier") {
            let b = matier_fetch::builder(super::matier_client::build_client(cfg)?)
                .insecure_allow_http();
            let b = if pipeline.is_empty() {
                b.minimal_pipeline()
            } else {
                b
            };
            return Ok(Self {
                client: b.build(),
                name,
            });
        }
        let mut b = HttpClient::builder_tokio(fetch::tokio::TokioDeps::default())
            .insecure_allow_http() // fetch is HTTPS-only by default
            .supported_http_versions(&[match cfg.protocol {
                Protocol::H1 => fetch::Version::HTTP_11,
                Protocol::H2 => fetch::Version::HTTP_2,
            }])
            .http2_options(Http2Options::default().initial_stream_window_size(H2_STREAM_WINDOW));
        if name != "fetch-standard" {
            b = b.minimal_pipeline();
        }
        if name == "fetch-pooled" {
            anyhow::ensure!(cfg.pool_size > 0, "--pool-size must be > 0");
            let selection = match cfg.pool_selection {
                PoolSelection::RoundRobin => FetchSelection::round_robin(),
                PoolSelection::Saturating => {
                    FetchSelection::saturating(SATURATING_REQUESTS_PER_CLIENT)
                }
            };
            b = b.connection_pool_options(
                ConnectionPoolOptions::default().multiple_pools(cfg.pool_size, selection),
            );
        }
        if cfg.tls {
            let pem = read_ca(cfg)?;
            let mut tls = native_tls::TlsConnector::builder();
            tls.disable_built_in_roots(true)
                .request_alpns(match cfg.protocol {
                    Protocol::H1 => &["http/1.1"],
                    Protocol::H2 => &["h2", "http/1.1"],
                });
            let roots = native_tls::Certificate::stack_from_pem(&pem)?;
            anyhow::ensure!(!roots.is_empty(), "no certificates found in PEM");
            for root in roots {
                tls.add_root_certificate(root);
            }
            b = b.tls_options(TlsOptions::from(tls.build()?));
        }
        Ok(Self {
            client: b.build(),
            name,
        })
    }
}

impl Subject for FetchSubject {
    fn name(&self) -> &str {
        self.name
    }

    async fn request(&self, req: BenchRequest) -> Result<BenchResponse> {
        let mut rb = self.client.request(req.method, req.uri.to_string());
        if !req.body.is_empty() {
            rb = rb.bytes(req.body);
        }
        let resp = rb.fetch().await?;
        let status = resp.status().as_u16();
        let mut body = std::pin::pin!(resp.into_body());
        let mut body_len = 0;
        while let Some(frame) = body.frame().await {
            if let Some(data) = frame?.data_ref() {
                body_len += data.len() as u64;
            }
        }
        Ok(BenchResponse { status, body_len })
    }
}

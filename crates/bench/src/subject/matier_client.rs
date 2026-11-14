//! `matier` subject: the Matier client (HTTP/1.1, h2c prior knowledge, or TLS + ALPN).
//!
//! `--pool-size` maps to `h2_min_conns_per_origin` (connections dialed eagerly once the origin is
//! known to speak h2). For h1 it is ignored: the pool grows to the concurrency of the run.

use anyhow::{Result, anyhow};
use matier::{Body, Client, ConnectConfig, H1Config, H2Config, PoolConfig, TlsConfig};

use super::{
    BenchRequest, BenchResponse, H2_CONN_WINDOW, H2_MAX_FRAME, H2_STREAM_WINDOW, Subject,
    SubjectConfig, read_ca,
};
use crate::scenario::Protocol;

/// A Matier client (one pool).
pub struct MatierSubject {
    client: Client,
}

impl MatierSubject {
    pub fn new(cfg: &SubjectConfig) -> Result<Self> {
        let mut builder = Client::builder()
            .pool(PoolConfig {
                h2_min_conns_per_origin: cfg.pool_size.max(1),
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

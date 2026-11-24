//! `reqwest` subject (rustls; HTTP/1.1 only, or HTTP/2 via prior knowledge / ALPN).

use anyhow::Result;

use super::{
    BenchRequest, BenchResponse, H2_CONN_WINDOW, H2_MAX_FRAME, H2_STREAM_WINDOW, Subject,
    SubjectConfig, read_ca,
};
use crate::scenario::Protocol;

pub struct ReqwestSubject {
    client: reqwest::Client,
}

impl ReqwestSubject {
    pub fn new(cfg: &SubjectConfig) -> Result<Self> {
        // reqwest's rustls-no-provider mode requires an installed provider, even when only
        // Ring is compiled in. A different subject may already have initialized it.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut b = reqwest::Client::builder().pool_max_idle_per_host(usize::MAX);
        match cfg.protocol {
            Protocol::H1 => b = b.http1_only(),
            Protocol::H2 => {
                // Over TLS, h2 is negotiated via ALPN; over plain TCP use prior knowledge.
                if !cfg.tls {
                    b = b.http2_prior_knowledge();
                }
                b = b
                    .http2_initial_stream_window_size(H2_STREAM_WINDOW)
                    .http2_initial_connection_window_size(H2_CONN_WINDOW)
                    .http2_max_frame_size(H2_MAX_FRAME);
            }
        }
        if cfg.tls {
            b = b.add_root_certificate(reqwest::Certificate::from_pem(&read_ca(cfg)?)?);
        }
        Ok(Self { client: b.build()? })
    }
}

impl Subject for ReqwestSubject {
    fn name(&self) -> &str {
        "reqwest"
    }

    async fn request(&self, req: BenchRequest) -> Result<BenchResponse> {
        let mut rb = self.client.request(req.method, req.uri.to_string());
        if !req.body.is_empty() {
            rb = rb.body(req.body);
        }
        let mut resp = rb.send().await?;
        let status = resp.status().as_u16();
        let mut body_len = 0;
        while let Some(chunk) = resp.chunk().await? {
            body_len += chunk.len() as u64;
        }
        Ok(BenchResponse { status, body_len })
    }
}

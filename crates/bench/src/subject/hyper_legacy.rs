//! `hyper_util::client::legacy::Client` subject (HTTP/1.1, h2c prior knowledge, or TLS+ALPN).

use anyhow::Result;
use http_body_util::{BodyExt, Full};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioTimer},
};
use rustls::pki_types::{CertificateDer, pem::PemObject};

use super::{
    BenchRequest, BenchResponse, H2_CONN_WINDOW, H2_MAX_FRAME, H2_STREAM_WINDOW, Subject,
    SubjectConfig, read_ca,
};
use crate::scenario::Protocol;

/// A single hyper legacy client (one connection pool).
pub struct HyperLegacy {
    client: Client<HttpsConnector<HttpConnector>, Full<bytes::Bytes>>,
}

impl HyperLegacy {
    pub fn new(cfg: &SubjectConfig) -> Result<Self> {
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);

        let mut roots = rustls::RootCertStore::empty();
        if cfg.tls {
            let pem = read_ca(cfg)?;
            for cert in CertificateDer::pem_slice_iter(&pem) {
                roots.add(cert?)?;
            }
        }
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let builder = HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http();
        let connector = match cfg.protocol {
            Protocol::H1 => builder.enable_http1().wrap_connector(http),
            Protocol::H2 => builder.enable_http2().wrap_connector(http),
        };

        let mut b = Client::builder(TokioExecutor::new());
        b.pool_timer(TokioTimer::new());
        if cfg.protocol == Protocol::H2 {
            b.http2_only(true)
                .http2_initial_stream_window_size(H2_STREAM_WINDOW)
                .http2_initial_connection_window_size(H2_CONN_WINDOW)
                .http2_max_frame_size(H2_MAX_FRAME);
        }
        Ok(Self {
            client: b.build(connector),
        })
    }
}

impl Subject for HyperLegacy {
    fn name(&self) -> &str {
        "hyper-legacy"
    }

    async fn request(&self, req: BenchRequest) -> Result<BenchResponse> {
        let req = http::Request::builder()
            .method(req.method)
            .uri(req.uri)
            .body(Full::new(req.body))?;
        let resp = self.client.request(req).await?;
        let status = resp.status().as_u16();
        let mut body = resp.into_body();
        let mut body_len = 0;
        while let Some(frame) = body.frame().await {
            if let Some(data) = frame?.data_ref() {
                body_len += data.len() as u64;
            }
        }
        Ok(BenchResponse { status, body_len })
    }
}

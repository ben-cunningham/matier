//! `hyper_util::client::legacy::Client` subject (HTTP/1.1, h2c prior knowledge, or TLS+ALPN).
//!
//! `BENCH_TRACE_IO=1` counts socket reads/writes per connection and prints the totals when the connection
//! closes (read/write batching comparison; matier exposes the same counters through its `Observer`).

use anyhow::Result;
use http_body_util::{BodyExt, Full};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioIo, TokioTimer},
};
use rustls::pki_types::{CertificateDer, pem::PemObject};

use super::{
    BenchRequest, BenchResponse, H2_CONN_WINDOW, H2_MAX_FRAME, H2_STREAM_WINDOW, Subject,
    SubjectConfig, read_ca,
};
use crate::scenario::Protocol;

/// A single hyper legacy client (one connection pool).
pub struct HyperLegacy {
    client: Client<CountSvc<HttpsConnector<HttpConnector>>, Full<bytes::Bytes>>,
}

/// Connector wrapper that wraps every connection in [`CountIo`].
#[derive(Clone)]
pub struct CountSvc<S>(S);

impl<S> tower_service::Service<http::Uri> for CountSvc<S>
where
    S: tower_service::Service<http::Uri>,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = CountIo<S::Response>;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }
    fn call(&mut self, req: http::Uri) -> Self::Future {
        let f = self.0.call(req);
        Box::pin(async move { f.await.map(CountIo::new) })
    }
}

/// Counts socket reads/writes; prints the totals on drop when `BENCH_TRACE_IO` is set.
pub struct CountIo<T> {
    io: TokioIo<T>,
    reads: u64,
    read_bytes: u64,
    writes: u64,
    write_bytes: u64,
}

impl<T> CountIo<T> {
    fn new(io: T) -> Self {
        Self {
            io: TokioIo::new(io),
            reads: 0,
            read_bytes: 0,
            writes: 0,
            write_bytes: 0,
        }
    }
}

impl<T> Drop for CountIo<T> {
    fn drop(&mut self) {
        if std::env::var_os("BENCH_TRACE_IO").is_some() {
            eprintln!(
                "[hyper-io] reads={} avg_read={} writes={} avg_write={}",
                self.reads,
                self.read_bytes / self.reads.max(1),
                self.writes,
                self.write_bytes / self.writes.max(1)
            );
        }
    }
}

impl<T: hyper_util::client::legacy::connect::Connection>
    hyper_util::client::legacy::connect::Connection for CountIo<T>
{
    fn connected(&self) -> hyper_util::client::legacy::connect::Connected {
        self.io.inner().connected()
    }
}

impl<T: hyper::rt::Read + Unpin> hyper::rt::Read for CountIo<T> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // SAFETY: as in hyper_util::rt::TokioIo: the bytes `rb` reports as filled are initialized, and only
        // that many are advanced.
        let mut rb = tokio::io::ReadBuf::uninit(unsafe { buf.as_mut() });
        let r = tokio::io::AsyncRead::poll_read(std::pin::Pin::new(&mut self.io), cx, &mut rb);
        let n = rb.filled().len();
        if let std::task::Poll::Ready(Ok(())) = &r {
            self.reads += 1;
            self.read_bytes += n as u64;
            unsafe { buf.advance(n) };
        }
        r
    }
}

impl<T: hyper::rt::Write + Unpin> hyper::rt::Write for CountIo<T> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let r = tokio::io::AsyncWrite::poll_write(std::pin::Pin::new(&mut self.io), cx, buf);
        if let std::task::Poll::Ready(Ok(n)) = &r {
            self.writes += 1;
            self.write_bytes += *n as u64;
        }
        r
    }
    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let r =
            tokio::io::AsyncWrite::poll_write_vectored(std::pin::Pin::new(&mut self.io), cx, bufs);
        if let std::task::Poll::Ready(Ok(n)) = &r {
            self.writes += 1;
            self.write_bytes += *n as u64;
        }
        r
    }
    fn is_write_vectored(&self) -> bool {
        tokio::io::AsyncWrite::is_write_vectored(&self.io)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(std::pin::Pin::new(&mut self.io), cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(std::pin::Pin::new(&mut self.io), cx)
    }
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
            client: b.build(CountSvc(connector)),
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
        let phases = super::phase::enabled();
        let t0 = phases.then(std::time::Instant::now);
        let resp = self.client.request(req).await?;
        let t_head = phases.then(std::time::Instant::now);
        let mut frames = 0u64;
        let status = resp.status().as_u16();
        let mut body = resp.into_body();
        let mut body_len = 0;
        while let Some(frame) = body.frame().await {
            if let Some(data) = frame?.data_ref() {
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

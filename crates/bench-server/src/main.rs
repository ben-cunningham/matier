//! `matier-bench-server`: a small, fast hyper server that serves as the load target for
//! `matier-bench`. It speaks HTTP/1.1 and HTTP/2 (h2c with prior knowledge) on a plain
//! port, and HTTP/1.1 + HTTP/2 via ALPN on a TLS port using a self-signed certificate.
//!
//! Endpoints:
//! - `GET /bytes/{n}`: `n` bytes of a pre-allocated payload (zero-copy slice).
//! - `POST /echo`: reads the whole body, returns its length as text; `?mirror=1` streams it back.
//! - `GET /delay/{ms}`: sleeps `ms` milliseconds, then returns an empty 200.
//! - `GET /health`: `ok`.
//!
//! `BENCH_SERVER_TRACE_IO=1` prints, when each plain connection closes, how many socket reads/writes it did and
//! their average sizes (how well the client batches requests, and how the server batches responses).

use std::{convert::Infallible, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::{Method, Request, Response, StatusCode, body::Incoming, service::service_fn};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Plain-text listen address (HTTP/1.1 and h2c prior knowledge).
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: SocketAddr,
    /// TLS listen address (ALPN h2 / http/1.1). Pass `off` to disable.
    #[arg(long, default_value = "127.0.0.1:8443")]
    tls_addr: String,
    /// Tokio worker threads (default: half the CPUs, leaving the rest to the client).
    #[arg(long)]
    threads: Option<usize>,
    /// SETTINGS_MAX_CONCURRENT_STREAMS advertised to HTTP/2 clients.
    #[arg(long, default_value_t = 1000)]
    h2_max_concurrent_streams: u32,
    /// Initial HTTP/2 stream window in bytes.
    #[arg(long, default_value_t = 1 << 20)]
    h2_stream_window: u32,
    /// Initial HTTP/2 connection window in bytes.
    #[arg(long, default_value_t = 16 << 20)]
    h2_conn_window: u32,
    /// Largest `/bytes/{n}` payload in bytes.
    #[arg(long, default_value_t = 16 << 20)]
    max_payload: usize,
    /// Write the self-signed certificate (PEM) here so clients can trust it.
    #[arg(long)]
    cert_out: Option<PathBuf>,
}

type Body = BoxBody<Bytes, hyper::Error>;

fn full(b: impl Into<Bytes>) -> Body {
    Full::new(b.into())
        .map_err(|e: Infallible| match e {})
        .boxed()
}

enum Route {
    Health,
    Bytes(usize),
    Delay(u64),
    Echo { mirror: bool },
    NotFound,
}

fn route(req: &Request<Incoming>) -> Route {
    let path = req.uri().path();
    match *req.method() {
        Method::GET | Method::HEAD => {
            if path == "/health" {
                Route::Health
            } else if let Some(n) = path.strip_prefix("/bytes/").and_then(|n| n.parse().ok()) {
                Route::Bytes(n)
            } else if let Some(ms) = path.strip_prefix("/delay/").and_then(|n| n.parse().ok()) {
                Route::Delay(ms)
            } else {
                Route::NotFound
            }
        }
        Method::POST | Method::PUT if path == "/echo" => Route::Echo {
            mirror: req
                .uri()
                .query()
                .is_some_and(|q| q.split('&').any(|kv| kv == "mirror=1")),
        },
        _ => Route::NotFound,
    }
}

fn status(code: StatusCode) -> Response<Body> {
    Response::builder()
        .status(code)
        .body(full(Bytes::new()))
        .expect("static response")
}

async fn handle(payload: Bytes, req: Request<Incoming>) -> Result<Response<Body>, hyper::Error> {
    Ok(match route(&req) {
        Route::Health => Response::new(full("ok")),
        Route::Bytes(n) if n <= payload.len() => Response::new(full(payload.slice(..n))),
        Route::Bytes(_) => status(StatusCode::PAYLOAD_TOO_LARGE),
        Route::Delay(ms) => {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Response::new(full(Bytes::new()))
        }
        Route::Echo { mirror: true } => Response::new(req.into_body().boxed()),
        Route::Echo { mirror: false } => {
            let mut body = req.into_body();
            let mut len = 0u64;
            while let Some(frame) = body.frame().await {
                if let Ok(data) = frame?.into_data() {
                    len += data.len() as u64;
                }
            }
            Response::new(full(len.to_string()))
        }
        Route::NotFound => status(StatusCode::NOT_FOUND),
    })
}

/// Accepts connections forever, serving each on its own task.
async fn serve(
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    payload: Bytes,
    builder: Arc<auto::Builder<TokioExecutor>>,
) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                eprintln!("accept error: {e}");
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let (tls, payload, builder) = (tls.clone(), payload.clone(), builder.clone());
        tokio::spawn(async move {
            let svc = service_fn(move |req| handle(payload.clone(), req));
            // Connection-level errors (client hangups, resets) are expected under load.
            let _ = match tls {
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(s) => builder.serve_connection(TokioIo::new(s), svc).await,
                    Err(_) => return,
                },
                None if trace_io() => {
                    builder
                        .serve_connection(TokioIo::new(CountIo::new(stream)), svc)
                        .await
                }
                None => builder.serve_connection(TokioIo::new(stream), svc).await,
            };
        });
    }
}

fn trace_io() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| std::env::var_os("BENCH_SERVER_TRACE_IO").is_some())
}

/// Counts socket reads/writes (`BENCH_SERVER_TRACE_IO=1`); prints the totals when the connection closes.
struct CountIo<T> {
    io: T,
    reads: u64,
    read_bytes: u64,
    writes: u64,
    write_bytes: u64,
    opened: std::time::Instant,
}

impl<T> CountIo<T> {
    fn new(io: T) -> Self {
        Self {
            io,
            reads: 0,
            read_bytes: 0,
            writes: 0,
            write_bytes: 0,
            opened: std::time::Instant::now(),
        }
    }
}

impl<T> Drop for CountIo<T> {
    fn drop(&mut self) {
        eprintln!(
            "[server-io] conn {:.1}s reads={} avg_read={} writes={} avg_write={}",
            self.opened.elapsed().as_secs_f64(),
            self.reads,
            self.read_bytes / self.reads.max(1),
            self.writes,
            self.write_bytes / self.writes.max(1)
        );
    }
}

impl<T: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for CountIo<T> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let r = std::pin::Pin::new(&mut self.io).poll_read(cx, buf);
        if let std::task::Poll::Ready(Ok(())) = &r {
            self.reads += 1;
            self.read_bytes += (buf.filled().len() - before) as u64;
        }
        r
    }
}

impl<T: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for CountIo<T> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let r = std::pin::Pin::new(&mut self.io).poll_write(cx, buf);
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
        let r = std::pin::Pin::new(&mut self.io).poll_write_vectored(cx, bufs);
        if let std::task::Poll::Ready(Ok(n)) = &r {
            self.writes += 1;
            self.write_bytes += *n as u64;
        }
        r
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

fn tls_acceptor(cert_out: Option<&PathBuf>) -> Result<TlsAcceptor> {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])?;
    if let Some(path) = cert_out {
        std::fs::write(path, ck.cert.pem()).with_context(|| format!("writing {path:?}"))?;
    }
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(ck.signing_key.serialize_der()));
    let mut cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![ck.cert.der().clone()], key)?;
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(cfg)))
}

fn main() -> Result<()> {
    let args = Args::parse();
    let threads = args.threads.unwrap_or_else(|| {
        std::thread::available_parallelism().map_or(1, |n| (n.get() / 2).max(1))
    });
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?;
    rt.block_on(async {
        let mut builder = auto::Builder::new(TokioExecutor::new());
        builder
            .http2()
            .max_concurrent_streams(args.h2_max_concurrent_streams)
            .initial_stream_window_size(args.h2_stream_window)
            .initial_connection_window_size(args.h2_conn_window)
            .max_frame_size(256 * 1024);
        let builder = Arc::new(builder);
        let payload = Bytes::from(vec![b'x'; args.max_payload]);

        let plain = TcpListener::bind(args.addr).await.context("bind --addr")?;
        eprintln!(
            "matier-bench-server: http  on {} ({threads} threads)",
            args.addr
        );
        let mut tasks = vec![tokio::spawn(serve(
            plain,
            None,
            payload.clone(),
            builder.clone(),
        ))];

        if args.tls_addr != "off" {
            let acceptor = tls_acceptor(args.cert_out.as_ref())?;
            let listener = TcpListener::bind(&args.tls_addr)
                .await
                .context("bind --tls-addr")?;
            eprintln!("matier-bench-server: https on {}", args.tls_addr);
            tasks.push(tokio::spawn(serve(
                listener,
                Some(acceptor),
                payload,
                builder,
            )));
        }
        for t in tasks {
            t.await?;
        }
        Ok(())
    })
}

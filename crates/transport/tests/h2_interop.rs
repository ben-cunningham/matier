//! Interop: the matier HTTP/2 actor against a real hyper server (h2c prior knowledge and TLS ALPN).

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming as HyperIncoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use matier_core::{Body, BytesView, NoopObserver, OriginKey, Protocol, Scheme};
use matier_transport::h2::{H2Config, H2Handle, handshake};
use matier_transport::{ConnectConfig, Incoming, TlsConfig, connect};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

async fn handle(
    req: hyper::Request<HyperIncoming>,
) -> Result<hyper::Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_owned();
    if let Some(n) = path.strip_prefix("/size/") {
        let n: usize = n.parse().unwrap();
        return Ok(hyper::Response::new(Full::new(Bytes::from(vec![
            0x5au8;
            n
        ]))));
    }
    let body = req.into_body().collect().await.unwrap().to_bytes();
    let sum: u64 = body.iter().map(|&b| u64::from(b)).sum();
    Ok(hyper::Response::new(Full::new(Bytes::from(format!(
        "{} {}",
        body.len(),
        sum
    )))))
}

async fn serve<S>(io: S)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
        .initial_stream_window_size(1 << 20)
        .initial_connection_window_size(4 << 20)
        .serve_connection(TokioIo::new(io), service_fn(handle))
        .await;
}

async fn h2c_server() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((s, _)) = l.accept().await else { return };
            s.set_nodelay(true).unwrap();
            tokio::spawn(serve(s));
        }
    });
    port
}

async fn tls_server() -> (u16, String) {
    let provider = matier_transport::tls::default_provider();
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let pem = ck.cert.pem();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(ck.signing_key.serialize_der()));
    let mut cfg = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![ck.cert.der().clone()], key)
        .unwrap();
    cfg.alpn_protocols = vec![b"h2".to_vec()];
    let acceptor = TlsAcceptor::from(Arc::new(cfg));
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((s, _)) = l.accept().await else { return };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Ok(s) = acceptor.accept(s).await {
                    serve(s).await;
                }
            });
        }
    });
    (port, pem)
}

async fn dial(origin: OriginKey, cfg: ConnectConfig) -> H2Handle {
    let c = connect(&origin, &cfg, &NoopObserver).await.unwrap();
    assert_eq!(c.protocol, Protocol::Http2);
    handshake(c.io, origin, H2Config::default(), Arc::new(NoopObserver))
        .await
        .unwrap()
}

async fn reserve(h: &H2Handle) -> matier_transport::h2::StreamSlot {
    loop {
        let n = h.slot_released();
        tokio::pin!(n);
        n.as_mut().enable();
        if let Some(s) = h.try_reserve() {
            return s;
        }
        n.await;
    }
}

async fn read_all(resp: http::Response<Incoming>) -> Vec<u8> {
    resp.into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec()
}

async fn exercise(h: H2Handle, origin: OriginKey) {
    let base = origin.to_string();
    let base = base.trim_end_matches('/').to_owned();
    // 10k requests at concurrency 256 (mix of GET and small POST).
    let sem = Arc::new(tokio::sync::Semaphore::new(
        std::env::var("CONC")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(256),
    ));
    let mut tasks = Vec::new();
    for i in 0..std::env::var("N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000usize)
    {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let h = h.clone();
        let base = base.clone();
        tasks.push(tokio::spawn(async move {
            let _p = permit;
            let slot = reserve(&h).await;
            let (req, expect): (http::Request<Body>, Vec<u8>) = if i % 5 == 0 {
                let payload = vec![(i % 251) as u8; 100 + i % 3000];
                let sum: u64 = payload.iter().map(|&b| u64::from(b)).sum();
                let exp = format!("{} {}", payload.len(), sum).into_bytes();
                (
                    http::Request::post(format!("{base}/echo"))
                        .body(Body::full(matier_core::buf::copy_from_slice(&payload)))
                        .unwrap(),
                    exp,
                )
            } else {
                let n = i % 5000;
                (
                    http::Request::get(format!("{base}/size/{n}"))
                        .body(Body::empty())
                        .unwrap(),
                    vec![0x5a; n],
                )
            };
            let resp = h.send(slot, req).await.unwrap();
            assert_eq!(resp.status(), 200);
            assert_eq!(read_all(resp).await, expect, "request {i}");
        }));
    }
    for (idx, t) in tasks.into_iter().enumerate() {
        if tokio::time::timeout(Duration::from_secs(8), t)
            .await
            .is_err()
        {
            panic!(
                "stuck at task {idx}: in_flight={} accepting={} max={}",
                h.in_flight(),
                h.is_accepting(),
                h.max_concurrent_streams()
            );
        }
    }
    assert_eq!(h.in_flight(), 0);

    // Large download (8 MiB), large Full upload (8 MiB), large streaming upload (6 MiB).
    let resp = h
        .send(
            reserve(&h).await,
            http::Request::get(format!("{base}/size/{}", 8 << 20))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = read_all(resp).await;
    assert_eq!(body.len(), 8 << 20);
    assert!(body.iter().all(|&b| b == 0x5a));

    let big = vec![3u8; 8 << 20];
    let resp = h
        .send(
            reserve(&h).await,
            http::Request::post(format!("{base}/echo"))
                .body(Body::full(matier_core::buf::copy_from_slice(&big)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        read_all(resp).await,
        format!("{} {}", 8 << 20, 3u64 * (8 << 20)).into_bytes()
    );

    let (mut tx, body) = Body::channel(4);
    let fut = h.send(
        reserve(&h).await,
        http::Request::post(format!("{base}/echo"))
            .body(body)
            .unwrap(),
    );
    let producer = tokio::spawn(async move {
        let chunk: BytesView = matier_core::buf::copy_from_slice(&vec![1u8; 64 * 1024]);
        for _ in 0..96 {
            tx.send(chunk.clone()).await.unwrap();
        }
        tx.finish();
    });
    let resp = tokio::time::timeout(Duration::from_secs(30), fut)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        read_all(resp).await,
        format!("{} {}", 96 * 64 * 1024, 96u64 * 64 * 1024).into_bytes()
    );
    producer.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2c_prior_knowledge_against_hyper() {
    let port = h2c_server().await;
    let origin = OriginKey {
        scheme: Scheme::Http,
        host: "127.0.0.1".into(),
        port,
    };
    let h = dial(
        origin.clone(),
        ConnectConfig {
            http2_prior_knowledge: true,
            ..Default::default()
        },
    )
    .await;
    exercise(h, origin).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_alpn_against_hyper() {
    let (port, pem) = tls_server().await;
    let origin = OriginKey {
        scheme: Scheme::Https,
        host: "localhost".into(),
        port,
    };
    let cfg = ConnectConfig {
        tls: Some(TlsConfig::with_root_pem(pem.as_bytes()).unwrap()),
        ..Default::default()
    };
    let h = dial(origin.clone(), cfg).await;
    exercise(h, origin).await;
}

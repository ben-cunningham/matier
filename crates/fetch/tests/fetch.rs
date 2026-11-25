use std::convert::Infallible;

use bytes::Bytes;
use bytesbuf::BytesView;
use bytesbuf::mem::{GlobalPool, OpaqueMemory};
use fetch::{HttpBodyBuilder, HttpClient, Recovery, RecoveryInfo};
use http_body_util::{BodyExt, Full};
use http_extensions::HttpBodyOptions;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tick::Clock;
use tokio::net::TcpListener;

/// Echoes the request body; `GET /bytes/N` returns N bytes.
async fn handle(req: hyper::Request<Incoming>) -> Result<hyper::Response<Full<Bytes>>, Infallible> {
    let body = if let Some(n) = req.uri().path().strip_prefix("/bytes/") {
        Bytes::from(vec![b'x'; n.parse().unwrap()])
    } else {
        req.into_body().collect().await.unwrap().to_bytes()
    };
    Ok(hyper::Response::new(Full::new(body)))
}

/// Plain-text server speaking HTTP/1.1 or HTTP/2 (prior knowledge).
async fn server(h2: bool) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (s, _) = l.accept().await.unwrap();
            let io = TokioIo::new(s);
            tokio::spawn(async move {
                let svc = service_fn(handle);
                if h2 {
                    let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(io, svc)
                        .await;
                } else {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, svc)
                        .await;
                }
            });
        }
    });
    port
}

fn client(h2: bool) -> HttpClient {
    let b = matier::Client::builder();
    let b = if h2 {
        b.http2_prior_knowledge()
    } else {
        b.http1_only()
    };
    matier_fetch::builder(b.build().unwrap())
        .insecure_allow_http()
        .build()
}

#[tokio::test]
async fn get_and_post_over_both_protocols() {
    for h2 in [false, true] {
        let port = server(h2).await;
        let client = client(h2);

        let resp = client
            .get(format!("http://127.0.0.1:{port}/bytes/100000"))
            .fetch()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.version(),
            if h2 {
                http::Version::HTTP_2
            } else {
                http::Version::HTTP_11
            }
        );
        assert_eq!(resp.into_body().into_bytes().await.unwrap().len(), 100_000);

        let body = client
            .post(format!("http://127.0.0.1:{port}/echo"))
            .text("hello fetch")
            .fetch_text_body()
            .await
            .unwrap();
        assert_eq!(body, "hello fetch", "h2={h2}");
    }
}

#[tokio::test]
async fn streams_request_bodies() {
    for h2 in [false, true] {
        let port = server(h2).await;
        let client = client(h2);
        let builder =
            HttpBodyBuilder::new(OpaqueMemory::new(GlobalPool::new()), &Clock::new_tokio());
        let chunks = (0..3).map(|i| Ok(BytesView::from(Bytes::from(format!("chunk{i};")))));
        let body = builder.stream(
            futures_util::stream::iter(chunks),
            &HttpBodyOptions::default(),
        );

        let echoed = client
            .post(format!("http://127.0.0.1:{port}/echo"))
            .body(body)
            .fetch_text_body()
            .await
            .unwrap();
        assert_eq!(echoed, "chunk0;chunk1;chunk2;", "h2={h2}");
    }
}

#[tokio::test]
async fn connect_failure_is_retryable() {
    // Bind and drop to get a port nothing listens on.
    let port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let e = client(false)
        .get(format!("http://127.0.0.1:{port}/"))
        .fetch()
        .await
        .unwrap_err();
    assert_eq!(e.recovery(), RecoveryInfo::retry(), "{e}");
}

//! Future-size guard: `H2Handle::send` must return a compact named future (the request is moved into the
//! actor's inbox immediately; nothing request-sized lives in the future).

use std::mem::size_of_val;
use std::sync::Arc;

use matier_core::{Body, NoopObserver, OriginKey};
use matier_transport::Io;
use matier_transport::h2::{H2Config, handshake};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn send_future_is_small() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let _ = s.read(&mut buf).await.unwrap();
        s.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await.unwrap(); // empty SETTINGS
        let mut sink = vec![0u8; 65536];
        while s.read(&mut sink).await.map(|n| n > 0).unwrap_or(false) {}
    });
    let tcp = TcpStream::connect(addr).await.unwrap();
    let origin = OriginKey::from_uri(&format!("http://{addr}/").parse().unwrap()).unwrap();
    let h = handshake(
        Io::Tcp(tcp),
        origin,
        H2Config::default(),
        Arc::new(NoopObserver),
    )
    .await
    .unwrap();
    let req = http::Request::builder()
        .method("POST")
        .uri(format!("http://{addr}/x"))
        .body(Body::from_static(b"hello"))
        .unwrap();
    let fut = h.send(h.try_reserve().unwrap(), req);
    let n = size_of_val(&fut);
    println!("size_of_val(H2Handle::send future) = {n} bytes");
    assert!(n <= 1024, "send future is {n} bytes");
    assert!(n <= 64, "expected a tiny future, got {n}");
    drop(fut);
    drop(h);
    server.abort();
}

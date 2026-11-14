use http_body_util::BodyExt;
use matier::{Body, Client, ErrorKind, Service};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn echo_server() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = l.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = s.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let body = req.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = s.write_all(resp.as_bytes()).await;
            });
        }
    });
    port
}

#[tokio::test]
async fn get_and_post_roundtrip() {
    let port = echo_server().await;
    let client = Client::new();
    let req = http::Request::post(format!("http://127.0.0.1:{port}/echo"))
        .body(Body::from("hello matier"))
        .unwrap();
    let resp = client.execute(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"hello matier");
}

#[tokio::test]
async fn rejects_relative_and_unsupported_uris() {
    let client = Client::new();
    for uri in ["/relative", "ftp://example.com/x"] {
        let req = http::Request::get(uri).body(Body::empty()).unwrap();
        let e = client.send(req).await.unwrap_err();
        assert_eq!(e.kind(), ErrorKind::InvalidRequest, "{uri}");
    }
}

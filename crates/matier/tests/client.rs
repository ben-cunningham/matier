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

#[test]
fn builder_rejects_out_of_range_settings() {
    let frame = Client::builder().http2_max_frame_size(1024).build();
    assert_eq!(frame.unwrap_err().kind(), ErrorKind::InvalidRequest);
    let window = Client::builder()
        .http2_initial_connection_window_size(u32::MAX)
        .build();
    assert_eq!(window.unwrap_err().kind(), ErrorKind::InvalidRequest);
    for ok in [16_384, 16_777_215] {
        assert!(Client::builder().http2_max_frame_size(ok).build().is_ok());
    }
    assert!(
        Client::builder()
            .http2_initial_stream_window_size(0x7fff_ffff)
            .build()
            .is_ok()
    );
}

#[tokio::test]
async fn builder_settings_reach_the_connection() {
    let port = echo_server().await;
    let client = Client::builder()
        .tcp_nodelay(false)
        .tcp_keepalive(None)
        .tcp_send_buffer_size(Some(64 * 1024))
        .tcp_recv_buffer_size(Some(64 * 1024))
        .http1_only()
        .http1_title_case_headers(true)
        .local_address(Some("127.0.0.1".parse().unwrap()))
        .response_head_timeout(Some(std::time::Duration::from_secs(5)))
        .build()
        .unwrap();
    let req = http::Request::post(format!("http://127.0.0.1:{port}/echo"))
        .body(Body::from("tuned"))
        .unwrap();
    let resp = client.send(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"tuned");
}

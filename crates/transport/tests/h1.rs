use std::time::Duration;

use matier_core::{Body, ConnectionInfo, OriginKey, Scheme, buf};
use matier_transport::h1::{H1Config, H1Conn};
use matier_transport::{Incoming, Io};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

fn origin(port: u16) -> OriginKey {
    OriginKey {
        scheme: Scheme::Http,
        host: "127.0.0.1".into(),
        port,
    }
}

/// Reads one request head (until CRLFCRLF) and returns everything read so far.
async fn read_head(s: &mut TcpStream) -> Vec<u8> {
    let mut v = Vec::new();
    let mut b = [0u8; 4096];
    while !v.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = s.read(&mut b).await.unwrap();
        if n == 0 {
            break;
        }
        v.extend_from_slice(&b[..n]);
    }
    v
}

async fn conn_to<F, Fut>(handler: F) -> H1Conn
where
    F: FnOnce(TcpStream) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (s, _) = l.accept().await.unwrap();
        handler(s).await;
    });
    let s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s.set_nodelay(true).unwrap();
    H1Conn::new(Io::Tcp(s), origin(port), H1Config::default())
}

fn get(path: &str) -> http::Request<Body> {
    http::Request::get(format!("http://127.0.0.1/{path}"))
        .body(Body::empty())
        .unwrap()
}

type Rel = mpsc::UnboundedReceiver<H1Conn>;

async fn send(conn: H1Conn, req: http::Request<Body>) -> (http::Response<Incoming>, Rel) {
    let (tx, rx) = mpsc::unbounded_channel();
    let resp = conn
        .send(req, move |c| {
            let _ = tx.send(c);
        })
        .await
        .unwrap();
    (resp, rx)
}

async fn drain(body: Incoming) -> (Vec<u8>, usize) {
    use http_body::Body as _;
    let mut body = std::pin::pin!(body);
    let mut out = Vec::new();
    let mut frames = 0;
    while let Some(f) = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
        let d = f.unwrap().into_data().unwrap();
        frames += 1;
        for (s, _) in d.slices() {
            out.extend_from_slice(s);
        }
    }
    (out, frames)
}

#[tokio::test]
async fn content_length() {
    let c = conn_to(|mut s| async move {
        read_head(&mut s).await;
        s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-A: b\r\n\r\nhello")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    })
    .await;
    let (resp, mut rx) = send(c, get("x")).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers()["x-a"], "b");
    let info = *resp.extensions().get::<ConnectionInfo>().unwrap();
    assert!(!info.reused);
    let (b, _) = drain(resp.into_body()).await;
    assert_eq!(b, b"hello");
    assert!(rx.try_recv().is_ok(), "released at end of body");
}

#[tokio::test]
async fn chunked_split_across_reads() {
    let c = conn_to(|mut s| async move {
        read_head(&mut s).await;
        s.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhe")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        s.write_all(b"llo\r\n6\r\n world\r").await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        s.write_all(b"\n0\r\n\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    })
    .await;
    let (resp, mut rx) = send(c, get("x")).await;
    let (b, _) = drain(resp.into_body()).await;
    assert_eq!(b, b"hello world");
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn close_delimited_not_released() {
    let c = conn_to(|mut s| async move {
        read_head(&mut s).await;
        s.write_all(b"HTTP/1.0 200 OK\r\n\r\nabc").await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        s.write_all(b"def").await.unwrap();
    })
    .await;
    let (resp, mut rx) = send(c, get("x")).await;
    let (b, _) = drain(resp.into_body()).await;
    assert_eq!(b, b"abcdef");
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn empty_responses_release_immediately() {
    for (method, status_line) in [
        ("HEAD", "200 OK\r\nContent-Length: 10"),
        ("GET", "204 No Content"),
        ("GET", "304 Not Modified\r\nContent-Length: 10"),
    ] {
        let c = conn_to(move |mut s| async move {
            read_head(&mut s).await;
            s.write_all(format!("HTTP/1.1 {status_line}\r\n\r\n").as_bytes())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        })
        .await;
        let req = http::Request::builder()
            .method(method)
            .uri("http://127.0.0.1/")
            .body(Body::empty())
            .unwrap();
        let (resp, mut rx) = send(c, req).await;
        assert!(matches!(resp.body(), Incoming::Empty), "{status_line}");
        assert!(rx.try_recv().is_ok(), "{status_line}");
    }
}

#[tokio::test]
async fn keep_alive_reuse_same_conn() {
    let c = conn_to(|mut s| async move {
        for _ in 0..2 {
            read_head(&mut s).await;
            s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    })
    .await;
    let id = c.id();
    let (resp, mut rx) = send(c, get("1")).await;
    drain(resp.into_body()).await;
    let mut c = rx.recv().await.unwrap();
    assert!(c.is_reusable_now());
    let (resp, mut rx) = send(c, get("2")).await;
    let info = *resp.extensions().get::<ConnectionInfo>().unwrap();
    assert!(info.reused);
    assert_eq!(info.conn_id, id);
    drain(resp.into_body()).await;
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn connection_close_not_reused() {
    let c = conn_to(|mut s| async move {
        read_head(&mut s).await;
        s.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    })
    .await;
    let (resp, mut rx) = send(c, get("x")).await;
    let (b, _) = drain(resp.into_body()).await;
    assert_eq!(b, b"ok");
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn informational_skipped() {
    let c = conn_to(|mut s| async move {
        read_head(&mut s).await;
        s.write_all(
            b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: x\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nz",
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    })
    .await;
    let (resp, _rx) = send(c, get("x")).await;
    assert_eq!(resp.status(), 200);
    let (b, _) = drain(resp.into_body()).await;
    assert_eq!(b, b"z");
}

#[tokio::test]
async fn large_body_multiple_zero_copy_frames() {
    const N: usize = 1 << 20;
    let c = conn_to(|mut s| async move {
        read_head(&mut s).await;
        s.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {N}\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let chunk = vec![7u8; 64 * 1024];
        for _ in 0..N / chunk.len() {
            s.write_all(&chunk).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    })
    .await;
    let (resp, mut rx) = send(c, get("x")).await;
    let (b, frames) = drain(resp.into_body()).await;
    assert_eq!(b.len(), N);
    assert!(b.iter().all(|&x| x == 7));
    assert!(frames > 1, "frames = {frames}");
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn full_body_post_is_one_write_and_correct() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let c = conn_to(move |mut s| async move {
        let mut v = read_head(&mut s).await;
        while v.len() < 100_000 {
            let mut b = vec![0u8; 65536];
            let n = s.read(&mut b).await.unwrap();
            if n == 0 {
                break;
            }
            v.extend_from_slice(&b[..n]);
        }
        tx.send(v).unwrap();
        s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    })
    .await;
    let payload = vec![b'q'; 100_000];
    let req = http::Request::post("http://127.0.0.1/p")
        .body(Body::full(buf::copy_from_slice(&payload)))
        .unwrap();
    let (resp, _r) = send(c, req).await;
    assert_eq!(resp.status(), 200);
    let got = rx.recv().await.unwrap();
    let text = String::from_utf8_lossy(&got[..80]).to_lowercase();
    assert!(text.contains("post /p http/1.1"));
    assert!(
        String::from_utf8_lossy(&got)
            .to_lowercase()
            .contains("content-length: 100000")
    );
    assert!(got.ends_with(&payload));
}

#[tokio::test]
async fn streaming_request_body_is_chunked() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let c = conn_to(move |mut s| async move {
        let mut v = Vec::new();
        let mut b = [0u8; 4096];
        while !v.ends_with(b"0\r\n\r\n") {
            let n = s.read(&mut b).await.unwrap();
            if n == 0 {
                break;
            }
            v.extend_from_slice(&b[..n]);
        }
        tx.send(v).unwrap();
        s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    })
    .await;
    let (mut sender, body) = Body::channel(4);
    let req = http::Request::post("http://127.0.0.1/s")
        .body(body)
        .unwrap();
    tokio::spawn(async move {
        sender.send(buf::copy_from_slice(b"hello")).await.unwrap();
        sender.send(buf::copy_from_slice(b" world!")).await.unwrap();
        sender.finish();
    });
    let (resp, _r) = send(c, req).await;
    assert_eq!(resp.status(), 200);
    let got = String::from_utf8(rx.recv().await.unwrap()).unwrap();
    assert!(got.to_lowercase().contains("transfer-encoding: chunked"));
    assert!(
        got.ends_with("\r\n\r\n5\r\nhello\r\n7\r\n world!\r\n0\r\n\r\n"),
        "{got:?}"
    );
}

#[tokio::test]
async fn liveness_probe_detects_server_close() {
    let c = conn_to(|mut s| async move {
        read_head(&mut s).await;
        s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .unwrap();
        // closes right after responding
    })
    .await;
    let (resp, mut rx) = send(c, get("x")).await;
    drain(resp.into_body()).await;
    let mut c = rx.recv().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!c.is_reusable_now());
}

#[tokio::test]
async fn stale_reused_connection_is_flagged() {
    let c = conn_to(|mut s| async move {
        read_head(&mut s).await;
        s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .unwrap();
    })
    .await;
    let (resp, mut rx) = send(c, get("1")).await;
    drain(resp.into_body()).await;
    let c = rx.recv().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let err = c.send(get("2"), |_| {}).await.unwrap_err();
    assert!(matier_transport::h1::is_stale_keepalive(&err), "{err:?}");
}

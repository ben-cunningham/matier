use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use http_body::Body as _;
use matier_core::{Body, ErrorKind, NoopObserver};
use matier_pool::{Pool, PoolConfig};
use matier_transport::h1::H1Config;
use matier_transport::h2::H2Config;
use matier_transport::{ConnectConfig, Incoming};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Default)]
struct Stats {
    accepted: AtomicUsize,
    open: AtomicUsize,
    max_open: AtomicUsize,
    requests: AtomicUsize,
}

#[derive(Clone, Copy)]
struct Behavior {
    delay: Duration,
    /// Serve this many requests per connection, then read the next request and close silently.
    drop_after: Option<usize>,
}

async fn server(b: Behavior) -> (u16, Arc<Stats>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let stats = Arc::new(Stats::default());
    let st = Arc::clone(&stats);
    tokio::spawn(async move {
        loop {
            let (s, _) = l.accept().await.unwrap();
            st.accepted.fetch_add(1, Ordering::SeqCst);
            let now = st.open.fetch_add(1, Ordering::SeqCst) + 1;
            st.max_open.fetch_max(now, Ordering::SeqCst);
            let st = Arc::clone(&st);
            tokio::spawn(async move {
                handle(s, b, &st).await;
                st.open.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    (port, stats)
}

async fn handle(mut s: TcpStream, b: Behavior, st: &Stats) {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let mut served = 0;
    loop {
        while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
            match s.read(&mut tmp).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&tmp[..n]),
            }
        }
        let end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        buf.drain(..end);
        st.requests.fetch_add(1, Ordering::SeqCst);
        if b.drop_after == Some(served) {
            return;
        }
        served += 1;
        if !b.delay.is_zero() {
            tokio::time::sleep(b.delay).await;
        }
        if s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
            .await
            .is_err()
        {
            return;
        }
    }
}

fn pool(cfg: PoolConfig) -> Pool {
    Pool::new(
        cfg,
        ConnectConfig::default(),
        H1Config::default(),
        H2Config::default(),
        Arc::new(NoopObserver),
    )
}

fn get(port: u16, method: &str) -> http::Request<Body> {
    http::Request::builder()
        .method(method)
        .uri(format!("http://127.0.0.1:{port}/x"))
        .body(Body::empty())
        .unwrap()
}

async fn fetch(p: &Pool, req: http::Request<Body>) -> matier_core::Result<(u16, usize, u64)> {
    let resp = p.send(req).await?;
    let status = resp.status().as_u16();
    let info = resp.body().connection_info().unwrap();
    let mut body: Incoming = resp.into_body();
    let mut n = 0;
    while let Some(f) =
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await
    {
        if let Some(d) = f?.data_ref() {
            n += d.len();
        }
    }
    Ok((status, n, info.conn_id))
}

fn uri(port: u16) -> http::Uri {
    format!("http://127.0.0.1:{port}/").parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_conns_respected_under_concurrency() {
    let (port, stats) = server(Behavior {
        delay: Duration::from_micros(200),
        drop_after: None,
    })
    .await;
    let p = pool(PoolConfig {
        max_conns_per_origin: 8,
        ..PoolConfig::default()
    });
    let sem = Arc::new(tokio::sync::Semaphore::new(64));
    let mut tasks = Vec::new();
    for _ in 0..1000 {
        let permit = Arc::clone(&sem).acquire_owned().await.unwrap();
        let p = p.clone();
        tasks.push(tokio::spawn(async move {
            let r = fetch(&p, get(port, "GET")).await;
            drop(permit);
            r
        }));
    }
    for t in tasks {
        let (s, n, _) = t.await.unwrap().unwrap();
        assert_eq!((s, n), (200, 5));
    }
    assert_eq!(stats.requests.load(Ordering::SeqCst), 1000);
    assert!(
        stats.max_open.load(Ordering::SeqCst) <= 8,
        "max_open {}",
        stats.max_open.load(Ordering::SeqCst)
    );
    assert!(stats.accepted.load(Ordering::SeqCst) <= 8);
    let st = p.stats(&uri(port)).unwrap();
    assert_eq!(st.h1_waiters, 0);
    assert!(st.h1_total <= 8);
    assert_eq!(
        st.h1_total, st.h1_idle,
        "everything returned to the idle stack: {st:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_waiters_leave_no_leaks() {
    let (port, stats) = server(Behavior {
        delay: Duration::from_millis(150),
        drop_after: None,
    })
    .await;
    let p = pool(PoolConfig {
        max_conns_per_origin: 1,
        ..PoolConfig::default()
    });
    let first = {
        let p = p.clone();
        tokio::spawn(async move { fetch(&p, get(port, "GET")).await })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    let waiters: Vec<_> = (0..50)
        .map(|_| {
            let p = p.clone();
            tokio::spawn(async move { fetch(&p, get(port, "GET")).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(p.stats(&uri(port)).unwrap().h1_waiters, 50);
    for w in &waiters[..49] {
        w.abort();
    }
    for w in waiters.into_iter().take(49) {
        let _ = w.await;
    }
    assert_eq!(
        p.stats(&uri(port)).unwrap().h1_waiters,
        1,
        "cancellation removed waiters"
    );
    first.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let st = p.stats(&uri(port)).unwrap();
    assert_eq!(
        (st.h1_waiters, st.h1_total, st.h1_idle),
        (0, 1, 1),
        "{st:?}"
    );
    assert_eq!(stats.accepted.load(Ordering::SeqCst), 1);
    fetch(&p, get(port, "GET")).await.unwrap();
}

#[tokio::test]
async fn aborting_an_in_flight_request_frees_its_slot() {
    let (port, _s) = server(Behavior {
        delay: Duration::from_millis(300),
        drop_after: None,
    })
    .await;
    let p = pool(PoolConfig {
        max_conns_per_origin: 1,
        ..PoolConfig::default()
    });
    let t = {
        let p = p.clone();
        tokio::spawn(async move { fetch(&p, get(port, "GET")).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    t.abort();
    let _ = t.await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        p.stats(&uri(port)).unwrap().h1_total,
        0,
        "dropped connection released its slot"
    );
    let (s, n, _) = fetch(&p, get(port, "GET")).await.unwrap();
    assert_eq!((s, n), (200, 5));
}

#[tokio::test]
async fn checkout_timeout() {
    let (port, _s) = server(Behavior {
        delay: Duration::from_millis(300),
        drop_after: None,
    })
    .await;
    let p = pool(PoolConfig {
        max_conns_per_origin: 1,
        checkout_timeout: Some(Duration::from_millis(50)),
        ..PoolConfig::default()
    });
    let p2 = p.clone();
    let hold = tokio::spawn(async move { fetch(&p2, get(port, "GET")).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let e = fetch(&p, get(port, "GET")).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::PoolTimeout);
    hold.await.unwrap().unwrap();
    assert_eq!(p.stats(&uri(port)).unwrap().h1_waiters, 0);
}

#[tokio::test]
async fn stale_keepalive_is_retried_for_idempotent_requests() {
    // Each connection serves one request, then silently closes on the next one.
    let (port, stats) = server(Behavior {
        delay: Duration::ZERO,
        drop_after: Some(1),
    })
    .await;
    let p = pool(PoolConfig::default());
    let (_, _, id1) = fetch(&p, get(port, "GET")).await.unwrap();
    // The server only closes after reading the 2nd request, so the idle probe cannot see it.
    let (s, n, id2) = fetch(&p, get(port, "GET")).await.unwrap();
    assert_eq!((s, n), (200, 5));
    assert_ne!(id1, id2, "retried on a fresh connection");
    assert_eq!(stats.accepted.load(Ordering::SeqCst), 2);

    // Non-idempotent requests are not replayed: the error surfaces.
    let (port2, _) = server(Behavior {
        delay: Duration::ZERO,
        drop_after: Some(1),
    })
    .await;
    fetch(&p, get(port2, "POST")).await.unwrap();
    let e = fetch(&p, get(port2, "POST")).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Io);
}

#[tokio::test]
async fn idle_timeout_discards_connection() {
    let (port, stats) = server(Behavior {
        delay: Duration::ZERO,
        drop_after: None,
    })
    .await;
    let p = pool(PoolConfig {
        idle_timeout: Duration::from_millis(50),
        ..PoolConfig::default()
    });
    let (_, _, a) = fetch(&p, get(port, "GET")).await.unwrap();
    let (_, _, b) = fetch(&p, get(port, "GET")).await.unwrap();
    assert_eq!(a, b, "reused while fresh");
    tokio::time::sleep(Duration::from_millis(120)).await;
    let (_, _, c) = fetch(&p, get(port, "GET")).await.unwrap();
    assert_ne!(b, c, "expired connection discarded");
    assert_eq!(stats.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn max_connection_lifetime_retires_http1_connections() {
    let (port, stats) = server(Behavior {
        delay: Duration::ZERO,
        drop_after: None,
    })
    .await;
    let p = pool(PoolConfig {
        max_connection_lifetime: Some(Duration::from_millis(80)),
        ..PoolConfig::default()
    });
    let (_, _, a) = fetch(&p, get(port, "GET")).await.unwrap();
    let (_, _, b) = fetch(&p, get(port, "GET")).await.unwrap();
    assert_eq!(a, b, "reused while young");
    tokio::time::sleep(Duration::from_millis(120)).await;
    let (_, _, c) = fetch(&p, get(port, "GET")).await.unwrap();
    assert_ne!(b, c, "connection past its lifetime replaced");
    assert_eq!(stats.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn connection_that_ages_out_mid_request_is_closed_on_return() {
    let (port, stats) = server(Behavior {
        delay: Duration::from_millis(150),
        drop_after: None,
    })
    .await;
    let p = pool(PoolConfig {
        max_connection_lifetime: Some(Duration::from_millis(80)),
        ..PoolConfig::default()
    });
    let (_, _, a) = fetch(&p, get(port, "GET")).await.unwrap();
    assert_eq!(
        p.stats(&uri(port)).unwrap().h1_idle,
        0,
        "not returned to the idle stack"
    );
    let (_, _, b) = fetch(&p, get(port, "GET")).await.unwrap();
    assert_ne!(a, b);
    assert_eq!(stats.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn connect_failure_is_unprocessed_and_frees_slot() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let p = pool(PoolConfig {
        max_conns_per_origin: 1,
        ..PoolConfig::default()
    });
    for _ in 0..3 {
        let e = fetch(&p, get(port, "GET")).await.unwrap_err();
        assert_eq!(e.recovery(), matier_core::Recovery::Unprocessed);
    }
    assert_eq!(p.stats(&uri(port)).unwrap().h1_total, 0);
}

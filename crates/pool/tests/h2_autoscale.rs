//! HTTP/2 connection autoscaling against a local hyper h2 server (zero-config: nobody sets a connection count).

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming as HyperIncoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use matier_core::{Body, Observer, OriginKey, ScaleReason};
use matier_pool::scale::ScaleThresholds;
use matier_pool::{Pool, PoolConfig};
use matier_transport::ConnectConfig;
use matier_transport::h1::H1Config;
use matier_transport::h2::H2Config;
use tokio::net::TcpListener;

const MAX_CONNS: usize = 4;

/// The tests measure scheduling-sensitive load signals: run them one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Default)]
struct Server {
    accepted: AtomicUsize,
    open: AtomicUsize,
}

async fn handle(
    _req: hyper::Request<HyperIncoming>,
) -> Result<hyper::Response<Full<Bytes>>, Infallible> {
    Ok(hyper::Response::new(Full::new(Bytes::from_static(
        &[b'x'; 128],
    ))))
}

/// `slow_dial`: every connection after the first completes its handshake this much later.
async fn server(slow_dial: Duration) -> (u16, Arc<Server>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let st = Arc::new(Server::default());
    let st2 = Arc::clone(&st);
    tokio::spawn(async move {
        loop {
            let Ok((s, _)) = l.accept().await else { return };
            s.set_nodelay(true).unwrap();
            let n = st2.accepted.fetch_add(1, Ordering::SeqCst);
            st2.open.fetch_add(1, Ordering::SeqCst);
            let st = Arc::clone(&st2);
            tokio::spawn(async move {
                if n > 0 {
                    tokio::time::sleep(slow_dial).await;
                }
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(s), service_fn(handle))
                    .await;
                st.open.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    (port, st)
}

#[derive(Default)]
struct Events {
    log: Log,
}

#[derive(Default)]
struct Log(std::sync::Mutex<Vec<(usize, ScaleReason)>>);

impl Observer for Events {
    fn pool_scaled(&self, _o: &OriginKey, conns: usize, reason: ScaleReason) {
        self.log.0.lock().unwrap().push((conns, reason));
    }
}

fn pool(idle_timeout: Duration, events: Arc<Events>) -> Pool {
    Pool::new(
        PoolConfig {
            h2_max_conns_per_origin: MAX_CONNS,
            idle_timeout,
            // Deterministic trigger: any activity of the actor counts as saturation (the "at least 4 streams
            // in flight" rule still keeps c=1 and c=2 on one connection). The real thresholds are covered by
            // unit tests and observed in the benchmark.
            h2_scale: ScaleThresholds {
                busy_permille: 0,
                dispatch_delay: Duration::ZERO,
                inbox: 0,
                cooldown: Duration::from_millis(20),
            },
            ..PoolConfig::default()
        },
        ConnectConfig {
            http2_prior_knowledge: true,
            ..ConnectConfig::default()
        },
        H1Config::default(),
        H2Config::default(),
        events,
    )
}

fn uri(port: u16) -> http::Uri {
    format!("http://127.0.0.1:{port}/x").parse().unwrap()
}

async fn one(p: &Pool, u: &http::Uri) {
    let req = http::Request::get(u.clone()).body(Body::empty()).unwrap();
    let resp = p.send(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    let _ = resp.into_body().collect().await.unwrap();
}

struct Load {
    stop: Arc<AtomicBool>,
    max_latency_us: Arc<AtomicU64>,
    done: Arc<AtomicU64>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

fn start_load(p: &Pool, u: &http::Uri, concurrency: usize) -> Load {
    let stop = Arc::new(AtomicBool::new(false));
    let max = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicU64::new(0));
    let tasks = (0..concurrency)
        .map(|_| {
            let (p, u, stop, max, done) = (
                p.clone(),
                u.clone(),
                Arc::clone(&stop),
                Arc::clone(&max),
                Arc::clone(&done),
            );
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let t = Instant::now();
                    one(&p, &u).await;
                    max.fetch_max(t.elapsed().as_micros() as u64, Ordering::Relaxed);
                    done.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();
    Load {
        stop,
        max_latency_us: max,
        done,
        tasks,
    }
}

impl Load {
    async fn finish(self) -> (u64, Duration) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.tasks {
            t.await.unwrap();
        }
        (
            self.done.load(Ordering::Relaxed),
            Duration::from_micros(self.max_latency_us.load(Ordering::Relaxed)),
        )
    }
}

async fn wait_for(what: &str, limit: Duration, mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < limit, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn conns(p: &Pool, u: &http::Uri) -> usize {
    p.stats(u).map_or(0, |s| s.h2_conns)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grows_under_load_never_exceeds_max_then_shrinks_when_idle() {
    let _serial = SERIAL.lock().await;
    let (port, srv) = server(Duration::ZERO).await;
    let u = uri(port);
    let ev = Arc::new(Events::default());
    let p = pool(Duration::from_millis(300), Arc::clone(&ev));
    one(&p, &u).await;
    assert_eq!(conns(&p, &u), 1, "zero-config starts with one connection");

    let load = start_load(&p, &u, 64);
    let mut peak = 1;
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(8) {
        peak = peak.max(conns(&p, &u));
        assert!(peak <= MAX_CONNS, "exceeded the maximum: {peak}");
        if peak >= 2 && t.elapsed() > Duration::from_secs(2) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let (done, _) = load.finish().await;
    assert!(done > 0);
    assert!(peak >= 2, "c=64 never scaled past {peak} connection(s)");
    assert!(srv.accepted.load(Ordering::SeqCst) <= MAX_CONNS);
    let saturated = ev
        .log
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, r)| *r == ScaleReason::Saturated)
        .count();
    assert!(saturated >= 1, "scale-up was not attributed to saturation");

    // Load is gone: connections idle out down to the minimum, and the surplus ones are closed gracefully.
    wait_for("shrink to min", Duration::from_secs(10), || {
        conns(&p, &u) == 1
    })
    .await;
    wait_for("server sees the closes", Duration::from_secs(5), || {
        srv.open.load(Ordering::SeqCst) == 1
    })
    .await;
    one(&p, &u).await; // the survivor still works
    assert!(
        ev.log
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|(_, r)| *r == ScaleReason::Idle)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn light_load_stays_on_one_connection() {
    let _serial = SERIAL.lock().await;
    let (port, srv) = server(Duration::ZERO).await;
    let u = uri(port);
    let p = pool(Duration::from_secs(90), Arc::new(Events::default()));
    let t = Instant::now();
    let mut n = 0u64;
    while t.elapsed() < Duration::from_secs(1) {
        one(&p, &u).await;
        n += 1;
    }
    assert!(n > 50);
    // c=2 (still too little to split across connections).
    let load = start_load(&p, &u, 2);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (done, _) = load.finish().await;
    assert!(done > 50);
    assert_eq!(conns(&p, &u), 1);
    assert_eq!(srv.accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_never_wait_for_a_scale_up_dial() {
    let _serial = SERIAL.lock().await;
    const DIAL: Duration = Duration::from_millis(600);
    let (port, srv) = server(DIAL).await;
    let u = uri(port);
    let p = pool(Duration::from_secs(90), Arc::new(Events::default()));
    one(&p, &u).await;
    let load = start_load(&p, &u, 64);
    // Wait until at least one scale-up dial has been started (the server has accepted a 2nd connection) ...
    wait_for("a scale-up dial", Duration::from_secs(8), || {
        srv.accepted.load(Ordering::SeqCst) >= 2
    })
    .await;
    // ... and keep running through its (slow) handshake and for a while after.
    wait_for("the new connection", Duration::from_secs(8), || {
        conns(&p, &u) >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (done, max) = load.finish().await;
    assert!(done > 1000, "{done}");
    assert!(
        max < DIAL / 2,
        "a request waited {max:?}: it must proceed on the existing connection during a {DIAL:?} dial"
    );
}

/// Connections that are not idle (a trickle of requests keeps them busy-ish) but under-used are drained one
/// at a time down to the minimum, after the hold-off that follows the last scale-up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn underused_connections_are_drained_with_hysteresis() {
    let _serial = SERIAL.lock().await;
    let (port, _srv) = server(Duration::ZERO).await;
    let u = uri(port);
    let ev = Arc::new(Events::default());
    let p = pool(Duration::from_secs(90), Arc::clone(&ev));
    one(&p, &u).await;
    let load = start_load(&p, &u, 64);
    wait_for("scale-up", Duration::from_secs(8), || conns(&p, &u) >= 2).await;
    let _ = load.finish().await;
    // A trickle: sequential requests, far below any saturation threshold.
    let t = Instant::now();
    while conns(&p, &u) > 1 {
        assert!(t.elapsed() < Duration::from_secs(30), "never drained");
        one(&p, &u).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let log = ev.log.0.lock().unwrap().clone();
    assert!(
        log.iter().any(|(_, r)| *r == ScaleReason::Underused),
        "{log:?}"
    );
    // Drained one at a time and never below the minimum.
    assert!(log.iter().all(|(n, _)| *n >= 1));
    assert_eq!(conns(&p, &u), 1);
}

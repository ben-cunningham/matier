//! Future-size budget (like Microsoft fetch's): the futures on the request hot path must stay
//! small because they are moved/stored by every caller. Sizes are printed with `--nocapture`.
//!
//! Measured (debug and release give identical numbers): `http::Request<Body>` = 504 bytes,
//! `H1Conn::send_mut` 344, `H1Conn::send` 616, `Pool::send` / `Client::send` /
//! `Service::execute` 928 (HTTP/2 sends are boxed inside the pool to keep these small).
//! Rule: hot-path futures are plain `fn`s returning `async move` blocks or combinators, never
//! `async fn`s taking big arguments (async-fn parameters are stored twice).

use std::mem::size_of_val;
use std::sync::Arc;

use matier::{Body, Client, ConnectConfig, H1Config, H2Config, Pool, PoolConfig, Service};
use matier_core::{NoopObserver, OriginKey, Scheme};
use matier_transport::Io;
use matier_transport::h1::H1Conn;
use tokio::net::{TcpListener, TcpStream};

const BUDGET: usize = 1024;

fn req() -> http::Request<Body> {
    http::Request::get("http://127.0.0.1:1/x")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn hot_path_futures_stay_small() {
    let client = Client::new();
    let pool = Pool::new(
        PoolConfig::default(),
        ConnectConfig::default(),
        H1Config::default(),
        H2Config::default(),
        Arc::new(NoopObserver),
    );

    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let conn = H1Conn::new(
        Io::Tcp(s),
        OriginKey {
            scheme: Scheme::Http,
            host: "127.0.0.1".into(),
            port,
        },
        H1Config::default(),
    );

    let sizes = [
        ("Client::send", {
            let f = client.send(req());
            let n = size_of_val(&f);
            drop(f);
            n
        }),
        ("Service::execute", {
            let f = client.execute(req());
            let n = size_of_val(&f);
            drop(f);
            n
        }),
        ("Pool::send", {
            let f = pool.send(req());
            let n = size_of_val(&f);
            drop(f);
            n
        }),
        ("H1Conn::send_mut", {
            let mut r = req();
            let f = {
                let c2 = H1Conn::new(
                    Io::Tcp(TcpStream::connect(("127.0.0.1", port)).await.unwrap()),
                    OriginKey {
                        scheme: Scheme::Http,
                        host: "127.0.0.1".into(),
                        port,
                    },
                    H1Config::default(),
                );
                struct R;
                impl matier_transport::h1::Release for R {
                    fn release(&self, _c: H1Conn) {}
                }
                c2.send_mut(&mut r, Arc::new(R))
            };
            let n = size_of_val(&f);
            drop(f);
            n
        }),
        ("H1Conn::send", {
            let f = conn.send(req(), |_c| {});
            let n = size_of_val(&f);
            drop(f);
            n
        }),
    ];
    println!(
        "size_of::<http::Request<Body>>() = {}",
        std::mem::size_of::<http::Request<Body>>()
    );
    let mut bad = Vec::new();
    for (name, n) in sizes {
        println!("{name:<18} {n:>5} bytes");
        if n > BUDGET {
            bad.push(format!("{name} future is {n} bytes (budget {BUDGET})"));
        }
    }
    assert!(bad.is_empty(), "{bad:?}");
}

use std::io::IoSlice;
use std::time::{Duration, Instant};

use matier_core::{
    ConnectPhase, ErrorKind, NoopObserver, Observer, OriginKey, Protocol, Recovery, Scheme,
};
use matier_transport::{ConnectConfig, Io, TlsConfig, connect};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

fn origin(scheme: Scheme, host: &str, port: u16) -> OriginKey {
    OriginKey {
        scheme,
        host: host.into(),
        port,
    }
}

/// Spawns a TLS server offering `alpn`; returns port and root PEM. Each connection reports bytes it reads.
async fn tls_server(
    alpn: &[&[u8]],
) -> (u16, String, tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let signing_key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    // Distinct issuers let OpenSSL select the right self-signed root in a PEM bundle,
    // rather than choosing between rcgen's identical default subject names.
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, format!("matier-test-{port}"));
    let cert = params.self_signed(&signing_key).unwrap();
    let pem = cert.pem();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    let mut cfg = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert.der().clone()], key)
    .unwrap();
    cfg.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    let acceptor = TlsAcceptor::from(std::sync::Arc::new(cfg));
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let Ok((s, _)) = l.accept().await else { return };
            let acceptor = acceptor.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let Ok(mut s) = acceptor.accept(s).await else {
                    return;
                };
                let mut v = Vec::new();
                let mut b = [0u8; 1024];
                while let Ok(n) = s.read(&mut b).await {
                    if n == 0 {
                        break;
                    }
                    v.extend_from_slice(&b[..n]);
                    if v.len() >= 11 {
                        break;
                    }
                }
                let _ = tx.send(v);
            });
        }
    });
    (port, pem, rx)
}

fn tls_cfg(pem: &str) -> ConnectConfig {
    ConnectConfig {
        tls: Some(TlsConfig::with_root_pem(pem.as_bytes()).unwrap()),
        ..Default::default()
    }
}

#[tokio::test]
async fn plaintext_connect_and_vectored_write() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let mut v = Vec::new();
        s.read_to_end(&mut v).await.unwrap();
        v
    });
    let mut c = connect(
        &origin(Scheme::Http, "127.0.0.1", port),
        &ConnectConfig::default(),
        &NoopObserver,
    )
    .await
    .unwrap();
    assert_eq!(c.protocol, Protocol::Http1);
    assert!(matches!(c.io, Io::Tcp(_)));
    assert!(c.io.tcp().nodelay().unwrap());
    assert!(tokio::io::AsyncWrite::is_write_vectored(&c.io));
    let bufs = [
        IoSlice::new(b"hello "),
        IoSlice::new(b"vectored "),
        IoSlice::new(b"world"),
    ];
    let mut n = 0;
    let total: usize = bufs.iter().map(|b| b.len()).sum();
    while n < total {
        // Single call must be able to take more than one slice at once on the first pass.
        let w = tokio::io::AsyncWriteExt::write_vectored(&mut c.io, &bufs_from(&bufs, n))
            .await
            .unwrap();
        n += w;
    }
    c.io.shutdown().await.unwrap();
    assert_eq!(server.await.unwrap(), b"hello vectored world");
}

fn bufs_from<'a>(bufs: &'a [IoSlice<'a>], mut skip: usize) -> Vec<IoSlice<'a>> {
    let mut out = Vec::new();
    for b in bufs {
        if skip >= b.len() {
            skip -= b.len();
        } else {
            out.push(IoSlice::new(&b[skip..]));
            skip = 0;
        }
    }
    out
}

#[tokio::test]
async fn prior_knowledge_selects_h2() {
    let l = TcpListener::bind("[::1]:0").await;
    let Ok(l) = l else { return };
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move { drop(l.accept().await) });
    let cfg = ConnectConfig {
        http2_prior_knowledge: true,
        ..Default::default()
    };
    let c = connect(&origin(Scheme::Http, "[::1]", port), &cfg, &NoopObserver)
        .await
        .unwrap();
    assert_eq!(c.protocol, Protocol::Http2);
}

#[tokio::test]
async fn alpn_negotiates_h2_and_h1() {
    let (port, pem, mut rx) = tls_server(&[b"h2", b"http/1.1"]).await;
    let o = origin(Scheme::Https, "localhost", port);
    let c = connect(&o, &tls_cfg(&pem), &NoopObserver).await.unwrap();
    assert_eq!(c.protocol, Protocol::Http2);

    // Client opts out of h2: server falls back to http/1.1.
    let cfg = ConnectConfig {
        alpn_h2: false,
        ..tls_cfg(&pem)
    };
    let mut c = connect(&o, &cfg, &NoopObserver).await.unwrap();
    assert_eq!(c.protocol, Protocol::Http1);

    // Vectored write through TLS reaches the server intact.
    assert!(!tokio::io::AsyncWrite::is_write_vectored(&c.io));
    let bufs = [
        IoSlice::new(b"hello "),
        IoSlice::new(b"tls"),
        IoSlice::new(b" io"),
    ];
    let mut n = 0;
    while n < 12 {
        n += tokio::io::AsyncWriteExt::write_vectored(&mut c.io, &bufs_from(&bufs, n))
            .await
            .unwrap();
    }
    c.io.flush().await.unwrap();
    let mut got = Vec::new();
    while got.len() < 12 {
        got.extend(rx.recv().await.unwrap());
    }
    assert_eq!(got, b"hello tls io");
}

#[tokio::test]
async fn server_without_h2_yields_http1() {
    let (port, pem, _rx) = tls_server(&[b"http/1.1"]).await;
    let c = connect(
        &origin(Scheme::Https, "localhost", port),
        &tls_cfg(&pem),
        &NoopObserver,
    )
    .await
    .unwrap();
    assert_eq!(c.protocol, Protocol::Http1);
}

#[tokio::test]
async fn server_without_alpn_yields_http1() {
    let (port, pem, _rx) = tls_server(&[]).await;
    let c = connect(
        &origin(Scheme::Https, "localhost", port),
        &tls_cfg(&pem),
        &NoopObserver,
    )
    .await
    .unwrap();
    assert_eq!(c.protocol, Protocol::Http1);
}

#[tokio::test]
async fn root_pem_bundle_trusts_each_certificate() {
    let (port_a, pem_a, _rx_a) = tls_server(&[b"h2"]).await;
    let (port_b, pem_b, _rx_b) = tls_server(&[b"h2"]).await;
    let cfg = tls_cfg(&format!("{pem_a}\n{pem_b}"));
    for port in [port_a, port_b] {
        let c = connect(
            &origin(Scheme::Https, "localhost", port),
            &cfg,
            &NoopObserver,
        )
        .await
        .unwrap();
        assert_eq!(c.protocol, Protocol::Http2);
    }
}

#[test]
fn root_pem_rejects_empty_or_malformed_certificates() {
    for pem in [
        b"".as_slice(),
        b"not a certificate",
        b"-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----",
    ] {
        let e = TlsConfig::with_root_pem(pem).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Tls);
        assert_eq!(e.recovery(), Recovery::Never);
    }
}

#[tokio::test]
async fn trusted_cert_with_wrong_hostname_is_tls_never() {
    let (port, pem, _rx) = tls_server(&[b"h2"]).await;
    // The certificate is trusted, but it contains only the DNS name localhost, not this IP.
    let e = connect(
        &origin(Scheme::Https, "127.0.0.1", port),
        &tls_cfg(&pem),
        &NoopObserver,
    )
    .await
    .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Tls);
    assert_eq!(e.recovery(), Recovery::Never);
}

#[tokio::test]
async fn connection_closed_during_tls_handshake_is_tls_unprocessed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    // Accept and close without answering the ClientHello: the socket failed, not TLS.
    tokio::spawn(async move { drop(listener.accept().await) });
    let cfg = ConnectConfig {
        tls: Some(TlsConfig::with_platform_verifier().unwrap()),
        ..Default::default()
    };
    let e = connect(
        &origin(Scheme::Https, "127.0.0.1", port),
        &cfg,
        &NoopObserver,
    )
    .await
    .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Tls);
    assert_eq!(e.recovery(), Recovery::Unprocessed);
}

#[tokio::test]
async fn native_builder_preserves_custom_roots_and_transport_owns_alpn() {
    let (port, pem, _rx) = tls_server(&[b"h2", b"http/1.1"]).await;
    let mut builder = native_tls::TlsConnector::builder();
    builder
        .disable_built_in_roots(true)
        .add_root_certificate(native_tls::Certificate::from_pem(pem.as_bytes()).unwrap())
        .request_alpns(&["not-http"]);
    let tls = TlsConfig::from_native_tls(builder).unwrap();
    for (alpn_h2, expected) in [(true, Protocol::Http2), (false, Protocol::Http1)] {
        let cfg = ConnectConfig {
            tls: Some(tls.clone()),
            alpn_h2,
            ..Default::default()
        };
        let c = connect(
            &origin(Scheme::Https, "localhost", port),
            &cfg,
            &NoopObserver,
        )
        .await
        .unwrap();
        assert_eq!(c.protocol, expected);
    }
}

#[tokio::test]
async fn stalled_tls_handshake_is_unprocessed_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let cfg = ConnectConfig {
        connect_timeout: Duration::from_millis(100),
        tls: Some(TlsConfig::with_platform_verifier().unwrap()),
        ..Default::default()
    };
    // Keep the accepted socket alive, but never send a TLS handshake response.
    let origin = origin(Scheme::Https, "127.0.0.1", port);
    let (result, socket) = tokio::join!(connect(&origin, &cfg, &NoopObserver), listener.accept());
    drop(socket.unwrap());
    let e = result.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Timeout);
    assert_eq!(e.recovery(), Recovery::Unprocessed);
}

#[tokio::test]
async fn untrusted_cert_is_tls_never() {
    let (port, _pem, _rx) = tls_server(&[b"h2"]).await;
    let other = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .unwrap()
        .cert
        .pem();
    let e = connect(
        &origin(Scheme::Https, "localhost", port),
        &tls_cfg(&other),
        &NoopObserver,
    )
    .await
    .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Tls);
    assert_eq!(e.recovery(), Recovery::Never);
}

#[tokio::test]
async fn https_without_tls_config_is_invalid() {
    let e = connect(
        &origin(Scheme::Https, "localhost", 1),
        &ConnectConfig::default(),
        &NoopObserver,
    )
    .await
    .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidRequest);
}

#[tokio::test]
async fn connection_refused_is_unprocessed() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let e = connect(
        &origin(Scheme::Http, "127.0.0.1", port),
        &ConnectConfig::default(),
        &NoopObserver,
    )
    .await
    .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Connect);
    assert_eq!(e.recovery(), Recovery::Unprocessed);
}

#[tokio::test]
async fn dns_failure_is_unprocessed_dns() {
    let e = connect(
        &origin(Scheme::Http, "does-not-exist.invalid", 80),
        &ConnectConfig::default(),
        &NoopObserver,
    )
    .await
    .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Dns);
    assert_eq!(e.recovery(), Recovery::Unprocessed);
}

#[tokio::test]
#[ignore = "needs a network that black-holes 10.255.255.1 (timing dependent)"]
async fn connect_timeout_non_routable() {
    let cfg = ConnectConfig {
        connect_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    let t = Instant::now();
    let e = connect(
        &origin(Scheme::Http, "10.255.255.1", 80),
        &cfg,
        &NoopObserver,
    )
    .await
    .unwrap_err();
    assert!(t.elapsed() < Duration::from_secs(5));
    // Some networks answer with an immediate "unreachable" instead of dropping packets.
    assert!(
        matches!(e.kind(), ErrorKind::Timeout | ErrorKind::Connect),
        "{e}"
    );
    assert_eq!(e.recovery(), Recovery::Unprocessed);
}

#[tokio::test]
async fn observer_sees_phases() {
    use std::sync::Mutex;
    #[derive(Default)]
    struct Rec(Mutex<Vec<(ConnectPhase, bool)>>);
    impl Observer for Rec {
        fn enabled(&self) -> bool {
            true
        }
        fn connect_phase(&self, _: &OriginKey, p: ConnectPhase, _: Duration, ok: bool) {
            self.0.lock().unwrap().push((p, ok));
        }
    }
    let (port, pem, _rx) = tls_server(&[b"h2"]).await;
    let obs = Rec::default();
    connect(
        &origin(Scheme::Https, "localhost", port),
        &tls_cfg(&pem),
        &obs,
    )
    .await
    .unwrap();
    assert_eq!(
        *obs.0.lock().unwrap(),
        [
            (ConnectPhase::Dns, true),
            (ConnectPhase::Tcp, true),
            (ConnectPhase::Tls, true)
        ]
    );
}

#[test]
fn platform_verifier_builds() {
    TlsConfig::with_platform_verifier().unwrap();
}

#[tokio::test]
async fn socket_options_are_applied_before_connect() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let accept = tokio::spawn(async move { l.accept().await.unwrap() });
    let cfg = ConnectConfig {
        nodelay: false,
        recv_buffer_size: Some(256 * 1024),
        send_buffer_size: Some(128 * 1024),
        local_address: Some("127.0.0.1".parse().unwrap()),
        ..ConnectConfig::default()
    };
    let c = connect(
        &origin(Scheme::Http, "127.0.0.1", port),
        &cfg,
        &NoopObserver,
    )
    .await
    .unwrap();
    let (_, peer) = accept.await.unwrap();
    let tcp = c.io.tcp();
    assert!(!tcp.nodelay().unwrap());
    assert_eq!(peer, tcp.local_addr().unwrap());
    let sock = socket2::SockRef::from(tcp);
    // Linux reports double the requested size; macOS and Windows report it as set.
    assert!(sock.recv_buffer_size().unwrap() >= 256 * 1024);
    assert!(sock.send_buffer_size().unwrap() >= 128 * 1024);
}

#[tokio::test]
async fn local_address_of_the_other_family_fails() {
    let cfg = ConnectConfig {
        local_address: Some("::1".parse().unwrap()),
        ..ConnectConfig::default()
    };
    let e = connect(&origin(Scheme::Http, "127.0.0.1", 9), &cfg, &NoopObserver)
        .await
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Connect);
}

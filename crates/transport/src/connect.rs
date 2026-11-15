//! DNS, TCP and TLS connection establishment.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use matier_core::{ConnectPhase, Error, ErrorKind, Observer, OriginKey, Protocol, Result};
use rustls::pki_types::ServerName;
use tokio::net::{TcpSocket, TcpStream, lookup_host};
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

use crate::io::Io;
use crate::tls::{ALPN_H2, TlsConfig};

#[derive(Clone, Debug)]
pub struct ConnectConfig {
    /// Applies separately to DNS, to each TCP attempt, and to the TLS handshake.
    pub connect_timeout: Duration,
    pub nodelay: bool,
    /// TCP keepalive idle time; `None` leaves the OS default (off).
    pub keepalive: Option<Duration>,
    /// `SO_SNDBUF`; `None` leaves the OS default (auto-tuned on Linux and macOS).
    pub send_buffer_size: Option<u32>,
    /// `SO_RCVBUF`; `None` leaves the OS default. Set before connecting, so it takes part in TCP window scaling.
    pub recv_buffer_size: Option<u32>,
    /// Local address to bind before connecting (`None`: the OS picks). Addresses of the other family are
    /// skipped.
    pub local_address: Option<IpAddr>,
    /// Required for `https` origins.
    pub tls: Option<TlsConfig>,
    /// Speak HTTP/2 over plaintext (h2c) without upgrade.
    pub http2_prior_knowledge: bool,
    /// Offer `h2` in ALPN. When false only `http/1.1` is offered.
    pub alpn_h2: bool,
}

impl Default for ConnectConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            nodelay: true,
            keepalive: Some(Duration::from_secs(60)),
            send_buffer_size: None,
            recv_buffer_size: None,
            local_address: None,
            tls: None,
            http2_prior_knowledge: false,
            alpn_h2: true,
        }
    }
}

#[derive(Debug)]
pub struct Connected {
    pub io: Io,
    /// From ALPN, or `http2_prior_knowledge` for plaintext.
    pub protocol: Protocol,
}

/// Resolves, connects and (for https) handshakes. Failures before any request byte is sent are
/// `Recovery::Unprocessed`, except certificate/config errors which are `Never`.
///
/// # Errors
/// `Dns`, `Connect`, `Tls`, `Timeout` or `InvalidRequest` kinds.
pub async fn connect(
    origin: &OriginKey,
    cfg: &ConnectConfig,
    obs: &dyn Observer,
) -> Result<Connected> {
    let tls =
        if origin.is_tls() {
            Some(cfg.tls.as_ref().ok_or_else(|| {
                Error::invalid_request("https origin but no TlsConfig configured")
            })?)
        } else {
            None
        };
    let timed = obs.enabled();
    let host = origin.connect_host();

    // DNS (skipped for IP literals).
    let addrs: Vec<SocketAddr> = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, origin.port)]
    } else {
        let t = timed.then(Instant::now);
        let r = match timeout(cfg.connect_timeout, lookup_host((host, origin.port))).await {
            Ok(Ok(it)) => {
                let v: Vec<_> = it.collect();
                if v.is_empty() {
                    Err(Error::dns(format!("no addresses for {host}")))
                } else {
                    Ok(v)
                }
            }
            Ok(Err(e)) => Err(Error::dns(e)),
            Err(_) => Err(Error::unprocessed(ErrorKind::Dns, "DNS lookup timed out")),
        };
        if let Some(t) = t {
            obs.connect_phase(origin, ConnectPhase::Dns, t.elapsed(), r.is_ok());
        }
        r?
    };

    // TCP: sequential attempts (Happy Eyeballs comes in a later phase).
    let t = timed.then(Instant::now);
    let tcp = connect_tcp(&addrs, cfg).await;
    if let Some(t) = t {
        obs.connect_phase(origin, ConnectPhase::Tcp, t.elapsed(), tcp.is_ok());
    }
    let tcp = tcp?;

    let Some(tls) = tls else {
        let protocol = if cfg.http2_prior_knowledge {
            Protocol::Http2
        } else {
            Protocol::Http1
        };
        return Ok(Connected {
            io: Io::Tcp(tcp),
            protocol,
        });
    };

    // TLS.
    let t = timed.then(Instant::now);
    let r = handshake(tcp, origin, tls, cfg).await;
    if let Some(t) = t {
        obs.connect_phase(origin, ConnectPhase::Tls, t.elapsed(), r.is_ok());
    }
    let stream = r?;
    let protocol = match stream.get_ref().1.alpn_protocol() {
        Some(p) if p == ALPN_H2 => Protocol::Http2,
        _ => Protocol::Http1,
    };
    Ok(Connected {
        io: Io::Tls(Box::new(stream)),
        protocol,
    })
}

async fn connect_tcp(addrs: &[SocketAddr], cfg: &ConnectConfig) -> Result<TcpStream> {
    let mut last = None;
    for &addr in addrs {
        match timeout(cfg.connect_timeout, connect_addr(addr, cfg)).await {
            Ok(Ok(s)) => {
                configure_socket(&s, cfg).map_err(Error::connect)?;
                return Ok(s);
            }
            Ok(Err(e)) => last = Some(Error::connect(AddrError { addr, source: e })),
            Err(_) => {
                last = Some(Error::unprocessed(
                    ErrorKind::Timeout,
                    format!(
                        "connect to {addr} timed out after {:?}",
                        cfg.connect_timeout
                    ),
                ));
            }
        }
    }
    Err(last.unwrap_or_else(|| Error::connect("no addresses to connect to")))
}

async fn connect_addr(addr: SocketAddr, cfg: &ConnectConfig) -> io::Result<TcpStream> {
    let sock = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    if let Some(n) = cfg.send_buffer_size {
        sock.set_send_buffer_size(n)?;
    }
    if let Some(n) = cfg.recv_buffer_size {
        sock.set_recv_buffer_size(n)?;
    }
    if let Some(ip) = cfg.local_address {
        if ip.is_ipv4() != addr.is_ipv4() {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "local_address is of the other address family",
            ));
        }
        sock.bind(SocketAddr::new(ip, 0))?;
    }
    sock.connect(addr).await
}

fn configure_socket(s: &TcpStream, cfg: &ConnectConfig) -> io::Result<()> {
    let sock = socket2::SockRef::from(s);
    sock.set_tcp_nodelay(cfg.nodelay)?;
    if let Some(idle) = cfg.keepalive {
        sock.set_tcp_keepalive(&socket2::TcpKeepalive::new().with_time(idle))?;
    }
    Ok(())
}

async fn handshake(
    tcp: TcpStream,
    origin: &OriginKey,
    tls: &TlsConfig,
    cfg: &ConnectConfig,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let name =
        ServerName::try_from(origin.connect_host().to_owned()).map_err(Error::invalid_request)?;
    let rustls = if cfg.alpn_h2 {
        &tls.rustls
    } else {
        &tls.rustls_h1_only
    };
    let connector = TlsConnector::from(std::sync::Arc::clone(rustls));
    match timeout(cfg.connect_timeout, connector.connect(name, tcp)).await {
        Ok(Ok(s)) => Ok(s),
        Ok(Err(e)) => {
            // rustls-level failures (bad cert, no common version) will not fix themselves; I/O ones may.
            let rustls_err = e.get_ref().is_some_and(|i| i.is::<rustls::Error>());
            Err(if rustls_err {
                Error::tls(e)
            } else {
                Error::unprocessed(ErrorKind::Tls, e)
            })
        }
        Err(_) => Err(Error::unprocessed(
            ErrorKind::Timeout,
            "TLS handshake timed out",
        )),
    }
}

#[derive(Debug)]
struct AddrError {
    addr: SocketAddr,
    source: io::Error,
}

impl std::fmt::Display for AddrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "connect to {}", self.addr)
    }
}

impl std::error::Error for AddrError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

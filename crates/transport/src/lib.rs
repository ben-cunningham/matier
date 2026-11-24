//! Matier transport: TCP/TLS connection establishment, the caller-driven HTTP/1.1 connection
//! ([`h1::H1Conn`]) and the HTTP/2 connection actor ([`h2`]).

mod connect;
mod io;
pub mod tls;

pub mod h1;
pub mod h2;

use std::pin::Pin;
use std::task::{Context, Poll};

pub use connect::{ConnectConfig, Connected, connect};
pub use io::Io;
use matier_core::{BytesView, Error};
pub use tls::TlsConfig;

/// Response body produced by either protocol. An enum (not a `Box<dyn Body>`) so responses
/// need no extra allocation.
pub enum Incoming {
    /// No body (HEAD, 204, 304, `Content-Length: 0`, ...).
    Empty,
    /// HTTP/1.1 body; owns the connection until the body completes.
    H1(h1::RecvBody),
    /// HTTP/2 stream body.
    H2(h2::RecvStream),
}

impl std::fmt::Debug for Incoming {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Empty => "Incoming::Empty",
            Self::H1(_) => "Incoming::H1",
            Self::H2(_) => "Incoming::H2",
        })
    }
}

impl Incoming {
    /// The connection that served this response (protocol, whether it was reused, its id), or `None` for a
    /// response without a body (`Incoming::Empty`; the body-less case never needed the connection).
    #[must_use]
    pub fn connection_info(&self) -> Option<matier_core::ConnectionInfo> {
        match self {
            Self::Empty => None,
            Self::H1(b) => Some(b.connection_info()),
            Self::H2(b) => Some(b.connection_info()),
        }
    }
}

impl http_body::Body for Incoming {
    type Data = BytesView;
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<BytesView>, Error>>> {
        match self.get_mut() {
            Self::Empty => Poll::Ready(None),
            Self::H1(b) => Pin::new(b).poll_frame(cx),
            Self::H2(b) => Pin::new(b).poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::H1(b) => b.is_end_stream(),
            Self::H2(b) => b.is_end_stream(),
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        match self {
            Self::Empty => http_body::SizeHint::with_exact(0),
            Self::H1(b) => b.size_hint(),
            Self::H2(b) => b.size_hint(),
        }
    }
}

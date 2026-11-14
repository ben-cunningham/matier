//! `Io`: plaintext or TLS stream behind one type.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

/// TCP or TLS stream. Real `poll_write_vectored` on both (`WSASend` with several `WSABUF`s on Windows).
#[derive(Debug)]
pub enum Io {
    Tcp(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl Io {
    /// The underlying TCP socket.
    #[must_use]
    pub fn tcp(&self) -> &TcpStream {
        match self {
            Self::Tcp(s) => s,
            Self::Tls(s) => s.get_ref().0,
        }
    }

    /// ALPN protocol negotiated on a TLS connection.
    #[must_use]
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        match self {
            Self::Tcp(_) => None,
            Self::Tls(s) => s.get_ref().1.alpn_protocol(),
        }
    }
}

macro_rules! delegate {
    ($self:ident, $s:ident => $e:expr) => {
        match $self.get_mut() {
            Io::Tcp($s) => $e(Pin::new($s)),
            Io::Tls($s) => $e(Pin::new(&mut **$s)),
        }
    };
}

impl AsyncRead for Io {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        delegate!(self, s => |p: Pin<_>| AsyncRead::poll_read(p, cx, buf))
    }
}

impl AsyncWrite for Io {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        delegate!(self, s => |p: Pin<_>| AsyncWrite::poll_write(p, cx, buf))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        delegate!(self, s => |p: Pin<_>| AsyncWrite::poll_write_vectored(p, cx, bufs))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        delegate!(self, s => |p: Pin<_>| AsyncWrite::poll_flush(p, cx))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        delegate!(self, s => |p: Pin<_>| AsyncWrite::poll_shutdown(p, cx))
    }
}

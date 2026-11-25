//! `Io`: plaintext or TLS stream behind one type.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, BufReader, ReadBuf};
use tokio::net::TcpStream;
use tokio_native_tls::TlsStream;

/// TCP or TLS stream. TCP supports vectored writes; native TLS uses the scalar fallback.
#[derive(Debug)]
pub enum Io {
    Tcp(TcpStream),
    Tls(Box<TlsStream<TlsTcp>>),
}

impl Io {
    /// The underlying TCP socket.
    #[must_use]
    pub fn tcp(&self) -> &TcpStream {
        match self {
            Self::Tcp(s) => s,
            Self::Tls(s) => s.get_ref().get_ref().get_ref().tcp.get_ref(),
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
        match self {
            Self::Tcp(s) => s.is_write_vectored(),
            Self::Tls(s) => s.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        delegate!(self, s => |p: Pin<_>| AsyncWrite::poll_flush(p, cx))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        delegate!(self, s => |p: Pin<_>| AsyncWrite::poll_shutdown(p, cx))
    }
}

/// Two full TLS records. Secure Transport and OpenSSL read each record from the socket as a 5-byte
/// header and then its body; reading ahead turns those into one `recv` for several records.
const TLS_READ_BUFFER: usize = 32 * 1024;

/// The TCP socket under a TLS stream, with read-ahead. It also records when the socket itself fails
/// or closes, which is how a broken connection during the handshake is told apart from a TLS rejection.
#[derive(Debug)]
pub struct TlsTcp {
    tcp: BufReader<TcpStream>,
    failed: Arc<AtomicBool>,
}

impl TlsTcp {
    pub(crate) fn new(tcp: TcpStream, failed: Arc<AtomicBool>) -> Self {
        Self {
            tcp: BufReader::with_capacity(TLS_READ_BUFFER, tcp),
            failed,
        }
    }

    fn note<T>(&self, r: &Poll<io::Result<T>>) {
        if matches!(r, Poll::Ready(Err(_))) {
            self.failed.store(true, Ordering::Relaxed);
        }
    }
}

impl AsyncRead for TlsTcp {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        let before = buf.filled().len();
        let r = Pin::new(&mut me.tcp).poll_read(cx, buf);
        me.note(&r);
        if matches!(r, Poll::Ready(Ok(()))) && buf.filled().len() == before && buf.remaining() > 0 {
            me.failed.store(true, Ordering::Relaxed); // end of stream
        }
        r
    }
}

impl AsyncWrite for TlsTcp {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let me = self.get_mut();
        let r = Pin::new(&mut me.tcp).poll_write(cx, buf);
        me.note(&r);
        r
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        let r = Pin::new(&mut me.tcp).poll_flush(cx);
        me.note(&r);
        r
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().tcp).poll_shutdown(cx)
    }
}

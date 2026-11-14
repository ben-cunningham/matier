use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use http_body::{Frame, SizeHint};
use matier_core::{BytesView, ConnectionInfo, Error, ErrorKind, Recovery};
use matier_h1::ChunkedDecoder;

use super::Releaser;
use super::conn::H1Conn;

pub(super) enum Decoder {
    /// Bytes still expected.
    Length(u64),
    Chunked(Box<ChunkedDecoder>),
    /// Body runs until EOF.
    Close,
}

/// HTTP/1.1 response body. Owns the connection; yields zero-copy [`BytesView`]s of the read
/// buffer. When the final byte is observed on a keep-alive connection the connection is given to
/// the release callback (eagerly, not at drop). Dropping before completion closes the connection.
pub struct RecvBody {
    conn: Option<H1Conn>,
    dec: Decoder,
    keep: bool,
    release: Option<Releaser>,
    hint: Option<u64>,
    info: ConnectionInfo,
}

impl fmt::Debug for RecvBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecvBody")
            .field("finished", &self.conn.is_none())
            .finish_non_exhaustive()
    }
}

impl RecvBody {
    pub(super) fn new(
        conn: H1Conn,
        dec: Decoder,
        keep: bool,
        release: Releaser,
        info: ConnectionInfo,
    ) -> Self {
        let hint = match &dec {
            Decoder::Length(n) => Some(*n),
            _ => None,
        };
        Self {
            conn: Some(conn),
            dec,
            keep,
            release: Some(release),
            hint,
            info,
        }
    }

    /// The connection that served this response.
    #[must_use]
    pub fn connection_info(&self) -> ConnectionInfo {
        self.info
    }

    /// Body complete: reuse the connection if allowed, otherwise close it.
    fn finish(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            let rel = self.release.take();
            if self.keep && conn.inner.rbuf.is_empty() {
                conn.inner.last_used = Instant::now();
                if let Some(rel) = rel {
                    rel.release(conn);
                }
            }
        }
    }

    fn fail(&mut self, e: Error) -> Poll<Option<Result<Frame<BytesView>, Error>>> {
        self.conn = None;
        self.release = None;
        Poll::Ready(Some(Err(e)))
    }
}

fn eof_error() -> Error {
    Error::with_source(
        ErrorKind::BodyRead,
        Recovery::Never,
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed before the response body was complete",
        ),
    )
}

impl http_body::Body for RecvBody {
    type Data = BytesView;
    type Error = Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<BytesView>, Error>>> {
        let this = &mut *self;
        loop {
            let Some(conn) = this.conn.as_mut() else {
                return Poll::Ready(None);
            };
            let inner = &mut *conn.inner;
            match &mut this.dec {
                Decoder::Length(rem) => {
                    let avail = inner.rbuf.len();
                    if avail > 0 {
                        let n = avail.min(usize::try_from(*rem).unwrap_or(usize::MAX));
                        let v = inner.rbuf.consume(n);
                        *rem -= n as u64;
                        let done = *rem == 0;
                        if let Some(h) = this.hint.as_mut() {
                            *h = *rem;
                        }
                        if done {
                            this.finish();
                        }
                        return Poll::Ready(Some(Ok(Frame::data(v))));
                    }
                }
                Decoder::Chunked(d) => {
                    if !inner.rbuf.is_empty() {
                        let view = inner.rbuf.peek();
                        let r = match d.decode(view.first_slice()) {
                            Ok(r) => r,
                            Err(e) => {
                                return this.fail(Error::with_source(
                                    ErrorKind::Protocol,
                                    Recovery::Never,
                                    e,
                                ));
                            }
                        };
                        drop(view);
                        if r.consumed > 0 {
                            let v = inner.rbuf.consume(r.consumed);
                            let data = r.data.map(|rg| v.range(rg));
                            drop(v);
                            if r.done {
                                this.finish();
                            }
                            if let Some(data) = data {
                                return Poll::Ready(Some(Ok(Frame::data(data))));
                            }
                            if r.done {
                                return Poll::Ready(None);
                            }
                            continue;
                        }
                        if r.done {
                            this.finish();
                            return Poll::Ready(None);
                        }
                        // Decoder needs more input than the first slice holds and made no progress.
                        if inner.rbuf.peek().first_slice().is_empty() {
                            return this.fail(eof_error());
                        }
                    }
                }
                Decoder::Close => {
                    if !inner.rbuf.is_empty() {
                        let v = inner.rbuf.consume_all();
                        return Poll::Ready(Some(Ok(Frame::data(v))));
                    }
                }
            }
            // Need more bytes.
            let idle = inner.body_timeout;
            match inner.poll_fill_timed(cx, idle) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => {
                    let kind = if e.kind() == io::ErrorKind::TimedOut {
                        ErrorKind::Timeout
                    } else {
                        ErrorKind::BodyRead
                    };
                    return this.fail(Error::with_source(kind, Recovery::Never, e));
                }
                Poll::Ready(Ok(0)) => {
                    if matches!(this.dec, Decoder::Close) {
                        this.keep = false;
                        this.finish();
                        return Poll::Ready(None);
                    }
                    return this.fail(eof_error());
                }
                Poll::Ready(Ok(_)) => inner.wait_dl = None,
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.conn.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        match (&self.dec, self.hint) {
            (Decoder::Length(_), Some(n)) => SizeHint::with_exact(n),
            _ => SizeHint::default(),
        }
    }
}

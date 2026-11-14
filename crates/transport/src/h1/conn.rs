use std::borrow::BorrowMut;
use std::fmt;
use std::future::Future;
use std::future::poll_fn;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use futures_util::future::Either;
use http::header::CONNECTION;
use http::{HeaderMap, Method};
use matier_core::buf::{self, GlobalPool};
use matier_core::{
    Body, BytesBuf, BytesView, ConnectionInfo, Error, ErrorKind, OriginKey, Protocol, Recovery,
    Result,
};
use matier_h1::{
    BodyKind, BodyLength, ChunkedDecoder, EncodeOptions, Limits, ParseStatus, ResponseHead,
    chunk_header, encode_last_chunk, encode_request_head, keep_alive, parse_response_head,
    response_body_kind,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};

use super::body::{Decoder, RecvBody};
use super::{Release, ReleaseFn, Releaser};
use crate::{Incoming, Io};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Request bodies up to this size are copied into the head buffer (one `write`).
const INLINE_BODY_MAX: usize = 1024;
const MIN_READ: usize = 1024;
const INITIAL_READ: usize = 4096;
const MAX_IOV: usize = 16;

/// HTTP/1.1 connection settings.
#[derive(Clone, Debug)]
pub struct H1Config {
    /// Response head parser limits.
    pub limits: Limits,
    /// Write `Content-Type` instead of `content-type`.
    pub title_case_headers: bool,
    /// Upper bound for a single socket read (the read size adapts between 4 KiB and this).
    pub max_buffer: usize,
}

impl Default for H1Config {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            title_case_headers: false,
            max_buffer: 64 * 1024,
        }
    }
}

/// Marker source attached to errors that look like the stale keep-alive race: the request failed
/// to write, or the peer closed the (reused) connection before sending any response byte. The
/// pool may transparently retry such a request once when it is idempotent or its body replayable.
#[derive(Debug)]
pub struct StaleKeepAlive(pub io::Error);

impl fmt::Display for StaleKeepAlive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "reused connection was closed by the peer: {}", self.0)
    }
}

impl std::error::Error for StaleKeepAlive {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// True when `err` carries a [`StaleKeepAlive`] marker.
#[must_use]
pub fn is_stale_keepalive(err: &Error) -> bool {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(err);
    while let Some(e) = cur {
        if e.is::<StaleKeepAlive>() {
            return true;
        }
        cur = e.source();
    }
    false
}

pub(super) struct Inner {
    pub(super) io: Io,
    pub(super) rbuf: BytesBuf,
    mem: GlobalPool,
    whead: Vec<u8>,
    id: u64,
    created_at: Instant,
    pub(super) last_used: Instant,
    origin: OriginKey,
    cfg: Arc<H1Config>,
    uses: u32,
    read_hint: usize,
    owner: Option<std::sync::Weak<dyn Release>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(o) = self.owner.take().and_then(|w| w.upgrade()) {
            o.closed(self.id, self.created_at.elapsed());
        }
    }
}

impl Inner {
    /// Reads more bytes from the socket into the pooled buffer.
    pub(super) fn poll_fill(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        if self.rbuf.remaining_capacity() < MIN_READ {
            self.rbuf.reserve(self.read_hint, &self.mem);
        }
        let mut slice = self.rbuf.first_unfilled_slice();
        if slice.is_empty() {
            self.rbuf.reserve(self.read_hint, &self.mem);
            slice = self.rbuf.first_unfilled_slice();
        }
        let cap = slice.len();
        let mut rb = ReadBuf::uninit(slice);
        std::task::ready!(Pin::new(&mut self.io).poll_read(cx, &mut rb))?;
        let n = rb.filled().len();
        // SAFETY: `ReadBuf` initialised exactly `n` bytes at the start of the unfilled slice.
        unsafe { self.rbuf.advance(n) };
        if n == cap && self.read_hint < self.cfg.max_buffer {
            self.read_hint = (self.read_hint * 2).min(self.cfg.max_buffer.max(INITIAL_READ));
        }
        Poll::Ready(Ok(n))
    }
}

/// Caller-driven HTTP/1.1 connection. See the [module docs](super).
pub struct H1Conn {
    pub(super) inner: Box<Inner>,
}

impl fmt::Debug for H1Conn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("H1Conn")
            .field("id", &self.inner.id)
            .field("origin", &self.inner.origin.to_string())
            .finish_non_exhaustive()
    }
}

impl H1Conn {
    /// Wraps an established connection.
    #[must_use]
    pub fn new(io: Io, origin: OriginKey, cfg: H1Config) -> Self {
        Self::new_shared(io, origin, Arc::new(cfg))
    }

    /// Like [`H1Conn::new`] but shares one config between connections (no allocation).
    #[must_use]
    pub fn new_shared(io: Io, origin: OriginKey, cfg: Arc<H1Config>) -> Self {
        let now = Instant::now();
        Self {
            inner: Box::new(Inner {
                io,
                rbuf: BytesBuf::new(),
                mem: buf::memory(),
                whead: Vec::with_capacity(512),
                id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
                created_at: now,
                last_used: now,
                origin,
                cfg,
                uses: 0,
                read_hint: INITIAL_READ,
                owner: None,
            }),
        }
    }

    /// Registers `owner` to be told (via [`Release::closed`]) when this connection is finally
    /// dropped. Held weakly so idle connections do not keep their pool alive.
    pub fn set_owner(&mut self, owner: std::sync::Weak<dyn Release>) {
        self.inner.owner = Some(owner);
    }

    /// Process-unique connection id (`ConnectionInfo::conn_id`).
    #[must_use]
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    /// The origin this connection is established to.
    #[must_use]
    pub fn origin(&self) -> &OriginKey {
        &self.inner.origin
    }

    /// When the connection was created.
    #[must_use]
    pub fn created_at(&self) -> Instant {
        self.inner.created_at
    }

    /// When the connection last finished a request (or was created).
    #[must_use]
    pub fn last_used(&self) -> Instant {
        self.inner.last_used
    }

    /// Number of requests this connection has carried.
    #[must_use]
    pub fn requests_sent(&self) -> u32 {
        self.inner.uses
    }

    /// Non-blocking liveness probe for an idle connection: `false` if the peer closed it, reset
    /// it, or sent unexpected bytes. A `Pending` read means alive. Usually costs no syscall
    /// (tokio's cached readiness).
    pub fn is_reusable_now(&mut self) -> bool {
        let inner = &mut *self.inner;
        if !inner.rbuf.is_empty() {
            return false;
        }
        let mut cx = Context::from_waker(Waker::noop());
        let mut probe = [0u8; 1];
        let mut rb = ReadBuf::new(&mut probe);
        Pin::new(&mut inner.io)
            .poll_read(&mut cx, &mut rb)
            .is_pending()
    }

    /// Sends `req` and returns the response once its head has been read. `on_release` receives
    /// the connection when the response body completes cleanly on a keep-alive connection.
    ///
    /// The head is encoded synchronously and the request's headers are dropped before the
    /// returned future is first polled; the future only keeps the body (keeps it small).
    ///
    /// # Errors
    /// See [`H1Conn::send_mut`].
    pub fn send(
        mut self,
        mut req: http::Request<Body>,
        on_release: impl ReleaseFn,
    ) -> impl Future<Output = Result<http::Response<Incoming>>> + Send {
        let prep = self.prepare(&req);
        let body = std::mem::take(req.body_mut());
        drop(req);
        let rel = Releaser::Fn(Box::new(on_release));
        match prep {
            Ok(prep) => Either::Left(self.exchange(body, prep, rel)),
            Err(e) => Either::Right(std::future::ready(Err(e))),
        }
    }

    /// Allocation-free variant of [`H1Conn::send`] that borrows the request: on failure with a
    /// replayable body (`Empty`/`Full`) the request is intact and may be sent again. A `Stream`
    /// body is consumed.
    ///
    /// # Errors
    /// `Io`/`Retryable` for transport failures (marked with [`StaleKeepAlive`] when it looks like
    /// a stale reused connection), `Protocol` for malformed responses, `InvalidRequest` when the
    /// request cannot be encoded.
    pub fn send_mut<'a>(
        mut self,
        req: &'a mut http::Request<Body>,
        releaser: Arc<dyn Release>,
    ) -> impl Future<Output = Result<http::Response<Incoming>>> + Send + use<'a> {
        let prep = self.prepare(req);
        let body = req.body_mut();
        let rel = Releaser::Shared(releaser);
        match prep {
            Ok(prep) => Either::Left(self.exchange(body, prep, rel)),
            Err(e) => Either::Right(std::future::ready(Err(e))),
        }
    }

    /// Synchronous part of a send: encodes the head (and a small body) into the reusable buffer.
    fn prepare(&mut self, req: &http::Request<Body>) -> Result<Prep> {
        let reused = self.inner.uses > 0;
        self.inner.uses = self.inner.uses.saturating_add(1);
        let inner = &mut *self.inner;
        let opts = EncodeOptions {
            title_case_headers: inner.cfg.title_case_headers,
            absolute_form: false,
        };
        let blen = match req.body() {
            Body::Empty => BodyLength::None,
            Body::Full(v) => BodyLength::Known(v.len() as u64),
            Body::Stream(_) => match req.body().size_hint() {
                Some(n) => BodyLength::Known(n),
                None => BodyLength::Chunked,
            },
        };
        inner.whead.clear();
        encode_request_head(
            req.method(),
            req.uri(),
            req.headers(),
            blen,
            &opts,
            &mut inner.whead,
        )
        .map_err(Error::invalid_request)?;
        let mut inlined = false;
        if let Body::Full(v) = req.body()
            && v.len() <= INLINE_BODY_MAX
        {
            for (s, _) in v.slices() {
                inner.whead.extend_from_slice(s);
            }
            inlined = true;
        }
        Ok(Prep {
            reused,
            req_close: wants_close(req.headers()),
            inlined,
            blen,
            method: req.method().clone(),
        })
    }

    #[allow(
        clippy::manual_async_fn,
        reason = "async fn would store the arguments twice (future size budget)"
    )]
    /// Written as an `async move` block (not an `async fn`) so the captured arguments are stored
    /// once instead of twice (async-fn parameters get copied into locals).
    fn exchange<B: BorrowMut<Body> + Send>(
        mut self,
        mut body: B,
        prep: Prep,
        rel: Releaser,
    ) -> impl Future<Output = Result<http::Response<Incoming>>> + Send {
        async move {
            self.write_phase(body.borrow_mut(), &prep).await?;
            let head = self.read_final_head(prep.reused).await?;
            self.finish(head, &prep, rel)
        }
    }

    /// Builds the response (and decides whether the connection goes back to the pool).
    fn finish(
        mut self,
        head: ResponseHead,
        prep: &Prep,
        rel: Releaser,
    ) -> Result<http::Response<Incoming>> {
        let framing = response_body_kind(&prep.method, &head).map_err(Error::protocol)?;
        let keep = keep_alive(&head) && !framing.must_close && !prep.req_close;
        let info = ConnectionInfo {
            protocol: Protocol::Http1,
            reused: prep.reused,
            conn_id: self.inner.id,
        };
        let decoder = match framing.kind {
            BodyKind::Empty | BodyKind::Length(0) => None,
            BodyKind::Length(n) => Some(Decoder::Length(n)),
            BodyKind::Chunked => Some(Decoder::Chunked(Box::new(ChunkedDecoder::new()))),
            BodyKind::CloseDelimited => Some(Decoder::Close),
        };
        let incoming = if let Some(decoder) = decoder {
            Incoming::H1(RecvBody::new(self, decoder, keep, rel))
        } else {
            if keep && self.inner.rbuf.is_empty() {
                self.inner.last_used = Instant::now();
                rel.release(self);
            }
            Incoming::Empty
        };
        let mut resp = http::Response::new(incoming);
        *resp.status_mut() = head.status;
        *resp.version_mut() = head.version;
        *resp.headers_mut() = head.headers;
        resp.extensions_mut().insert(info);
        Ok(resp)
    }

    async fn write_phase(&mut self, body: &mut Body, prep: &Prep) -> Result<()> {
        let inner = &mut *self.inner;
        let reused = prep.reused;
        let map_write = move |e: io::Error| -> Error {
            if reused {
                Error::with_source(ErrorKind::Io, Recovery::Retryable, StaleKeepAlive(e))
            } else {
                Error::io(e)
            }
        };
        match body {
            Body::Full(v) if !prep.inlined => {
                write_all(&mut inner.io, &inner.whead, v, &[])
                    .await
                    .map_err(map_write)?;
            }
            Body::Stream(_) => {
                inner.io.write_all(&inner.whead).await.map_err(map_write)?;
                // Cold path: boxed so its large state does not inflate the hot future.
                Box::pin(write_stream(inner, body, prep.blen)).await?;
            }
            _ => inner.io.write_all(&inner.whead).await.map_err(map_write)?,
        }
        Ok(())
    }

    /// Reads response heads until a final (non-1xx) one; consumes it from the buffer.
    async fn read_final_head(&mut self, reused: bool) -> Result<ResponseHead> {
        loop {
            let (head, head_len) = self.read_head(reused).await?;
            let _ = self.inner.rbuf.consume(head_len);
            if head.status.is_informational() {
                if head.status.as_u16() == 101 {
                    return Err(Error::protocol("unexpected 101 Switching Protocols"));
                }
                continue;
            }
            return Ok(head);
        }
    }

    /// Reads until a full response head is buffered; returns it with its length (not consumed).
    async fn read_head(&mut self, reused: bool) -> Result<(ResponseHead, usize)> {
        let inner = &mut *self.inner;
        let mut got_bytes = !inner.rbuf.is_empty();
        loop {
            if !inner.rbuf.is_empty() {
                match try_parse(&inner.rbuf, &inner.cfg.limits).map_err(Error::protocol)? {
                    ParseStatus::Complete { value, head_len } => return Ok((value, head_len)),
                    ParseStatus::Partial => {}
                }
            }
            let n = poll_fn(|cx| inner.poll_fill(cx)).await.map_err(|e| {
                if reused && !got_bytes {
                    Error::with_source(ErrorKind::Io, Recovery::Retryable, StaleKeepAlive(e))
                } else {
                    Error::io(e)
                }
            })?;
            if n == 0 {
                let e = io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed before the response head was complete",
                );
                return Err(if reused && !got_bytes {
                    Error::with_source(ErrorKind::Io, Recovery::Retryable, StaleKeepAlive(e))
                } else {
                    Error::io(e)
                });
            }
            got_bytes = true;
        }
    }
}

/// State computed synchronously before the first await.
struct Prep {
    reused: bool,
    req_close: bool,
    /// A small `Full` body was appended to the head buffer.
    inlined: bool,
    blen: BodyLength,
    method: Method,
}

fn wants_close(h: &HeaderMap) -> bool {
    h.get_all(CONNECTION).iter().any(|v| {
        v.as_bytes()
            .split(|&b| b == b',')
            .any(|t| t.trim_ascii().eq_ignore_ascii_case(b"close"))
    })
}

/// Parses from the first contiguous slice; copies into a scratch `Vec` only when the buffered
/// head spans several blocks (rare).
fn try_parse(
    rbuf: &BytesBuf,
    limits: &Limits,
) -> std::result::Result<ParseStatus<ResponseHead>, matier_h1::ParseError> {
    let view = rbuf.peek();
    let first = view.first_slice();
    match parse_response_head(first, limits)? {
        ParseStatus::Partial if first.len() < view.len() => {
            parse_response_head(&view.to_vec(), limits)
        }
        r => Ok(r),
    }
}

/// Writes `head`, then `body`'s spans, then `tail` with as few `writev` calls as possible.
/// Keeps only offsets in the future state (futures stay small); the body is borrowed.
async fn write_all(io: &mut Io, head: &[u8], body: &BytesView, tail: &[u8]) -> io::Result<()> {
    let total = head.len() + body.len() + tail.len();
    let mut done = 0usize;
    while done < total {
        let n = poll_fn(|cx| {
            let mut iov = [IoSlice::new(&[]); MAX_IOV];
            let mut k = 0;
            let mut off = done;
            if off < head.len() {
                iov[0] = IoSlice::new(&head[off..]);
                k = 1;
                off = 0;
            } else {
                off -= head.len();
            }
            if off < body.len() {
                if off == 0 {
                    k += buf::io_slices(body, &mut iov[k..MAX_IOV - 1]);
                } else {
                    let rest = body.range(off..);
                    k += buf::io_slices(&rest, &mut iov[k..MAX_IOV - 1]);
                    // `iov` borrows `rest`: write while it is alive.
                    if !tail.is_empty() {
                        iov[k] = IoSlice::new(tail);
                        k += 1;
                    }
                    return Pin::new(&mut *io).poll_write_vectored(cx, &iov[..k]);
                }
                off = 0;
            } else {
                off -= body.len();
            }
            if !tail.is_empty() && (off < tail.len()) {
                iov[k] = IoSlice::new(&tail[off..]);
                k += 1;
            }
            Pin::new(&mut *io).poll_write_vectored(cx, &iov[..k])
        })
        .await?;
        if n == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        done += n;
    }
    Ok(())
}

async fn write_stream(inner: &mut Inner, body: &mut Body, blen: BodyLength) -> Result<()> {
    use http_body::Body as _;
    let mut sent: u64 = 0;
    let mut trailers = None;
    loop {
        let frame = poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await;
        let Some(frame) = frame else { break };
        let frame = frame?;
        let data = match frame.into_data() {
            Ok(d) => d,
            Err(f) => {
                if let Ok(t) = f.into_trailers() {
                    trailers = Some(t);
                }
                continue;
            }
        };
        if data.is_empty() {
            continue;
        }
        sent += data.len() as u64;
        let r = match blen {
            BodyLength::Chunked => {
                let mut hb = [0u8; 18];
                let hdr = chunk_header(data.len(), &mut hb);
                write_all(&mut inner.io, hdr, &data, matier_h1::CHUNK_END).await
            }
            _ => write_all(&mut inner.io, &[], &data, &[]).await,
        };
        r.map_err(Error::io)?;
    }
    match blen {
        BodyLength::Chunked => {
            inner.whead.clear();
            encode_last_chunk(trailers.as_ref(), &mut inner.whead)
                .map_err(Error::invalid_request)?;
            inner.io.write_all(&inner.whead).await.map_err(Error::io)?;
        }
        BodyLength::Known(n) if n != sent => {
            return Err(Error::with_source(
                ErrorKind::BodyWrite,
                Recovery::Never,
                format!("request body produced {sent} bytes, declared {n}"),
            ));
        }
        _ => {}
    }
    Ok(())
}

/// Helper for tests/pool: is `method` safe to replay after an ambiguous failure (RFC 9110 9.2.2).
#[must_use]
pub fn is_idempotent(method: &Method) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE | Method::PUT | Method::DELETE
    )
}

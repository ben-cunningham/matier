//! Request body.

use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytesbuf::BytesView;
use http_body::{Frame, SizeHint};
use tokio::sync::mpsc;

use crate::error::{Error, ErrorKind, Recovery, Result};

/// Request body.
#[derive(Default)]
#[allow(
    clippy::large_enum_variant,
    reason = "BytesView is ~272 bytes (8 inline spans); boxing would cost an allocation per request"
)]
pub enum Body {
    #[default]
    Empty,
    /// Fast path: cloneable (for retries), sent as iovecs, zero-copy.
    Full(BytesView),
    /// From [`Body::channel`].
    Stream(BodyStream),
}

impl fmt::Debug for Body {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("Body::Empty"),
            Self::Full(v) => write!(f, "Body::Full({} bytes)", v.len()),
            Self::Stream(_) => f.write_str("Body::Stream"),
        }
    }
}

impl Body {
    #[must_use]
    pub const fn empty() -> Self {
        Self::Empty
    }

    pub fn full(data: impl Into<BytesView>) -> Self {
        let v = data.into();
        if v.is_empty() {
            Self::Empty
        } else {
            Self::Full(v)
        }
    }

    #[must_use]
    pub fn from_static(data: &'static [u8]) -> Self {
        Self::full(crate::buf::from_static(data))
    }

    /// Bounded single-producer single-consumer body. `buffer` is the number of chunks that may be queued
    /// before `BodySender::send` waits (backpressure). Clamped to at least 1.
    // v0: tokio's mpsc. A purpose-built SPSC (no multi-producer machinery, byte-credit accounting) comes later.
    #[must_use]
    pub fn channel(buffer: usize) -> (BodySender, Self) {
        let (tx, rx) = mpsc::channel(buffer.max(1));
        let abort = Arc::new(Mutex::new(None));
        (
            BodySender {
                tx,
                abort: Arc::clone(&abort),
                finished: false,
            },
            Self::Stream(BodyStream {
                rx,
                abort,
                done: false,
            }),
        )
    }

    /// Exact length when known (`Some` for Empty/Full, `None` for streams).
    #[must_use]
    pub fn size_hint(&self) -> Option<u64> {
        match self {
            Self::Empty => Some(0),
            Self::Full(v) => Some(v.len() as u64),
            Self::Stream(_) => None,
        }
    }

    /// `Some` for Empty/Full (cheap: refcount bumps), `None` for streams.
    #[must_use]
    pub fn try_clone(&self) -> Option<Self> {
        match self {
            Self::Empty => Some(Self::Empty),
            Self::Full(v) => Some(Self::Full(v.clone())),
            Self::Stream(_) => None,
        }
    }

    #[must_use]
    pub fn is_empty_hint(&self) -> bool {
        self.size_hint() == Some(0)
    }
}

impl From<BytesView> for Body {
    fn from(v: BytesView) -> Self {
        Self::full(v)
    }
}
impl From<bytes::Bytes> for Body {
    fn from(b: bytes::Bytes) -> Self {
        Self::full(crate::buf::from_bytes(b))
    }
}
impl From<Vec<u8>> for Body {
    fn from(b: Vec<u8>) -> Self {
        Self::full(bytes::Bytes::from(b))
    }
}
impl From<&'static str> for Body {
    fn from(s: &'static str) -> Self {
        Self::from_static(s.as_bytes())
    }
}
impl From<String> for Body {
    fn from(s: String) -> Self {
        Self::from(s.into_bytes())
    }
}

/// Receiving half of [`Body::channel`].
pub struct BodyStream {
    rx: mpsc::Receiver<Result<BytesView>>,
    abort: Arc<Mutex<Option<Error>>>,
    done: bool,
}

impl fmt::Debug for BodyStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BodyStream").finish_non_exhaustive()
    }
}

impl BodyStream {
    fn poll_chunk(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<BytesView>>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) => {
                self.done = true;
                // Sender gone: clean finish, or an abort/drop-without-finish recorded an error.
                let err = self
                    .abort
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                Poll::Ready(err.map(Err))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Producer half of [`Body::channel`].
///
/// Dropping the sender without calling [`finish`](Self::finish) aborts the body with
/// `Canceled`, so a producer that dies mid-upload can never look like a complete body.
pub struct BodySender {
    tx: mpsc::Sender<Result<BytesView>>,
    abort: Arc<Mutex<Option<Error>>>,
    finished: bool,
}

impl fmt::Debug for BodySender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BodySender").finish_non_exhaustive()
    }
}

impl BodySender {
    /// Queues a chunk, waiting while the buffer is full. Empty chunks are skipped.
    ///
    /// # Errors
    /// `Canceled` if the receiving side (the request) is gone.
    pub async fn send(&mut self, chunk: BytesView) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        self.tx.send(Ok(chunk)).await.map_err(|_| {
            Error::with_source(
                ErrorKind::Canceled,
                Recovery::Never,
                "body receiver dropped",
            )
        })
    }

    /// Whether the receiving side is gone.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// Ends the stream cleanly.
    pub fn finish(mut self) {
        self.finished = true;
    }

    /// Fails the stream. Chunks already queued are delivered first, then `err`.
    pub fn abort(mut self, err: Error) {
        self.finished = true;
        self.record(err);
    }

    fn record(&self, err: Error) {
        *self
            .abort
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(err);
    }
}

impl Drop for BodySender {
    fn drop(&mut self) {
        if !self.finished {
            self.record(Error::with_source(
                ErrorKind::Canceled,
                Recovery::Never,
                "body sender dropped before finish",
            ));
        }
        // `tx` drops after this, closing the channel; the receiver then observes the recorded error.
    }
}

impl http_body::Body for Body {
    type Data = BytesView;
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<BytesView>>>> {
        let this = self.get_mut();
        match this {
            Self::Empty => Poll::Ready(None),
            Self::Full(_) => {
                let Self::Full(v) = std::mem::replace(this, Self::Empty) else {
                    unreachable!()
                };
                Poll::Ready(Some(Ok(Frame::data(v))))
            }
            Self::Stream(s) => s.poll_chunk(cx).map(|o| o.map(|r| r.map(Frame::data))),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::Full(v) => v.is_empty(),
            Self::Stream(s) => s.done,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Empty => SizeHint::with_exact(0),
            Self::Full(v) => SizeHint::with_exact(v.len() as u64),
            Self::Stream(_) => SizeHint::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;

    use http_body::Body as _;

    use super::*;
    use crate::buf;

    async fn next(b: &mut Body) -> Option<Result<BytesView>> {
        poll_fn(|cx| Pin::new(&mut *b).poll_frame(cx))
            .await
            .map(|r| r.map(|f| f.into_data().ok().unwrap()))
    }

    #[tokio::test]
    async fn empty_and_full() {
        let mut e = Body::empty();
        assert_eq!(e.size_hint(), Some(0));
        assert!(e.is_end_stream());
        assert!(next(&mut e).await.is_none());

        let mut f = Body::from_static(b"hello");
        assert_eq!(f.size_hint(), Some(5));
        assert_eq!(http_body::Body::size_hint(&f).exact(), Some(5));
        assert!(!f.is_end_stream());
        let c = f.try_clone().unwrap();
        assert_eq!(next(&mut f).await.unwrap().unwrap().to_vec(), b"hello");
        assert!(f.is_end_stream());
        assert_eq!(f.size_hint(), Some(0));
        assert!(next(&mut f).await.is_none());
        assert_eq!(c.size_hint(), Some(5));
        assert!(Body::full(buf::copy_from_slice(b"")).is_empty_hint());
        assert!(matches!(Body::from(String::new()), Body::Empty));
    }

    #[tokio::test]
    async fn channel_flow() {
        let (mut tx, mut body) = Body::channel(2);
        assert!(body.try_clone().is_none());
        assert_eq!(body.size_hint(), None);
        let prod = tokio::spawn(async move {
            for i in 0..5u8 {
                tx.send(buf::copy_from_slice(&[i; 3])).await.unwrap();
            }
            tx.finish();
        });
        let mut got = Vec::new();
        while let Some(c) = next(&mut body).await {
            got.extend(c.unwrap().to_vec());
        }
        prod.await.unwrap();
        assert_eq!(got.len(), 15);
        assert!(body.is_end_stream());
    }

    #[tokio::test]
    async fn channel_abort_after_queued() {
        let (mut tx, mut body) = Body::channel(4);
        tx.send(buf::copy_from_slice(b"ab")).await.unwrap();
        tx.abort(Error::io("boom"));
        assert_eq!(next(&mut body).await.unwrap().unwrap().to_vec(), b"ab");
        assert_eq!(
            next(&mut body).await.unwrap().unwrap_err().kind(),
            ErrorKind::Io
        );
        assert!(next(&mut body).await.is_none());
    }

    #[tokio::test]
    async fn dropped_sender_is_not_clean_eof() {
        let (tx, mut body) = Body::channel(1);
        drop(tx);
        assert_eq!(
            next(&mut body).await.unwrap().unwrap_err().kind(),
            ErrorKind::Canceled
        );
    }

    #[tokio::test]
    async fn dropped_receiver_fails_send() {
        let (mut tx, body) = Body::channel(1);
        drop(body);
        assert!(tx.is_closed());
        assert_eq!(
            tx.send(buf::copy_from_slice(b"x"))
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Canceled
        );
        tx.finish();
    }

    #[tokio::test]
    async fn backpressure() {
        let (mut tx, mut body) = Body::channel(1);
        tx.send(buf::copy_from_slice(b"1")).await.unwrap();
        let second = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            tx.send(buf::copy_from_slice(b"2")),
        )
        .await;
        assert!(second.is_err(), "send must wait while buffer is full");
        next(&mut body).await.unwrap().unwrap();
        tx.send(buf::copy_from_slice(b"3")).await.unwrap();
    }

    #[test]
    fn multi_span_io_slices() {
        let v = BytesView::from_views([buf::copy_from_slice(b"ab"), buf::from_static(b"cd")]);
        let mut slices = [std::io::IoSlice::new(&[]); 4];
        assert_eq!(buf::io_slices(&v, &mut slices), 2);
        assert_eq!(&*slices[1], b"cd");
    }
}

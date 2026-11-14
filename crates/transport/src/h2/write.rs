//! Zero-copy write queue: an arena of encoded bytes (frame headers, HPACK blocks, control frames) plus
//! caller-owned `BytesView` payloads, flushed with vectored writes. Body bytes are never copied unless
//! they are tiny (<= `INLINE_PAYLOAD`), where one contiguous arena range keeps the iovec count low.

use std::collections::VecDeque;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

use matier_core::BytesView;
use matier_core::buf::io_slices;
use tokio::io::AsyncWrite;

pub(crate) const MAX_IOVECS: usize = 64;
pub(crate) const INLINE_PAYLOAD: usize = 256;

#[allow(
    clippy::large_enum_variant,
    reason = "BytesView inlines 8 spans; the queue is a reused ring"
)]
enum Seg {
    Arena { start: usize, end: usize },
    Data(BytesView),
}

pub(crate) struct WriteQueue {
    arena: Vec<u8>,
    segs: VecDeque<Seg>,
    pending: usize,
    written_total: u64,
    enqueued_total: u64,
    need_flush: bool,
}

/// Result of one vectored write.
pub(crate) struct Written {
    pub(crate) bytes: usize,
    pub(crate) iovecs: usize,
}

impl WriteQueue {
    pub(crate) fn new() -> Self {
        Self {
            arena: Vec::with_capacity(16 * 1024),
            segs: VecDeque::new(),
            pending: 0,
            written_total: 0,
            enqueued_total: 0,
            need_flush: false,
        }
    }

    /// Bytes queued but not yet written.
    pub(crate) fn pending(&self) -> usize {
        self.pending
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending == 0
    }

    /// Absolute offset (in the byte stream we write) just past everything queued so far.
    pub(crate) fn enqueued_total(&self) -> u64 {
        self.enqueued_total
    }

    pub(crate) fn written_total(&self) -> u64 {
        self.written_total
    }

    #[allow(dead_code)]
    pub(crate) fn queue_len(&self) -> usize {
        self.segs.len()
    }

    /// Start encoding into the arena. Pair with [`Self::commit`].
    pub(crate) fn begin(&mut self) -> (&mut Vec<u8>, usize) {
        if self.segs.is_empty() {
            self.arena.clear();
            if self.arena.capacity() > (1 << 20) {
                self.arena.shrink_to(64 * 1024);
            }
        }
        let start = self.arena.len();
        (&mut self.arena, start)
    }

    /// Register the bytes appended to the arena since `start` as one or more queued segments.
    pub(crate) fn commit(&mut self, start: usize) {
        let end = self.arena.len();
        if end == start {
            return;
        }
        let n = end - start;
        self.pending += n;
        self.enqueued_total += n as u64;
        if let Some(Seg::Arena { end: e, .. }) = self.segs.back_mut()
            && *e == start
        {
            *e = end;
            return;
        }
        self.segs.push_back(Seg::Arena { start, end });
    }

    /// Queue a DATA frame: 9-byte header + payload. Tiny payloads are copied next to the header.
    pub(crate) fn push_frame_with_payload(&mut self, header: [u8; 9], payload: BytesView) {
        if payload.len() <= INLINE_PAYLOAD {
            let (arena, start) = self.begin();
            arena.extend_from_slice(&header);
            for (s, _) in payload.slices() {
                arena.extend_from_slice(s);
            }
            self.commit(start);
        } else {
            let (arena, start) = self.begin();
            arena.extend_from_slice(&header);
            self.commit(start);
            self.pending += payload.len();
            self.enqueued_total += payload.len() as u64;
            self.segs.push_back(Seg::Data(payload));
        }
    }

    /// Issue one vectored write. `Ready(Ok(None))` means the queue is empty.
    pub(crate) fn poll_write<T: AsyncWrite + Unpin>(
        &mut self,
        io: &mut T,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Option<Written>>> {
        if self.pending == 0 {
            return Poll::Ready(Ok(None));
        }
        let mut bufs = [IoSlice::new(&[]); MAX_IOVECS];
        let mut n = 0;
        for seg in &self.segs {
            if n >= MAX_IOVECS {
                break;
            }
            match seg {
                Seg::Arena { start, end } => {
                    bufs[n] = IoSlice::new(&self.arena[*start..*end]);
                    n += 1;
                }
                Seg::Data(v) => n += io_slices(v, &mut bufs[n..]),
            }
        }
        let wrote = match Pin::new(&mut *io).poll_write_vectored(cx, &bufs[..n]) {
            Poll::Ready(Ok(w)) => w,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        };
        if wrote == 0 {
            return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
        }
        self.advance(wrote);
        self.need_flush = true;
        Poll::Ready(Ok(Some(Written {
            bytes: wrote,
            iovecs: n,
        })))
    }

    fn advance(&mut self, mut n: usize) {
        self.pending -= n;
        self.written_total += n as u64;
        while n > 0 {
            match self.segs.front_mut() {
                Some(Seg::Arena { start, end }) => {
                    let l = *end - *start;
                    if n >= l {
                        n -= l;
                        self.segs.pop_front();
                    } else {
                        *start += n;
                        n = 0;
                    }
                }
                Some(Seg::Data(v)) => {
                    if n >= v.len() {
                        n -= v.len();
                        self.segs.pop_front();
                    } else {
                        v.advance(n);
                        n = 0;
                    }
                }
                None => break,
            }
        }
    }

    /// Flush the transport if anything was written since the last flush (TLS needs this).
    pub(crate) fn poll_flush<T: AsyncWrite + Unpin>(
        &mut self,
        io: &mut T,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.need_flush {
            return Poll::Ready(Ok(()));
        }
        match Pin::new(io).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                self.need_flush = false;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

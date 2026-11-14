//! Buffer facade over `bytesbuf`. Everything in Matier touches `bytesbuf` through this module so the
//! (frequently breaking, 0.x) dependency can be swapped or upgraded locally.

use std::io::IoSlice;

pub use bytesbuf::mem::{GlobalPool, Memory};
pub use bytesbuf::{BytesBuf, BytesView};

/// Body chunk type everywhere.
pub type Chunk = BytesView;

thread_local! {
    // One pool per thread: `GlobalPool` is fastest when not moved across threads (see bytesbuf docs).
    static POOL: GlobalPool = GlobalPool::new();
}

/// Per-thread pooled memory (1/4/16/64 KiB blocks). Returns a clone of this thread's `GlobalPool`
/// (a cheap shared handle). A connection should grab one at creation and keep it.
///
/// Deviation from the contract: returns the concrete `GlobalPool` rather than `impl Memory`
/// (it is a strict superset: `Clone`, `Memory`, `MemoryShared`).
#[must_use]
pub fn memory() -> GlobalPool {
    POOL.with(Clone::clone)
}

/// Zero-copy conversion (costs one small heap allocation for the refcount block).
#[must_use]
pub fn from_bytes(b: bytes::Bytes) -> Chunk {
    b.into()
}

/// Wraps static data without copying (costs one small allocation, like [`from_bytes`]).
/// bytesbuf deliberately has no `From<&'static [u8]>`; this goes through `Bytes::from_static`.
#[must_use]
pub fn from_static(b: &'static [u8]) -> Chunk {
    bytes::Bytes::from_static(b).into()
}

/// Copies `b` into pooled memory from [`memory()`].
#[must_use]
pub fn copy_from_slice(b: &[u8]) -> Chunk {
    BytesView::copied_from_slice(b, &memory())
}

/// Converts to `Bytes`: zero-copy for 0/1 spans, otherwise copies. Avoid on hot paths.
#[must_use]
pub fn to_bytes(v: BytesView) -> bytes::Bytes {
    v.into()
}

/// Fills `out` with one `IoSlice` per span of `view` (starting at its current front) and returns how
/// many were filled. Use for `poll_write_vectored`, then `view.advance(n)` with the bytes written.
pub fn io_slices<'a>(view: &'a BytesView, out: &mut [IoSlice<'a>]) -> usize {
    bytes::Buf::chunks_vectored(view, out)
}

# Research: Microsoft oxidizer — `bytesbuf`, `layered`, `fetch`

Source: https://github.com/microsoft/oxidizer `main` (snapshot taken 2026-11-14), read from raw source.
Local copy: `/private/tmp/claude-501/-Users-benjamincunningham-dev-rust-http/0f87bdb6-914f-4cf7-9021-effc1d071f95/scratchpad/oxidizer-src/`
(paths below are relative to the repo root, e.g. `crates/bytesbuf/src/view.rs`).

Statements marked **[inference]** are my reasoning about hyper/h2/tokio behaviour, not something found in oxidizer source.

## 0. Crate inventory (names and versions confirmed)

| Crate (crates.io name) | Version on `main` = latest on crates.io | Role |
|---|---|---|
| `bytesbuf` | 0.14.0 (first release 2025-12-10) | Multi-span byte sequences + memory providers/pools |
| `bytesbuf_io` | 0.14.0 | Async `Read`/`Write` traits written in terms of `BytesBuf`/`BytesView` |
| `layered` | 0.3.9 | `Service` trait (`async fn(&self)`), `Stack`, `Intercept`, `DynamicService`, tower adapters |
| `http_extensions` | 0.15.0 | `HttpRequest`/`HttpResponse` aliases, `HttpBody`, `RequestHandler`, `HttpError`, request builder, routing |
| `fetch` | 0.21.0 | HTTP client, pipelines, handlers (`Dispatch`, `Metrics`, `Logging`, `Buffering`) |
| `fetch_options` | 0.2.5 | Transport options (pooling, HTTP/2, keep-alive, socket, request filter, `ConnectionInfo`) |
| `fetch_hyper` | 0.12.0 | hyper-util `legacy::Client` transport with its own connector/TLS/metrics |
| `seatbelt`, `seatbelt_http` | (not read in depth) | Resilience: timeout, retry, breaker, hedging, fallback, chaos |
| also present | `fetch_tls`, `fetch_winhttp(_impl)`, `fetch_azure`, `http_compression`, `compressors`, `thread_aware`, `tick`, `ohno`, `recoverable`, `templated_uri`, `plurality`, `performables`, `nm`, `seismograph*` | |

Workspace (`Cargo.toml`): **license MIT**, edition 2024, **MSRV 1.95**. Pinned workspace deps: `bytes 1.11.1`, `http 1.4.1`, `hyper 1.10.1`, `hyper-util 0.1.20`, `opentelemetry 0.33.0`, `tower-layer 0.3.3`.

**Maturity:** all 0.x. `bytesbuf` CHANGELOG shows breaking minor bumps roughly every 2–4 weeks (0.10.0 2026-09-10 → 0.14.0 2026-11-06), many caused only by internal dependency bumps (`thread_aware`, `performables`, `seismograph_io`) that leak into the public API. Code quality is high (Miri tests, mutation-testing annotations `#[gamma::skip]`, criterion + callgrind benches, `static_assertions` for Send/Sync), but the API churn and the deep internal dependency graph (`thread_aware`, `plurality`, `performables`, `nm`) are the main adoption risks. Downloads are low (bytesbuf ~18k total).

---

## 1. `bytesbuf`

### 1.1 Core types (`crates/bytesbuf/src/lib.rs` re-exports)

```rust
pub use buf::{BytesBuf, BytesBufRemaining, BytesBufVectoredWrite};
pub use buf_writer::BytesBufWriter;          // std
pub use view::{BytesView, BytesViewSlices};
pub use memory_guard::MemoryGuard;
pub mod mem { Block, BlockSize /* = u32 */, BlockRef, BlockRefDynamic, BlockRefDynamicWithMeta,
              BlockRefVTable, BlockMeta, CallbackMemory, GlobalPool /* std */, HasMemory,
              Memory, MemoryShared, OpaqueMemory, testing /* test-util */ }
```

- **`BytesView`** (`src/view.rs:66`) — immutable, *non-contiguous* byte sequence; consumable from the front.
  ```rust
  pub struct BytesView {
      identity: BufferIdentity,
      pub(crate) spans_reversed: SmallVec<[Span; MAX_INLINE_SPANS]>, // MAX_INLINE_SPANS = 8
      len: usize,
  }
  ```
  Each `Span` = (ptr, len, `BlockRef`) — a refcounted window into a memory block. `Clone` is a SmallVec clone + refcount bumps (no data copy). `Send + Sync + Eq + Hash` (`view.rs:1201`). Up to 8 spans are inline; more spill to the heap.
- **`BytesBuf`** (`src/buf.rs:93`) — append-only builder. Holds `frozen_spans` (already-immutable data, e.g. appended views) and `span_builders_reversed` (reserved, partially-filled capacity). `Send + Sync` (`buf.rs:1685`).
- **`Memory`** (`src/mem/memory.rs`):
  ```rust
  pub trait Memory: Debug { fn reserve(&self, min_bytes: usize) -> BytesBuf; }
  ```
- **`MemoryShared`** (`src/mem/memory_shared.rs`): `Memory + ThreadAware + Send + Sync + 'static` + `clone_boxed()`; blanket-impl for any `Clone` such type.
- **`HasMemory`** (`src/mem/has_memory.rs`): `fn memory(&self) -> impl MemoryShared;` — implemented by I/O endpoints (connections, clients, body builders) so producers allocate memory that is optimal *for that consumer*.
- **`OpaqueMemory`** — type-erased `MemoryShared` (extra indirection per reserve). `fetch`/`http_extensions` store this.
- **`CallbackMemory<D>`** — `MemoryShared` built from `(data: D, fn(&D, usize) -> BytesBuf)`; used to wrap a provider with config (alignment, registered memory, …).
- **`Block` / `BlockRef` / `BlockRefDynamic`** (`src/mem/block.rs`, `block_ref.rs`) — the provider SPI: a provider hands out exclusively-owned `Block { ptr: NonNull<MaybeUninit<u8>>, len: NonZero<u32>, block_ref }` with a custom vtable (clone/drop/meta). `BytesBuf::from_blocks(iter)` builds a buffer from them. **Block size is limited to `u32`** ("Many OS APIs are limited to 32-bit block sizes").
- **`BlockMeta: Any + Send + Sync + Debug`** — per-block metadata, downcastable. Lets a consumer check "is every slice of this view in registered/page-aligned memory?" and pick an optimal I/O path (`lib.rs` "bb_optimal_path" example; `BytesView::slices()` yields `(&[u8], Option<&dyn BlockMeta>)`).
- **`MemoryGuard`** — keeps blocks alive (e.g. while the kernel/io_uring/IOCP owns a pointer) independent of the view: `BytesView::extend_lifetime()`, `BytesBuf::extend_lifetime()`.

### 1.2 Allocation: `GlobalPool` (`src/mem/global.rs`)

- "Global" = memory from the Rust global allocator, **pooled**. Not a singleton: `GlobalPool::new()` creates an independent pool; clones share it.
- Four size-class sub-pools: **1 KiB, 4 KiB, 16 KiB, 64 KiB** blocks. `reserve(n)` picks the smallest class ≥ n; for n > 64 KiB it allocates *multiple 64 KiB blocks* (so a large reservation is multi-span by construction). Each sub-pool grows in ~64 KiB chunks (`plurality::Pool`, chunk = 64/16/4/1 slots).
- Each sub-pool is `Arc<Mutex<Pool<..>>>`; the mutex guards only allocation ("uncontended mutex is almost an annotation"). Release is lock-free (atomic MPSC free list) and can happen on any thread.
- `GlobalPool` is `ThreadAware` and wraps `performables::arc::Arc<_, PerThread>`: when relocated via a thread-aware runtime it picks a **per-thread pool**. Moving a clone across threads manually (e.g. capture in `tokio::spawn`) still works but "will suffer degraded performance". If you have no thread-aware runtime, docs recommend a `thread_local!` pool.
- Blocks keep their pool alive (leaked `plurality::Box`), so dropping the pool while I/O holds memory is safe; leaks look like "in use".
- Memory is reference-counted per block: **a block returns to the pool only when every span referencing any part of it is dropped** (a 10-byte view kept alive pins a whole 64 KiB block). Plan retention accordingly.
- Metrics (via `nm` crate, thread-local events): `bytesbuf_global_pool_block_rented_size`, `bytesbuf_global_pool_reservation_requested_size`, `bytesbuf_view_to_bytes_shared`, `bytesbuf_view_to_bytes_copied`, instances created.
- Without `std`: no `GlobalPool`; bring your own `Memory`.

"Memory providers per I/O subsystem" is the design intent (`lib.rs` "Producing Byte Sequences"): sockets/files implement `HasMemory` and return a provider configured for them (e.g. `CallbackMemory` that requests registered, page-aligned memory); generic code uses a shared `GlobalPool`. In this repo the only concrete providers are `GlobalPool`, `CallbackMemory`, `OpaqueMemory`, and test providers (`FixedBlockMemory`, `TransparentMemory`, `StdAllocBlock`…). There is **no tokio, io_uring or registered-buffer provider in the repo**.

### 1.3 How it differs from `bytes`

| | `bytes::Bytes` / `BytesMut` | `bytesbuf::BytesView` / `BytesBuf` |
|---|---|---|
| Contiguity | Always one contiguous slice | Sequence of spans (non-contiguous); `first_slice()` may be partial |
| Mutability | `BytesMut` mutable in place; `split_to`/`freeze` | Append-only; written bytes are immutable; no in-place edits |
| Allocation | Global allocator per buffer (`Vec` growth/realloc+copy) | Pre-reserved pooled fixed-size blocks; growth adds blocks, never reallocates/copies |
| Provider awareness | none | `Memory`/`HasMemory`, per-block `BlockMeta` for I/O-optimal memory |
| Concatenation | Requires copy | `from_views`/`append`/`put_bytes` are zero-copy (span moves) |
| Static data | `Bytes::from_static` zero-copy | Deliberately **no** zero-copy `&'static [u8]` → view; use `OnceLock<BytesView>` + `copied_from_slice` once (`view.rs:~200`) |
| Size | 32 B handle | `BytesView` holds up to 8 inline spans (much larger handle); `Content` enum notes `large_enum_variant` |

### 1.4 Key APIs

`BytesView` (`view.rs`, `view_get.rs`, `view_read.rs`):
```rust
pub const fn new() -> Self;
pub fn from_views<I>(views: I) -> Self where I: IntoIterator<Item = Self>, I::IntoIter: DoubleEndedIterator; // zero-copy concat
pub fn copied_from_slice(bytes: &[u8], memory: &impl Memory) -> Self;   // copies
pub fn range<R: RangeBounds<usize>>(&self, r: R) -> Self;               // zero-copy sub-view
pub fn range_checked<R>(&self, r: R) -> Option<Self>;
pub fn first_slice(&self) -> &[u8];
pub fn slices(&self) -> BytesViewSlices<'_>;   // Iterator<Item = (&[u8], Option<&dyn BlockMeta>)>
pub fn first_slice_meta(&self) -> Option<&dyn BlockMeta>;
pub fn advance(&mut self, count: usize);
pub fn append(&mut self, other: Self); pub fn concat(&self, other: Self) -> Self;
pub fn consume_all_slices(&mut self, f: impl FnMut(&[u8]));
pub fn extend_lifetime(&self) -> MemoryGuard;
pub fn to_vec(&self) -> Vec<u8>;
get_u8/get_byte, get_{u16..u128,i*,f32,f64}_{le,be,ne}, copy_to_slice, copy_to_uninit_slice
impl std::io::Read + BufRead (std)
```
`BytesBuf` (`buf.rs`, `buf_put.rs`):
```rust
pub fn new() -> Self;  pub fn from_blocks<I>(blocks: I) -> Self;
pub fn reserve<M: Memory + ?Sized>(&mut self, additional: usize, memory: &M);
pub fn put_slice(&mut self, src: impl Borrow<[u8]>); pub fn put_bytes(&mut self, view: BytesView); // zero-copy
pub fn put_byte(..); put_byte_repeated(..); put_{u16..}_{le,be,ne}(..);
pub fn peek(&self) -> BytesView;                 // view of filled data, buffer keeps it
pub fn consume(&mut self, len: usize) -> BytesView; consume_checked; consume_all;
pub fn split_off_remaining(&mut self, count: usize) -> Self;
pub fn len/capacity/remaining_capacity(&self) -> usize;
pub fn first_unfilled_slice(&mut self) -> &mut [MaybeUninit<u8>];
pub unsafe fn advance(&mut self, count: usize);  // declare bytes written
pub fn begin_vectored_write(&mut self, max_len: Option<usize>) -> BytesBufVectoredWrite<'_>;
//   .slices_mut() -> iter of (&mut [MaybeUninit<u8>], Option<&dyn BlockMeta>); unsafe .commit(n)
pub fn into_writer<M: Memory>(self, memory: M) -> BytesBufWriter<M>; // std::io::Write, auto-reserves
```
Important nuance: you must **reserve before writing** — `put_*` on a full buffer panics; only `put_bytes` brings its own capacity. `BytesBufWriter` auto-reserves.

### 1.5 Multi-span views

A view is a reversed `SmallVec<[Span; 8]>`; reading pops from the end. `advance` trims/pops spans. `range` builds a new span list referencing the same blocks. Spans may come from different providers (Bytes, Vec, pool) in one view. All consumers must handle `first_slice()` shorter than the logical length (the `get_*` helpers already straddle span boundaries).

### 1.6 Vectored I/O (`IoSlice`)

- There is **no inherent `IoSlice` API** on `BytesView`. Vectored write support comes from `bytes::Buf` (feature `bytes-compat`, `src/bytes_compat/view.rs`):
  ```rust
  impl Buf for BytesView {
      fn chunk(&self) -> &[u8] { self.first_slice() }
      fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize; // one IoSlice per span
      fn advance(&mut self, cnt: usize);
  }
  ```
  or manually: `view.slices().map(|(s, _)| IoSlice::new(s))`.
- Vectored *read* (readv) is `BytesBuf::begin_vectored_write` (`buf.rs:1166`): exposes all reserved spans as uninit slices, then `unsafe commit(bytes_written)`. Also `unsafe impl BufMut for BytesBuf` (`bytes_compat/buf.rs`) so tokio's `AsyncReadExt::read_buf` works directly.

### 1.7 Conversions to/from `bytes::Bytes` (`bytes-compat` feature)

| Conversion | Cost (from source) |
|---|---|
| `BytesView::from(Bytes)` (`bytes_compat/from_bytes.rs`) | Zero-copy; **one small heap allocation** for the `BytesBlock` refcount state; splits into ≤ `u32::MAX` chunks |
| `BytesView::from(Vec<u8>)` (`vec.rs`) | Zero-copy, one small allocation |
| `view.to_bytes()` / `Bytes::from(view)` (`to_bytes.rs`) | 0 spans → `Bytes::new()`. 1 span → `Bytes::from_owner(span.clone())` zero-copy + small alloc. ≥2 spans → **copies** into a new `BytesMut` from the global allocator. Docs: "avoid in performance-sensitive code"; tracked by the `..._copied` metric |
| `HeaderValueExt` (http_extensions) | builds `HeaderValue` from `Bytes` |

### 1.8 Thread-safety

`BytesView`, `BytesBuf`, `Span`, `SpanBuilder`: `Send + Sync` (static-asserted). Block refcounts are atomic. Memory providers: `MemoryShared` requires `Send + Sync + ThreadAware`. Pools are per-thread-partition when relocated via `thread_aware`; otherwise correct but slower when shared across threads.

### 1.9 `bytesbuf_io` (0.14.0)

```rust
#[trait_variant::make(Send)]
pub trait Read: HasMemory + Memory + Debug {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn read_at_most_into(&mut self, len: usize, into: BytesBuf) -> Result<(usize, BytesBuf), Self::Error>;
    async fn read_more_into(&mut self, into: BytesBuf) -> Result<(usize, BytesBuf), Self::Error>;
    async fn read_any(&mut self) -> Result<BytesBuf, Self::Error>;
}
#[trait_variant::make(Send)]
pub trait Write: HasMemory + Memory + Debug {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn write(&mut self, data: BytesView) -> Result<(), Self::Error>;
}
```
Owned-buffer (completion-style, io_uring/IOCP-friendly) API. **No tokio adapters in the repo** — we would write `TcpStream`/TLS impls ourselves.

### 1.10 How we would use it

**(i) Encoding HTTP/1 headers and HTTP/2 frame headers without extra allocations**

- Per connection, keep a `memory: impl MemoryShared` (our connection implements `HasMemory`; default `GlobalPool` clone).
- HTTP/1 request head: estimate size (method + path + Σ header name/value + 4/header + 12), `let mut buf = memory.reserve(est)`, then `put_slice(method)`, `put_byte(b' ')`, … `put_slice(b"\r\n")`. One pooled block (1 KiB/4 KiB class), no heap allocation in steady state (pool hit), no realloc. If an estimate is short, `buf.reserve(extra, &memory)` adds a block (head becomes 2 spans — still fine for writev). `consume_all()` → `BytesView`.
- Static fragments (`"HTTP/1.1\r\n"`, `"content-length: "`, `"transfer-encoding: chunked\r\n"`) : `OnceLock<BytesView>` per memory configuration, then `buf.put_bytes(STATIC.clone())` — zero-copy span reference (costs a span slot + refcount bump; for tiny strings `put_slice` copy is probably cheaper than consuming a span slot — benchmark).
- Chunked encoding: chunk-size line `put_slice(hex)` + `put_bytes(chunk_view)` + `put_slice(b"\r\n")` → one view, body bytes not copied.
- HTTP/2: the 9-byte frame header `put_u32_be`/`put_u8`… into a small reservation; for DATA frames build `BytesView::from_views([hdr9, payload.range(..n)])` — zero-copy split of the body into ≤ `SETTINGS_MAX_FRAME_SIZE` frames via `range`. HPACK: encode into `BytesBuf` directly (HPACK writer must take a `BytesBuf`/`BufMut`; `BytesBuf: BufMut` with `bytes-compat`).
- Amortise: reserve one 4 KiB block per connection write-batch and `consume(n)` per frame header — consumed views share the block; remaining capacity stays in the buf for the next header (`consume` splits the span builder).

**(ii) Handing body chunks to a socket/TLS writer with writev and zero copies**

- Body type `Body<Data = BytesView>` (as `http_extensions::HttpBody`). Writer loop: collect `head_view.slices()` + `body_chunk.slices()` into a stack `[IoSlice; N]` (or `Buf::chunks_vectored`), call `TcpStream::poll_write_vectored`, then `advance(n)` on the views. Zero copy to the kernel for plaintext.
- TLS: **rustls copies plaintext into its own record buffers** when encrypting **[inference]**, so "zero-copy" ends at the TLS boundary; still, using `writev` of the rustls output and avoiding a coalescing copy before rustls is the win. kTLS is the only true zero-copy TLS path.
- If we instead use hyper: `hyper::client::conn` accepts `B: Body` with `B::Data: Buf`; `BytesView: Buf` with `chunks_vectored`, so hyper's h1 "queue" write strategy can writev the spans **[inference about hyper internals]**. This is exactly what `fetch_hyper` does: `Client<WrappedConnector, HttpBody>` sends `HttpBody` (Data = `BytesView`) directly (`crates/fetch_hyper/src/connection/hyper_handler.rs:49`).

**(iii) Receiving from the socket into pooled buffers and slicing frames/bodies without copying**

- Per connection: `let mut rbuf = memory.reserve(16 * 1024)`. Read with `read_buf(&mut rbuf)` (BufMut) or `ReadBuf::uninit(rbuf.first_unfilled_slice())` + `unsafe rbuf.advance(n)`, or readv over `begin_vectored_write`.
- Parse on `rbuf.peek()` (cheap view; parsing helpers handle span boundaries). When a full frame/head is available: `let frame = rbuf.consume(frame_len)` — zero-copy; body: `rbuf.consume(body_len)` → `BytesView` → `Frame::data`. Leftover bytes and unfilled capacity stay in `rbuf`; top up with `rbuf.reserve(n, &memory)` when `remaining_capacity()` is low.
- HTTP/1 header parsing with `httparse` needs a **contiguous** `&[u8]`: works if the head fits in `first_slice()`; if the head straddles blocks, copy just the head into a scratch buffer (rare if reservation ≥ max header size). Header values into `http::HeaderValue` require `Bytes`: `range(..).to_bytes()` is zero-copy only for single-span ranges (plus a small alloc per call) — consider keeping our own header map storing `BytesView` ranges, or accept the copy for headers.
- Caveat: retained small body/header views pin whole pooled blocks; for long-lived retention, copy out.

---

## 2. `layered` (0.3.9)

### 2.1 `Service` (`crates/layered/src/service.rs`)
```rust
pub trait Service<In>: Send + Sync {
    type Out;
    fn execute(&self, input: In) -> impl Future<Output = Self::Out> + Send;
}
// impls for Box<S> and Arc<S>
```
Implementors can write `async fn execute(&self, input: In) -> Self::Out` (RPITIT). No error type — fallible services use `Out = Result<T, E>`.

### 2.2 `Layer` and `Stack` (`src/layer/`)
- `Layer` is **re-exported `tower_layer::Layer`** (`pub use tower_layer::Layer;`) — `fn layer(&self, inner: S) -> Self::Service`.
- ```rust
  pub trait Stack { type Service; fn into_service(self) -> Self::Service; }
  ```
  implemented for tuples `(L1, S)` … `(L1..L16, S)`: **first element is outermost**, last is the leaf service: `(LogLayer, TimeoutLayer, Execute::new(f)).into_service()`.
- Other pieces: `Execute::new(|in| async {..})` (closure → service); `Intercept`/`InterceptLayer` (`on_input`, `on_output`, `modify_input`, `modify_output` hooks; feature `intercept`); `DynamicService<In, Out>` (type erasure via `Arc<dyn Fn(In) -> plurality::Box<dyn Future + Send>>`, futures boxed into a pooled allocator to avoid malloc per call; `.into_dynamic()`; feature `dynamic-service`); `tower::Adapter` both directions + `tower_layer(l)` to use tower layers in a stack (feature `tower-service`).

### 2.3 Differences from `tower::Service` and backpressure

| tower | layered |
|---|---|
| `fn call(&mut self, req) -> Self::Future` | `fn execute(&self, input) -> impl Future + Send` |
| `poll_ready(&mut self, cx)` | none |
| Named `Future` assoc type, `Error` assoc type | RPITIT, `Out` only |
| Clone service per concurrent call (`&mut`) | Shared `&self`; service must be `Send + Sync`; futures `Send` |

Implications:
- No readiness protocol → **no built-in backpressure/load-shedding**. `Adapter`'s tower `poll_ready` always returns `Ready(Ok(()))` (`tower.rs:85`); the reverse adapter awaits `poll_ready` inside the execute future on a cloned service. Backpressure must be expressed *inside* the future (await a semaphore permit, a pool checkout, or an h2 stream slot) — i.e. queueing happens in `execute` rather than before `call`. Concurrency limits/rate limits become layers that await internally (like `tower::limit` would, but without "not ready" signalling to callers).
- Middleware state must be `Sync` (atomics/locks) since many requests run through `&self` concurrently.
- RPITIT futures are unnameable → type erasure needs boxing (`DynamicService`), and returned futures borrow `&self` (the future's lifetime is tied to the service unless you clone an `Arc` into it). `fetch`'s `Pipeline` uses `Either` to avoid boxing for the minimal path.
- Hard `Send` bounds: no `!Send` (thread-per-core/local) services without a separate trait.

---

## 3. `fetch` (0.21.0) + `http_extensions` (0.15.0)

### 3.1 Request/response/body types (`crates/http_extensions/src`)

```rust
pub type HttpRequest  = http::Request<HttpBody>;   // lib.rs:202
pub type HttpResponse = http::Response<HttpBody>;  // lib.rs:207
pub trait RequestHandler: Service<HttpRequest, Out = Result<HttpResponse>> {} // blanket impl (request_handler.rs)
pub type Result<T> = std::result::Result<T, HttpError>;
```
`HttpBody` (`body/body.rs:191`):
```rust
pub struct HttpBody { kind: Kind, options: HttpBodyOptions }
enum Kind { Bytes(Option<BytesView>), Empty, Body(StreamingBody) }
pub struct StreamingBody { inner: Pin<Box<dyn http_body::Body<Data = BytesView, Error = HttpError> + Send>> }
impl http_body::Body for HttpBody { type Data = BytesView; type Error = HttpError; .. }
```
- Methods: `into_bytes()`, `into_text()`, `into_buffered()` (default buffer limit 2 GB, `HttpBodyOptions::buffer_limit`), `into_json()`/`into_json_ref()` (feature `json`), `into_stream()`, `try_clone()` (only buffered bodies → needed for retry/hedging), `content_length()`, `map(|Content| ..)` (used by decompression). Idle per-frame timeout (`HttpBodyOptions::timeout`). Buffering collects frames into `Vec<BytesView>` then `BytesView::from_views` (zero-copy).
- `HttpBodyBuilder { memory: OpaqueMemory, clock: Clock, options }` — creates bodies (`text`, `slice`, `bytes`, `stream`, `body`, `json`, `empty`) and implements `Memory`/`HasMemory` so body bytes come from the client's provider. `HttpClient` itself implements `Memory`/`HasMemory` by delegating to it.
- `HttpError` — `ohno`-based error with backtrace, **`RecoveryInfo`** (recoverable crate: retry/never/unavailable + optional delay) and **error labels** (used for `error.type`).
- Headers are plain `http::HeaderMap` (i.e. `Bytes`-backed); bytesbuf is used only for bodies.

### 3.2 Per-request options via `http::Extensions`

Request builder (`http_request_builder.rs`): `.extension(T)`, `.response_timeout(d)`, `.body_timeout(d)`, `.timeout(d)`, `.version(v)`, `.header(..)`, `.fetch*()`. Extension types observed flowing through the pipeline:

| Extension | Read by |
|---|---|
| `ResponseTimeout` | `HttpClient::execute` wraps the whole pipeline in that timeout (`fetch/src/client.rs:317`) |
| `BodyTimeout` | hyper transport sets body idle timeout (`fetch_hyper/.../hyper_handler.rs`) |
| `UriTemplateLabel`, templated `PathAndQuery`/`RequestUris` | metrics `url.template`, logging `url.path.template` |
| `TelemetryAttributes` | metrics: extra attributes (also read from *response* extensions) |
| `PoolIndex` | `Dispatch` pins the request to a specific connection pool |
| `Router` | inserted by client so retries/hedges can pick alternative endpoints |
| `seatbelt::retry::Attempt` | set by retry/hedging; `Dispatch` copies it to the response extensions |
| `ConnectionInfo` (response) | added by hyper transport (age, pool index, poison flag) |

### 3.3 Pipeline

`HttpClient { pipeline: Shared(Arc<Pipeline>) | Isolated(Arc<Pipeline, PerThread>), body_builder, clock, router }`. Three modes (`pipeline/builder.rs`): **Standard** (default), **Minimal** (just `Dispatch`), **Custom** (`custom_pipeline(|dispatch, ctx| (layers.., dispatch).into_service())`). The stack is erased to `DynamicService<HttpRequest, Result<HttpResponse>>`.

Standard pipeline, outermost → innermost (`pipeline/standard.rs`, `builder.rs`):
```
HttpClient::execute  (router resolve + per-request ResponseTimeout)
 1 total_metrics      Metrics.report_total_duration(true) → http.client.request.total_duration
 2 total_timeout      seatbelt HttpTimeout "standard.total_timeout" 30s
 3 retry | hedging    seatbelt HttpRetry "standard.retry" (3 retries, 10ms base, exp+jitter, honours Retry-After,
                      5xx/429/timeouts, safe methods w/ cloneable bodies) | HttpHedging "standard.hedging"
 4 breaker            seatbelt HttpBreaker "standard.breaker" (open → HttpError::unavailable carrying the request)
 5 attempt_timeout    "standard.attempt_timeout" 10s
 6 attempt_intercept  layered InterceptLayer (no-op by default)
 7 attempt_logs       Logging (tracing events, redaction engine)
 8 attempt_metrics    Metrics → http.client.request.duration (per attempt)
 9 Dispatch           scheme filter (HTTPS-only default), pool selection, → TransportHandler
     └ per-transport wrapper: decompression layer (http_compression), applied once per transport
       └ transport: fetch_hyper HyperTransport (hyper-util legacy Client) | custom RequestHandler | FakeHandler
```
Each layer is customised via `builder.standard_pipeline(|p, ctx| p.retry(|r| ..).recovery_mode(RecoveryMode::Hedging))`.

- **Decompression** (`dispatch_builder.rs`): wraps each transport, so it sits *inside* retries/metrics; it rewrites encoding metadata eagerly but decompresses lazily as the body is read (timing excluded from metrics). Formats by feature: zstd, br, gzip, deflate (zlib); limits 64 MiB output / 1024 streams by default.
- **Pooling** (`fetch_options/src/pooling.rs`): `ConnectionPoolOptions { connection_idle_timeout (60s), max_connections (usize::MAX → hyper pool_max_idle_per_host), multiple_pools: Option<(n, PoolSelection)>, connection_lifetime }`. `multiple_pools` creates N independent transports (N hyper clients) and `Dispatch` selects with `PoolSelection::saturating(100 req/client)` or `round_robin()` — used to spread load across multiple HTTP/2 connections. `ConnectionLifetime::per_connection(fn)` allows jittered recycling. `Isolation::Isolated` builds a pipeline (and pools) per runtime thread.
- **Transport**: `fetch_hyper` = **hyper-util `client::legacy::Client`** (`hyper_handler.rs`: `Client<WrappedConnector<C,S>, HttpBody>`), custom connector chain `HyperConnectorAdapter<ClientConnector<TlsConnector<C,S>>, TrackedStream<..>>` (connect timeout, ALPN/version check, connection metrics, lifetime/poisoning), executor/timer from `anyspawn`/`tick`. Response body: `body.map_frame(|f| f.map_data(BytesView::from))` — i.e. **response data arrives as hyper `Bytes` and is wrapped zero-copy (+1 small alloc per frame); it is not read into bytesbuf pools**. Request body goes to hyper as `HttpBody` with `Data = BytesView` (via `bytes::Buf`). TLS: rustls (aws-lc-rs, platform verifier) or native-tls. Also a WinHTTP transport (`fetch_winhttp`).

### 3.4 Telemetry (`fetch/src/documentation/telemetry.rs`, `handlers/metrics.rs`, `telemetry.rs`)

Meter name `fetch`; scope attributes: `fetch.runtime` ("tokio"/"fake"/custom), `fetch.transport` ("hyper"/…), `http.client.name` (default `"http_client"`, `HttpClientBuilder::name`). Global provider unless `HttpClientBuilder::meter_provider(p)`.

| Metric | Type/unit | Buckets (s) | Attributes |
|---|---|---|---|
| `http.client.request.duration` (OTel semconv) | `Histogram<f64>`, `s` | 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10 | `http.request.method` (known methods or `_OTHER`), `server.address`, `server.port` (derived from scheme), `url.scheme`, `url.template` (opt), on success `network.protocol.name`, `network.protocol.version`, `http.response.status_code`; on failure `error.type` |
| `http.client.request.total_duration` (Oxidizer-specific) | same | same | same |
| `http.client.connection.setup.duration` (hyper transport) | `Histogram<f64>`, `s` | …, 10, 25, 50 | `server.address`, `server.port`, `url.scheme`, `network.protocol.version` ("1"/"2") on success, `error.type` on failure |
| `http.client.connection.duration` (hyper transport) | `Histogram<f64>`, `s` | 0.01 … 300 | `server.address`, `server.port`, `url.scheme`, `network.protocol.version` |

- `error.type` = dot-joined error-label chain from the `source()` chain, e.g. `request_hyper.connect.timed_out`, `scheme_not_allowed`, `compression_invalid`, `abandoned` (future dropped — recorded by a drop guard `MetricsDropGuard`).
- **Adding dimensions**: (1) attach `TelemetryAttributes` (SmallVec<[KeyValue; 9]>) to request extensions (or response extensions, e.g. from a transport); (2) `MetricsLayer::enrich_from_request(|attrs, &req| ..)` and `enrich_from_response(|attrs, &result| ..)`; (3) `MetricsLayer::on_record(|duration, &result, &[KeyValue]| ..)` callback for custom sinks.
- Logging (`handlers/logging.rs`): `tracing` events `http.response.complete` (DEBUG) / `http.response.error` (WARN) with semconv-style fields incl. `url.path.redacted` (via `data_privacy::RedactionEngine`) and `http.client.request.duration`. **No tracing spans / no distributed-trace context propagation** found in fetch.
- Seatbelt layers emit their own logs/metrics (features `logs`, `metrics`) — not examined in detail.

---

## 4. Recommendations for our design

**Copy**
1. `Service<In>` with `fn execute(&self, In) -> impl Future<Output = Out> + Send` and `Out = Result<Response, Error>`; tuple `Stack` with outermost-first ordering; reuse `tower_layer::Layer` so tower layers' *types* compose. Consider depending on `layered` directly (small, MIT, 0.3.x relatively stable) or copying the ~100 lines.
2. Pipeline shape: `total_metrics → total_timeout → retry|hedging → breaker → attempt_timeout → intercept → logs → attempt_metrics → dispatch → (decompression) → transport`. Two duration histograms (per-attempt semconv + logical total). Decompression *below* retries, lazily on body read.
3. Per-request knobs as typed `http::Extensions` (`ResponseTimeout`, `BodyTimeout`, `TelemetryAttributes`, `UriTemplateLabel`, `PoolIndex`); response-side `ConnectionInfo`/`Attempt`.
4. OTel: semconv names/attributes, scope attributes for client name/transport, `error.type` from a label chain, `abandoned` via drop guard, enrichers + `TelemetryAttributes` for user dimensions, explicit bucket boundaries, optional (feature-gated) dependency on `opentelemetry`.
5. Errors carrying `RecoveryInfo` (retryable/never/unavailable + Retry-After delay) so resilience layers are protocol-agnostic.
6. Body type = `http_body::Body<Data = BytesView>` with a buffered fast path (`Kind::Bytes`) that is cloneable for retries; `HasMemory` on client/connection so producers allocate from the right provider.
7. bytesbuf write path: pooled reservations per connection, `put_bytes` for zero-copy splicing, writev via `slices()`/`chunks_vectored`.

**Change**
1. **Backpressure**: since there is no `poll_ready`, make it explicit: a concurrency-limit layer that awaits a permit, pool checkout with bounded wait queue + timeout, and H2 `MAX_CONCURRENT_STREAMS` awaited inside `execute`; expose "queue wait time" as a metric (fetch has none).
2. **Own the transport** instead of hyper-util legacy: fetch's receive path is hyper `Bytes` → `BytesView` wrap (no pooling, alloc per frame). To get pooled zero-copy receive we need our own HTTP/1 codec (and ideally HTTP/2 framing) reading into `BytesBuf` from the connection's provider, slicing frames with `consume`.
3. Add **tracing spans + context propagation** (W3C traceparent injection) and connection-pool gauges (idle/active connections, pending checkouts) — fetch only emits durations.
4. Keep bytesbuf **optional / behind our own thin abstraction** (`trait IoBuf`/type alias) given its rapid 0.x breaking cadence and transitive deps (`thread_aware`, `plurality`, `performables`, `nm`). Pin exact versions. Don't expose `ThreadAware` in our public API unless we adopt a thread-aware runtime; use a `thread_local!` `GlobalPool` per tokio worker otherwise.
5. Headers: bytesbuf doesn't cover `HeaderMap`; for HTTP/1 parsing need contiguous heads (size the read reservation ≥ max header size, fall back to copy when the head straddles blocks). Converting header ranges to `Bytes` allocates per value — either accept it or design a `BytesView`-backed header map.
6. Beware memory pinning: a retained small `BytesView` pins a whole 1–64 KiB block; copy out small long-lived data, and expose a "detach/compact" helper.
7. Avoid `to_bytes()` on multi-span views in hot paths (copies); avoid `DynamicService` boxing on the hot path where generics suffice (fetch uses `Either` for minimal mode).
8. Make TLS's copy explicit in docs/benchmarks (rustls encrypts into its own buffers); consider kTLS later.

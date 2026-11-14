# HTTP/2 connection actor — detailed design

This is the core fix for hyperium/h2#531. See PLAN §4 for the rationale; this document is the implementation blueprint.

## Invariants (enforced in review; listed in CLAUDE.md)

1. **One owner.** The actor task exclusively owns the I/O, TLS session, HPACK encoder/decoder, stream table, stream-ID counter,
   connection windows, and write queue. No other task ever touches them.
2. **No connection-wide lock.** Cross-task state is per-stream (`StreamShared`) or per-connection atomics (`ConnShared`).
   The only mutexes allowed are *per-stream one-shot slots* (written once by the actor, taken once by the caller, never contended
   in steady state). None is ever held across `.await`.
3. **Reading never waits on writing.** A blocked socket write never stops frame processing.
4. **Bounded work per tick.** The actor yields after a bounded number of frames, commands, and bytes.
5. **Every cross-task signal is "set state, then wake".** The waker is registered *before* state is checked (to avoid lost wakeups).
   Each such protocol is covered by a `loom` test.

## Shared structures

```rust
struct ConnShared {
    in_flight: AtomicUsize,          // reserved slots (pool CAS), includes streams not yet opened
    max_streams: AtomicUsize,        // peer SETTINGS_MAX_CONCURRENT_STREAMS (u32::MAX if absent)
    accepting: AtomicBool,           // false after GOAWAY / error / close
    tx: mpsc::UnboundedSender<Command>, // the actor's single inbox (tokio mpsc v0; custom MPSC later)
    slot_released: tokio::sync::Notify, // pool waiters
    closed: CancellationToken-like,  // resolves when the actor exits
    origin: OriginKey, conn_id: u64,
}

enum Command {
    Open(OpenStream),                 // new request
    Ready(Arc<StreamShared>),         // "this stream has work": data/credit/cancel. Deduped by QUEUED flag.
}

struct OpenStream {
    shared: Arc<StreamShared>,
    head: http::request::Parts,       // HPACK-encoded by the actor
    body: OpenBody,                   // Empty | Full(BytesView) | Streaming
    enqueued_at: Option<Instant>,     // for Observer::request_dispatched
}

struct StreamShared {
    flags: AtomicU32,                 // QUEUED, CANCELED, HEAD_READY, RECV_DONE, SEND_DONE, RESET
    // Response head / terminal error: one-shot slot written by the actor.
    head: Mutex<Option<Result<http::response::Parts, Error>>>,
    head_waker: AtomicWaker,
    // Receive side: actor produces, RecvStream consumes (SPSC semantics on an MPMC lock-free queue in v0).
    recv: SegQueue<RecvEvent>,        // Data(BytesView) | Trailers(HeaderMap) | End | Error(Error)
    recv_waker: AtomicWaker,
    recv_consumed: AtomicU32,         // bytes consumed but not yet credited back; the consumer pushes Ready at a threshold
    // Send side (streaming bodies only): producer pushes, actor pops.
    send: SegQueue<SendEvent>,        // Data(BytesView) | End | Trailers
    send_credit: AtomicI64,           // bytes the producer may still enqueue (min(window, send_buffer_cap))
    send_waker: AtomicWaker,
}
```

## Actor-owned state

```rust
struct Actor {
    io: Io, origin: OriginKey, obs: Arc<dyn Observer>,
    shared: Arc<ConnShared>, rx: mpsc::UnboundedReceiver<Command>,
    hpack_enc: hpack::Encoder, hpack_dec: hpack::Decoder,
    next_stream_id: u32,                       // odd, +2
    streams: Slab<StreamEntry>, by_id: HashMap<u32, usize>  // or a sorted Vec: ids are monotonic
    conn_send_window: Window, conn_recv_window: Window, conn_recv_unacked: u32,
    initial_send_window: i32,                  // streams store offsets (O(1) SETTINGS change)
    peer: Settings, local: Settings, settings_ack_pending: bool,
    read_buf: ReadBuf,                         // pooled blocks; frames are sliced zero-copy
    write: WriteQueue,                         // see below
    send_ready: VecDeque<usize>,               // slab keys with data and window, round-robin (RFC 9218 buckets later)
    control: VecDeque<ControlFrame>,           // SETTINGS ack, PING ack, RST_STREAM, WINDOW_UPDATE, GOAWAY: written first
    goaway: Option<GoAwayState>, ping: PingState,
}
struct StreamEntry {
    id: u32, shared: Arc<StreamShared>,
    send_window_offset: i32, recv_window: Window,
    send: SendState,            // Idle | Full(BytesView remaining) | Streaming | Done
    recv: RecvState,            // AwaitHead | Body | Done
    headers_buf: Option<...>,   // CONTINUATION reassembly (bounded)
}
```

### WriteQueue (zero-copy, one writev per tick)
```rust
struct WriteQueue {
    arena: Vec<u8>,              // frame headers, HPACK blocks, control frames; reused each tick (cleared once fully written)
    segs: VecDeque<Seg>,         // in order: Arena(Range<usize>) | Data(BytesView)
    written_in_front: usize,     // partial-write offset into segs.front()
}
```
- Encoding appends bytes to `arena` and pushes `Arena(range)`. A DATA frame pushes `Arena(9-byte header)` then `Data(view.slice(..n))`. Payloads ≤ 256 B are copied into the arena instead, which keeps the iovec count low.
- Flushing builds a stack array of up to 64 `IoSlice`s from `segs` (BytesView spans count individually), then calls `poll_write_vectored`, then advances. Repeat until `Pending`, empty, or the per-tick byte cap (256 KiB) is reached. Call `poll_flush` only when the queue drains, which TLS needs.
- The arena is reset only when `segs` is empty. Otherwise it keeps growing until drained; a high-water mark triggers backpressure (stop pulling new commands).

## The tick

```
poll(cx):
  register: rx waker (via poll_recv), io read/write wakers (via the polls below)
  1. READ:  loop up to READ_BUDGET frames (default 64) or 256 KiB:
              poll_read into read_buf → decode_frame → apply_frame
            Pending → stop. EOF → connection closed (fail open streams: Retryable or Unprocessed per GOAWAY state).
  2. INBOX: drain up to CMD_BUDGET (default 128) commands:
              Open  → if goaway or !accepting: fail with Unprocessed. Otherwise assign id, HPACK-encode
                      HEADERS (+CONTINUATION) into the arena, set END_STREAM if the body is Empty, insert into the
                      stream table, and if the body is Full push to send_ready. Record request_dispatched later (when the frame is flushed).
              Ready → clear QUEUED; then handle: CANCELED → RST_STREAM(CANCEL) on the control queue, release recv bytes
                      to the connection window, remove. recv_consumed → stream WINDOW_UPDATE if ≥ 25% of the window.
                      send events → move to send_ready if window > 0.
  3. SCHEDULE: control frames first; then round-robin over send_ready. Each turn emits one DATA frame of
               min(max_frame, stream window, conn window, remaining) bytes, slicing the BytesView (no copy). Stop
               when the conn window or the per-tick byte cap runs out. Streams whose window is exhausted leave
               send_ready until a WINDOW_UPDATE arrives (record a stall start for Observer::flow_control_stall).
  4. WRITE: flush the WriteQueue (see above). Pending is fine: keep the data and continue.
  5. If READ or INBOX hit their budget, or the schedule still has sendable data with a writable socket:
         cx.waker().wake_by_ref(); return Pending   // yield to the runtime, then continue (fairness)
     Small-flush coalescing (experiment): if bytes queued < 4 KiB and the inbox was non-empty this tick, wake and yield once before flushing.
  6. obs.actor_tick(busy, rx.len()) when enabled.
```

### apply_frame (abridged)
- **DATA:** check the stream exists and is receiving, and check flow control: `conn_recv_window` and `stream.recv_window` must not go negative, otherwise FLOW_CONTROL_ERROR.
  - Push `Data(read_buf.slice(range))` to `shared.recv` and wake `recv_waker`.
  - Connection window: add len to `conn_recv_unacked`. If that is ≥ 25% of the connection target **and** total buffered bytes are under the budget, queue a connection WINDOW_UPDATE. The connection window is replenished on *receipt*, so one slow stream can't stall the connection (hyper#2899).
  - The stream window is replenished only on consumption, through `recv_consumed` and `Ready`.
- **HEADERS/CONTINUATION:** reassemble (bounded count and size: CONTINUATION flood guard) → HPACK decode (always, even for unknown or reset streams, to keep the table in sync).
  - First block → `:status`. 1xx → ignore, except 100 (wakes any expect-continue). Otherwise put `Ok(parts)` in the `head` slot and wake `head_waker`.
  - A later block with END_STREAM → trailers.
- **SETTINGS:** validate and apply.
  - INITIAL_WINDOW_SIZE: change `initial_send_window` (O(1) for all streams); streams whose window becomes positive go back on send_ready.
  - MAX_CONCURRENT_STREAMS → `max_streams.store`.
  - HEADER_TABLE_SIZE → encoder size update.
  - MAX_FRAME_SIZE → used by the scheduler.
  - Queue the SETTINGS ACK.
- **WINDOW_UPDATE:** check for overflow (>2^31-1 → FLOW_CONTROL_ERROR). Zero increment → PROTOCOL_ERROR (connection) or RST_STREAM (stream). Re-arm stalled streams.
- **RST_STREAM:** fail the stream with REFUSED_STREAM → `Unprocessed`, others → `Retryable`/`Never` by code; release the slot.
- **GOAWAY:** set `accepting = false`. Streams with `id > last_stream_id` fail with `Unprocessed`. The others finish. When none are left, close cleanly.
- **PING:** non-ack → queue an ack (control priority). Ack → RTT sample (for BDP later; also used for keepalive health).
- **PUSH_PROMISE:** PROTOCOL_ERROR (we send ENABLE_PUSH=0). Unknown frame types are ignored.
- Every connection error → queue GOAWAY(code) → best-effort flush → fail all streams → exit.

### Slot accounting
- `in_flight` is incremented by the pool's CAS in `try_reserve()` and decremented exactly once: when the `StreamSlot` is dropped unused, or when the actor removes a stream (closed in both directions or reset).
- On decrement, call `slot_released.notify_one()`.
- `Open` while `!accepting` fails `Unprocessed`, and the pool retries on another connection.

### Request body pumping
- **Empty / Full:** handled entirely by the actor; no extra task.
- **Streaming:** `send()` spawns a small pump task:
  1. take the next chunk from `Body`;
  2. wait until `send_credit > 0`, registering `send_waker` first;
  3. subtract the chunk length, push to `shared.send`, set QUEUED and push `Ready` if it wasn't set.
  4. The actor adds credit back as it frames bytes, waking `send_waker`.

  This is the only per-request task, and only for streaming uploads. (A later optimization is to poll the body from the response future.)

### Caller side
```rust
async fn send(&self, slot: StreamSlot, req: Request<Body>) -> Result<Response<Incoming>> {
    let shared = Arc::new(StreamShared::new(send_buffer_cap));  // TODO: per-conn free list
    tx.send(Command::Open(..))?;                                 // closed → Unprocessed
    let parts = HeadFuture(&shared).await?;                      // register head_waker, then check the slot
    Ok(Response::from_parts(parts, Incoming::H2(RecvStream { shared, conn: tx.clone() })))
}
```
`RecvStream::poll_frame` pops `recv`. It adds consumed bytes to `recv_consumed`, and once that crosses the threshold it sets QUEUED and sends `Ready`.

**Drop** of the head future or the `RecvStream` before completion sets CANCELED and sends `Ready` (the actor sends RST_STREAM(CANCEL)).

## Handshake
1. Write the preface, then our SETTINGS (ENABLE_PUSH=0, INITIAL_WINDOW_SIZE=2 MiB, MAX_HEADER_LIST_SIZE, NO_RFC7540_PRIORITIES=1), then a connection WINDOW_UPDATE raising the window to 16 MiB.
2. Read until the peer's first SETTINGS (timeout), apply it, and queue the ACK.
3. Spawn the actor and return the `H2Handle`, with `max_streams` already known.

## Tests
- **Unit:** an actor driven over `tokio::io::duplex` against a scripted fake peer (written with the matier-h2 codec). Cover: settings exchange, 1/100/10k concurrent streams, flow-control stalls and resumption, INITIAL_WINDOW_SIZE decrease making windows negative, GOAWAY mid-flight (Unprocessed vs Retryable), RST_STREAM REFUSED, cancellation sending RST, CONTINUATION flood → GOAWAY, PING ack, a blocked writer with continued reading (no deadlock: h2#975 scenario).
- **loom:** `StreamShared` head-slot handoff, recv queue + waker, QUEUED-flag dedup, slot accounting.
- **Interop:** against the hyper server in `matier-bench-server` (h2c and TLS), and the Go server when available.

## As built (v0)

Code: `crates/transport/src/h2/` — `mod.rs` (public API, `ResponseFuture`, `RecvStream`, upload `Pump`), `actor.rs` (Reader, Core, Actor, handshake),
`shared.rs` (`ConnShared`, `StreamShared`), `write.rs` (`WriteQueue`), `sync.rs` (std/loom shim), `tests.rs`, `loom_tests.rs`;
integration tests in `crates/transport/tests/h2_interop.rs` (real hyper server, h2c + TLS) and `h2_future_sizes.rs`.

Follows the blueprint; deviations and additions:
- **`H2Handle::send` is not `async`.** It moves the request into `Command::Open` synchronously and returns `ResponseFuture` (16 bytes, `'static`, `Send`):
  `Option<Arc<StreamShared>>` + `Option<Box<Error>>`. Dropping it before the head arrives cancels the request. Upload pumps are a hand-written `Pump` future.
- **Inbox** is `tokio::sync::mpsc::unbounded_channel`. `ConnShared` does *not* hold the sender (otherwise the actor's own `Arc<ConnShared>` would keep the
  channel open); `H2Handle` = `Arc<ConnShared>` + sender, and each `StreamShared` holds a sender clone for `Ready`. When every handle and stream is gone the actor sends GOAWAY(NO_ERROR) and exits.
- **Receive queue is an inline SPSC ring.** `StreamShared` holds a 4-slot ring of `RecvEvent` (`ring.rs`; `UnsafeCell<MaybeUninit<_>>` + head/tail atomics, Release/Acquire) with a
  `SegQueue<Box<_>>` spill used only when the ring is full: zero allocations and no cross-thread frees per chunk in the common case. FIFO across ring + spill relies on `spill_len`
  (producer pushes to the ring only while it is 0; the consumer drains the ring, re-checks it after seeing `spill_len > 0`, then the spill). Producer = actor, consumer = body reader
  (on cancel the reader drains and hands over to the actor through the command channel). `RecvEvent::Data { view, flow, end }` folds END_STREAM into the last DATA event. The send-side
  queue (request body) is still a boxed `SegQueue`.
- **Wake batching.** Actor-side producers set state and `owe_wake` (per-stream `wake_pending`, plain load/store: actor-only); streams go on `Core::dirty` and are woken once by
  `drain_dirty` at the end of the read phase and once more in the `poll` wrapper (every exit path: teardown, fail_connection, Closing, Drop). A response = one wake instead of three.
  Wake is still "set state, then wake"; the consumer still registers, then checks.
- **Receive accounting.** Every DATA event is charged `flow_len + 64 B` (`EVENT_OVERHEAD`) to the connection's `delivered` (actor-local) and, on consumption/drop, to the atomic
  `ConnShared::consumed` (`buffered = delivered - consumed`). Padding is credited to the stream window on consumption, not on receipt, so padded 1-byte DATA floods cannot
  earn credit. Payloads < 16 B (or padded < 256 B) are copied out of the read block so they cannot pin it. At most 1024 unconsumed events per stream (ENHANCE_YOUR_CALM).
  The consumer sends a `Ready` only when the stream is still open and its stream-window credit crossed the threshold, or when the actor has parked connection credit: the actor sets
  `CONSUMED_BLOCKED` (top bit of the same `consumed` word, via `fetch_or`, which also returns the counter) and consumers' `fetch_add` sees that bit in the previous value, so the
  hand-off is decided by one word's modification order (no Dekker fences; loom's SeqCst model is too weak to check fence-based versions). Cancelled/dropped streams account their queued events in `Drop`.
- **Timeouts.** Per stream: `activity` (updated on open, request DATA emitted, response frames), `head_idle`/`head_abs` (config or `ResponseTimeout`), `body_idle` (config or
  `BodyTimeout`); header-block deadline on `HeaderAsm`. One `Sleep` per connection armed for the earliest known deadline (`Core::want_timer`, re-armed only when a new deadline is
  earlier or after it fires); when it fires `scan_timeouts` walks the stream table (O(streams), only on fire), cancels expired streams with RST(CANCEL) and re-arms. Deadlines only move
  later, so early fires are harmless. Defaults in `H2Config`; keep-alive PING defaults to 30 s.
- **Flood limits.** Leaky buckets (`Leaky`, milli-unit fixed point, tick time) for SETTINGS, PING, RST_STREAM, WINDOW_UPDATE and tiny DATA; overflow is a connection error (GOAWAY ENHANCE_YOUR_CALM).
- **Response validation.** `content-length` vs DATA bytes (not for HEAD/204/304), 16 interim responses max, no RST for streams we do not know, request trailers filtered like headers.
- **Write coalescing (`H2Config::coalesce_small_writes`).** If a tick produced < 1 KiB of output from newly opened requests while > 8 streams are in flight, the write is deferred once
  (self-wake) so concurrent submitters share one `writev`; never twice in a row. Measured +5-10% req/s at c=256 (64 B..64 KiB), no effect below 8 in flight.
- **Actor task is `unconstrained`**: it bounds its own work per tick; tokio's coop budget would make a busy actor read `Pending` as "inbox empty".
- **Vectored TCP reads were tried and rejected** (`readv` over up to 4 blocks via socket2): -20% req/s / +25% CPU per request at 1 MiB, no gain at 64 KiB. `poll_fill` reserves exactly one
  block (the old `max(READ_BLOCK, want)` reserved a second block that the re-wrap dropped unused for every frame larger than a block).
- **`ConnectionInfo`** is no longer inserted into `http::Extensions` by default (3 allocations per response): `Incoming::connection_info()`; `H2Config::connection_info` / `H1Config::connection_info`
  / `ClientBuilder::connection_info(true)` restore the extension (needed by the otel layers for `network.protocol.name`). Huffman-decoded strings are split off a per-decoder arena.
- **Read path.** `Reader` = pooled `BytesBuf` (64 KiB blocks, `buf::memory()` re-fetched per block so the thread-local pool follows the task) + a `peek()` view for parsing.
  DATA frames are parsed by hand (header only) and sliced with `consume` => zero-copy even when the payload spans blocks. Non-DATA frames go through `decode_frame` on the
  contiguous bytes; if one straddles two blocks it is copied into a small scratch `Vec` (<= MAX_FRAME_SIZE; control/header frames only). Parsed non-DATA bytes are consumed lazily in one batch (`skip`).
  The only `unsafe` is `BytesBuf::advance` after `ReadBuf::filled()` (commented).
- **Header blocks** are copied once into a shared `BytesMut` (split + freeze, so HPACK values are slices of it). CONTINUATION reassembly is bounded: 32 frames and 2x MAX_HEADER_LIST_SIZE, else ENHANCE_YOUR_CALM.
- **Flow control.** Connection recv window is credited on receipt after each read burst, gated by `buffered <= buffered_budget` (= `initial_conn_window`).
  `buffered` is decremented from consumer reports. Connection-level consumption is reported through the atomic `consumed` counter (see "Receive accounting"); stream-level credit goes through `Ready` once consumed >= `min(window/4, 32 KiB)` and on cancel
  (regression tests `many_small_responses_never_stall_the_connection_window`, `consumption_without_ready_still_credits_the_connection_window`).
  Stream windows use `RecvWindow`'s 25% policy on the actor side. INITIAL_WINDOW_SIZE increases check overflow by iterating live streams (rare) rather than tracking a max offset.
- **Write path.** As blueprint. Control frames are encoded straight into the arena in arrival order (ahead of any not-yet-scheduled DATA, behind already-queued bytes; DATA scheduling stops at 256 KiB queued).
  Reads pause only if >1 MiB is queued unwritten; commands pause at 4 MiB. Small payloads (<= 256 B) are copied into the arena.
- **Completed-early responses**: if the server ends the response before our request body is done, we send RST_STREAM(NO_ERROR) and drop the stream (RFC 9113 8.1).
- **Errors.** Stream/connection failure => `Retryable` if the stream's HEADERS were fully written, else `Unprocessed`; GOAWAY above last_stream_id and REFUSED_STREAM => `Unprocessed`;
  RST codes CANCEL/INTERNAL/NO_ERROR/STREAM_CLOSED/CONNECT => `Retryable`, others `Never`.
- **Extras in `H2Config`:** `keepalive_timeout`, `handshake_timeout`, `connection_info` (attach `ConnectionInfo`; costs ~3 allocations, the pool may prefer to do it).
- **loom.** `--cfg matier_loom` (not `loom`: tokio disables `net` under `cfg(loom)`). `sync.rs` swaps atomics/Arc/Mutex/AtomicWaker/queue/UnsafeCell; models: head handoff (value and error), recv queue + waker, QUEUED dedup,
  cancel vs push accounting, ring + spill FIFO with END, drop with queued events, deferred-wake dedup vs register-then-check, blocked-bit handshake, send-credit wakeup, slot accounting (the larger ones use `preemption_bound = 3`). `RUSTFLAGS="--cfg matier_loom" cargo test -p matier-transport --lib --release loom_` (use a separate `CARGO_TARGET_DIR`).
- Not done: RFC 9218 priority scheduling (round-robin only), BDP estimation, per-connection free list for `StreamShared`, custom MPSC inbox, `Expect: 100-continue` wake.

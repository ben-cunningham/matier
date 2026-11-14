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

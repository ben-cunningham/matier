# Matier — plan for a simpler, faster HTTP client stack for Rust

> Named after Mount Matier (Pemberton, BC). Status: **planning / Phase 0**. Last updated 2026-11-14.

Matier is a from-scratch HTTP/1.1 + HTTP/2 client stack for tokio, built to replace hyper / h2 / hyper-util for
high-throughput client workloads. It targets the problems that force people to pool *clients* today (see
microsoft/oxidizer `fetch`'s `multiple_pools`):

1. one HTTP/2 connection per origin (hyper-util legacy pool, by policy), and
2. a connection that cannot use more than ~one core because of h2's single big mutex (hyperium/h2#531, PR #917).

Research backing every decision below lives in [`docs/research/`](research/):

| File | Contents |
|---|---|
| [go-http-client.md](research/go-http-client.md) | Go master (2026-11) net/http + internal/http2, line-referenced |
| [hyper-complaints-and-specs.md](research/hyper-complaints-and-specs.md) | 35 prioritized hyper/h2/hyper-util/reqwest complaints + RFC baseline |
| [oxidizer-bytesbuf-fetch.md](research/oxidizer-bytesbuf-fetch.md) | `bytesbuf`, `layered`, `fetch` design and telemetry |
| [tls.md](research/tls.md) | TLS backend performance comparison and recommendation |

---

## 1. Goals, non-goals, success criteria

**Goals**
- Scale across cores with **no connection-wide lock on the request path**. A connection's protocol state has exactly one owner.
- **Go-style multi-connection HTTP/2 pools** per origin, chosen by free stream capacity and load, so "pool of clients" is never needed.
- **Minimal copies and allocations**: zero-copy body send (vectored writes of caller buffers), pooled receive buffers sliced into bodies, and a measured allocations-per-request budget.
- **Composable layers** in the style of `fetch` (`layered::Service`), with per-request options carried in `http::Extensions`.
- **Optional OpenTelemetry** with deep transport insight: queueing latency, connection-task saturation, flow-control stalls, write batching.
- **Spec-current**: RFC 9110/9112/9113/7541/9218 (+ 9651 structured fields), hardened against CONTINUATION, rapid-reset-style and flood attacks.
- **AI-iterable**: every performance claim is backed by a reproducible benchmark with machine-readable output and a regression gate.

**Platforms (tier 1, tested in CI):** Linux (x86_64, aarch64), **Windows (x86_64-pc-windows-msvc)**, macOS (aarch64). See §16.

**Non-goals (for now)**: server side; HTTP/3 (planned in Phase 7, via quinn); runtimes other than tokio. The protocol
cores are sans-IO, so other runtimes and io_uring stay possible later.

**Success criteria** (measured by `matier-bench` on the same host, server in a separate process):

| Scenario | Target vs. best of hyper-legacy / hyper-pooled(N) / reqwest |
|---|---|
| h2, 1 origin, c=512, 1 KiB responses | ≥ 1.5× req/s, ≤ 0.7× CPU-µs/req, p99 no worse |
| h2, 1 origin, c=512, single connection forced | ≥ 1.3× req/s (proves the per-connection design, not just more sockets) |
| h1, c=256, 1 KiB | ≥ 1.0× req/s, ≤ 0.7× allocs/req |
| 1 MiB POST/GET bulk, h2 | ≥ 1.5× GB/s (h2#902 shows ~1.8× from write batching alone) |
| Open loop at 80% of max rate | p99.9 ≤ 2× p50 (no ~40 ms stalls, cf. hyper#3071, h2#879) |
| Allocations per request (h2 GET, small) | ≤ 6, enforced in CI |

---

## 2. What we learned (short version)

**hyper / h2** ([complaints](research/hyper-complaints-and-specs.md))
- One `Mutex` around all stream state per connection; every stream handle and the connection task contend on it (h2#531). The PR #917 retrofit shows how hard fine-grained locking is to add afterwards (lost wakeups, deadlocks, a single-connection drop from 25k to 24 req/s during review).
- One syscall per DATA frame (h2#902: batching gave 4.28 → 7.81 GB/s). HEADERS and the body go out in separate writes (hyper#4241).
- The receive loop decodes every buffered frame before yielding, giving 17× worse p95 than proxygen (h2#879). Reads wait on a blocked control-frame write, which deadlocks (h2#975).
- The connection window is released only when the body is read, so one slow consumer stalls the whole connection (hyper#2899).
- The pool removes a waiter by scanning every waiter under the pool lock, which is O(n²) on cold start (hyper#4245). Per-connection channel memory (hyper#4057). Parsed headers pin read buffers (hyper#4060). GOAWAY errors don't say whether the request is safe to retry (hyper#2500).

**Go** ([source study](research/go-http-client.md)). Go does *not* have a better per-connection design.
- Callers write frames under `cc.wmu`, with one flush (syscall and TLS record) per frame.
- The read loop takes the same write lock for WINDOW_UPDATEs.
- One `cc.mu` guards all stream and flow-control state.
- The 2026 unified pool (`transport_pool.go`) sits behind one Transport-wide mutex.

Go wins on **policy**: it opens new HTTP/2 connections when existing ones are full, coalesces dials, and has cheap goroutine blocking. We copy the policy and the pool state machine, and *improve* on the mechanism.

**oxidizer fetch** ([study](research/oxidizer-bytesbuf-fetch.md))
- Copy: `layered::Service` with `execute(&self)` and no `poll_ready`; the pipeline order; typed `Extensions` options; semconv metrics with `TelemetryAttributes` enrichment; errors carrying `RecoveryInfo`.
- Change: own the transport, make backpressure explicit, add queue-time metrics, pool gauges and spans.

---

## 3. Architecture

```
 user code
    │  http::Request<Body>  (+ Extensions: options, telemetry dimensions)
    ▼
┌──────────────────────── matier (client + layers, layered::Service) ────────────────────────┐
│ total_metrics → total_timeout → retry|hedge → attempt_timeout → redirect → decompress →    │
│ attempt_metrics → Dispatch                                                                  │
└──────────────────────────────────────────────┬──────────────────────────────────────────────┘
                                               ▼
┌──────────────── matier-pool ────────────────┐   per-origin, sharded, no global lock
│ OriginKey → OriginPool                       │   h1: LIFO idle stack + O(1)-cancel waiter list
│   ├─ H1 pool (caller-driven conns)           │   h2: RCU snapshot of conns, atomic stream slots,
│   └─ H2 pool (N conns, P2C on free slots)    │       P2C select, coalesced dials, min/max conns
└───────┬──────────────────────────┬──────────┘
        ▼                          ▼
  matier-h1 (sans-IO codec)   matier-h2 (sans-IO frames, HPACK, streams, flow control)
        │                          │
        ▼                          ▼
  H1Conn: owned by the        H2 connection actor: ONE task owns socket + TLS + protocol state.
  request in flight —         Callers talk to it only through lock-free queues and per-stream
  no task, no channel         atomics/wakers. Batched writev per wake-up; bounded read budget.
        └────────────┬─────────────┘
                     ▼
        matier-transport: TCP (socket2), DNS (pluggable), Happy Eyeballs v2, TLS trait
        (rustls+aws-lc-rs default; see §8), pooled BytesBuf memory per connection
```

### 3.1 Crates

| Crate | Responsibility |
|---|---|
| `matier` | Public client, builder, layer stack, `Body`, `Error`/`RecoveryInfo`, per-request option types |
| `matier-core` | Shared types: `Service` re-export, buffer abstraction over `bytesbuf`, instrumentation hooks (`Observer`), origin keys, timers |
| `matier-h1` | Sans-IO HTTP/1.1 request encoder + response parser (RFC 9112) |
| `matier-h2` | Sans-IO HTTP/2: frame codec, HPACK (RFC 7541), stream state machine, flow control, RFC 9218 scheduler |
| `matier-transport` | tokio I/O: TCP connect, DNS, Happy Eyeballs, TLS backends, the H1Conn and H2 connection actor drivers |
| `matier-pool` | Origin map, h1/h2 pools, protocol negotiation cache, idle reaping |
| `matier-otel` | OpenTelemetry layer + `Observer` impl (feature `otel` on `matier`) |
| `matier-bench`, `matier-bench-server` | Benchmark harness (done in Phase 0) |

Sans-IO cores (`-h1`, `-h2`) do no I/O and own no tasks. They are deterministic, fuzzable, and `loom`/simulation-friendly.

---

## 4. HTTP/2: the core redesign (fixes the lock problem by construction)

### 4.1 Ownership model

Each connection has **one actor task** that exclusively owns:
- the socket (both halves) and the TLS session (rustls is a single state object for both directions, so this fits naturally);
- the HPACK encoder and decoder, the stream-ID counter, the stream table (slab), and the connection-level flow-control windows;
- the write arena (pooled `BytesBuf`) and the pending-iovec list.

Nothing else touches these, so there is **no mutex around protocol state**. Callers interact with the connection through four narrow channels:

| Direction | Mechanism | Bounded by |
|---|---|---|
| caller → conn: open stream | lock-free MPSC command queue (`OpenStream { head, body, shared }`) | stream-slot permit reserved *before* enqueue (atomic CAS in the pool) |
| caller → conn: "stream N has work" (data ready, cancel, window released) | MPSC ready-queue of `Arc<StreamShared>` with an atomic `queued` flag for dedup | one entry per stream |
| conn → caller: response head / trailers / error | single slot inside `StreamShared` + `AtomicWaker` (no oneshot allocation) | 1 |
| conn → body consumer: DATA | SPSC queue of `BytesView` (zero-copy slices of receive blocks) | stream receive window |
| body producer → conn: DATA | SPSC queue of `BytesView` + atomic send-credit counter | per-stream send buffer (min(window, cap)) |

`StreamShared` holds only atomics, wakers, and the two SPSC queues. The producer/consumer sides each have exactly one
writer, so there is no lock and no lost-wakeup class of bugs of the kind PR #917 hit. The protocol is small enough to model
exhaustively with `loom`.

### 4.2 The actor loop

One iteration ("tick") of the actor:

1. **Read with a budget.** Read into pooled 16–64 KiB `BytesBuf` blocks. Decode at most *N* frames or *B* bytes (fixes h2#879), then continue to the next step. Reading never waits on writing (fixes h2#975). Control-frame replies are queued, and if the peer stops reading we stop *accepting* new work rather than deadlocking.
2. **Apply frames.**
   - DATA: hand a `BytesView` slice to the stream's SPSC (no copy) and replenish the **connection** window eagerly on receipt, within a global buffered-bytes budget. Stream windows are replenished on consumption, so a slow stream stalls only itself (fixes hyper#2899).
   - HEADERS / CONTINUATION: HPACK-decode, enforcing limits.
   - SETTINGS / PING / GOAWAY / RST_STREAM / WINDOW_UPDATE: handled here.
3. **Drain the command and ready queues** (bounded batch): assign stream IDs, HPACK-encode HEADERS into the arena, schedule DATA.
4. **Schedule and write.** Gather everything writable into one iovec list:
   - priority order: control frames (SETTINGS ack, PING ack, RST_STREAM — so a cancel never queues behind data, cf. h2#880), then HEADERS, then DATA via the RFC 9218 urgency/incremental scheduler;
   - limited by connection and stream windows;
   - capped at ~256 KiB or IOV_MAX per write;
   - then **one `writev` per tick** (fixes h2#902 and hyper#4241: HEADERS and the first DATA of a known body go out together). With TLS, the plaintext iovecs are encrypted straight into one ciphertext buffer (§8).
   - If the socket returns `Pending`, keep the batch and keep reading.
5. **Small-flush yield.** If the batch is tiny and more work is likely imminent, self-wake once before flushing to coalesce (the grpc-go trick seanmonstar adopted in #917). This is a tunable that the benchmarks will decide.
6. **Health.** Read-idle PING, and pairing RST_STREAM with PING to detect dead connections (from Go). Record the actor's busy-time ratio for telemetry.

### 4.3 Stream lifecycle and cancellation
- Open: pool reserves a slot (atomic) → caller allocates `StreamShared` (from a per-connection free list to avoid an `Arc` allocation per stream) → enqueue → actor assigns ID and writes HEADERS.
- Full bodies (`Body::Full`) are attached to the open command and framed by the actor immediately: one writev, zero copy.
- Drop of the response or body sets a cancel flag and pushes to the ready queue. The actor emits RST_STREAM(CANCEL) with control priority and releases the slot.
- GOAWAY: streams above `last_stream_id` fail with `Error::Unprocessed` (marked safe to retry in `RecoveryInfo`, fixes hyper#2500). REFUSED_STREAM is treated the same way. The connection's `accepting` flag clears so the pool stops selecting it.

### 4.4 Flow control defaults
- Stream receive window 2 MiB and connection window 16 MiB, configurable, with optional BDP auto-tuning via PING RTT samples. Go uses 4 MiB / 1 GiB fixed with no BDP.
- WINDOW_UPDATE is sent when ≥ 25% of the window is consumed (Go: 4 KiB or doubling; ours batches more).
- Stream send windows are stored as offsets from the shared initial window, so a SETTINGS change is O(1) (from Go).
- Hardening limits (all configurable):
  - CONTINUATION count and header-list size;
  - max header-table size;
  - SETTINGS, PING and empty-DATA flood counters;
  - WINDOW_UPDATE overflow checks;
  - max frame size of 16 KiB send / 1 MiB receive.

### 4.5 HPACK
Encoding runs in the actor (ordering requires it). It's cheap, and the multi-connection pool spreads it across cores.

Experiment worth benchmarking: a *stateless* encoding mode (static table + Huffman, no dynamic inserts). Callers could then pre-encode headers in parallel and the actor would only copy bytes. It trades wire size for actor CPU.

---

## 5. HTTP/1.1: no task, no channel

hyper spawns a connection task plus a dispatch channel per HTTP/1 connection; Go uses two goroutines per connection. Matier uses
neither: an `H1Conn` is **owned by the request in flight**.

- **Checkout** → encode the head into the connection's reusable write buffer → **one `writev(head, body…)`**. Small bodies (≤ ~1 KiB) are copied into the head buffer; larger ones are sent as caller iovecs. Chunked framing uses a stack buffer for the size line.
- **Read** the response head into pooled blocks, then parse with an `httparse`-style parser. The head is copied once into a single `Bytes` (typically < 1 KiB), and every `HeaderValue` is a refcounted slice of it, so there is no per-header allocation and no pinning of big read blocks (fixes hyper#4060). Body bytes stay zero-copy `BytesView`s.
- **Response body** holds the `H1Conn`. A drop guard returns it to the pool at body end. If the body is dropped early, a small remainder (≤ 64 KiB) is drained, otherwise the connection is closed.
- **Liveness** is checked at checkout with a non-blocking read probe; there is no background reader per idle connection. Idle reaping uses one coarse timer per pool, not per connection.
- **No pipelining.** Header case is preserved optionally. 100-continue is supported. 1xx responses are handled per RFC 9110 §15.2.

---

## 6. Pool

- **Origin map.** `OriginKey` = (scheme, host, port, TLS config id, proxy). A read-mostly concurrent map (`papaya` or a sharded `RwLock<HashMap>`) maps it to `Arc<OriginPool>`. Per-origin attribute sets for telemetry are precomputed here.
- **H1 OriginPool.**
  - LIFO idle stack (warm connections first), with a short critical section (push/pop only).
  - `total` is an atomic; `max_per_origin` is enforced.
  - Waiters live in an **intrusive list with O(1) removal on cancel** (fixes hyper#4245), and connections are handed directly to the oldest waiter (Go `wantConn` semantics).
- **H2 OriginPool.**
  - An RCU snapshot (`arc-swap`) of live connections. Each connection exposes atomics `in_flight`, `max_streams` (from peer SETTINGS) and `accepting`.
  - **Selection**: power-of-two-choices on `in_flight` among connections below `target_streams_per_conn`, reserving the slot with a CAS.
  - **When nothing is available**: if `conns + dialing < max_conns_per_origin`, start a coalesced dial (one in flight per origin unless demand exceeds what that dial will provide); otherwise wait in the waiter list.
  - **Tuning knobs**: `target_streams_per_conn` (default `min(peer_max, 128)`), `min_conns_per_origin` (pre-spread hot origins across cores), and `max_conns_per_origin`. These replace "N clients round-robin" with load-aware, capacity-aware spreading.
- **Protocol negotiation.** The first dial to an origin uses ALPN, and the result is cached per origin. h2c prior knowledge is opt-in.
- **Connect.**
  - **DNS** sits behind a `Resolver` trait. The default is an async resolver with TTL cache; `getaddrinfo` on a *bounded* blocking pool is the fallback (cf. hyper#3838).
  - **Happy Eyeballs v2** (RFC 8305).
  - **Phase timestamps** are recorded for DNS, TCP and TLS.

---

## 7. Buffers, bodies and the allocation budget

- **Buffers.** `bytesbuf` (MIT, Microsoft) is used through a thin `matier-core::buf` facade, pinned to exact versions, because it publishes breaking 0.x releases often.
  - Each connection gets memory from a per-worker `GlobalPool` (1/4/16/64 KiB blocks).
  - `BytesView` (≤ 8 inline spans) is the body chunk type.
  - Small long-lived slices are copied out so they don't pin whole blocks.
- **Body**:
  ```rust
  pub enum Body { Empty, Full(BytesView), Stream(BodyStream) }  // also impl http_body::Body<Data = BytesView>
  ```
  - `Full` is the fast path: cloneable for retries, sent as iovecs with no copy and no task.
  - `BodyStream` comes from `Body::channel()` → `(BodySender, Body)`, a bounded SPSC with flow-control credit as backpressure. `BodySender::buffer(n)` hands producers pooled memory from the *connection's* provider, so data is written once and never copied again.
- **Write path, end to end**:
  - plaintext: caller memory → iovec → kernel (0 user-space copies);
  - TLS (rustls): caller memory → encrypted into a pooled record buffer → kernel (1, the encryption itself);
  - kTLS (Linux, optional): 0 user-space copies.
- **Read path.** Socket → pooled block → frames and bodies are `BytesView` slices (0 copies). Header heads are copied once (§5).
- **Allocation budget.** The bench reports allocs/req, and CI fails if a scenario exceeds its budget. Known per-request allocations: the `HeaderMap` (×2) and the response head `Bytes`. `StreamShared` comes from a free list.

---

## 8. TLS

See [research/tls.md](research/tls.md).

**Default: rustls 0.23 with the `aws-lc-rs` provider** (the same default as reqwest 0.13 and hyper-rustls 0.27). Source: rustls.dev's 2026-03 report, comparing rustls 0.23.37 with OpenSSL 3.6.1 and BoringSSL.

| | rustls | BoringSSL | OpenSSL |
|---|---|---|---|
| TLS 1.3 full handshakes / core (RSA, client) | **4767** | 3179 | 2220 |
| Resumed handshakes / core | **6736** | 4648 | 3595 |
| Bulk throughput / core, each way | ~7.4 GB/s | ~7.4 GB/s (≈2% ahead on send) | — |
| Memory per connection (2024 figure) | **~13 KiB** | — | ~69 KiB |

rustls also has post-quantum key exchange (`X25519MLKEM768`) on by default and offers a FIPS option.

- **Abstraction**, both statically dispatched:
  - a stream-level `TlsConnector` trait, for handshake, ALPN and resumption;
  - a sans-IO `TlsSession` with `encrypt_into(&[&[u8]], &mut Vec<u8>)` and in-place `decrypt`, for our own drivers.
- **Write path:** one reusable output buffer per connection. Gather plaintext iovecs without concatenating them, encrypt them into full 16 KiB records, and flush once per actor tick.
  - Today, rustls 0.23's normal writer already packs vectored writes into full records; tokio-openssl, tokio-boring and s2n have no vectored write.
  - The 0.23 *unbuffered* API is **not** zero-copy: it adds a memcpy. Its value is caller-owned buffers, for io_uring and the kTLS handoff.
  - **rustls 0.24** (currently 0.24.0-dev) encrypts records directly onto a caller-owned `Vec<u8>` and adds separate send and receive halves plus in-place decryption. This is the single-pass path we want: plaintext → ciphertext in our buffer → kernel. Phase 4 targets it.
- **Read path:** decrypt in place, then hand body bytes up as zero-copy slices.
- **Optional backends:**
  - rustls + `ring`: easiest to cross-compile, but no post-quantum and no FIPS.
  - **kTLS** (Linux; TLS 1.3 key updates need kernel 6.13+): for bulk transfers and `sendfile` uploads.
  - OpenSSL / BoringSSL: policy-driven only, since neither is faster for a client.
  - s2n-tls: skipped.
- **macOS:** no kTLS or io_uring, so rustls is the only performant path.

---

## 9. Telemetry (`matier-otel`, feature `otel`)

**Zero-cost when off.** `matier-core` defines an `Observer` trait with no-op defaults, and the transport, pool and actor call it with
timestamps. `matier-otel` implements it with pre-created instruments and per-origin cached attribute sets, so the hot path does no attribute allocation.

**Request metrics** (OTel HTTP semconv names where they exist):

| Metric | Type | What it shows |
|---|---|---|
| `http.client.request.duration` | histogram (s) | per attempt, semconv |
| `matier.client.request.total_duration` | histogram | logical request including retries/hedges |
| `http.client.request.time_in_queue` | histogram | **queueing latency**: pool checkout wait + H2 stream-slot wait + actor dispatch delay (enqueue → HEADERS on the wire). The direct signal for "is the connection the bottleneck?" |
| `matier.client.request.time_to_first_byte` | histogram | |
| `http.client.request.body.size` / `http.client.response.body.size` | histogram (By) | |
| `http.client.active_requests` | up-down counter | |

**Connection metrics:**

| Metric | Type |
|---|---|
| `http.client.open_connections` (`http.connection.state` = active / idle) | up-down counter |
| `http.client.connection.duration` | histogram |
| `matier.client.connection.setup.duration` (`phase` = dns / tcp / tls) | histogram |
| `matier.tls.resumed` | counter |

**Transport metrics** (the deep view):
- `matier.transport.bytes` (direction), `matier.transport.write.syscalls`, `matier.transport.write.batch.bytes` and `matier.transport.write.batch.iovecs` (is batching working?);
- `matier.h2.actor.busy_ratio` and `matier.h2.actor.queue_depth` (connection saturation, i.e. when to add connections);
- `matier.h2.streams_per_connection`, `matier.h2.flow_control.stall_duration` (time streams wait for window), `matier.h2.goaway`, `matier.h2.rst_stream` (by error code);
- `matier.pool.waiters`, `matier.pool.dials_coalesced`.

**Dimensions.**
- Semconv attributes: `http.request.method`, `server.address`, `server.port`, `url.scheme`, `http.response.status_code`, `network.protocol.version`, `error.type` (from the error label chain).
- User attributes:
  - per request, `req.extensions_mut().insert(TelemetryAttributes::new([KeyValue::new("tenant", t)]))`;
  - globally, `Enricher` closures on the layer (`from_request` / `from_response`);
  - a `UriTemplate` extension for low-cardinality route labels.
- A configurable cardinality guard caps distinct values per key.

**Tracing** (optional): a span per logical request and per attempt, W3C `traceparent` injection, and span events for connection
acquired / headers sent / first byte.

---

## 10. Layers (fetch-style)

Matier uses `layered::Service<In>` (`execute(&self, In) -> impl Future + Send`), depending on the small `layered` crate so it
interoperates with the oxidizer ecosystem (seatbelt retry/hedging). It is re-exported from `matier-core` so it can be replaced if its churn becomes a problem.

Default stack, outermost first:

```
total_metrics → total_timeout → retry | hedging → attempt_timeout → redirect → decompression (lazy)
→ attempt_metrics → Dispatch (validation, scheme policy) → Transport (pool)
```

**Future size budget** (from fetch's practice): every layer's `execute` returns a hand-written future (pin-project-lite) or a
`futures-util` combinator rather than an `async` block, so a layer adds only its own fields and stacks don't nest generated
state machines. Requests are moved into the connection or command early. A test asserts each public future is ≤ 1024 bytes;
growth fails CI.

**Backpressure** is explicit because there is no `poll_ready`: an optional `ConcurrencyLimit` layer awaits a permit, pool waits are bounded with
timeouts, and H2 slot waits happen inside `execute`, all of which feed `time_in_queue`.

**Errors** carry `RecoveryInfo` (`Retryable`, `Unprocessed` (always safe), `Never`, `Unavailable(retry_after)`), so resilience layers stay protocol-agnostic.

**Per-request options** go in `Extensions`: `ResponseTimeout`, `BodyTimeout`, `Priority` (RFC 9218), `TelemetryAttributes`, `UriTemplate`, `ProtocolPreference`.

---

## 11. Where channels are used (and where they deliberately aren't)

| Path | Choice | Why |
|---|---|---|
| H1 request ↔ connection | **none**, direct ownership | No task hop, no dispatch channel memory (hyper#4057) |
| Pool waiter handoff | intrusive waiter list + direct handoff | O(1) cancel; handoff avoids a thundering herd |
| Caller → H2 actor (open) | lock-free MPSC | Single consumer owns the state; admission is bounded by slot permits |
| Stream → actor (work ready) | MPSC of stream refs + dedup flag | Wakes the actor once per stream per tick |
| Actor → body consumer | SPSC of `BytesView` | Zero-copy, single producer and single consumer |
| Body producer → actor | SPSC + atomic credit | Backpressure = flow-control credit |
| Response head | in-struct slot + `AtomicWaker` | Saves a oneshot allocation per request |
| Telemetry | none; direct instrument record | Exporters batch on their own |

---

## 12. Correctness strategy

- **Sans-IO codecs:** `proptest` round-trips, plus `cargo-fuzz` targets for the H1 head parser, H2 frame decoder and HPACK decoder (with differential fuzzing against `h2`'s HPACK).
- **Concurrency:** `loom` models for `StreamShared` (open/data/cancel/window races) and the pool slot CAS. Deterministic network simulation (`turmoil`) for actor/pool behaviour under loss, GOAWAY and stalls. `miri` on buffer code.
- **Conformance and interop:** test servers in CI (Go net/http, nghttp2/nghttpx, h2o, Envoy), adversarial test servers (flood and limit tests), and RFC 9113 MUST checklist tests (stripping connection-specific headers, SETTINGS ACK, window limits, zero-increment WINDOW_UPDATE, MAX_FRAME_SIZE changes).
- **Soak:** 24h open-loop runs with memory and handle tracking.

---

## 13. AI-enabled continuous improvement

The repository is set up so an agent (or a human) can safely run a **hypothesis → change → measure → keep/revert** loop:

1. **Fitness functions.**
   - `scripts/bench.sh bench/quick.toml` (under 60 s) and `matier-bench compare base.json new.json --threshold 5`, which exits non-zero on regression;
   - criterion micro-benchmarks for the codecs;
   - allocs/req budgets.
2. **Guardrails.** `cargo test`, loom, fuzz smoke runs, and clippy must pass. Invariants are documented in `CLAUDE.md` (single owner of connection state, no locks across `.await`, no std `Mutex` on the actor path).
3. **Ledger.** `bench-results/ledger.jsonl` gets one line per experiment (commit, hypothesis, scenario deltas, kept or reverted), so agents learn from past attempts.
4. **Profiles.** `scripts/profile.sh` (samply / perf) writes flamegraphs that agents can read.
5. **Production feedback.** `matier-otel` metrics (`time_in_queue`, `actor.busy_ratio`, `write.batch.*`, `flow_control.stall_duration`) point at the next bottleneck. A runbook maps symptom to likely cause to experiment.
6. **Automation.** A Claude Code skill/command (`/perf-iterate`) wraps the loop. CI posts bench diffs on PRs.

---

## 14. Roadmap

| Phase | Deliverable | Exit criteria |
|---|---|---|
| **0 — Foundations** (now) | Research, this plan, `matier-bench` + server, baseline results, `CLAUDE.md` | Baselines committed for hyper-legacy, hyper-pooled, reqwest |
| **1 — Core + HTTP/1** | `matier-core` (buf facade, Observer, errors), `matier-h1` codec, H1Conn, H1 pool, TCP + rustls, `matier` client + Dispatch; `matier` bench subject | h1 ≥ hyper req/s, ≤ 0.7× allocs/req; codec fuzzed |
| **2 — HTTP/2 sans-IO** | Frames, HPACK, stream state machine, flow control, RFC 9218 scheduler | Fuzzed; differential HPACK tests; RFC 9113 MUST tests |
| **3 — HTTP/2 actor + pool** | Actor, `StreamShared`, multi-connection P2C pool, coalesced dials, GOAWAY/health | loom-clean; meets the §1 h2 targets, including single-connection forced |
| **4 — Transport polish** | rustls unbuffered write path, kTLS (Linux), async DNS, Happy Eyeballs, proxies (HTTP CONNECT, SOCKS5) | Bulk TLS ≥ 1.3× hyper; no blocking-pool thread blowups |
| **5 — Layers + telemetry** | `matier-otel`, timeouts, retry/hedging (seatbelt), redirect, decompression, tracing propagation | Telemetry overhead < 3% CPU at c=512 |
| **6 — Hardening + 0.1** | Interop matrix, flood limits, 24h soak, docs, API review | No P0/P1 issues open; published 0.1 |
| **7 — HTTP/3** | quinn-based h3 with the same pool/telemetry model | — |

---

## 15. Open questions and risks

- **`http` crate types at the API.** Yes for interop (`http::Request`, `HeaderMap`). The `HeaderMap` cost is accepted until measured; a `BytesView`-backed map can come later.
- **`bytesbuf` churn.** Mitigated by the facade and exact pins. Fallback is `bytes` + our own block pool.
- **`layered` vs `tower`.** `layered` is chosen to match fetch. Both use `tower_layer::Layer`, so tower layers still compose.
- **Single actor per connection.** Per-connection throughput is bounded by one core. That is deliberate: multiple connections give the parallelism, and the actor gives correctness and batching. We will measure the single-connection ceiling against h2 #917 and Go.
- **Windows parity.** See §16. The risk is mainly build tooling (aws-lc-rs) and benchmark realism, not the design.
- **Spec drift.** New since 2023: QUERY (RFC 10008), incremental forwarding (RFC 10036), RFC 9931 (optimistic protocol transitions). Track httpbis and add these when relevant to clients.

---

## 16. Windows support (tier 1)

The design is portable by construction: sans-IO cores, tokio (IOCP via mio), and `socket2` for socket options. What needs explicit care:

| Area | Windows approach |
|---|---|
| Vectored writes | tokio `TcpStream::poll_write_vectored` maps to `WSASend` with multiple `WSABUF`s, so batching works the same. Cap iovecs per call at 64–1024 (there is no `IOV_MAX`); benchmark the value. |
| TLS | rustls + aws-lc-rs builds with MSVC (CMake; NASM is optional with recent aws-lc-sys prebuilt objects; verify in CI). The `ring` provider is the low-friction fallback feature. kTLS is Linux-only and cfg-gated. |
| Certificate roots | `rustls-platform-verifier`, which uses the Windows cert store (SChannel chain validation). Also macOS Keychain and Linux system roots. Default on all platforms; `webpki-roots` is optional. |
| Socket options | `socket2` for `TCP_NODELAY`, keepalive (`SIO_KEEPALIVE_VALS`) and buffer sizes. No `libc` outside `cfg(unix)`. |
| DNS | `getaddrinfo` on a bounded blocking pool (works on Windows). An async resolver behind the `Resolver` trait is optional. |
| Timers | tokio timers. Windows default timer resolution is ~15.6 ms for sleeps: use `Instant` for measurements, never sleep-based pacing in hot paths (the bench's open-loop scheduler spins/yields for sub-ms precision). |
| Benchmarks | Process metrics via `GetProcessTimes` / `GetProcessMemoryInfo` (`windows-sys`). The harness spawns the server itself (no bash dependency); `scripts/bench.ps1` mirrors `bench.sh`. Watch out for ephemeral port exhaustion / TIME_WAIT on h1 churn tests and Defender real-time scanning skew. |
| Comparison | On Windows, also benchmark fetch's **WinHTTP** transport (`fetch_winhttp`) as an OS-native baseline. |
| CI | GitHub Actions matrix: `ubuntu-latest`, `windows-latest`, `macos-latest`, running build, test, clippy, and a quick bench smoke (no perf gating on shared runners; perf gating happens on a dedicated runner). |
| Future | IOCP-native / registered I/O (RIO) is a possible later backend through the sans-IO cores, like io_uring on Linux. Not planned now. |

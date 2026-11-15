# Fable 5.1 reviews — 2026-11-14

These are read-only expert reviews of the code at commit `10d2ce8`: one for performance, one for security. The findings were
spot-checked against the code before implementation. Fixes are tracked in `bench-results/ledger.jsonl` and in later commits.

## Performance (priority order)

| ID | Pri | Finding | Proposed change |
|---|---|---|---|
| F1 | P0 | Receive path does one wake per event and ≥ 2 wakes per response, and allocates `Box<BytesView>` per DATA chunk in a `SegQueue` (whose first push allocates a ~770 B block). This explains the measured ~5–6 µs CPU per DATA frame. | Batch wakes per tick with a `WAKE_PENDING` flag and a dirty list; fold END_STREAM into the data event; use an inline SPSC ring with an overflow spill. All need loom models. |
| F2 | P0 | Every response sends a `Ready` command to the actor just to decrement the buffered-bytes counter. | Keep buffered accounting in atomics (`delivered − consumed`); send `Ready` only for stream WINDOW_UPDATE thresholds and cancels. |
| F3 | P1 | Reads are capped at one 64 KiB block, so a 65,545-byte frame reserves 2 blocks, reads 1, and drops the other. | Fix the reservation; use `try_read_vectored` over several blocks on TCP. |
| F4 | P1 | Tokio's per-task coop budget truncates actor ticks, and a budget `Pending` is misread as "inbox empty" or "not writable". | Spawn the actor with `tokio::task::unconstrained`. |
| F5 | P1 | Scales worse than hyper from 1 to 4 connections (×1.4 vs ×2.2). | F1 + F2 + F4 first, then test actor task migration, `SO_RCVBUF`, and a larger read budget. |
| F6 | P1 | 7 allocs/req (h1) and 10 (h2). | Move `ConnectionInfo` out of `Extensions` (−3); Huffman strings into a per-connection scratch buffer; copy the h1 head into a per-connection `BytesMut`; free-list `StreamShared`; borrowed `ResponseRef` API with lazy `HeaderMap` (the fasthttp model). |
| F7 | P1 | Pool hot path: about 6 contended atomic RMWs per request (Arc clones, an unconditional `Notify`, hot and cold fields sharing a cache line). | Borrow inside the papaya guard; separate the inbox Arc; notify only when there are waiters; `repr(align(128))` field groups. |
| F8 | P1 | Request HEADERS go out as ~19-byte writes (0.7 writev per request). | Bounded small-write deferral. |
| F9 | P1 | Huffman decode runs at 228 MiB/s; the encoder looks up the dynamic table with a linear scan. | nghttp2-style 4-bit state machine; hashed index; per-origin pre-encoded prefix. |
| F10–F13 | P2 | `Instant::now` per tick; telemetry costs 1.08 µs/request (4 SDK calls); no DNS cache or `SO_RCVBUF`; invariant notes. | See the full review text in the session log. |

Correctness bug found in passing: in `h1/conn.rs` `write_all`, a body with more than ~14 spans gets the chunk trailer
(`\r\n`) appended mid-body, so the bytes go out of order. Verified and fixed first.

## Security (no critical findings)

The codecs held up: no panics reachable from the network, HPACK and flow-control arithmetic correct, framing safe
against splitting and smuggling, 3 `unsafe` blocks sound, no double-send path in retries, and TLS defaults sound (0-RTT off).

| ID | Severity | Finding | Fix |
|---|---|---|---|
| H2-01 | High | The buffered budget counts payload bytes only, so padded or 1-byte DATA floods pin 64 KiB read blocks (GBs of memory). | Charge flow bytes plus per-event overhead; cap queued events per stream; copy tiny payloads out. |
| TO-01 | High | No response-head, body-idle, header-block or checkout timeouts; HTTP/2 keepalive off. | Defaults of 30 s head, 60 s body idle, 10 s header block, 30 s checkout, 30 s keepalive. |
| H2-02 | Medium | `:authority` carries URI userinfo (credentials leak; RFC 9113 §8.3.1). | Strip userinfo. |
| POOL-01 | Medium | MAX_CONCURRENT_STREAMS=0 wedges the origin and triggers autoscale dials. | Treat as unselectable; bounded wait; drain. |
| H1-01 | Medium | A trickled head is re-parsed from scratch on every read, which is quadratic. | Incremental parse; cap reads per head. |
| H2-03 | Med-Low | No SETTINGS/PING/RST/empty-DATA flood limits; O(streams) work per SETTINGS. | Leaky buckets → GOAWAY(ENHANCE_YOUR_CALM). |
| OTEL-01 | Low-Med | `url.full` keeps sensitive query values; traceparent goes to every host. | Redaction policy; propagation allowlist. |
| Low/Info | | RST for never-opened streams; unbounded 1xx; chunk-size leniency (`0x10`); h2 content-length not checked; request trailers unfiltered; `Client::new()` has no TLS; leftover `MATIER_DBG` print; latent over-length streaming body. | See the implementation commit. |

## Second round (simplification + performance), 2026-11-14

**Simplification**: applied as one commit:
- Dead fields, wrappers and leftover bindings removed from the h2 actor and the pool.
- A single `drain_recv()` shared by cancel, Drop and `cancel_stream`.
- `Actor::drop` delegates to `teardown`.
- The pool's `origin_pool` reuses the allocation-free `lookup`.
- Bench diagnostics trimmed.

Not applied: helper extractions that would add as many lines as they remove, and public API that merely has no in-repo caller.

**Performance**: measured A/B on `bench/gaps.toml`; see `bench-results/ledger.jsonl`.

| Item | Result |
|---|---|
| WINDOW_UPDATE at 1/2 window, consumer reports at the same threshold (was 1/4, capped at 32 KiB) | Kept. 1 MiB c=32: +9% req/s, −9.5% CPU. 8 MiB c=8: +4%, now level with hyper-pooled. |
| Skip coalescing hold when the last read moved ≥ 256 KiB | Rejected. 64 KiB c=256 got worse; 1 MiB did not improve. |
| Re-arm hold fallback timer only when expired | Rejected. Neutral. (`TInstant::from_std` also breaks paused-clock tests.) |
| Autoscaler deep-rule constants for c=64 bimodality | Not changed. Pinned sweep: 4 connections is optimal (247k vs 193k at 2). 16/16 auto runs settled at 4–6 connections after the WU change. |
| Ring head/tail padding, 256 KiB read blocks, Notify waiter count, Arc free list | Deferred. Gains are below the noise floor here, or need a new loom-checked protocol. |

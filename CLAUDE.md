# Matier — agent guide

Matier is a from-scratch HTTP/1.1 + HTTP/2 client stack for tokio. The goal is to beat hyper/h2/hyper-util on
multi-core throughput, tail latency and allocations. Start with `docs/PLAN.md`; the contracts are in `docs/design/`.

## Layout
- `crates/h1`, `crates/h2`: sans-IO codecs (no tokio, no I/O, deterministic). `matier-h1`, `matier-h2`.
- `crates/core`: shared types (Body, Error/Recovery, Observer, OriginKey, buffer facade over `bytesbuf`).
- `crates/transport`: connect/TLS, H1Conn (caller-driven), H2 connection actor.
- `crates/pool`, `crates/matier`: pooling, client and layers (`layered::Service`).
- `crates/bench`, `crates/bench-server`: benchmark harness and its server.

## Invariants (do not break; review every change against these)
1. HTTP/2 connection state has exactly one owner: the actor task. No connection-wide `Mutex`/`RwLock`, ever.
2. Never hold any lock across `.await`. No `std::sync::Mutex` on the actor's hot path. The only exception is the per-stream one-shot slots (see `docs/design/h2-actor.md`).
3. Cross-task signalling: register the waker, then check state; set state, then wake. Every such protocol has a `loom` test.
4. Reading never waits on writing. Per-tick work is bounded (frames, commands, bytes).
5. No copies of body bytes on the send path (iovecs over caller `BytesView`s). Receive-path bodies are slices of read buffers. Header heads are copied once.
6. Sans-IO crates stay free of tokio and I/O.
7. **Small futures.** Public and hot-path futures (`Client::send`, `Service::execute` in every layer, `Pool::send`, `H1Conn::send`,
   `H2Handle::send`) are hand-written `Future` types (pin-project-lite) or `futures-util` combinators, not `async fn`/async blocks.
   Generated state machines balloon and nest across layers. A layer's future adds only its own fields to the inner future.
   Move `Request<Body>` into the connection or command early rather than holding it across awaits (`Body` is ~272 B).
   `tests/future_sizes.rs` asserts every such future is ≤ 1024 bytes; a growth beyond that is a regression.
8. Windows (x86_64-pc-windows-msvc) is tier 1: no `libc` or unix-only APIs outside `cfg(unix)`; use `socket2`.

## Commands
- Build/test: `cargo test --workspace`; lint: `cargo clippy --workspace --all-targets -- -D warnings`; `cargo fmt --all`.
- Windows check: `cargo check --workspace --target x86_64-pc-windows-msvc`.
- Quick perf loop (< 60 s): `scripts/bench.sh bench/quick.toml <name>`, then
  `cargo run --release -p matier-bench -- compare bench-results/baseline/<base>.json bench-results/<name>.json --threshold 5`.
- Full matrix: `scripts/bench.sh bench/matrix.toml <name>`.

## Performance work protocol
1. State a hypothesis, tied to a metric: req/s, p99, CPU-µs/req, allocs/req, write batch size.
2. Measure a baseline on the same machine, make the change, re-measure, then `compare`.
3. Keep the change only if it improves the target metric with no regression above the threshold elsewhere, and all tests pass.
4. Append one line to `bench-results/ledger.jsonl`:
   `{"date","commit","hypothesis","scenario","before","after","kept"}`.
5. Never commit benchmark numbers produced while other heavy processes were running.

# Matier

Matier is an HTTP/1.1 and HTTP/2 client for Rust and tokio, written from scratch to scale across cores without
configuration. It is named after Mount Matier, near Pemberton, British Columbia.

> **Status: pre-release.** The client works end to end and is tested on Linux, macOS and Windows, but the API is
> not stable and nothing is published to crates.io yet. See [Status](#status) for what is missing.

## Why another HTTP client

A single hyper client sends all HTTP/2 traffic to an origin over one connection, and the `h2` crate guards each
connection's state with one mutex ([hyperium/h2#531](https://github.com/hyperium/h2/issues/531)). Under load,
every request thread contends on that lock, and adding cores stops helping. The usual workaround is to create
several clients and spread requests across them by hand, as Microsoft's
[fetch](https://github.com/microsoft/oxidizer) does in its pooled mode. That works, but you have to pick the
number of clients in advance, and each client still has the same lock.

Matier removes the lock and the guesswork:

- **One owner per HTTP/2 connection.** Each connection is run by a single task that owns its state. Callers
  hand it requests through a channel and read responses from lock-free per-stream queues. There is no
  connection-wide mutex, and all frames queued in one pass go out in a single `writev`.
- **Automatic connection scaling.** The pool watches how busy each connection is and how long requests wait,
  opens more HTTP/2 connections when one is saturated, and closes them when load drops. The ceiling is the
  runtime's worker count. You do not configure a client count.
- **Request coalescing.** When many requests are in flight, new requests are held for a few microseconds so
  they share one write. The server then reads and answers them together, which cuts system calls on both
  ends.
- **Few allocations.** Request and response bodies are views into pooled buffers
  ([`bytesbuf`](https://crates.io/crates/bytesbuf)); body bytes are not copied on the send path. A request
  costs 2 heap allocations over HTTP/1.1 and 4 over HTTP/2.
- **HTTP/1.1 without a background task.** An HTTP/1.1 request runs on the caller's task. There is no
  per-connection task and no channel hop.

It also includes:
- rustls TLS with aws-lc-rs and the operating system's certificate store;
- composable middleware via [`layered`](https://crates.io/crates/layered) (the same model fetch uses);
- optional OpenTelemetry metrics and tracing that cover queueing time, connection load and flow-control
  stalls.

## Performance

Requests per second with a fixed number of requests in flight (c). Matier uses default settings. "hyper" is
`hyper-util`'s client, alone and as 4 clients side by side; "fetch" is Microsoft's fetch in its standard and
pooled modes. Higher is better.

| Protocol | Workload | c | **Matier** | hyper | hyper ×4 | fetch | fetch ×4 |
|---|---|--:|--:|--:|--:|--:|--:|
| HTTP/2 | 1 KiB responses | 1 | **32,308** | 24,380 | 24,593 | 22,606 | 22,163 |
| HTTP/2 | 1 KiB responses | 64 | **259,946** | 79,423 | 163,333 | 66,504 | 116,594 |
| HTTP/2 | 1 KiB responses | 256 | **379,164** | 82,025 | 188,300 | 65,894 | 125,183 |
| HTTP/2 + TLS | 1 KiB responses | 256 | **359,777** | 80,259 | 183,298 | 65,375 | 122,722 |
| HTTP/2 | 64 KiB responses | 64 | **124,458** | 55,898 | 107,643 | 29,787 | 66,885 |
| HTTP/2 | 1 MiB responses | 8 | **10,088** | 6,562 | 9,670 | 2,660 | 4,388 |
| HTTP/2 | 8 MiB responses | 8 | **1,238** | 804 | 1,216 | 313 | 530 |
| HTTP/2 | 1 MiB uploads | 16 | **9,472** | 5,997 | 8,905 | 6,362 | 9,122 |
| HTTP/1.1 | 1 KiB responses | 1 | **37,617** | 33,512 | 28,766 | 30,608 | 25,961 |
| HTTP/1.1 | 1 KiB responses | 256 | **158,550** | 153,347 | 151,156 | 121,398 | 133,613 |
| HTTP/1.1 + TLS | 1 KiB responses | 256 | **154,588** | 151,264 | 151,369 | 116,313 | 129,825 |
| HTTP/1.1 | 8 MiB responses | 8 | **1,228** | 1,170 | 1,181 | 1,176 | 1,132 |

Latency and efficiency against hyper ×4, the strongest alternative:

| Protocol | Workload | c | p99 latency | CPU per request | Allocations per request |
|---|---|--:|--:|--:|--:|
| HTTP/2 | 1 KiB responses | 256 | 1.29 ms vs 2.50 ms | 9.7 µs vs 20.9 µs | 4 vs 14 |
| HTTP/2 | 64 KiB responses | 64 | 0.76 ms vs 0.94 ms | 23.7 µs vs 37.2 µs | 4 vs 17 |
| HTTP/2 | 8 MiB responses | 8 | 8.06 ms vs 8.02 ms | 1,609 µs vs 1,537 µs | 4 vs 110 |
| HTTP/1.1 | 1 KiB responses | 256 | 1.97 ms vs 2.23 ms | 21.7 µs vs 28.0 µs | 2 vs 14 |
| HTTP/1.1 | 8 MiB responses | 8 | 8.95 ms vs 9.24 ms | 1,484 µs vs 1,728 µs | 2 vs 15 |

How to read these numbers:

- All runs used one 10-core Apple Silicon Mac, with client and server on the same machine over loopback. They
  compare clients against each other; they are not production capacity figures.
- The test server is built on hyper. On HTTP/1.1 it saturates near 155k requests/s, so every client hits the
  same ceiling and the differences show up in CPU and allocations instead.
- Repeated runs vary by about 5%. Rows within that margin, such as HTTP/1.1 64 KiB and 1 MiB uploads, should
  be read as ties.
- Matier is not ahead everywhere. On HTTP/2 8 MiB downloads, a single hyper client uses less CPU per request
  (1,184 µs vs 1,609 µs), though it completes about a third fewer requests.
- The full matrix covers 16 scenarios across all five clients. It is in
  [`bench-results/report.md`](bench-results/report.md), and [`bench/report.toml`](bench/report.toml)
  reproduces it.

## Usage

Matier is not on crates.io yet. Use it from git:

```toml
[dependencies]
matier = { git = "https://github.com/ben-cunningham/matier" }
http = "1"
http-body-util = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use http_body_util::BodyExt;
use matier::{Body, Client};

#[tokio::main]
async fn main() -> matier::Result<()> {
    let client = Client::new(); // cheap to clone; clones share one connection pool
    let req = http::Request::get("https://example.com/").body(Body::empty()).unwrap();
    let resp = client.send(req).await?;
    println!("status: {}", resp.status());

    let mut body = resp.into_body();
    while let Some(frame) = body.frame().await {
        if let Some(chunk) = frame?.data_ref() {
            println!("{} bytes", chunk.len());
        }
    }
    Ok(())
}
```

`Client::builder()` exposes pool, HTTP/1.1, HTTP/2, TLS and connection settings, and accepts an `Observer` for
telemetry. HTTPS uses the operating system's trust store by default. Per-request options go in the request's
extensions: `ResponseTimeout`, `BodyTimeout`, `Priority` (RFC 9218) and `UriTemplate` (a low-cardinality route
name for telemetry).

## Crates

| Crate | Purpose |
|---|---|
| `matier` | The client: builder, request dispatch and middleware composition. |
| `matier-core` | Shared types: `Body`, errors with retry classification, `Observer`, buffer helpers. |
| `matier-h1` | HTTP/1.1 codec. Sans-IO: no tokio, no sockets. |
| `matier-h2` | HTTP/2 frames, HPACK and flow control. Sans-IO, `#![forbid(unsafe_code)]`. |
| `matier-transport` | TCP and TLS connect, the HTTP/1.1 connection and the HTTP/2 connection task. |
| `matier-pool` | Per-origin pooling and HTTP/2 autoscaling. |
| `matier-otel` | Optional OpenTelemetry metrics and tracing layers. |
| `matier-bench`, `matier-bench-server` | Benchmark harness and its test server. |

## Status

Done:
- HTTP/1.1 and HTTP/2 over plain TCP and TLS (ALPN), with HTTP/2 prior knowledge as an option.
- Automatic HTTP/2 connection scaling, GOAWAY handling and connection draining.
- Flow control, request and response streaming, trailers.
- Response-head, body-idle, keepalive and header-block timeouts.
- Flood limits for SETTINGS, PING, RST_STREAM and empty DATA frames.
- OpenTelemetry metrics and tracing.

Not done yet:
- Middleware for redirects, retries and decompression.
- Proxies (HTTP CONNECT, SOCKS5).
- Happy Eyeballs: connection attempts are sequential.
- kTLS.
- HTTP/3.

The roadmap is in [docs/PLAN.md](docs/PLAN.md#14-roadmap).

## Design documents

- [docs/PLAN.md](docs/PLAN.md): goals, architecture and roadmap.
- [docs/design/h2-actor.md](docs/design/h2-actor.md): the HTTP/2 connection task, its concurrency protocol and
  request coalescing.
- [docs/design/interfaces.md](docs/design/interfaces.md): contracts between the crates.
- [docs/research/](docs/research/): notes on Go's `net/http`, hyper's open issues, TLS library choice and
  fetch.
- [docs/reviews/](docs/reviews/): performance and security review findings, and what was done about each.

## Development

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all

# Concurrency models for the HTTP/2 connection task (loom)
RUSTFLAGS="--cfg matier_loom" cargo test --release -p matier-transport --lib loom

# Benchmarks: builds release binaries, starts the test server, writes bench-results/<name>.json
scripts/bench.sh bench/quick.toml my-change       # about 1 minute
cargo run --release -p matier-bench -- compare \
    bench-results/baseline/<baseline>.json bench-results/my-change.json --threshold 5
```

Fuzz targets for the HTTP/1.1 and HTTP/2 codecs are in `crates/h1/fuzz` and `crates/h2/fuzz` (cargo-fuzz).

Changes must hold the invariants in [CLAUDE.md](CLAUDE.md). Among them:
- No lock spans an `.await`.
- An HTTP/2 connection's state has exactly one owner.
- Hot-path futures stay under 1 KiB; `crates/matier/tests/future_sizes.rs` checks this.
- Windows builds stay first-class.

Performance changes come with before-and-after numbers in
[`bench-results/ledger.jsonl`](bench-results/ledger.jsonl), including the changes that were measured and
rejected.

The minimum supported Rust version is 1.95.

## License

MIT. See [LICENSE](LICENSE).

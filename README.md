# Matier

Matier is an HTTP/1.1 and HTTP/2 client for Rust and tokio, written from scratch to scale across cores without
configuration. It is named after Mount Matier, near Pemberton, British Columbia.

> **Status: early release.** The client works end to end and is tested on Linux, macOS and Windows, but the API is
> not stable. See [Status](#status) for what is missing.

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

### macOS

Requests per second with a fixed number of requests in flight (c). Higher is better. Matier runs with its
default settings. "hyper" is `hyper-util`'s client, alone and as 4 clients side by side; "fetch" is Microsoft's
fetch in its standard and pooled modes. Both are given the same HTTP/2 frame size and flow-control windows as
Matier's defaults (fetch accepts only the stream window); left at 16 KiB frames, they would be about half as
fast on large bodies.

| Protocol | Workload | c | Matier | hyper | hyper ×4 | fetch | fetch ×4 |
|---|---|--:|--:|--:|--:|--:|--:|
| HTTP/2 | 1 KiB responses | 1 | **32,848** | 25,432 | 24,154 | 21,862 | 22,268 |
| HTTP/2 | 1 KiB responses | 64 | **260,141** | 78,003 | 164,964 | 64,294 | 115,896 |
| HTTP/2 | 1 KiB responses | 256 | **380,085** | 82,271 | 188,776 | 62,506 | 125,132 |
| HTTP/2 + TLS | 1 KiB responses | 256 | **347,391** | 80,407 | 185,018 | 62,968 | 122,984 |
| HTTP/2 | 64 KiB responses | 64 | **120,032** | 56,379 | 99,158 | 28,336 | 67,560 |
| HTTP/2 | 1 MiB responses | 8 | 10,035 | 6,303 | 9,992 | 2,686 | 4,775 |
| HTTP/2 | 8 MiB responses | 8 | 1,118 | 822 | 1,085 | 326 | 506 |
| HTTP/2 | 1 MiB uploads | 16 | 9,258 | 6,194 | 8,739 | 6,106 | 9,117 |
| HTTP/1.1 | 1 KiB responses | 1 | **36,469** | 31,362 | 28,924 | 28,103 | 26,558 |
| HTTP/1.1 | 1 KiB responses | 256 | 161,645 | 160,669 | 145,087 | 121,975 | 137,578 |
| HTTP/1.1 + TLS | 1 KiB responses | 256 | 156,190 | 147,923 | 153,231 | 117,230 | 126,298 |
| HTTP/1.1 | 8 MiB responses | 8 | 1,177 | 1,196 | 1,188 | 1,202 | 1,186 |

Bold marks a lead of more than 5%, which is the run-to-run noise. The other rows are ties with the best
alternative.

Latency and efficiency against hyper ×4, the strongest alternative:

| Protocol | Workload | c | p99 latency | CPU per request | Allocations per request |
|---|---|--:|--:|--:|--:|
| HTTP/2 | 1 KiB responses | 256 | 1.29 ms vs 2.49 ms | 9.3 µs vs 20.8 µs | 4 vs 14 |
| HTTP/2 | 64 KiB responses | 64 | 0.94 ms vs 1.08 ms | 23.4 µs vs 37.2 µs | 4 vs 17 |
| HTTP/2 | 8 MiB responses | 8 | 10.4 ms vs 9.5 ms | 1,725 µs vs 1,624 µs | 4 vs 110 |
| HTTP/1.1 | 1 KiB responses | 256 | 2.01 ms vs 3.82 ms | 19.9 µs vs 26.0 µs | 2 vs 14 |
| HTTP/1.1 | 8 MiB responses | 8 | 9.4 ms vs 9.0 ms | 1,543 µs vs 1,687 µs | 2 vs 15 |

How to read these numbers:

- All runs used one 10-core Apple Silicon Mac, with client and server on the same machine over loopback. They
  compare clients against each other; they are not production capacity figures.
- The test server is built on hyper. On HTTP/1.1 it saturates near 160k requests/s, so every client hits the
  same ceiling and the differences show up in CPU and allocations instead.
- Matier is not ahead everywhere. Large HTTP/2 downloads are a tie on throughput, and Matier uses about 6%
  more CPU per request than hyper ×4 there (a single hyper client uses less still, 1,132 µs, at a quarter less
  throughput).
- The frame size matters more than any other setting for large bodies: at 16 KiB frames every client spends
  about twice the CPU per byte, mostly in extra socket reads. Some servers (nginx, Envoy) send small frames
  whatever the client asks for.
- The full matrix covers 16 scenarios across all five clients. It is in
  [`bench-results/report.md`](bench-results/report.md), and [`bench/report.toml`](bench/report.toml)
  reproduces it.

### Windows

The same 80-run matrix has a separate native Windows baseline in
[`bench-results/report-windows.md`](bench-results/report-windows.md), with raw measurements in
`bench-results/baseline/report-windows-2026-11-16.json`. It covers all five clients over HTTP/1.1,
HTTP/2 and TLS, including large downloads and uploads. The report records the Windows host, toolchain
and worker configuration; these results are not directly comparable to the Apple Silicon numbers above.
This is a Windows platform baseline, not a WinHTTP comparison.

## Usage

Add the client to your dependencies:

```toml
[dependencies]
matier = "0.2"
http = "1"
http-body-util = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The client pulls in its internal Matier crates automatically. For OpenTelemetry instrumentation, also add
`matier-otel = "0.2"`; otherwise it is not included.

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

The defaults are tuned and need no changes for most uses. `Client::builder()` has a method for every setting:

```rust
use std::time::Duration;

let client = matier::Client::builder()
    .connect_timeout(Duration::from_secs(5))
    .max_connection_lifetime(Some(Duration::from_secs(300))) // follow DNS and load-balancer changes
    .http2_initial_stream_window_size(8 << 20)               // long fat pipes
    .build()?;
```

| Area | Settings (defaults) |
|---|---|
| Connections | `connect_timeout` (10 s), `tcp_nodelay` (on), `tcp_keepalive` (60 s), `tcp_send_buffer_size` / `tcp_recv_buffer_size` (OS), `local_address`, `http1_only`, `http2_prior_knowledge` |
| Pool | `pool_idle_timeout` (90 s), `pool_max_idle_per_origin` (256), `http1_max_connections_per_origin` (256), `max_connection_lifetime` (none), `pool_checkout_timeout` (30 s), `http2_connections_per_origin` (1 to the worker count, scaled by load) |
| Timeouts | `response_head_timeout` (30 s), `body_idle_timeout` (60 s) |
| HTTP/1.1 | `http1_max_response_head_size` (64 KiB), `http1_max_buf_size` (64 KiB), `http1_title_case_headers`, `http1_allow_obsolete_multiline_headers_in_responses` |
| HTTP/2 | `http2_max_frame_size` (256 KiB), `http2_initial_stream_window_size` (4 MiB), `http2_initial_connection_window_size` (16 MiB), `http2_max_header_list_size` (64 KiB), `http2_keep_alive_interval` (30 s) / `_timeout` (20 s), `http2_max_streams_per_connection` (128), `http2_request_coalescing` (on), `http2_fifo_send` (off) |
| Other | `tls`, `observer`, `connection_info` |

HTTPS uses the operating system's trust store by default. Per-request options go in the request's
extensions: `ResponseTimeout`, `BodyTimeout`, `Priority` (RFC 9218) and `UriTemplate` (a low-cardinality route
name for telemetry).

`matier-transport`'s rustls crypto backend is a cargo feature: `aws-lc-rs` (default) or `ring`, or
`symcrypt` (Microsoft's FIPS 140-3 validated [SymCrypt](https://github.com/microsoft/SymCrypt), Windows and
Linux only, needs the native library present at build and run time) as `default-features = false, features =
["symcrypt"]`.

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
- Response-head, body-idle, keepalive and header-block timeouts; connection lifetime limits.
- Flood limits for SETTINGS, PING, RST_STREAM and empty DATA frames.
- A reset stream keeps its concurrency slot until its RST_STREAM is flushed, so a new request never reuses
  it in the same write (HTTP.sys drops connections that do).
- OpenTelemetry metrics and tracing.

Not done yet:
- Middleware for redirects, retries, hedging, circuit breaking and decompression, and a total request
  timeout. `fetch` has these.
- Adaptive (BDP-based) HTTP/2 flow-control windows.
- Proxies (HTTP CONNECT, SOCKS5).
- Happy Eyeballs: connection attempts are sequential.
- kTLS.
- HTTP/3.

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

On Windows, use PowerShell and keep a separate baseline:

```powershell
.\scripts\bench.ps1 bench\quick.toml quick-windows
.\scripts\bench.ps1 bench\report.toml report-windows
.\target\release\matier-bench.exe compare `
    bench-results\baseline\report-windows-2026-11-16.json bench-results\report-windows.json --threshold 5
```

Compare runs on the same host, with the same matrix and worker count, not Windows results against macOS.

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

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
- native TLS (SChannel on Windows, Secure Transport on macOS, OpenSSL elsewhere) with the
  operating system's certificate store;
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
| HTTP/2 | 1 KiB responses | 1 | **33,512** | 25,833 | 25,256 | 23,490 | 22,798 |
| HTTP/2 | 1 KiB responses | 64 | **262,105** | 79,700 | 163,673 | 66,272 | 114,996 |
| HTTP/2 | 1 KiB responses | 256 | **380,205** | 86,460 | 189,198 | 65,954 | 124,816 |
| HTTP/2 + TLS | 1 KiB responses | 256 | **371,155** | 79,047 | 189,145 | 63,039 | 110,816 |
| HTTP/2 | 64 KiB responses | 64 | **125,384** | 56,128 | 107,802 | 29,537 | 67,364 |
| HTTP/2 | 1 MiB responses | 8 | **10,359** | 6,402 | 9,444 | 2,692 | 4,658 |
| HTTP/2 | 8 MiB responses | 8 | **1,152** | 832 | 1,038 | 343 | 502 |
| HTTP/2 | 1 MiB uploads | 16 | 9,250 | 6,498 | 8,700 | 6,405 | 9,090 |
| HTTP/1.1 | 1 KiB responses | 1 | **38,258** | 35,092 | 31,083 | 31,546 | 25,492 |
| HTTP/1.1 | 1 KiB responses | 256 | 154,214 | 149,663 | 152,552 | 119,518 | 133,350 |
| HTTP/1.1 + TLS | 1 KiB responses | 256 | 162,002 | 151,532 | 151,716 | 107,487 | 118,545 |
| HTTP/1.1 | 8 MiB responses | 8 | 1,217 | 1,215 | 1,124 | 1,098 | 1,146 |

Bold marks a lead of more than 5%, which is the run-to-run noise. The other rows are ties with the best
alternative. HTTP/1.1 + TLS measured 7% ahead in this run but 0–2% ahead of hyper ×4 in two separate
TLS runs, so it is counted as a tie.

Latency and efficiency against hyper ×4, the strongest alternative:

| Protocol | Workload | c | p99 latency | CPU per request | Allocations per request |
|---|---|--:|--:|--:|--:|
| HTTP/2 | 1 KiB responses | 256 | 1.28 ms vs 2.48 ms | 10.0 µs vs 20.8 µs | 4 vs 14 |
| HTTP/2 + TLS | 1 KiB responses | 256 | 1.32 ms vs 2.46 ms | 11.3 µs vs 21.3 µs | 4 vs 15 |
| HTTP/2 | 64 KiB responses | 64 | 0.75 ms vs 0.92 ms | 23.3 µs vs 37.3 µs | 4 vs 17 |
| HTTP/2 | 8 MiB responses | 8 | 10.1 ms vs 10.2 ms | 1,674 µs vs 1,770 µs | 4 vs 110 |
| HTTP/1.1 | 1 KiB responses | 256 | 1.98 ms vs 2.22 ms | 22.6 µs vs 28.0 µs | 2 vs 14 |
| HTTP/1.1 + TLS | 1 KiB responses | 256 | 2.15 ms vs 2.53 ms | 24.1 µs vs 29.0 µs | 2 vs 16 |
| HTTP/1.1 | 8 MiB responses | 8 | 9.25 ms vs 10.2 ms | 1,509 µs vs 1,876 µs | 2 vs 15 |

How to read these numbers:

- All runs used one 10-core Apple Silicon Mac, with client and server on the same machine over loopback. They
  compare clients against each other; they are not production capacity figures.
- The test server is built on hyper. On HTTP/1.1 it saturates near 160k requests/s, so every client hits the
  same ceiling and the differences show up in CPU and allocations instead.
- Matier is not ahead everywhere. On large HTTP/2 downloads it leads hyper ×4 by about 10% with 5% less
  CPU per request, but a single hyper client uses about 30% less CPU (1,132 µs at 8 MiB) at 28% less
  throughput.
- The TLS rows compare TLS stacks as well as clients. Matier and fetch use the operating system's TLS
  (Secure Transport on macOS); hyper uses rustls. Matier reads TLS records through a 32 KiB read-ahead
  buffer, so Secure Transport adds about 1.5 µs of CPU per request over HTTP/1.1 (rustls about 1 µs) and
  Matier's HTTP/1.1 + TLS uses 17% less CPU than hyper ×4 (24.1 µs vs 29.0 µs).
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
matier = "0.3"
http = "1"
http-body-util = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The client pulls in its internal Matier crates automatically. For OpenTelemetry instrumentation, also add
`matier-otel = "0.3"`; otherwise it is not included.

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

TLS uses `native-tls` and `tokio-native-tls`, without rustls or AWS-LC in the client's dependency
graph. Linux builds require the OpenSSL development libraries and `pkg-config`. Windows uses
SChannel; macOS uses Secure Transport. ALPN still negotiates `h2` / `http/1.1`, or only `http/1.1`
for `http1_only`. Certificate and hostname validation remain enabled by default.

`TlsConfig::with_platform_verifier()` keeps the platform trust store.
`TlsConfig::with_root_pem()` trusts only the supplied PEM certificates.
For client certificates or other custom settings, pass a `native_tls::TlsConnectorBuilder` to
`TlsConfig::from_native_tls()`. Native TLS manages session resumption; unlike the previous rustls
connectors, the two ALPN configurations do not explicitly share a session cache.

This is a breaking TLS API change: `from_rustls`, the public rustls configuration fields,
`tls::default_provider`, `with_webpki_roots`, and the `aws-lc-rs`, `ring`, `symcrypt` and
`webpki-roots` transport features are removed. `Io::Tls` now holds a native TLS stream rather than a
rustls stream tuple, and `Io::alpn_protocol` is removed (`Connected::protocol` has the result).
Handshake failures keep their retry classification: a connection that breaks or closes during the
handshake is retryable (`Tls` / `Unprocessed`), and a TLS rejection (certificate, hostname, protocol)
is not (`Tls` / `Never`). DNS/TCP failures and handshake timeouts are unchanged.
Native TLS uses scalar writes (including the standard vectored-write fallback); TCP keeps its
real vectored writes. Independent test servers and the hyper/reqwest benchmark subjects still
use rustls with Ring, and the fetch benchmark subject uses native TLS, so workspace builds do
not require AWS-LC either.
Cargo can still retain AWS-LC entries in the workspace lockfile through fetch's weak optional
rustls feature; these are not in the active client or workspace build trees (`cargo tree`).

### fetch over Matier

`matier-fetch` plugs Matier into Microsoft's [fetch](https://github.com/microsoft/oxidizer) as its transport
handler. fetch keeps its pipeline (retries, hedging, circuit breaker, timeouts, telemetry), and Matier sends
the requests over HTTP/1.1 or HTTP/2, plain or TLS:

```rust
let client = matier_fetch::builder(matier::Client::new()).build();
let resp = client.get("https://example.com/").fetch().await?;
```

Configure connections, protocols and TLS on the `matier::Client`; fetch's own transport options (connection
pool, HTTP/2 settings, supported versions, TLS) do not reach this transport. Every fetch pool slot shares the
one Matier pool, so fetch's `multiple_pools` is not needed.

Requests per second and CPU per request on the macOS machine above, fetch's Minimal pipeline (Standard in
brackets):

| Protocol | Workload | c | fetch over Matier | fetch over hyper | fetch ×4 over hyper | Matier alone |
|---|---|--:|--:|--:|--:|--:|
| HTTP/2 | 1 KiB responses | 256 | 291,820 (144,098) · 14.7 µs | 65,780 (63,048) · 52.0 µs | 124,014 · 34.8 µs | 378,654 · 9.9 µs |
| HTTP/2 + TLS | 1 KiB responses | 256 | 274,442 (138,330) · 15.9 µs | 67,135 (64,623) · 53.6 µs | 108,148 · 40.7 µs | 357,496 · 11.2 µs |
| HTTP/2 | 64 KiB responses | 64 | 122,648 (116,632) · 28.4 µs | 29,507 (29,418) · 91.3 µs | 66,522 · 65.1 µs | 124,140 · 23.6 µs |
| HTTP/2 | 1 MiB responses | 8 | 10,262 (10,302) · 203 µs | 2,691 (2,684) · 703 µs | 4,704 · 539 µs | 10,469 · 195 µs |
| HTTP/1.1 | 1 KiB responses | 256 | 151,747 (122,715) · 27.3 µs | 118,806 (89,185) · 38.9 µs | 132,077 · 36.8 µs | 156,463 · 22.0 µs |
| HTTP/1.1 + TLS | 1 KiB responses | 256 | 149,164 (111,436) · 28.8 µs | 109,430 (81,966) · 42.6 µs | 114,684 · 42.1 µs | 154,443 · 26.2 µs |

fetch's Minimal pipeline over Matier costs 14–16 allocations per small request against 51–62 over hyper
(16 against 309 for 1 MiB HTTP/2 downloads).

What fetch's pipeline costs on top of Matier, averaged over two runs of the full 16-scenario matrix (requests
per second · CPU per request):

| Protocol | Workload | c | Matier | fetch over Matier, Minimal | fetch over Matier, Standard |
|---|---|--:|--:|--:|--:|
| HTTP/2 | 1 KiB responses | 1 | 32,819 · 11.9 µs | 30,547 · 14.0 µs | 22,919 · 25.5 µs |
| HTTP/2 | 1 KiB responses | 64 | 254,478 · 12.3 µs | 232,382 · 17.5 µs | 138,940 · 30.4 µs |
| HTTP/2 | 1 KiB responses | 256 | 377,736 · 9.3 µs | 292,748 · 14.4 µs | 146,605 · 28.7 µs |
| HTTP/2 + TLS | 1 KiB responses | 256 | 358,991 · 10.7 µs | 277,367 · 15.5 µs | 143,025 · 29.6 µs |
| HTTP/1.1 | 1 KiB responses | 1 | 39,673 · 10.4 µs | 35,021 · 12.7 µs | 31,377 · 15.9 µs |
| HTTP/1.1 | 1 KiB responses | 256 | 154,720 · 22.5 µs | 150,550 · 27.0 µs | 126,267 · 34.1 µs |
| HTTP/1.1 + TLS | 1 KiB responses | 256 | 155,458 · 24.6 µs | 151,832 · 28.5 µs | 117,759 · 37.1 µs |
| HTTP/2 | 1 MiB responses | 8 | 10,733 · 191 µs | 10,586 · 198 µs | 10,496 · 210 µs |
| HTTP/2 | 1 MiB uploads | 16 | 9,790 · 210 µs | 9,688 · 219 µs | 9,710 · 226 µs |

The Minimal pipeline adds about 2 µs of CPU per request one at a time and about 5 µs at high HTTP/2
concurrency, plus 12 allocations (one of them the adapter boxing the response body into fetch's body type).
The Standard pipeline adds 13–19 µs per small HTTP/2 request. On medium and large bodies (64 KiB to 8 MiB,
downloads and uploads) all three are within 3% of each other. The `fetch-matier` and
`fetch-matier-standard` benchmark subjects reproduce these runs; raw results are in
`bench-results/baseline/`.

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
| `matier-fetch` | Matier as the transport handler for Microsoft's `fetch` client. |
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
  timeout. `fetch` has these, and `matier-fetch` runs it over Matier.
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

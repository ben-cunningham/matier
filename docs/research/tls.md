# TLS backend research for a high-throughput tokio HTTP client

Researched 2026-11-14. Scope: client side, HTTP/1.1 and HTTP/2, many connections, small-request and bulk workloads.
Where a claim comes from reading source code, the file and tag are named. Statements marked **(unverified)** come from memory and were not re-checked against a primary source.

## TL;DR recommendation

1. **Default backend: rustls 0.23.x with the aws-lc-rs provider** (rustls's own default), wired in through our own thin adapter and not exposed in the public API. It is first or roughly tied on every client-relevant benchmark in the 2026 ISRG/rustls report. It also has the smallest memory per connection, the only Rust-native vectored-write path that coalesces many buffers into one record, a built-in client resumption cache, X25519MLKEM768 on by default, and an optional FIPS mode. reqwest 0.13 and hyper-rustls 0.27 make the same choice.
2. **Plan for rustls 0.24.** It is currently `0.24.0-dev.3` on `main`. Its new send API encrypts records directly into a caller-owned `&mut Vec<u8>`, which gives the zero-extra-copy write path we want. It also adds a split send/receive mode that suits HTTP/2. Design our trait now so the 0.24 switch stays inside the adapter.
3. **Optional backends, behind cargo features:**
   - `rustls-ring` for builds that cannot use aws-lc. It drops post-quantum and FIPS.
   - `ktls` on Linux for bulk transfers: rustls does the handshake and the kernel does record encryption. Enables `sendfile` and one copy that is fused with encryption.
   - `boring` or `openssl` only if a user needs that exact stack (corporate OpenSSL policy, BoringSSL-specific features). Neither is faster than rustls+aws-lc for a client. Both cost C toolchain pain and lack vectored writes in their tokio wrappers.
   - Skip s2n-tls (rationale below).
4. **Do not use native-tls/hyper-tls on macOS for performance.** It uses Security.framework (SecureTransport), which is deprecated and TLS 1.2-only **(unverified, widely reported)**.

---

## 1. Benchmark data (most recent first)

### rustls.dev / ISRG report, 2026-03-07 (rustls 0.23.37 + aws-lc-rs 1.16.0 vs OpenSSL 3.6.1 vs BoringSSL `30cd935`)
Source: <https://rustls.dev/perf/2026-03-07-report/> (2026-03-07). Write-up: <https://www.memorysafety.org/blog/26q1-rustls-performance/> (2026-04-09).
Hardware: Xeon E-2386G (AVX-512), Debian 12, HT and frequency scaling off. Each test is a single core, in-memory, with no syscalls. OpenSSL and BoringSSL use `openssl-bench`, while rustls uses its own harness.

| Test (per core) | BoringSSL | OpenSSL 3.6.1 | rustls 0.23.37 |
|---|---|---|---|
| TLS1.3 AES-256-GCM **send** MB/s | **7565** | 5844 | 7421 |
| TLS1.3 AES-256-GCM **recv** MB/s | 6218 | 6238 | **7333** |
| TLS1.2 AES-128-GCM send MB/s | **8292** | 6610 | 8134 |
| TLS1.2 AES-128-GCM recv MB/s | 6722 | 7129 | **7947** |
| TLS1.3 full HS, **client**, RSA /s | 3179 | 2220 | **4767** |
| TLS1.3 full HS, **client**, ECDSA /s | 2365 | 1643 | **3160** |
| TLS1.3 full HS, server, RSA /s | 1302 | 1713 | **2357** |
| TLS1.3 **resumed**, client /s | 4648 | 3595 | **6736** |
| TLS1.2 resumed, client /s | 45390 | 21137 | **63870** |

The overall tally in the 2026 write-up is rustls 14 firsts, BoringSSL 2 (both are send throughput, about 2% ahead), and OpenSSL 0. Neither BoringSSL nor OpenSSL changed much since the July 2025 round (<https://rustls.dev/perf/2025-07-31-report/>).
Caveat raised on LWN (<https://lwn.net/Articles/1093391/>, 2026): the bulk numbers mostly measure aws-lc's AES-GCM, so "rustls vs BoringSSL" bulk results are really one assembly implementation against a close relative.

### Multithreaded scaling
Source: <https://rustls.dev/perf/2024-11-28-threading/> (2024-11-28) and <https://www.memorysafety.org/blog/rustls-server-perf/> (2025-05-13). Tests ran on an 80-core Ampere Altra.
- OpenSSL 3.0 has severe lock contention when scaling handshakes across threads. OpenSSL 3.4 is better.
- BoringSSL and rustls scale flat.
- The rustls ticketer mutex was changed to an RwLock in 0.23.17.
- Tail latency ranks rustls < BoringSSL < OpenSSL 3.4 < OpenSSL 3.0.

HAProxy's "State of SSL Stacks" (2025) independently reports large OpenSSL 3.x handshake regressions and recommends AWS-LC (<https://www.haproxy.com/blog/state-of-ssl-stacks>). pyca/cryptography reports the same regressions (<https://cryptography.io/en/latest/statements/state-of-openssl/>).

### Memory per connection
Source: <https://www.memorysafety.org/blog/rustls-performance/> (2024-01-04, rustls 0.22 vs OpenSSL 3.2).
- Peak per session: **rustls ~13 KiB**, **OpenSSL ~69 KiB**.
- At C10K: 132 MiB vs 688 MiB.
- No 2025-2026 re-measurement was found.
- The same post found **jemalloc more than doubles rustls send throughput vs glibc malloc**, because the 0.23 write path allocates per record (see §4).
- Older data from 2019: <https://jbp.io/2019/07/02/rustls-vs-openssl-memory-usage/>.

### Post-quantum handshake cost
Source: <https://rustls.dev/perf/2024-12-17-pq-kx/> (2024-12-17).
- X25519MLKEM768 has a visible CPU cost, mainly from ML-KEM keygen on the client.
- Even so, rustls with X25519MLKEM768 still beats OpenSSL with plain X25519 on every row except client resumption.
- The wire cost matters more in practice: about +1.2 KB in the ClientHello (1184 B encapsulation key) and about +1.1 KB in the ServerHello (1088 B ciphertext). The ClientHello can then exceed one MSS, adding a packet and possibly a round trip on lossy or slow-start-limited paths **(sizes from FIPS 203 parameters)**.
- For a pooled client this is paid once per connection, so it matters only for connection-churn workloads.

---

## 2. Comparison table

| | **rustls + aws-lc-rs** | **rustls + ring** | **OpenSSL 3.x** (`openssl`/`tokio-openssl`) | **BoringSSL** (`boring`/`tokio-boring`) | **s2n-tls** (`s2n-tls-tokio` 0.3.48) | **kTLS** (rustls handshake + kernel records) |
|---|---|---|---|---|---|---|
| Client full HS (TLS1.3) | Best (4.8k RSA / 3.2k ECDSA per core) | Not in 2026 report; RSA/ECDSA in ring are slower than aws-lc **(unverified)** | ~0.45-0.5x rustls | ~0.67-0.75x rustls | No published comparable data | Same as rustls (handshake is userspace) |
| Client resumption (TLS1.3) | Best (6.7k/s); built-in in-memory cache (256 server names) | Same | 0.53x; app must manage `SslSession` itself | 0.69x; app must manage sessions | Supports tickets; app plumbs them | Same as rustls; tickets via `KernelConnection` |
| Bulk send / recv | 7.4 / 7.3 GB/s per core | Lower on AVX-512 machines **(unverified)** | 5.8 / 6.2 | 7.6 / 6.2 | Uses aws-lc crypto; no 3rd-party numbers | Kernel AES-GCM; fused copy+encrypt; `sendfile` capable |
| Memory per conn | ~13 KiB peak (2024) | ~same | ~69 KiB peak (2024) | Lower than OpenSSL **(unverified)** | Not published | Small userspace state; kernel skb memory |
| Vectored writes into one record | **Yes**: `Writer::write_vectored` builds `OutboundChunks` across slices; the fragmenter makes ≤16 KiB records spanning buffers; `tokio-rustls` implements `poll_write_vectored`, `is_write_vectored()==true` | Yes | **No**: `tokio-openssl` has only `poll_write`; every `SSL_write` is ≥1 record | **No** (`tokio-boring` only `poll_write`) | **No** (`poll_send(&[u8])` only in Rust bindings) | `sendmsg` with iovecs: one record per call up to 16 KiB; `MSG_MORE` coalesces across calls |
| Record size control | `ClientConfig::max_fragment_size` | Same | `SSL_CTX_set_max_send_fragment`, split fragment | `SSL_set_max_send_fragment` | `set_dynamic_record_threshold` | `TLS_TX_MAX_PAYLOAD_LEN` (64-16384) |
| ALPN | Yes | Yes | Yes | Yes | Yes | Negotiated by rustls |
| 0-RTT | Client yes (`enable_early_data`; tokio-rustls `early-data` feature) | Yes | Yes (`poll_write_early_data`) | Yes (C API) | Yes | Early data before handoff only |
| PQ X25519MLKEM768 | **On by default** (`prefer-post-quantum` default feature; since ~0.23.22-0.23.27) | **No ML-KEM** | Default keyshare from **3.5** (<https://openssl-library.org/news/openssl-3.5-notes>); 2026-03-13 advisory on `DEFAULT` group negotiation | Always on in `boring` (`pq-experimental` is now a no-op) | Via PQ security policies | Same as rustls |
| FIPS | `fips` feature → aws-lc-fips-sys (needs CMake + Go) | No | FIPS provider (3.x) | `fips` feature (older validated BoringSSL) | Via aws-lc FIPS | Kernel crypto is not part of the FIPS module boundary **(check)** |
| Build burden | C compiler only for aws-lc-sys (prebuilt bindings; **no CMake/Go/bindgen** for non-FIPS: <https://aws.github.io/aws-lc-rs/requirements/index.html>) | Small C/asm (cc crate); easiest cross-compile | System lib or `vendored` (perl, make); cross needs `<TARGET>_OPENSSL_DIR` | CMake + clang/bindgen; symbol clash risk with OpenSSL | C lib + aws-lc | Linux-only; needs `tls` ULP module |
| tokio integration | Excellent (`tokio-rustls` 0.26.x, maintained by the rustls org) | Same | OK (`tokio-openssl`, Rust BIO shim) | OK (`tokio-boring`) | OK but niche | `ktls` 6.0.2 (2025-04-07), `ktls-core`/`ktls-stream` 0.0.x (TLS1.3 rekey) |
| Userspace copies on write path (plaintext → wire) | 0.23 buffered: 1 memcpy + in-place seal into a per-record `Vec`, then `writev` of ≤64 records. 0.24: appended into caller `Vec`, no per-record alloc | Same | 1 (seal out-of-place into `wbuf`) + hyper flattens first (+1) | Same as OpenSSL | Same as OpenSSL | **0 userspace**; kernel encrypts from user pages into skb; `sendfile` none |

---

## 3. Per-library notes

### rustls (0.23.46 current; 0.24.0-dev on main)
- **Providers.** aws-lc-rs is the default and is faster, with AVX-512/VAES paths, ML-KEM and FIPS. ring is smaller and easier to cross-compile but has no PQ or FIPS, and its maintenance has been questioned (Fedora and Arch threads, 2025: <https://lists.fedoraproject.org/archives/list/rust@lists.fedoraproject.org/2025/4/>). Docs: <https://docs.rs/rustls/latest/rustls/>.
- **0.24 status.** Covered by <https://www.infoq.com/news/2026/09/rustls-one-decade/> (2026-09).
  - Providers move out into `rustls-aws-lc-rs` and `rustls-ring` crates, and an explicit provider is required.
  - New `TlsInputBuffer` (in-place decrypt, removes the receive-side copy).
  - Split mode with `Send` send and receive halves.
  - Sequential typed handshake states.
  - It is "due this year" per ISRG. Not released as of this writing (docs.rs shows `0.24.0-dev.*`).
- **Resumption.** On by default: in-memory, 256 server names, TLS1.3 tickets plus TLS1.2 ids/tickets. Share one `Arc<ClientConfig>` across connections, or resumption will not hit.
- **Per-request CPU for small requests.** The record layer is cheap. The 0.23 costs worth watching are one heap allocation per record and an eager socket write per `poll_write` (tokio-rustls flushes inside `poll_write`). Mitigations: hyper's vectored queue strategy (one call per flush) and jemalloc or mimalloc.
- **Receive path (0.23).** Ciphertext goes into the deframer buffer, is decrypted in place, then copied out through `reader()`. That copy is the 7-17% receive gap cited in the 2024 post. 0.24's `TlsInputBuffer` targets it.

### OpenSSL 3.x
- Slowest client handshakes and resumption in the 2026 report. Historic thread-scaling problems in 3.0-3.3. Largest memory footprint.
- Strengths: ubiquity, the OS-policy and FIPS provider story, and PQ by default since 3.5 LTS.
- `tokio-openssl` has no vectored write, so hyper/h1 falls back to `WriteStrategy::Flatten` (copies headers and body into one buffer; `hyper/src/proto/h1/io.rs`), and h2 raises its chain threshold from 256 B to 1 KiB (`h2/src/codec/framed_write.rs`).
- Client session reuse is manual: `SslRef::set_session` plus a new-session callback.

### BoringSSL (`boring` 4.x/5.x, Cloudflare)
- Bulk send is about equal to rustls. Handshakes are 25-35% behind. PQ is always on. FIPS is available.
- Costs: CMake + clang build, cannot co-link with OpenSSL symbols easily, no vectored tokio write. Only worth it if you need BoringSSL-specific knobs.

### s2n-tls (`s2n-tls-tokio` 0.3.48, MSRV 1.91)
- C library on aws-lc. Good security track record, dynamic record sizing, PQ and FIPS policies.
- No independent client benchmarks, no vectored write in the Rust binding, and a smaller ecosystem.
- No advantage over rustls+aws-lc for this use case: same crypto, more C.

### kTLS (Linux)
Kernel docs: <https://docs.kernel.org/networking/tls.html>.
- Setup: `setsockopt(TCP_ULP,"tls")`, then `SOL_TLS` with `TLS_TX`/`TLS_RX`.
- Send: `send()` encrypts directly from user pages. One record per `send` unless `MSG_MORE` is set. `sendfile`/`splice` produce 16 KiB records. `TLS_TX_ZEROCOPY_RO` applies only with NIC offload.
- Receive: decrypts into the user buffer when it is large enough (TLS1.3 `TLS_RX_EXPECT_NO_PAD` for trusted peers). Non-data records come back via cmsg.
- Key updates: TLS1.3 KeyUpdate pauses RX until userspace installs the new key. Per ktls-core docs, rekey needs **Linux ≥ 6.13**.
- rustls integration: `rustls::kernel` (`dangerous_into_kernel_connection` on an unbuffered connection after the handshake; requires `enable_secret_extraction`) gives a `KernelConnection` that only handles key updates and session tickets (<https://docs.rs/rustls/latest/rustls/kernel/index.html>). The legacy route is `dangerous_extract_secrets()` plus the `ktls` crate (6.0.2, 2025-04-07; owners include ctz and fasterthanlime; <https://docs.rs/crate/ktls/latest>). The `ktls2` fork adds Monoio.
- When it wins: bulk uploads from files (`sendfile`), very large bodies, and io_uring send paths.
- When it does not: small requests (syscall per record, cmsg handling) and portability. HTTP/2 control frames are fine (they are app data).
- Risks: confidentiality-limit tracking becomes the caller's job, and the kernel module may be missing in containers.

### io_uring
- tokio's reactor is epoll. The io_uring runtimes are tokio-uring (stagnant), monoio and compio. compio was adopted by Apache Iggy in Feb 2026 (<https://dev.to/sospeter/how-apache-iggy-ditched-tokio-for-thread-per-core-iouring-and-what-it-cost-them-3m75>).
- io_uring needs **owned buffers**. That fits a sans-IO TLS API that encrypts into an owned `Vec<u8>` / `BytesMut` (rustls unbuffered or 0.24 send API) and does not fit `std::io::Write`-driven TLS.
- kTLS plus io_uring (`IORING_OP_SEND`, `SPLICE`) moves both crypto and I/O out of the hot user path.
- Recommendation: keep io_uring out of v1, but keep the TLS layer sans-IO so it remains possible.

### reqwest / hyper defaults
- **reqwest 0.13** (2025-12-30): default TLS is **rustls with aws-lc-rs** plus `rustls-platform-verifier`. `rustls-no-provider` lets you bring ring. native-tls is opt-in. Source: <https://seanmonstar.com/blog/reqwest-v013-rustls-default/>, <https://newreleases.io/project/cargo/reqwest/release/0.13.0>. A hyper survey found 93% of users already on rustls.
- **hyper-rustls 0.27.10** default features: `native-tokio, http1, tls12, logging, aws-lc-rs` (raw Cargo.toml on GitHub).
- **hyper-tls** uses native-tls: OpenSSL on Linux, Security.framework on macOS, SChannel on Windows.

### macOS vs Linux
- **Linux:** everything is available, including kTLS, io_uring, `sendfile`, and AVX-512 aws-lc paths on server CPUs.
- **macOS:**
  - No kTLS or io_uring (kqueue only).
  - native-tls/SecureTransport is deprecated and not a performance option.
  - rustls+aws-lc-rs builds out of the box (ARMv8 crypto extensions on Apple Silicon).
  - `rustls-platform-verifier` calls `SecTrustEvaluateWithError`. That can add per-handshake latency and may do network revocation checks **(unverified magnitude)**.
  - For handshake-heavy benchmarks, use `webpki-roots` with `WebPkiServerVerifier`, or make the verifier configurable.
  - Benchmark on Linux for publication. Treat macOS as a dev target.

---

## 4. Write-path copy analysis (from source)

**rustls 0.23 buffered (tokio-rustls).** Read from `rustls/src/conn.rs`, `common_state.rs`, `vecbuf.rs` and `crypto/aws_lc_rs/tls13.rs` @ `v/0.23.35`, and `tokio-rustls/src/common/mod.rs` @ main.
```
hyper IoSlices ─write_vectored→ OutboundChunks (no copy, borrows slices)
  → MessageFragmenter: ≤16 KiB records spanning slices
  → per record: PrefixedPayload::with_capacity (ALLOC) + extend_from_chunks (COPY #1) + seal_in_place (encrypt pass)
  → push to sendable_tls ChunkVecBuffer
  → write_tls → write_vectored(up to 64 records) → kernel (COPY #2, kernel)
```

**rustls 0.23 unbuffered (`WriteTraffic::encrypt(&[u8], &mut [u8])`).** `write_fragments` still calls `encrypt_outgoing` (same per-record alloc and copy) and then **`copy_from_slice` into `outgoing_tls`**. In 0.23 it therefore costs **one more memcpy** than the buffered path. Its benefits are caller-owned output memory (good for io_uring), no `std::io`, no_std, and the `kernel` handoff, not fewer copies. It also takes a single `&[u8]`, so it has no vectored input.

**rustls 0.24-dev.** `RecordEncrypter::encrypt(record, seq, out: &mut Vec<u8>)` appends the finished wire record to the caller's Vec. `Connection::write(OutboundPlain, tls: &mut Vec<u8>)` (`rustls/src/conn/mod.rs` @ main). This gives a single pass from plaintext chunks into our reusable output buffer, with no per-record allocation. That is the target design.

**OpenSSL/BoringSSL.** `SSL_write` seals out-of-place into the library's internal `wbuf` (one pass), then calls the BIO, which calls tokio `poll_write`. There is no vectored entry, so hyper flattens first, which adds one copy for headers+body on h1.

**kTLS.** `sendmsg(iov)` makes the kernel encrypt from user pages into skbs (one fused copy+encrypt). `sendfile` has no userspace copy at all.

---

## 5. Recommended architecture

### 5.1 Two-level TLS abstraction
Keep the hot path statically dispatched. Expose a stream-level trait for simplicity and a sans-IO session trait for the optimized path.

```rust
/// Stream level: what the connection pool sees. One impl per backend, chosen by a cargo feature.
pub trait TlsConnector: Clone + Send + Sync + 'static {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + TlsInfo + 'static;
    fn connect(&self, server: &ServerName<'static>, tcp: TcpStream, alpn: &[&[u8]])
        -> impl Future<Output = io::Result<Self::Stream>> + Send;
}
pub trait TlsInfo {
    fn alpn(&self) -> Option<&[u8]>;          // "h2" vs "http/1.1" pool routing
    fn resumed(&self) -> bool;                // metrics
    fn negotiated_group(&self) -> Option<&'static str>; // PQ visibility
}

/// Sans-IO level: for our own I/O driver (and the io_uring/kTLS paths later).
pub trait TlsSession: Send {
    /// Feed ciphertext; decrypt in place; yields plaintext ranges inside `buf`.
    fn read_tls(&mut self, buf: &mut BytesMut) -> Result<ReadOutcome, TlsError>;
    /// Encrypt a gather list of plaintext directly onto the end of `out`.
    /// Returns bytes of plaintext consumed. No internal queueing.
    fn encrypt_into(&mut self, plain: &[&[u8]], out: &mut Vec<u8>) -> Result<usize, TlsError>;
    fn record_overhead(&self) -> usize;       // for pre-reserving `out`
    fn max_fragment(&self) -> usize;
    fn into_kernel(self: Box<Self>) -> Option<KernelHandoff>; // Linux kTLS
}
```
- Select the backend with cargo features (`tls-rustls-aws-lc` as default, `tls-rustls-ring`, `tls-boring`, `tls-openssl`, `ktls`), plus a `enum AnyConnector` for runtime choice. Avoid `dyn` per I/O call.
- rustls 0.23 adapter: implement `TlsConnector` over `tokio-rustls`. Implement `TlsSession` over `UnbufferedClientConnection`, accepting the extra memcpy until 0.24.
- rustls 0.24 adapter: implement `encrypt_into` natively. Use split mode to give HTTP/2 separate read and write tasks without a mutex.
- OpenSSL/Boring adapters: implement only `TlsConnector`. Those backends cannot give us `encrypt_into` without memory-BIO hacks, and that work is not worth it.

### 5.2 Write path
1. **One reusable per-connection output buffer.** Use a `Vec<u8>` or `BytesMut`, say 64 KiB, grown on demand and shrunk when idle. Encrypt into it, send with a single `write`/`send`, then `clear()`. This removes rustls 0.23's per-record allocations once on 0.24, and is allocator-independent.
2. **Gather, do not concatenate.** Build HTTP/1 heads in a small scratch buffer, keep body chunks as `Bytes`, and pass all of them as slices to `encrypt_into` / `write_vectored`. TLS merges them into full records, so the TLS layer does the coalescing and there is no separate flatten copy.
3. **Cork per poll.** Accumulate everything produced in one task wakeup (many small h2 frames, pipelined h1 requests), then encrypt and flush once. That means one syscall and the fewest records. With tokio-rustls, achieve this by giving hyper a vectored stream (the Queue strategy) and not calling `poll_write` per frame.
4. **Record sizing.**
   - Small requests: send whatever is pending as one record. Do not pad or split.
   - Bulk: use 16 KiB records (the default `max_fragment_size = None`). Optionally start with ~1-4 KiB records for the first ~64 KiB after idle, if time-to-first-byte matters (s2n-style dynamic record sizing), though this matters more for servers.
5. **Bulk upload of files on Linux.** Optionally hand off to kTLS after the handshake and use `sendfile`/`splice` (no userspace copy). Gate this on kernel ≥ 6.13 if we need TLS1.3 KeyUpdate handling, and fall back automatically on `ENOENT`/`EOPNOTSUPP` from `TCP_ULP`.
6. **Receive.** Read into a reusable `BytesMut`, decrypt in place (rustls unbuffered/0.24 `TlsInputBuffer`), and hand body bytes upward as `Bytes` slices of that buffer (`split_to` + `freeze`), so there is no copy to the user.

### 5.3 Config defaults
- One shared `Arc<ClientConfig>` per client, so resumption works.
- ALPN `["h2","http/1.1"]`.
- `prefer-post-quantum` on, with an option to disable it for handshake-churn benchmarks.
- `enable_early_data = false` by default. If enabled, allow 0-RTT only for idempotent requests.
- Use mimalloc/jemalloc in benches. The jemalloc 2x send effect on rustls 0.23 is documented.
- Verifier: platform verifier for correctness by default, and a `webpki-roots` option for benchmarks and containers.

---

## 6. Sources
- rustls 2026 report: <https://rustls.dev/perf/2026-03-07-report/> (2026-03-07). ISRG summary: <https://www.memorysafety.org/blog/26q1-rustls-performance/> (2026-04-09)
- rustls perf index: <https://rustls.dev/perf/>; 2025-07-31 report; PQ 2024-12-17: <https://rustls.dev/perf/2024-12-17-pq-kx/>; threading 2024-11-28: <https://rustls.dev/perf/2024-11-28-threading/>
- ISRG server perf (2025-05-13): <https://www.memorysafety.org/blog/rustls-server-perf/>; memory/jemalloc (2024-01-04): <https://www.memorysafety.org/blog/rustls-performance/>
- InfoQ "rustls one decade" (2026-09): <https://www.infoq.com/news/2026/09/rustls-one-decade/>; LWN discussion: <https://lwn.net/Articles/1093391/>
- rustls docs 0.23.46: <https://docs.rs/rustls/latest/rustls/>, unbuffered: <https://docs.rs/rustls/latest/rustls/unbuffered/index.html>, `WriteTraffic`: <https://docs.rs/rustls/latest/rustls/unbuffered/struct.WriteTraffic.html>, kernel: <https://docs.rs/rustls/latest/rustls/kernel/index.html>
- Source read (raw.githubusercontent.com): rustls `v/0.23.35` `rustls/src/{conn.rs,common_state.rs,vecbuf.rs,crypto/aws_lc_rs/tls13.rs}`; rustls `main` (0.24.0-dev.3) `rustls/src/{conn/mod.rs,conn/send.rs,crypto/cipher/mod.rs}`; tokio-rustls `main` `src/common/mod.rs` (0.26.6); tokio-openssl `src/lib.rs`; cloudflare/boring `tokio-boring/src/lib.rs`, `boring/Cargo.toml`; aws/s2n-tls `bindings/rust/extended/s2n-tls-tokio/{Cargo.toml,src/lib.rs}`; hyper `src/proto/h1/io.rs`; h2 `src/codec/framed_write.rs`; hyper-rustls `Cargo.toml` (0.27.10)
- reqwest 0.13 (2025-12-30): <https://seanmonstar.com/blog/reqwest-v013-rustls-default/>, <https://newreleases.io/project/cargo/reqwest/release/0.13.0>
- OpenSSL 3.5 notes (PQ default keyshares): <https://openssl-library.org/news/openssl-3.5-notes>; 2026-03-13 advisory: <https://groups.google.com/a/openssl.org/g/openssl-announce/c/xA3bhNwBTL0>
- HAProxy State of SSL Stacks (2025): <https://www.haproxy.com/blog/state-of-ssl-stacks>; pyca on OpenSSL 3: <https://cryptography.io/en/latest/statements/state-of-openssl/>
- aws-lc-rs build requirements: <https://aws.github.io/aws-lc-rs/requirements/index.html>
- Linux kTLS: <https://docs.kernel.org/networking/tls.html>; ktls crate: <https://docs.rs/crate/ktls/latest>; ktls-core (rekey needs ≥6.13): <https://docs.rs/ktls-core/latest/src/ktls_core/context.rs.html>
- s2n-tls-tokio: <https://docs.rs/s2n-tls-tokio>
- Apache Iggy → compio/io_uring (2026-02): <https://dev.to/sospeter/how-apache-iggy-ditched-tokio-for-thread-per-core-iouring-and-what-it-cost-them-3m75>

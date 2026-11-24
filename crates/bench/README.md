# matier-bench

Reproducible, machine-readable HTTP client benchmarks. Subjects: `matier`, `hyper-legacy`
(one hyper-util client), `hyper-pooled` (N hyper-util clients), `reqwest`, `fetch` and `fetch-pooled`
(Microsoft's fetch, standard and pooled modes). Each implements the `Subject` trait.

## Run

```sh
scripts/bench.sh bench/quick.toml quick      # ~45s, for iteration loops
scripts/bench.sh bench/matrix.toml mybranch  # ~7 min, full matrix
# -> bench-results/<name>.json (JSON array) + markdown table on stdout
```

Windows (PowerShell):

```powershell
.\scripts\bench.ps1 bench\quick.toml quick-windows
.\scripts\bench.ps1 bench\report.toml report-windows
.\target\release\matier-bench.exe compare `
    bench-results\baseline\report-windows-2026-11-16.json bench-results\report-windows.json --threshold 5
```

`bench/report.toml` is shared by the macOS and Windows reports: 16 workloads across five
clients (80 runs), including HTTP/1.1, HTTP/2, TLS, downloads and uploads. Keep separate
baselines per OS and host; `compare` matches scenario IDs, not hardware or OS, so only
compare measurements from the same host and worker configuration.

`scripts/bench.sh` (Unix) and `scripts/bench.ps1` (Windows) are thin wrappers: they build
release binaries and run `matier-bench matrix --spawn-server`, which starts
`matier-bench-server` as a child (found next to the `matier-bench` executable, or
`--server-bin PATH`), writes its self-signed cert to the matrix file's `ca_cert`
(default `target/bench-server-cert.pem`), waits for `/health`, and kills it on exit and on
Ctrl-C. Without `--spawn-server` the matrix assumes a server is already running.

Manual single run:

```sh
target/release/matier-bench-server --cert-out target/bench-server-cert.pem &
target/release/matier-bench run --url http://127.0.0.1:8080 --subject hyper-legacy \
    --protocol h2 --load closed --concurrency 256 --warmup 2s --duration 10s \
    --path /bytes/1024 --out result.json
# TLS: --url https://127.0.0.1:8443 --ca-cert target/bench-server-cert.pem
# open loop: --load open --rate 50000 --max-inflight 4096
# POST: --method POST --path /echo --req-body-size 65536
```

Compare two runs (exit code 1 on regression, for CI / AI loops):

```sh
target/release/matier-bench compare bench-results/baseline/quick-<date>.json bench-results/quick.json --threshold 5
```

Scenarios are matched by `scenario_id` = subject(+pool) / protocol / tls / load model +
level / method path / request size. Compared: req/s, p50, p99, cpu us/req, allocs/req.

## Subjects

`hyper-legacy`, `hyper-pooled` (N hyper clients round-robin, `--pool-size`), `reqwest`,
and Microsoft oxidizer `fetch` 0.21: `fetch` (Minimal pipeline: Dispatch -> hyper transport
only, for a fair transport comparison), `fetch-standard` (default Standard pipeline with
timeouts/retry/breaker/metrics/logging, to show layer overhead) and `fetch-pooled` (Minimal
pipeline, `multiple_pools = (pool-size, --pool-selection round-robin|saturating)`).
`fetch` cannot set the h2 connection window or max frame size (only the 1 MiB stream window
is matched), and uses `insecure_allow_http` for plaintext runs.

## Server endpoints

`GET /bytes/{n}`, `POST /echo` (returns body length; `?mirror=1` streams the body back),
`GET /delay/{ms}`, `GET /health`. Flags: `--addr`, `--tls-addr` (`off` disables),
`--threads`, `--h2-max-concurrent-streams`, `--h2-stream-window`, `--h2-conn-window`,
`--cert-out`. The server runs in its own process so client CPU/alloc numbers are clean.

## Metrics

- **req/s, bytes/s**: successful requests (or response/request body bytes) in the measured
  window divided by the window length. Warmup is excluded.
- **latency** (`latency`): closed loop = send -> body fully read. Open loop = from the
  *intended* send time to completion.
- **service_time**: actual send -> body fully read. **schedule_lag** (open loop only):
  intended -> actual send.
- **cpu us/req**: user+sys CPU of the client process (all threads, `getrusage` on Unix,
  `GetProcessTimes` on Windows) during the window per request. `cpu_cores` is the average
  number of busy cores.
- **allocs/req, alloc bytes/req**: from the counting global allocator (feature
  `count-allocs`, on by default). It adds relaxed-atomic overhead on every allocation
  (a few ns, more under cross-core contention); build with `--no-default-features
  --features hyper-legacy,hyper-pooled,reqwest` for pure throughput numbers. Counts include
  the driver itself (open loop spawns a task per request and sends one channel sample).
- **peak RSS**: `ru_maxrss` (bytes on macOS, KiB on Linux, normalised to bytes). It is a
  process-lifetime high-water mark, so it includes warmup and earlier matrix runs.
  Windows reports peak working set via `GetProcessMemoryInfo`.
- Errors are counted by kind (`timeout`, `connect`, `connection_closed`, `status_4xx`,
  `status_5xx`, `other`); open-loop arrivals rejected at `--max-inflight` are `drops`.

## Coordinated omission

A closed loop slows down when the system under test stalls, hiding the stall (the "omitted"
requests never get sent). The open-loop driver instead issues requests on a fixed schedule
and measures latency from when each request *should* have been sent, so queueing during a
stall is charged to the tail. Timer resolution is ~1 ms: the scheduler wakes up and sends
everything that has become due, so expect up to ~1 ms of schedule lag at moderate rates even
when nothing is wrong; compare `service_time` for the pure client+server cost.

## Adding a subject

1. Create `src/subject/<name>.rs` implementing `Subject` (`name`, `request`); `request` must
   read the entire response body, counting bytes, and return `BenchResponse`.
2. Add a cargo feature, a variant in `AnySubject`, a match arm in `build_subject`, `available`
   and the `Subject for AnySubject` impl (a commented `matier` placeholder marks the spots).
3. Use it with `--subject <name>` or in a matrix `subjects = [...]`.

## Caveats

- Windows (x86_64-pc-windows-msvc): CPU time comes from `GetProcessTimes` and peak RSS from
  `GetProcessMemoryInfo` (peak working set). HTTP/1.1 with high concurrency churns
  connections: watch for ephemeral port exhaustion / `TIME_WAIT` build-up (errors of kind
  `connect`); keep-alive reuse should avoid it, but a failing subject can trigger it.
  Windows loopback has a different fast path than Linux/macOS, and Defender real-time
  scanning can skew results: exclude the repo, `target\` and the binaries from scanning when
  benchmarking. Building needs the MSVC toolchain; AWS-LC/NASM/cmake are not required.

- Loopback only measures client + kernel + server CPU; there is no real network. Absolute
  numbers are not comparable across machines; compare runs on the same host.
- macOS vs Linux: different schedulers, loopback and timer behaviour; keep baselines per OS.
- Client runtime defaults to half the CPUs (`--worker-threads`) so the server (also half by
  default) has its own cores. Check the server is not the bottleneck: watch its CPU
  (`top -pid $(pgrep matier-bench-server)`); if it sits near `threads x 100%`, give it more
  threads or use a faster/external server.
- `--url` can point at an external server (Go `net/http`, h2o, ...) as long as it serves
  `/bytes/{n}`; the probe request must succeed before measuring starts.
- HTTP/2 uses identical tuning for all subjects: 1 MiB stream window, 16 MiB connection
  window, 256 KiB max frame size, no adaptive window. A single h2 client uses one
  connection; `hyper-pooled` (N clients, round-robin) mimics oxidizer `fetch`'s
  `multiple_pools` workaround.
- Matrix files: see `bench/matrix.toml`; each `[[group]]` is a cartesian product.

# Windows benchmark baseline

Native Windows run on November 16, 2026, using the same `bench/report.toml` matrix as the
macOS report: 16 workloads across five clients, for 80 measured runs.
This measures Matier, hyper and fetch on Windows, not the WinHTTP client API.

## Environment and method

- Windows 11 Enterprise, build 26300, x86_64.
- AMD EPYC 7763; 16 logical processors exposed to the process; 63.95 GiB RAM.
- Rust 1.97.1 (`f4154c5f17`), MSVC target, release profile, default Cargo features
  (including allocation counting).
- Source commit: `c7f6aa23c6569a99bc1225ef85ddb264fd286bf9`. No transport tuning changes.
- Client and hyper-based server on IPv4 loopback, in separate processes, with eight
  Tokio workers each. Matier uses its default autoscaling; pooled competitors use four clients.
- Per scenario: one second warmup, four seconds measurement, 500 ms cooldown.
- CPU is client-process user plus kernel time from `GetProcessTimes`; memory is peak
  working set from `GetProcessMemoryInfo`, including earlier scenarios and warmup.

The host is shared, not an isolated performance lab. Measurement started after an observed
competing compiler finished. A ten-second preflight sample measured 0.513 busy logical
cores across accessible processes. Background CPU was also sampled every five seconds
during the matrix; this does not detect all sources of interference or hypervisor contention.
Across 93 samples, other processes averaged 0.653 cores, peaking at 1.812; Copilot was the
largest consumer in the three samples above one core. No compiler or linker was the top
background consumer in any sample. No OS, power, affinity or antivirus settings were changed.

This is an initial single-run baseline, not a statistically established ranking. Compare
changes on the same host with the same worker count, and repeat close results. Do not
interpret differences from the Apple Silicon report as an operating-system comparison.
The benchmark includes both client and server effects; it does not isolate socket calls.

## Reproduce

From the repository root in PowerShell:

```powershell
.\scripts\bench.ps1 bench\report.toml report-windows
.\target\release\matier-bench.exe compare `
    bench-results\baseline\report-windows-2026-11-16.json bench-results\report-windows.json --threshold 5
```

The raw measurements, including latency distributions, CPU, allocations, errors, timestamps
and scenario settings, are in `baseline/report-windows-2026-11-16.json`.

## Results

All 80 scenarios completed: 8,860,898 successful measured requests, zero errors and zero drops.
Result timestamps span 19:34:06 to 19:41:29 UTC on November 16, 2026.

Requests per second; higher is better. `c` is the number of requests in flight. GET sizes
are response sizes; POST sizes are upload sizes. Pooled subjects use four clients.

| Protocol | Workload | c | Matier | hyper | hyper x4 | fetch | fetch x4 |
|---|---|--:|--:|--:|--:|--:|--:|
| HTTP/1.1 | 1 KiB responses | 1 | 7,900 | 7,279 | 5,751 | 6,282 | 4,923 |
| HTTP/1.1 | 1 KiB responses | 64 | 53,810 | 51,364 | 49,190 | 45,024 | 45,451 |
| HTTP/1.1 | 1 KiB responses | 256 | 48,704 | 48,443 | 48,011 | 44,452 | 44,568 |
| HTTP/1.1 | 64 KiB responses | 64 | 42,946 | 41,799 | 40,774 | 39,628 | 39,932 |
| HTTP/1.1 | 1 MiB responses | 8 | 6,905 | 6,886 | 7,209 | 7,180 | 6,842 |
| HTTP/1.1 | 8 MiB responses | 8 | 766 | 781 | 820 | 828 | 784 |
| HTTP/1.1 | 1 MiB uploads | 16 | 8,978 | 9,552 | 9,338 | 9,450 | 8,977 |
| HTTP/1.1 + TLS | 1 KiB responses | 256 | 56,694 | 48,762 | 48,272 | 44,746 | 44,963 |
| HTTP/2 | 1 KiB responses | 1 | 6,724 | 4,970 | 4,945 | 4,272 | 4,343 |
| HTTP/2 | 1 KiB responses | 64 | 104,091 | 31,159 | 60,670 | 20,762 | 49,994 |
| HTTP/2 | 1 KiB responses | 256 | 148,322 | 35,122 | 70,262 | 24,760 | 64,225 |
| HTTP/2 | 64 KiB responses | 64 | 73,513 | 4,230 | 37,858 | 8,169 | 18,742 |
| HTTP/2 | 1 MiB responses | 8 | 2,808 | 364 | 1,748 | 778 | 1,908 |
| HTTP/2 | 8 MiB responses | 8 | 230 | 99 | 245 | 97 | 262 |
| HTTP/2 | 1 MiB uploads | 16 | 5,778 | 452 | 1,488 | 506 | 1,494 |
| HTTP/2 + TLS | 1 KiB responses | 256 | 152,432 | 37,853 | 77,756 | 29,866 | 76,964 |

Selected latency and efficiency measurements against hyper x4. Each cell is Matier / hyper x4.

| HTTP/2 workload | c | p99 latency (ms) | CPU (us/request) | Allocations/request |
|---|--:|--:|--:|--:|
| 1 KiB responses | 256 | 3.090 / 11.370 | 28.9 / 83.0 | 3.0 / 14.2 |
| 1 KiB responses + TLS | 256 | 3.086 / 7.852 | 30.4 / 73.8 | 4.4 / 15.8 |
| 64 KiB responses | 64 | 1.978 / 5.632 | 79.0 / 143.9 | 3.0 / 17.0 |
| 8 MiB responses | 8 | 110.559 / 124.912 | 3,196.4 / 3,539.5 | 4.1 / 110.0 |

In this run, Matier delivered about 2.11 times hyper x4's throughput for HTTP/2 1 KiB
responses at c=256, and 1.96 times with TLS. It was not ahead everywhere: HTTP/2 8 MiB
downloads reached 230 req/s versus 245 for hyper x4 and 262 for fetch x4. These are
priorities for repeated measurements and profiling, not evidence that a particular
Windows API or buffering change would improve performance.

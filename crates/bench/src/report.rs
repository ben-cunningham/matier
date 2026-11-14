//! Machine-readable run results and human-readable tables.

use std::{
    fmt::Write as _,
    time::{SystemTime, UNIX_EPOCH},
};

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

use crate::{alloc_counter, driver::DriverOutput, metrics, scenario::ScenarioConfig};

/// Bump when the JSON layout changes incompatibly.
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HostInfo {
    pub os: String,
    pub arch: String,
    pub cpus: usize,
}

/// Latency distribution summary, all values in microseconds.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Percentiles {
    pub p50_us: f64,
    pub p90_us: f64,
    pub p99_us: f64,
    pub p99_9_us: f64,
    pub p99_99_us: f64,
    pub max_us: f64,
    pub mean_us: f64,
}

impl Percentiles {
    pub fn from_hist(h: &Histogram<u64>) -> Self {
        if h.is_empty() {
            return Self::default();
        }
        let q = |q: f64| h.value_at_quantile(q) as f64 / 1e3;
        Self {
            p50_us: q(0.5),
            p90_us: q(0.9),
            p99_us: q(0.99),
            p99_9_us: q(0.999),
            p99_99_us: q(0.9999),
            max_us: h.max() as f64 / 1e3,
            mean_us: h.mean() / 1e3,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Totals {
    /// Successful requests in the measured window.
    pub requests: u64,
    pub errors: u64,
    pub errors_by_kind: std::collections::BTreeMap<String, u64>,
    /// Open-loop arrivals dropped at the max-inflight limit.
    pub drops: u64,
    pub duration_secs: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Throughput {
    pub req_per_sec: f64,
    pub resp_bytes_per_sec: f64,
    pub req_bytes_per_sec: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resources {
    pub cpu_user_secs: f64,
    pub cpu_sys_secs: f64,
    /// Average CPU cores used by the client process during the window.
    pub cpu_cores: f64,
    pub cpu_us_per_req: f64,
    /// Process lifetime high-water mark (includes warmup and earlier runs).
    pub peak_rss_bytes: u64,
    /// False if the counting allocator was not installed (alloc fields are then 0).
    pub alloc_counting: bool,
    pub allocs_per_req: f64,
    pub alloc_bytes_per_req: f64,
    pub deallocs_per_req: f64,
}

/// One measured scenario, as written to JSON.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunResult {
    pub schema_version: u32,
    /// RFC 3339 UTC.
    pub timestamp: String,
    pub git_commit: Option<String>,
    pub host: HostInfo,
    pub scenario_id: String,
    pub subject: String,
    pub scenario: ScenarioConfig,
    pub totals: Totals,
    pub throughput: Throughput,
    /// Total latency (from intended send time in open loop).
    pub latency: Percentiles,
    pub service_time: Percentiles,
    pub schedule_lag: Option<Percentiles>,
    pub resources: Resources,
}

impl RunResult {
    pub fn new(cfg: &ScenarioConfig, subject: &str, out: &DriverOutput) -> Self {
        let secs = out.window.as_secs_f64();
        let n = out.ok.max(1) as f64;
        let cpu = (out.end.user + out.end.sys).saturating_sub(out.start.user + out.start.sys);
        let a = (out.end.alloc, out.start.alloc);
        let errors: u64 = out.errors.values().sum();
        Self {
            schema_version: SCHEMA_VERSION,
            timestamp: rfc3339_now(),
            git_commit: git_commit(),
            host: HostInfo {
                os: std::env::consts::OS.into(),
                arch: std::env::consts::ARCH.into(),
                cpus: std::thread::available_parallelism().map_or(1, |n| n.get()),
            },
            scenario_id: cfg.id(),
            subject: subject.to_owned(),
            scenario: cfg.clone(),
            totals: Totals {
                requests: out.ok,
                errors,
                errors_by_kind: out.errors.clone(),
                drops: out.drops,
                duration_secs: secs,
            },
            throughput: Throughput {
                req_per_sec: out.ok as f64 / secs,
                resp_bytes_per_sec: out.resp_bytes as f64 / secs,
                req_bytes_per_sec: out.req_bytes as f64 / secs,
            },
            latency: Percentiles::from_hist(&out.total),
            service_time: Percentiles::from_hist(&out.service),
            schedule_lag: out.lag.as_ref().map(Percentiles::from_hist),
            resources: Resources {
                cpu_user_secs: (out.end.user.saturating_sub(out.start.user)).as_secs_f64(),
                cpu_sys_secs: (out.end.sys.saturating_sub(out.start.sys)).as_secs_f64(),
                cpu_cores: cpu.as_secs_f64() / secs,
                cpu_us_per_req: cpu.as_secs_f64() * 1e6 / n,
                peak_rss_bytes: metrics::peak_rss_bytes(),
                alloc_counting: alloc_counter::enabled(),
                allocs_per_req: a.0.allocs.saturating_sub(a.1.allocs) as f64 / n,
                alloc_bytes_per_req: a.0.bytes.saturating_sub(a.1.bytes) as f64 / n,
                deallocs_per_req: a.0.deallocs.saturating_sub(a.1.deallocs) as f64 / n,
            },
        }
    }
}

/// Commit hash from `MATIER_GIT_COMMIT` or `git rev-parse HEAD`; `None` if unavailable.
fn git_commit() -> Option<String> {
    if let Ok(c) = std::env::var("MATIER_GIT_COMMIT") {
        return Some(c);
    }
    let out = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

fn rfc3339_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs()) as i64;
    let (days, rem) = (secs.div_euclid(86400), secs.rem_euclid(86400));
    // Civil-from-days (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + (m <= 2) as i64;
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// Compact markdown table of results.
pub fn markdown_table(results: &[RunResult]) -> String {
    let mut s = String::from(
        "| scenario | req/s | p50 us | p99 us | p99.9 us | errs | drops | cpu us/req | allocs/req | rss MB |\n\
         |---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n",
    );
    for r in results {
        let _ = writeln!(
            s,
            "| {} | {:.0} | {:.0} | {:.0} | {:.0} | {} | {} | {:.1} | {:.1} | {:.0} |",
            r.scenario_id,
            r.throughput.req_per_sec,
            r.latency.p50_us,
            r.latency.p99_us,
            r.latency.p99_9_us,
            r.totals.errors,
            r.totals.drops,
            r.resources.cpu_us_per_req,
            r.resources.allocs_per_req,
            r.resources.peak_rss_bytes as f64 / 1048576.0,
        );
    }
    s
}

/// Reads a result file holding either one `RunResult` or an array of them.
pub fn read_results(path: &std::path::Path) -> anyhow::Result<Vec<RunResult>> {
    let text = std::fs::read_to_string(path)?;
    let v: serde_json::Value = serde_json::from_str(&text)?;
    Ok(if v.is_array() {
        serde_json::from_value(v)?
    } else {
        vec![serde_json::from_value(v)?]
    })
}

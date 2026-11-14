//! Compares two result files and flags regressions.

use std::{collections::BTreeMap, fmt::Write as _, path::Path};

use anyhow::Result;

use crate::report::{RunResult, read_results};

struct Metric {
    name: &'static str,
    get: fn(&RunResult) -> f64,
    higher_is_better: bool,
}

const METRICS: [Metric; 5] = [
    Metric {
        name: "req/s",
        get: |r| r.throughput.req_per_sec,
        higher_is_better: true,
    },
    Metric {
        name: "p50 us",
        get: |r| r.latency.p50_us,
        higher_is_better: false,
    },
    Metric {
        name: "p99 us",
        get: |r| r.latency.p99_us,
        higher_is_better: false,
    },
    Metric {
        name: "cpu us/req",
        get: |r| r.resources.cpu_us_per_req,
        higher_is_better: false,
    },
    Metric {
        name: "allocs/req",
        get: |r| r.resources.allocs_per_req,
        higher_is_better: false,
    },
];

/// Prints a delta table for scenarios present in both files. Returns true if any metric
/// regressed by more than `threshold` percent.
pub fn compare(base: &Path, new: &Path, threshold: f64) -> Result<bool> {
    let base: BTreeMap<_, _> = read_results(base)?
        .into_iter()
        .map(|r| (r.scenario_id.clone(), r))
        .collect();
    let new = read_results(new)?;
    let mut out = String::from(
        "| scenario | metric | base | new | delta | verdict |\n|---|---|---:|---:|---:|---|\n",
    );
    let (mut regressions, mut improvements, mut matched) = (0, 0, 0);
    let mut missing = vec![];
    for n in &new {
        let Some(b) = base.get(&n.scenario_id) else {
            missing.push(n.scenario_id.as_str());
            continue;
        };
        matched += 1;
        for m in &METRICS {
            let (bv, nv) = ((m.get)(b), (m.get)(n));
            if bv <= 0.0 {
                continue; // metric unavailable in the baseline (e.g. allocs not counted)
            }
            let delta = (nv - bv) / bv * 100.0;
            let worse = if m.higher_is_better { -delta } else { delta };
            let verdict = if worse > threshold {
                regressions += 1;
                "REGRESSION"
            } else if worse < -threshold {
                improvements += 1;
                "improved"
            } else {
                ""
            };
            let _ = writeln!(
                out,
                "| {} | {} | {bv:.1} | {nv:.1} | {delta:+.1}% | {verdict} |",
                n.scenario_id, m.name
            );
        }
    }
    print!("{out}");
    for id in missing {
        println!("not in base: {id}");
    }
    println!(
        "\n{matched} scenarios compared, {regressions} regressions, {improvements} improvements (threshold {threshold}%)"
    );
    Ok(regressions > 0)
}

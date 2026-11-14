//! `matier-bench` CLI: `run`, `matrix`, `compare`.

use std::{path::PathBuf, process::ExitCode, time::Duration};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use matier_bench::{
    compare,
    matrix::MatrixConfig,
    report::{RunResult, markdown_table},
    runner::run_scenario,
    scenario::{
        LoadSpec, PoolSelection, Protocol, ScenarioConfig, default_worker_threads, parse_duration,
    },
};

#[cfg(feature = "count-allocs")]
#[global_allocator]
static ALLOC: matier_bench::alloc_counter::CountingAlloc =
    matier_bench::alloc_counter::CountingAlloc;

#[derive(Parser)]
#[command(version, about = "HTTP client benchmark harness")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a single scenario.
    Run(RunArgs),
    /// Run a cartesian matrix from a TOML file, sequentially.
    Matrix {
        #[arg(long)]
        config: PathBuf,
        /// Output JSON array path.
        #[arg(long)]
        out: PathBuf,
        /// Client tokio worker threads (default: half the CPUs).
        #[arg(long)]
        worker_threads: Option<usize>,
        /// Start matier-bench-server as a child process (killed on exit and on Ctrl-C), using
        /// the matrix file's `url` / `tls_url` / `ca_cert`.
        #[arg(long)]
        spawn_server: bool,
        /// Server binary (default: next to this executable).
        #[arg(long, requires = "spawn_server")]
        server_bin: Option<PathBuf>,
    },
    /// Compare two result files; exits non-zero on regression.
    Compare {
        base: PathBuf,
        new: PathBuf,
        /// Percent change that counts as a regression/improvement.
        #[arg(long, default_value_t = 5.0)]
        threshold: f64,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum LoadKind {
    Closed,
    Open,
}

#[derive(Args)]
struct RunArgs {
    /// Server base URL.
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    url: String,
    #[arg(long, default_value = "hyper-legacy")]
    subject: String,
    #[arg(long, value_enum, default_value = "h1")]
    protocol: Protocol,
    /// Use TLS (rewrites an http:// --url to https://; pass the TLS port in --url).
    #[arg(long)]
    tls: bool,
    /// Server certificate PEM to trust (required for TLS).
    #[arg(long)]
    ca_cert: Option<PathBuf>,
    /// Clients/pools in the `hyper-pooled` and `fetch-pooled` subjects.
    #[arg(long, default_value_t = 4)]
    pool_size: usize,
    /// Pool selection for `fetch-pooled`.
    #[arg(long, value_enum, default_value = "round-robin")]
    pool_selection: PoolSelection,
    #[arg(long, value_enum, default_value = "closed")]
    load: LoadKind,
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    /// Open-loop target rate in req/s.
    #[arg(long, default_value_t = 10_000.0)]
    rate: f64,
    #[arg(long, default_value_t = 4096)]
    max_inflight: usize,
    #[arg(long, default_value = "2s", value_parser = parse_duration)]
    warmup: Duration,
    #[arg(long, default_value = "10s", value_parser = parse_duration)]
    duration: Duration,
    /// Per-request timeout.
    #[arg(long, default_value = "30s", value_parser = parse_duration)]
    timeout: Duration,
    #[arg(long, default_value = "GET")]
    method: String,
    #[arg(long, default_value = "/bytes/1024")]
    path: String,
    #[arg(long, default_value_t = 0)]
    req_body_size: usize,
    /// Client tokio worker threads (default: half the CPUs).
    #[arg(long)]
    worker_threads: Option<usize>,
    /// Write the result JSON here.
    #[arg(long)]
    out: Option<PathBuf>,
}

fn runtime(threads: usize) -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?)
}

fn write_json<T: serde::Serialize>(path: &PathBuf, v: &T) -> Result<()> {
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(path, serde_json::to_string_pretty(v)?)
        .with_context(|| format!("writing {path:?}"))
}

fn main() -> Result<ExitCode> {
    #[cfg(feature = "count-allocs")]
    matier_bench::alloc_counter::mark_enabled();
    match Cli::parse().cmd {
        Cmd::Run(a) => {
            let threads = a.worker_threads.unwrap_or_else(default_worker_threads);
            let url = if a.tls {
                a.url.replacen("http://", "https://", 1)
            } else {
                a.url.clone()
            };
            let cfg = ScenarioConfig {
                subject: a.subject,
                protocol: a.protocol,
                tls: url.starts_with("https://"),
                ca_cert: a.ca_cert,
                pool_size: a.pool_size,
                pool_selection: a.pool_selection,
                url,
                method: a.method,
                path: a.path,
                req_body_size: a.req_body_size,
                load: match a.load {
                    LoadKind::Closed => LoadSpec::Closed {
                        concurrency: a.concurrency,
                    },
                    LoadKind::Open => LoadSpec::Open {
                        rate: a.rate,
                        max_inflight: a.max_inflight,
                    },
                },
                warmup_secs: a.warmup.as_secs_f64(),
                duration_secs: a.duration.as_secs_f64(),
                timeout_secs: a.timeout.as_secs_f64(),
                worker_threads: threads,
            };
            let res = runtime(threads)?.block_on(run_scenario(&cfg))?;
            println!("{}", markdown_table(std::slice::from_ref(&res)));
            if let Some(out) = a.out {
                write_json(&out, &res)?;
            }
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Matrix {
            config,
            out,
            worker_threads,
            spawn_server,
            server_bin,
        } => {
            let mut mc =
                MatrixConfig::load(&config).with_context(|| format!("loading {config:?}"))?;
            if worker_threads.is_some() {
                mc.worker_threads = worker_threads;
            }
            let scenarios = mc.expand()?;
            let cooldown = mc.cooldown()?;
            let rt = runtime(scenarios.first().map_or(1, |s| s.worker_threads))?;
            let _server = if spawn_server {
                let cert = mc
                    .ca_cert
                    .clone()
                    .unwrap_or_else(matier_bench::server::default_cert);
                let tls = mc
                    .tls_url
                    .as_deref()
                    .map(matier_bench::server::authority)
                    .transpose()?;
                let server = matier_bench::server::SpawnedServer::spawn(
                    server_bin.as_deref(),
                    &matier_bench::server::authority(&mc.url)?,
                    tls.as_deref(),
                    &cert,
                )?;
                let on_ctrl_c = server.clone();
                rt.spawn(async move {
                    let _ = tokio::signal::ctrl_c().await;
                    on_ctrl_c.kill();
                    std::process::exit(130);
                });
                Some(server)
            } else {
                None
            };
            let mut results: Vec<RunResult> = vec![];
            let total = scenarios.len();
            for (i, sc) in scenarios.iter().enumerate() {
                eprintln!("[{}/{total}] {}", i + 1, sc.id());
                match rt.block_on(run_scenario(sc)) {
                    Ok(r) => {
                        eprintln!(
                            "    {:.0} req/s, p99 {:.0} us, {} errors",
                            r.throughput.req_per_sec, r.latency.p99_us, r.totals.errors
                        );
                        results.push(r);
                    }
                    Err(e) => eprintln!("    FAILED: {e:#}"),
                }
                write_json(&out, &results)?; // incremental, so partial runs are usable
                std::thread::sleep(cooldown);
            }
            println!("{}", markdown_table(&results));
            let failed = total - results.len();
            if failed > 0 {
                eprintln!("{failed} scenario(s) failed");
            }
            Ok(if failed > 0 {
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            })
        }
        Cmd::Compare {
            base,
            new,
            threshold,
        } => Ok(if compare::compare(&base, &new, threshold)? {
            ExitCode::FAILURE
        } else {
            ExitCode::SUCCESS
        }),
    }
}

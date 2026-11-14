//! Load drivers: closed-loop (fixed concurrency) and open-loop (fixed arrival rate).
//!
//! Latencies are recorded in nanoseconds into per-task [`hdrhistogram`] histograms that are
//! merged at the end; there is no shared lock on the hot path (the open-loop path sends one
//! small sample per request over an unbounded channel to a single collector task).
//!
//! Open-loop latency is measured from the *intended* send time, which avoids coordinated
//! omission: if the client or server stalls, requests that should have been sent during the
//! stall still accrue the waiting time. Timer granularity is ~1 ms, so the scheduler wakes
//! up and issues all requests that have become due; the resulting delay shows up as
//! schedule lag.

use std::{collections::BTreeMap, pin::Pin, sync::Arc, time::Duration};

use hdrhistogram::Histogram;
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinHandle,
    time::{Instant, Sleep, sleep_until, timeout},
};

use crate::{
    metrics::Snapshot,
    scenario::LoadSpec,
    subject::{BenchRequest, BenchResponse, Subject},
};

/// Driver parameters.
#[derive(Clone, Debug)]
pub struct DriverConfig {
    pub load: LoadSpec,
    pub warmup: Duration,
    pub duration: Duration,
    /// Per-request timeout (counted as a `timeout` error).
    pub timeout: Duration,
}

/// Raw driver results for the measured window.
pub struct DriverOutput {
    pub ok: u64,
    pub errors: BTreeMap<String, u64>,
    /// Open-loop arrivals dropped because `max_inflight` was reached.
    pub drops: u64,
    /// Latency from intended send time (open) or send time (closed).
    pub total: Histogram<u64>,
    /// Actual send -> response fully read.
    pub service: Histogram<u64>,
    /// Intended -> actual send (open loop only).
    pub lag: Option<Histogram<u64>>,
    pub resp_bytes: u64,
    pub req_bytes: u64,
    pub window: Duration,
    /// Process counters at the start and end of the measured window.
    pub start: Snapshot,
    pub end: Snapshot,
}

fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 120_000_000_000, 3).expect("valid histogram bounds")
}

struct Stats {
    total: Histogram<u64>,
    service: Option<Histogram<u64>>,
    lag: Option<Histogram<u64>>,
    ok: u64,
    errors: BTreeMap<String, u64>,
    resp_bytes: u64,
    req_bytes: u64,
}

impl Stats {
    fn new(open: bool) -> Self {
        Self {
            total: hist(),
            service: open.then(hist),
            lag: open.then(hist),
            ok: 0,
            errors: BTreeMap::new(),
            resp_bytes: 0,
            req_bytes: 0,
        }
    }

    fn err(&mut self, kind: &str) {
        *self.errors.entry(kind.to_owned()).or_default() += 1;
    }

    fn merge(&mut self, o: Stats) {
        let _ = self.total.add(&o.total);
        if let (Some(a), Some(b)) = (&mut self.service, &o.service) {
            let _ = a.add(b);
        }
        if let (Some(a), Some(b)) = (&mut self.lag, &o.lag) {
            let _ = a.add(b);
        }
        self.ok += o.ok;
        self.resp_bytes += o.resp_bytes;
        self.req_bytes += o.req_bytes;
        for (k, v) in o.errors {
            *self.errors.entry(k).or_default() += v;
        }
    }
}

/// Maps an error to a coarse kind for the per-kind error counters.
fn classify(e: &anyhow::Error) -> &'static str {
    let s = format!("{e:#}").to_ascii_lowercase();
    if s.contains("timed out") || s.contains("timeout") {
        "timeout"
    } else if s.contains("connect")
        && !s.contains("connection closed")
        && !s.contains("connection reset")
    {
        "connect"
    } else if s.contains("reset")
        || s.contains("broken pipe")
        || s.contains("closed")
        || s.contains("eof")
    {
        "connection_closed"
    } else {
        "other"
    }
}

/// Issues one request; `Ok(response body bytes)` or the error kind.
async fn issue<S: Subject>(s: &S, req: BenchRequest, limit: Duration) -> Result<u64, &'static str> {
    match timeout(limit, s.request(req)).await {
        Err(_) => Err("timeout"),
        Ok(res) => outcome(res),
    }
}

/// Like [`issue`], but without a per-request timer: arming and dropping a tokio `Sleep` per request
/// takes the timer driver's global mutex, which showed up as the top lock contention in client
/// profiles and skewed every subject. Closed-loop workers instead share one per-worker `deadline`
/// (re-polled by the same task, so it registers once) and classify slow requests after the fact.
async fn issue_until<S: Subject>(
    s: &S,
    req: BenchRequest,
    limit: Duration,
    deadline: Pin<&mut Sleep>,
) -> Result<u64, &'static str> {
    let started = Instant::now();
    tokio::select! {
        biased;
        res = s.request(req) => {
            if started.elapsed() > limit {
                return Err("timeout");
            }
            outcome(res)
        }
        () = deadline => Err("timeout"),
    }
}

fn outcome(res: anyhow::Result<BenchResponse>) -> Result<u64, &'static str> {
    match res {
        Err(e) => Err(classify(&e)),
        Ok(r) if r.status >= 500 => Err("status_5xx"),
        Ok(r) if r.status >= 400 => Err("status_4xx"),
        Ok(r) => Ok(r.body_len),
    }
}

/// Samples process counters at the window boundaries.
async fn window(start: Instant, end: Instant) -> (Snapshot, Snapshot) {
    sleep_until(start).await;
    let a = Snapshot::take();
    sleep_until(end).await;
    (a, Snapshot::take())
}

/// Runs the configured load against `subject` and returns the merged results.
pub async fn run<S: Subject>(
    subject: Arc<S>,
    req: BenchRequest,
    cfg: &DriverConfig,
) -> DriverOutput {
    let t0 = Instant::now();
    let measure_start = t0 + cfg.warmup;
    let end = measure_start + cfg.duration;
    let win = tokio::spawn(window(measure_start, end));
    let (stats, drops) = match cfg.load {
        LoadSpec::Closed { concurrency } => {
            let workers: Vec<_> = (0..concurrency.max(1))
                .map(|_| {
                    tokio::spawn(closed_worker(
                        subject.clone(),
                        req.clone(),
                        cfg.timeout,
                        measure_start,
                        end,
                    ))
                })
                .collect();
            (join_stats(workers, false).await, 0)
        }
        LoadSpec::Open { rate, max_inflight } => {
            open_loop(
                subject,
                req,
                cfg.timeout,
                rate,
                max_inflight.max(1),
                t0,
                measure_start,
                end,
            )
            .await
        }
    };
    let (start_snap, end_snap) = win.await.expect("window task");
    let mut stats = stats;
    // Closed loop has no schedule lag, so service time equals total latency.
    let service = stats.service.take().unwrap_or_else(|| stats.total.clone());
    DriverOutput {
        ok: stats.ok,
        errors: stats.errors,
        drops,
        total: stats.total,
        service,
        lag: stats.lag,
        resp_bytes: stats.resp_bytes,
        req_bytes: stats.req_bytes,
        window: cfg.duration,
        start: start_snap,
        end: end_snap,
    }
}

async fn join_stats(handles: Vec<JoinHandle<Stats>>, open: bool) -> Stats {
    let mut all = Stats::new(open);
    for h in handles {
        match h.await {
            Ok(s) => all.merge(s),
            Err(_) => all.err("task_panic"),
        }
    }
    all
}

async fn closed_worker<S: Subject>(
    subject: Arc<S>,
    req: BenchRequest,
    limit: Duration,
    measure_start: Instant,
    end: Instant,
) -> Stats {
    let mut st = Stats::new(false);
    let req_len = req.body.len() as u64;
    // A request still outstanding this long after the window ends is hung: stop waiting for it.
    let deadline = sleep_until(end + limit);
    tokio::pin!(deadline);
    loop {
        let start = Instant::now();
        if start >= end {
            break;
        }
        let res = issue_until(&*subject, req.clone(), limit, deadline.as_mut()).await;
        if deadline.is_elapsed() {
            break;
        }
        let done = Instant::now();
        // Only requests completing inside the measured window count.
        if done < measure_start || done >= end {
            continue;
        }
        match res {
            Ok(n) => {
                let _ = st.total.record(((done - start).as_nanos() as u64).max(1));
                st.ok += 1;
                st.resp_bytes += n;
                st.req_bytes += req_len;
            }
            Err(kind) => st.err(kind),
        }
    }
    st
}

struct Sample {
    total_ns: u64,
    service_ns: u64,
    lag_ns: u64,
    resp: u64,
}

#[allow(clippy::too_many_arguments)]
async fn open_loop<S: Subject>(
    subject: Arc<S>,
    req: BenchRequest,
    limit: Duration,
    rate: f64,
    max_inflight: usize,
    t0: Instant,
    measure_start: Instant,
    end: Instant,
) -> (Stats, u64) {
    let (tx, mut rx) = mpsc::unbounded_channel::<Result<Sample, &'static str>>();
    let req_len = req.body.len() as u64;
    let collector = tokio::spawn(async move {
        let mut st = Stats::new(true);
        while let Some(s) = rx.recv().await {
            match s {
                Ok(s) => {
                    let _ = st.total.record(s.total_ns.max(1));
                    let _ = st
                        .service
                        .as_mut()
                        .expect("open")
                        .record(s.service_ns.max(1));
                    let _ = st.lag.as_mut().expect("open").record(s.lag_ns.max(1));
                    st.ok += 1;
                    st.resp_bytes += s.resp;
                    st.req_bytes += req_len;
                }
                Err(kind) => st.err(kind),
            }
        }
        st
    });

    let sem = Arc::new(Semaphore::new(max_inflight));
    let ns_per_req = 1e9 / rate.max(1e-9);
    let mut drops = 0u64;
    let mut k = 0u64;
    loop {
        let intended = t0 + Duration::from_nanos((k as f64 * ns_per_req) as u64);
        if intended >= end {
            break;
        }
        let now = Instant::now();
        if intended > now {
            sleep_until(intended).await;
        } else if k % 256 == 255 {
            tokio::task::yield_now().await; // let tasks run while catching up
        }
        k += 1;
        let in_window = intended >= measure_start;
        let Ok(permit) = sem.clone().try_acquire_owned() else {
            drops += in_window as u64;
            continue;
        };
        let (subject, req, tx) = (subject.clone(), req.clone(), tx.clone());
        tokio::spawn(async move {
            let start = Instant::now();
            let res = issue(&*subject, req, limit).await;
            let done = Instant::now();
            drop(permit);
            if in_window {
                let _ = tx.send(res.map(|resp| Sample {
                    total_ns: (done - intended).as_nanos() as u64,
                    service_ns: (done - start).as_nanos() as u64,
                    lag_ns: start.saturating_duration_since(intended).as_nanos() as u64,
                    resp,
                }));
            }
        });
    }
    drop(tx);
    (collector.await.unwrap_or_else(|_| Stats::new(true)), drops)
}

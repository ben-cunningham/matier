//! Scenario description shared by the CLI, the matrix runner and result files.

use std::{path::PathBuf, time::Duration};

use anyhow::{Result, bail};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::subject::{BenchRequest, SubjectConfig};

/// HTTP version used on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    /// HTTP/1.1
    H1,
    /// HTTP/2 (prior knowledge over plain TCP, ALPN over TLS)
    H2,
}

impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Protocol::H1 => "h1",
            Protocol::H2 => "h2",
        }
    }
}

/// How `fetch-pooled` picks a pool for each request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum PoolSelection {
    #[default]
    RoundRobin,
    /// Fill one pool to its per-client request budget (100) before moving to the next.
    Saturating,
}

/// Load model and its parameters.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "model", rename_all = "lowercase")]
pub enum LoadSpec {
    /// `concurrency` workers, each looping request -> response.
    Closed { concurrency: usize },
    /// Fixed arrival `rate` (req/s) with at most `max_inflight` outstanding requests.
    Open { rate: f64, max_inflight: usize },
}

/// Everything needed to reproduce one measured run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScenarioConfig {
    pub subject: String,
    pub protocol: Protocol,
    pub tls: bool,
    pub ca_cert: Option<PathBuf>,
    /// Number of independent clients for `hyper-pooled`.
    pub pool_size: usize,
    /// Pool selection strategy for `fetch-pooled`.
    #[serde(default)]
    pub pool_selection: PoolSelection,
    /// Base URL (scheme://host:port).
    pub url: String,
    pub method: String,
    pub path: String,
    pub req_body_size: usize,
    pub load: LoadSpec,
    pub warmup_secs: f64,
    pub duration_secs: f64,
    pub timeout_secs: f64,
    pub worker_threads: usize,
}

impl ScenarioConfig {
    /// Stable identifier used to match scenarios across result files.
    pub fn id(&self) -> String {
        let subject = if self.subject.ends_with("-pooled") {
            let sel = if self.pool_selection == PoolSelection::Saturating {
                ",saturating"
            } else {
                ""
            };
            format!("{}(pool={}{sel})", self.subject, self.pool_size)
        } else {
            self.subject.clone()
        };
        let load = match &self.load {
            LoadSpec::Closed { concurrency } => format!("closed-c{concurrency}"),
            LoadSpec::Open { rate, .. } => format!("open-r{rate}"),
        };
        format!(
            "{subject}/{}/{}/{load}/{} {} req{}",
            self.protocol.as_str(),
            if self.tls { "tls" } else { "plain" },
            self.method,
            self.path,
            self.req_body_size,
        )
    }

    /// The request template issued by the driver.
    pub fn request(&self) -> Result<BenchRequest> {
        let base = self.url.trim_end_matches('/');
        let uri = format!("{base}{}", self.path).parse()?;
        let method = self.method.to_ascii_uppercase().parse()?;
        Ok(BenchRequest {
            method,
            uri,
            body: Bytes::from(vec![b'a'; self.req_body_size]),
        })
    }

    pub fn subject_config(&self) -> SubjectConfig {
        SubjectConfig {
            name: self.subject.clone(),
            protocol: self.protocol,
            tls: self.tls,
            ca_cert: self.ca_cert.clone(),
            pool_size: self.pool_size,
            pool_selection: self.pool_selection,
        }
    }

    pub fn warmup(&self) -> Duration {
        Duration::from_secs_f64(self.warmup_secs)
    }
    pub fn duration(&self) -> Duration {
        Duration::from_secs_f64(self.duration_secs)
    }
    pub fn timeout(&self) -> Duration {
        Duration::from_secs_f64(self.timeout_secs)
    }
}

/// Parses `500ms`, `2s`, `1.5s`, `2m` (a bare number means seconds).
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    let (num, mult) = if let Some(n) = s.strip_suffix("ms") {
        (n, 0.001)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1.0)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60.0)
    } else {
        (s, 1.0)
    };
    let v: f64 = num
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid duration {s:?}"))?;
    if v.is_nan() || v < 0.0 {
        bail!("invalid duration {s:?}");
    }
    Ok(Duration::from_secs_f64(v * mult))
}

/// Default tokio worker threads for the client: half the CPUs, leaving the rest for the server.
pub fn default_worker_threads() -> usize {
    std::thread::available_parallelism().map_or(1, |n| (n.get() / 2).max(1))
}

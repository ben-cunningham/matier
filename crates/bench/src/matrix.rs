//! Cartesian scenario matrices loaded from TOML (see `bench/matrix.toml`).

use std::path::PathBuf;

use anyhow::{Result, ensure};
use serde::Deserialize;

use crate::scenario::{
    LoadSpec, PoolSelection, Protocol, ScenarioConfig, default_worker_threads, parse_duration,
};

fn d_warmup() -> String {
    "1s".into()
}
fn d_duration() -> String {
    "5s".into()
}
fn d_cooldown() -> String {
    "1s".into()
}
fn d_timeout() -> String {
    "30s".into()
}
fn d_method() -> String {
    "GET".into()
}
fn d_pool() -> usize {
    4
}
fn d_inflight() -> usize {
    4096
}

/// Top-level matrix file: shared settings plus a list of groups, each a cartesian product.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatrixConfig {
    /// Plain base URL, e.g. `http://127.0.0.1:8080`.
    pub url: String,
    /// TLS base URL, e.g. `https://127.0.0.1:8443` (needed by groups with `tls = [true]`).
    pub tls_url: Option<String>,
    pub ca_cert: Option<PathBuf>,
    #[serde(default = "d_warmup")]
    pub warmup: String,
    #[serde(default = "d_duration")]
    pub duration: String,
    /// Pause between runs.
    #[serde(default = "d_cooldown")]
    pub cooldown: String,
    #[serde(default = "d_timeout")]
    pub timeout: String,
    pub worker_threads: Option<usize>,
    #[serde(rename = "group")]
    pub groups: Vec<Group>,
}

/// One cartesian product: subjects x protocols x tls x load levels x sizes.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Group {
    pub subjects: Vec<String>,
    pub protocols: Vec<Protocol>,
    /// Defaults to `[false]`.
    #[serde(default)]
    pub tls: Vec<bool>,
    /// Closed-loop concurrency levels.
    #[serde(default)]
    pub concurrency: Vec<usize>,
    /// Open-loop arrival rates (req/s).
    #[serde(default)]
    pub rates: Vec<f64>,
    #[serde(default = "d_inflight")]
    pub max_inflight: usize,
    #[serde(default = "d_pool")]
    pub pool_size: usize,
    /// `round-robin` (default) or `saturating`; only affects `fetch-pooled`.
    #[serde(default)]
    pub pool_selection: PoolSelection,
    /// Response sizes; used to build `/bytes/{n}` when `path` is unset.
    #[serde(default)]
    pub resp_sizes: Vec<usize>,
    /// Request body sizes (default `[0]`).
    #[serde(default)]
    pub req_sizes: Vec<usize>,
    #[serde(default = "d_method")]
    pub method: String,
    pub path: Option<String>,
    pub warmup: Option<String>,
    pub duration: Option<String>,
}

impl MatrixConfig {
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }

    /// Cooldown between runs.
    pub fn cooldown(&self) -> Result<std::time::Duration> {
        parse_duration(&self.cooldown)
    }

    /// Expands all groups into concrete scenarios.
    pub fn expand(&self) -> Result<Vec<ScenarioConfig>> {
        let threads = self.worker_threads.unwrap_or_else(default_worker_threads);
        let mut out = vec![];
        for g in &self.groups {
            let tls_opts = if g.tls.is_empty() {
                vec![false]
            } else {
                g.tls.clone()
            };
            let req_sizes = if g.req_sizes.is_empty() {
                vec![0]
            } else {
                g.req_sizes.clone()
            };
            let resp_sizes = if g.resp_sizes.is_empty() || g.path.is_some() {
                vec![64]
            } else {
                g.resp_sizes.clone()
            };
            let mut loads: Vec<LoadSpec> = g
                .concurrency
                .iter()
                .map(|&concurrency| LoadSpec::Closed { concurrency })
                .collect();
            loads.extend(g.rates.iter().map(|&rate| LoadSpec::Open {
                rate,
                max_inflight: g.max_inflight,
            }));
            ensure!(
                !loads.is_empty(),
                "group needs `concurrency` and/or `rates`"
            );
            let warmup = parse_duration(g.warmup.as_ref().unwrap_or(&self.warmup))?.as_secs_f64();
            let duration =
                parse_duration(g.duration.as_ref().unwrap_or(&self.duration))?.as_secs_f64();
            let timeout = parse_duration(&self.timeout)?.as_secs_f64();
            for subject in &g.subjects {
                for &protocol in &g.protocols {
                    for &tls in &tls_opts {
                        let url = if tls {
                            self.tls_url
                                .clone()
                                .ok_or_else(|| anyhow::anyhow!("tls group needs `tls_url`"))?
                        } else {
                            self.url.clone()
                        };
                        for load in &loads {
                            for &resp in &resp_sizes {
                                for &req in &req_sizes {
                                    out.push(ScenarioConfig {
                                        subject: subject.clone(),
                                        protocol,
                                        tls,
                                        ca_cert: self.ca_cert.clone(),
                                        pool_size: g.pool_size,
                                        pool_selection: g.pool_selection,
                                        url: url.clone(),
                                        method: g.method.clone(),
                                        path: g
                                            .path
                                            .clone()
                                            .unwrap_or_else(|| format!("/bytes/{resp}")),
                                        req_body_size: req,
                                        load: load.clone(),
                                        warmup_secs: warmup,
                                        duration_secs: duration,
                                        timeout_secs: timeout,
                                        worker_threads: threads,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(out)
    }
}

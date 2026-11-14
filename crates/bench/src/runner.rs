//! Runs one scenario end to end.

use std::sync::Arc;

use anyhow::{Context, Result};

use crate::{
    driver::{self, DriverConfig},
    report::RunResult,
    scenario::ScenarioConfig,
    subject::{Subject, build_subject},
};

/// Builds the subject, verifies it can reach the server, then measures. Call inside the runtime.
pub async fn run_scenario(cfg: &ScenarioConfig) -> Result<RunResult> {
    let subject = Arc::new(build_subject(&cfg.subject_config())?);
    let req = cfg.request()?;
    let probe = tokio::time::timeout(cfg.timeout(), subject.request(req.clone()))
        .await
        .context("probe request timed out")?
        .with_context(|| format!("probe request to {} failed", req.uri))?;
    anyhow::ensure!(
        probe.status < 400,
        "probe request returned HTTP {}",
        probe.status
    );

    let dcfg = DriverConfig {
        load: cfg.load.clone(),
        warmup: cfg.warmup(),
        duration: cfg.duration(),
        timeout: cfg.timeout(),
    };
    let out = driver::run(subject.clone(), req, &dcfg).await;
    Ok(RunResult::new(cfg, subject.name(), &out))
}

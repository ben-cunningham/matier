//! N independent hyper legacy clients used round-robin.
//!
//! Mimics microsoft/oxidizer `fetch`'s `multiple_pools` workaround: with HTTP/2 a single
//! client multiplexes everything over one connection, so spreading requests over several
//! clients (hence several connections) avoids that single-connection bottleneck.

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use anyhow::{Result, ensure};

use super::{BenchRequest, BenchResponse, Subject, SubjectConfig, hyper_legacy::HyperLegacy};

pub struct HyperPooled {
    clients: Vec<HyperLegacy>,
    next: AtomicUsize,
}

impl HyperPooled {
    pub fn new(cfg: &SubjectConfig) -> Result<Self> {
        ensure!(cfg.pool_size > 0, "--pool-size must be > 0");
        let clients = (0..cfg.pool_size)
            .map(|_| HyperLegacy::new(cfg))
            .collect::<Result<_>>()?;
        Ok(Self {
            clients,
            next: AtomicUsize::new(0),
        })
    }
}

impl Subject for HyperPooled {
    fn name(&self) -> &str {
        "hyper-pooled"
    }

    async fn request(&self, req: BenchRequest) -> Result<BenchResponse> {
        let i = self.next.fetch_add(1, Relaxed) % self.clients.len();
        self.clients[i].request(req).await
    }
}

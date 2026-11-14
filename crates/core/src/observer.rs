//! Instrumentation hooks.

use std::time::Duration;

use crate::origin::{OriginKey, Protocol};

/// Connection establishment phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConnectPhase {
    Dns,
    Tcp,
    Tls,
}

impl ConnectPhase {
    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dns => "dns",
            Self::Tcp => "tcp",
            Self::Tls => "tls",
        }
    }
}

/// Instrumentation hooks. All methods default to no-ops; `matier-otel` implements them.
/// Called on hot paths: implementations must not block or allocate per call where avoidable.
#[allow(unused_variables, reason = "default no-op bodies")]
pub trait Observer: Send + Sync + 'static {
    /// Callers skip timestamping when this is false.
    fn enabled(&self) -> bool {
        false
    }
    /// Pool checkout + stream-slot wait.
    fn request_queued(&self, o: &OriginKey, wait: Duration) {}
    /// H2: enqueue to HEADERS written.
    fn request_dispatched(&self, o: &OriginKey, delay: Duration) {}
    fn connect_phase(&self, o: &OriginKey, phase: ConnectPhase, d: Duration, ok: bool) {}
    fn connection_opened(&self, o: &OriginKey, p: Protocol) {}
    fn connection_closed(&self, o: &OriginKey, p: Protocol, lifetime: Duration) {}
    fn write_batch(&self, o: &OriginKey, bytes: usize, iovecs: usize) {}
    fn bytes_read(&self, o: &OriginKey, n: usize) {}
    fn actor_tick(&self, o: &OriginKey, busy: Duration, queue_depth: usize) {}
    fn flow_control_stall(&self, o: &OriginKey, d: Duration) {}
    fn h2_goaway(&self, o: &OriginKey, code: u32) {}
    fn h2_rst_stream(&self, o: &OriginKey, code: u32, sent: bool) {}
    fn pool_waiters(&self, o: &OriginKey, n: usize) {}
}

/// The default observer: does nothing, `enabled()` is false.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopObserver;

impl Observer for NoopObserver {}

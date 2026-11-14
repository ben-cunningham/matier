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

/// Why the pool changed the number of connections to an origin (see `Observer::pool_scaled`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScaleReason {
    /// A connection was saturated (busy actor, dispatch delay, full of streams): one more was dialed.
    Saturated,
    /// Requests were waiting for a stream slot and a connection was dialed for them.
    Waiters,
    /// Dialed to (re-)establish `h2_min_conns_per_origin`.
    Min,
    /// A connection with no streams for `idle_timeout` was closed.
    Idle,
    /// The origin was underused and one connection was drained and closed.
    Underused,
}

impl ScaleReason {
    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Saturated => "saturated",
            Self::Waiters => "waiters",
            Self::Min => "min",
            Self::Idle => "idle",
            Self::Underused => "underused",
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
    /// The pool changed the HTTP/2 connection count of `o`; `conns` is the new number of live connections.
    /// Rare (at most a few per second); called from pool tasks, never from the request hot path.
    fn pool_scaled(&self, o: &OriginKey, conns: usize, reason: ScaleReason) {}
}

/// The default observer: does nothing, `enabled()` is false.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopObserver;

impl Observer for NoopObserver {}

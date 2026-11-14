//! Caller-driven HTTP/1.1 connection (PLAN §5): no background task, no channel.
//!
//! An [`H1Conn`] is owned by the request in flight. [`H1Conn::send`] writes the request (one
//! vectored write for head + body), reads the response head and returns the response; the
//! [`RecvBody`] inside owns the connection and hands it back through a release callback as soon
//! as the final body byte has been observed (keep-alive), so a pool gets it back without waiting
//! for the caller to drop the body.
//!
//! # Deviations / v0 behaviour
//!
//! * Dropping a [`RecvBody`] before completion closes the connection (no drain of small
//!   remainders yet).
//! * The request is written completely before the response is read (no early-response handling
//!   while a large upload is still in flight). `Expect: 100-continue` is not waited for: the body
//!   is sent immediately and any `100 Continue` is skipped like every other 1xx.
//! * Trailers (response) are discarded.

mod body;
mod conn;

use std::sync::Arc;

pub use body::RecvBody;
pub use conn::{H1Config, H1Conn, StaleKeepAlive, is_idempotent, is_stale_keepalive};

/// Callback that takes a finished keep-alive connection back (typically into a pool).
pub trait ReleaseFn: FnOnce(H1Conn) + Send + 'static {}
impl<F: FnOnce(H1Conn) + Send + 'static> ReleaseFn for F {}

/// Allocation-free alternative to [`ReleaseFn`]: a shared releaser (e.g. an `Arc<OriginPool>`).
pub trait Release: Send + Sync + 'static {
    /// Called exactly once per request when the connection can be reused.
    fn release(&self, conn: H1Conn);

    /// Called when a connection registered with [`H1Conn::set_owner`] is dropped for good (it was
    /// not handed back through `release`, or the pool dropped it). Must not block; it may run
    /// inside any task, including while the pool is dropping idle connections, so it must not
    /// be called with the owner's locks held.
    fn closed(&self, _conn_id: u64, _lifetime: std::time::Duration) {}
}

pub(crate) enum Releaser {
    Fn(Box<dyn FnOnce(H1Conn) + Send>),
    Shared(Arc<dyn Release>),
}

impl Releaser {
    pub(crate) fn release(self, conn: H1Conn) {
        match self {
            Self::Fn(f) => f(conn),
            Self::Shared(r) => r.release(conn),
        }
    }
}

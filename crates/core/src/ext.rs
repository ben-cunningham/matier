//! Per-request / per-response `http::Extensions` types.

use std::time::Duration;

use crate::origin::Protocol;

/// Deadline for receiving the response head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResponseTimeout(pub Duration);

/// Idle timeout between body chunks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BodyTimeout(pub Duration);

/// RFC 9218 priority. Defaults: urgency 3, non-incremental.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Priority {
    /// 0 (highest) ..= 7 (lowest).
    pub urgency: u8,
    pub incremental: bool,
}

impl Default for Priority {
    fn default() -> Self {
        Self {
            urgency: 3,
            incremental: false,
        }
    }
}

impl Priority {
    /// Creates a priority, clamping urgency to 0..=7.
    #[must_use]
    pub fn new(urgency: u8, incremental: bool) -> Self {
        Self {
            urgency: urgency.min(7),
            incremental,
        }
    }
}

/// Low-cardinality URI template for telemetry (`url.template`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UriTemplate(pub &'static str);

/// Response-side extension describing the connection that served the request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionInfo {
    pub protocol: Protocol,
    pub reused: bool,
    pub conn_id: u64,
}

//! `matier-bench`: a benchmark harness for HTTP clients.
//!
//! A [`subject::Subject`] wraps a client under test; the [`driver`] applies closed-loop or
//! open-loop load; [`metrics`] samples process CPU / memory / allocations around the measured
//! window; [`report`] turns it all into a machine-readable [`report::RunResult`]; and
//! [`compare`] diffs two result files for regression detection.

pub mod alloc_counter;
pub mod compare;
pub mod driver;
pub mod matrix;
pub mod metrics;
pub mod report;
pub mod runner;
pub mod scenario;
pub mod server;
pub mod subject;

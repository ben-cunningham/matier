//! HTTP/2 connection autoscaling policy: thresholds and the pure decision functions (unit tested).
//!
//! Zero-config: nobody tells the pool how many connections to open. It starts with
//! `h2_min_conns_per_origin` (1), watches the load signals each connection actor publishes
//! (`matier_transport::h2::LoadSnapshot`) and
//!
//! * **scales up** by one background dial when the connection a request just picked is saturated, at most
//!   once per [`SCALE_UP_COOLDOWN`], never above `h2_max_conns_per_origin` (default: the number of CPUs) or the runtime's worker count;
//!   the triggering request is not delayed;
//! * **scales down** by closing connections that carried no stream for `idle_timeout`, and by draining
//!   the newest connection when the whole origin has been under-used for [`DRAIN_AFTER_LOW`]
//!   (with the hysteresis rules below), always keeping `h2_min_conns_per_origin`.

use std::time::Duration;

use matier_transport::h2::LoadSnapshot;

/// A connection actor busy more than this fraction (permille) of wall time (EWMA over 10 ms windows) is
/// saturated. This is deliberately far below 100 %: the actor is one link of a request/response pipeline
/// (callers -> actor -> socket -> server -> socket -> actor -> callers), so at ~50 % busy it already spends
/// the rest of its time waiting for work it could have overlapped with a second connection. Measured on
/// the bench at c=64 / 1 KiB: one connection is 57 % busy and carries 113k req/s; two connections are 43 %
/// busy each and carry 213k req/s.
pub const BUSY_SATURATED_PERMILLE: u32 = 500;
/// A connection whose requests wait longer than this (EWMA of enqueue -> HEADERS handed to the socket) is
/// saturated even if the actor is not 100% busy (it is blocked on the socket or lagging behind its inbox).
pub const DISPATCH_SATURATED: Duration = Duration::from_micros(100);
/// A command queue longer than this at a tick means the actor cannot keep up with its callers.
pub const INBOX_SATURATED: usize = 256;
/// The load signals only count as saturation with at least this many streams in flight on the connection:
/// a connection carrying one or two requests cannot be helped by a second one (nothing to split), however
/// slow its actor or its peer is.
pub const SCALE_MIN_IN_FLIGHT: usize = 4;
/// A connection this deep is saturated at a much lower busy fraction: with this many streams multiplexed on one
/// actor, a second connection splits the per-request actor work even though the actor is not close to
/// saturation (it alternates between small bursts and waiting on the peer). Measured at c=64 / 1 KiB after the
/// receive-path work: one connection sits at 45-50 % busy (just under [`BUSY_SATURATED_PERMILLE`]) and carries
/// 114k req/s, two ~195k, three ~233k, four or five ~245k (5 runtime workers). At 40 (the old value) c=32 stayed
/// on one connection (115k vs 176k) and c=64 sometimes stalled at two for seconds. Overshoot is bounded by the
/// ceiling ([`h2_ceiling`]: never more connections than runtime workers), and an actor with little to do stays
/// under [`SCALE_DEEP_BUSY_PERMILLE`] however many slow requests it carries.
pub const SCALE_DEEP_IN_FLIGHT: usize = 16;
/// Busy fraction (permille) above which a connection with [`SCALE_DEEP_IN_FLIGHT`] streams is saturated.
pub const SCALE_DEEP_BUSY_PERMILLE: u32 = 250;
/// Minimum spacing between scale-up dials for one origin. Roughly the time the busy EWMA needs to reflect the
/// new connection (a few 10 ms windows), so a burst does not overshoot to the maximum.
pub const SCALE_UP_COOLDOWN: Duration = Duration::from_millis(50);
/// "Under-used" means every selectable connection is below this busy fraction (permille) ...
pub const BUSY_LOW_PERMILLE: u32 = 200;
/// ... and the remaining connections would carry the load at no more than this busy fraction (permille) after
/// one is drained. The gap between this (300) and [`BUSY_SATURATED_PERMILLE`] (500) is the hysteresis band that
/// stops drain/scale-up flapping.
pub const DRAIN_PROJECTED_MAX_PERMILLE: u32 = 300;
/// The origin must be under-used continuously for this long before a connection is drained; the timer restarts
/// after every drain, so connections go one at a time.
pub const DRAIN_AFTER_LOW: Duration = Duration::from_secs(1);
/// No drain within this long after a scale-up.
pub const DRAIN_HOLDOFF_AFTER_SCALE_UP: Duration = Duration::from_secs(5);
/// Bounds of the maintenance tick (idle / drain bookkeeping); the tick is `idle_timeout / 4` within them.
pub const REAP_TICK_MIN: Duration = Duration::from_millis(5);
/// See [`REAP_TICK_MIN`].
pub const REAP_TICK_MAX: Duration = Duration::from_millis(250);

/// Default upper bound of HTTP/2 connections per origin: the number of CPUs (a connection actor is one task,
/// so more connections than cores cannot add parallelism).
#[must_use]
pub fn default_h2_max_conns() -> usize {
    std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
}

/// The ceiling autoscaling actually uses: `max_conns`, further capped at the current tokio runtime's worker
/// count (more actors than workers cannot run in parallel; they only split each connection's write batches,
/// which costs syscalls on both ends), but never below `min_conns`, so an explicit pin is honoured.
#[must_use]
pub fn h2_ceiling(min_conns: usize, max_conns: usize) -> usize {
    let workers =
        tokio::runtime::Handle::try_current().map_or(usize::MAX, |h| h.metrics().num_workers());
    max_conns.min(workers).max(min_conns).max(1)
}

/// Scale-up thresholds. The defaults are the named constants above; override them only for tests or for
/// workloads whose connections you know to be unusual.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScaleThresholds {
    /// See [`BUSY_SATURATED_PERMILLE`].
    pub busy_permille: u32,
    /// See [`DISPATCH_SATURATED`].
    pub dispatch_delay: Duration,
    /// See [`INBOX_SATURATED`].
    pub inbox: usize,
    /// See [`SCALE_UP_COOLDOWN`].
    pub cooldown: Duration,
}

impl Default for ScaleThresholds {
    fn default() -> Self {
        Self {
            busy_permille: BUSY_SATURATED_PERMILLE,
            dispatch_delay: DISPATCH_SATURATED,
            inbox: INBOX_SATURATED,
            cooldown: SCALE_UP_COOLDOWN,
        }
    }
}

impl ScaleThresholds {
    /// Is a connection with these signals (and `in_flight` of `limit` streams) saturated?
    #[must_use]
    pub fn is_saturated(&self, load: LoadSnapshot, in_flight: usize, limit: usize) -> bool {
        in_flight >= limit
            || (in_flight >= SCALE_DEEP_IN_FLIGHT && load.busy_permille > SCALE_DEEP_BUSY_PERMILLE)
            || (in_flight >= SCALE_MIN_IN_FLIGHT
                && (load.busy_permille > self.busy_permille
                    || load.dispatch_delay > self.dispatch_delay
                    || load.inbox > self.inbox))
    }
}

/// [`ScaleThresholds::is_saturated`] with the default thresholds.
#[must_use]
pub fn is_saturated(load: LoadSnapshot, in_flight: usize, limit: usize) -> bool {
    ScaleThresholds::default().is_saturated(load, in_flight, limit)
}

/// May one connection be drained, given the busy fraction (permille) of every selectable connection?
/// (The caller checks `len > h2_min_conns_per_origin`.)
#[must_use]
pub fn can_drain(busy_permille: &[u32]) -> bool {
    let n = busy_permille.len();
    if n < 2 {
        return false;
    }
    let sum: u32 = busy_permille.iter().sum();
    busy_permille.iter().all(|&b| b < BUSY_LOW_PERMILLE)
        && sum / (n as u32 - 1) < DRAIN_PROJECTED_MAX_PERMILLE
}

/// Maintenance tick for an origin with this `idle_timeout`.
#[must_use]
pub fn reap_tick(idle_timeout: Duration) -> Duration {
    (idle_timeout / 4).clamp(REAP_TICK_MIN, REAP_TICK_MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(busy: u32, disp_us: u64, inbox: usize) -> LoadSnapshot {
        LoadSnapshot {
            busy_permille: busy,
            dispatch_delay: Duration::from_micros(disp_us),
            inbox,
        }
    }

    #[test]
    fn saturation_signals() {
        assert!(!is_saturated(load(0, 0, 0), 3, 128));
        assert!(!is_saturated(load(250, 100, 256), 39, 128)); // boundaries are exclusive
        assert!(!is_saturated(
            load(500, 100, 256),
            SCALE_DEEP_IN_FLIGHT - 1,
            128
        ));
        assert!(is_saturated(load(501, 0, 0), 8, 128));
        assert!(is_saturated(load(10, 101, 0), 8, 128));
        assert!(is_saturated(load(10, 0, 257), 8, 128));
        // Nothing to split: a lone request never justifies another connection.
        assert!(!is_saturated(load(1000, 5000, 1000), 1, 128));
        assert!(!is_saturated(
            load(1000, 5000, 1000),
            SCALE_MIN_IN_FLIGHT - 1,
            128
        ));
        assert!(is_saturated(load(1000, 0, 0), SCALE_MIN_IN_FLIGHT, 128));
        assert!(is_saturated(load(0, 0, 0), 128, 128));
        assert!(is_saturated(load(0, 0, 0), 5, 4));
    }

    #[test]
    fn deep_connections_saturate_at_a_lower_busy_fraction() {
        // c=64 on one connection at ~47 % busy: a second connection helps.
        assert!(is_saturated(load(470, 20, 6), 64, 128));
        assert!(is_saturated(load(251, 0, 0), SCALE_DEEP_IN_FLIGHT, 128));
        // Light or balanced load never dials extra connections.
        assert!(!is_saturated(load(250, 0, 0), 64, 128));
        assert!(!is_saturated(
            load(470, 20, 6),
            SCALE_DEEP_IN_FLIGHT - 1,
            128
        ));
        assert!(!is_saturated(load(470, 20, 6), 1, 128));
        // Two balanced c=64 connections (32 each) still help with a third.
        assert!(is_saturated(load(430, 20, 6), 32, 128));
    }

    #[test]
    fn drain_needs_everyone_low_and_projection_ok() {
        assert!(!can_drain(&[]));
        assert!(!can_drain(&[10]));
        assert!(can_drain(&[10, 10]));
        assert!(can_drain(&[140, 140, 140])); // 420/2 = 210 < 300
        assert!(!can_drain(&[190, 190])); // projected 380 would be too close to saturation
        assert!(!can_drain(&[10, 200])); // one connection not low
        assert!(can_drain(&[199, 199, 199, 199, 199, 199, 199, 199, 100])); // 1491/8 = 186
        // Hysteresis: the projected load after a drain never reaches the scale-up threshold.
        for n in 2..=64u32 {
            let busy = vec![BUSY_LOW_PERMILLE - 1; n as usize];
            if can_drain(&busy) {
                let projected = (BUSY_LOW_PERMILLE - 1) * n / (n - 1);
                assert!(projected < BUSY_SATURATED_PERMILLE);
            }
        }
    }

    #[test]
    fn ceiling_follows_runtime_workers_but_honours_a_pin() {
        assert_eq!(h2_ceiling(1, 8), 8); // no runtime: config only
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(3)
            .build()
            .unwrap();
        rt.block_on(async {
            assert_eq!(h2_ceiling(1, 8), 3);
            assert_eq!(h2_ceiling(1, 2), 2);
            assert_eq!(h2_ceiling(6, 6), 6);
        });
    }

    #[test]
    fn tick_is_bounded() {
        assert_eq!(reap_tick(Duration::from_millis(1)), REAP_TICK_MIN);
        assert_eq!(reap_tick(Duration::from_secs(90)), REAP_TICK_MAX);
        assert_eq!(
            reap_tick(Duration::from_millis(200)),
            Duration::from_millis(50)
        );
    }

    #[test]
    fn default_max_is_positive() {
        assert!(default_h2_max_conns() >= 1);
    }
}

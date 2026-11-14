//! Per-connection load signals for pool autoscaling.
//!
//! The actor owns a [`LoadMeter`] (plain fields, no atomics) and publishes its smoothed values into
//! [`LoadCell`] (atomics inside `ConnShared`) once per [`WINDOW`], not per tick. Readers (the pool) pay only
//! relaxed loads of a cache line that is written ~100 times a second.
//!
//! Signals:
//! * **busy**: EWMA of `time spent inside Actor::poll / wall time` over 10 ms windows, in permille.
//! * **dispatch delay**: EWMA of the time from `Command::Open` enqueue to its HEADERS being handed to the
//!   socket, sampled on a subset of requests (see [`SAMPLE_EVERY`]).
//! * **inbox**: the largest command-queue depth seen at a tick during the last window.

use std::cell::Cell;
use std::time::{Duration, Instant};

use super::sync::{AtomicU32, Ordering};

/// Length of one busy-ratio bucket.
pub(crate) const WINDOW: Duration = Duration::from_millis(10);
/// EWMA weight (permille) of one window. A gap of `n` windows weighs `n` times as much (capped at 1000).
const ALPHA_PERMILLE: u32 = 300;
/// Weight (1/N) of a single dispatch-delay sample.
const DISPATCH_ALPHA_SHIFT: u32 = 3;
/// One request in this many carries an enqueue timestamp (when no observer asks for all of them).
pub(crate) const SAMPLE_EVERY: u32 = 4;
/// Published values older than this many windows are treated as decayed to zero (the actor stopped ticking).
const STALE_WINDOWS: u32 = 8;

/// `prev + (sample - prev) * alpha`, all in integer permille / raw units.
pub(crate) fn ewma_step(prev: u32, sample: u32, alpha_permille: u32) -> u32 {
    let a = u64::from(alpha_permille.min(1000));
    let p = u64::from(prev);
    let s = u64::from(sample);
    ((p * (1000 - a) + s * a) / 1000) as u32
}

thread_local! {
    static SAMPLE_CTR: Cell<u32> = const { Cell::new(0) };
}

/// True for one call in every [`SAMPLE_EVERY`] on this thread (cheap: no shared state).
pub(crate) fn sample_now() -> bool {
    SAMPLE_CTR.with(|c| {
        let n = c.get().wrapping_add(1);
        c.set(n);
        n.is_multiple_of(SAMPLE_EVERY)
    })
}

/// Actor-side accumulator.
pub(crate) struct LoadMeter {
    win_start: Instant,
    busy: Duration,
    busy_ewma: u32,
    dispatch_ewma_ns: u32,
    inbox_max: u32,
}

impl LoadMeter {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            win_start: now,
            busy: Duration::ZERO,
            busy_ewma: 0,
            dispatch_ewma_ns: 0,
            inbox_max: 0,
        }
    }

    pub(crate) fn record_dispatch(&mut self, d: Duration) {
        let ns = u32::try_from(d.as_nanos()).unwrap_or(u32::MAX);
        let prev = self.dispatch_ewma_ns;
        self.dispatch_ewma_ns = if prev == 0 {
            ns
        } else if ns >= prev {
            prev + ((ns - prev) >> DISPATCH_ALPHA_SHIFT)
        } else {
            prev - ((prev - ns) >> DISPATCH_ALPHA_SHIFT)
        };
    }

    /// Records one actor poll that ran from `start` to `end`, with `inbox` commands still queued.
    /// Returns true when a window closed (the caller should [`Self::publish`]).
    pub(crate) fn record_tick(&mut self, start: Instant, end: Instant, inbox: usize) -> bool {
        self.busy += end.saturating_duration_since(start);
        self.inbox_max = self.inbox_max.max(u32::try_from(inbox).unwrap_or(u32::MAX));
        let elapsed = end.saturating_duration_since(self.win_start);
        if elapsed < WINDOW {
            return false;
        }
        let ratio = (self.busy.as_nanos() * 1000 / elapsed.as_nanos().max(1)).min(1000) as u32;
        let n = u32::try_from(elapsed.as_nanos() / WINDOW.as_nanos()).unwrap_or(u32::MAX);
        self.busy_ewma = ewma_step(self.busy_ewma, ratio, ALPHA_PERMILLE.saturating_mul(n));
        self.win_start = end;
        self.busy = Duration::ZERO;
        true
    }

    /// Writes the smoothed values and resets the per-window inbox maximum.
    pub(crate) fn publish(&mut self, cell: &LoadCell, now_ms: u32) {
        cell.busy_permille.store(self.busy_ewma, Ordering::Relaxed);
        cell.dispatch_ns
            .store(self.dispatch_ewma_ns, Ordering::Relaxed);
        cell.inbox.store(self.inbox_max, Ordering::Relaxed);
        cell.stamp_ms
            .store(now_ms.saturating_add(1), Ordering::Release);
        self.inbox_max = 0;
    }
}

/// Published signals (inside `ConnShared`, one cache line of its own).
#[repr(align(64))]
pub(crate) struct LoadCell {
    busy_permille: AtomicU32,
    dispatch_ns: AtomicU32,
    inbox: AtomicU32,
    /// Milliseconds since the connection's epoch at the last publish (+1, so 0 means "never").
    stamp_ms: AtomicU32,
}

impl LoadCell {
    pub(crate) fn new() -> Self {
        Self {
            busy_permille: AtomicU32::new(0),
            dispatch_ns: AtomicU32::new(0),
            inbox: AtomicU32::new(0),
            stamp_ms: AtomicU32::new(0),
        }
    }

    /// Raw read, no staleness handling (no clock needed).
    pub(crate) fn raw(&self) -> LoadSnapshot {
        LoadSnapshot {
            busy_permille: self.busy_permille.load(Ordering::Relaxed),
            dispatch_delay: Duration::from_nanos(u64::from(
                self.dispatch_ns.load(Ordering::Relaxed),
            )),
            inbox: self.inbox.load(Ordering::Relaxed) as usize,
        }
    }

    /// True if the last publish is older than the staleness bound at `now_ms`.
    pub(crate) fn is_stale(&self, now_ms: u32) -> bool {
        let s = self.stamp_ms.load(Ordering::Acquire);
        s == 0
            || now_ms.saturating_add(1).saturating_sub(s)
                > STALE_WINDOWS * (WINDOW.as_millis() as u32)
    }
}

/// A connection's load signals at one instant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LoadSnapshot {
    /// EWMA fraction of wall time the actor spent inside its poll, in permille (0..=1000).
    pub busy_permille: u32,
    /// EWMA enqueue-to-HEADERS-written delay.
    pub dispatch_delay: Duration,
    /// Largest command-queue depth seen at a tick during the last window.
    pub inbox: usize,
}

#[cfg(all(test, not(matier_loom)))]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn ewma_step_math() {
        assert_eq!(ewma_step(0, 1000, 300), 300);
        assert_eq!(ewma_step(1000, 0, 300), 700);
        assert_eq!(ewma_step(500, 500, 300), 500);
        assert_eq!(ewma_step(0, 1000, 5000), 1000); // alpha capped
    }

    #[test]
    fn busy_converges_up_and_down() {
        let t0 = Instant::now();
        let mut m = LoadMeter::new(t0);
        let mut now = t0;
        // 100% busy: every 10 ms window is fully spent in polls.
        for _ in 0..30 {
            let end = now + WINDOW;
            m.record_tick(now, end, 0);
            now = end;
        }
        assert!(m.busy_ewma > 950, "{}", m.busy_ewma);
        // Idle windows: one tiny poll at the end of each window.
        for _ in 0..30 {
            let end = now + WINDOW;
            m.record_tick(end - Duration::from_micros(10), end, 0);
            now = end;
        }
        assert!(m.busy_ewma < 50, "{}", m.busy_ewma);
    }

    #[test]
    fn half_busy_is_about_half() {
        let t0 = Instant::now();
        let mut m = LoadMeter::new(t0);
        let mut now = t0;
        for _ in 0..40 {
            m.record_tick(now, now + ms(5), 0);
            now += WINDOW;
            m.record_tick(now - Duration::from_nanos(1), now, 0);
        }
        assert!((450..=550).contains(&m.busy_ewma), "{}", m.busy_ewma);
    }

    #[test]
    fn long_idle_gap_decays_fast() {
        let t0 = Instant::now();
        let mut m = LoadMeter::new(t0);
        m.busy_ewma = 900;
        // One short poll after a 100 ms gap.
        let end = t0 + ms(100);
        assert!(m.record_tick(end - Duration::from_micros(5), end, 0));
        assert!(m.busy_ewma < 10, "{}", m.busy_ewma);
    }

    #[test]
    fn dispatch_ewma_and_publish() {
        let t0 = Instant::now();
        let mut m = LoadMeter::new(t0);
        for _ in 0..50 {
            m.record_dispatch(Duration::from_micros(200));
        }
        assert!((190_000..=210_000).contains(&m.dispatch_ewma_ns));
        for _ in 0..50 {
            m.record_dispatch(Duration::from_micros(10));
        }
        assert!(m.dispatch_ewma_ns < 20_000);
        let cell = LoadCell::new();
        assert!(cell.is_stale(5));
        m.inbox_max = 7;
        m.publish(&cell, 100);
        let s = cell.raw();
        assert_eq!(s.inbox, 7);
        assert!(!cell.is_stale(120));
        assert!(cell.is_stale(100 + 8 * 10 + 1));
    }

    #[test]
    fn sampling_rate() {
        let n = (0..400).filter(|_| sample_now()).count();
        assert_eq!(n, 100);
    }
}

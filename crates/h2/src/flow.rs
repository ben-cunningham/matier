//! Flow-control window arithmetic (RFC 9113 §5.2, §6.9).
//!
//! # Send-window representation
//!
//! A connection holds one shared `initial` value (the peer's SETTINGS_INITIAL_WINDOW_SIZE).
//! Each stream stores only a signed *offset* (`credited - sent`, [`StreamSendWindow`]) so
//! that the effective window is `initial + offset`. When the peer changes
//! SETTINGS_INITIAL_WINDOW_SIZE the actor just overwrites `initial`; every stream's window
//! shifts by the delta in O(1) and may become negative, exactly as §6.9.2 requires.
//!
//! The §6.9.2 requirement that a settings change must not push any stream window above
//! 2^31-1 needs the maximum offset over live streams; the actor can track that cheaply (or
//! re-check lazily in [`StreamSendWindow::available`]'s callers). [`StreamSendWindow::check`]
//! performs the check for one stream.

use crate::error::{H2Error, Reason};
use crate::settings::MAX_WINDOW;

/// Marker for window arithmetic failure; convert with [`FlowControlError::into_error`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlowControlError(&'static str);

impl FlowControlError {
    /// Map to a connection error for stream 0, or a stream error otherwise (both FLOW_CONTROL_ERROR).
    pub fn into_error(self, stream_id: u32) -> H2Error {
        if stream_id == 0 {
            H2Error::Connection(Reason::FLOW_CONTROL_ERROR, self.0)
        } else {
            H2Error::Stream {
                id: stream_id,
                reason: Reason::FLOW_CONTROL_ERROR,
            }
        }
    }
}

/// A flow-control window with `i32` semantics. It may be negative after the peer lowers
/// SETTINGS_INITIAL_WINDOW_SIZE; it can never exceed 2^31-1.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Window(i32);

impl Window {
    /// A window with the given size (must be at most 2^31-1).
    pub const fn new(size: u32) -> Window {
        assert!(size <= MAX_WINDOW);
        Window(size as i32)
    }
    /// Current value; negative means the sender must wait for the window to recover.
    pub const fn size(self) -> i32 {
        self.0
    }
    /// Bytes that may be sent now (0 when negative).
    pub const fn available(self) -> u32 {
        if self.0 < 0 { 0 } else { self.0 as u32 }
    }
    /// Apply a WINDOW_UPDATE increment; exceeding 2^31-1 is FLOW_CONTROL_ERROR.
    pub fn increase(&mut self, inc: u32) -> Result<(), FlowControlError> {
        let v = i64::from(self.0) + i64::from(inc);
        if v > i64::from(MAX_WINDOW) {
            return Err(FlowControlError("window exceeds 2^31-1"));
        }
        self.0 = v as i32;
        Ok(())
    }
    /// Consume `n` bytes; fails (leaving the window untouched) if more than is available.
    pub fn consume(&mut self, n: u32) -> Result<(), FlowControlError> {
        if i64::from(n) > i64::from(self.0) {
            return Err(FlowControlError("window exhausted"));
        }
        self.0 -= n as i32;
        Ok(())
    }
    /// Shift by a signed delta (INITIAL_WINDOW_SIZE change). Errors on overflow past 2^31-1.
    pub fn shift(&mut self, delta: i64) -> Result<(), FlowControlError> {
        let v = i64::from(self.0) + delta;
        if v > i64::from(MAX_WINDOW) || v < i64::from(i32::MIN) {
            return Err(FlowControlError(
                "window out of range after settings change",
            ));
        }
        self.0 = v as i32;
        Ok(())
    }
}

/// Per-stream *send* window stored as an offset from the connection-wide initial value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamSendWindow {
    offset: i64,
}

impl StreamSendWindow {
    /// A fresh stream: window equals the shared initial value.
    pub const fn new() -> Self {
        StreamSendWindow { offset: 0 }
    }
    /// Effective window given the shared initial value (may be negative).
    pub fn size(&self, initial: u32) -> i64 {
        i64::from(initial) + self.offset
    }
    /// Bytes sendable now.
    pub fn available(&self, initial: u32) -> u32 {
        self.size(initial).clamp(0, i64::from(MAX_WINDOW)) as u32
    }
    /// Raw offset (credited minus sent) for bookkeeping such as max-offset tracking.
    pub fn offset(&self) -> i64 {
        self.offset
    }
    /// Apply WINDOW_UPDATE; the effective window must stay <= 2^31-1.
    pub fn increase(&mut self, inc: u32, initial: u32) -> Result<(), FlowControlError> {
        if self.size(initial) + i64::from(inc) > i64::from(MAX_WINDOW) {
            return Err(FlowControlError("stream window exceeds 2^31-1"));
        }
        self.offset += i64::from(inc);
        Ok(())
    }
    /// Record `n` DATA bytes sent; fails if more than the window allows.
    pub fn consume(&mut self, n: u32, initial: u32) -> Result<(), FlowControlError> {
        if i64::from(n) > self.size(initial) {
            return Err(FlowControlError("stream window exhausted"));
        }
        self.offset -= i64::from(n);
        Ok(())
    }
    /// Verify the window is representable under `initial` (after a SETTINGS change).
    pub fn check(&self, initial: u32) -> Result<(), FlowControlError> {
        if self.size(initial) > i64::from(MAX_WINDOW) {
            Err(FlowControlError("settings change overflows stream window"))
        } else {
            Ok(())
        }
    }
}

/// When to announce freed receive window to the peer.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WindowUpdatePolicy {
    /// Emit a WINDOW_UPDATE once `released >= threshold_ratio * target`. Default 0.25.
    pub threshold_ratio: f32,
}

impl Default for WindowUpdatePolicy {
    fn default() -> Self {
        WindowUpdatePolicy {
            threshold_ratio: 0.25,
        }
    }
}

impl WindowUpdatePolicy {
    /// Whether `released` bytes freed against a `target` window warrant an update.
    pub fn should_update(&self, target: u32, released: u32) -> bool {
        released > 0 && (released as f32) >= (target as f32) * self.threshold_ratio
    }
}

/// Receive-side window tracking for one stream or the connection.
///
/// `on_data` is called when DATA arrives (peer's view shrinks), `release` when the application
/// has consumed bytes (for the connection window, the actor may call `release` immediately on
/// receipt), and `poll_update` yields the increment to put in a WINDOW_UPDATE.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecvWindow {
    target: u32,
    /// What the peer believes it may still send.
    peer_view: i32,
    /// Freed but not yet announced.
    pending: u32,
}

impl RecvWindow {
    /// New window of `target` bytes, fully open.
    pub const fn new(target: u32) -> Self {
        assert!(target <= MAX_WINDOW);
        RecvWindow {
            target,
            peer_view: target as i32,
            pending: 0,
        }
    }
    /// Configured size the window is replenished towards.
    pub fn target(&self) -> u32 {
        self.target
    }
    /// Bytes the peer may still send.
    pub fn peer_view(&self) -> i32 {
        self.peer_view
    }
    /// Account an incoming flow-controlled frame of `len` bytes (padding included).
    /// More than the peer's view is FLOW_CONTROL_ERROR.
    pub fn on_data(&mut self, len: u32) -> Result<(), FlowControlError> {
        if i64::from(len) > i64::from(self.peer_view) {
            return Err(FlowControlError("peer exceeded receive window"));
        }
        self.peer_view -= len as i32;
        Ok(())
    }
    /// The application freed `n` bytes.
    pub fn release(&mut self, n: u32) {
        self.pending = self.pending.saturating_add(n);
    }
    /// If the policy says so, returns the WINDOW_UPDATE increment and updates internal state.
    pub fn poll_update(&mut self, policy: &WindowUpdatePolicy) -> Option<u32> {
        if !policy.should_update(self.target, self.pending) {
            return None;
        }
        // Never open the window beyond the target.
        let room = (i64::from(self.target) - i64::from(self.peer_view)).max(0) as u32;
        let inc = self.pending.min(room);
        self.pending -= inc;
        if inc == 0 {
            return None;
        }
        self.peer_view += inc as i32;
        Some(inc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn increase_overflow() {
        let mut w = Window::new(MAX_WINDOW - 5);
        assert!(w.increase(5).is_ok());
        assert!(w.increase(1).is_err());
        assert_eq!(w.size(), i32::MAX);
        assert_eq!(
            Window::new(MAX_WINDOW)
                .increase(1)
                .unwrap_err()
                .into_error(0),
            H2Error::Connection(Reason::FLOW_CONTROL_ERROR, "window exceeds 2^31-1")
        );
        assert_eq!(
            FlowControlError("x").into_error(3),
            H2Error::Stream {
                id: 3,
                reason: Reason::FLOW_CONTROL_ERROR
            }
        );
    }

    #[test]
    fn consume_and_negative() {
        let mut w = Window::new(100);
        w.consume(60).unwrap();
        assert!(w.consume(41).is_err());
        assert_eq!(w.available(), 40);
        w.shift(-100).unwrap();
        assert_eq!(w.size(), -60);
        assert_eq!(w.available(), 0);
        assert!(w.consume(1).is_err());
        w.increase(61).unwrap();
        assert_eq!(w.available(), 1);
    }

    #[test]
    fn stream_window_offsets() {
        let mut s = StreamSendWindow::new();
        s.consume(1000, 65_535).unwrap();
        // initial drops to 500: window = 500 - 1000 = -500
        assert_eq!(s.size(500), -500);
        assert_eq!(s.available(500), 0);
        assert!(s.consume(1, 500).is_err());
        s.increase(600, 500).unwrap();
        assert_eq!(s.available(500), 100);
        // initial grows to max: window would exceed? -400 + ... fine
        assert!(s.check(MAX_WINDOW).is_ok());
        let mut t = StreamSendWindow::new();
        t.increase(10, 100).unwrap();
        assert!(t.check(MAX_WINDOW).is_err());
        assert!(t.increase(MAX_WINDOW, 100).is_err());
    }

    #[test]
    fn policy_threshold() {
        let p = WindowUpdatePolicy::default();
        assert!(!p.should_update(1000, 249));
        assert!(p.should_update(1000, 250));
        assert!(!p.should_update(1000, 0));
    }

    #[test]
    fn recv_window_cycle() {
        let p = WindowUpdatePolicy::default();
        let mut r = RecvWindow::new(1000);
        r.on_data(200).unwrap();
        r.release(200);
        assert_eq!(r.poll_update(&p), None);
        r.on_data(100).unwrap();
        r.release(100);
        assert_eq!(r.poll_update(&p), Some(300));
        assert_eq!(r.peer_view(), 1000);
        assert!(r.on_data(1001).is_err());
    }
}

//! SETTINGS parameters (RFC 9113 §6.5.2, RFC 8441, RFC 9218).

use crate::error::{H2Error, Reason};

/// SETTINGS_HEADER_TABLE_SIZE.
pub const HEADER_TABLE_SIZE: u16 = 0x1;
/// SETTINGS_ENABLE_PUSH.
pub const ENABLE_PUSH: u16 = 0x2;
/// SETTINGS_MAX_CONCURRENT_STREAMS.
pub const MAX_CONCURRENT_STREAMS: u16 = 0x3;
/// SETTINGS_INITIAL_WINDOW_SIZE.
pub const INITIAL_WINDOW_SIZE: u16 = 0x4;
/// SETTINGS_MAX_FRAME_SIZE.
pub const MAX_FRAME_SIZE: u16 = 0x5;
/// SETTINGS_MAX_HEADER_LIST_SIZE.
pub const MAX_HEADER_LIST_SIZE: u16 = 0x6;
/// SETTINGS_ENABLE_CONNECT_PROTOCOL (RFC 8441).
pub const ENABLE_CONNECT_PROTOCOL: u16 = 0x8;
/// SETTINGS_NO_RFC7540_PRIORITIES (RFC 9218).
pub const NO_RFC7540_PRIORITIES: u16 = 0x9;

/// Largest legal flow-control window, 2^31 - 1.
pub const MAX_WINDOW: u32 = 0x7fff_ffff;
/// Smallest legal and default MAX_FRAME_SIZE.
pub const MIN_MAX_FRAME_SIZE: u32 = 16_384;
/// Largest legal MAX_FRAME_SIZE, 2^24 - 1.
pub const MAX_MAX_FRAME_SIZE: u32 = 16_777_215;

/// The set of SETTINGS values in effect for one direction of a connection.
///
/// `Settings::default()` holds the RFC 9113 initial values (what a peer is assumed to use until
/// it says otherwise). [`Settings::client_default`] holds Matier's recommended advertisement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    /// HPACK dynamic table size limit (default 4096).
    pub header_table_size: u32,
    /// Server push permitted (default true; Matier advertises false).
    pub enable_push: bool,
    /// Concurrent stream limit (default: unlimited).
    pub max_concurrent_streams: Option<u32>,
    /// Initial stream flow-control window (default 65 535).
    pub initial_window_size: u32,
    /// Largest frame payload the sender of these settings will accept (default 16 384).
    pub max_frame_size: u32,
    /// Advisory header list limit (default: unlimited).
    pub max_header_list_size: Option<u32>,
    /// Extended CONNECT permitted (RFC 8441, default false).
    pub enable_connect_protocol: bool,
    /// RFC 7540 priority signals disabled (RFC 9218, default false).
    pub no_rfc7540_priorities: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            header_table_size: 4096,
            enable_push: true,
            max_concurrent_streams: None,
            initial_window_size: 65_535,
            max_frame_size: MIN_MAX_FRAME_SIZE,
            max_header_list_size: None,
            enable_connect_protocol: false,
            no_rfc7540_priorities: false,
        }
    }
}

/// What a single applied setting changed, so the caller can react (resize HPACK table, adjust windows).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    /// Unknown identifier (ignored) or a value that did not change anything the caller must act on.
    None,
    /// HEADER_TABLE_SIZE set; the *sender of the SETTINGS* limits the *receiver's* encoder.
    HeaderTableSize(u32),
    /// INITIAL_WINDOW_SIZE changed from `old` to `new`; stream send windows shift by `new - old`.
    InitialWindowSize {
        /// Previous value.
        old: u32,
        /// New value.
        new: u32,
    },
    /// MAX_FRAME_SIZE set.
    MaxFrameSize(u32),
}

/// Aggregated changes after applying a whole SETTINGS frame.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Changes {
    /// Last HEADER_TABLE_SIZE value seen in the frame.
    pub header_table_size: Option<u32>,
    /// (old, new) INITIAL_WINDOW_SIZE if it changed across the frame.
    pub initial_window_size: Option<(u32, u32)>,
    /// MAX_FRAME_SIZE if present.
    pub max_frame_size: Option<u32>,
}

impl Settings {
    /// Matier's recommended client advertisement: ENABLE_PUSH=0, INITIAL_WINDOW_SIZE=2 MiB,
    /// MAX_HEADER_LIST_SIZE=64 KiB (advisory), NO_RFC7540_PRIORITIES=1, HEADER_TABLE_SIZE=4096.
    pub fn client_default() -> Self {
        Settings {
            header_table_size: 4096,
            enable_push: false,
            max_concurrent_streams: None,
            initial_window_size: 2 * 1024 * 1024,
            max_frame_size: MIN_MAX_FRAME_SIZE,
            max_header_list_size: Some(64 * 1024),
            enable_connect_protocol: false,
            no_rfc7540_priorities: true,
        }
    }

    /// Validate and apply one `(identifier, value)` pair (§6.5.2). Unknown identifiers are ignored.
    ///
    /// On error nothing is modified. Errors are connection errors:
    /// ENABLE_PUSH / ENABLE_CONNECT_PROTOCOL / NO_RFC7540_PRIORITIES not 0 or 1 and
    /// MAX_FRAME_SIZE outside 16384..=16777215 are PROTOCOL_ERROR; INITIAL_WINDOW_SIZE above
    /// 2^31-1 is FLOW_CONTROL_ERROR.
    pub fn apply(&mut self, id: u16, value: u32) -> Result<Change, H2Error> {
        let bool_of = |v: u32, what: &'static str| match v {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(H2Error::Connection(Reason::PROTOCOL_ERROR, what)),
        };
        Ok(match id {
            HEADER_TABLE_SIZE => {
                self.header_table_size = value;
                Change::HeaderTableSize(value)
            }
            ENABLE_PUSH => {
                self.enable_push = bool_of(value, "invalid SETTINGS_ENABLE_PUSH")?;
                Change::None
            }
            MAX_CONCURRENT_STREAMS => {
                self.max_concurrent_streams = Some(value);
                Change::None
            }
            INITIAL_WINDOW_SIZE => {
                if value > MAX_WINDOW {
                    return Err(H2Error::Connection(
                        Reason::FLOW_CONTROL_ERROR,
                        "SETTINGS_INITIAL_WINDOW_SIZE above 2^31-1",
                    ));
                }
                let old = core::mem::replace(&mut self.initial_window_size, value);
                Change::InitialWindowSize { old, new: value }
            }
            MAX_FRAME_SIZE => {
                if !(MIN_MAX_FRAME_SIZE..=MAX_MAX_FRAME_SIZE).contains(&value) {
                    return Err(H2Error::Connection(
                        Reason::PROTOCOL_ERROR,
                        "SETTINGS_MAX_FRAME_SIZE out of range",
                    ));
                }
                self.max_frame_size = value;
                Change::MaxFrameSize(value)
            }
            MAX_HEADER_LIST_SIZE => {
                self.max_header_list_size = Some(value);
                Change::None
            }
            ENABLE_CONNECT_PROTOCOL => {
                let v = bool_of(value, "invalid SETTINGS_ENABLE_CONNECT_PROTOCOL")?;
                // RFC 8441 §3: once enabled, a sender MUST NOT send 0 again.
                if self.enable_connect_protocol && !v {
                    return Err(H2Error::Connection(
                        Reason::PROTOCOL_ERROR,
                        "SETTINGS_ENABLE_CONNECT_PROTOCOL disabled after enabling",
                    ));
                }
                self.enable_connect_protocol = v;
                Change::None
            }
            NO_RFC7540_PRIORITIES => {
                let v = bool_of(value, "invalid SETTINGS_NO_RFC7540_PRIORITIES")?;
                // RFC 9218 §2.1 forbids changing this after the first SETTINGS; the caller tracks that.
                self.no_rfc7540_priorities = v;
                Change::None
            }
            _ => Change::None,
        })
    }

    /// Apply every pair of a SETTINGS frame in order (e.g. a `SettingsIter`), stopping at the
    /// first error. Allocation-free.
    pub fn apply_all(
        &mut self,
        params: impl IntoIterator<Item = (u16, u32)>,
    ) -> Result<Changes, H2Error> {
        let start_window = self.initial_window_size;
        let mut out = Changes::default();
        for (id, v) in params {
            match self.apply(id, v)? {
                Change::HeaderTableSize(n) => out.header_table_size = Some(n),
                Change::MaxFrameSize(n) => out.max_frame_size = Some(n),
                Change::InitialWindowSize { new, .. } if new != start_window => {
                    out.initial_window_size = Some((start_window, new));
                }
                Change::InitialWindowSize { .. } => out.initial_window_size = None,
                Change::None => {}
            }
        }
        Ok(out)
    }

    /// Parameters that differ from the RFC defaults, in identifier order, written to `out`
    /// without allocating. Returns how many were written (at most 8).
    pub fn to_params(&self, out: &mut [(u16, u32); 8]) -> usize {
        let d = Settings::default();
        let mut n = 0;
        let mut push = |id: u16, v: u32| {
            out[n] = (id, v);
            n += 1;
        };
        if self.header_table_size != d.header_table_size {
            push(HEADER_TABLE_SIZE, self.header_table_size);
        }
        if self.enable_push != d.enable_push {
            push(ENABLE_PUSH, self.enable_push as u32);
        }
        if let Some(v) = self.max_concurrent_streams {
            push(MAX_CONCURRENT_STREAMS, v);
        }
        if self.initial_window_size != d.initial_window_size {
            push(INITIAL_WINDOW_SIZE, self.initial_window_size);
        }
        if self.max_frame_size != d.max_frame_size {
            push(MAX_FRAME_SIZE, self.max_frame_size);
        }
        if let Some(v) = self.max_header_list_size {
            push(MAX_HEADER_LIST_SIZE, v);
        }
        if self.enable_connect_protocol {
            push(ENABLE_CONNECT_PROTOCOL, 1);
        }
        if self.no_rfc7540_priorities {
            push(NO_RFC7540_PRIORITIES, 1);
        }
        n
    }

    /// Encode a SETTINGS frame carrying every non-default parameter. Allocation-free given capacity.
    pub fn encode(&self, out: &mut impl bytes::BufMut) {
        let mut p = [(0u16, 0u32); 8];
        let n = self.to_params(&mut p);
        crate::frame::encode_settings(out, &p[..n]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn(r: Result<Change, H2Error>) -> Reason {
        match r {
            Err(H2Error::Connection(r, _)) => r,
            other => panic!("expected connection error, got {other:?}"),
        }
    }

    #[test]
    fn initial_window_bounds() {
        let mut s = Settings::default();
        assert_eq!(
            s.apply(INITIAL_WINDOW_SIZE, MAX_WINDOW),
            Ok(Change::InitialWindowSize {
                old: 65_535,
                new: MAX_WINDOW
            })
        );
        assert_eq!(
            conn(s.apply(INITIAL_WINDOW_SIZE, MAX_WINDOW + 1)),
            Reason::FLOW_CONTROL_ERROR
        );
        assert_eq!(s.initial_window_size, MAX_WINDOW);
    }

    #[test]
    fn max_frame_size_bounds() {
        let mut s = Settings::default();
        assert_eq!(
            conn(s.apply(MAX_FRAME_SIZE, 16_383)),
            Reason::PROTOCOL_ERROR
        );
        assert_eq!(
            conn(s.apply(MAX_FRAME_SIZE, 16_777_216)),
            Reason::PROTOCOL_ERROR
        );
        assert!(s.apply(MAX_FRAME_SIZE, 16_384).is_ok());
        assert!(s.apply(MAX_FRAME_SIZE, 16_777_215).is_ok());
    }

    #[test]
    fn boolean_params() {
        let mut s = Settings::default();
        assert_eq!(conn(s.apply(ENABLE_PUSH, 2)), Reason::PROTOCOL_ERROR);
        assert_eq!(
            conn(s.apply(NO_RFC7540_PRIORITIES, 2)),
            Reason::PROTOCOL_ERROR
        );
        assert_eq!(
            conn(s.apply(ENABLE_CONNECT_PROTOCOL, 7)),
            Reason::PROTOCOL_ERROR
        );
        s.apply(ENABLE_PUSH, 0).unwrap();
        assert!(!s.enable_push);
        s.apply(ENABLE_CONNECT_PROTOCOL, 1).unwrap();
        assert_eq!(
            conn(s.apply(ENABLE_CONNECT_PROTOCOL, 0)),
            Reason::PROTOCOL_ERROR
        );
    }

    #[test]
    fn unknown_ignored() {
        let mut s = Settings::default();
        assert_eq!(s.apply(0x7f, 123), Ok(Change::None));
        assert_eq!(s, Settings::default());
    }

    #[test]
    fn apply_all_aggregates() {
        let mut s = Settings::default();
        let c = s
            .apply_all([
                (HEADER_TABLE_SIZE, 100),
                (INITIAL_WINDOW_SIZE, 1000),
                (MAX_FRAME_SIZE, 32768),
            ])
            .unwrap();
        assert_eq!(c.header_table_size, Some(100));
        assert_eq!(c.initial_window_size, Some((65_535, 1000)));
        assert_eq!(c.max_frame_size, Some(32768));
    }

    #[test]
    fn client_default_encodes() {
        let mut v = Vec::new();
        Settings::client_default().encode(&mut v);
        // 9-byte header + 4 params (push, window, header list, no-priorities)
        assert_eq!(v.len(), 9 + 4 * 6);
        assert_eq!(&v[..4], &[0, 0, 24, 4]);
    }
}

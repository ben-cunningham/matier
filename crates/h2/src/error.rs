//! Error codes (RFC 9113 §7) and the connection/stream error split (§5.4).

use core::fmt;

/// An HTTP/2 error code (RFC 9113 §7), an open newtype over `u32` so unknown codes round-trip.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Reason(pub u32);

macro_rules! reasons {
    ($($(#[$m:meta])* $name:ident = $v:expr, $s:expr;)*) => {
        impl Reason {
            $($(#[$m])* pub const $name: Reason = Reason($v);)*
            /// Canonical name of the code, if it is a registered one.
            pub fn name(self) -> Option<&'static str> {
                match self.0 { $($v => Some($s),)* _ => None }
            }
        }
    };
}

reasons! {
    /// Graceful shutdown / no error.
    NO_ERROR = 0x0, "NO_ERROR";
    /// Protocol error detected.
    PROTOCOL_ERROR = 0x1, "PROTOCOL_ERROR";
    /// Implementation fault.
    INTERNAL_ERROR = 0x2, "INTERNAL_ERROR";
    /// Flow-control protocol violated.
    FLOW_CONTROL_ERROR = 0x3, "FLOW_CONTROL_ERROR";
    /// SETTINGS not acknowledged in time.
    SETTINGS_TIMEOUT = 0x4, "SETTINGS_TIMEOUT";
    /// Frame received on a half-closed (remote) stream.
    STREAM_CLOSED = 0x5, "STREAM_CLOSED";
    /// Frame size invalid.
    FRAME_SIZE_ERROR = 0x6, "FRAME_SIZE_ERROR";
    /// Stream refused before any processing (safe to retry).
    REFUSED_STREAM = 0x7, "REFUSED_STREAM";
    /// Stream no longer needed.
    CANCEL = 0x8, "CANCEL";
    /// HPACK state could not be maintained.
    COMPRESSION_ERROR = 0x9, "COMPRESSION_ERROR";
    /// CONNECT connection reset or closed abnormally.
    CONNECT_ERROR = 0xa, "CONNECT_ERROR";
    /// Peer is generating excessive load.
    ENHANCE_YOUR_CALM = 0xb, "ENHANCE_YOUR_CALM";
    /// Transport properties inadequate.
    INADEQUATE_SECURITY = 0xc, "INADEQUATE_SECURITY";
    /// Use HTTP/1.1 for the request.
    HTTP_1_1_REQUIRED = 0xd, "HTTP_1_1_REQUIRED";
}

impl fmt::Debug for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(n) => f.write_str(n),
            None => write!(f, "Reason({:#x})", self.0),
        }
    }
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl From<u32> for Reason {
    fn from(v: u32) -> Self {
        Reason(v)
    }
}

/// A protocol error, either fatal to the connection or confined to one stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum H2Error {
    /// Connection error (§5.4.1): send GOAWAY with `reason`, then close.
    Connection(Reason, &'static str),
    /// Stream error (§5.4.2): send RST_STREAM(`reason`) on `id`; the connection survives.
    Stream {
        /// The affected stream.
        id: u32,
        /// RST_STREAM code to send.
        reason: Reason,
    },
}

impl H2Error {
    /// The error code carried by either variant.
    pub fn reason(&self) -> Reason {
        match self {
            H2Error::Connection(r, _) | H2Error::Stream { reason: r, .. } => *r,
        }
    }
    /// True for connection errors.
    pub fn is_connection(&self) -> bool {
        matches!(self, H2Error::Connection(..))
    }
}

impl fmt::Display for H2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            H2Error::Connection(r, m) => write!(f, "connection error {r}: {m}"),
            H2Error::Stream { id, reason } => write!(f, "stream {id} error {reason}"),
        }
    }
}

impl std::error::Error for H2Error {}

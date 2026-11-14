//! The single error type, with a recovery classification.

use std::error::Error as StdError;
use std::fmt;
use std::time::Duration;

type BoxError = Box<dyn StdError + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    Connect,
    Tls,
    Dns,
    Timeout,
    Protocol,
    Io,
    Canceled,
    BodyWrite,
    BodyRead,
    PoolTimeout,
    InvalidRequest,
    Closed,
}

impl ErrorKind {
    /// Stable lowercase snake_case label (OTel `error.type`).
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Tls => "tls",
            Self::Dns => "dns",
            Self::Timeout => "timeout",
            Self::Protocol => "protocol",
            Self::Io => "io",
            Self::Canceled => "canceled",
            Self::BodyWrite => "body_write",
            Self::BodyRead => "body_read",
            Self::PoolTimeout => "pool_timeout",
            Self::InvalidRequest => "invalid_request",
            Self::Closed => "closed",
        }
    }

    const fn describe(self) -> &'static str {
        match self {
            Self::Connect => "connection failed",
            Self::Tls => "TLS error",
            Self::Dns => "DNS resolution failed",
            Self::Timeout => "operation timed out",
            Self::Protocol => "protocol error",
            Self::Io => "I/O error",
            Self::Canceled => "request canceled",
            Self::BodyWrite => "failed to write body",
            Self::BodyRead => "failed to read body",
            Self::PoolTimeout => "timed out waiting for a pooled connection",
            Self::InvalidRequest => "invalid request",
            Self::Closed => "connection closed",
        }
    }
}

/// Whether and how a failed request may be retried (fetch/seatbelt `RecoveryInfo` idea).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recovery {
    Never,
    /// May have been processed; retry only idempotent requests.
    Retryable,
    /// Guaranteed not processed (GOAWAY above last_stream_id, REFUSED_STREAM, connect failure): always safe.
    Unprocessed,
    /// Server asked us to back off, optionally for this long.
    Unavailable(Option<Duration>),
}

/// One error type for the whole client. Constructing one without a source does not allocate.
pub struct Error {
    kind: ErrorKind,
    recovery: Recovery,
    source: Option<BoxError>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// No source, no allocation.
    #[must_use]
    pub const fn new(kind: ErrorKind, recovery: Recovery) -> Self {
        Self {
            kind,
            recovery,
            source: None,
        }
    }

    pub fn with_source(kind: ErrorKind, recovery: Recovery, source: impl Into<BoxError>) -> Self {
        Self {
            kind,
            recovery,
            source: Some(source.into()),
        }
    }

    /// Connect failure: the request was never sent.
    pub fn connect(source: impl Into<BoxError>) -> Self {
        Self::with_source(ErrorKind::Connect, Recovery::Unprocessed, source)
    }

    pub fn dns(source: impl Into<BoxError>) -> Self {
        Self::with_source(ErrorKind::Dns, Recovery::Unprocessed, source)
    }

    /// TLS failure that will not fix itself on retry (bad certificate, config).
    pub fn tls(source: impl Into<BoxError>) -> Self {
        Self::with_source(ErrorKind::Tls, Recovery::Never, source)
    }

    pub fn io(source: impl Into<BoxError>) -> Self {
        Self::with_source(ErrorKind::Io, Recovery::Retryable, source)
    }

    pub fn protocol(source: impl Into<BoxError>) -> Self {
        Self::with_source(ErrorKind::Protocol, Recovery::Never, source)
    }

    pub fn invalid_request(source: impl Into<BoxError>) -> Self {
        Self::with_source(ErrorKind::InvalidRequest, Recovery::Never, source)
    }

    #[must_use]
    pub const fn timeout() -> Self {
        Self::new(ErrorKind::Timeout, Recovery::Retryable)
    }

    #[must_use]
    pub const fn canceled() -> Self {
        Self::new(ErrorKind::Canceled, Recovery::Never)
    }

    #[must_use]
    pub const fn closed() -> Self {
        Self::new(ErrorKind::Closed, Recovery::Retryable)
    }

    #[must_use]
    pub const fn pool_timeout() -> Self {
        Self::new(ErrorKind::PoolTimeout, Recovery::Unprocessed)
    }

    /// Guaranteed-unprocessed failure of the given kind.
    pub fn unprocessed(kind: ErrorKind, source: impl Into<BoxError>) -> Self {
        Self::with_source(kind, Recovery::Unprocessed, source)
    }

    /// Possibly-processed failure of the given kind.
    pub fn retryable(kind: ErrorKind, source: impl Into<BoxError>) -> Self {
        Self::with_source(kind, Recovery::Retryable, source)
    }

    #[must_use]
    pub const fn unavailable(kind: ErrorKind, retry_after: Option<Duration>) -> Self {
        Self::new(kind, Recovery::Unavailable(retry_after))
    }

    /// Overrides the recovery classification.
    #[must_use]
    pub fn with_recovery(mut self, recovery: Recovery) -> Self {
        self.recovery = recovery;
        self
    }

    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    #[must_use]
    pub const fn recovery(&self) -> Recovery {
        self.recovery
    }

    /// Stable lowercase snake_case label for OTel `error.type`.
    #[must_use]
    pub const fn label(&self) -> &'static str {
        self.kind.label()
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("Error");
        d.field("kind", &self.kind)
            .field("recovery", &self.recovery);
        if let Some(s) = &self.source {
            d.field("source", s);
        }
        d.finish()
    }
}

impl fmt::Display for Error {
    /// Prints only this level; walk `source()` for the chain.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.kind.describe())?;
        if let Some(s) = &self.source {
            write!(f, ": {s}")?;
        }
        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn StdError + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_are_snake_case_and_unique() {
        use ErrorKind::*;
        let all = [
            Connect,
            Tls,
            Dns,
            Timeout,
            Protocol,
            Io,
            Canceled,
            BodyWrite,
            BodyRead,
            PoolTimeout,
            InvalidRequest,
            Closed,
        ];
        let mut seen = std::collections::HashSet::new();
        for k in all {
            let l = k.label();
            assert!(
                l.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'),
                "{l}"
            );
            assert!(seen.insert(l));
        }
        assert_eq!(Error::new(BodyRead, Recovery::Never).label(), "body_read");
    }

    #[test]
    fn recovery_and_source_chain() {
        let e = Error::connect(std::io::Error::other("refused"));
        assert_eq!(e.kind(), ErrorKind::Connect);
        assert_eq!(e.recovery(), Recovery::Unprocessed);
        assert_eq!(e.source().unwrap().to_string(), "refused");
        assert_eq!(e.to_string(), "connection failed: refused");
        assert_eq!(Error::dns("x").recovery(), Recovery::Unprocessed);
        assert_eq!(Error::tls("x").recovery(), Recovery::Never);
        assert_eq!(Error::timeout().recovery(), Recovery::Retryable);
        assert!(Error::closed().source().is_none());
        let u = Error::unprocessed(ErrorKind::Protocol, "refused stream");
        assert_eq!(
            (u.kind(), u.recovery()),
            (ErrorKind::Protocol, Recovery::Unprocessed)
        );
        let d = Duration::from_secs(1);
        assert_eq!(
            Error::unavailable(ErrorKind::Protocol, Some(d)).recovery(),
            Recovery::Unavailable(Some(d))
        );
        assert_eq!(
            Error::canceled()
                .with_recovery(Recovery::Retryable)
                .recovery(),
            Recovery::Retryable
        );
    }

    #[test]
    fn is_send_sync() {
        fn check<T: Send + Sync + 'static>() {}
        check::<Error>();
    }
}

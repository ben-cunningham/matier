//! Connection identity.

use std::fmt;
use std::sync::Arc;

use crate::error::{Error, ErrorKind, Recovery, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Scheme {
    Http,
    Https,
}

impl Scheme {
    #[must_use]
    pub const fn default_port(self) -> u16 {
        match self {
            Self::Http => 80,
            Self::Https => 443,
        }
    }
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

/// Negotiated application protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Protocol {
    Http1,
    Http2,
}

/// Connection identity. Precomputed telemetry attributes hang off the pool entry for this key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OriginKey {
    pub scheme: Scheme,
    /// Lowercased host exactly as in the URI authority: IPv6 literals keep their brackets
    /// (`[::1]`), so it can be used verbatim in a `Host` header. Use [`OriginKey::connect_host`] to dial.
    pub host: Arc<str>,
    pub port: u16,
}

impl OriginKey {
    /// Derives the origin from an absolute URI. Default ports are 80/443.
    ///
    /// # Errors
    /// `InvalidRequest` if the scheme is missing or not http/https, or the authority/host is missing.
    pub fn from_uri(uri: &http::Uri) -> Result<Self> {
        let scheme = match uri.scheme_str() {
            Some(s) if s.eq_ignore_ascii_case("http") => Scheme::Http,
            Some(s) if s.eq_ignore_ascii_case("https") => Scheme::Https,
            Some(_) => return Err(invalid("unsupported URI scheme")),
            None => return Err(invalid("URI has no scheme")),
        };
        let Some(host) = uri.host().filter(|h| !h.is_empty()) else {
            return Err(invalid("URI has no authority/host"));
        };
        let port = uri.port_u16().unwrap_or_else(|| scheme.default_port());
        let host: Arc<str> = if host.bytes().any(|b| b.is_ascii_uppercase()) {
            host.to_ascii_lowercase().into()
        } else {
            host.into()
        };
        Ok(Self { scheme, host, port })
    }

    /// Host without IPv6 brackets, suitable for DNS / `IpAddr` parsing / TLS SNI.
    #[must_use]
    pub fn connect_host(&self) -> &str {
        self.host
            .strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .unwrap_or(&self.host)
    }

    #[must_use]
    pub const fn is_tls(&self) -> bool {
        matches!(self.scheme, Scheme::Https)
    }
}

fn invalid(msg: &'static str) -> Error {
    Error::with_source(ErrorKind::InvalidRequest, Recovery::Never, msg)
}

impl fmt::Display for OriginKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}:{}", self.scheme.as_str(), self.host, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(s: &str) -> Result<OriginKey> {
        OriginKey::from_uri(&s.parse().unwrap())
    }

    #[test]
    fn defaults_and_lowercase() {
        let k = key("HTTPS://Example.COM/path?q").unwrap();
        assert_eq!(
            (k.scheme, &*k.host, k.port),
            (Scheme::Https, "example.com", 443)
        );
        let k = key("http://a.b").unwrap();
        assert_eq!((k.scheme, k.port), (Scheme::Http, 80));
        assert_eq!(key("http://a.b:8080/").unwrap().port, 8080);
        assert_eq!(key("http://a.b:80/").unwrap(), key("http://A.B/").unwrap());
    }

    #[test]
    fn ipv6() {
        let k = key("https://[::1]:8443/").unwrap();
        assert_eq!(&*k.host, "[::1]");
        assert_eq!(k.connect_host(), "::1");
        assert_eq!(k.to_string(), "https://[::1]:8443");
    }

    #[test]
    fn rejects() {
        for u in ["/relative", "example.com:80", "ftp://x/", "*"] {
            let e = u.parse::<http::Uri>().map(|u| OriginKey::from_uri(&u));
            if let Ok(r) = e {
                assert_eq!(r.unwrap_err().kind(), ErrorKind::InvalidRequest, "{u}");
            }
        }
        assert_eq!(key("/x").unwrap_err().recovery(), Recovery::Never);
    }
}

//! TLS client configuration using the platform's native TLS implementation.

use matier_core::{Error, Result};
use native_tls::{Certificate, TlsConnector, TlsConnectorBuilder};

pub(crate) const ALPN_H2: &[u8] = b"h2";

/// TLS configuration backed by SChannel on Windows, Secure Transport on macOS, and OpenSSL
/// on other platforms. ALPN is managed by the transport: the default connector offers `h2` +
/// `http/1.1`, and an HTTP/1.1-only sibling is used when `alpn_h2` is false.
#[derive(Clone, Debug)]
pub struct TlsConfig {
    pub(crate) connector: TlsConnector,
    pub(crate) connector_h1_only: TlsConnector,
}

impl TlsConfig {
    /// Builds both ALPN variants from the same caller-supplied native TLS settings.
    /// Any ALPN protocols on the builder are overwritten; all other settings are preserved.
    ///
    /// # Errors
    /// `Tls` if either native connector cannot be initialized.
    pub fn from_native_tls(mut builder: TlsConnectorBuilder) -> Result<Self> {
        let connector = builder
            .request_alpns(&["h2", "http/1.1"])
            .build()
            .map_err(Error::tls)?;
        let connector_h1_only = builder
            .request_alpns(&["http/1.1"])
            .build()
            .map_err(Error::tls)?;
        Ok(Self {
            connector,
            connector_h1_only,
        })
    }

    /// Trusts the operating system's roots and validates certificates and hostnames.
    ///
    /// # Errors
    /// `Tls` if the platform TLS implementation cannot be initialized.
    pub fn with_platform_verifier() -> Result<Self> {
        Self::from_native_tls(TlsConnector::builder())
    }

    /// Trusts exactly the certificates in `pem` (one or more `CERTIFICATE` blocks), without
    /// adding the platform's built-in roots. Certificate and hostname validation stay enabled.
    ///
    /// # Errors
    /// `Tls` if the PEM is malformed, contains no certificates, or a connector cannot be built.
    pub fn with_root_pem(pem: &[u8]) -> Result<Self> {
        let roots = Certificate::stack_from_pem(pem).map_err(Error::tls)?;
        if roots.is_empty() {
            return Err(Error::tls("no certificates found in PEM"));
        }
        let mut builder = TlsConnector::builder();
        builder.disable_built_in_roots(true);
        for root in roots {
            builder.add_root_certificate(root);
        }
        Self::from_native_tls(builder)
    }
}

//! TLS client configuration (rustls 0.23).

use std::sync::Arc;

use matier_core::{Error, Result};
use rustls::ClientConfig;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;

pub(crate) const ALPN_H2: &[u8] = b"h2";
pub(crate) const ALPN_H1: &[u8] = b"http/1.1";

/// The crypto provider selected by cargo features: `symcrypt` (if enabled) replaces rustls' own
/// backends entirely, otherwise `aws-lc-rs` wins over `ring` if both of those are enabled.
/// aws-lc-rs keeps rustls' default post-quantum hybrid key exchange (X25519MLKEM768); `ring` and
/// `symcrypt` have none. `symcrypt` is FIPS 140-3 validated but needs the native SymCrypt library
/// present at build and run time, and does not support macOS; see `rustls_symcrypt`'s crate docs.
#[must_use]
pub fn default_provider() -> Arc<CryptoProvider> {
    #[cfg(feature = "symcrypt")]
    let p = rustls_symcrypt::default_symcrypt_provider();
    #[cfg(all(feature = "aws-lc-rs", not(feature = "symcrypt")))]
    let p = rustls::crypto::aws_lc_rs::default_provider();
    #[cfg(all(
        feature = "ring",
        not(any(feature = "aws-lc-rs", feature = "symcrypt"))
    ))]
    let p = rustls::crypto::ring::default_provider();
    Arc::new(p)
}

/// TLS configuration. ALPN is managed by the transport: `rustls` offers `h2` + `http/1.1`, and an
/// http/1.1-only sibling (sharing the same session cache and verifier) is used when `alpn_h2` is false.
#[derive(Clone, Debug)]
pub struct TlsConfig {
    pub rustls: Arc<ClientConfig>,
    pub(crate) rustls_h1_only: Arc<ClientConfig>,
}

impl TlsConfig {
    /// Wraps a caller-built config; ALPN protocols on it are overwritten. Session resumption is whatever
    /// the config has (rustls' default is an in-memory cache of 256 sessions with tickets).
    #[must_use]
    pub fn from_rustls(mut cfg: ClientConfig) -> Self {
        cfg.alpn_protocols = vec![ALPN_H2.to_vec(), ALPN_H1.to_vec()];
        let mut h1 = cfg.clone(); // Arc-shared resumption store and verifier
        h1.alpn_protocols = vec![ALPN_H1.to_vec()];
        Self {
            rustls: Arc::new(cfg),
            rustls_h1_only: Arc::new(h1),
        }
    }

    /// Trusts the operating system's roots and validation (Windows cert store, macOS Keychain, Linux roots).
    ///
    /// # Errors
    /// `Tls` if the platform verifier cannot be initialised.
    pub fn with_platform_verifier() -> Result<Self> {
        let provider = default_provider();
        let verifier =
            rustls_platform_verifier::Verifier::new(Arc::clone(&provider)).map_err(Error::tls)?;
        let cfg = builder(provider)?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();
        Ok(Self::from_rustls(cfg))
    }

    /// Trusts the bundled Mozilla root set.
    #[cfg(feature = "webpki-roots")]
    #[must_use]
    pub fn with_webpki_roots() -> Self {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let cfg = builder(default_provider())
            .expect("default protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        Self::from_rustls(cfg)
    }

    /// Trusts exactly the certificates in `pem` (one or more `CERTIFICATE` blocks).
    ///
    /// # Errors
    /// `Tls` if the PEM is malformed, contains no certificates, or a certificate is not a valid trust anchor.
    pub fn with_root_pem(pem: &[u8]) -> Result<Self> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(pem) {
            roots.add(cert.map_err(Error::tls)?).map_err(Error::tls)?;
        }
        if roots.is_empty() {
            return Err(Error::tls("no certificates found in PEM"));
        }
        let cfg = builder(default_provider())?
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Self::from_rustls(cfg))
    }
}

fn builder(
    provider: Arc<CryptoProvider>,
) -> Result<rustls::ConfigBuilder<ClientConfig, rustls::WantsVerifier>> {
    ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(Error::tls)
}

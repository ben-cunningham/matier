//! `Dispatch`: request validation before the pool (scheme/authority policy).

use matier_core::{Body, Error, Result};

/// Requires an absolute `http`/`https` URI with a host.
pub(crate) fn validate(req: &http::Request<Body>) -> Result<()> {
    let uri = req.uri();
    match uri.scheme_str() {
        Some(s) if s.eq_ignore_ascii_case("http") || s.eq_ignore_ascii_case("https") => {}
        Some(_) => return Err(Error::invalid_request("unsupported URI scheme")),
        None => {
            return Err(Error::invalid_request(
                "request URI must be absolute (no scheme)",
            ));
        }
    }
    if uri.host().is_none_or(str::is_empty) {
        return Err(Error::invalid_request("request URI has no authority"));
    }
    Ok(())
}

//! Matier as the transport for Microsoft's [`fetch`] HTTP client.
//!
//! fetch keeps its request pipeline (validation, timeouts, retries, circuit breaker, telemetry); Matier sends the
//! requests over HTTP/1.1 or HTTP/2, plain or TLS. Configure connections, protocols and TLS on the
//! [`matier::Client`]: fetch's transport options (connection pool, HTTP/2 settings, supported versions, TLS) do not
//! reach this transport.
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let client = matier_fetch::builder(matier::Client::new()).build();
//! let resp = client.get("https://example.com").fetch().await?;
//! println!("status = {}", resp.status());
//! # Ok(()) }
//! ```

use std::fmt;
use std::future::Future;

use bytesbuf::BytesView;
use bytesbuf::mem::{GlobalPool, OpaqueMemory};
use fetch::custom::{CustomContext, CustomDeps, Isolation, create_builder};
use fetch::{
    HttpBody, HttpBodyBuilder, HttpClientBuilder, HttpError, HttpRequest, HttpResponse,
    RecoveryInfo,
};
use futures_util::FutureExt;
use http_body_util::BodyExt;
use http_extensions::HttpBodyOptions;
use http_extensions::timeout::BodyTimeout;
use layered::Service;
use matier::{Body, Client, Error, ErrorKind, Incoming, Recovery};
use thread_aware::{Unaware, unaware};
use tick::Clock;

/// Chunks a streaming request body may queue ahead of the connection.
const STREAM_BUFFER: usize = 4;

/// A fetch [`HttpClientBuilder`] whose requests go through `client`.
///
/// Every pool slot fetch creates shares `client` and its connection pool, so fetch's `multiple_pools` adds no
/// connections; Matier already spreads HTTP/2 load across connections. Must be called inside a tokio runtime.
pub fn builder(client: Client) -> HttpClientBuilder {
    let deps = CustomDeps {
        clock: Clock::new_tokio(),
        memory: OpaqueMemory::new(GlobalPool::new()),
        // The pool is meant to be shared by every thread, which is what `Isolation::Shared` gives it.
        extras: unaware(client),
    };
    create_builder(
        "tokio",
        "matier",
        |ctx: CustomContext<Unaware<Client>>| MatierTransport::new(ctx.extras.0, ctx.body_builder),
        Isolation::Shared,
        deps,
    )
}

/// fetch transport handler that sends requests with a [`matier::Client`].
///
/// [`builder`] installs it; use it directly with [`fetch::custom::create_builder`] for a custom setup.
pub struct MatierTransport {
    client: Client,
    body_builder: HttpBodyBuilder,
}

impl MatierTransport {
    /// Sends through `client`; response bodies are built with `body_builder` (from `CustomContext::body_builder`).
    #[must_use]
    pub fn new(client: Client, body_builder: HttpBodyBuilder) -> Self {
        Self {
            client,
            body_builder,
        }
    }
}

impl fmt::Debug for MatierTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MatierTransport").finish_non_exhaustive()
    }
}

type Response = http::Response<Incoming>;

impl Service<HttpRequest> for MatierTransport {
    type Out = fetch::Result<HttpResponse>;

    fn execute(&self, req: HttpRequest) -> impl Future<Output = Self::Out> + Send {
        let options = req
            .extensions()
            .get::<BodyTimeout>()
            .map(|t| HttpBodyOptions::default().timeout(t.duration()))
            .unwrap_or_default();
        let body_builder = &self.body_builder;
        let into_response = move |r: matier::Result<Response>| match r {
            Ok(resp) => Ok(resp.map(|b| body_builder.body(b.map_err(to_http_error), &options))),
            Err(e) => Err(to_http_error(e)),
        };
        self.client.send(into_matier(req)).map(into_response)
    }
}

/// Moves the head over as is. A buffered body is passed by reference count; a streaming one is pumped by a task.
fn into_matier(req: HttpRequest) -> http::Request<Body> {
    let (parts, body) = req.into_parts();
    let body = match body.try_clone() {
        // `try_clone` succeeds only for buffered bodies, which always convert.
        Some(buffered) => BytesView::try_from(buffered).map_or(Body::Empty, Body::full),
        None => stream_body(body),
    };
    http::Request::from_parts(parts, body)
}

fn stream_body(mut body: HttpBody) -> Body {
    let (mut tx, rx) = Body::channel(STREAM_BUFFER);
    tokio::spawn(async move {
        while let Some(frame) = body.frame().await {
            match frame {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data()
                        && tx.send(data).await.is_err()
                    {
                        return; // the request is gone
                    }
                }
                Err(e) => {
                    return tx.abort(Error::with_source(ErrorKind::BodyWrite, Recovery::Never, e));
                }
            }
        }
        tx.finish();
    });
    rx
}

fn to_http_error(e: Error) -> HttpError {
    let recovery = match e.recovery() {
        Recovery::Never => RecoveryInfo::never(),
        Recovery::Retryable | Recovery::Unprocessed => RecoveryInfo::retry(),
        Recovery::Unavailable(None) => RecoveryInfo::unavailable(),
        Recovery::Unavailable(Some(after)) => RecoveryInfo::unavailable().delay(after),
    };
    let label = e.label();
    HttpError::other(e, recovery, label)
}

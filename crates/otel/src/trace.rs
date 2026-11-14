//! [`TracingLayer`]: client spans and W3C context propagation.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use http::{HeaderName, HeaderValue, Request, Response};
use http_body::{Body, Frame, SizeHint};
use matier_core::{ConnectionInfo, Error, Service, UriTemplate};
use opentelemetry::global::{self, BoxedTracer};
use opentelemetry::propagation::Injector;
use opentelemetry::trace::{SpanKind, Status, TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue};
use pin_project_lite::pin_project;

use crate::attrs::{DEFAULT_ERROR_STATUS_THRESHOLD, proto_str};
use crate::metrics::method_str;

/// What the `url.full` span attribute carries. URLs often embed credentials in the query string (presigned
/// S3/GCS URLs, API tokens), so the default redacts the values of well-known sensitive parameters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UrlFullPolicy {
    /// The whole URL (userinfo is still removed).
    Full,
    /// The whole URL with the values of well-known sensitive query parameters replaced by `REDACTED`
    /// (`AWSAccessKeyId`, `Signature`, `sig`, `X-Goog-Signature`, `X-Amz-Signature`, `X-Amz-Credential`,
    /// `X-Amz-Security-Token`, `token`, `access_token`; names match case-insensitively).
    #[default]
    Redacted,
    /// Scheme, host, port and path only; the query is dropped.
    PathOnly,
    /// No `url.full` attribute.
    Off,
}

/// Which requests get `traceparent`/`tracestate` headers injected. Injection leaks the trace id (and, with
/// baggage propagators, application data) to whichever server the request goes to.
#[derive(Clone, Debug, Default)]
pub enum Propagation {
    /// Inject on every request (default).
    #[default]
    Everywhere,
    /// Only inject when the request host equals an entry, or ends with `.entry` (case-insensitive).
    Hosts(std::sync::Arc<[String]>),
}

impl Propagation {
    fn allows(&self, uri: &http::Uri) -> bool {
        match self {
            Self::Everywhere => true,
            Self::Hosts(list) => uri.host().is_some_and(|h| {
                let h = h.trim_matches(['[', ']']).to_ascii_lowercase();
                list.iter().any(|e| {
                    let e = e.to_ascii_lowercase();
                    h == e || h.ends_with(&format!(".{e}"))
                })
            }),
        }
    }
}

/// Request extension: do not create a span (nor inject trace headers) for this request.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoTrace;

struct HeaderInjector<'a>(&'a mut http::HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(k), Ok(v)) = (
            HeaderName::from_bytes(key.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            self.0.insert(k, v);
        }
    }
}

/// Creates a `Client` span per request, injects `traceparent`/`tracestate` using the global text-map
/// propagator and ends the span when the response body completes (or is dropped), or immediately on error.
///
/// While the inner future is polled the span's context is current, so spans created inside become children.
#[derive(Clone)]
pub struct TracingLayer<T = BoxedTracer> {
    tracer: T,
    err_threshold: u16,
    url_policy: UrlFullPolicy,
    propagation: Propagation,
}

impl Default for TracingLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl TracingLayer {
    /// Uses the global tracer named `matier`.
    #[must_use]
    pub fn new() -> Self {
        Self::with_tracer(global::tracer("matier"))
    }
}

impl<T> TracingLayer<T> {
    /// Uses an explicit tracer.
    pub fn with_tracer(tracer: T) -> Self {
        Self {
            tracer,
            err_threshold: DEFAULT_ERROR_STATUS_THRESHOLD,
            url_policy: UrlFullPolicy::default(),
            propagation: Propagation::default(),
        }
    }
    /// How `url.full` is recorded (default: [`UrlFullPolicy::Redacted`]).
    #[must_use]
    pub fn url_full(mut self, p: UrlFullPolicy) -> Self {
        self.url_policy = p;
        self
    }
    /// Restricts trace-context injection (default: every request).
    #[must_use]
    pub fn propagation(mut self, p: Propagation) -> Self {
        self.propagation = p;
        self
    }
    /// Responses with status >= `t` mark the span as errored (default 400).
    #[must_use]
    pub fn error_status_threshold(mut self, t: u16) -> Self {
        self.err_threshold = t;
        self
    }
}

impl<S, T: Clone> layered::Layer<S> for TracingLayer<T> {
    type Service = Tracing<S, T>;
    fn layer(&self, inner: S) -> Tracing<S, T> {
        Tracing {
            svc: inner,
            tracer: self.tracer.clone(),
            err_threshold: self.err_threshold,
            url_policy: self.url_policy,
            propagation: self.propagation.clone(),
        }
    }
}

/// Service produced by [`TracingLayer`].
pub struct Tracing<S, T = BoxedTracer> {
    svc: S,
    tracer: T,
    err_threshold: u16,
    url_policy: UrlFullPolicy,
    propagation: Propagation,
}

const SENSITIVE_PARAMS: &[&str] = &[
    "awsaccesskeyid",
    "signature",
    "sig",
    "x-goog-signature",
    "x-amz-signature",
    "x-amz-credential",
    "x-amz-security-token",
    "token",
    "access_token",
];

/// The `url.full` value for `policy` (`None` for [`UrlFullPolicy::Off`]). Userinfo is never included.
fn url_full(uri: &http::Uri, policy: UrlFullPolicy) -> Option<String> {
    if policy == UrlFullPolicy::Off {
        return None;
    }
    let scheme = uri.scheme_str().unwrap_or("http");
    let Some(a) = uri.authority() else {
        return Some(uri.to_string());
    };
    let port = a.port().map(|p| format!(":{p}")).unwrap_or_default();
    // `a.host()` keeps IPv6 brackets.
    let mut out = format!("{scheme}://{}{port}{}", a.host(), uri.path());
    if let (Some(q), UrlFullPolicy::Full | UrlFullPolicy::Redacted) = (uri.query(), policy) {
        out.push('?');
        for (i, pair) in q.split('&').enumerate() {
            if i > 0 {
                out.push('&');
            }
            match pair.split_once('=') {
                Some((k, _))
                    if policy == UrlFullPolicy::Redacted
                        && SENSITIVE_PARAMS.iter().any(|s| k.eq_ignore_ascii_case(s)) =>
                {
                    out.push_str(k);
                    out.push_str("=REDACTED");
                }
                _ => out.push_str(pair),
            }
        }
    }
    Some(out)
}

impl<S, T, B, RB> Service<Request<B>> for Tracing<S, T>
where
    S: Service<Request<B>, Out = Result<Response<RB>, Error>>,
    T: Tracer + Send + Sync,
    T::Span: Send + Sync + 'static,
    B: Send,
    RB: Body + Send,
{
    type Out = Result<Response<TracedBody<RB>>, Error>;

    fn execute(&self, mut req: Request<B>) -> impl Future<Output = Self::Out> + Send {
        let cx = if req.extensions().get::<NoTrace>().is_some() {
            None
        } else {
            let m = method_str(req.method());
            let name = if m == "_OTHER" {
                "HTTP".to_owned()
            } else {
                format!("HTTP {m}")
            };
            let uri = req.uri();
            let mut attrs = vec![KeyValue::new("http.request.method", m)];
            if let Some(u) = url_full(uri, self.url_policy) {
                attrs.push(KeyValue::new("url.full", u));
            }
            if let Some(h) = uri.host() {
                attrs.push(KeyValue::new(
                    "server.address",
                    h.trim_matches(['[', ']']).to_owned(),
                ));
            }
            if let Some(p) = uri.port_u16() {
                attrs.push(KeyValue::new("server.port", i64::from(p)));
            }
            if let Some(s) = uri.scheme_str() {
                attrs.push(KeyValue::new("url.scheme", s.to_owned()));
            }
            if let Some(t) = req.extensions().get::<UriTemplate>() {
                attrs.push(KeyValue::new("url.template", t.0));
            }
            let parent = Context::current();
            let span = self
                .tracer
                .span_builder(name)
                .with_kind(SpanKind::Client)
                .with_attributes(attrs)
                .start_with_context(&self.tracer, &parent);
            let cx = parent.with_span(span);
            if self.propagation.allows(req.uri()) {
                global::get_text_map_propagator(|p| {
                    p.inject_context(&cx, &mut HeaderInjector(req.headers_mut()))
                });
            }
            Some(cx)
        };
        TracingFuture {
            inner: self.svc.execute(req),
            cx,
            threshold: self.err_threshold,
        }
    }
}

pin_project! {
    /// Future of [`Tracing`].
    #[project = TracingFutureProj]
    pub struct TracingFuture<F> {
        #[pin]
        inner: F,
        cx: Option<Context>,
        threshold: u16,
    }

    impl<F> PinnedDrop for TracingFuture<F> {
        fn drop(this: Pin<&mut Self>) {
            // Dropped before a response: abandoned.
            if let Some(cx) = this.project().cx.take() {
                let s = cx.span();
                s.set_attribute(KeyValue::new("error.type", "abandoned"));
                s.set_status(Status::error("abandoned"));
                s.end();
            }
        }
    }
}

impl<F, RB> Future for TracingFuture<F>
where
    F: Future<Output = Result<Response<RB>, Error>>,
    RB: Body,
{
    type Output = Result<Response<TracedBody<RB>>, Error>;

    fn poll(self: Pin<&mut Self>, task: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let TracingFutureProj {
            inner,
            cx,
            threshold,
        } = self.project();
        let res = {
            let _g = cx.as_ref().map(|c| c.clone().attach());
            std::task::ready!(inner.poll(task))
        };
        let cx = cx.take();
        match res {
            Err(e) => {
                if let Some(cx) = &cx {
                    let s = cx.span();
                    s.set_attribute(KeyValue::new("error.type", e.label()));
                    s.set_status(Status::error(e.to_string()));
                    s.end();
                }
                Poll::Ready(Err(e))
            }
            Ok(resp) => {
                let (parts, body) = resp.into_parts();
                if let Some(cx) = &cx {
                    let s = cx.span();
                    let status = parts.status.as_u16();
                    s.set_attribute(KeyValue::new(
                        "http.response.status_code",
                        i64::from(status),
                    ));
                    let mut ev = vec![
                        KeyValue::new("http.response.status_code", i64::from(status)),
                        KeyValue::new("matier.headers.count", parts.headers.len() as i64),
                    ];
                    if let Some(ci) = parts.extensions.get::<ConnectionInfo>() {
                        let v = proto_str(ci.protocol);
                        s.set_attribute(KeyValue::new("network.protocol.version", v));
                        ev.push(KeyValue::new("network.protocol.version", v));
                        ev.push(KeyValue::new("matier.connection.reused", ci.reused));
                    }
                    if status >= *threshold {
                        s.set_attribute(KeyValue::new("error.type", status.to_string()));
                        s.set_status(Status::error(""));
                    }
                    s.add_event("matier.response.headers", ev);
                }
                Poll::Ready(Ok(Response::from_parts(
                    parts,
                    TracedBody {
                        eos: body.is_end_stream(),
                        inner: body,
                        cx,
                        bytes: 0,
                    },
                )))
            }
        }
    }
}

pin_project! {
    /// Response body that ends the span on completion (`matier.response.body` event with byte count).
    pub struct TracedBody<B> {
        #[pin]
        inner: B,
        cx: Option<Context>,
        bytes: u64,
        eos: bool,
    }

    impl<B> PinnedDrop for TracedBody<B> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            finish(this.cx, *this.bytes, *this.eos);
        }
    }
}

fn finish(cx: &mut Option<Context>, bytes: u64, complete: bool) {
    if let Some(cx) = cx.take() {
        let s = cx.span();
        s.add_event(
            "matier.response.body",
            vec![
                KeyValue::new("bytes", bytes as i64),
                KeyValue::new("matier.body.complete", complete),
            ],
        );
        s.end();
    }
}

impl<B: Body> Body for TracedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        task: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        let r = std::task::ready!(this.inner.as_mut().poll_frame(task));
        *this.eos = this.inner.is_end_stream();
        match &r {
            Some(Ok(f)) => {
                if let Some(d) = f.data_ref() {
                    *this.bytes += bytes::Buf::remaining(d) as u64;
                }
            }
            Some(Err(e)) => {
                if let Some(cx) = &*this.cx {
                    cx.span().set_status(Status::error(format!(
                        "body error: {}",
                        std::any::type_name_of_val(e)
                    )));
                }
                finish(this.cx, *this.bytes, false);
            }
            None => finish(this.cx, *this.bytes, true),
        }
        Poll::Ready(r)
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u(s: &str) -> http::Uri {
        s.parse().unwrap()
    }

    #[test]
    fn redacts_sensitive_query_values_and_strips_userinfo() {
        let uri = u(
            "https://user:pw@b.s3.amazonaws.com:8443/k?AWSAccessKeyId=AKIA&Signature=abc&x=1&X-Amz-Signature=zz&token",
        );
        let want = "https://b.s3.amazonaws.com:8443/k?AWSAccessKeyId=REDACTED&Signature=REDACTED&x=1&X-Amz-Signature=REDACTED&token";
        assert_eq!(url_full(&uri, UrlFullPolicy::Redacted).unwrap(), want);
        let r = url_full(
            &u("http://h/p?access_token=s&sig=1&TOKEN=2&keep=3"),
            UrlFullPolicy::Redacted,
        )
        .unwrap();
        assert_eq!(
            r,
            "http://h/p?access_token=REDACTED&sig=REDACTED&TOKEN=REDACTED&keep=3"
        );
    }

    #[test]
    fn policies() {
        let uri = u("http://u@h/p?sig=1&a=b");
        assert_eq!(
            url_full(&uri, UrlFullPolicy::Full).unwrap(),
            "http://h/p?sig=1&a=b"
        );
        assert_eq!(
            url_full(&uri, UrlFullPolicy::PathOnly).unwrap(),
            "http://h/p"
        );
        assert!(url_full(&uri, UrlFullPolicy::Off).is_none());
    }

    #[test]
    fn propagation_allowlist() {
        let p = Propagation::Hosts(vec!["internal.example".to_owned()].into());
        assert!(p.allows(&u("https://internal.example/x")));
        assert!(p.allows(&u("https://api.Internal.example/x")));
        assert!(!p.allows(&u("https://evil-internal.example/x")));
        assert!(!p.allows(&u("https://other.com/x")));
        assert!(Propagation::Everywhere.allows(&u("https://other.com/x")));
    }
}

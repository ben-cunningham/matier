//! [`MetricsLayer`] / [`Metrics`] service and [`MeteredBody`].

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use http::{Method, Request, Response};
use http_body::{Body, Frame, SizeHint};
use matier_core::{ConnectionInfo, Error, Scheme, Service, UriTemplate};
use opentelemetry::metrics::{Histogram, Meter, UpDownCounter};
use opentelemetry::{Key, KeyValue, global};
use pin_project_lite::pin_project;
use smallvec::SmallVec;

use crate::attrs::*;
use crate::observer::{LATENCY_BUCKETS, SIZE_BUCKETS, hist_f64, hist_u64};

pub(crate) fn method_str(m: &Method) -> &'static str {
    match *m {
        Method::GET => "GET",
        Method::HEAD => "HEAD",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::DELETE => "DELETE",
        Method::CONNECT => "CONNECT",
        Method::OPTIONS => "OPTIONS",
        Method::TRACE => "TRACE",
        Method::PATCH => "PATCH",
        _ => "_OTHER",
    }
}

pub(crate) fn scheme_of(uri: &http::Uri) -> Option<Scheme> {
    let s = uri.scheme_str()?;
    if s.eq_ignore_ascii_case("https") {
        Some(Scheme::Https)
    } else if s.eq_ignore_ascii_case("http") {
        Some(Scheme::Http)
    } else {
        None
    }
}

struct Inner {
    cache: Arc<OriginAttrCache>,
    duration: Histogram<f64>,
    active: UpDownCounter<i64>,
    req_size: Histogram<u64>,
    resp_size: Histogram<u64>,
    guard: CardinalityGuard,
    enrich: Enrichers,
    err_threshold: u16,
}

/// Builder for [`MetricsLayer`].
pub struct MetricsLayerBuilder {
    meter: Option<Meter>,
    cache: Option<Arc<OriginAttrCache>>,
    guard: CardinalityGuard,
    enrich: Enrichers,
    err_threshold: u16,
}

impl MetricsLayerBuilder {
    /// Meter to create instruments on (default: `global::meter("matier")`).
    #[must_use]
    pub fn meter(mut self, meter: &Meter) -> Self {
        self.meter = Some(meter.clone());
        self
    }
    /// Shares an origin attribute cache (e.g. with `OtelObserver::with_cache`).
    #[must_use]
    pub fn cache(mut self, cache: Arc<OriginAttrCache>) -> Self {
        self.cache = Some(cache);
        self
    }
    /// Adds a global enricher run on every request's `Parts`; pushed attributes apply to all request-scoped metrics.
    #[must_use]
    pub fn enrich_request(
        mut self,
        f: impl Fn(&http::request::Parts, &mut Vec<KeyValue>) + Send + Sync + 'static,
    ) -> Self {
        self.enrich.req.push(Box::new(f));
        self
    }
    /// Adds a global enricher run on every response's `Parts`; attributes apply to duration and response-body metrics.
    #[must_use]
    pub fn enrich_response(
        mut self,
        f: impl Fn(&http::response::Parts, &mut Vec<KeyValue>) + Send + Sync + 'static,
    ) -> Self {
        self.enrich.resp.push(Box::new(f));
        self
    }
    /// Cardinality guard: only these extra keys (from `TelemetryAttributes` and enrichers) are kept.
    #[must_use]
    pub fn allowed_keys<K: Into<Key>>(mut self, keys: impl IntoIterator<Item = K>) -> Self {
        self.guard.allow = Some(keys.into_iter().map(Into::into).collect());
        self
    }
    /// Cardinality guard: at most `n` distinct values per extra key; later values become `"_other"`.
    #[must_use]
    pub fn max_values_per_key(mut self, n: usize) -> Self {
        self.guard.max_values = Some(n);
        self
    }
    /// Responses with status >= `t` get `error.type` set to the status code (default 400, per semconv for clients).
    #[must_use]
    pub fn error_status_threshold(mut self, t: u16) -> Self {
        self.err_threshold = t;
        self
    }
    /// Builds the layer.
    #[must_use]
    pub fn build(self) -> MetricsLayer {
        let m = self.meter.unwrap_or_else(|| global::meter("matier"));
        MetricsLayer {
            inner: Arc::new(Inner {
                cache: self.cache.unwrap_or_default(),
                duration: hist_f64(
                    &m,
                    "http.client.request.duration",
                    "s",
                    "Duration of HTTP client requests",
                    LATENCY_BUCKETS,
                ),
                active: m
                    .i64_up_down_counter("http.client.active_requests")
                    .with_unit("{request}")
                    .with_description("In-flight HTTP client requests")
                    .build(),
                req_size: hist_u64(
                    &m,
                    "http.client.request.body.size",
                    "By",
                    "Size of request bodies (when known)",
                    SIZE_BUCKETS,
                ),
                resp_size: hist_u64(
                    &m,
                    "http.client.response.body.size",
                    "By",
                    "Size of response bodies as consumed",
                    SIZE_BUCKETS,
                ),
                guard: self.guard,
                enrich: self.enrich,
                err_threshold: self.err_threshold,
            }),
        }
    }
}

/// Layer recording request-scoped HTTP client metrics. See the crate docs.
#[derive(Clone)]
pub struct MetricsLayer {
    inner: Arc<Inner>,
}

impl Default for MetricsLayer {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl MetricsLayer {
    /// Layer with default configuration on the global meter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Starts a builder.
    #[must_use]
    pub fn builder() -> MetricsLayerBuilder {
        MetricsLayerBuilder {
            meter: None,
            cache: None,
            guard: CardinalityGuard::default(),
            enrich: Enrichers {
                req: Vec::new(),
                resp: Vec::new(),
            },
            err_threshold: DEFAULT_ERROR_STATUS_THRESHOLD,
        }
    }
    /// Origin attribute cache used by this layer.
    #[must_use]
    pub fn cache(&self) -> &Arc<OriginAttrCache> {
        &self.inner.cache
    }
}

impl<S> layered::Layer<S> for MetricsLayer {
    type Service = Metrics<S>;
    fn layer(&self, inner: S) -> Metrics<S> {
        Metrics {
            svc: inner,
            shared: self.inner.clone(),
        }
    }
}

/// Service produced by [`MetricsLayer`].
pub struct Metrics<S> {
    svc: S,
    shared: Arc<Inner>,
}

impl<S: Clone> Clone for Metrics<S> {
    fn clone(&self) -> Self {
        Self {
            svc: self.svc.clone(),
            shared: self.shared.clone(),
        }
    }
}

/// Per-request state, boxed so the future stays small.
struct ReqState {
    attrs: Attrs,
    req_len: Option<u64>,
}

impl Inner {
    fn extras(
        &self,
        ext: &http::Extensions,
        out: &mut SmallVec<[KeyValue; 4]>,
        enrich: impl FnOnce(&mut Vec<KeyValue>),
    ) {
        if let Some(t) = ext.get::<TelemetryAttributes>() {
            out.extend(t.iter().cloned());
        }
        let mut v = Vec::new();
        enrich(&mut v);
        out.extend(v);
        if !self.guard.is_noop() {
            self.guard.apply(out);
        }
    }

    fn finish(&self, st: &mut ReqState, start: Instant, tail: &[KeyValue]) {
        self.active.add(-1, &st.attrs);
        st.attrs.extend(tail.iter().cloned());
        self.duration
            .record(start.elapsed().as_secs_f64(), &st.attrs);
        if let Some(n) = st.req_len {
            self.req_size.record(n, &st.attrs);
        }
    }
}

impl<S> Metrics<S> {
    fn begin<B: Body>(&self, req: Request<B>) -> (Request<B>, Box<ReqState>) {
        let sh = &*self.shared;
        let (parts, body) = req.into_parts();
        let req_len = body.size_hint().exact();
        let mut attrs = Attrs::new();
        match scheme_of(&parts.uri).zip(parts.uri.host()) {
            Some((scheme, host)) => {
                let port = parts.uri.port_u16().unwrap_or(scheme.default_port());
                sh.cache
                    .with(scheme, host, port, |a| attrs.extend(a.base.iter().cloned()));
            }
            None => attrs.push(KeyValue::new(
                K_SCHEME,
                parts.uri.scheme_str().unwrap_or("unknown").to_owned(),
            )),
        }
        attrs.push(KeyValue::new(K_METHOD, method_str(&parts.method)));
        if let Some(t) = parts.extensions.get::<UriTemplate>() {
            attrs.push(KeyValue::new(K_TEMPLATE, t.0));
        }
        let mut extras = SmallVec::new();
        sh.extras(&parts.extensions, &mut extras, |v| {
            for f in &sh.enrich.req {
                f(&parts, v);
            }
        });
        attrs.extend(extras);
        sh.active.add(1, &attrs);
        (
            Request::from_parts(parts, body),
            Box::new(ReqState { attrs, req_len }),
        )
    }
}

impl<S, B, RB> Service<Request<B>> for Metrics<S>
where
    S: Service<Request<B>, Out = Result<Response<RB>, Error>>,
    B: Body + Send,
    RB: Body + Send,
{
    type Out = Result<Response<MeteredBody<RB>>, Error>;

    fn execute(&self, req: Request<B>) -> impl Future<Output = Self::Out> + Send {
        let (req, state) = self.begin(req);
        MetricsFuture {
            inner: self.svc.execute(req),
            state: Some(state),
            shared: self.shared.clone(),
            start: Instant::now(),
            _rb: PhantomData,
        }
    }
}

pin_project! {
    /// Future of [`Metrics`]: the inner future plus a few words (the per-request state is boxed).
    #[project = MetricsFutureProj]
    pub struct MetricsFuture<F, RB> {
        #[pin]
        inner: F,
        state: Option<Box<ReqState>>,
        shared: Arc<Inner>,
        start: Instant,
        _rb: PhantomData<fn() -> RB>,
    }

    impl<F, RB> PinnedDrop for MetricsFuture<F, RB> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let Some(mut st) = this.state.take() {
                this.shared.finish(&mut st, *this.start, &[KeyValue::new(K_ERROR, "abandoned")]);
            }
        }
    }
}

impl<F, RB> Future for MetricsFuture<F, RB>
where
    F: Future<Output = Result<Response<RB>, Error>>,
    RB: Body,
{
    type Output = Result<Response<MeteredBody<RB>>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let MetricsFutureProj {
            inner,
            state,
            shared,
            start,
            ..
        } = self.project();
        let res = std::task::ready!(inner.poll(cx));
        let mut st = state.take().expect("polled after completion");
        match res {
            Err(e) => {
                shared.finish(&mut st, *start, &[KeyValue::new(K_ERROR, e.label())]);
                Poll::Ready(Err(e))
            }
            Ok(resp) => {
                let (parts, body) = resp.into_parts();
                let mut tail: SmallVec<[KeyValue; 8]> = SmallVec::new();
                let status = parts.status.as_u16();
                tail.push(KeyValue::new(K_STATUS, i64::from(status)));
                if let Some(ci) = parts.extensions.get::<ConnectionInfo>() {
                    tail.push(KeyValue::new(K_PROTO, proto_str(ci.protocol)));
                }
                if status >= shared.err_threshold {
                    tail.push(KeyValue::new(K_ERROR, status.to_string()));
                }
                let mut extras = SmallVec::new();
                shared.extras(&parts.extensions, &mut extras, |v| {
                    for f in &shared.enrich.resp {
                        f(&parts, v);
                    }
                });
                tail.extend(extras);
                shared.finish(&mut st, *start, &tail);
                let body = MeteredBody {
                    eos: body.is_end_stream(),
                    inner: body,
                    bytes: 0,
                    rec: Some(Rec {
                        shared: shared.clone(),
                        attrs: st,
                    }),
                };
                Poll::Ready(Ok(Response::from_parts(parts, body)))
            }
        }
    }
}

struct Rec {
    shared: Arc<Inner>,
    attrs: Box<ReqState>,
}

pin_project! {
    /// Response body wrapper counting data-frame bytes. Records `http.client.response.body.size` once,
    /// at end of stream (`matier.body.complete=true`) or when dropped/errored first (`false`).
    pub struct MeteredBody<B> {
        #[pin]
        inner: B,
        bytes: u64,
        eos: bool,
        rec: Option<Rec>,
    }

    impl<B> PinnedDrop for MeteredBody<B> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            record(this.rec, *this.bytes, *this.eos);
        }
    }
}

fn record(rec: &mut Option<Rec>, bytes: u64, complete: bool) {
    if let Some(mut r) = rec.take() {
        r.attrs
            .attrs
            .push(KeyValue::new("matier.body.complete", complete));
        r.shared.resp_size.record(bytes, &r.attrs.attrs);
    }
}

impl<B> MeteredBody<B> {
    /// Bytes of data frames seen so far.
    pub fn bytes_seen(&self) -> u64 {
        self.bytes
    }
    /// Unwraps the inner body without recording (drop of `self` is bypassed only by recording first).
    pub fn get_ref(&self) -> &B {
        &self.inner
    }
}

impl<B: Body> Body for MeteredBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        let r = std::task::ready!(this.inner.as_mut().poll_frame(cx));
        *this.eos = this.inner.is_end_stream();
        match &r {
            Some(Ok(f)) => {
                if let Some(d) = f.data_ref() {
                    *this.bytes += bytes_of(d);
                }
            }
            Some(Err(_)) => record(this.rec, *this.bytes, false),
            None => record(this.rec, *this.bytes, true),
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

fn bytes_of<D: bytes::Buf>(d: &D) -> u64 {
    d.remaining() as u64
}

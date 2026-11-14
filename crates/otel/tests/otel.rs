use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use layered::{Layer, Service};
use matier_core::{
    ConnectPhase, ConnectionInfo, Error, Observer, OriginKey, Protocol, UriTemplate,
};
use matier_otel::{MetricsLayer, NoTrace, OtelObserver, TelemetryAttributes, TracingLayer};
use opentelemetry::trace::{SpanKind, TracerProvider as _};
use opentelemetry::{KeyValue, metrics::MeterProvider as _};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

type Req = Request<Full<Bytes>>;
type Resp = Response<Full<Bytes>>;

type Handler = Box<dyn Fn(&Req) -> Result<Resp, Error> + Send + Sync>;
struct Fake(Handler);

impl Service<Req> for Fake {
    type Out = Result<Resp, Error>;
    async fn execute(&self, r: Req) -> Self::Out {
        (self.0)(&r)
    }
}

struct Pending;
impl Service<Req> for Pending {
    type Out = Result<Resp, Error>;
    async fn execute(&self, _r: Req) -> Self::Out {
        std::future::pending().await
    }
}

fn ok_with(n: usize, status: u16) -> Fake {
    Fake(Box::new(move |_| {
        let mut r = Response::new(Full::new(Bytes::from(vec![0u8; n])));
        *r.status_mut() = StatusCode::from_u16(status).unwrap();
        r.extensions_mut().insert(ConnectionInfo {
            protocol: Protocol::Http2,
            reused: true,
            conn_id: 1,
        });
        Ok(r)
    }))
}

fn meter_setup() -> (SdkMeterProvider, InMemoryMetricExporter) {
    let exp = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exp.clone()).build();
    (SdkMeterProvider::builder().with_reader(reader).build(), exp)
}

#[derive(Debug, Clone)]
struct Point {
    attrs: BTreeMap<String, String>,
    sum: f64,
    count: u64,
}

fn attrs_of<'a>(it: impl Iterator<Item = &'a KeyValue>) -> BTreeMap<String, String> {
    it.map(|kv| (kv.key.to_string(), kv.value.as_str().into_owned()))
        .collect()
}

fn points(p: &SdkMeterProvider, exp: &InMemoryMetricExporter, name: &str) -> Vec<Point> {
    p.force_flush().unwrap();
    let all = exp.get_finished_metrics().unwrap();
    let Some(rm) = all.last() else { return vec![] };
    let mut out = vec![];
    for sm in rm.scope_metrics() {
        for m in sm.metrics().filter(|m| m.name() == name) {
            macro_rules! dp {
                ($d:expr) => {
                    match $d {
                        MetricData::Histogram(h) => out.extend(h.data_points().map(|d| Point {
                            attrs: attrs_of(d.attributes()),
                            sum: d.sum() as f64,
                            count: d.count(),
                        })),
                        MetricData::Sum(s) => out.extend(s.data_points().map(|d| Point {
                            attrs: attrs_of(d.attributes()),
                            sum: d.value() as f64,
                            count: 1,
                        })),
                        MetricData::Gauge(g) => out.extend(g.data_points().map(|d| Point {
                            attrs: attrs_of(d.attributes()),
                            sum: d.value() as f64,
                            count: 1,
                        })),
                        _ => {}
                    }
                };
            }
            match m.data() {
                AggregatedMetrics::F64(d) => dp!(d),
                AggregatedMetrics::U64(d) => dp!(d),
                AggregatedMetrics::I64(d) => dp!(d),
            }
        }
    }
    out
}

fn one(p: &SdkMeterProvider, e: &InMemoryMetricExporter, name: &str) -> Point {
    let v = points(p, e, name);
    assert_eq!(v.len(), 1, "{name}: {v:?}");
    v[0].clone()
}

fn get(uri: &str) -> Req {
    Request::get(uri).body(Full::new(Bytes::new())).unwrap()
}

async fn drain(resp: Response<impl http_body::Body<Error = impl std::fmt::Debug>>) -> usize {
    let b = resp.into_body().collect().await.unwrap().to_bytes();
    b.len()
}

#[tokio::test]
async fn request_metrics_success() {
    let (p, e) = meter_setup();
    let layer = MetricsLayer::builder().meter(&p.meter("t")).build();
    let svc = layer.layer(ok_with(1000, 200));
    let mut req = Request::post("https://Example.com:8443/a/b?q=1")
        .body(Full::new(Bytes::from_static(b"hello")))
        .unwrap();
    req.extensions_mut()
        .insert(TelemetryAttributes::new([KeyValue::new("tenant", "acme")]));
    req.extensions_mut().insert(UriTemplate("/a/{id}"));
    let resp = svc.execute(req).await.unwrap();

    // Response body size is not recorded until the body completes.
    assert!(points(&p, &e, "http.client.response.body.size").is_empty());
    assert_eq!(drain(resp).await, 1000);

    let d = one(&p, &e, "http.client.request.duration");
    assert_eq!(d.count, 1);
    let a = &d.attrs;
    assert_eq!(a["http.request.method"], "POST");
    assert_eq!(a["server.address"], "example.com");
    assert_eq!(a["server.port"], "8443");
    assert_eq!(a["url.scheme"], "https");
    assert_eq!(a["http.response.status_code"], "200");
    assert_eq!(a["network.protocol.version"], "2");
    assert_eq!(a["tenant"], "acme");
    assert_eq!(a["url.template"], "/a/{id}");
    assert!(!a.contains_key("error.type"));

    let act = one(&p, &e, "http.client.active_requests");
    assert_eq!(act.sum, 0.0);
    assert_eq!(act.attrs["tenant"], "acme");
    assert_eq!(one(&p, &e, "http.client.request.body.size").sum, 5.0);
    let rb = one(&p, &e, "http.client.response.body.size");
    assert_eq!(rb.sum, 1000.0);
    assert_eq!(rb.attrs["matier.body.complete"], "true");
}

#[tokio::test]
async fn active_requests_while_in_flight() {
    let (p, e) = meter_setup();
    let svc = MetricsLayer::builder()
        .meter(&p.meter("t"))
        .build()
        .layer(ok_with(1, 200));
    let fut = svc.execute(get("http://h/"));
    assert_eq!(one(&p, &e, "http.client.active_requests").sum, 1.0);
    drop(fut.await);
    assert_eq!(one(&p, &e, "http.client.active_requests").sum, 0.0);
}

#[tokio::test]
async fn error_labels_and_status() {
    let (p, e) = meter_setup();
    let layer = MetricsLayer::builder().meter(&p.meter("t")).build();
    let svc = layer.layer(Fake(Box::new(|r| {
        if r.uri().path() == "/err" {
            Err(Error::connect("boom"))
        } else if r.uri().path() == "/500" {
            Ok(Response::new(Full::new(Bytes::new())).map(|b| b)).map(|mut r: Resp| {
                *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                r
            })
        } else {
            Ok(Response::new(Full::new(Bytes::new())))
        }
    })));
    assert!(svc.execute(get("http://h/err")).await.is_err());
    let _ = svc.execute(get("http://h/500")).await.unwrap();
    let _ = svc.execute(get("http://h/ok")).await.unwrap();
    let pts = points(&p, &e, "http.client.request.duration");
    let by = |k: &str| {
        pts.iter()
            .find(|p| p.attrs.get("error.type").is_some_and(|v| v == k))
    };
    assert!(by("connect").is_some(), "{pts:?}");
    let s = by("500").unwrap();
    assert_eq!(s.attrs["http.response.status_code"], "500");
    assert!(pts.iter().any(|p| !p.attrs.contains_key("error.type")));
    assert_eq!(one(&p, &e, "http.client.active_requests").sum, 0.0);
}

#[tokio::test]
async fn abandoned_request() {
    let (p, e) = meter_setup();
    let svc = MetricsLayer::builder()
        .meter(&p.meter("t"))
        .build()
        .layer(Pending);
    let r =
        tokio::time::timeout(Duration::from_millis(20), svc.execute(get("http://h/slow"))).await;
    assert!(r.is_err());
    let d = one(&p, &e, "http.client.request.duration");
    assert_eq!(d.attrs["error.type"], "abandoned");
    assert_eq!(d.count, 1);
    assert_eq!(one(&p, &e, "http.client.active_requests").sum, 0.0);
}

#[tokio::test]
async fn body_dropped_early_is_incomplete() {
    let (p, e) = meter_setup();
    let svc = MetricsLayer::builder()
        .meter(&p.meter("t"))
        .build()
        .layer(ok_with(500, 200));
    let resp = svc.execute(get("http://h/")).await.unwrap();
    drop(resp);
    let rb = one(&p, &e, "http.client.response.body.size");
    assert_eq!(rb.attrs["matier.body.complete"], "false");
    assert_eq!(rb.sum, 0.0);
}

#[tokio::test]
async fn enrichers_and_response_attributes() {
    let (p, e) = meter_setup();
    let layer = MetricsLayer::builder()
        .meter(&p.meter("t"))
        .enrich_request(|parts, out| {
            out.push(KeyValue::new("path0", parts.uri.path().len() as i64))
        })
        .enrich_response(|parts, out| {
            out.push(KeyValue::new(
                "srv",
                parts
                    .headers
                    .get("server")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned(),
            ))
        })
        .build();
    let svc = layer.layer(Fake(Box::new(|_| {
        let mut r = Response::new(Full::new(Bytes::new()));
        r.headers_mut().insert("server", "nginx".parse().unwrap());
        r.extensions_mut()
            .insert(TelemetryAttributes::new([KeyValue::new("cache", "hit")]));
        Ok(r)
    })));
    let _ = svc.execute(get("http://h/abc")).await.unwrap();
    let d = one(&p, &e, "http.client.request.duration");
    assert_eq!(d.attrs["path0"], "4");
    assert_eq!(d.attrs["srv"], "nginx");
    assert_eq!(d.attrs["cache"], "hit");
    // request-side dimensions are on active_requests, response-side are not.
    let a = one(&p, &e, "http.client.active_requests");
    assert_eq!(a.attrs["path0"], "4");
    assert!(!a.attrs.contains_key("srv"));
}

#[tokio::test]
async fn cardinality_guard() {
    let (p, e) = meter_setup();
    let layer = MetricsLayer::builder()
        .meter(&p.meter("t"))
        .allowed_keys(["tenant"])
        .max_values_per_key(2)
        .build();
    let svc = layer.layer(ok_with(0, 200));
    for t in ["a", "b", "c", "d", "a"] {
        let mut r = get("http://h/");
        r.extensions_mut().insert(TelemetryAttributes::new([
            KeyValue::new("tenant", t),
            KeyValue::new("secret", "x"),
        ]));
        let _ = svc.execute(r).await.unwrap();
    }
    let pts = points(&p, &e, "http.client.request.duration");
    let mut vals: Vec<_> = pts
        .iter()
        .map(|p| (p.attrs["tenant"].clone(), p.count))
        .collect();
    vals.sort();
    assert_eq!(
        vals,
        vec![("_other".into(), 2), ("a".into(), 2), ("b".into(), 1)]
    );
    assert!(pts.iter().all(|p| !p.attrs.contains_key("secret")));
}

#[tokio::test]
async fn attribute_cache_reuse() {
    let (p, _e) = meter_setup();
    let layer = MetricsLayer::builder().meter(&p.meter("t")).build();
    let svc = layer.layer(ok_with(0, 200));
    for i in 0..50 {
        let _ = svc
            .execute(get(&format!("https://HOST.test/{i}")))
            .await
            .unwrap();
        let _ = svc.execute(get("https://host.test:443/")).await.unwrap();
    }
    assert_eq!(layer.cache().len(), 1);
    let _ = svc.execute(get("https://other.test/")).await.unwrap();
    let _ = svc.execute(get("http://[::1]:8080/")).await.unwrap();
    assert_eq!(layer.cache().len(), 3);
    // Shared with an observer: same origin, no growth.
    let obs = OtelObserver::with_cache(&p.meter("t"), layer.cache().clone());
    obs.request_queued(
        &OriginKey::from_uri(&"https://host.test/".parse().unwrap()).unwrap(),
        Duration::from_millis(1),
    );
    assert_eq!(obs.cache().len(), 3);
}

#[test]
fn observer_records_everything() {
    let (p, e) = meter_setup();
    let obs = OtelObserver::new(&p.meter("t"));
    assert!(obs.enabled());
    let o = OriginKey::from_uri(&"https://svc.test:8443/".parse().unwrap()).unwrap();
    let ms = Duration::from_millis(2);
    obs.request_queued(&o, ms);
    obs.request_dispatched(&o, ms);
    obs.connect_phase(&o, ConnectPhase::Tls, ms, true);
    obs.connect_phase(&o, ConnectPhase::Dns, ms, false);
    obs.connection_opened(&o, Protocol::Http2);
    obs.connection_opened(&o, Protocol::Http2);
    obs.connection_closed(&o, Protocol::Http2, Duration::from_secs(3));
    obs.write_batch(&o, 4096, 7);
    obs.bytes_read(&o, 100);
    obs.actor_tick(&o, Duration::from_micros(30), 5);
    obs.flow_control_stall(&o, ms);
    obs.h2_goaway(&o, 0xb);
    obs.h2_rst_stream(&o, 0x8, true);
    obs.pool_waiters(&o, 3);
    obs.pool_scaled(&o, 4, matier_core::ScaleReason::Saturated);

    let q = one(&p, &e, "http.client.request.time_in_queue");
    assert_eq!(
        (
            q.count,
            q.attrs["server.address"].as_str(),
            q.attrs["server.port"].as_str(),
            q.attrs["url.scheme"].as_str()
        ),
        (1, "svc.test", "8443", "https")
    );
    one(&p, &e, "matier.client.request.dispatch_delay");
    let setup = points(&p, &e, "matier.client.connection.setup.duration");
    assert_eq!(setup.len(), 2);
    assert!(
        setup
            .iter()
            .any(|s| s.attrs["matier.connect.phase"] == "tls" && s.attrs["outcome"] == "success")
    );
    assert!(
        setup
            .iter()
            .any(|s| s.attrs["matier.connect.phase"] == "dns" && s.attrs["outcome"] == "failure")
    );
    let oc = one(&p, &e, "http.client.open_connections");
    assert_eq!(
        (oc.sum, oc.attrs["network.protocol.version"].as_str()),
        (1.0, "2")
    );
    assert_eq!(one(&p, &e, "http.client.connection.duration").sum, 3.0);
    assert_eq!(
        one(&p, &e, "matier.transport.write.batch.bytes").sum,
        4096.0
    );
    assert_eq!(one(&p, &e, "matier.transport.write.batch.iovecs").sum, 7.0);
    let bytes = points(&p, &e, "matier.transport.bytes");
    let dir = |d: &str| {
        bytes
            .iter()
            .find(|b| b.attrs["network.io.direction"] == d)
            .unwrap()
            .sum
    };
    assert_eq!((dir("transmit"), dir("receive")), (4096.0, 100.0));
    assert!(one(&p, &e, "matier.h2.actor.busy").sum > 0.0);
    assert_eq!(one(&p, &e, "matier.h2.actor.queue_depth").sum, 5.0);
    one(&p, &e, "matier.h2.flow_control.stall_duration");
    assert_eq!(
        one(&p, &e, "matier.h2.goaway").attrs["matier.h2.error_code"],
        "ENHANCE_YOUR_CALM"
    );
    let r = one(&p, &e, "matier.h2.rst_stream");
    assert_eq!(
        (
            r.attrs["matier.h2.error_code"].as_str(),
            r.attrs["matier.h2.direction"].as_str()
        ),
        ("CANCEL", "sent")
    );
    assert_eq!(one(&p, &e, "matier.pool.waiters").sum, 3.0);
    assert_eq!(one(&p, &e, "matier.pool.connections").sum, 4.0);
    let se = one(&p, &e, "matier.pool.scale_events");
    assert_eq!(
        (se.sum, se.attrs["matier.pool.scale_reason"].as_str()),
        (1.0, "saturated")
    );
    assert_eq!(obs.cache().len(), 1);
}

fn tracer_setup() -> (SdkTracerProvider, InMemorySpanExporter) {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let exp = InMemorySpanExporter::default();
    (
        SdkTracerProvider::builder()
            .with_simple_exporter(exp.clone())
            .build(),
        exp,
    )
}

#[tokio::test]
async fn tracing_span_and_traceparent() {
    let (tp, exp) = tracer_setup();
    let layer = TracingLayer::with_tracer(tp.tracer("t"));
    let seen = Arc::new(std::sync::Mutex::new(None));
    let s2 = seen.clone();
    let svc = layer.layer(Fake(Box::new(move |r| {
        *s2.lock().unwrap() = r
            .headers()
            .get("traceparent")
            .map(|v| v.to_str().unwrap().to_owned());
        Ok(Response::new(Full::new(Bytes::from_static(b"abc"))))
    })));
    let resp = svc
        .execute(get("https://user:pw@h.test/p?x=1"))
        .await
        .unwrap();
    assert!(
        exp.get_finished_spans().unwrap().is_empty(),
        "span ends with the body"
    );
    assert_eq!(drain(resp).await, 3);
    let tp_hdr = seen.lock().unwrap().clone().expect("traceparent injected");
    let spans = exp.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 1);
    let s = &spans[0];
    assert_eq!(s.name, "HTTP GET");
    assert_eq!(s.span_kind, SpanKind::Client);
    assert!(tp_hdr.contains(&s.span_context.trace_id().to_string()));
    assert!(tp_hdr.contains(&s.span_context.span_id().to_string()));
    let a = attrs_of(s.attributes.iter());
    assert_eq!(a["server.address"], "h.test");
    assert_eq!(a["url.full"], "https://h.test/p?x=1", "userinfo redacted");
    assert_eq!(a["http.response.status_code"], "200");
    let names: Vec<_> = s.events.iter().map(|e| e.name.to_string()).collect();
    assert_eq!(names, ["matier.response.headers", "matier.response.body"]);
}

#[tokio::test]
async fn tracing_error_5xx_notrace() {
    let (tp, exp) = tracer_setup();
    let layer = TracingLayer::with_tracer(tp.tracer("t"));
    let svc = layer.layer(Fake(Box::new(|r| {
        assert_eq!(
            r.headers().contains_key("traceparent"),
            r.uri().path() != "/quiet"
        );
        match r.uri().path() {
            "/err" => Err(Error::connect("x")),
            "/503" => {
                let mut r = Response::new(Full::new(Bytes::new()));
                *r.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
                Ok(r)
            }
            _ => Ok(Response::new(Full::new(Bytes::new()))),
        }
    })));
    assert!(svc.execute(get("http://h/err")).await.is_err());
    drain(svc.execute(get("http://h/503")).await.unwrap()).await;
    let mut q = get("http://h/quiet");
    q.extensions_mut().insert(NoTrace);
    drain(svc.execute(q).await.unwrap()).await;
    let spans = exp.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 2);
    for s in &spans {
        assert!(matches!(
            s.status,
            opentelemetry::trace::Status::Error { .. }
        ));
    }
    assert_eq!(
        attrs_of(spans[0].attributes.iter())["error.type"],
        "connect"
    );
    assert_eq!(attrs_of(spans[1].attributes.iter())["error.type"], "503");

    // Dropped before response: abandoned.
    exp.reset();
    let svc = layer.layer(Pending);
    let _ = tokio::time::timeout(Duration::from_millis(10), svc.execute(get("http://h/"))).await;
    let spans = exp.get_finished_spans().unwrap();
    assert_eq!(
        attrs_of(spans[0].attributes.iter())["error.type"],
        "abandoned"
    );
}

#[tokio::test]
async fn future_size_overhead() {
    let (p, _e) = meter_setup();
    let (tp, _x) = tracer_setup();
    let inner = ok_with(10, 200);
    let m = MetricsLayer::builder()
        .meter(&p.meter("t"))
        .build()
        .layer(ok_with(10, 200));
    let t = TracingLayer::with_tracer(tp.tracer("t")).layer(ok_with(10, 200));
    let both = TracingLayer::with_tracer(tp.tracer("t")).layer(
        MetricsLayer::builder()
            .meter(&p.meter("t"))
            .build()
            .layer(ok_with(10, 200)),
    );
    let base = std::mem::size_of_val(&inner.execute(get("http://h/")));
    let ms = std::mem::size_of_val(&m.execute(get("http://h/")));
    let ts = std::mem::size_of_val(&t.execute(get("http://h/")));
    let bs = std::mem::size_of_val(&both.execute(get("http://h/")));
    println!(
        "inner future: {base} B; +metrics: {ms} B (+{}); +tracing: {ts} B (+{}); both: {bs} B (+{})",
        ms - base,
        ts - base,
        bs - base
    );
    assert!(ms - base <= 128, "metrics adds {}", ms - base);
    assert!(ts - base <= 128, "tracing adds {}", ts - base);
    assert!(bs - base <= 256);
}

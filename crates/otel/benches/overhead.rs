//! Per-request overhead of `MetricsLayer` and `OtelObserver` calls versus a no-op inner service.
//! Uses an SDK meter provider with a (never-firing) periodic reader so aggregation really runs.

use std::hint::black_box;
use std::time::Duration;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use layered::{Layer, Service};
use matier_core::{ConnectionInfo, Error, NoopObserver, Observer, OriginKey, Protocol};
use matier_otel::{MetricsLayer, OtelObserver};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

struct Inner;
impl Service<Request<Full<Bytes>>> for Inner {
    type Out = Result<Response<Full<Bytes>>, Error>;
    async fn execute(&self, _r: Request<Full<Bytes>>) -> Self::Out {
        let mut r = Response::new(Full::new(Bytes::from_static(b"0123456789")));
        r.extensions_mut().insert(ConnectionInfo {
            protocol: Protocol::Http2,
            reused: true,
            conn_id: 1,
        });
        Ok(r)
    }
}

fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let reader = PeriodicReader::builder(InMemoryMetricExporter::default())
        .with_interval(Duration::from_secs(3600))
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let meter = provider.meter("bench");
    let layered = MetricsLayer::builder().meter(&meter).build().layer(Inner);
    let req = || {
        Request::get("https://example.com/x")
            .body(Full::new(Bytes::new()))
            .unwrap()
    };

    let mut g = c.benchmark_group("per_request");
    g.bench_function("baseline_inner_only", |b| {
        b.iter(|| {
            rt.block_on(async {
                let r = Inner.execute(req()).await.unwrap();
                black_box(r.into_body().collect().await.unwrap());
            })
        })
    });
    g.bench_function("metrics_layer", |b| {
        b.iter(|| {
            rt.block_on(async {
                let r = layered.execute(req()).await.unwrap();
                black_box(r.into_body().collect().await.unwrap());
            })
        })
    });
    g.finish();

    let o = OriginKey::from_uri(&"https://example.com/".parse().unwrap()).unwrap();
    let noop = NoopObserver;
    let obs = OtelObserver::new(&meter);
    let mut g = c.benchmark_group("observer_call");
    let d = Duration::from_micros(50);
    g.bench_function("noop_write_batch", |b| {
        b.iter(|| noop.write_batch(black_box(&o), 4096, 4))
    });
    g.bench_function("otel_write_batch", |b| {
        b.iter(|| obs.write_batch(black_box(&o), 4096, 4))
    });
    g.bench_function("otel_request_queued", |b| {
        b.iter(|| obs.request_queued(black_box(&o), d))
    });
    g.bench_function("otel_actor_tick", |b| {
        b.iter(|| obs.actor_tick(black_box(&o), d, 3))
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);

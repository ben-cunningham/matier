//! OpenTelemetry instrumentation for Matier: metrics and (optionally) traces.
//!
//! * [`OtelObserver`] implements `matier_core::Observer` (transport, pool and HTTP/2 actor signals).
//! * [`MetricsLayer`] wraps any `matier_core::Service<http::Request<B>>` returning
//!   `Result<http::Response<RB>, matier_core::Error>` and records request-scoped semconv metrics. The
//!   response body becomes a [`MeteredBody`].
//! * [`TracingLayer`] (feature `tracing`, on by default) creates client spans and injects W3C trace context.
//!
//! Layers return hand-written futures that add only a few words to the inner future (per-request state
//! is boxed), see `tests/otel.rs::future_size_overhead`.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//! use layered::Layer;
//! use matier_otel::{MetricsLayer, OtelObserver, TelemetryAttributes};
//! use opentelemetry::{KeyValue, metrics::MeterProvider};
//! use opentelemetry_sdk::metrics::SdkMeterProvider;
//!
//! let provider = SdkMeterProvider::builder().build(); // add a reader/exporter in real code
//! let meter = provider.meter("matier");
//!
//! // 1. Observer for transport/pool/actor metrics: hand this to the client builder.
//! let observer: Arc<dyn matier_core::Observer> = Arc::new(OtelObserver::new(&meter));
//! assert!(observer.enabled());
//!
//! // 2. Request metrics layer, sharing the per-origin attribute cache with the observer.
//! let layer = MetricsLayer::builder()
//!     .meter(&meter)
//!     .enrich_request(|parts, out| {
//!         if parts.uri.path().starts_with("/admin") {
//!             out.push(KeyValue::new("area", "admin"));
//!         }
//!     })
//!     .allowed_keys(["tenant", "area"])
//!     .max_values_per_key(100)
//!     .build();
//! # struct Svc;
//! # impl matier_core::Service<http::Request<()>> for Svc {
//! #     type Out = Result<http::Response<()>, matier_core::Error>;
//! #     async fn execute(&self, _r: http::Request<()>) -> Self::Out { Ok(http::Response::new(())) }
//! # }
//! let service = layer.layer(Svc); // Metrics<Svc>
//!
//! // 3. Per-request dimension:
//! let mut req = http::Request::new(());
//! req.extensions_mut().insert(TelemetryAttributes::new([KeyValue::new("tenant", "acme")]));
//! # let _ = (service, req);
//! ```
//!
//! # Metrics
//!
//! Attribute shorthand: **origin** = `server.address`, `server.port`, `url.scheme`.
//!
//! | Metric | Type | Unit | Attributes | Meaning / question answered |
//! |---|---|---|---|---|
//! | `http.client.request.duration` | histogram | s | origin, `http.request.method`, `http.response.status_code`, `network.protocol.version`, `error.type`, `url.template`, custom | End-to-end latency to response headers. "How slow are we, per route / status?" |
//! | `http.client.active_requests` | up-down counter | {request} | origin, method, `url.template`, custom (request-side only) | In-flight requests; futures dropped early are decremented too. "How much concurrency do we have?" |
//! | `http.client.request.body.size` | histogram | By | as duration | Request body size when the exact length is known. |
//! | `http.client.response.body.size` | histogram | By | as duration + `matier.body.complete` | Bytes actually consumed from the response body; `complete=false` means the caller dropped or errored early. "Are callers abandoning large downloads?" |
//! | `http.client.request.time_in_queue` | histogram | s | origin | Wait for a pool checkout / H2 stream slot. "Is the connection the bottleneck?" |
//! | `matier.client.request.dispatch_delay` | histogram | s | origin | H2 enqueue to HEADERS written. "Is the connection actor slow to pick up work?" |
//! | `matier.client.connection.setup.duration` | histogram | s | origin, `matier.connect.phase` = dns/tcp/tls, `outcome` = success/failure | Connect time per phase. "Is DNS or TLS the slow part?" |
//! | `http.client.open_connections` | up-down counter | {connection} | origin, `network.protocol.version` | Open connections. |
//! | `http.client.connection.duration` | histogram | s | origin, `network.protocol.version` | Connection lifetime. "Are connections churning?" |
//! | `matier.transport.write.batch.bytes` | histogram | By | origin | Bytes per vectored write. "Is write batching working?" |
//! | `matier.transport.write.batch.iovecs` | histogram | {iovec} | origin | Iovecs per write; ~1 means no coalescing. |
//! | `matier.transport.bytes` | counter | By | origin, `network.io.direction` = transmit/receive | Wire throughput. |
//! | `matier.h2.actor.busy` | histogram | s | origin | Busy time per actor tick; sum/ wall time is the busy ratio. "Is the h2 connection actor saturated?" (add connections when it approaches the tick rate) |
//! | `matier.h2.actor.queue_depth` | histogram | {command} | origin | Command queue depth at each tick; persistent depth means saturation. |
//! | `matier.h2.flow_control.stall_duration` | histogram | s | origin | Time streams wait for window. "Are we window-limited?" |
//! | `matier.h2.goaway` | counter | {frame} | origin, `matier.h2.error_code` | GOAWAYs received. |
//! | `matier.h2.rst_stream` | counter | {frame} | origin, `matier.h2.error_code`, `matier.h2.direction` = sent/received | Stream resets. |
//! | `matier.pool.waiters` | histogram | {request} | origin | Pool waiters when a request has to wait. |
//!
//! `error.type` is `Error::label()` (e.g. `connect`, `timeout`), `abandoned` for dropped futures, or the
//! status code string for statuses at or above the threshold (default 400, as semconv specifies for clients;
//! change with `error_status_threshold`). Methods outside the standard set are reported as `_OTHER`.
//!
//! Histograms use explicit buckets: latency 100 us to 10 s, sizes 64 B to 64 MiB, counts 0 to 1024.
//!
//! # Hot-path cost
//!
//! Origin attributes are cached per origin in a lock-free map ([`OriginAttrCache`]); a hit is a hash of
//! the host plus a table probe, with no allocation. The cache holds at most 8192 origins and interns their
//! host strings for the life of the process. `MetricsLayer` costs one boxed per-request state and clones
//! of the attributes it records (see the benchmark in `benches/overhead.rs`).
//!
//! # Adding dimensions
//!
//! * Per request: insert [`TelemetryAttributes`] into `Request::extensions_mut()`. The service may also
//!   put them in the response extensions to add outcome-dependent dimensions (duration / response body only).
//! * Globally: `MetricsLayer::builder().enrich_request(..)` / `.enrich_response(..)` closures receive the
//!   `Parts` and push `KeyValue`s.
//! * Route labels: insert `matier_core::UriTemplate` for `url.template`.
//! * Cardinality: `allowed_keys` drops unlisted extra keys; `max_values_per_key(n)` maps values beyond the
//!   first `n` distinct ones per key to `"_other"`. The guard applies to custom dimensions only, never to
//!   semconv attributes. Prefer `&'static str`/`Arc<str>`/integer values; owned `String`s allocate when
//!   cloned.
//!
//! # Tracing
//!
//! `TracingLayer` starts a `Client` span `HTTP {METHOD}`, injects `traceparent`/`tracestate` through the global
//! text-map propagator (install one, e.g. `TraceContextPropagator`), sets error status on failures and
//! statuses over the threshold, and adds the events `matier.response.headers` and `matier.response.body`.
//! The span ends when the response body finishes or is dropped. Insert [`NoTrace`] to suppress it per request.
pub mod attrs;
pub mod metrics;
pub mod observer;
#[cfg(feature = "tracing")]
pub mod trace;

pub use attrs::{OriginAttrCache, TelemetryAttributes};
pub use metrics::{MeteredBody, Metrics, MetricsLayer, MetricsLayerBuilder};
pub use observer::OtelObserver;
#[cfg(feature = "tracing")]
pub use trace::{NoTrace, TracedBody, Tracing, TracingLayer};

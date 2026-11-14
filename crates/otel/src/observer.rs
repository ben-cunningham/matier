//! [`OtelObserver`]: implements `matier_core::Observer` with pre-created instruments.

use std::sync::Arc;
use std::time::Duration;

use matier_core::{ConnectPhase, Observer, OriginKey, Protocol, ScaleReason};
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter};
use smallvec::SmallVec;

use crate::attrs::OriginAttrCache;

/// Latency buckets in seconds, 100 us .. 10 s.
pub(crate) const LATENCY_BUCKETS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
    5.0, 10.0,
];
/// Sub-millisecond buckets for per-tick actor work.
const TICK_BUCKETS: &[f64] = &[
    0.000_001, 0.000_005, 0.000_01, 0.000_05, 0.000_1, 0.000_5, 0.001, 0.005, 0.01, 0.05, 0.1,
];
/// Long-lived (connection lifetime) buckets, 1 s .. 1 h.
const LIFETIME_BUCKETS: &[f64] = &[0.1, 1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 900.0, 3600.0];
/// Byte-size buckets, 64 B .. 64 MiB (powers of 4).
pub(crate) const SIZE_BUCKETS: &[f64] = &[
    64.0,
    256.0,
    1024.0,
    4096.0,
    16384.0,
    65536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
];
const COUNT_BUCKETS: &[f64] = &[
    0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0,
];

pub(crate) fn hist_f64(
    m: &Meter,
    name: &'static str,
    unit: &'static str,
    desc: &'static str,
    b: &[f64],
) -> Histogram<f64> {
    m.f64_histogram(name)
        .with_unit(unit)
        .with_description(desc)
        .with_boundaries(b.to_vec())
        .build()
}

pub(crate) fn hist_u64(
    m: &Meter,
    name: &'static str,
    unit: &'static str,
    desc: &'static str,
    b: &[f64],
) -> Histogram<u64> {
    m.u64_histogram(name)
        .with_unit(unit)
        .with_description(desc)
        .with_boundaries(b.to_vec())
        .build()
}

/// Stable HTTP/2 error code names (RFC 9113 section 7).
#[must_use]
pub fn h2_error_name(code: u32) -> &'static str {
    match code {
        0x0 => "NO_ERROR",
        0x1 => "PROTOCOL_ERROR",
        0x2 => "INTERNAL_ERROR",
        0x3 => "FLOW_CONTROL_ERROR",
        0x4 => "SETTINGS_TIMEOUT",
        0x5 => "STREAM_CLOSED",
        0x6 => "FRAME_SIZE_ERROR",
        0x7 => "REFUSED_STREAM",
        0x8 => "CANCEL",
        0x9 => "COMPRESSION_ERROR",
        0xa => "CONNECT_ERROR",
        0xb => "ENHANCE_YOUR_CALM",
        0xc => "INADEQUATE_SECURITY",
        0xd => "HTTP_1_1_REQUIRED",
        _ => "UNKNOWN",
    }
}

/// Records transport, pool and H2 actor telemetry. Create once and share via `Arc` as the client's
/// observer. All attribute sets are cached per origin ([`OriginAttrCache`]), so the hot path performs
/// no allocation and clones no reference-counted values.
pub struct OtelObserver {
    cache: Arc<OriginAttrCache>,
    time_in_queue: Histogram<f64>,
    dispatch_delay: Histogram<f64>,
    setup: Histogram<f64>,
    open_conns: UpDownCounter<i64>,
    conn_duration: Histogram<f64>,
    batch_bytes: Histogram<u64>,
    batch_iovecs: Histogram<u64>,
    bytes: Counter<u64>,
    actor_busy: Histogram<f64>,
    queue_depth: Histogram<u64>,
    fc_stall: Histogram<f64>,
    goaway: Counter<u64>,
    rst: Counter<u64>,
    waiters: Histogram<u64>,
    pool_conns: Gauge<u64>,
    scale_events: Counter<u64>,
}

impl Default for OtelObserver {
    fn default() -> Self {
        Self::new(&global::meter("matier"))
    }
}

impl OtelObserver {
    /// Creates the instruments on `meter`.
    #[must_use]
    pub fn new(meter: &Meter) -> Self {
        Self::with_cache(meter, Arc::new(OriginAttrCache::new()))
    }

    /// Like [`new`](Self::new) but shares an attribute cache (e.g. with a `MetricsLayer`).
    #[must_use]
    pub fn with_cache(m: &Meter, cache: Arc<OriginAttrCache>) -> Self {
        Self {
            cache,
            time_in_queue: hist_f64(
                m,
                "http.client.request.time_in_queue",
                "s",
                "Time a request waited for a connection / stream slot before dispatch",
                LATENCY_BUCKETS,
            ),
            dispatch_delay: hist_f64(
                m,
                "matier.client.request.dispatch_delay",
                "s",
                "H2: enqueue to HEADERS written",
                LATENCY_BUCKETS,
            ),
            setup: hist_f64(
                m,
                "matier.client.connection.setup.duration",
                "s",
                "Connection establishment time per phase",
                LATENCY_BUCKETS,
            ),
            open_conns: m
                .i64_up_down_counter("http.client.open_connections")
                .with_unit("{connection}")
                .with_description("Currently open connections")
                .build(),
            conn_duration: hist_f64(
                m,
                "http.client.connection.duration",
                "s",
                "Connection lifetime",
                LIFETIME_BUCKETS,
            ),
            batch_bytes: hist_u64(
                m,
                "matier.transport.write.batch.bytes",
                "By",
                "Bytes per vectored write",
                SIZE_BUCKETS,
            ),
            batch_iovecs: hist_u64(
                m,
                "matier.transport.write.batch.iovecs",
                "{iovec}",
                "Iovecs per vectored write",
                COUNT_BUCKETS,
            ),
            bytes: m
                .u64_counter("matier.transport.bytes")
                .with_unit("By")
                .with_description("Bytes moved on the wire")
                .build(),
            actor_busy: hist_f64(
                m,
                "matier.h2.actor.busy",
                "s",
                "H2 connection actor busy time per tick",
                TICK_BUCKETS,
            ),
            queue_depth: hist_u64(
                m,
                "matier.h2.actor.queue_depth",
                "{command}",
                "H2 actor command queue depth at tick",
                COUNT_BUCKETS,
            ),
            fc_stall: hist_f64(
                m,
                "matier.h2.flow_control.stall_duration",
                "s",
                "Time a stream waited for flow-control window",
                LATENCY_BUCKETS,
            ),
            goaway: m
                .u64_counter("matier.h2.goaway")
                .with_unit("{frame}")
                .with_description("GOAWAY frames, by error code")
                .build(),
            rst: m
                .u64_counter("matier.h2.rst_stream")
                .with_unit("{frame}")
                .with_description("RST_STREAM frames, by error code and direction")
                .build(),
            waiters: hist_u64(
                m,
                "matier.pool.waiters",
                "{request}",
                "Pool waiters sampled when a request waits",
                COUNT_BUCKETS,
            ),
            pool_conns: m
                .u64_gauge("matier.pool.connections")
                .with_unit("{connection}")
                .with_description(
                    "HTTP/2 connections the pool holds for the origin, at the last scale event",
                )
                .build(),
            scale_events: m
                .u64_counter("matier.pool.scale_events")
                .with_unit("{event}")
                .with_description("Pool autoscaling decisions, by reason")
                .build(),
        }
    }

    /// The attribute cache (for inspection and sharing).
    #[must_use]
    pub fn cache(&self) -> &Arc<OriginAttrCache> {
        &self.cache
    }
}

impl Observer for OtelObserver {
    fn enabled(&self) -> bool {
        true
    }
    fn request_queued(&self, o: &OriginKey, wait: Duration) {
        self.cache.with_key(o, |a| {
            self.time_in_queue.record(wait.as_secs_f64(), &a.base)
        });
    }
    fn request_dispatched(&self, o: &OriginKey, delay: Duration) {
        self.cache.with_key(o, |a| {
            self.dispatch_delay.record(delay.as_secs_f64(), &a.base)
        });
    }
    fn connect_phase(&self, o: &OriginKey, phase: ConnectPhase, d: Duration, ok: bool) {
        let p = match phase {
            ConnectPhase::Dns => 0,
            ConnectPhase::Tcp => 1,
            ConnectPhase::Tls => 2,
        };
        self.cache.with_key(o, |a| {
            self.setup
                .record(d.as_secs_f64(), &a.phase[p][usize::from(ok)])
        });
    }
    fn connection_opened(&self, o: &OriginKey, p: Protocol) {
        self.cache
            .with_key(o, |a| self.open_conns.add(1, &a.proto[p as usize]));
    }
    fn connection_closed(&self, o: &OriginKey, p: Protocol, lifetime: Duration) {
        self.cache.with_key(o, |a| {
            let at = &a.proto[p as usize];
            self.open_conns.add(-1, at);
            self.conn_duration.record(lifetime.as_secs_f64(), at);
        });
    }
    fn write_batch(&self, o: &OriginKey, bytes: usize, iovecs: usize) {
        self.cache.with_key(o, |a| {
            self.batch_bytes.record(bytes as u64, &a.base);
            self.batch_iovecs.record(iovecs as u64, &a.base);
            self.bytes.add(bytes as u64, &a.dir[0]);
        });
    }
    fn bytes_read(&self, o: &OriginKey, n: usize) {
        self.cache
            .with_key(o, |a| self.bytes.add(n as u64, &a.dir[1]));
    }
    fn actor_tick(&self, o: &OriginKey, busy: Duration, queue_depth: usize) {
        self.cache.with_key(o, |a| {
            self.actor_busy.record(busy.as_secs_f64(), &a.base);
            self.queue_depth.record(queue_depth as u64, &a.base);
        });
    }
    fn flow_control_stall(&self, o: &OriginKey, d: Duration) {
        self.cache
            .with_key(o, |a| self.fc_stall.record(d.as_secs_f64(), &a.base));
    }
    fn h2_goaway(&self, o: &OriginKey, code: u32) {
        self.cache.with_key(o, |a| {
            let mut v: SmallVec<[KeyValue; 4]> = a.base.iter().cloned().collect();
            v.push(KeyValue::new("matier.h2.error_code", h2_error_name(code)));
            self.goaway.add(1, &v);
        });
    }
    fn h2_rst_stream(&self, o: &OriginKey, code: u32, sent: bool) {
        self.cache.with_key(o, |a| {
            let mut v: SmallVec<[KeyValue; 5]> = a.base.iter().cloned().collect();
            v.push(KeyValue::new("matier.h2.error_code", h2_error_name(code)));
            v.push(KeyValue::new(
                "matier.h2.direction",
                if sent { "sent" } else { "received" },
            ));
            self.rst.add(1, &v);
        });
    }
    fn pool_waiters(&self, o: &OriginKey, n: usize) {
        self.cache
            .with_key(o, |a| self.waiters.record(n as u64, &a.base));
    }
    fn pool_scaled(&self, o: &OriginKey, conns: usize, reason: ScaleReason) {
        self.cache.with_key(o, |a| {
            self.pool_conns.record(conns as u64, &a.base);
            let mut v: SmallVec<[KeyValue; 4]> = a.base.iter().cloned().collect();
            v.push(KeyValue::new("matier.pool.scale_reason", reason.as_str()));
            self.scale_events.add(1, &v);
        });
    }
}

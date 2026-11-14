//! Attribute plumbing: [`TelemetryAttributes`], the per-origin attribute cache and the cardinality guard.

use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use matier_core::{OriginKey, Protocol, Scheme};
use opentelemetry::{Key, KeyValue, Value};
use smallvec::SmallVec;

pub(crate) const K_ADDR: Key = Key::from_static_str("server.address");
pub(crate) const K_PORT: Key = Key::from_static_str("server.port");
pub(crate) const K_SCHEME: Key = Key::from_static_str("url.scheme");
pub(crate) const K_METHOD: Key = Key::from_static_str("http.request.method");
pub(crate) const K_STATUS: Key = Key::from_static_str("http.response.status_code");
pub(crate) const K_PROTO: Key = Key::from_static_str("network.protocol.version");
pub(crate) const K_ERROR: Key = Key::from_static_str("error.type");
pub(crate) const K_TEMPLATE: Key = Key::from_static_str("url.template");
pub(crate) const K_DIR: Key = Key::from_static_str("network.io.direction");
pub(crate) const K_PHASE: Key = Key::from_static_str("matier.connect.phase");
pub(crate) const K_OUTCOME: Key = Key::from_static_str("outcome");

/// Default `error.type` threshold: statuses at or above this are reported as errors (semconv: 400 for clients).
pub const DEFAULT_ERROR_STATUS_THRESHOLD: u16 = 400;

/// Inline attribute buffer used on the request path (no heap allocation up to 12 attributes).
pub(crate) type Attrs = SmallVec<[KeyValue; 12]>;

/// Extra low-cardinality dimensions attached to a request (or response) via `http::Extensions`.
///
/// ```
/// use matier_otel::TelemetryAttributes;
/// use opentelemetry::KeyValue;
/// let mut req = http::Request::new(());
/// req.extensions_mut().insert(TelemetryAttributes::new([KeyValue::new("tenant", "acme")]));
/// ```
///
/// Cloning the attributes happens a couple of times per request: prefer `&'static str`, integer or
/// `Arc<str>` values over owned `String`s (which allocate on clone).
#[derive(Clone, Debug, Default)]
pub struct TelemetryAttributes(SmallVec<[KeyValue; 4]>);

impl TelemetryAttributes {
    /// Creates the set from any iterator of key/values.
    pub fn new(attrs: impl IntoIterator<Item = KeyValue>) -> Self {
        Self(attrs.into_iter().collect())
    }
    /// Adds one attribute.
    pub fn push(&mut self, kv: KeyValue) {
        self.0.push(kv);
    }
    /// Iterates the attributes.
    pub fn iter(&self) -> impl Iterator<Item = &KeyValue> {
        self.0.iter()
    }
    /// Number of attributes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }
    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromIterator<KeyValue> for TelemetryAttributes {
    fn from_iter<I: IntoIterator<Item = KeyValue>>(iter: I) -> Self {
        Self::new(iter)
    }
}

/// Precomputed attribute sets for one origin. Built once; recording clones nothing but plain slices.
pub struct OriginAttrs {
    scheme: Scheme,
    port: u16,
    host: Box<str>,
    /// `server.address`, `server.port`, `url.scheme`.
    pub(crate) base: Box<[KeyValue]>,
    /// base + `network.io.direction` (transmit, receive).
    pub(crate) dir: [Box<[KeyValue]>; 2],
    /// base + `matier.connect.phase` + `outcome` (indexed `[phase][ok]`).
    pub(crate) phase: [[Box<[KeyValue]>; 2]; 3],
    /// base + `network.protocol.version` (h1, h2).
    pub(crate) proto: [Box<[KeyValue]>; 2],
}

pub(crate) fn proto_str(p: Protocol) -> &'static str {
    match p {
        Protocol::Http1 => "1.1",
        Protocol::Http2 => "2",
    }
}

impl OriginAttrs {
    fn build(scheme: Scheme, host: &str, port: u16, intern: bool) -> Self {
        // Interned hosts are leaked (bounded by the cache cap) so that cloning the KeyValue is a
        // plain copy: no refcount traffic on a cache line shared between cores.
        let addr: Value = if intern {
            Value::from(&*Box::leak(host.to_owned().into_boxed_str()))
        } else {
            Value::from(host.to_owned())
        };
        let base = vec![
            KeyValue::new(K_ADDR, addr),
            KeyValue::new(K_PORT, i64::from(port)),
            KeyValue::new(K_SCHEME, scheme.as_str()),
        ];
        let with = |extra: &[KeyValue]| -> Box<[KeyValue]> {
            base.iter().cloned().chain(extra.iter().cloned()).collect()
        };
        let dir = |d: &'static str| with(&[KeyValue::new(K_DIR, d)]);
        let phase = |p: &'static str, ok: bool| {
            with(&[
                KeyValue::new(K_PHASE, p),
                KeyValue::new(K_OUTCOME, if ok { "success" } else { "failure" }),
            ])
        };
        let ph = |p| [phase(p, false), phase(p, true)];
        let pr = |p| with(&[KeyValue::new(K_PROTO, p)]);
        Self {
            scheme,
            port,
            host: host.into(),
            dir: [dir("transmit"), dir("receive")],
            phase: [ph("dns"), ph("tcp"), ph("tls")],
            proto: [pr("1.1"), pr("2")],
            base: base.into_boxed_slice(),
        }
    }

    fn matches(&self, scheme: Scheme, host: &str, port: u16) -> bool {
        self.scheme == scheme && self.port == port && self.host.eq_ignore_ascii_case(host)
    }
}

fn strip_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

/// Lock-free (`papaya`) cache of [`OriginAttrs`] keyed by a 64-bit hash of (scheme, host, port).
///
/// **Cost:** a hit is one hash of the host bytes plus an epoch pin and a lock-free table probe (tens of
/// nanoseconds, no allocation, no shared-cache-line writes). A miss allocates the attribute sets once
/// (about a dozen small boxes) and leaks the host string for the process lifetime. The cache stops
/// growing at [`OriginAttrCache::MAX_ORIGINS`]; further origins are built per call (correct, but they
/// allocate), so unbounded origin counts degrade to the uncached cost instead of growing memory.
/// Hash collisions are detected by comparing the stored identity and fall back to the uncached path.
pub struct OriginAttrCache {
    map: papaya::HashMap<u64, OriginAttrs>,
    len: AtomicUsize,
}

impl Default for OriginAttrCache {
    fn default() -> Self {
        Self::new()
    }
}

impl OriginAttrCache {
    /// Maximum number of cached origins.
    pub const MAX_ORIGINS: usize = 8192;

    /// Creates an empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: papaya::HashMap::new(),
            len: AtomicUsize::new(0),
        }
    }

    /// Number of cached origins.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// True if nothing is cached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn with_key<R>(&self, o: &OriginKey, f: impl FnOnce(&OriginAttrs) -> R) -> R {
        self.with(o.scheme, &o.host, o.port, f)
    }

    pub(crate) fn with<R>(
        &self,
        scheme: Scheme,
        host: &str,
        port: u16,
        f: impl FnOnce(&OriginAttrs) -> R,
    ) -> R {
        let host = strip_brackets(host);
        let mut h = DefaultHasher::new();
        scheme.hash(&mut h);
        port.hash(&mut h);
        for b in host.bytes() {
            b.to_ascii_lowercase().hash(&mut h);
        }
        let hash = h.finish();
        let map = self.map.pin();
        if let Some(a) = map.get(&hash) {
            if a.matches(scheme, host, port) {
                return f(a);
            }
            return f(&OriginAttrs::build(scheme, host, port, false));
        }
        if self.len.load(Ordering::Relaxed) >= Self::MAX_ORIGINS {
            return f(&OriginAttrs::build(scheme, host, port, false));
        }
        let built = OriginAttrs::build(scheme, &host.to_ascii_lowercase(), port, true);
        match map.try_insert(hash, built) {
            Ok(a) => {
                self.len.fetch_add(1, Ordering::Relaxed);
                f(a)
            }
            // Lost a race: use the winner if it is ours, else go uncached.
            Err(e) => {
                if e.current.matches(scheme, host, port) {
                    f(e.current)
                } else {
                    f(&OriginAttrs::build(scheme, host, port, false))
                }
            }
        }
    }
}

type Enricher<T> = Box<dyn Fn(&T, &mut Vec<KeyValue>) + Send + Sync>;

pub(crate) struct Enrichers {
    pub req: Vec<Enricher<http::request::Parts>>,
    pub resp: Vec<Enricher<http::response::Parts>>,
}

/// Cardinality guard for user-supplied dimensions.
///
/// * `allow`: when set, extra attributes whose key is not listed are dropped.
/// * `max_values`: each key may take at most this many distinct values (string form); further values
///   are replaced with `"_other"`. Values already admitted keep working. Costs a read lock per extra
///   attribute plus a write lock the first time a value is seen.
#[derive(Default)]
pub(crate) struct CardinalityGuard {
    pub allow: Option<HashSet<Key>>,
    pub max_values: Option<usize>,
    seen: RwLock<HashMap<Key, HashSet<String>>>,
}

impl CardinalityGuard {
    pub(crate) fn is_noop(&self) -> bool {
        self.allow.is_none() && self.max_values.is_none()
    }

    pub(crate) fn apply(&self, extras: &mut SmallVec<[KeyValue; 4]>) {
        if let Some(allow) = &self.allow {
            extras.retain(|kv| allow.contains(&kv.key));
        }
        let Some(max) = self.max_values else { return };
        for kv in extras.iter_mut() {
            let v = kv.value.as_str();
            let known = self
                .seen
                .read()
                .ok()
                .is_some_and(|m| m.get(&kv.key).is_some_and(|s| s.contains(&*v)));
            if known {
                continue;
            }
            let Ok(mut m) = self.seen.write() else {
                continue;
            };
            let set = m.entry(kv.key.clone()).or_default();
            if set.contains(&*v) {
                continue;
            }
            if set.len() < max {
                set.insert(v.into_owned());
            } else {
                kv.value = Value::from("_other");
            }
        }
    }
}

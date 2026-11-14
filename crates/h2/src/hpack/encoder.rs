//! HPACK encoder (RFC 7541) with request encoding for HTTP/2 clients.
//!
//! # Allocation behaviour
//!
//! Output goes to a caller-supplied `Vec<u8>`; with sufficient capacity no allocation occurs for
//! static-table and dynamic-table hits, nor in [`Encoder::stateless`] mode at all. Inserting a
//! new dynamic-table entry copies `name || value` into a buffer recycled from evicted entries
//! when possible (otherwise one allocation).
//!
//! Dynamic-table lookup is a linear scan from the newest entry (length-first comparison); with
//! a 4 KiB table that is at most ~100 entries, and typical hits are among the newest.

use super::{ENTRY_OVERHEAD, encode_int, huffman, static_table};
use http::{HeaderMap, Method, Uri, header};
use std::collections::VecDeque;

/// Which fields are encoded as "never indexed" (§6.2.3) so intermediaries cannot index them.
#[derive(Clone, Copy, Debug)]
pub struct SensitivePolicy {
    /// `authorization` and `proxy-authorization` (always).
    pub auth: bool,
    /// `cookie` / `set-cookie` values shorter than this many bytes (short values are
    /// typically session tokens, which are the CRIME-style targets). `0` disables.
    pub short_cookie_len: usize,
}

impl Default for SensitivePolicy {
    fn default() -> Self {
        SensitivePolicy {
            auth: true,
            short_cookie_len: 20,
        }
    }
}

impl SensitivePolicy {
    fn is_sensitive(&self, name: &[u8], value: &[u8]) -> bool {
        match name {
            b"authorization" | b"proxy-authorization" => self.auth,
            b"cookie" | b"set-cookie" => value.len() < self.short_cookie_len,
            _ => false,
        }
    }
}

struct Entry {
    data: Vec<u8>,
    name_len: usize,
}

/// HPACK encoder. One per connection; blocks must be written to the wire in encode order.
pub struct Encoder {
    table: VecDeque<Entry>,
    size: usize,
    /// Current capacity (what the peer's decoder table is set to).
    capacity: usize,
    /// Capacity we are willing to use at most (our own preference).
    max_capacity: usize,
    stateless: bool,
    /// Pending size updates: (smallest value since last block, latest value).
    pending: Option<(usize, usize)>,
    spare: Vec<Vec<u8>>,
    policy: SensitivePolicy,
}

impl Encoder {
    /// New stateful encoder. `table_size` is the peer's SETTINGS_HEADER_TABLE_SIZE (4096 initially).
    pub fn new(table_size: usize) -> Encoder {
        Encoder {
            table: VecDeque::new(),
            size: 0,
            capacity: table_size,
            max_capacity: table_size,
            stateless: false,
            pending: None,
            spare: Vec::new(),
            policy: SensitivePolicy::default(),
        }
    }

    /// Encoder that never inserts into the dynamic table (static table + Huffman only). It
    /// holds no state between blocks, so any number of them can encode in parallel off the
    /// connection actor; the peer's table stays empty.
    pub fn stateless() -> Encoder {
        let mut e = Encoder::new(0);
        e.stateless = true;
        e
    }

    /// Replace the never-index policy.
    pub fn set_sensitive_policy(&mut self, p: SensitivePolicy) {
        self.policy = p;
    }

    /// Cap the table size we use regardless of what the peer allows (default: the constructor value).
    pub fn set_max_capacity(&mut self, n: usize) {
        self.max_capacity = n;
    }

    /// The peer announced SETTINGS_HEADER_TABLE_SIZE = `n`. The table shrinks/grows to
    /// `min(n, max_capacity)` and a Dynamic Table Size Update is queued for the next block.
    /// Call when the SETTINGS frame is received (the update goes out in the first block after).
    pub fn set_peer_table_size(&mut self, n: usize) {
        if self.stateless {
            return;
        }
        let new = n.min(self.max_capacity);
        if new == self.capacity && self.pending.is_none() {
            return;
        }
        self.pending = Some(match self.pending {
            Some((min, _)) => (min.min(new), new),
            None => (new, new),
        });
        self.capacity = new;
        self.evict_to(new);
    }

    /// Current dynamic table size (accounting units).
    pub fn table_size(&self) -> usize {
        self.size
    }

    fn evict_to(&mut self, target: usize) {
        while self.size > target {
            let e = self.table.pop_back().expect("size accounting");
            self.size -= e.data.len() + ENTRY_OVERHEAD;
            if self.spare.len() < 8 {
                self.spare.push(e.data);
            }
        }
    }

    /// Start a header block: emits queued Dynamic Table Size Updates. [`Self::encode_request`]
    /// calls this; callers using [`Self::encode_field`] directly must call it first.
    pub fn begin_block(&mut self, out: &mut Vec<u8>) {
        if let Some((min, last)) = self.pending.take() {
            if min < last {
                encode_int(out, 0x20, 5, min);
            }
            encode_int(out, 0x20, 5, last);
        }
    }

    fn dyn_lookup(&self, name: &[u8], value: &[u8]) -> (usize, usize) {
        let mut name_idx = 0;
        for (i, e) in self.table.iter().enumerate() {
            if e.name_len == name.len() && &e.data[..e.name_len] == name {
                let idx = static_table::LEN + 1 + i;
                if &e.data[e.name_len..] == value {
                    return (idx, idx);
                }
                if name_idx == 0 {
                    name_idx = idx;
                }
            }
        }
        (0, name_idx)
    }

    fn insert(&mut self, name: &[u8], value: &[u8]) {
        let size = name.len() + value.len() + ENTRY_OVERHEAD;
        self.evict_to(self.capacity - size);
        let mut data = self.spare.pop().unwrap_or_default();
        data.clear();
        data.extend_from_slice(name);
        data.extend_from_slice(value);
        self.size += size;
        self.table.push_front(Entry {
            data,
            name_len: name.len(),
        });
    }

    /// Encode one field. `sensitive` forces a never-indexed literal (e.g. `HeaderValue::is_sensitive`);
    /// the [`SensitivePolicy`] may also force it. `name` must be lowercase.
    pub fn encode_field(&mut self, name: &[u8], value: &[u8], sensitive: bool, out: &mut Vec<u8>) {
        let never = sensitive || self.policy.is_sensitive(name, value);
        let (s_full, s_name) = static_table::lookup(name, value);
        let (d_full, d_name) = if self.stateless || self.table.is_empty() {
            (0, 0)
        } else {
            self.dyn_lookup(name, value)
        };
        if !never {
            let full = if s_full != 0 { s_full } else { d_full };
            if full != 0 {
                encode_int(out, 0x80, 7, full);
                return;
            }
        }
        let name_idx = if s_name != 0 { s_name } else { d_name };
        let index =
            !never && !self.stateless && name.len() + value.len() + ENTRY_OVERHEAD <= self.capacity;
        let (flags, prefix) = if never {
            (0x10, 4)
        } else if index {
            (0x40, 6)
        } else {
            (0x00, 4)
        };
        encode_int(out, flags, prefix, name_idx);
        if name_idx == 0 {
            write_string(out, name);
        }
        write_string(out, value);
        if index {
            self.insert(name, value);
        }
    }

    /// Encode a request header block (HEADERS fragment) and append it to `out`.
    ///
    /// Emits `:method`, `:scheme` (from the URI, default `https`), `:authority` (from the URI,
    /// else the `Host` header), `:path` (path and query; `/` if empty; `*` for `OPTIONS *`), then
    /// the regular headers. Dropped: `host` (superseded by `:authority`), `connection`,
    /// `keep-alive`, `proxy-connection`, `transfer-encoding`, `upgrade`, and `te` unless its
    /// value is `trailers`. Header names from `HeaderMap` are already lowercase. Plain
    /// `CONNECT` sends only `:method` and `:authority`.
    pub fn encode_request(
        &mut self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        out: &mut Vec<u8>,
    ) {
        self.encode_request_ext(method, uri, None, headers, out)
    }

    /// Like [`Self::encode_request`] with an optional `:protocol` pseudo-header for extended
    /// CONNECT (RFC 8441); the peer must have advertised SETTINGS_ENABLE_CONNECT_PROTOCOL.
    pub fn encode_request_ext(
        &mut self,
        method: &Method,
        uri: &Uri,
        protocol: Option<&str>,
        headers: &HeaderMap,
        out: &mut Vec<u8>,
    ) {
        self.begin_block(out);
        self.encode_field(b":method", method.as_str().as_bytes(), false, out);
        let plain_connect = method == Method::CONNECT && protocol.is_none();
        if !plain_connect {
            self.encode_field(
                b":scheme",
                uri.scheme_str().unwrap_or("https").as_bytes(),
                false,
                out,
            );
        }
        match uri.authority() {
            // RFC 9113 8.3.1: `:authority` MUST NOT include the deprecated userinfo subcomponent.
            Some(a) => {
                let a = a.as_str();
                let host = a.rsplit_once('@').map_or(a, |(_, h)| h);
                self.encode_field(b":authority", host.as_bytes(), false, out);
            }
            None => {
                if let Some(h) = headers.get(header::HOST) {
                    self.encode_field(b":authority", h.as_bytes(), false, out);
                }
            }
        }
        if !plain_connect {
            let path = match uri.path_and_query() {
                Some(pq) if !pq.as_str().is_empty() => pq.as_str(),
                _ if method == Method::OPTIONS => "*",
                _ => "/",
            };
            self.encode_field(b":path", path.as_bytes(), false, out);
        }
        if let Some(p) = protocol {
            self.encode_field(b":protocol", p.as_bytes(), false, out);
        }
        for (name, value) in headers {
            if is_connection_specific(name.as_str(), value.as_bytes()) {
                continue;
            }
            self.encode_field(
                name.as_str().as_bytes(),
                value.as_bytes(),
                value.is_sensitive(),
                out,
            );
        }
    }
}

/// Fields that must not be forwarded over HTTP/2 (RFC 9113 8.2.2) plus `host` (replaced by `:authority`).
/// Applies to request headers and request trailers alike.
#[must_use]
pub fn is_connection_specific(name: &str, value: &[u8]) -> bool {
    match name {
        "host" | "connection" | "keep-alive" | "proxy-connection" | "transfer-encoding"
        | "upgrade" => true,
        "te" => !value.eq_ignore_ascii_case(b"trailers"),
        _ => false,
    }
}

/// String literal (§5.2): Huffman when strictly shorter.
#[inline]
fn write_string(out: &mut Vec<u8>, s: &[u8]) {
    let h = huffman::encoded_len(s);
    if h < s.len() {
        encode_int(out, 0x80, 7, h);
        huffman::encode(s, out);
    } else {
        encode_int(out, 0, 7, s.len());
        out.extend_from_slice(s);
    }
}

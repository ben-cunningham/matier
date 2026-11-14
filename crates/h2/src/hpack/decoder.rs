//! HPACK decoder (RFC 7541 §3, §6) plus HTTP/2 header-block validation (RFC 9113 §8).
//!
//! # Allocation behaviour
//!
//! * Static-table hits: none (`Bytes::from_static`).
//! * Dynamic-table hits: a refcount increment on the entry's shared buffer.
//! * Literal strings that are not Huffman-coded: none, they are `Bytes` sub-slices of the
//!   input block. (Such slices keep the block's receive buffer alive until dropped; entries
//!   inserted into the dynamic table are *copied* into one compact allocation so the table never
//!   pins read buffers, cf. hyper#4060.)
//! * Huffman strings: one `Vec` allocation each, sized by the decoded length.
//! * Each dynamic-table insertion: one allocation for the combined `name || value` buffer (plus a
//!   small shared-state allocation the first time that buffer is cloned).
//!
//! Limits are checked *before* allocating: declared string lengths are compared to the
//! per-string and remaining header-list budgets, and Huffman output is capped by the same budget.

use super::{ENTRY_OVERHEAD, huffman, static_table};
use crate::error::{H2Error, Reason};
use bytes::{Bytes, BytesMut};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use std::collections::VecDeque;

/// HPACK decoding failure. All of these desynchronize or endanger the compression context and
/// are therefore connection errors (see [`DecodeError::into_h2`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// Input ended inside a representation.
    Truncated,
    /// Integer larger than `u32::MAX` or with too many continuation bytes.
    IntegerOverflow,
    /// Index 0, or beyond the table.
    InvalidIndex,
    /// Invalid Huffman data.
    InvalidHuffman,
    /// A string longer than the configured maximum.
    StringTooLong,
    /// Header list (sum of `name + value + 32`) exceeds the configured maximum.
    HeaderListTooLarge,
    /// Dynamic table size update after a header field in the same block.
    SizeUpdateNotAtStart,
    /// Dynamic table size update above SETTINGS_HEADER_TABLE_SIZE.
    SizeUpdateTooLarge,
}

impl DecodeError {
    /// The connection error to send.
    pub fn into_h2(self) -> H2Error {
        match self {
            DecodeError::HeaderListTooLarge | DecodeError::StringTooLong => {
                H2Error::Connection(Reason::ENHANCE_YOUR_CALM, "header list too large")
            }
            DecodeError::Truncated => {
                H2Error::Connection(Reason::COMPRESSION_ERROR, "hpack: truncated block")
            }
            DecodeError::IntegerOverflow => {
                H2Error::Connection(Reason::COMPRESSION_ERROR, "hpack: integer overflow")
            }
            DecodeError::InvalidIndex => {
                H2Error::Connection(Reason::COMPRESSION_ERROR, "hpack: invalid index")
            }
            DecodeError::InvalidHuffman => {
                H2Error::Connection(Reason::COMPRESSION_ERROR, "hpack: invalid huffman")
            }
            DecodeError::SizeUpdateNotAtStart => H2Error::Connection(
                Reason::COMPRESSION_ERROR,
                "hpack: table size update not at block start",
            ),
            DecodeError::SizeUpdateTooLarge => H2Error::Connection(
                Reason::COMPRESSION_ERROR,
                "hpack: table size update exceeds limit",
            ),
        }
    }
}

impl core::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl std::error::Error for DecodeError {}

/// Capacity added to the Huffman scratch arena when it runs out.
const SCRATCH_CHUNK: usize = 4096;

/// One dynamic-table entry: `name || value` in a single buffer.
struct Entry {
    data: Bytes,
    name_len: usize,
}

impl Entry {
    fn size(&self) -> usize {
        self.data.len() + ENTRY_OVERHEAD
    }
}

/// HPACK decoder with its dynamic table. One per connection (header blocks must be decoded in
/// wire order).
pub struct Decoder {
    /// Newest entry at the front (HPACK index 62), oldest at the back. A `VecDeque` is a ring buffer.
    table: VecDeque<Entry>,
    size: usize,
    max_size: usize,
    /// Upper bound for peer-initiated table size updates (our SETTINGS_HEADER_TABLE_SIZE).
    limit: usize,
    max_header_list_size: usize,
    max_string_len: usize,
    /// Huffman output arena: strings are `split().freeze()`d off it, so they share chunk allocations.
    scratch: BytesMut,
}

/// Which kind of header block [`Decoder::decode_head`] validates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockKind {
    /// Response header block: requires exactly one `:status`.
    Response,
    /// Trailers: no pseudo-headers allowed.
    Trailers,
}

/// A validated response head or trailer block.
#[derive(Debug)]
pub struct DecodedHead {
    /// `:status` for responses, `None` for trailers.
    pub status: Option<StatusCode>,
    /// Regular header fields.
    pub headers: HeaderMap,
}

struct Cursor<'a> {
    src: &'a [u8],
    pos: usize,
}

impl Cursor<'_> {
    fn int(&mut self, prefix: u8) -> Result<u32, DecodeError> {
        let mask = (1u64 << prefix) - 1;
        let first = *self.src.get(self.pos).ok_or(DecodeError::Truncated)?;
        self.pos += 1;
        let mut v = u64::from(first) & mask;
        if v < mask {
            return Ok(v as u32);
        }
        let mut shift = 0u32;
        loop {
            let b = *self.src.get(self.pos).ok_or(DecodeError::Truncated)?;
            self.pos += 1;
            v += u64::from(b & 0x7f) << shift;
            if v > u64::from(u32::MAX) {
                return Err(DecodeError::IntegerOverflow);
            }
            if b & 0x80 == 0 {
                return Ok(v as u32);
            }
            shift += 7;
            if shift > 28 {
                return Err(DecodeError::IntegerOverflow);
            }
        }
    }
}

impl Decoder {
    /// New decoder; `table_size` is our advertised SETTINGS_HEADER_TABLE_SIZE (initial table
    /// size and the cap for peer size updates). Defaults: header list 64 KiB, string 16 KiB.
    pub fn new(table_size: usize) -> Decoder {
        Decoder {
            table: VecDeque::new(),
            size: 0,
            max_size: table_size,
            limit: table_size,
            max_header_list_size: 64 * 1024,
            max_string_len: 16 * 1024,
            scratch: BytesMut::new(),
        }
    }

    /// Hard cap on the decoded header list (sum of `name + value + 32`) per block. Default 64 KiB.
    pub fn set_max_header_list_size(&mut self, n: usize) {
        self.max_header_list_size = n;
    }

    /// Hard cap on any single name or value (declared, and decoded for Huffman). Default 16 KiB.
    pub fn set_max_string_len(&mut self, n: usize) {
        self.max_string_len = n;
    }

    /// Change the cap for peer table-size updates after we send a SETTINGS_HEADER_TABLE_SIZE
    /// change. The table itself only shrinks when the peer sends its size update.
    pub fn set_table_size_limit(&mut self, n: usize) {
        self.limit = n;
    }

    /// Current dynamic table size in HPACK accounting units.
    pub fn table_size(&self) -> usize {
        self.size
    }

    /// Current dynamic table capacity.
    pub fn table_capacity(&self) -> usize {
        self.max_size
    }

    /// Number of entries in the dynamic table.
    pub fn table_len(&self) -> usize {
        self.table.len()
    }

    fn evict_to(&mut self, target: usize) {
        while self.size > target {
            let e = self.table.pop_back().expect("size accounting");
            self.size -= e.size();
        }
    }

    fn get(&self, idx: u32) -> Result<(Bytes, Bytes), DecodeError> {
        let idx = idx as usize;
        if idx == 0 {
            return Err(DecodeError::InvalidIndex);
        }
        if idx <= static_table::LEN {
            let (n, v) = static_table::ENTRIES[idx - 1];
            return Ok((
                Bytes::from_static(n.as_bytes()),
                Bytes::from_static(v.as_bytes()),
            ));
        }
        let e = self
            .table
            .get(idx - static_table::LEN - 1)
            .ok_or(DecodeError::InvalidIndex)?;
        Ok((e.data.slice(..e.name_len), e.data.slice(e.name_len..)))
    }

    fn get_name(&self, idx: u32) -> Result<Bytes, DecodeError> {
        let idx = idx as usize;
        if idx == 0 {
            return Err(DecodeError::InvalidIndex);
        }
        if idx <= static_table::LEN {
            return Ok(Bytes::from_static(
                static_table::ENTRIES[idx - 1].0.as_bytes(),
            ));
        }
        let e = self
            .table
            .get(idx - static_table::LEN - 1)
            .ok_or(DecodeError::InvalidIndex)?;
        Ok(e.data.slice(..e.name_len))
    }

    /// Insert; returns the entry's `(name, value)` views, or `None` if it was too large to store
    /// (the table is then emptied, per §4.4).
    fn insert(&mut self, name: &[u8], value: &[u8]) -> Option<(Bytes, Bytes)> {
        let size = name.len() + value.len() + ENTRY_OVERHEAD;
        if size > self.max_size {
            self.evict_to(0);
            return None;
        }
        self.evict_to(self.max_size - size);
        let mut b = BytesMut::with_capacity(name.len() + value.len());
        b.extend_from_slice(name);
        b.extend_from_slice(value);
        let data = b.freeze();
        let out = (data.slice(..name.len()), data.slice(name.len()..));
        self.size += size;
        self.table.push_front(Entry {
            data,
            name_len: name.len(),
        });
        Some(out)
    }

    /// Decode a complete header block (HEADERS fragment plus any CONTINUATIONs, already
    /// concatenated; a single-frame block is passed through zero-copy).
    ///
    /// `f(name, value, never_indexed)` is invoked for each field in order. On error the
    /// compression context is unreliable and the connection must be closed.
    pub fn decode<F>(&mut self, block: &Bytes, mut f: F) -> Result<(), DecodeError>
    where
        F: FnMut(Bytes, Bytes, bool),
    {
        let mut c = Cursor { src: block, pos: 0 };
        let mut seen_field = false;
        let mut list = 0usize;
        while c.pos < c.src.len() {
            let b = c.src[c.pos];
            if b & 0x80 != 0 {
                let idx = c.int(7)?;
                let (n, v) = self.get(idx)?;
                list += n.len() + v.len() + ENTRY_OVERHEAD;
                if list > self.max_header_list_size {
                    return Err(DecodeError::HeaderListTooLarge);
                }
                seen_field = true;
                f(n, v, false);
            } else if b & 0xe0 == 0x20 {
                if seen_field {
                    return Err(DecodeError::SizeUpdateNotAtStart);
                }
                let n = c.int(5)? as usize;
                if n > self.limit {
                    return Err(DecodeError::SizeUpdateTooLarge);
                }
                self.max_size = n;
                self.evict_to(n);
            } else {
                let (prefix, index) = if b & 0xc0 == 0x40 {
                    (6, true)
                } else {
                    (4, false)
                };
                let never = !index && b & 0x10 != 0;
                let idx = c.int(prefix)?;
                let budget = self
                    .max_header_list_size
                    .saturating_sub(list + ENTRY_OVERHEAD);
                let name = if idx == 0 {
                    self.read_string(&mut c, block, budget)?
                } else {
                    let n = self.get_name(idx)?;
                    if n.len() > budget {
                        return Err(DecodeError::HeaderListTooLarge);
                    }
                    n
                };
                let budget = budget - name.len();
                let value = self.read_string(&mut c, block, budget)?;
                list += name.len() + value.len() + ENTRY_OVERHEAD;
                seen_field = true;
                if index {
                    match self.insert(&name, &value) {
                        Some((n, v)) => f(n, v, false),
                        None => f(name, value, false),
                    }
                } else {
                    f(name, value, never);
                }
            }
        }
        Ok(())
    }

    fn read_string(
        &mut self,
        c: &mut Cursor<'_>,
        block: &Bytes,
        budget: usize,
    ) -> Result<Bytes, DecodeError> {
        let huff = c.src.get(c.pos).ok_or(DecodeError::Truncated)? & 0x80 != 0;
        let len = c.int(7)? as usize;
        let too_long = |budget: usize| {
            if budget < self.max_string_len {
                DecodeError::HeaderListTooLarge
            } else {
                DecodeError::StringTooLong
            }
        };
        let cap = self.max_string_len.min(budget);
        if len > cap {
            return Err(too_long(budget));
        }
        let end = c
            .pos
            .checked_add(len)
            .filter(|&e| e <= c.src.len())
            .ok_or(DecodeError::Truncated)?;
        let raw = &c.src[c.pos..end];
        c.pos = end;
        if !huff {
            return Ok(block.slice(c.pos - len..c.pos));
        }
        // Huffman output is at most 8/5 of the input; reserve exactly what could be needed.
        let need = (len * 8 / 5).min(cap);
        if self.scratch.capacity() < need {
            self.scratch.reserve(need.max(SCRATCH_CHUNK));
        }
        if let Err(e) = huffman::decode(raw, &mut self.scratch, cap) {
            self.scratch.clear();
            return Err(match e {
                huffman::HuffmanError::Invalid => DecodeError::InvalidHuffman,
                huffman::HuffmanError::TooLong => too_long(budget),
            });
        }
        Ok(self.scratch.split().freeze())
    }

    /// Decode a block and build a validated head: `HeaderMap` plus `:status` (RFC 9113 §8.1-8.3).
    ///
    /// The whole block is always decoded (keeping the HPACK context in sync) even when a field
    /// is malformed; the malformation is then reported as `Stream { stream_id, PROTOCOL_ERROR }`.
    /// HPACK-level failures are connection errors. Checks: pseudo-headers precede regular
    /// fields, only `:status` (once, three digits) in responses, none in trailers, no
    /// upper-case names, valid name/value syntax, no leading/trailing SP/HTAB in values, no
    /// connection-specific headers (`connection`, `keep-alive`, `proxy-connection`,
    /// `transfer-encoding`, `upgrade`; `te` only as `trailers`).
    ///
    /// Allocations: the `HeaderMap` itself, custom header names, Huffman strings; standard
    /// names are static and value `Bytes` are zero-copy slices.
    pub fn decode_head(
        &mut self,
        block: &Bytes,
        kind: BlockKind,
        stream_id: u32,
    ) -> Result<DecodedHead, H2Error> {
        let mut headers = HeaderMap::new();
        let mut status = None;
        let mut seen_regular = false;
        let mut bad = false;
        self.decode(block, |name, value, _| {
            if bad {
                return;
            }
            if validate_field(
                kind,
                &mut seen_regular,
                &mut status,
                &mut headers,
                name,
                value,
            )
            .is_none()
            {
                bad = true;
            }
        })
        .map_err(DecodeError::into_h2)?;
        if bad || (kind == BlockKind::Response && status.is_none()) {
            return Err(H2Error::Stream {
                id: stream_id,
                reason: Reason::PROTOCOL_ERROR,
            });
        }
        Ok(DecodedHead { status, headers })
    }
}

/// Returns `None` if the field is malformed.
fn validate_field(
    kind: BlockKind,
    seen_regular: &mut bool,
    status: &mut Option<StatusCode>,
    headers: &mut HeaderMap,
    name: Bytes,
    value: Bytes,
) -> Option<()> {
    if name.is_empty() {
        return None;
    }
    if name[0] == b':' {
        if *seen_regular
            || kind == BlockKind::Trailers
            || &name[..] != b":status"
            || status.is_some()
        {
            return None;
        }
        if value.len() != 3 {
            return None;
        }
        *status = Some(StatusCode::from_bytes(&value).ok()?);
        return Some(());
    }
    *seen_regular = true;
    if name.iter().any(u8::is_ascii_uppercase) {
        return None;
    }
    let hn = HeaderName::from_bytes(&name).ok()?;
    match hn.as_str() {
        "connection" | "keep-alive" | "proxy-connection" | "transfer-encoding" | "upgrade" => {
            return None;
        }
        "te" if !value.eq_ignore_ascii_case(b"trailers") => return None,
        _ => {}
    }
    if matches!(value.first(), Some(b' ' | b'\t')) || matches!(value.last(), Some(b' ' | b'\t')) {
        return None;
    }
    let hv = HeaderValue::from_maybe_shared(value).ok()?;
    headers.append(hn, hv);
    Some(())
}

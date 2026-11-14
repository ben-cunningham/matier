//! HTTP/2 frame codec (RFC 9113 §4, §6; RFC 9218 §7).
//!
//! Decoding is zero-copy: [`decode_frame`] borrows payloads from the caller's buffer, so the
//! connection actor can turn the borrowed slices back into `Bytes` sub-slices of its pooled
//! receive block (`Bytes::slice_ref`) without copying. Decoding never allocates.
//!
//! Encoding appends to any [`bytes::BufMut`] (typically `Vec<u8>` or `BytesMut`) and never
//! allocates beyond that buffer's own growth. DATA payloads are not copied at all:
//! [`data_frame_header`] yields just the 9-byte header so the payload can be a separate iovec.
//!
//! # Error handling contract
//!
//! `decode_frame` returns `Err` for violations. Connection errors are fatal. A
//! [`H2Error::Stream`] error is returned for self-dependent PRIORITY/HEADERS and a
//! WINDOW_UPDATE with zero increment on a non-zero stream, and for a PRIORITY frame of wrong
//! length; in those cases the caller can still skip the frame with
//! `FrameHeader::decode(first 9 bytes)` and `9 + len`. (For HEADERS with a self-dependency the
//! header block is then *not* HPACK-decoded; callers that need table sync must treat that as a
//! connection error, which Matier's actor does.)

use crate::error::{H2Error, Reason};
use bytes::BufMut;

/// The client connection preface (§3.4), sent before the first SETTINGS frame.
pub const CONNECTION_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Length of the fixed frame header.
pub const HEADER_LEN: usize = 9;

/// Frame type codes.
pub mod kind {
    /// DATA.
    pub const DATA: u8 = 0x0;
    /// HEADERS.
    pub const HEADERS: u8 = 0x1;
    /// PRIORITY.
    pub const PRIORITY: u8 = 0x2;
    /// RST_STREAM.
    pub const RST_STREAM: u8 = 0x3;
    /// SETTINGS.
    pub const SETTINGS: u8 = 0x4;
    /// PUSH_PROMISE.
    pub const PUSH_PROMISE: u8 = 0x5;
    /// PING.
    pub const PING: u8 = 0x6;
    /// GOAWAY.
    pub const GOAWAY: u8 = 0x7;
    /// WINDOW_UPDATE.
    pub const WINDOW_UPDATE: u8 = 0x8;
    /// CONTINUATION.
    pub const CONTINUATION: u8 = 0x9;
    /// PRIORITY_UPDATE (RFC 9218).
    pub const PRIORITY_UPDATE: u8 = 0x10;
}

/// Frame flags.
pub mod flags {
    /// END_STREAM (DATA, HEADERS).
    pub const END_STREAM: u8 = 0x1;
    /// ACK (SETTINGS, PING).
    pub const ACK: u8 = 0x1;
    /// END_HEADERS (HEADERS, PUSH_PROMISE, CONTINUATION).
    pub const END_HEADERS: u8 = 0x4;
    /// PADDED (DATA, HEADERS, PUSH_PROMISE).
    pub const PADDED: u8 = 0x8;
    /// PRIORITY (HEADERS).
    pub const PRIORITY: u8 = 0x20;
}

/// Alias: frame decoding reports the same error type as the rest of the crate.
pub type FrameError = H2Error;

/// The 9-octet frame header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    /// Payload length (24 bits).
    pub len: u32,
    /// Frame type.
    pub kind: u8,
    /// Flags.
    pub flags: u8,
    /// Stream identifier (31 bits; the reserved bit is dropped on decode and on encode).
    pub stream_id: u32,
}

impl FrameHeader {
    /// Serialize to 9 bytes.
    #[inline]
    pub fn encode(&self) -> [u8; 9] {
        let l = self.len.to_be_bytes();
        let s = (self.stream_id & 0x7fff_ffff).to_be_bytes();
        [
            l[1], l[2], l[3], self.kind, self.flags, s[0], s[1], s[2], s[3],
        ]
    }
    /// Parse 9 bytes.
    #[inline]
    pub fn decode(b: &[u8; 9]) -> FrameHeader {
        FrameHeader {
            len: u32::from_be_bytes([0, b[0], b[1], b[2]]),
            kind: b[3],
            flags: b[4],
            stream_id: u32::from_be_bytes([b[5], b[6], b[7], b[8]]) & 0x7fff_ffff,
        }
    }
}

/// Parsed priority fields (RFC 7540 §6.2/§6.3), validated and otherwise ignored by Matier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PriorityInfo {
    /// Exclusive dependency bit.
    pub exclusive: bool,
    /// Stream dependency.
    pub dependency: u32,
    /// Weight minus one, as on the wire.
    pub weight: u8,
}

/// Iterator over the `(identifier, value)` pairs in a SETTINGS payload. No allocation.
#[derive(Clone, Debug)]
pub struct SettingsIter<'a> {
    chunks: core::slice::Iter<'a, [u8; 6]>,
}

impl Iterator for SettingsIter<'_> {
    type Item = (u16, u32);
    #[inline]
    fn next(&mut self) -> Option<(u16, u32)> {
        let c = self.chunks.next()?;
        Some((
            u16::from_be_bytes([c[0], c[1]]),
            u32::from_be_bytes([c[2], c[3], c[4], c[5]]),
        ))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.chunks.len();
        (n, Some(n))
    }
}

impl ExactSizeIterator for SettingsIter<'_> {}

/// A decoded frame borrowing from the input buffer.
#[derive(Clone, Debug)]
pub enum Frame<'a> {
    /// DATA.
    Data {
        /// Stream.
        stream_id: u32,
        /// END_STREAM flag.
        end_stream: bool,
        /// Payload with padding stripped.
        data: &'a [u8],
        /// Bytes charged to flow control: the full payload length including pad-length
        /// octet and padding (§6.1).
        flow_len: u32,
    },
    /// HEADERS.
    Headers {
        /// Stream.
        stream_id: u32,
        /// END_STREAM flag.
        end_stream: bool,
        /// END_HEADERS flag.
        end_headers: bool,
        /// Priority fields if present.
        priority: Option<PriorityInfo>,
        /// HPACK fragment (padding stripped).
        fragment: &'a [u8],
    },
    /// PRIORITY (deprecated by RFC 9113; parsed and validated only).
    Priority {
        /// Stream.
        stream_id: u32,
        /// Fields.
        priority: PriorityInfo,
    },
    /// RST_STREAM.
    RstStream {
        /// Stream.
        stream_id: u32,
        /// Error code.
        code: Reason,
    },
    /// SETTINGS.
    Settings {
        /// ACK flag (payload is then empty).
        ack: bool,
        /// Parameters.
        params: SettingsIter<'a>,
    },
    /// PUSH_PROMISE. A client that advertised ENABLE_PUSH=0 must treat receipt as PROTOCOL_ERROR.
    PushPromise {
        /// Associated stream.
        stream_id: u32,
        /// END_HEADERS flag.
        end_headers: bool,
        /// Promised stream.
        promised_id: u32,
        /// HPACK fragment.
        fragment: &'a [u8],
    },
    /// PING.
    Ping {
        /// ACK flag.
        ack: bool,
        /// Opaque data.
        data: [u8; 8],
    },
    /// GOAWAY.
    GoAway {
        /// Last stream the sender may have processed.
        last_stream_id: u32,
        /// Error code.
        code: Reason,
        /// Opaque diagnostic data.
        debug_data: &'a [u8],
    },
    /// WINDOW_UPDATE (increment is non-zero for stream 0; see [`decode_frame`] for zero).
    WindowUpdate {
        /// Stream (0 = connection).
        stream_id: u32,
        /// Increment, 1..=2^31-1.
        increment: u32,
    },
    /// CONTINUATION.
    Continuation {
        /// Stream.
        stream_id: u32,
        /// END_HEADERS flag.
        end_headers: bool,
        /// HPACK fragment.
        fragment: &'a [u8],
    },
    /// PRIORITY_UPDATE (RFC 9218 §7.1).
    PriorityUpdate {
        /// Stream whose priority changes.
        prioritized_stream_id: u32,
        /// Priority Field Value (structured field dictionary), unparsed.
        field_value: &'a [u8],
    },
    /// Unknown type: must be ignored (§4.1).
    Unknown {
        /// Type code.
        kind: u8,
        /// Stream.
        stream_id: u32,
        /// Flags.
        flags: u8,
        /// Payload.
        payload: &'a [u8],
    },
}

fn conn(r: Reason, m: &'static str) -> H2Error {
    H2Error::Connection(r, m)
}

fn require_stream(h: &FrameHeader) -> Result<(), H2Error> {
    if h.stream_id == 0 {
        Err(conn(
            Reason::PROTOCOL_ERROR,
            "frame requires non-zero stream id",
        ))
    } else {
        Ok(())
    }
}

fn require_zero_stream(h: &FrameHeader) -> Result<(), H2Error> {
    if h.stream_id != 0 {
        Err(conn(Reason::PROTOCOL_ERROR, "frame requires stream id 0"))
    } else {
        Ok(())
    }
}

/// Header-only validation: stream-id rules and fixed sizes (so oversized/malformed frames are
/// rejected before their payload has even arrived).
fn validate_header(h: &FrameHeader) -> Result<(), H2Error> {
    let size = |ok: bool, m: &'static str| {
        if ok {
            Ok(())
        } else {
            Err(conn(Reason::FRAME_SIZE_ERROR, m))
        }
    };
    match h.kind {
        kind::DATA | kind::CONTINUATION => require_stream(h),
        kind::HEADERS => {
            require_stream(h)?;
            let mut min = 0;
            if h.flags & flags::PADDED != 0 {
                min += 1;
            }
            if h.flags & flags::PRIORITY != 0 {
                min += 5;
            }
            size(h.len >= min, "HEADERS frame too short")
        }
        kind::PRIORITY => {
            require_stream(h)?;
            if h.len != 5 {
                return Err(H2Error::Stream {
                    id: h.stream_id,
                    reason: Reason::FRAME_SIZE_ERROR,
                });
            }
            Ok(())
        }
        kind::RST_STREAM => {
            require_stream(h)?;
            size(h.len == 4, "RST_STREAM length != 4")
        }
        kind::SETTINGS => {
            require_zero_stream(h)?;
            size(
                h.len.is_multiple_of(6),
                "SETTINGS length not a multiple of 6",
            )?;
            size(
                h.flags & flags::ACK == 0 || h.len == 0,
                "SETTINGS ack with payload",
            )
        }
        kind::PUSH_PROMISE => {
            require_stream(h)?;
            size(
                h.len >= 4 + u32::from(h.flags & flags::PADDED != 0),
                "PUSH_PROMISE too short",
            )
        }
        kind::PING => {
            require_zero_stream(h)?;
            size(h.len == 8, "PING length != 8")
        }
        kind::GOAWAY => {
            require_zero_stream(h)?;
            size(h.len >= 8, "GOAWAY too short")
        }
        kind::WINDOW_UPDATE => size(h.len == 4, "WINDOW_UPDATE length != 4"),
        kind::PRIORITY_UPDATE => {
            require_zero_stream(h)?;
            size(h.len >= 4, "PRIORITY_UPDATE too short")
        }
        _ => Ok(()),
    }
}

#[inline]
fn be32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

fn parse_priority(b: &[u8]) -> PriorityInfo {
    let v = be32(b);
    PriorityInfo {
        exclusive: v & 0x8000_0000 != 0,
        dependency: v & 0x7fff_ffff,
        weight: b[4],
    }
}

/// Strip padding from `p` if the PADDED flag is set. `p` is the payload after any fixed
/// fields that precede the padding length octet position handling is done by the caller.
fn strip_padding(p: &[u8], padded: bool) -> Result<(&[u8], usize), H2Error> {
    if !padded {
        return Ok((p, 0));
    }
    let Some((&pad, rest)) = p.split_first() else {
        return Err(conn(Reason::FRAME_SIZE_ERROR, "missing pad length"));
    };
    Ok((rest, pad as usize))
}

fn unpad(rest: &[u8], pad: usize) -> Result<&[u8], H2Error> {
    if pad > rest.len() {
        return Err(conn(Reason::PROTOCOL_ERROR, "padding exceeds payload"));
    }
    Ok(&rest[..rest.len() - pad])
}

/// Decode one frame from the front of `buf`.
///
/// * `Ok(None)`: not enough bytes yet (header or payload incomplete).
/// * `Ok(Some((frame, consumed)))`: `consumed` is `9 + payload length`.
/// * `Err(_)`: see the module docs for the connection/stream split.
///
/// `max_frame_size` is the SETTINGS_MAX_FRAME_SIZE *we* advertised; larger frames are rejected as
/// soon as their header is visible. A WINDOW_UPDATE with increment 0 is a connection
/// PROTOCOL_ERROR on stream 0 and `Stream { PROTOCOL_ERROR }` otherwise.
pub fn decode_frame(
    buf: &[u8],
    max_frame_size: u32,
) -> Result<Option<(Frame<'_>, usize)>, FrameError> {
    let Some(hb) = buf.first_chunk::<9>() else {
        return Ok(None);
    };
    let h = FrameHeader::decode(hb);
    if h.len > max_frame_size {
        return Err(conn(
            Reason::FRAME_SIZE_ERROR,
            "frame exceeds SETTINGS_MAX_FRAME_SIZE",
        ));
    }
    validate_header(&h)?;
    let total = HEADER_LEN + h.len as usize;
    if buf.len() < total {
        return Ok(None);
    }
    let payload = &buf[HEADER_LEN..total];
    let id = h.stream_id;
    let f = h.flags;
    let frame = match h.kind {
        kind::DATA => {
            let (rest, pad) = strip_padding(payload, f & flags::PADDED != 0)?;
            Frame::Data {
                stream_id: id,
                end_stream: f & flags::END_STREAM != 0,
                data: unpad(rest, pad)?,
                flow_len: h.len,
            }
        }
        kind::HEADERS => {
            let (mut rest, pad) = strip_padding(payload, f & flags::PADDED != 0)?;
            let mut priority = None;
            if f & flags::PRIORITY != 0 {
                let p = parse_priority(rest);
                if p.dependency == id {
                    return Err(H2Error::Stream {
                        id,
                        reason: Reason::PROTOCOL_ERROR,
                    });
                }
                priority = Some(p);
                rest = &rest[5..];
            }
            Frame::Headers {
                stream_id: id,
                end_stream: f & flags::END_STREAM != 0,
                end_headers: f & flags::END_HEADERS != 0,
                priority,
                fragment: unpad(rest, pad)?,
            }
        }
        kind::PRIORITY => {
            let p = parse_priority(payload);
            if p.dependency == id {
                return Err(H2Error::Stream {
                    id,
                    reason: Reason::PROTOCOL_ERROR,
                });
            }
            Frame::Priority {
                stream_id: id,
                priority: p,
            }
        }
        kind::RST_STREAM => Frame::RstStream {
            stream_id: id,
            code: Reason(be32(payload)),
        },
        kind::SETTINGS => Frame::Settings {
            ack: f & flags::ACK != 0,
            params: SettingsIter {
                chunks: payload.as_chunks::<6>().0.iter(),
            },
        },
        kind::PUSH_PROMISE => {
            let (rest, pad) = strip_padding(payload, f & flags::PADDED != 0)?;
            if rest.len() < 4 {
                return Err(conn(Reason::FRAME_SIZE_ERROR, "PUSH_PROMISE too short"));
            }
            Frame::PushPromise {
                stream_id: id,
                end_headers: f & flags::END_HEADERS != 0,
                promised_id: be32(rest) & 0x7fff_ffff,
                fragment: unpad(&rest[4..], pad)?,
            }
        }
        kind::PING => {
            let mut data = [0u8; 8];
            data.copy_from_slice(payload);
            Frame::Ping {
                ack: f & flags::ACK != 0,
                data,
            }
        }
        kind::GOAWAY => Frame::GoAway {
            last_stream_id: be32(payload) & 0x7fff_ffff,
            code: Reason(be32(&payload[4..])),
            debug_data: &payload[8..],
        },
        kind::WINDOW_UPDATE => {
            let inc = be32(payload) & 0x7fff_ffff;
            if inc == 0 {
                return Err(if id == 0 {
                    conn(Reason::PROTOCOL_ERROR, "WINDOW_UPDATE with zero increment")
                } else {
                    H2Error::Stream {
                        id,
                        reason: Reason::PROTOCOL_ERROR,
                    }
                });
            }
            Frame::WindowUpdate {
                stream_id: id,
                increment: inc,
            }
        }
        kind::CONTINUATION => Frame::Continuation {
            stream_id: id,
            end_headers: f & flags::END_HEADERS != 0,
            fragment: payload,
        },
        kind::PRIORITY_UPDATE => {
            let sid = be32(payload) & 0x7fff_ffff;
            if sid == 0 {
                return Err(conn(Reason::PROTOCOL_ERROR, "PRIORITY_UPDATE for stream 0"));
            }
            Frame::PriorityUpdate {
                prioritized_stream_id: sid,
                field_value: &payload[4..],
            }
        }
        k => Frame::Unknown {
            kind: k,
            stream_id: id,
            flags: f,
            payload,
        },
    };
    Ok(Some((frame, total)))
}

// ---------------------------------------------------------------------------------------------
// Encoders
// ---------------------------------------------------------------------------------------------

#[inline]
fn put_header(out: &mut impl BufMut, len: usize, kind: u8, flags: u8, stream_id: u32) {
    debug_assert!(len <= 0xff_ffff);
    out.put_slice(
        &FrameHeader {
            len: len as u32,
            kind,
            flags,
            stream_id,
        }
        .encode(),
    );
}

/// The 9-byte header of a DATA frame, so the payload can be sent as its own iovec (zero copy).
/// `len` must fit in the peer's MAX_FRAME_SIZE; padding is never added.
#[inline]
pub fn data_frame_header(stream_id: u32, len: u32, end_stream: bool) -> [u8; 9] {
    FrameHeader {
        len,
        kind: kind::DATA,
        flags: if end_stream { flags::END_STREAM } else { 0 },
        stream_id,
    }
    .encode()
}

/// SETTINGS frame with the given parameters.
pub fn encode_settings(out: &mut impl BufMut, params: &[(u16, u32)]) {
    put_header(out, params.len() * 6, kind::SETTINGS, 0, 0);
    for &(id, v) in params {
        out.put_u16(id);
        out.put_u32(v);
    }
}

/// SETTINGS acknowledgement (empty payload, ACK flag).
pub fn encode_settings_ack(out: &mut impl BufMut) {
    put_header(out, 0, kind::SETTINGS, flags::ACK, 0);
}

/// PING (or PING ack).
pub fn encode_ping(out: &mut impl BufMut, ack: bool, data: [u8; 8]) {
    put_header(out, 8, kind::PING, if ack { flags::ACK } else { 0 }, 0);
    out.put_slice(&data);
}

/// GOAWAY.
pub fn encode_goaway(out: &mut impl BufMut, last_stream_id: u32, code: Reason, debug_data: &[u8]) {
    put_header(out, 8 + debug_data.len(), kind::GOAWAY, 0, 0);
    out.put_u32(last_stream_id & 0x7fff_ffff);
    out.put_u32(code.0);
    out.put_slice(debug_data);
}

/// RST_STREAM.
pub fn encode_rst_stream(out: &mut impl BufMut, stream_id: u32, code: Reason) {
    put_header(out, 4, kind::RST_STREAM, 0, stream_id);
    out.put_u32(code.0);
}

/// WINDOW_UPDATE (`stream_id` 0 for the connection window). `increment` must be 1..=2^31-1.
pub fn encode_window_update(out: &mut impl BufMut, stream_id: u32, increment: u32) {
    debug_assert!((1..=0x7fff_ffff).contains(&increment));
    put_header(out, 4, kind::WINDOW_UPDATE, 0, stream_id);
    out.put_u32(increment & 0x7fff_ffff);
}

/// PRIORITY_UPDATE (RFC 9218 §7.1) with a Priority Field Value such as `b"u=3, i"`.
pub fn encode_priority_update(
    out: &mut impl BufMut,
    prioritized_stream_id: u32,
    field_value: &[u8],
) {
    put_header(out, 4 + field_value.len(), kind::PRIORITY_UPDATE, 0, 0);
    out.put_u32(prioritized_stream_id & 0x7fff_ffff);
    out.put_slice(field_value);
}

/// Write an already-HPACK-encoded header block as HEADERS followed by as many CONTINUATION
/// frames as `max_frame_size` requires. END_HEADERS is set on the last frame. An empty block
/// still produces one HEADERS frame. Copies `block` once into `out`.
pub fn encode_headers(
    out: &mut impl BufMut,
    stream_id: u32,
    end_stream: bool,
    block: &[u8],
    max_frame_size: u32,
) {
    let max = max_frame_size.max(1) as usize;
    let mut chunks = block.chunks(max);
    let first = chunks.next().unwrap_or(&[]);
    let single = first.len() == block.len();
    let mut fl = if end_stream { flags::END_STREAM } else { 0 };
    if single {
        fl |= flags::END_HEADERS;
    }
    put_header(out, first.len(), kind::HEADERS, fl, stream_id);
    out.put_slice(first);
    let mut rest = block.len() - first.len();
    for c in chunks {
        rest -= c.len();
        put_header(
            out,
            c.len(),
            kind::CONTINUATION,
            if rest == 0 { flags::END_HEADERS } else { 0 },
            stream_id,
        );
        out.put_slice(c);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(kind: u8, fl: u8, sid: u32, payload: &[u8]) -> Vec<u8> {
        let mut v = FrameHeader {
            len: payload.len() as u32,
            kind,
            flags: fl,
            stream_id: sid,
        }
        .encode()
        .to_vec();
        v.extend_from_slice(payload);
        v
    }
    fn dec(b: &[u8]) -> Result<Option<(Frame<'_>, usize)>, H2Error> {
        decode_frame(b, 16_384)
    }
    fn err(b: &[u8]) -> H2Error {
        dec(b).unwrap_err()
    }
    fn is_conn(e: H2Error, r: Reason) -> bool {
        matches!(e, H2Error::Connection(x, _) if x == r)
    }

    #[test]
    fn header_roundtrip() {
        let h = FrameHeader {
            len: 0xabcdef,
            kind: 9,
            flags: 0x5,
            stream_id: 0x7fff_ffff,
        };
        assert_eq!(FrameHeader::decode(&h.encode()), h);
        // reserved bit ignored
        let mut b = h.encode();
        b[5] |= 0x80;
        assert_eq!(FrameHeader::decode(&b), h);
    }

    #[test]
    fn partial_returns_none() {
        let f = frame(kind::PING, 0, 0, &[1; 8]);
        for n in 0..f.len() {
            assert!(dec(&f[..n]).unwrap().is_none(), "n={n}");
        }
        let (fr, used) = dec(&f).unwrap().unwrap();
        assert_eq!(used, 17);
        assert!(matches!(fr, Frame::Ping { ack: false, data } if data == [1; 8]));
    }

    #[test]
    fn oversize_rejected_early() {
        // header only: len = 16385, DATA, stream 1
        let f = FrameHeader {
            len: 16_385,
            kind: kind::DATA,
            flags: 0,
            stream_id: 1,
        }
        .encode();
        assert!(is_conn(err(&f[..9]), Reason::FRAME_SIZE_ERROR));
        assert!(decode_frame(&f[..9], 1 << 20).unwrap().is_none());
    }

    #[test]
    fn data_padding() {
        let f = frame(
            kind::DATA,
            flags::PADDED | flags::END_STREAM,
            1,
            &[2, b'a', b'b', 0, 0],
        );
        match dec(&f).unwrap().unwrap().0 {
            Frame::Data {
                stream_id: 1,
                end_stream: true,
                data,
                flow_len: 5,
            } => assert_eq!(data, b"ab"),
            o => panic!("{o:?}"),
        }
        // pad len == payload len
        assert!(is_conn(
            err(&frame(kind::DATA, flags::PADDED, 1, &[3, 0, 0])),
            Reason::PROTOCOL_ERROR
        ));
        // exactly all padding, empty data is fine
        assert!(
            dec(&frame(kind::DATA, flags::PADDED, 1, &[2, 0, 0]))
                .unwrap()
                .is_some()
        );
        // PADDED with empty payload
        assert!(is_conn(
            err(&frame(kind::DATA, flags::PADDED, 1, &[])),
            Reason::FRAME_SIZE_ERROR
        ));
        // stream 0
        assert!(is_conn(
            err(&frame(kind::DATA, 0, 0, b"x")),
            Reason::PROTOCOL_ERROR
        ));
    }

    #[test]
    fn headers_variants() {
        let f = frame(
            kind::HEADERS,
            flags::END_HEADERS | flags::END_STREAM,
            3,
            b"abc",
        );
        assert!(matches!(
            dec(&f).unwrap().unwrap().0,
            Frame::Headers { stream_id: 3, end_stream: true, end_headers: true, priority: None, fragment } if fragment == b"abc"
        ));
        // with priority + padding
        let mut p = vec![1u8];
        p.extend_from_slice(&(0x8000_0001u32).to_be_bytes());
        p.push(15);
        p.extend_from_slice(b"xy");
        p.push(0);
        let f = frame(
            kind::HEADERS,
            flags::PADDED | flags::PRIORITY | flags::END_HEADERS,
            3,
            &p,
        );
        match dec(&f).unwrap().unwrap().0 {
            Frame::Headers {
                priority: Some(pi),
                fragment,
                ..
            } => {
                assert_eq!(
                    pi,
                    PriorityInfo {
                        exclusive: true,
                        dependency: 1,
                        weight: 15
                    }
                );
                assert_eq!(fragment, b"xy");
            }
            o => panic!("{o:?}"),
        }
        // self dependency is a stream error
        let mut p = 3u32.to_be_bytes().to_vec();
        p.push(0);
        assert_eq!(
            err(&frame(kind::HEADERS, flags::PRIORITY, 3, &p)),
            H2Error::Stream {
                id: 3,
                reason: Reason::PROTOCOL_ERROR
            }
        );
        // priority flag but too short
        assert!(is_conn(
            err(&frame(kind::HEADERS, flags::PRIORITY, 3, &[0; 4])),
            Reason::FRAME_SIZE_ERROR
        ));
        // padding larger than what remains after priority fields
        let mut p = vec![3u8, 0, 0, 0, 2, 0];
        p.push(0);
        assert!(is_conn(
            err(&frame(
                kind::HEADERS,
                flags::PADDED | flags::PRIORITY,
                3,
                &p
            )),
            Reason::PROTOCOL_ERROR
        ));
        assert!(is_conn(
            err(&frame(kind::HEADERS, 0, 0, b"")),
            Reason::PROTOCOL_ERROR
        ));
    }

    #[test]
    fn priority_frame() {
        let mut p = 5u32.to_be_bytes().to_vec();
        p.push(9);
        assert!(matches!(
            dec(&frame(kind::PRIORITY, 0, 1, &p)).unwrap().unwrap().0,
            Frame::Priority {
                stream_id: 1,
                priority: PriorityInfo {
                    dependency: 5,
                    weight: 9,
                    exclusive: false
                }
            }
        ));
        let mut me = 1u32.to_be_bytes().to_vec();
        me.push(0);
        assert_eq!(
            err(&frame(kind::PRIORITY, 0, 1, &me)),
            H2Error::Stream {
                id: 1,
                reason: Reason::PROTOCOL_ERROR
            }
        );
        assert_eq!(
            err(&frame(kind::PRIORITY, 0, 1, &p[..4])),
            H2Error::Stream {
                id: 1,
                reason: Reason::FRAME_SIZE_ERROR
            }
        );
        assert!(is_conn(
            err(&frame(kind::PRIORITY, 0, 0, &p)),
            Reason::PROTOCOL_ERROR
        ));
    }

    #[test]
    fn rst_stream() {
        let f = frame(kind::RST_STREAM, 0, 5, &8u32.to_be_bytes());
        assert!(matches!(
            dec(&f).unwrap().unwrap().0,
            Frame::RstStream {
                stream_id: 5,
                code: Reason::CANCEL
            }
        ));
        assert!(is_conn(
            err(&frame(kind::RST_STREAM, 0, 5, &[0; 3])),
            Reason::FRAME_SIZE_ERROR
        ));
        assert!(is_conn(
            err(&frame(kind::RST_STREAM, 0, 0, &[0; 4])),
            Reason::PROTOCOL_ERROR
        ));
    }

    #[test]
    fn settings_frame() {
        let mut v = Vec::new();
        encode_settings(&mut v, &[(1, 100), (4, 200)]);
        match dec(&v).unwrap().unwrap().0 {
            Frame::Settings { ack: false, params } => {
                assert_eq!(params.len(), 2);
                assert_eq!(params.collect::<Vec<_>>(), vec![(1, 100), (4, 200)]);
            }
            o => panic!("{o:?}"),
        }
        assert!(is_conn(
            err(&frame(kind::SETTINGS, 0, 0, &[0; 7])),
            Reason::FRAME_SIZE_ERROR
        ));
        assert!(is_conn(
            err(&frame(kind::SETTINGS, flags::ACK, 0, &[0; 6])),
            Reason::FRAME_SIZE_ERROR
        ));
        assert!(is_conn(
            err(&frame(kind::SETTINGS, 0, 1, &[])),
            Reason::PROTOCOL_ERROR
        ));
        let mut a = Vec::new();
        encode_settings_ack(&mut a);
        assert!(matches!(
            dec(&a).unwrap().unwrap().0,
            Frame::Settings { ack: true, .. }
        ));
    }

    #[test]
    fn push_promise() {
        let mut p = vec![1u8];
        p.extend_from_slice(&2u32.to_be_bytes());
        p.extend_from_slice(b"hb");
        p.push(0);
        let f = frame(
            kind::PUSH_PROMISE,
            flags::PADDED | flags::END_HEADERS,
            1,
            &p,
        );
        assert!(matches!(
            dec(&f).unwrap().unwrap().0,
            Frame::PushPromise { stream_id: 1, promised_id: 2, end_headers: true, fragment } if fragment == b"hb"
        ));
        assert!(is_conn(
            err(&frame(kind::PUSH_PROMISE, 0, 1, &[0; 3])),
            Reason::FRAME_SIZE_ERROR
        ));
        assert!(is_conn(
            err(&frame(kind::PUSH_PROMISE, 0, 0, &[0; 4])),
            Reason::PROTOCOL_ERROR
        ));
    }

    #[test]
    fn ping_goaway() {
        let mut v = Vec::new();
        encode_ping(&mut v, true, *b"12345678");
        assert!(
            matches!(dec(&v).unwrap().unwrap().0, Frame::Ping { ack: true, data } if &data == b"12345678")
        );
        assert!(is_conn(
            err(&frame(kind::PING, 0, 0, &[0; 7])),
            Reason::FRAME_SIZE_ERROR
        ));
        assert!(is_conn(
            err(&frame(kind::PING, 0, 1, &[0; 8])),
            Reason::PROTOCOL_ERROR
        ));

        let mut g = Vec::new();
        encode_goaway(&mut g, 7, Reason::ENHANCE_YOUR_CALM, b"slow down");
        assert!(matches!(
            dec(&g).unwrap().unwrap().0,
            Frame::GoAway { last_stream_id: 7, code: Reason::ENHANCE_YOUR_CALM, debug_data } if debug_data == b"slow down"
        ));
        assert!(is_conn(
            err(&frame(kind::GOAWAY, 0, 0, &[0; 7])),
            Reason::FRAME_SIZE_ERROR
        ));
        assert!(is_conn(
            err(&frame(kind::GOAWAY, 0, 1, &[0; 8])),
            Reason::PROTOCOL_ERROR
        ));
    }

    #[test]
    fn window_update() {
        let mut v = Vec::new();
        encode_window_update(&mut v, 3, 1000);
        assert!(matches!(
            dec(&v).unwrap().unwrap().0,
            Frame::WindowUpdate {
                stream_id: 3,
                increment: 1000
            }
        ));
        assert!(is_conn(
            err(&frame(kind::WINDOW_UPDATE, 0, 0, &0u32.to_be_bytes())),
            Reason::PROTOCOL_ERROR
        ));
        assert_eq!(
            err(&frame(kind::WINDOW_UPDATE, 0, 3, &0u32.to_be_bytes())),
            H2Error::Stream {
                id: 3,
                reason: Reason::PROTOCOL_ERROR
            }
        );
        assert!(is_conn(
            err(&frame(kind::WINDOW_UPDATE, 0, 0, &[0, 0, 1])),
            Reason::FRAME_SIZE_ERROR
        ));
    }

    #[test]
    fn continuation_and_unknown() {
        let f = frame(kind::CONTINUATION, flags::END_HEADERS, 1, b"zz");
        assert!(matches!(
            dec(&f).unwrap().unwrap().0,
            Frame::Continuation { stream_id: 1, end_headers: true, fragment } if fragment == b"zz"
        ));
        assert!(is_conn(
            err(&frame(kind::CONTINUATION, 0, 0, b"")),
            Reason::PROTOCOL_ERROR
        ));
        let f = frame(0xee, 0x7, 0, b"hello");
        assert!(matches!(
            dec(&f).unwrap().unwrap().0,
            Frame::Unknown { kind: 0xee, .. }
        ));
    }

    #[test]
    fn priority_update() {
        let mut v = Vec::new();
        encode_priority_update(&mut v, 5, b"u=2, i");
        assert!(matches!(
            dec(&v).unwrap().unwrap().0,
            Frame::PriorityUpdate { prioritized_stream_id: 5, field_value } if field_value == b"u=2, i"
        ));
        assert!(is_conn(
            err(&frame(kind::PRIORITY_UPDATE, 0, 0, &0u32.to_be_bytes())),
            Reason::PROTOCOL_ERROR
        ));
        assert!(is_conn(
            err(&frame(kind::PRIORITY_UPDATE, 0, 0, &[0, 0, 1])),
            Reason::FRAME_SIZE_ERROR
        ));
        assert!(is_conn(
            err(&frame(kind::PRIORITY_UPDATE, 0, 1, &5u32.to_be_bytes())),
            Reason::PROTOCOL_ERROR
        ));
    }

    #[test]
    fn headers_splitting() {
        let block = vec![7u8; 40];
        let mut v = Vec::new();
        encode_headers(&mut v, 1, true, &block, 16);
        let mut off = 0;
        let mut seen = Vec::new();
        while off < v.len() {
            let (f, n) = decode_frame(&v[off..], 16).unwrap().unwrap();
            seen.push(match f {
                Frame::Headers {
                    end_stream,
                    end_headers,
                    fragment,
                    ..
                } => (0, end_stream, end_headers, fragment.len()),
                Frame::Continuation {
                    end_headers,
                    fragment,
                    ..
                } => (1, false, end_headers, fragment.len()),
                o => panic!("{o:?}"),
            });
            off += n;
        }
        assert_eq!(
            seen,
            vec![
                (0, true, false, 16),
                (1, false, false, 16),
                (1, false, true, 8)
            ]
        );
        let mut e = Vec::new();
        encode_headers(&mut e, 1, false, &[], 16);
        assert_eq!(e.len(), 9);
        assert_eq!(e[4], flags::END_HEADERS);
        let mut x = Vec::new();
        encode_headers(&mut x, 1, false, &[1; 16], 16);
        assert_eq!(x.len(), 25); // exact fit: single frame
    }

    #[test]
    fn data_header_and_rst() {
        let h = data_frame_header(9, 300, true);
        assert_eq!(
            FrameHeader::decode(&h),
            FrameHeader {
                len: 300,
                kind: 0,
                flags: 1,
                stream_id: 9
            }
        );
        let mut v = Vec::new();
        encode_rst_stream(&mut v, 9, Reason::CANCEL);
        assert_eq!(v.len(), 13);
        assert_eq!(&CONNECTION_PREFACE[..3], b"PRI");
    }

    #[test]
    fn many_frames_sequence() {
        let mut v = Vec::new();
        encode_settings_ack(&mut v);
        encode_ping(&mut v, false, [0; 8]);
        v.extend_from_slice(&data_frame_header(1, 3, false));
        v.extend_from_slice(b"abc");
        let mut off = 0;
        let mut n = 0;
        while let Some((_, used)) = decode_frame(&v[off..], 16_384).unwrap() {
            off += used;
            n += 1;
        }
        assert_eq!((n, off), (3, v.len()));
    }
}

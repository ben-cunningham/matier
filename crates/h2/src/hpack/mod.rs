//! HPACK header compression (RFC 7541).
//!
//! * [`Decoder`] decodes header blocks into `(name, value)` pairs of [`bytes::Bytes`],
//!   slicing the input block when strings are not Huffman-coded, and offers
//!   [`Decoder::decode_head`] to build an `http::HeaderMap` with RFC 9113 §8 validation.
//! * [`Encoder`] encodes requests (and arbitrary fields) into a `Vec<u8>`, with a dynamic table
//!   or in stateless mode.
//! * [`huffman`] holds the Huffman coder.

mod decoder;
mod encoder;
pub mod huffman;
mod huffman_table;
pub mod static_table;

pub use decoder::{BlockKind, DecodeError, DecodedHead, Decoder};
pub use encoder::{Encoder, SensitivePolicy, is_connection_specific};

/// Per-entry overhead in dynamic-table size accounting (RFC 7541 §4.1).
pub const ENTRY_OVERHEAD: usize = 32;

/// Encode an HPACK integer (§5.1) with `prefix` bits; `flags` are the high bits of the first octet.
#[inline]
pub(crate) fn encode_int(out: &mut Vec<u8>, flags: u8, prefix: u8, mut value: usize) {
    let max = (1usize << prefix) - 1;
    if value < max {
        out.push(flags | value as u8);
        return;
    }
    out.push(flags | max as u8);
    value -= max;
    while value >= 128 {
        out.push((value & 0x7f) as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

#[cfg(test)]
mod tests;

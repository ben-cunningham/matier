//! `matier-h1`: a sans-IO HTTP/1.1 client codec (RFC 9112 / RFC 9110).
//!
//! No sockets, no async runtime, no tasks: everything here is a pure function or a small
//! state machine over byte slices, so it is deterministic and fuzzable.
//!
//! * [`encode_request_head`] writes a request head into a caller-owned `Vec<u8>`.
//! * [`chunk_header`], [`CHUNK_END`] and [`encode_last_chunk`] frame chunked request bodies.
//! * [`parse_response_head`] parses a response head from a contiguous buffer.
//! * [`response_body_kind`] and [`keep_alive`] implement RFC 9112 section 6.3 and 9.3.
//! * [`ChunkedDecoder`] and [`LengthDecoder`] decode bodies without copying: they report
//!   byte ranges into the caller's input.
//!
//! # Allocation summary
//!
//! | operation | allocations |
//! |---|---|
//! | `encode_request_head` | 0 if `dst` has spare capacity, else at most one `reserve` growth |
//! | `chunk_header` | 0 (stack buffer) |
//! | `encode_last_chunk` | 0 (same caveat as above) |
//! | `parse_response_head` | `Partial`: 0. `Complete`: 1 (head copy) + 1 (`Bytes` shared state, created by the first slice) + 2 (`HeaderMap` indices and entries) + 1 per non-standard header name + growth of the extra-values vector when a header repeats. Max headers above 128 adds one scratch `Vec`. |
//! | `response_body_kind`, `keep_alive` | 0 |
//! | `ChunkedDecoder::decode`, `LengthDecoder::decode` | 0 (trailer collection, if enabled, buffers trailer bytes and parses them into a `HeaderMap` when taken) |

mod chunked;
mod encode;
mod error;
mod framing;
mod parse;

pub use chunked::{ChunkedDecoder, Decoded, LengthDecoder};
pub use encode::{
    BodyLength, CHUNK_END, EncodeOptions, chunk_header, encode_last_chunk, encode_request_head,
};
pub use error::{ChunkedError, EncodeError, FramingError, ParseError};
pub use framing::{BodyKind, Framing, keep_alive, response_body_kind};
pub use parse::{Limits, ParseStatus, ResponseHead, parse_response_head};

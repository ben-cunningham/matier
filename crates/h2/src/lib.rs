//! Sans-IO HTTP/2 framing, HPACK and flow-control primitives (RFC 9113, RFC 7541, RFC 9218).
//!
//! Nothing here performs I/O, spawns tasks or reads clocks. Every function is a pure
//! transformation of bytes and state, which makes the crate deterministic and fuzzable.
//!
//! * [`frame`]: zero-copy frame decoding (borrowed payloads) and frame encoders.
//! * [`settings`]: SETTINGS parameters, validation and defaults.
//! * [`flow`]: flow-control window arithmetic.
//! * [`hpack`]: HPACK decoder and encoder (RFC 7541) with Huffman coding.
//!
//! The stream state machine and connection actor live elsewhere; these pieces are designed
//! so that the actor can drive them without copying payload bytes.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod error;
pub mod flow;
pub mod frame;
pub mod hpack;
pub mod settings;

pub use error::{H2Error, Reason};

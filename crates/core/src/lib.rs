//! Shared types for Matier: buffer facade over `bytesbuf`, request [`Body`], [`Error`] with recovery
//! classification, [`OriginKey`], the [`Observer`] instrumentation hooks and per-request extension types.

pub mod body;
pub mod buf;
pub mod error;
pub mod ext;
pub mod observer;
pub mod origin;

pub use body::{Body, BodySender, BodyStream};
pub use buf::{BytesBuf, BytesView};
pub use error::{Error, ErrorKind, Recovery, Result};
pub use ext::{BodyTimeout, ConnectionInfo, Priority, ResponseTimeout, UriTemplate};
pub use layered::Service;
pub use observer::{ConnectPhase, NoopObserver, Observer, ScaleReason};
pub use origin::{OriginKey, Protocol, Scheme};

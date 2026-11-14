//! Zero-copy body decoders.

use crate::error::ChunkedError;
use http::HeaderMap;
use std::ops::Range;

/// Result of one decode step. Call again with `input[consumed..]` (plus any new bytes)
/// until nothing is consumed, no data is produced and `done` is false: that means "need more".
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Decoded {
    /// Bytes of `input` consumed.
    pub consumed: usize,
    /// At most one range of body bytes inside `input`.
    pub data: Option<Range<usize>>,
    /// The body (including trailers) is complete.
    pub done: bool,
}

#[derive(Debug, Clone, Copy)]
enum State {
    /// Reading hex digits of the chunk size.
    Size {
        digits: u8,
        value: u64,
    },
    /// After the size: skipping extension / whitespace until the line ends.
    Ext {
        value: u64,
        ext_len: u32,
    },
    /// Saw CR in the size line, expect LF.
    SizeLf {
        value: u64,
    },
    Data {
        remaining: u64,
    },
    /// Expect CR (or LF) after chunk data.
    DataEnd,
    DataLf,
    /// Start of a trailer line (or the final empty line).
    TrailerStart,
    /// Inside a trailer line.
    Trailer {
        saw_cr: bool,
    },
    /// Saw CR on the empty final line.
    EndLf,
    Done,
}

/// Incremental chunked-transfer decoder (RFC 9112 section 7.1). Allocation free unless
/// trailer collection is enabled and trailers are present.
///
/// Leniency: a bare LF is accepted as a line end; a CR must always be followed by LF.
#[derive(Debug, Clone)]
pub struct ChunkedDecoder {
    state: State,
    max_ext: u32,
    max_trailer: usize,
    trailer_bytes: usize,
    collect: bool,
    trailers: Vec<u8>,
}

impl Default for ChunkedDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkedDecoder {
    /// Defaults: extensions bounded to 4 KiB per chunk line, trailers to 16 KiB and skipped.
    pub fn new() -> Self {
        Self {
            state: State::Size {
                digits: 0,
                value: 0,
            },
            max_ext: 4096,
            max_trailer: 16 * 1024,
            trailer_bytes: 0,
            collect: false,
            trailers: Vec::new(),
        }
    }

    /// Set the per-chunk-line extension bound and total trailer bound.
    pub fn with_limits(mut self, max_ext: u32, max_trailer: usize) -> Self {
        self.max_ext = max_ext;
        self.max_trailer = max_trailer;
        self
    }

    /// Buffer trailer bytes so [`take_trailers`](Self::take_trailers) can parse them.
    pub fn collect_trailers(mut self) -> Self {
        self.collect = true;
        self
    }

    /// True once the terminating chunk and trailers were consumed.
    pub fn is_done(&self) -> bool {
        matches!(self.state, State::Done)
    }

    /// Parse collected trailers (`None` if none were received or collection is off).
    pub fn take_trailers(&mut self) -> Result<Option<HeaderMap>, ChunkedError> {
        if self.trailers.is_empty() {
            return Ok(None);
        }
        let mut buf = std::mem::take(&mut self.trailers);
        buf.extend_from_slice(b"\r\n");
        let mut hs = [httparse::EMPTY_HEADER; 64];
        let n = match httparse::parse_headers(&buf, &mut hs) {
            Ok(httparse::Status::Complete((_, hs))) => hs,
            _ => return Err(ChunkedError::InvalidTrailer),
        };
        let mut map = HeaderMap::with_capacity(n.len());
        for h in n {
            let name = http::HeaderName::from_bytes(h.name.as_bytes())
                .map_err(|_| ChunkedError::InvalidTrailer)?;
            // Fields that must not be honoured in trailers (RFC 9110 6.5.2): framing, routing, pseudo.
            if matches!(
                name.as_str(),
                "content-length" | "transfer-encoding" | "host"
            ) || name.as_str().starts_with(':')
            {
                continue;
            }
            let value =
                http::HeaderValue::from_bytes(h.value).map_err(|_| ChunkedError::InvalidTrailer)?;
            map.append(name, value);
        }
        Ok(Some(map))
    }

    /// Advance over `input`. See [`Decoded`]. Zero allocations (trailer buffering aside).
    pub fn decode(&mut self, input: &[u8]) -> Result<Decoded, ChunkedError> {
        let mut pos = 0;
        let mut data = None;
        while pos < input.len() {
            match self.state {
                State::Size { digits, value } => {
                    let b = input[pos];
                    let d = match b {
                        b'0'..=b'9' => Some(b - b'0'),
                        b'a'..=b'f' => Some(b - b'a' + 10),
                        b'A'..=b'F' => Some(b - b'A' + 10),
                        _ => None,
                    };
                    if let Some(d) = d {
                        if digits >= 16 {
                            return Err(ChunkedError::ChunkSizeOverflow);
                        }
                        self.state = State::Size {
                            digits: digits + 1,
                            value: (value << 4) | u64::from(d),
                        };
                    } else if digits == 0 {
                        return Err(ChunkedError::InvalidChunkSize);
                    } else if !matches!(b, b';' | b' ' | b'\t' | b'\r' | b'\n') {
                        // Only an extension, BWS or the line end may follow the size digits (so `0x10`
                        // or `1g` are errors, not a size of 0/1 with a bogus extension).
                        return Err(ChunkedError::InvalidChunkSize);
                    } else {
                        // Terminator or extension: reprocess this byte in Ext.
                        self.state = State::Ext { value, ext_len: 0 };
                        continue;
                    }
                    pos += 1;
                }
                State::Ext { value, ext_len } => {
                    let b = input[pos];
                    pos += 1;
                    match b {
                        b'\r' => self.state = State::SizeLf { value },
                        b'\n' => self.after_size_line(value),
                        _ => {
                            if ext_len >= self.max_ext {
                                return Err(ChunkedError::ExtensionTooLong);
                            }
                            self.state = State::Ext {
                                value,
                                ext_len: ext_len + 1,
                            };
                        }
                    }
                }
                State::SizeLf { value } => {
                    if input[pos] != b'\n' {
                        return Err(ChunkedError::InvalidLineEnding);
                    }
                    pos += 1;
                    self.after_size_line(value);
                }
                State::Data { remaining } => {
                    if data.is_some() {
                        break;
                    }
                    let avail = (input.len() - pos) as u64;
                    let n = remaining.min(avail) as usize;
                    data = Some(pos..pos + n);
                    pos += n;
                    let remaining = remaining - n as u64;
                    self.state = if remaining == 0 {
                        State::DataEnd
                    } else {
                        State::Data { remaining }
                    };
                }
                State::DataEnd => {
                    match input[pos] {
                        b'\r' => self.state = State::DataLf,
                        b'\n' => {
                            self.state = State::Size {
                                digits: 0,
                                value: 0,
                            }
                        }
                        _ => return Err(ChunkedError::InvalidLineEnding),
                    }
                    pos += 1;
                }
                State::DataLf => {
                    if input[pos] != b'\n' {
                        return Err(ChunkedError::InvalidLineEnding);
                    }
                    pos += 1;
                    self.state = State::Size {
                        digits: 0,
                        value: 0,
                    };
                }
                State::TrailerStart => {
                    let b = input[pos];
                    pos += 1;
                    match b {
                        b'\r' => self.state = State::EndLf,
                        b'\n' => self.state = State::Done,
                        _ => {
                            self.push_trailer(b)?;
                            self.state = State::Trailer { saw_cr: false };
                        }
                    }
                }
                State::Trailer { saw_cr } => {
                    let b = input[pos];
                    pos += 1;
                    self.push_trailer(b)?;
                    match (b, saw_cr) {
                        (b'\n', _) => self.state = State::TrailerStart,
                        (b'\r', false) => self.state = State::Trailer { saw_cr: true },
                        (_, true) => return Err(ChunkedError::InvalidLineEnding),
                        _ => {}
                    }
                }
                State::EndLf => {
                    if input[pos] != b'\n' {
                        return Err(ChunkedError::InvalidLineEnding);
                    }
                    pos += 1;
                    self.state = State::Done;
                }
                State::Done => break,
            }
            if matches!(self.state, State::Done) {
                break;
            }
        }
        Ok(Decoded {
            consumed: pos,
            data,
            done: self.is_done(),
        })
    }

    fn after_size_line(&mut self, size: u64) {
        self.state = if size == 0 {
            State::TrailerStart
        } else {
            State::Data { remaining: size }
        };
    }

    fn push_trailer(&mut self, b: u8) -> Result<(), ChunkedError> {
        self.trailer_bytes += 1;
        if self.trailer_bytes > self.max_trailer {
            return Err(ChunkedError::TrailersTooLarge);
        }
        if self.collect {
            self.trailers.push(b);
        }
        Ok(())
    }
}

/// Fixed-length body decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LengthDecoder {
    /// Bytes still expected.
    pub remaining: u64,
}

impl LengthDecoder {
    /// Expect `len` bytes.
    pub fn new(len: u64) -> Self {
        Self { remaining: len }
    }

    /// Consume up to `remaining` bytes of `input`. Never copies or allocates.
    pub fn decode(&mut self, input: &[u8]) -> Decoded {
        let n = self.remaining.min(input.len() as u64) as usize;
        self.remaining -= n as u64;
        Decoded {
            consumed: n,
            data: (n > 0).then_some(0..n),
            done: self.remaining == 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feed `input` in pieces of `step` bytes; returns body and whether finished.
    pub(crate) fn run(
        d: &mut ChunkedDecoder,
        input: &[u8],
        step: usize,
    ) -> Result<(Vec<u8>, usize), ChunkedError> {
        let mut out = Vec::new();
        let mut fed = 0; // bytes made available
        let mut pos = 0; // bytes consumed
        loop {
            if d.is_done() {
                return Ok((out, pos));
            }
            fed = (fed + step).min(input.len()).max(fed);
            let avail = &input[pos..fed];
            let r = d.decode(avail)?;
            if let Some(rg) = r.data.clone() {
                out.extend_from_slice(&avail[rg]);
            }
            pos += r.consumed;
            if r.consumed == 0 && r.data.is_none() && !r.done {
                if fed == input.len() {
                    return Ok((out, pos)); // starved
                }
                fed = (fed + step).min(input.len());
            }
        }
    }

    const WIRE: &[u8] = b"5\r\nhello\r\n6;ext=1\r\n world\r\n0\r\nX-T: 1\r\n\r\nNEXT";

    #[test]
    fn whole_and_bytewise() {
        for step in [usize::MAX / 2, 1, 2, 3, 7] {
            let mut d = ChunkedDecoder::new().collect_trailers();
            let (body, used) = run(&mut d, WIRE, step).unwrap();
            assert_eq!(body, b"hello world", "step {step}");
            assert!(d.is_done());
            assert_eq!(&WIRE[used..], b"NEXT");
            let t = d.take_trailers().unwrap().unwrap();
            assert_eq!(t["x-t"], "1");
        }
    }

    #[test]
    fn data_range_points_into_input() {
        let mut d = ChunkedDecoder::new();
        let r = d.decode(WIRE).unwrap();
        assert_eq!(r.data, Some(3..8));
        assert_eq!(&WIRE[3..8], b"hello");
    }

    #[test]
    fn lenient_lf_strict_cr() {
        let mut d = ChunkedDecoder::new();
        let (b, _) = run(&mut d, b"3\nabc\n0\n\n", 1).unwrap();
        assert_eq!(b, b"abc");
        for bad in [
            &b"3\rabc"[..],
            b"3\r\nabc\rX",
            b"3\r\nabcX",
            b"3\r\nabc\r\n0\r\n\rX",
            b"0\r\nA: b\rc\r\n\r\n",
        ] {
            let mut d = ChunkedDecoder::new();
            assert_eq!(
                run(&mut d, bad, 1).unwrap_err(),
                ChunkedError::InvalidLineEnding,
                "{bad:?}"
            );
        }
    }

    #[test]
    fn sizes() {
        let mut d = ChunkedDecoder::new();
        assert_eq!(
            d.decode(b"\r\n").unwrap_err(),
            ChunkedError::InvalidChunkSize
        );
        let mut d = ChunkedDecoder::new();
        assert_eq!(
            d.decode(b"xyz").unwrap_err(),
            ChunkedError::InvalidChunkSize
        );
        let mut d = ChunkedDecoder::new();
        assert_eq!(
            d.decode(b"10000000000000000\r\n").unwrap_err(),
            ChunkedError::ChunkSizeOverflow
        );
        // 16 digits is fine and huge: data is just streamed
        let mut d = ChunkedDecoder::new();
        let r = d.decode(b"ffffffffffffffff\r\nabc").unwrap();
        assert_eq!(r.data, Some(18..21));
        let r = d.decode(b"def").unwrap();
        assert_eq!(r.data, Some(0..3));
        // uppercase hex and whitespace before ext
        let mut d = ChunkedDecoder::new();
        let (b, _) = run(&mut d, b"A \r\n0123456789\r\n0\r\n\r\n", 1).unwrap();
        assert_eq!(b, b"0123456789");
    }

    #[test]
    fn bounds() {
        let mut d = ChunkedDecoder::new().with_limits(8, 16);
        assert_eq!(
            d.decode(b"1;aaaaaaaaaaaaaaaa\r\n").unwrap_err(),
            ChunkedError::ExtensionTooLong
        );
        let mut d = ChunkedDecoder::new().with_limits(8, 16);
        assert_eq!(
            d.decode(b"0\r\nA: bbbbbbbbbbbbbbbbbbbbbbbbbbb\r\n\r\n")
                .unwrap_err(),
            ChunkedError::TrailersTooLarge
        );
    }

    #[test]
    fn done_is_sticky() {
        let mut d = ChunkedDecoder::new();
        let r = d.decode(b"0\r\n\r\nXX").unwrap();
        assert!(r.done);
        assert_eq!(r.consumed, 5);
        let r = d.decode(b"XX").unwrap();
        assert_eq!((r.consumed, r.data, r.done), (0, None, true));
    }

    #[test]
    fn length_decoder() {
        let mut d = LengthDecoder::new(5);
        let r = d.decode(b"abc");
        assert_eq!((r.consumed, r.data.clone(), r.done), (3, Some(0..3), false));
        let r = d.decode(b"defgh");
        assert_eq!((r.consumed, r.data, r.done), (2, Some(0..2), true));
        assert_eq!(LengthDecoder::new(0).decode(b"x").data, None);
    }

    #[test]
    fn junk_right_after_the_size_digits_is_rejected() {
        for bad in [&b"0x10\r\n"[..], b"1g\r\n", b"5!\r\nhello\r\n"] {
            let mut d = ChunkedDecoder::new();
            assert_eq!(d.decode(bad).unwrap_err(), ChunkedError::InvalidChunkSize);
        }
        for ok in [
            &b"5;a=b\r\nhello\r\n0\r\n\r\n"[..],
            b"5 \r\nhello\r\n0\r\n\r\n",
        ] {
            assert!(ChunkedDecoder::new().decode(ok).is_ok());
        }
    }

    #[test]
    fn forbidden_trailer_fields_are_dropped() {
        let mut d = ChunkedDecoder::new().collect_trailers();
        let wire = b"0\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\nHost: evil\r\nX-Ok: 1\r\n\r\n";
        d.decode(wire).unwrap();
        let t = d.take_trailers().unwrap().unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!(t["x-ok"], "1");
    }
}

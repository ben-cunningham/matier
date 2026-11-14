use core::fmt;

macro_rules! error_enum {
    ($(#[$m:meta])* $name:ident { $($(#[$vm:meta])* $v:ident => $msg:literal,)+ }) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum $name { $($(#[$vm])* $v,)+ }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(match self { $(Self::$v => $msg,)+ })
            }
        }
        impl std::error::Error for $name {}
    };
}

error_enum! {
    /// Failure while encoding a request head. On error `dst` is left unchanged.
    EncodeError {
        /// Absolute-form or CONNECT target without an authority, or no way to derive `Host`.
        MissingAuthority => "request target has no authority to build Host / target from",
        /// Absolute-form requested but the URI has no scheme.
        MissingScheme => "absolute-form request target requires a URI scheme",
        /// A header value contains CR, LF or NUL (or another forbidden control byte).
        InvalidHeaderValue => "header value contains forbidden control characters",
        /// Supplied framing headers contradict the declared body length.
        ConflictingFraming => "Content-Length / Transfer-Encoding headers conflict with the body length",
    }
}

error_enum! {
    /// Failure while parsing a response head.
    ParseError {
        /// The head exceeds `Limits::max_head_bytes`.
        TooLarge => "response head too large",
        /// More headers than `Limits::max_headers`.
        TooManyHeaders => "too many response headers",
        /// Not `HTTP/1.0` or `HTTP/1.1`.
        InvalidVersion => "invalid HTTP version",
        /// Malformed or out-of-range status code.
        InvalidStatus => "invalid status line",
        /// Invalid header name.
        InvalidHeaderName => "invalid header name",
        /// Invalid byte in a header value.
        InvalidHeaderValue => "invalid header value",
        /// Obsolete line folding was received and `allow_obs_fold` is off.
        ObsFold => "obsolete line folding (obs-fold) rejected",
        /// Bad line ending (bare CR, etc.).
        InvalidNewLine => "invalid line ending",
        /// Other malformed syntax.
        Syntax => "malformed response head",
    }
}

error_enum! {
    /// Failure while determining message framing (RFC 9112 section 6.3).
    FramingError {
        /// Content-Length is not a valid non-negative integer.
        InvalidContentLength => "invalid Content-Length",
        /// Multiple Content-Length values that differ.
        ConflictingContentLength => "conflicting Content-Length values",
    }
}

error_enum! {
    /// Failure while decoding a chunked body.
    ChunkedError {
        /// Non-hex byte in chunk size, or no digits.
        InvalidChunkSize => "invalid chunk size",
        /// More than 16 hex digits.
        ChunkSizeOverflow => "chunk size overflows 64 bits",
        /// Chunk extension longer than the configured bound.
        ExtensionTooLong => "chunk extension too long",
        /// Missing or malformed CRLF (including a bare CR).
        InvalidLineEnding => "invalid chunk line ending",
        /// Trailer section larger than the configured bound.
        TrailersTooLarge => "trailer section too large",
        /// Trailer section failed to parse.
        InvalidTrailer => "invalid trailer",
    }
}

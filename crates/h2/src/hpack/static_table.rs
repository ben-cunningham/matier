//! The HPACK static table (RFC 7541 Appendix A), 1-based indices 1..=61.

/// Number of static entries.
pub const LEN: usize = 61;

/// `(name, value)` for static index `i + 1`.
pub static ENTRIES: [(&str, &str); LEN] = [
    (":authority", ""),
    (":method", "GET"),
    (":method", "POST"),
    (":path", "/"),
    (":path", "/index.html"),
    (":scheme", "http"),
    (":scheme", "https"),
    (":status", "200"),
    (":status", "204"),
    (":status", "206"),
    (":status", "304"),
    (":status", "400"),
    (":status", "404"),
    (":status", "500"),
    ("accept-charset", ""),
    ("accept-encoding", "gzip, deflate"),
    ("accept-language", ""),
    ("accept-ranges", ""),
    ("accept", ""),
    ("access-control-allow-origin", ""),
    ("age", ""),
    ("allow", ""),
    ("authorization", ""),
    ("cache-control", ""),
    ("content-disposition", ""),
    ("content-encoding", ""),
    ("content-language", ""),
    ("content-length", ""),
    ("content-location", ""),
    ("content-range", ""),
    ("content-type", ""),
    ("cookie", ""),
    ("date", ""),
    ("etag", ""),
    ("expect", ""),
    ("expires", ""),
    ("from", ""),
    ("host", ""),
    ("if-match", ""),
    ("if-modified-since", ""),
    ("if-none-match", ""),
    ("if-range", ""),
    ("if-unmodified-since", ""),
    ("last-modified", ""),
    ("link", ""),
    ("location", ""),
    ("max-forwards", ""),
    ("proxy-authenticate", ""),
    ("proxy-authorization", ""),
    ("range", ""),
    ("referer", ""),
    ("refresh", ""),
    ("retry-after", ""),
    ("server", ""),
    ("set-cookie", ""),
    ("strict-transport-security", ""),
    ("transfer-encoding", ""),
    ("user-agent", ""),
    ("vary", ""),
    ("via", ""),
    ("www-authenticate", ""),
];

/// First static index (1-based) whose name equals `name`, or 0. A `match` on the byte string,
/// compiled into length + memcmp decisions; no hashing, no allocation.
pub fn name_index(name: &[u8]) -> usize {
    match name {
        b":authority" => 1,
        b":method" => 2,
        b":path" => 4,
        b":scheme" => 6,
        b":status" => 8,
        b"accept-charset" => 15,
        b"accept-encoding" => 16,
        b"accept-language" => 17,
        b"accept-ranges" => 18,
        b"accept" => 19,
        b"access-control-allow-origin" => 20,
        b"age" => 21,
        b"allow" => 22,
        b"authorization" => 23,
        b"cache-control" => 24,
        b"content-disposition" => 25,
        b"content-encoding" => 26,
        b"content-language" => 27,
        b"content-length" => 28,
        b"content-location" => 29,
        b"content-range" => 30,
        b"content-type" => 31,
        b"cookie" => 32,
        b"date" => 33,
        b"etag" => 34,
        b"expect" => 35,
        b"expires" => 36,
        b"from" => 37,
        b"host" => 38,
        b"if-match" => 39,
        b"if-modified-since" => 40,
        b"if-none-match" => 41,
        b"if-range" => 42,
        b"if-unmodified-since" => 43,
        b"last-modified" => 44,
        b"link" => 45,
        b"location" => 46,
        b"max-forwards" => 47,
        b"proxy-authenticate" => 48,
        b"proxy-authorization" => 49,
        b"range" => 50,
        b"referer" => 51,
        b"refresh" => 52,
        b"retry-after" => 53,
        b"server" => 54,
        b"set-cookie" => 55,
        b"strict-transport-security" => 56,
        b"transfer-encoding" => 57,
        b"user-agent" => 58,
        b"vary" => 59,
        b"via" => 60,
        b"www-authenticate" => 61,
        _ => 0,
    }
}

/// Look up `(name, value)`: returns `(full_match_index, name_index)`, each 0 when absent.
pub fn lookup(name: &[u8], value: &[u8]) -> (usize, usize) {
    let ni = name_index(name);
    if ni == 0 {
        return (0, 0);
    }
    let mut i = ni;
    while i <= LEN && ENTRIES[i - 1].0.as_bytes() == name {
        if ENTRIES[i - 1].1.as_bytes() == value {
            return (i, ni);
        }
        i += 1;
    }
    (0, ni)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_index_matches_table() {
        for (i, (n, _)) in ENTRIES.iter().enumerate() {
            let first = ENTRIES.iter().position(|(m, _)| m == n).unwrap() + 1;
            assert_eq!(name_index(n.as_bytes()), first, "entry {}", i + 1);
        }
        assert_eq!(name_index(b"x-custom"), 0);
        assert_eq!(lookup(b":status", b"404"), (13, 8));
        assert_eq!(lookup(b":status", b"418"), (0, 8));
        assert_eq!(lookup(b"accept-encoding", b"gzip, deflate"), (16, 16));
        assert_eq!(lookup(b"x", b""), (0, 0));
    }
}

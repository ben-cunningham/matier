//! Huffman coding for HPACK strings (RFC 7541 §5.2, Appendix B).
//!
//! Decoding is table driven: a 256-entry first-level table resolves every code of up to 8 bits
//! (the vast majority of real-world text) in one lookup; longer codes (9..=30 bits) fall back to
//! a canonical-code search by length. All tables are built by `const fn` at compile time.
//! Neither direction allocates beyond growth of the output `Vec`.

use super::huffman_table::{CODES, LENS};

/// Huffman decoding failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HuffmanError {
    /// EOS symbol in the data, an undecodable code, or invalid padding (longer than 7 bits or
    /// not the all-ones prefix of EOS).
    Invalid,
    /// Decoded output would exceed the caller's limit.
    TooLong,
}

struct Tables {
    /// `sym | len << 8` for codes of at most 8 bits, indexed by the next 8 bits; 0 if longer.
    fast: [u16; 256],
    /// Symbols sorted by (code length, symbol).
    sorted: [u16; 257],
    /// First canonical code of each length.
    first: [u32; 32],
    /// Number of codes of each length.
    count: [u16; 32],
    /// Index into `sorted` of the first code of each length.
    offset: [u16; 32],
}

const fn build() -> Tables {
    let mut t = Tables {
        fast: [0; 256],
        sorted: [0; 257],
        first: [0; 32],
        count: [0; 32],
        offset: [0; 32],
    };
    let mut s = 0;
    while s < 257 {
        t.count[LENS[s] as usize] += 1;
        s += 1;
    }
    let mut pos = 0u16;
    let mut l = 1;
    while l <= 30 {
        t.offset[l] = pos;
        let mut s = 0;
        while s < 257 {
            if LENS[s] as usize == l {
                if pos == t.offset[l] {
                    t.first[l] = CODES[s];
                }
                t.sorted[pos as usize] = s as u16;
                pos += 1;
            }
            s += 1;
        }
        l += 1;
    }
    let mut s = 0;
    while s < 256 {
        let len = LENS[s] as u32;
        if len <= 8 {
            let base = (CODES[s] << (8 - len)) as usize;
            let mut k = 0;
            while k < (1usize << (8 - len)) {
                t.fast[base + k] = s as u16 | ((len as u16) << 8);
                k += 1;
            }
        }
        s += 1;
    }
    t
}

static T: Tables = build();

/// Number of bytes `src` occupies once Huffman coded.
#[inline]
pub fn encoded_len(src: &[u8]) -> usize {
    let mut bits = 0usize;
    for &b in src {
        bits += LENS[b as usize] as usize;
    }
    bits.div_ceil(8)
}

/// Append the Huffman coding of `src` to `out`, padding the last byte with EOS prefix bits.
pub fn encode(src: &[u8], out: &mut Vec<u8>) {
    out.reserve(encoded_len(src));
    let mut acc: u64 = 0;
    let mut n: u32 = 0;
    for &b in src {
        let len = LENS[b as usize] as u32;
        acc = (acc << len) | u64::from(CODES[b as usize]);
        n += len;
        while n >= 8 {
            n -= 8;
            out.push((acc >> n) as u8);
        }
    }
    if n > 0 {
        out.push(((acc << (8 - n)) as u8) | (0xff >> n));
    }
}

/// Decode `src`, appending to `out`; fails with [`HuffmanError::TooLong`] once more than
/// `max_out` bytes would be produced. On error `out` may contain partial output.
pub fn decode(src: &[u8], out: &mut Vec<u8>, max_out: usize) -> Result<(), HuffmanError> {
    let start = out.len();
    let mut acc: u64 = 0;
    let mut nbits: u32 = 0;
    let mut i = 0;
    loop {
        while nbits <= 56 && i < src.len() {
            acc = (acc << 8) | u64::from(src[i]);
            i += 1;
            nbits += 8;
        }
        let exhausted = i == src.len();
        loop {
            if nbits == 0 {
                break;
            }
            let peek = if nbits >= 8 {
                (acc >> (nbits - 8)) & 0xff
            } else {
                (acc << (8 - nbits)) & 0xff
            };
            let e = T.fast[peek as usize];
            let (sym, len) = if (e >> 8) != 0 && u32::from(e >> 8) <= nbits {
                (u32::from(e & 0xff), u32::from(e >> 8))
            } else {
                let mut found = None;
                let mut l = 9;
                while l <= 30 && l <= nbits {
                    let code = ((acc >> (nbits - l)) & ((1u64 << l) - 1)) as u32;
                    let first = T.first[l as usize];
                    if code >= first && code - first < u32::from(T.count[l as usize]) {
                        let s = T.sorted[(T.offset[l as usize] as u32 + code - first) as usize];
                        found = Some((u32::from(s), l));
                        break;
                    }
                    l += 1;
                }
                match found {
                    Some(f) => f,
                    None => break,
                }
            };
            if sym == 256 {
                return Err(HuffmanError::Invalid);
            }
            if out.len() - start >= max_out {
                return Err(HuffmanError::TooLong);
            }
            out.push(sym as u8);
            nbits -= len;
        }
        if exhausted {
            break;
        }
        if nbits >= 30 {
            // A full-length window that matches no code cannot happen except for EOS.
            return Err(HuffmanError::Invalid);
        }
    }
    // Remaining bits are padding: at most 7, all ones.
    if nbits > 7 {
        return Err(HuffmanError::Invalid);
    }
    let mask = (1u64 << nbits) - 1;
    if acc & mask != mask {
        return Err(HuffmanError::Invalid);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_property() {
        // Codes of one length are consecutive in symbol order, and lengths ascend canonically.
        for l in 1..=30usize {
            let c = T.count[l] as usize;
            for k in 0..c {
                let s = T.sorted[T.offset[l] as usize + k] as usize;
                assert_eq!(LENS[s] as usize, l);
                assert_eq!(CODES[s], T.first[l] + k as u32, "len {l} k {k}");
            }
        }
    }

    #[test]
    fn rfc_examples() {
        let mut o = Vec::new();
        encode(b"www.example.com", &mut o);
        assert_eq!(
            o,
            [
                0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff
            ]
        );
        let mut d = Vec::new();
        decode(&o, &mut d, 100).unwrap();
        assert_eq!(d, b"www.example.com");
    }

    #[test]
    fn all_bytes_roundtrip() {
        let src: Vec<u8> = (0..=255u8).chain((0..=255u8).rev()).collect();
        let mut e = Vec::new();
        encode(&src, &mut e);
        assert_eq!(e.len(), encoded_len(&src));
        let mut d = Vec::new();
        decode(&e, &mut d, 1000).unwrap();
        assert_eq!(d, src);
    }

    #[test]
    fn rejects_eos_and_bad_padding() {
        let mut d = Vec::new();
        // EOS (30 ones) + padding
        assert_eq!(
            decode(&[0xff, 0xff, 0xff, 0xfc], &mut d, 10),
            Err(HuffmanError::Invalid)
        );
        // 'a' = 00011 followed by zero padding
        d.clear();
        assert_eq!(
            decode(&[0b0001_1000], &mut d, 10),
            Err(HuffmanError::Invalid)
        );
        // 'a' + 3 one bits: valid
        d.clear();
        decode(&[0b0001_1111], &mut d, 10).unwrap();
        assert_eq!(d, b"a");
        // padding longer than 7 bits: 'a' + 11 ones
        d.clear();
        assert_eq!(
            decode(&[0b0001_1111, 0xff], &mut d, 10),
            Err(HuffmanError::Invalid)
        );
        // exceeding limit
        d.clear();
        let mut e = Vec::new();
        encode(b"aaaa", &mut e);
        assert_eq!(decode(&e, &mut d, 3), Err(HuffmanError::TooLong));
    }
}

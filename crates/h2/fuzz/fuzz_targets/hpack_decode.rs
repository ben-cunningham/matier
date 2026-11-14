#![no_main]
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use matier_h2::hpack::Decoder;

fuzz_target!(|data: &[u8]| {
    let mut dec = Decoder::new(4096);
    let block = Bytes::copy_from_slice(data);
    let _ = dec.decode(&block, |_n, _v, _s| {});
    // Table state must stay usable after errors.
    let _ = dec.decode(&block, |_n, _v, _s| {});
});

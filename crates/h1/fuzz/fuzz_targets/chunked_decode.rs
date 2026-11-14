#![no_main]
use libfuzzer_sys::fuzz_target;
use matier_h1::ChunkedDecoder;

fuzz_target!(|data: &[u8]| {
    // First byte picks the feed granularity so split points are explored too.
    let Some((&step, data)) = data.split_first() else { return };
    let step = usize::from(step % 16) + 1;
    let mut d = ChunkedDecoder::new().collect_trailers();
    let (mut pos, mut fed) = (0, 0);
    while !d.is_done() && pos < data.len() {
        fed = (fed + step).min(data.len());
        let avail = &data[pos..fed];
        match d.decode(avail) {
            Ok(r) => {
                if let Some(rg) = r.data {
                    assert!(rg.end <= avail.len());
                }
                assert!(r.consumed <= avail.len());
                pos += r.consumed;
            }
            Err(_) => return,
        }
    }
    let _ = d.take_trailers();
});

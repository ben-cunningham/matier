#![no_main]
use libfuzzer_sys::fuzz_target;
use matier_h2::frame::{Frame, decode_frame};

fuzz_target!(|data: &[u8]| {
    let mut buf = data;
    while let Ok(Some((frame, n))) = decode_frame(buf, 16_384) {
        if let Frame::Settings { params, .. } = frame {
            for _ in params {}
        }
        buf = &buf[n..];
    }
});

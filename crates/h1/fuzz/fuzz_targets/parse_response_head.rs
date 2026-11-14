#![no_main]
use libfuzzer_sys::fuzz_target;
use matier_h1::{Limits, ParseStatus, parse_response_head, response_body_kind, keep_alive};

fuzz_target!(|data: &[u8]| {
    for allow_obs_fold in [false, true] {
        let limits = Limits { allow_obs_fold, ..Limits::default() };
        if let Ok(ParseStatus::Complete { value, head_len }) = parse_response_head(data, &limits) {
            assert!(head_len <= data.len());
            let _ = response_body_kind(&http::Method::GET, &value);
            let _ = keep_alive(&value);
        }
    }
});

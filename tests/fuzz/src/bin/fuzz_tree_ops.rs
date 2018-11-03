#![no_main]
extern crate tests;

#[macro_use]
extern crate libfuzzer_sys;

fuzz_target!(|data: &[u8]| {
    tests::tree::fuzz_then_shrink(data);
});

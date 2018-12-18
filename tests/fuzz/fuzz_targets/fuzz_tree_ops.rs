#![no_main]
fuzz_target!(|data: &[u8]| {
    tests::tree::fuzz_then_shrink(data);
});

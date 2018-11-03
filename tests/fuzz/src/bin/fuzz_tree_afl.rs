extern crate tests;
#[macro_use]
extern crate afl;

fn main() {
    fuzz!(|data: &[u8]| {
        tests::tree::fuzz_then_shrink(data);
    }
}

#!/bin/bash
set -eo pipefail

echo "asan"
cargo clean
export RUSTFLAGS="-Z sanitizer=address"
# export ASAN_OPTIONS="detect_odr_violation=0"
cargo +nightly run --target x86_64-unknown-linux-gnu
unset ASAN_OPTIONS

echo "lsan"
cargo clean
export RUSTFLAGS="-Z sanitizer=leak"
cargo +nightly run --target x86_64-unknown-linux-gnu

echo "tsan"
cargo clean
export RUSTFLAGS="-Z sanitizer=thread"
export TSAN_OPTIONS=suppressions=../../tsan_suppressions.txt
cargo +nightly run --target x86_64-unknown-linux-gnu
unset RUSTFLAGS
unset TSAN_OPTIONS

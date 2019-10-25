#!/bin/bash
set -eo pipefail

pushd benchmarks/stress2
echo "lsan"
cargo clean
export RUSTFLAGS="-Z sanitizer=leak"
cargo +nightly build --features=lock_free_delays,no_jemalloc --target x86_64-unknown-linux-gnu
sudo rm -rf default.sled
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=30
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=6

echo "asan"
cargo clean
export RUSTFLAGS="-Z sanitizer=address"
export ASAN_OPTIONS="detect_odr_violation=0"
cargo +nightly build --features=lock_free_delays,no_jemalloc --target x86_64-unknown-linux-gnu
sudo rm -rf default.sled
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=30
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=6
unset ASAN_OPTIONS

echo "tsan"
cargo clean
export RUSTFLAGS="-Z sanitizer=thread"
export TSAN_OPTIONS=suppressions=../../tsan_suppressions.txt
sudo rm -rf default.sled
cargo +nightly run --features=lock_free_delays,no_jemalloc --target x86_64-unknown-linux-gnu -- --duration=30
cargo +nightly run --features=lock_free_delays,no_jemalloc --target x86_64-unknown-linux-gnu -- --duration=6
unset RUSTFLAGS
unset TSAN_OPTIONS

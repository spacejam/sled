#!/bin/bash
set -eo pipefail

pushd benchmarks/stress2

rustup toolchain install nightly --no-self-update
rustup update --no-self-update

export SLED_LOCK_FREE_DELAY_INTENSITY=2000

echo "asan"
cargo clean
export RUSTFLAGS="-Z sanitizer=address"
export ASAN_OPTIONS="detect_odr_violation=0"
cargo +nightly build --features=lock_free_delays --target x86_64-unknown-linux-gnu
sudo rm -rf default.sled
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=10
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=6
unset ASAN_OPTIONS

echo "lsan"
cargo clean
export RUSTFLAGS="-Z sanitizer=leak"
cargo +nightly build --features=lock_free_delays --target x86_64-unknown-linux-gnu
sudo rm -rf default.sled
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=10
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=6

echo "tsan"
cargo clean
export RUSTFLAGS="-Z sanitizer=thread"
export TSAN_OPTIONS=suppressions=../../tsan_suppressions.txt
sudo rm -rf default.sled
cargo +nightly run --features=lock_free_delays --target x86_64-unknown-linux-gnu -- --duration=10
cargo +nightly run --features=lock_free_delays --target x86_64-unknown-linux-gnu -- --duration=6
unset RUSTFLAGS
unset TSAN_OPTIONS

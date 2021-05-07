#!/bin/bash
set -eo pipefail

pushd benchmarks/stress2

rustup toolchain install nightly
rustup toolchain install nightly --component rust-src
rustup update

export SLED_LOCK_FREE_DELAY_INTENSITY=2000

echo "msan"
cargo clean
export RUSTFLAGS="-Zsanitizer=memory -Zsanitizer-memory-track-origins"
cargo +nightly build -Zbuild-std --target x86_64-unknown-linux-gnu
sudo rm -rf default.sled
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=30 --set-prop=100000000 --val-len=1000 --entries=100 --threads=100
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=30 --entries=100
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=30
unset MSAN_OPTIONS

echo "asan"
cargo clean
export RUSTFLAGS="-Z sanitizer=address"
export ASAN_OPTIONS="detect_odr_violation=0"
cargo +nightly build --features=lock_free_delays --target x86_64-unknown-linux-gnu
sudo rm -rf default.sled
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=60
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=6
unset ASAN_OPTIONS

echo "lsan"
cargo clean
export RUSTFLAGS="-Z sanitizer=leak"
cargo +nightly build --features=lock_free_delays --target x86_64-unknown-linux-gnu
sudo rm -rf default.sled
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=60
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=6

echo "tsan"
cargo clean
export RUSTFLAGS="-Z sanitizer=thread"
export TSAN_OPTIONS=suppressions=../../tsan_suppressions.txt
sudo rm -rf default.sled
cargo +nightly run --features=lock_free_delays --target x86_64-unknown-linux-gnu -- --duration=60
cargo +nightly run --features=lock_free_delays --target x86_64-unknown-linux-gnu -- --duration=6
unset RUSTFLAGS
unset TSAN_OPTIONS

#!/usr/bin/env bash

set -euxo pipefail

echo "asan"
# cargo clean
export RUSTFLAGS="-Z sanitizer=address"
# export ASAN_OPTIONS="detect_odr_violation=0"
export ASAN_OPTIONS=detect_odr_violation=0
cargo build --features=lock_free_delays,no_jemalloc --target x86_64-unknown-linux-gnu
sudo rm -rf default.sled
sudo ASAN_OPTIONS=detect_odr_violation=0 target/x86_64-unknown-linux-gnu/debug/stress2 --duration=30
sudo ASAN_OPTIONS=detect_odr_violation=0 target/x86_64-unknown-linux-gnu/debug/stress2 --duration=6
unset ASAN_OPTIONS

echo "lsan"
cargo clean
export RUSTFLAGS="-Z sanitizer=leak"
cargo build --features=lock_free_delays,no_jemalloc --target x86_64-unknown-linux-gnu
sudo rm -rf default.sled
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=6
sudo target/x86_64-unknown-linux-gnu/debug/stress2 --duration=6

echo "tsan"
cargo clean
export RUSTFLAGS="-Z sanitizer=thread"
export TSAN_OPTIONS="~/src/sled/tsan_suppressions.txt"
sudo rm -rf default.sled
cargo run --features=lock_free_delays,no_jemalloc --target x86_64-unknown-linux-gnu -- --duration=30
cargo run --features=lock_free_delays,no_jemalloc --target x86_64-unknown-linux-gnu -- --duration=6

#!/usr/bin/env bash

set -euxo pipefail

echo "tsan"
cargo clean
export RUSTFLAGS="-Z sanitizer=thread"
export TSAN_OPTIONS="suppressions=/home/t/src/sled/tsan_suppressions.txt"
sudo rm -rf default.sled
cargo run --features=lock_free_delays,no_jemalloc --target x86_64-unknown-linux-gnu -- --duration=30
cargo run --features=lock_free_delays,no_jemalloc --target x86_64-unknown-linux-gnu -- --duration=6

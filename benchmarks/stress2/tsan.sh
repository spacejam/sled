#!/usr/bin/env bash

set -euxo pipefail

echo "tsan"
export RUSTFLAGS="-Z sanitizer=thread"
export TSAN_OPTIONS="suppressions=/home/t/src/sled/tsan_suppressions.txt"
sudo rm -rf default.sled
cargo +nightly run --features=lock_free_delays,no_jemalloc --target x86_64-unknown-linux-gnu -- --duration=6
cargo +nightly run --features=lock_free_delays,no_jemalloc --target x86_64-unknown-linux-gnu -- --duration=6

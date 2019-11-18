#!/usr/bin/env bash

set -euxo pipefail

echo "lsan"
export RUSTFLAGS="-Z sanitizer=leak"
cargo build --features=no_jemalloc --target x86_64-unknown-linux-gnu
rm -rf default.sled
target/x86_64-unknown-linux-gnu/debug/stress2 --duration=10 --set-prop=100000000 --val-len=100000

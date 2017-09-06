#!/bin/bash
set -ex
echo "detecting running sled instance"
cargo test -- --nocapture &
PID=$!
perf record -F 99 -p "$PID" -g -- wait $PID || true
perf script > out.perf
./hack/stackcollapse-perf.pl out.perf > out.folded
./hack/flamegraph.pl out.folded > flamegraph.svg
rm perf.data out.perf out.folded

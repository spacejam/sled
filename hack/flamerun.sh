#!/bin/bash
set -ex
perf record -F 99 -g "$@"
perf script > out.perf
./hack/stackcollapse-perf.pl out.perf > out.folded
./hack/flamegraph.pl out.folded > flamegraph.svg
rm perf.data out.perf out.folded


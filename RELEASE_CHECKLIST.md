# Release Checklist

This checklist must be completed before publishing a release of any kind.

Over time, anything in this list that can be turned into an automated test should be, but
there are still some big blind spots.

## API stability

- [ ] rust-flavored semver respected

## Performance

- [ ] micro-benchmark regressions should not happen unless newly discovered correctness criteria demands them
- [ ] mixed point operation latency distribution should narrow over time
- [ ] sequential operation average throughput should increase over time
- [ ] workloads should pass TSAN and ASAN on macOS. Linux should additionally pass LSAN & MSAN.
- [ ] workload write and space amplification thresholds should see no regressions

## Concurrency Audit

- [ ] any new `Guard` objects are dropped inside the rayon threadpool
- [ ] no new EBR `Collector`s, as they destroy causality. These will be optimized in-bulk in the future.
- [ ] no code assumes a recently read page pointer will remain unchanged (transactions may change this if reads are inline)
- [ ] no calls to `rand::thread_rng` from a droppable function (anything in the SegmentAccountant)

## Burn-In

- [ ] fuzz tests should run at least 24 hours each with zero crashes
- [ ] sequential and point workloads run at least 24 hours in constrained docker container without OOM / out of disk

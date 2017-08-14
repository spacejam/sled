# RSDB

a flash-sympathetic persistent lock-free B+ tree

[documentation](https://docs.rs/rsdb)

progress

- [x] lock-free log-structured store with reservable slots
- [x] lock-free page store supporting cache-friendly partial updates
- [x] lock-free b-link tree
- [x] recovery
- [x] zstd compression
- [x] LRU cache
- [x] pagetable snapshotting for faster recovery
- [ ] epoch-based gc (LEAKS MEMORY FOR NOW LOLOLOLOLOL)
- [ ] multi-key transactions and MVCC using a higher-level `DB` interface

# Goals

1. don't use so much electricity. our data structures should play to modern hardware's strengths.
1. don't surprise users with performance traps.
1. bring reliability techniques from academia into real-world practice.

# Architecture

Lock-free trees on a lock-free pagecache on a lock-free log. The pagecache scatters
partial page fragments across the log, rather than rewriting entire pages at a time
as B+ trees for spinning disks historically have. On page reads, we scatter-gather
reads across the log to materialize the page from its fragments.

If you want to build a new tree/other structure on the `PageCache`, implement the `Materializer` trait.

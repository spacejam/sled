# RSDB

a flash-sympathetic persistent lock-free B+ tree, pagecache, and log 

[documentation](https://docs.rs/rsdb)

progress

- [x] lock-free log with reservable slots
- [x] lock-free pagecache with cache-friendly partial updates
- [x] lock-free b-link tree
- [x] recovery
- [x] zstd compression (build with the zstd feature if you want)
- [x] LRU cache
- [x] pagetable snapshotting for faster recovery
- [x] epoch-based gc
- [ ] higher-level interface with multi-key transaction support
- [ ] merge operator support
- [ ] formal verification of lock-free algorithms via symbolic execution

# Goals

1. don't use so much electricity. our data structures should play to modern hardware's strengths.
1. don't surprise users with performance traps.
1. bring reliability techniques from academia into real-world practice.
1. minimize dependencies, strive for simplicity

# Architecture

Lock-free trees on a lock-free pagecache on a lock-free log. The pagecache scatters
partial page fragments across the log, rather than rewriting entire pages at a time
as B+ trees for spinning disks historically have. On page reads, we concurrently
scatter-gather reads across the log to materialize the page from its fragments.
We are friendly to cache by minimizing the 

The system is largely inspired by the Deuteronomy architecture, and aims to implement
the best features from RocksDB as well.

# References

* [The Bw-Tree: A B-tree for New Hardware Platforms](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf)
* [LLAMA: A Cache/Storage Subsystem for Modern Hardware](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/llama-vldb2013.pdf)
* [Deuteronomy: Transaction Support for Cloud Data](https://www.microsoft.com/en-us/research/publication/deuteronomy-transaction-support-for-cloud-data/)

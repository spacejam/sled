# RSDB

A modern lock-free atomic embedded database designed to beat LSM trees for
reads and traditional B+ trees for writes. 

It uses a modular design which can also be used to implement your own high
performance persistent systems, using the included `LockFreeLog` and `PageCache`.
Eventually, a versioned DB will be built on top of the `Tree` which provides
multi-key transactions and snapshots.

The `Tree` has a C API, so you can use this from any mainstream language.

[![Build Status](https://travis-ci.org/spacejam/rsdb.svg?branch=master)](https://travis-ci.org/spacejam/rsdb)
[![crates.io](http://meritbadge.herokuapp.com/rsdb)](https://crates.io/crates/rsdb)
[![documentation](https://docs.rs/rsdb/badge.svg)](https://docs.rs/rsdb)

```rust
extern crate rsdb;

let tree = rsdb::Config::default()
  .path(Some("rsdb.state".to_owned()))
  .tree();

// set and get
tree.set(k, v1);
assert_eq!(tree.get(&k), Some(v1));

// compare and swap
tree.cas(k, Some(v1), Some(v2));

// scans
let mut iter = tree.scan(b"a non-present key < k!");
assert_eq!(iter.next(), Some((k, v2)));
assert_eq!(iter.next(), None);

// deletion
tree.del(&k);
```

# Warnings

* Quite young, there are lots of fuzz tests but don't bet a billion
  dollar business on it yet!
* Log cleaning is not yet implemented, so if you write the same
  key over and over, you will run out of disk space eventually.
  This is going to be implemented in the next week!
* The C API is likely to change rapidly
* Has not yet received much attention for performance tuning,
  it has an extremely high theoretical performance but there
  is a bit of tuning to get there. This will be happening soon!

# Contribution Welcome!

* Want to help advance the state of the art in open source embedded
  databases? Check out [CONTRIBUTING.md](CONTRIBUTING.md)!

# Features

- [x] lock-free b-link tree
- [x] lock-free log with reservable slots
- [x] lock-free pagecache with cache-friendly partial updates
- [x] zstd compression
- [x] configurable cache size
- [x] C API
- [ ] log cleaning
- [ ] merge operator support
- [ ] higher-level interface with multi-key transaction and snapshot support
- [ ] formal verification of lock-free algorithms via symbolic execution

# Goals

1. beat LSM's on read performance and traditional B+ trees on write performance.
1. don't use so much electricity. our data structures should play to modern hardware's strengths.
1. don't surprise users with performance traps.
1. bring reliability techniques from academia into real-world practice.

# Architecture

Lock-free trees on a lock-free pagecache on a lock-free log. The pagecache scatters
partial page fragments across the log, rather than rewriting entire pages at a time
as B+ trees for spinning disks historically have. On page reads, we concurrently
scatter-gather reads across the log to materialize the page from its fragments.

The system is largely inspired by the Deuteronomy architecture, and aims to implement
the best features from RocksDB as well.

# References

* [The Bw-Tree: A B-tree for New Hardware Platforms](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf)
* [LLAMA: A Cache/Storage Subsystem for Modern Hardware](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/llama-vldb2013.pdf)
* [Deuteronomy: Transaction Support for Cloud Data](https://www.microsoft.com/en-us/research/publication/deuteronomy-transaction-support-for-cloud-data/)

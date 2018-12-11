[![Build Status](https://travis-ci.org/spacejam/sled.svg?branch=master)](https://travis-ci.org/spacejam/sled)
[![crates.io](https://meritbadge.herokuapp.com/sled)](https://crates.io/crates/sled)
[![documentation](https://docs.rs/sled/badge.svg)](https://docs.rs/sled)
[![chat](https://img.shields.io/discord/509773073294295082.svg?logo=discord)](https://discord.gg/Z6VsXds)

<p align="center">
  <img src="https://raw.githubusercontent.com/spacejam/sled/master/art/tree_face.png" width="20%" height="auto" />
</p>


# sled

An (alpha) modern embedded database.

```rust
use sled::Tree;

let tree = Tree::start_default(path)?;

// set and get
tree.set(k, v1);
assert_eq!(tree.get(&k), Ok(Some(v1)));

// compare and swap
tree.cas(k, Some(v1), Some(v2));

// scan forward
let mut iter = tree.scan(k);
assert_eq!(iter.next(), Some(Ok((k, v2))));
assert_eq!(iter.next(), None);

// deletion
tree.del(&k);

// block until all operations are on-disk
tree.flush();
```

We also support [merge operators](https://github.com/spacejam/sled/wiki/merge-operators)!

# features

* API similar to a threadsafe `BTreeMap<Vec<u8>, Vec<u8>>`
* fully atomic single-key operations, supports CAS
* zero-copy reads
* merge operators
* forward and reverse iterators
* a crash-safe monotonic ID generator capable of generating 75-125 million ID's per second
* [zstd](https://github.com/facebook/zstd) compression (use the `compression` build feature)
* cpu-scalable lock-free implementation
* SSD-optimized log-structured storage

# goals

1. don't make the user think. the interface should be obvious.
1. don't surprise users with performance traps.
1. don't wake up operators. bring reliability techniques from academia into real-world practice.
1. don't use so much electricity. our data structures should play to modern hardware's strengths.

# plans

* [LSM tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)-like write performance
  with [traditional B+ tree](https://en.wikipedia.org/wiki/B%2B_tree)-like read performance
* MVCC, serializable transactions, and snapshots
* forward-compatible binary format
* FIFO prefix subscription semantics
* concurrent snapshot delta generation and recovery
* first-class access to replication stream
* consensus protocol for [PC/EC](https://en.wikipedia.org/wiki/PACELC_theorem) systems
* pluggable conflict detection and resolution strategies for [PA/EL](https://en.wikipedia.org/wiki/PACELC_theorem) systems
* SQL support
* native bindings

# known issues, warnings

* the on-disk format is going to change in non-forward compatible ways
  before the `1.0.0` release! after that, we will always support
  forward migrations.
* quite young, should be considered unstable for the time being
* the C API is likely to change rapidly
* writepath is not well optimized yet. readpath is essentially wait-free and zero-copy.
* 32 bit architectures [require Rust nightly with the `nightly` feature enabled](https://github.com/spacejam/sled/issues/145).

# contribution welcome!

want to help advance the state of the art in open source embedded
databases? check out [CONTRIBUTING.md](CONTRIBUTING.md)!

# architecture

lock-free tree on a lock-free pagecache on a lock-free log. the pagecache scatters
partial page fragments across the log, rather than rewriting entire pages at a time
as B+ trees for spinning disks historically have. on page reads, we concurrently
scatter-gather reads across the log to materialize the page from its fragments.
check out the [architectural outlook](https://github.com/spacejam/sled/wiki/sled-architectural-outlook)
for a more detailed overview of where we're at and where we see things going!

# References

* [The Bw-Tree: A B-tree for New Hardware Platforms](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf)
* [LLAMA: A Cache/Storage Subsystem for Modern Hardware](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/llama-vldb2013.pdf)
* [Cicada: Dependably Fast Multi-Core In-Memory Transactions](http://15721.courses.cs.cmu.edu/spring2018/papers/06-mvcc2/lim-sigmod2017.pdf)
* [The Design and Implementation of a Log-Structured File System](https://people.eecs.berkeley.edu/~brewer/cs262/LFS.pdf)

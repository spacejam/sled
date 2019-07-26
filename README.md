

<p align="center">
  <img src="https://raw.githubusercontent.com/spacejam/sled/master/art/tree_face.png" width="20%" height="auto" />
</p>


# sled - ~~it's all downhill from here!!!~~
[![Build Status](https://travis-ci.org/spacejam/sled.svg?branch=master)](https://travis-ci.org/spacejam/sled)
[![crates.io](https://meritbadge.herokuapp.com/sled)](https://crates.io/crates/sled)
[![documentation](https://docs.rs/sled/badge.svg)](https://docs.rs/sled)
[![chat](https://img.shields.io/discord/509773073294295082.svg?logo=discord)](https://discord.gg/Z6VsXds)

An (alpha) modern embedded database. Doesn't your data deserve an (alpha) beautiful new home?

```rust
use sled::Db;

let tree = Db::start_default(path)?;

// set and get
tree.insert(k, v1);
assert_eq!(tree.get(&k), Ok(Some(v1)));

// compare and swap
tree.cas(k, Some(v1), Some(v2));

// scan forward
let mut iter = tree.scan(k);
assert_eq!(iter.next(), Some(Ok((k, v2))));
assert_eq!(iter.next(), None);

// deletion
tree.remove(&k);

// block until all operations are on-disk
tree.flush();
```

We also support [merge operators](https://github.com/spacejam/sled/wiki/merge-operators)!

# features

* API similar to a threadsafe `BTreeMap<Vec<u8>, Vec<u8>>`
* fully atomic single-key operations, supports CAS
* zero-copy reads
* subscription/watch semantics on key prefixes
* multiple keyspace support
* merge operators
* forward and reverse iterators
* a crash-safe monotonic ID generator capable of generating 75-125 million ID's per second
* [zstd](https://github.com/facebook/zstd) compression (use the `compression` build feature)
* cpu-scalable lock-free implementation
* SSD-optimized log-structured storage

# architecture

lock-free tree on a lock-free pagecache on a lock-free log. the pagecache scatters
partial page fragments across the log, rather than rewriting entire pages at a time
as B+ trees for spinning disks historically have. on page reads, we concurrently
scatter-gather reads across the log to materialize the page from its fragments.
check out the [architectural outlook](https://github.com/spacejam/sled/wiki/sled-architectural-outlook)
for a more detailed overview of where we're at and where we see things going!

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
* concurrent snapshot delta generation and recovery
* first-class programmatic access to replication stream
* consensus protocol for [PC/EC](https://en.wikipedia.org/wiki/PACELC_theorem) systems
* pluggable conflict detection and resolution strategies for [PA/EL](https://en.wikipedia.org/wiki/PACELC_theorem) systems
* multiple collection types like tables, queues, Merkle trees, bloom filters, etc... unified under a single transactional and operational domain

# fund feature development and get commercial support

Want to support the project, prioritize a specific feature, or get commercial help with using sled in your project? [Ferrous Systems](https://ferrous-systems.com) provides commercial support for sled, and can work with you to solve a wide variety of storage problems across the latency-throughput, consistency, and price performance spectra. [Get in touch!](mailto:sled@ferrous-systems.com)

[![Ferrous Systems](https://ferrous-systems.com/images/ferrous-systems-mono-pos.svg)](https://ferrous-systems.com/)

# special thanks

[![Meili](https://avatars3.githubusercontent.com/u/43250847?s=200&v=4)](https://www.meilisearch.com/)

Special thanks to [Meili](https://www.meilisearch.com/) for providing engineering effort and other support to the sled project. They are building [an event store](https://blog.meilisearch.com/meilies-release/) backed by sled, and they offer [a full-text search system](https://github.com/meilisearch/MeiliDB) which has been a valuable case study helping to focus the sled roadmap for the future.

<p>
  <img src="https://user-images.githubusercontent.com/7989673/29498525-38a33f36-85cc-11e7-938d-ef6f10ba6fb3.png" width="20%" height="auto" />
</p>

Additional thanks to [Arm](https://www.arm.com/), [Works on Arm](https://www.worksonarm.com/) and [Packet](https://www.packet.com/), who have generously donated a 96 core monster machine to assist with intensive concurrency testing of sled. Each second that sled does not crash while running your critical stateful workloads, you are encouraged to thank these wonderful organizations. Each time sled does crash and lose your data, blame Intel.

# known issues, warnings

* the on-disk format is going to change in non-forward compatible ways
  before the `1.0.0` release! after that, we will always support
  forward migrations.
* quite young, should be considered unstable for the time being
* the C API is likely to change rapidly
* writepath is not well optimized yet. readpath is essentially wait-free and zero-copy.

# contribution welcome!

want to help advance the state of the art in open source embedded
databases? check out [CONTRIBUTING.md](CONTRIBUTING.md)!

# References

* [The Bw-Tree: A B-tree for New Hardware Platforms](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf)
* [LLAMA: A Cache/Storage Subsystem for Modern Hardware](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/llama-vldb2013.pdf)
* [Cicada: Dependably Fast Multi-Core In-Memory Transactions](http://15721.courses.cs.cmu.edu/spring2018/papers/06-mvcc2/lim-sigmod2017.pdf)
* [The Design and Implementation of a Log-Structured File System](https://people.eecs.berkeley.edu/~brewer/cs262/LFS.pdf)

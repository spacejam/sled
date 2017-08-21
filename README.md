# RSDB

a flash-sympathetic persistent lock-free B+ tree, pagecache, and log

[documentation](https://docs.rs/rsdb)

`rsdb = "0.7"`

```rust
extern crate rsdb;

let tree = rsdb::Config::default()
  .path(Some("/var/lib/mydb/storage_file".to_owned()))
  .tree();

let k1 = b"yo!".to_vec();
let v1 = b"v1".to_vec();

// set and get
tree.set(k1.clone(), v1.clone());
assert_eq!(tree.get(&k1), Some(v1.clone()));

let v2 = b"v2".to_vec();

// compare and swap
assert_eq!(
  //    key         old       new
  tree.cas(k1.clone(), Some(v1), Some(v2.clone())),
  Ok(()),
);

// scans
let mut iter = tree.scan(b"a non-present key before yo!");
assert_eq!(iter.next(), Some((k1, v2)));
assert_eq!(iter.next(), None);

// deletion
tree.del(b"yo!");
```

progress

- [x] lock-free log with reservable slots
- [x] lock-free pagecache with cache-friendly partial updates
- [x] lock-free b-link tree
- [x] recovery
- [x] zstd compression
- [x] LRU cache
- [x] pagetable snapshotting for faster recovery
- [x] epoch-based gc
- [ ] formal verification of lock-free algorithms via symbolic execution
- [ ] log cleaning
- [ ] higher-level interface with multi-key transaction and snapshot support
- [ ] merge operator support

# Goals

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

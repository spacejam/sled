
<table style="width:100%">
<tr>
  <td>
    <table style="width:100%">
      <tr>
        <td> key </td>
        <td> value </td>
      </tr>
      <tr>
        <td><a href="https://github.com/sponsors/spacejam">buy a coffee for us to convert into databases</a></td>
        <td><a href="https://github.com/sponsors/spacejam"><img src="https://img.shields.io/opencollective/backers/sled"></a></td>
      </tr>
      <tr>
        <td><a href="https://docs.rs/sled">documentation</a></td>
        <td><a href="https://docs.rs/sled"><img src="https://docs.rs/sled/badge.svg"></a></td>
      </tr>
      <tr>
        <td><a href="https://discord.gg/Z6VsXds">chat about databases with us</a></td>
        <td><a href="https://discord.gg/Z6VsXds"><img src="https://img.shields.io/discord/509773073294295082.svg?logo=discord"></a></td>
      </tr>
     </table>
  </td>
  <td>
<p align="center">
  <img src="https://raw.githubusercontent.com/spacejam/sled/master/art/tree_face_anti-transphobia.png" width="40%" height="auto" />
  </p>
  </td>
 </tr>
</table>


# sled - ~~it's all downhill from here!!!~~

A lightweight pure-rust high-performance transactional embedded database.

```rust
let tree = sled::open("/tmp/welcome-to-sled").expect("open");

// insert and get, similar to std's BTreeMap
tree.insert("KEY1", "VAL1");
assert_eq!(tree.get(&"KEY1"), Ok(Some(sled::IVec::from("VAL1"))));

// range queries
for kv in tree.range("KEY1".."KEY9") {}

// deletion
tree.remove(&"KEY1");

// atomic compare and swap
tree.compare_and_swap("KEY1", Some("VAL1"), Some("VAL2"));

// block until all operations are stable on disk
// (flush_async also available to get a Future)
tree.flush();
```

If you would like to work with structured data without paying expensive deserialization costs, check out the [structured](examples/structured.rs) example!

# performance

* [LSM tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)-like write performance
  with [traditional B+ tree](https://en.wikipedia.org/wiki/B%2B_tree)-like read performance
* over a billion operations in under a minute at 95% read 5% writes on 16 cores on a small dataset
* measure your own workloads rather than relying on some marketing for contrived workloads

what's the trade-off? sled uses too much disk space sometimes. this will improve significantly before 1.0.

# features

* [API](https://docs.rs/sled) similar to a threadsafe `BTreeMap<[u8], [u8]>`
* serializable multi-key and multi-Tree interactive [transactions](https://docs.rs/sled/latest/sled/struct.Tree.html#method.transaction)
* fully atomic single-key operations, supports [compare and swap](https://docs.rs/sled/latest/sled/struct.Tree.html#method.compare_and_swap)
* zero-copy reads
* [write batch support](https://docs.rs/sled/latest/sled/struct.Tree.html#method.apply_batch)
* [subscriber/watch semantics on key prefixes](https://github.com/spacejam/sled/wiki/reactive-semantics)
* [multiple keyspace/Tree support](https://docs.rs/sled/latest/sled/struct.Db.html#method.open_tree)
* [merge operators](https://github.com/spacejam/sled/wiki/merge-operators)
* forward and reverse iterators
* a crash-safe monotonic [ID generator](https://docs.rs/sled/latest/sled/struct.Db.html#method.generate_id) capable of generating 75-125 million unique ID's per second
* [zstd](https://github.com/facebook/zstd) compression (use the `compression` build feature)
* cpu-scalable lock-free implementation
* flash-optimized log-structured storage
* uses modern b-tree techniques such as prefix encoding and suffix truncation for reducing the storage costs of long keys

# a note on lexicographic ordering and endianness

If you want to store numerical keys in a way that will play nicely with sled's iterators and ordered operations, please remember to store your numerical items in big-endian form. Little endian (the default of many things) will often appear to be doing the right thing until you start working with more than 256 items (more than 1 byte), causing lexicographic ordering of the serialized bytes to diverge from the lexicographic ordering of their deserialized numerical form.

* Rust integral types have built-in `to_be_bytes` and `from_be_bytes` [methods](https://doc.rust-lang.org/std/primitive.u64.html#method.from_be_bytes).
* bincode [can be configured](https://docs.rs/bincode/1.2.0/bincode/struct.Config.html#method.big_endian) to store integral types in big-endian form.

# interaction with async

If your dataset resides entirely in cache (achievable at startup by setting the cache
to a large enough value and performing a full iteration) then all reads and writes are
non-blocking and async-friendly, without needing to use Futures or an async runtime.

To asynchronously suspend your async task on the durability of writes, we support the
[`flush_async` method](https://docs.rs/sled/latest/sled/struct.Tree.html#method.flush_async),
which returns a Future that your async tasks can await the completion of if they require
high durability guarantees and you are willing to pay the latency costs of fsync.
Note that sled automatically tries to sync all data to disk several times per second
in the background without blocking user threads.

We support async subscription to events that happen on key prefixes, because the
`Subscriber` struct implements `Future<Output=Option<Event>>`:

```rust
let sled = sled::open("my_db").unwrap();
let mut sub = sled.watch_prefix("");

sled.insert(b"a", b"a").unwrap();
sled.insert(b"a", b"a").unwrap();

drop(sled);

extreme::run(async move {
    while let Some(event) = (&mut sub).await {
        println!("got event {:?}", event);
    }
});
```

# minimum supported Rust version (MSRV)

We support Rust 1.39.0 and up.

# architecture

lock-free tree on a lock-free pagecache on a lock-free log. the pagecache scatters
partial page fragments across the log, rather than rewriting entire pages at a time
as B+ trees for spinning disks historically have. on page reads, we concurrently
scatter-gather reads across the log to materialize the page from its fragments.
check out the [architectural outlook](https://github.com/spacejam/sled/wiki/sled-architectural-outlook)
for a more detailed overview of where we're at and where we see things going!

# philosophy

1. don't make the user think. the interface should be obvious.
1. don't surprise users with performance traps.
1. don't wake up operators. bring reliability techniques from academia into real-world practice.
1. don't use so much electricity. our data structures should play to modern hardware's strengths.

# known issues, warnings

* if reliability is your primary constraint, use SQLite. sled is beta.
* if storage price performance is your primary constraint, use RocksDB. sled uses too much space sometimes.
* quite young, should be considered unstable for the time being.
* the on-disk format is going to change in ways that require [manual migrations](https://docs.rs/sled/latest/sled/struct.Db.html#method.export) before the `1.0.0` release!

# priorities

* rework the transaction API to eliminate surprises and limitations
* reduce space and memory usage
* the 1.0.0 release date is January 19, 2021 (sled's 5th birthday)
* combine merge operators with subscribers in a way that plays nicely with transactions
* typed trees for low-friction serialization
* replication support for both strongly and eventually consistent systems
* continue to improve testing and make certain bug classes impossible through construction
* continue to optimize the hell out of everything
* continue to improve documentation and examples
* continue to reduce compilation latency

# fund feature development

Like what we're doing? Help us out via [GitHub Sponsors](https://github.com/sponsors/spacejam)!

# special thanks

<p align="center">
  <a href="https://www.meilisearch.com/">
    <img src="https://avatars3.githubusercontent.com/u/43250847?s=200&v=4" width="20%" height="auto" />
  </a>
</p>

Special thanks to [Meili](https://www.meilisearch.com/) for providing engineering effort and other support to the sled project. They are building [an event store](https://blog.meilisearch.com/meilies-release/) backed by sled, and they offer [a full-text search system](https://github.com/meilisearch/MeiliDB) which has been a valuable case study helping to focus the sled roadmap for the future.

<p align="center">
  <a href="http://worksonarm.com">
    <img src="https://user-images.githubusercontent.com/7989673/29498525-38a33f36-85cc-11e7-938d-ef6f10ba6fb3.png" width="20%" height="auto" />
  </a>
</p>

Additional thanks to [Arm](https://www.arm.com/), [Works on Arm](https://www.worksonarm.com/) and [Packet](https://www.packet.com/), who have generously donated a 96 core monster machine to assist with intensive concurrency testing of sled. Each second that sled does not crash while running your critical stateful workloads, you are encouraged to thank these wonderful organizations. Each time sled does crash and lose your data, blame Intel.

# contribution welcome!

want to help advance the state of the art in open source embedded
databases? check out [CONTRIBUTING.md](CONTRIBUTING.md)!

# references

* [The Bw-Tree: A B-tree for New Hardware Platforms](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf)
* [LLAMA: A Cache/Storage Subsystem for Modern Hardware](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/llama-vldb2013.pdf)
* [Cicada: Dependably Fast Multi-Core In-Memory Transactions](http://15721.courses.cs.cmu.edu/spring2018/papers/06-mvcc2/lim-sigmod2017.pdf)
* [The Design and Implementation of a Log-Structured File System](https://people.eecs.berkeley.edu/~brewer/cs262/LFS.pdf)

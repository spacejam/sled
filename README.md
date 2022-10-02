
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
        <td><a href="https://github.com/sponsors/spacejam"><img src="https://img.shields.io/github/sponsors/spacejam"></a></td>
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
  <img src="https://raw.githubusercontent.com/spacejam/sled/main/art/tree_face_anti-transphobia.png" width="40%" height="auto" />
  </p>
  </td>
 </tr>
</table>


# sled - ~~it's all downhill from here!!!~~

An embedded database.

```rust
let tree = sled::open("/tmp/welcome-to-sled")?;

// insert and get, similar to std's BTreeMap
let old_value = tree.insert("key", "value")?;

assert_eq!(
  tree.get(&"key")?,
  Some(sled::IVec::from("value")),
);

// range queries
for kv_result in tree.range("key_1".."key_9") {}

// deletion
let old_value = tree.remove(&"key")?;

// atomic compare and swap
tree.compare_and_swap(
  "key",
  Some("current_value"),
  Some("new_value"),
)?;

// block until all operations are stable on disk
// (flush_async also available to get a Future)
tree.flush()?;
```

If you would like to work with structured data without paying expensive deserialization costs, check out the [structured](examples/structured.rs) example!

# features

* [API](https://docs.rs/sled) similar to a threadsafe `BTreeMap<[u8], [u8]>`
* serializable (ACID) [transactions](https://docs.rs/sled/latest/sled/struct.Tree.html#method.transaction)
  for atomically reading and writing to multiple keys in multiple keyspaces.
* fully atomic single-key operations, including [compare and swap](https://docs.rs/sled/latest/sled/struct.Tree.html#method.compare_and_swap)
* zero-copy reads
* [write batches](https://docs.rs/sled/latest/sled/struct.Tree.html#method.apply_batch)
* [subscribe to changes on key
  prefixes](https://docs.rs/sled/latest/sled/struct.Tree.html#method.watch_prefix)
* [multiple keyspaces](https://docs.rs/sled/latest/sled/struct.Db.html#method.open_tree)
* [merge operators](https://docs.rs/sled/latest/sled/doc/merge_operators/index.html)
* forward and reverse iterators over ranges of items
* a crash-safe monotonic [ID generator](https://docs.rs/sled/latest/sled/struct.Db.html#method.generate_id)
  capable of generating 75-125 million unique ID's per second
* [zstd](https://github.com/facebook/zstd) compression (use the
  `compression` build feature, disabled by default)
* cpu-scalable lock-free implementation
* flash-optimized log-structured storage
* uses modern b-tree techniques such as prefix encoding and suffix
  truncation for reducing the storage costs of long keys with shared
  prefixes. If keys are the same length and sequential then the
  system can avoid storing 99%+ of the key data in most cases,
  essentially acting like a learned index

# expectations, gotchas, advice

* Maybe one of the first things that seems weird is the `IVec` type.
  This is an inlinable `Arc`ed slice that makes some things more efficient.
* Durability: **sled automatically fsyncs every 500ms by default**,
  which can be configured with the `flush_every_ms` configurable, or you may
  call `flush` / `flush_async` manually after operations.
* **Transactions are optimistic** - do not interact with external state
  or perform IO from within a transaction closure unless it is
  [idempotent](https://en.wikipedia.org/wiki/Idempotent).
* Internal tree node optimizations: sled performs prefix encoding
  on long keys with similar prefixes that are grouped together in a range,
  as well as suffix truncation to further reduce the indexing costs of
  long keys. Nodes will skip potentially expensive length and offset pointers
  if keys or values are all the same length (tracked separately, don't worry
  about making keys the same length as values), so it may improve space usage
  slightly if you use fixed-length keys or values. This also makes it easier
  to use [structured access](examples/structured.rs) as well.
* sled does not support multiple open instances for the time being. Please
  keep sled open for the duration of your process's lifespan. It's totally
  safe and often quite convenient to use a global lazy_static sled instance,
  modulo the normal global variable trade-offs. Every operation is threadsafe,
  and most are implemented under the hood with lock-free algorithms that avoid
  blocking in hot paths.

# performance

* [LSM tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)-like write performance
  with [traditional B+ tree](https://en.wikipedia.org/wiki/B%2B_tree)-like read performance
* over a billion operations in under a minute at 95% read 5% writes on 16 cores on a small dataset
* measure your own workloads rather than relying on some marketing for contrived workloads

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

extreme::run(async move {
    while let Some(event) = (&mut sub).await {
        println!("got event {:?}", event);
    }
});
```

# minimum supported Rust version (MSRV)

We support Rust 1.62 and up.

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
* if you have a multi-process workload that rarely writes, use LMDB. sled is architected for use with long-running, highly-concurrent workloads such as stateful services or higher-level databases.
* quite young, should be considered unstable for the time being.
* the on-disk format is going to change in ways that require [manual migrations](https://docs.rs/sled/latest/sled/struct.Db.html#method.export) before the `1.0.0` release!

# priorities

1. A full rewrite of sled's storage subsystem is happening on a modular basis as part of the [komora project](https://github.com/komora-io), in particular the marble storage engine. This will dramatically lower both the disk space usage (space amplification) and garbage collection overhead (write amplification) of sled.
2. The memory layout of tree nodes is being completely rewritten to reduce fragmentation and eliminate serialization costs.
3. The merge operator feature will change into a trigger feature that resembles traditional database triggers, allowing state to be modified as part of the same atomic writebatch that triggered it for retaining serializability with reactive semantics.

# fund feature development

Like what we're doing? Help us out via [GitHub Sponsors](https://github.com/sponsors/spacejam)!

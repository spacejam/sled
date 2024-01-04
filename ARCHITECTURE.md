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

# sled 1.0 architecture

## in-memory

* Lock-free B+ tree index, extracted into the [`concurrent-map`](https://github.com/komora-io/concurrent-map) crate.
* The lowest key from each leaf is stored in this in-memory index.
* To read any leaf that is not already cached in memory, at most one disk read will be required.
* RwLock-backed leaves, using the ArcRwLock from the [`parking_lot`](https://github.com/Amanieu/parking_lot) crate. As a `Db` grows, leaf contention tends to go down in most use cases. But this may be revisited over time if many users have issues with RwLock-related contention. Avoiding full RCU for updates on the leaves results in many of the performance benefits over sled 0.34, with significantly lower memory pressure.
* A simple but very high performance epoch-based reclamation technique is used for safely deferring frees of in-memory index data and reuse of on-disk heap slots, extracted into the [`ebr`](https://github.com/komora-io/ebr) crate.
* A scan-resistant LRU is used for handling eviction. By default, 20% of the cache is reserved for leaves that are accessed at most once. This is configurable via `Config.entry_cache_percent`. This is handled by the extracted [`cache-advisor`](https://github.com/komora-io/cache-advisor) crate. The overall cache size is set by the `Config.cache_size` configurable.

## write path

* This is where things get interesting. There is no traditional WAL. There is no LSM. Only metadata is logged atomically after objects are written in parallel.
* The important guarantees are:
  * all previous writes are durable after a call to `Db::flush` (This is also called periodically in the background by a flusher thread)
  * all write batches written using `Db::apply_batch` are either 100% visible or 0% visible after crash recovery. If it was followed by a flush that returned `Ok(())` it is guaranteed to be present.
* Atomic ([linearizable](https://jepsen.io/consistency/models/linearizable)) durability is provided by marking dirty leaves as participants in "flush epochs" and performing atomic batch writes of the full epoch at a time, in order. Each call to `Db::flush` advances the current flush epoch by 1.
* The atomic write consists in the following steps:
  1. User code or the background flusher thread calls `Db::flush`.
  1. In parallel (via [rayon](https://docs.rs/rayon)) serialize and compress each dirty leaf with zstd (configurable via `Config.zstd_compression_level`).
  1. Based on the size of the bytes for each object, choose the smallest heap file slot that can hold the full set of bytes. This is an on-disk slab allocator.
  1. Slab slots are not power-of-two sized, but tend to increase in size by around 20% from one to the next, resulting in far lower fragmentation than typical page-oriented heaps with either constant-size or power-of-two sized leaves.
  1. Write the object to the allocated slot from the rayon threadpool.
  1. After all writes, fsync the heap files that were written to.
  1. If any writes were written to the end of the heap file, causing it to grow, fsync the directory that stores all heap files.
  1. After the writes are stable, it is now safe to write an atomic metadata batch that records the location of each written leaf in the heap. This is a simple framed batch of `(low_key, slab_slot)` tuples that are initially written to a log, but eventually merged into a simple snapshot file for the metadata store once the log becomes larger than the snapshot file.
  1. Fsync of the metadata log file.
  1. Fsync of the metadata log directory.
  1. After the atomic metadata batch write, the previously occupied slab slots are marked for future reuse with the epoch-based reclamation system. After all threads that may have witnessed the previous location have finished their work, the slab slot is added to the free `BinaryHeap` of the slot that it belongs to so that it may be reused in future atomic write batches.
  1. Return `Ok(())` to the caller of `Db::flush`.
* Writing objects before the metadata write is random, but modern SSDs handle this well. Even though the SSD's FTL will be working harder to defragment things periodically than if we wrote a few megabytes sequentially with each write, the data that the FTL will be copying will be mostly live due to the eager leaf write-backs.

## recovery

* Recovery involves simply reading the atomic metadata store that records the low key for each written leaf as well as its location and mapping it into the in-memory index. Any gaps in the slabs are then used as free slots.
* Any write that failed to complete its entire atomic writebatch is treated as if it never happened, because no user-visible flush ever returned successfully.
* Rayon is also used here for parallelizing reads of this metadata. In general, this is extremely fast compared to the previous sled recovery process.

## tuning

* The larger the `LEAF_FANOUT` const generic on the high-level `Db` struct (default `1024`), the smaller the in-memory leaf index and the better the compression ratio of the on-disk file, but the more expensive it will be to read the entire leaf off of disk and decompress it.
* You can choose to turn the `LEAF_FANOUT` relatively low to make the system behave more like an Index+Log architecture, but overall disk size will grow and write performance will decrease.
* NB: changing `LEAF_FANOUT` after writing data is not supported.

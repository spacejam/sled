# RSDB

flash-sympathetic lock-free persistent tree

[documentation](https://docs.rs/rsdb)

progress

- [x] lock-free log-structured store with reservable slots
- [x] lock-free page store supporting cache-friendly partial updates
- [x] lock-free b-link tree
- [x] recovery
- [x] zstd compression
- [ ] epoch-based gc (LEAKS MEMORY FOR NOW LOLOLOLOLOL)
- [ ] LRU cache
- [ ] pagetable snapshotting for faster recovery
- [ ] multi-key transactions and MVCC using a higher-level `DB` interface

# Goals

1. don't use so much electricity. our data structures should play to our hardware's current strengths.
1. don't surprise users with performance traps.
1. bring reliability techniques from academia into real-world practice.

# Architecture

Lock-free trees on a lock-free pagecache on a lock-free log. The pagecache scatters
partial page fragments across the log, rather than rewriting entire pages at a time
as B+ trees for spinning disks historically have. On page reads, we scatter-gather
reads across the log to materialize the page from its fragments.

If you want to build a new structure on the `PageCache`, implement the `Materializer` trait:

```
pub trait Materializer: Send + Sync {
    type MaterializedPage;
    type PartialPage: Serialize + DeserializeOwned;
    type Recovery;

    /// Used to generate the result of `get` requests on the `PageCache`
    fn materialize(&self, Vec<Self::PartialPage>) -> Self::MaterializedPage;

    /// Used to compress long chains of partial pages into a condensed form
    /// during compaction.
    fn consolidate(&self, Vec<Self::PartialPage>) -> Vec<Self::PartialPage>;

    /// Used to feed custom recovery information back to a higher-level abstraction
    /// during startup. For example, a B-Link tree must know what the current
    /// root node is before it can start serving requests.
    fn recover(&mut self, Self::MaterializedPage) -> Option<Self::Recovery>;
}
```

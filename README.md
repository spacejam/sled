# RSDB

flash-sympathetic lock-free persistent trees

progress

- [x] lock-free log-structured store with reservable slots
- [ ] lock-free pagecache supporting cache-friendly partial updates, simple transactions, epoch-based gc
- [x] lock-free b-link tree
- [ ] transaction manager
- [ ] lock-free quad/pr/r tree
- [ ] lock-free fractal tree

# Interfaces

## [Tree](src/tree.rs)

A lock-free tree.

```rust
impl Tree {
  pub fn new(store: Option<PageCache>) -> Tree;

  pub fn get(&self, key: &[u8]) -> Option<Value>;

  pub fn cas(&self, key: Key, old: Option<Value>, new: Value) -> Result<(), Option<Value>>;

  pub fn set(&self, key: Key, value: Value);

  pub fn del(&self, key: &[u8]) -> Option<Value>;

  pub fn scan(&self, from: Bound, to: Bound) -> Iterator<(Key, Value)>;
}
```

## [Paging](src/page.rs)

A lock-free pagecache.

```rust
impl PageCache {
  pub fn new(viewer: PageMaterializer, log: Log) -> PageCache;

  pub fn allocate(&self) -> PageID;

  pub fn free(&self, PageID);

  pub fn get(&self, PageID) -> (PageMaterializer::MaterializedPage, CASKey);

  pub fn append(&self, PageID, CASKey, PageMaterializer::PartialPage);

  pub fn flush(&self, PageID) -> LogID;

  /// returns the current stable offset written to disk
  fn stable_offset(&self) -> LogID;

  /// blocks until the specified id has been made stable on disk
  pub fn make_stable(&self, LogID);
}

trait PageMaterializer {
  type MaterializedPage;
  type PartialPage;

  fn materialize(&self, Vec<Self::PartialPage>) -> Self::MaterializedPage;
}
```

## [Log-Structured Storage](src/log.rs)

A lock-free log-structured store.

```rust
trait Log {
  /// claim a spot on disk, which can later be filled or aborted
  fn reserve(&self, buf: Vec<u8>) -> Reservation;

  /// write a buffer to disk
  fn write(&self, buf: Vec<u8>) -> LogID;

  /// read a buffer from the disk
  fn read(&self, id: LogID) -> io::Result<Option<Vec<u8>>>;

  /// returns the current stable offset written to disk
  fn stable_offset(&self) -> LogID;

  /// blocks until the specified id has been made stable on disk
  fn make_stable(&self, id: LogID);

  /// shut down the writer threads
  fn shutdown(self);

  /// deallocates the data part of a log id
  fn punch_hole(&self, id: LogID);

  /// traverse the entries in a log, starting from a particular offset
  fn iter_from(&self, id: LogID) -> LogIter;
}

impl Reservation {
  /// cancel the reservation, placing a failed flush on disk
  pub fn abort(self);

  /// complete the reservation, placing the buffer on disk at the log_id
  pub fn complete(self) -> LogID;

  /// get the log_id for accessing this buffer in the future
  pub fn log_id(&self) -> LogID;
}
```

# Architecture

## Log

goals:

1. (for: transactions) allow users to reserve space and
   a file offset for prospective writes
1. (for: transactions) reservations may be canceled
1. (for: durability) allow users to retrieve a message by
   providing an offset ID
1. (for: durability) checksum all messages written
1. (for: performance) support concurrent writes into
   buffers which will be asynchronously flushed when full

non-goals:

1. transactionality, this is handled by the transaction layer,
   and to a small extent the paging system (just for page merges & splits)
1. knowing about valid offsets, this is maintained by the page table

## paging

goals:

1. (for: transactions) provide the current stable log offset
1. (for: transactions) allow blocking on the stabilization of a log offset
1. (for: transactions) maintain a transaction table supporting deltas,
   allocations, and freed pages. we support transactions for facilitating
   page splits and merges.
1. (for: durability) scatter-gather page fragments when not in memory
1. (for: performance) allow pages to be compacted
1. (for: performance) maintain an LRU cache for accessed page fragments
1. (for: performance) use epoch-based GC for safely de/reallocating
   deleted pages, page ID's, and pages that fall out of LRU while being
   concurrently accessed by lock-free algorithms

non-goals:

1. our transactions are for facilitating splits and merges, and no full-page
   updates may be included
1. transactions at this layer do not enforce WAL protocols. higher layers must
   enforce durability needs by using offset observation and blocking
1. we have no knowledge of the structure of the pages


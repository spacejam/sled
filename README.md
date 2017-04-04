# RSDB

flash-sympathetic lock-free persistent trees

progress

- [x] lock-free log-structured store with reservable slots
- [ ] lock-free pagecache supporting cache-friendly partial updates, simple transactions, epoch-based gc
- [ ] lock-free b-link tree
- [ ] transaction manager
- [ ] lock-free quad/pr/r tree
- [ ] lock-free fractal tree

# Interfaces

## [Transactions](src/tx.rs)

api

```rust
```

## [Tree](src/tree.rs)

api

```rust
```

## [Paging](src/page.rs)

api

```rust
impl PageCache {
    pub fn enroll_in_epoch(&self) -> Epoch;
    pub fn delta(self, pid: PageID, delta: Delta) -> LogID;
    pub fn replace(self, pid: PageID, new_page: Page) -> LogID;
    pub fn read(self, pid: PageID) -> Data;
    pub fn flush(self, page_id: PageID, annotation: Vec<u8>) -> LogID;
    pub fn make_stable(self, log_coords: LogID);
    pub fn hi_stable(self, ) -> LogID;
    pub fn allocate(self, ) -> PageID;
    pub fn free(self, pid: PageID);
    pub fn tx_begin(self, id: TxID);
    pub fn tx_commit(self, id: TxID);
    pub fn tx_abort(self, id: TxID);
}
```

## [Logged Storage](src/log.rs)

api

```rust
impl Log {
    /// create new lock-free log
    pub fn start_system(path: String) -> Log;

    /// claim a spot on disk, which can later be filled or aborted
    pub fn reserve(&self, sz: usize) -> Reservation;

    /// write a buffer to disk
    pub fn write(&self, buf: Vec<u8>) -> LogID;

    /// read a buffer from the disk
    pub fn read(&self, id: LogID) -> io::Result<Option<Vec<u8>>>;

    /// returns the current stable offset written to disk
    pub fn stable_offset(&self) -> LogID;

    /// blocks until the specified id has been made stable on disk
    pub fn make_stable(&self, id: LogID);

    /// shut down the writer threads
    pub fn shutdown(self);

    /// deallocates the data part of a log id
    pub fn punch_hole(&self, id: LogID);
}

impl Reservation {
    /// cancel the reservation, placing a failed flush on disk
    pub fn abort(self);

    /// complete the reservation, placing the buffer on disk at the log_id
    pub fn complete(self, buf: Vec<u8>) -> LogID;

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


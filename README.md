# RSDB

flash-sympathetic lock-free persistent trees

progress

- [x] lock-free log-structured store with reservable slots
- [ ] lock-free pagecache supporting cache-friendly partial updates, simple transactions, epoch-based gc
- [ ] lock-free b-link tree
- [ ] transaction manager
- [ ] lock-free quad/pr/r tree

# Architecture

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
    fn delta(pid: PageID, delta: Delta) -> LogID;
    fn replace(pid: PageID, new_page: Page) -> LogID;
    fn read(pid: PageID) -> Data;
    fn flush(page_id: PageID, annotation: Vec<u8>) -> LogID;
    fn make_stable(log_coords: LogID);
    fn hi_stable() -> LogID;
    fn allocate() -> PageID;
    fn free(pid: PageID);
    fn tx_begin(id: TxID);
    fn tx_commit(id: TxID);
    fn tx_abort(id: TxID);
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

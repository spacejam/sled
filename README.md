# RSDB

transactional KV store backed by a lock-free b-link tree backed by a log-structured page store

uses fallocate to reduce write amplification. requires a filesystem that supports `FALLOC_FL_PUNCH_HOLE`:

* XFS (since Linux 2.6.38)
* ext4 (since Linux 3.0)
* Btrfs (since Linux 3.7)
* tmpfs (since Linux 3.5)

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

## [Logged storage](src/log.rs)

api

```rust
impl Log {
    pub fn start_system() -> Log;
    pub fn reserve(&self, sz: usize) -> Reservation;
    pub fn write(&self, buf: Vec<u8>) -> LogID;
    pub fn read(&self, id: LogID) -> io::Result<LogData>;
    pub fn make_stable(&self, id: LogID);
}
impl Reservation {
    pub fn abort(self);
    pub fn complete(self, buf: Vec<u8>) -> LogID;
    pub fn log_id(&self) -> LogID;
}
```

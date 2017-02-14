use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

trait PageCache {
    // data ops

    // NB don't let this grow beyond 4-8 (6 maybe ideal)
    fn delta(pid: PageID, delta: Delta) -> LogID;

    fn replace(pid: PageID, new_page: Page) -> LogID;

    /// return page. page table may only have disk ref, and will need to load it in
    fn read(pid: PageID) -> Data;

    // mgmt ops

    /// copies page into log I/O buf
    /// adds flush delta with caller annotation
    /// page table stores log addr
    /// may not yet be stable on disk
    fn flush(page_id: PageID, annotation: Vec<u8>) -> LogID;

    /// ensures all log data up until the provided address is stable
    fn make_stable(log_coords: LogID);

    /// returns current stable point in the log
    fn hi_stable() -> LogID;

    /// create new page, persist the table
    fn allocate() -> PageID;

    /// adds page to current epoch's pending freelist, persists table
    fn free(pid: PageID);

    // tx ops

    /// add a tx id (lsn) to tx table, maintained by CL
    fn tx_begin(id: TxID);

    /// tx removed from tx table
    /// tx is committed
    /// CAS page table
    /// tx flushed to LSS
    fn tx_commit(id: TxID);

    /// tx removed from tx table
    /// changed pages in cache are reset
    fn tx_abort(id: TxID);
}

#[derive(Debug, Clone, RustcDecodable, RustcEncodable)]
#[repr(C)]
struct Checkpoint {
    log_ids: HashMap<PageID, LogID>,
    free: Vec<PageID>,
    gc_pos: LogID,
    log_replay_idx: LogID,
}

#[derive(Debug, Clone, RustcDecodable, RustcEncodable)]
#[repr(C)]
struct PageLink {
    data: Data,
    pid: PageID,
    lid: Option<LogID>,
    child: Option<PageID>,
}

struct Cache {
    highest_pid: PageID,
    heads: RwLock<BTreeMap<PageID, AtomicUsize>>,
    cached: RwLock<BTreeMap<LogID, Arc<Data>>>,
    log: Box<log::Log>,
    // freelist managed as stack, biased to reuse low ID's
    free: Vec<PageID>,
}

struct ATT {

}

const MAX_BUF_SZ: usize = 1_000_000;

pub struct BufferLease;

impl BufferLease {
    fn failed_flush(&mut self) {
        // fills lease with a FailedFlush
    }
}

struct IOBuffer {
    file_offset: usize,
    meta: BufferMeta,
    buf: [u8; MAX_BUF_SZ],
}

struct BufferMeta(AtomicUsize);

impl Default for BufferMeta {
    fn default() -> BufferMeta {
        BufferMeta(AtomicUsize::new(0))
    }
}

impl BufferMeta {
    fn seal(&self) {
        0b1000_0000_0000_0000_u32;
    }

    fn is_sealed(v: u32) -> bool {
        v >> 31 == 1
    }

    fn mk_sealed(v: u32) -> u32 {
        v | 1 << 31
    }

    fn n_writers(v: u32) -> u32 {
        v << 1 >> 25
    }

    fn incr_writers(v: u32) -> u32 {
        v + (1 << 24)
    }

    fn decr_writers(v: u32) -> u32 {
        v - (1 << 24)
    }

    fn offset(v: u32) -> u32 {
        v << 8 >> 8
    }

    fn bump_offset(v: u32, by: u32) -> u32 {
        assert!(by >> 24 == 0);
        v + by
    }
}

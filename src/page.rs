#![allow(unused)]
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use super::*;

#[derive(Clone)]
pub struct PageCache {
    // end_stable_log is the highest LSN that may be flushed
    pub esl: Arc<AtomicUsize>,
    // highest_pid marks the max PageID we've allocated
    highest_pid: Arc<AtomicUsize>,
    // table maps from PageID to a stack of entries
    table: Radix<Stack<PageCacheEntry>>,
    // gc maps from Epoch to head of consolidated chain
    gc: Radix<Stack<PageCacheEntry>>,
    // free stores the per-epoch set of pages to free when epoch is clear
    free: Stack<PageID>,
    // current_epoch is used for GC of removed pages
    current_epoch: Arc<AtomicUsize>,
    // log is our lock-free log store
    log: Log,
}

#[derive(Clone)]
struct PageCacheEntry {
    log_id: LogID,
    page_id: PageID,
    data: *mut Data,
}

impl PageCache {
    pub fn open() -> PageCache {
        let log = Log::start_system();
        let esl = Arc::new(AtomicUsize::new(log.stable_offset() as usize));
        let highest_pid = Arc::new(AtomicUsize::new(log.stable_offset() as usize));
        PageCache {
            esl: esl,
            highest_pid: highest_pid,
            table: Radix::default(),
            gc: Radix::default(),
            free: Stack::default(),
            current_epoch: Arc::new(AtomicUsize::new(0)),
            log: log,
        }
    }

    // data ops

    // NB don't let this grow beyond 4-8 (6 maybe ideal)
    fn delta(pid: PageID, lsn: TxID, delta: Delta) -> Result<LogID, ()> {
        unimplemented!()
    }

    // NB must only consolidate deltas with LSN <= ESL
    fn attempt_consolidation(pid: PageID, new_page: Page) -> Result<(), ()> {
        unimplemented!()
    }

    /// return page. page table may only have disk ref, and will need to load it in
    fn read(pid: PageID) -> Result<Data, ()> {
        unimplemented!()
    }

    // mgmt ops

    /// copies page into log I/O buf
    /// adds flush delta with caller annotation
    /// page table stores log addr
    /// may not yet be stable on disk
    // NB don't include any insert/updates with higher LSN than ESL
    fn flush(page_id: PageID, annotation: Vec<u8>) -> LogID {
        unimplemented!()
    }

    /// ensures all log data up until the provided address is stable
    fn make_stable(log_coords: LogID) {
        unimplemented!()
    }

    /// returns current stable point in the log
    fn hi_stable() -> LogID {
        unimplemented!()
    }

    /// create new page, persist the table
    fn allocate() -> PageID {
        // try to pop free list

        // else bump max ID and create new base page
        unimplemented!()
    }

    /// adds page to current epoch's pending freelist, persists table
    fn free(pid: PageID) -> Result<(), ()> {
        unimplemented!()
    }

    // tx ops

    /// add a tx id (lsn) to tx table, maintained by CL
    fn tx_begin(id: TxID) {
        unimplemented!()
    }

    /// tx removed from tx table
    /// tx is committed
    /// CAS page table
    /// tx flushed to LSS
    fn tx_commit(id: TxID) {
        unimplemented!()
    }

    /// tx removed from tx table
    /// changed pages in cache are reset
    fn tx_abort(id: TxID) {
        unimplemented!()
    }
}

#[derive(Debug, Clone, RustcDecodable, RustcEncodable)]
#[repr(C)]
struct Checkpoint {
    log_ids: Vec<(PageID, LogID)>,
    free: Vec<PageID>,
    gc_pos: LogID,
    log_replay_idx: LogID,
}

struct PageLink {
    data: Data,
    pid: PageID,
    lid: Option<LogID>,
    child: Option<PageID>,
}

pub struct Page;

pub enum Record {
    Page(Page),
    Delta(Delta),
}

pub struct Annotation;

enum Data {
    // (separator, pointer)
    Index(Vec<(Key, *mut Data)>),
    Leaf(Vec<(Key, Record)>),
}

struct Node {
    data: Data,
    lo_k: Key,
    hi_k: Key,
    next: PageID,
}

pub enum Delta {
    Update(Key, Value),
    Insert(Key, Value),
    Delete(Key),
    DeleteNode,
    MergePage {
        right: *mut Delta,
        right_hi_k: Key,
    },
    MergeIndex {
        lo_k: Key,
        hi_k: Key,
    },
    SplitPage {
        key: Key,
        right: PageID,
    },
    SplitIndex {
        left_k: Key,
        right_k: Key,
        right: PageID,
    },
    TxBegin(TxID), // in-mem
    TxCommit(TxID), // in-mem
    TxAbort(TxID), // in-mem
    Load, // should this be a swap operation on the data pointer?
    Flush {
        annotation: Annotation,
        highest_lsn: TxID,
    }, // in-mem
    PartialSwap(LogID), /* indicates part of page has been swapped out,
                         * shows where to find it */
}

// PartialView incrementally seeks to answer update / read questions by
// being aware of splits / merges while traversing a tree.
// Remember traversal path, which facilitates left scans, split retries, etc...
// Whenever an incomplete SMO is encountered by update / SMO, this thread must try to
// complete it.
struct PartialView;

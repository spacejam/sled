#![allow(unused)]
use std::fs::{self, File, OpenOptions};
use std::path::Path;
use std::io::{self, Read};
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use super::*;

#[derive(Clone)]
pub struct Pager {
    // end_stable_log is the highest LSN that may be flushed
    pub esl: Arc<AtomicUsize>,
    // highest_pid marks the max PageID we've allocated
    highest_pid: Arc<AtomicUsize>,
    // table maps from PageID to a stack of entries
    table: Radix<Stack<Node>>,
    // gc maps from Epoch to head of consolidated chain
    gc: Radix<Stack<Node>>,
    // free stores the per-epoch set of pages to free when epoch is clear
    free: Stack<PageID>,
    // current_epoch is used for GC of removed pages
    current_epoch: Arc<AtomicUsize>,
    // log is our lock-free log store
    log: Log,
}

impl Pager {
    pub fn open() -> Pager {
        recover().unwrap_or_else(|_| {
            Pager {
                esl: Arc::new(AtomicUsize::new(0)),
                highest_pid: Arc::new(AtomicUsize::new(0)),
                table: Radix::default(),
                gc: Radix::default(),
                free: Stack::default(),
                current_epoch: Arc::new(AtomicUsize::new(0)),
                log: Log::start_system(),
            }
        })
    }

    // data ops

    // NB don't let this grow beyond 4-8 (6 maybe ideal)
    pub fn delta(pid: PageID, lsn: TxID, delta: Delta) -> Result<LogID, ()> {
        unimplemented!()
    }

    // NB must only consolidate deltas with LSN <= ESL
    pub fn attempt_consolidation(pid: PageID, new_page: Page) -> Result<(), ()> {
        unimplemented!()
    }

    /// return page. page table may only have disk ref, and will need to load it in
    pub fn read(&self, pid: PageID) -> Option<*mut Stack<Node>> {
        if let Some(stack_ptr) = self.table.get(pid) {
            Some(stack_ptr)
        } else {
            None
        }
    }

    // mgmt ops

    /// copies page into log I/O buf
    /// adds flush delta with caller annotation
    /// page table stores log addr
    /// may not yet be stable on disk
    // NB don't include any insert/updates with higher LSN than ESL
    pub fn flush(page_id: PageID, annotation: Vec<u8>) -> LogID {
        unimplemented!()
    }

    /// ensures all log data up until the provided address is stable
    pub fn make_stable(log_coords: LogID) {
        unimplemented!()
    }

    /// returns current stable point in the log
    pub fn hi_stable() -> LogID {
        unimplemented!()
    }

    /// create new page, persist the table
    pub fn allocate() -> PageID {
        // try to pop free list

        // else bump max ID and create new base page
        unimplemented!()
    }

    /// adds page to current epoch's pending freelist, persists table
    pub fn free(pid: PageID) -> Result<(), ()> {
        unimplemented!()
    }

    // tx ops

    /// add a tx id (lsn) to tx table, maintained by CL
    pub fn tx_begin(id: TxID) {
        unimplemented!()
    }

    /// tx removed from tx table
    /// tx is committed
    /// CAS page table
    /// tx flushed to LSS
    pub fn tx_commit(id: TxID) {
        unimplemented!()
    }

    /// tx removed from tx table
    /// changed pages in cache are reset
    pub fn tx_abort(id: TxID) {
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

impl Default for Checkpoint {
    fn default() -> Checkpoint {
        Checkpoint {
            log_ids: vec![],
            free: vec![],
            gc_pos: 0,
            log_replay_idx: 0,
        }
    }
}



// PartialView incrementally seeks to answer update / read questions by
// being aware of splits / merges while traversing a tree.
// Remember traversal path, which facilitates left scans, split retries, etc...
// Whenever an incomplete SMO is encountered by update / SMO, this thread must try to
// complete it.
struct PartialView;

// load checkpoint, then read from log
fn recover() -> io::Result<Pager> {
    fn file(path: &str) -> io::Result<fs::File> {
        OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path)
    }

    let checkpoint: Checkpoint = {
        let path = "rsdb.pagetable_checkpoint";
        let mut f = file(path)?;

        let mut data = vec![];
        f.read_to_end(&mut data).unwrap();
        ops::from_binary::<Checkpoint>(data).unwrap_or_else(|_| {
            Arc::new(AtomicUsize::new(0));
            // file does not contain valid checkpoint
            let corrupt_path = format!("rsdb.corrupt_checkpoint.{}", time::get_time().sec);
            fs::rename(path, corrupt_path);
            Checkpoint::default()
        })
    };

    // TODO mv failed to load log to .corrupt_log.`date +%s`
    // continue from snapshot's LogID

    let path = "rsdb.log";
    let corrupt_path = format!("rsdb.corrupt_log.{}", time::get_time().sec);
    let mut file = file(path)?;
    let mut read = 0;
    loop {
        // until we hit end/tear:
        //   sizeof(size) >= remaining ? read size : keep file ptr here and trim with warning
        //   size >= remaining ? read remaining : back up file ptr sizeof(size) and trim with warning
        //   add msg to map

        let (mut k_len_buf, mut v_len_buf) = ([0u8; 4], [0u8; 4]);
        read_or_break!(file, k_len_buf, read);
        read_or_break!(file, v_len_buf, read);
        let (klen, vlen) = (ops::array_to_usize(k_len_buf), ops::array_to_usize(v_len_buf));
        let (mut k_buf, mut v_buf) = (Vec::with_capacity(klen), Vec::with_capacity(vlen));
        read_or_break!(file, k_buf, read);
        read_or_break!(file, v_buf, read);
        break;
    }

    // clear potential tears
    file.set_len(read as u64)?;

    // TODO properly make these from checkpoint
    let esl = Arc::new(AtomicUsize::new(0));
    let highest_pid = Arc::new(AtomicUsize::new(0));
    let table = Radix::default();
    let gc = Radix::default();
    let free = Stack::default();
    let current_epoch = Arc::new(AtomicUsize::new(0));

    Ok(Pager {
        esl: esl, // Arc<AtomicUsize>,
        highest_pid: highest_pid, // Arc<AtomicUsize>,
        table: table, // Radix<Stack<PagerEntry>>,
        gc: gc, // Radix<Stack<PagerEntry>>,
        free: free, // Stack<PageID>,
        current_epoch: current_epoch, // Arc<AtomicUsize>,
        log: Log::start_system(),
    })
}

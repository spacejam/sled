#![allow(unused)]
use std::fs::{self, File, OpenOptions};
use std::path::Path;
use std::io::{self, Read};
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::thread;

use super::*;

// NB ping-pong correctness depends on this being 2
const N_EPOCHS: usize = 2;

pub struct Epoch {
    header: Arc<AtomicUsize>,
    gc_pids: Arc<Vec<Stack<PageID>>>,
    gc_pages: Arc<Vec<Stack<*mut stack::Node<Node>>>>,
    current_epoch: Arc<AtomicUsize>,
    cleaned_epochs: Arc<AtomicUsize>,
    free: Arc<Stack<PageID>>,
}

impl Drop for Epoch {
    fn drop(&mut self) {
        // TODO revisit when sane
        loop {
            let cur = self.header.load(Ordering::SeqCst);
            let decr = ops::decr_writers(cur as u32);
            if cur == self.header.compare_and_swap(cur, decr as usize, Ordering::SeqCst) {
                if ops::n_writers(decr) == 0 && ops::is_sealed(decr) {
                    // our responsibility to put PageID's into free stack, free pages
                    let idx = self.cleaned_epochs.load(Ordering::SeqCst);
                    let pids = self.gc_pids[idx % N_EPOCHS].pop_all();
                    let page_ptrs = self.gc_pages[idx % N_EPOCHS].pop_all();
                    for pid in pids.into_iter() {
                        while self.free.try_push(pid).is_err() {}
                    }
                    for ptr in page_ptrs.into_iter() {
                        let mut cursor = ptr;
                        while !cursor.is_null() {
                            let node: Box<stack::Node<Node>> = unsafe { Box::from_raw(cursor) };
                            cursor = node.next();
                        }
                    }
                    self.header.store(0, Ordering::SeqCst);
                    self.cleaned_epochs.fetch_add(1, Ordering::SeqCst);
                }
                return;
            }
            println!("epoch drop spin");
        }
    }
}

impl Epoch {
    pub fn free_page_id(&self, pid: PageID) {
        let idx = self.current_epoch.load(Ordering::SeqCst);
        while self.gc_pids[idx % N_EPOCHS].try_push(pid).is_err() {}
    }

    pub fn free_ptr(&self, ptr: *mut stack::Node<Node>) {
        let idx = self.current_epoch.load(Ordering::SeqCst);
        while self.gc_pages[idx % N_EPOCHS].try_push(ptr).is_err() {}
    }
}

#[derive(Clone)]
pub struct Pager {
    // end_stable_log is the highest LSN that may be flushed
    pub esl: Arc<AtomicUsize>,
    // highest_pid marks the max PageID we've allocated
    highest_pid: Arc<AtomicUsize>,
    // table maps from PageID to a stack of entries
    table: Radix<Stack<Node>>,
    // gc related stuff
    gc_pids: Arc<Vec<Stack<PageID>>>,
    gc_pages: Arc<Vec<Stack<*mut stack::Node<Node>>>>,
    gc_headers: Vec<Arc<AtomicUsize>>,
    // free stores the per-epoch set of pages to free when epoch is clear
    free: Arc<Stack<PageID>>,
    // current_epoch is used for GC of removed pages
    current_epoch: Arc<AtomicUsize>,
    cleaned_epochs: Arc<AtomicUsize>,
    // log is our lock-free log store
    log: Log,
}

impl Default for Pager {
    fn default() -> Pager {
        Pager {
            esl: Arc::new(AtomicUsize::new(0)),
            highest_pid: Arc::new(AtomicUsize::new(0)),
            table: Radix::default(),
            gc_pids: Arc::new(rep_no_copy!(Stack::default(); N_EPOCHS)),
            gc_pages: Arc::new(rep_no_copy!(Stack::default(); N_EPOCHS)),
            gc_headers: rep_no_copy!(Arc::new(AtomicUsize::new(0)); N_EPOCHS),
            free: Arc::new(Stack::default()),
            current_epoch: Arc::new(AtomicUsize::new(0)),
            cleaned_epochs: Arc::new(AtomicUsize::new(0)),
            log: Log::start_system(),
        }
    }
}

impl Pager {
    pub fn open() -> Pager {
        recover().unwrap_or_else(|_| Pager::default())
    }

    pub fn enroll_in_epoch(&self) -> Epoch {
        loop {
            let idx = self.current_epoch.load(Ordering::SeqCst);
            let clean_idx = self.cleaned_epochs.load(Ordering::SeqCst);
            if idx - clean_idx >= N_EPOCHS {
                // need to spin for gc to complete
                assert_eq!(idx - clean_idx, N_EPOCHS);
                println!("epoch enroll spin 1");
                thread::sleep_ms(500);
                continue;
            }
            let atomic_header = self.gc_headers[idx % N_EPOCHS].clone();
            let header = atomic_header.load(Ordering::SeqCst);
            if ops::is_sealed(header as u32) {
                // need to spin for current_epoch to be bumped
                println!("epoch enroll spin 2");
                continue;
            }
            let header2 = ops::incr_writers(header as u32);
            let header3 = ops::bump_offset(header2, 1);
            let header4 = if ops::offset(header3) > 512 {
                ops::mk_sealed(header3)
            } else {
                header3
            };
            if header ==
               atomic_header.compare_and_swap(header, header4 as usize, Ordering::SeqCst) {
                if ops::is_sealed(header4) {
                    // we were the sealer, so we can move this along
                    self.current_epoch.fetch_add(1, Ordering::SeqCst);
                }
                return Epoch {
                    header: atomic_header,
                    gc_pids: self.gc_pids.clone(),
                    gc_pages: self.gc_pages.clone(),
                    current_epoch: self.current_epoch.clone(),
                    cleaned_epochs: self.cleaned_epochs.clone(),
                    free: self.free.clone(),
                };
            }
            println!("epoch enroll spin 3");
        }
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

    Ok(Pager::default())
}

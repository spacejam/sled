/// A lock-free pagecache which supports fragmented pages for dramatically
/// improving write throughput. Reads need to scatter-gather, so this is
/// built with the assumption that it will be run on something like an
/// SSD that can efficiently handle random reads.
use std::fmt::{self, Debug};
use std::ptr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use bincode::{Infinite, deserialize, serialize};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::*;

/// A tenant of a `PageCache` needs to provide a `Materializer` which
/// handles the processing of pages.
pub trait Materializer: Send + Sync {
    /// The "complete" page, returned to clients who want to retrieve the
    /// logical page state.
    type MaterializedPage;

    /// The "partial" page, written to log storage sequentially, and
    /// read in parallel from multiple locations on disk when serving
    /// a request to read the page.
    type PartialPage: Serialize + DeserializeOwned;

    /// The state returned by a call to `PageCache::recover`, as
    /// described by `Materializer::recover`
    type Recovery;

    /// Appends may include optional annotations which are used to feed
    /// recovery state.
    type Annotation: Serialize + DeserializeOwned;

    /// Used to generate the result of `get` requests on the `PageCache`
    fn materialize(&self, Vec<Self::PartialPage>) -> Self::MaterializedPage;

    /// Used to compress long chains of partial pages into a condensed form
    /// during compaction.
    fn consolidate(&self, Vec<Self::PartialPage>) -> Vec<Self::PartialPage>;

    /// Used to feed custom recovery information back to a higher-level abstraction
    /// during startup. For example, a B-Link tree must know what the current
    /// root node is before it can start serving requests.
    fn recover(&mut self, Self::Annotation) -> Option<Self::Recovery>;
}

/// A lock-free pagecache.
pub struct PageCache<L: Log, M>
    where M: Materializer + Sized,
          L: Log + Sized
{
    t: M,
    inner: Radix<Stack<CacheEntry<*const M::PartialPage>>>,
    max_id: AtomicUsize,
    free: Stack<PageID>,
    log: Box<L>,
}

unsafe impl<L: Log, M: Materializer> Send for PageCache<L, M> {}
unsafe impl<L: Log, M: Materializer> Sync for PageCache<L, M> {}

impl<L: Log, M: Materializer> Debug for PageCache<L, M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str(&*format!("PageCache {{ max: {:?} free: {:?} }}\n",
                              self.max_id.load(SeqCst),
                              self.free))
    }
}

impl<M> PageCache<LockFreeLog, M>
    where M: Materializer,
          M::PartialPage: Clone,
          M: DeserializeOwned,
          M: Serialize
{
    /// Instantiate a new `PageCache`.
    pub fn new(pm: M, config: Config) -> PageCache<LockFreeLog, M> {
        PageCache {
            t: pm,
            inner: Radix::default(),
            max_id: AtomicUsize::new(0),
            free: Stack::default(),
            log: Box::new(LockFreeLog::start_system(config)),
        }
    }

    /// Return the configuration used by the underlying system.
    pub fn config(&self) -> Config {
        self.log.config()
    }

    /// Read updates from the log, apply them to our pagecache.
    pub fn recover(&mut self, from: LogID) -> Option<M::Recovery> {
        use std::collections::HashMap;

        let mut last_heads: HashMap<PageID, LogID> = HashMap::new();
        let mut last_good_id = 0;
        let mut free_pids = vec![];
        let mut recovery = None;

        for (log_id, bytes) in self.log.iter_from(from) {
            if let Ok(append) = deserialize::<LoggedUpdate<M>>(&*bytes) {
                if let Some(annotation) = append.annotation {
                    let r = self.t.recover(annotation);
                    if r.is_some() {
                        recovery = r;
                    }
                }

                last_good_id = log_id + bytes.len() as LogID + HEADER_LEN as LogID;
                match append.update {
                    Update::Append(appends) => {
                        let stack = self.inner.get(append.pid).unwrap();

                        // last flush will be head
                        let last = last_heads.get(&append.pid);
                        let flush = match last {
                            Some(last) => CacheEntry::PartialFlush(log_id, *last),
                            None => CacheEntry::Flush(log_id),
                        };

                        last_heads.insert(append.pid, log_id);

                        unsafe {
                            (*stack).push(flush);
                        }
                    }
                    Update::Compact(appends) => {
                        // TODO GC previous stack
                        // TODO feed compacted page to recover?
                        last_heads.remove(&append.pid);
                        self.inner.del(append.pid);
                        let stack = raw(Stack::default());
                        self.inner.insert(append.pid, stack).unwrap();
                        let flush = CacheEntry::Flush(log_id);
                        unsafe { (*stack).push(flush) }
                    }
                    Update::Del => {
                        self.inner.del(append.pid);
                        free_pids.push(append.pid);
                    }
                    Update::Alloc => {
                        let stack = raw(Stack::default());
                        self.inner.insert(append.pid, stack).unwrap();
                        free_pids.retain(|&pid| pid != append.pid);
                        if self.max_id.load(SeqCst) < append.pid {
                            self.max_id.store(append.pid, SeqCst);
                        }
                    }
                }
            }
        }
        free_pids.sort();
        free_pids.reverse();
        for free_pid in free_pids {
            self.free.push(free_pid);
        }

        self.max_id.store(last_good_id as usize, SeqCst);

        recovery
    }

    /// Create a new page, trying to reuse old freed pages if possible
    /// to maximize underlying `Radix` pointer density.
    pub fn allocate(&self) -> (PageID, *const stack::Node<*const M::PartialPage>) {
        let pid = self.free.pop().unwrap_or_else(|| self.max_id.fetch_add(1, SeqCst));
        let stack = raw(Stack::default());
        self.inner.insert(pid, stack).unwrap();

        // write info to log
        let append: LoggedUpdate<M> = LoggedUpdate {
            pid: pid,
            update: Update::Alloc,
            annotation: None,
        };
        let bytes = serialize(&append, Infinite).unwrap();
        self.log.write(bytes);

        (pid, ptr::null())
    }

    /// Free a particular page.
    pub fn free(&self, pid: PageID) {
        // TODO epoch-based gc for reusing pid & freeing stack
        // TODO iter through flushed pages, punch hole
        let stack_ptr = self.inner.del(pid);

        // write info to log
        let append: LoggedUpdate<M> = LoggedUpdate {
            pid: pid,
            update: Update::Del,
            annotation: None,
        };
        let bytes = serialize(&append, Infinite).unwrap();
        self.log.write(bytes);

        // add pid to free stack to reduce fragmentation over time
        self.free.push(pid);

        unsafe {
            let ptrs = (*stack_ptr).pop_all();
            for ce in ptrs {
                match ce {
                    CacheEntry::Resident(ptr) => {
                        Box::from_raw(ptr as *mut M::PartialPage);
                    }
                    _ => (),
                }
            }
        }
    }

    /// Try to retrieve a page by its logical ID.
    pub fn get(&self,
               pid: PageID)
               -> Option<(M::MaterializedPage, *const stack::Node<*const M::PartialPage>)> {
        let stack_ptr = self.inner.get(pid);
        if stack_ptr.is_none() {
            return None;
        }

        let stack_ptr = stack_ptr.unwrap();

        let mut head = unsafe { (*stack_ptr).head() };

        let stack_iter = StackIter::from_ptr(head);

        let cache_entries: Vec<CacheEntry<*const M::PartialPage>> =
            unsafe { stack_iter.map(|ptr| (*ptr).clone()).collect() };

        let mut partial_pages = vec![];
        for ce in &cache_entries {
            match *ce {
                CacheEntry::Resident(ptr) => {
                    let pp = unsafe { (*ptr).clone() };
                    partial_pages.push(pp);
                }
                CacheEntry::Flush(lid) => {}
                CacheEntry::PartialFlush(lid, _) => {}
            }
        }
        partial_pages.reverse();
        let partial_pages = partial_pages;

        if partial_pages.len() > self.config().get_page_consolidation_threshold() {
            let consolidated = self.t.consolidate(partial_pages.clone());

            let node = node_from_frag_vec(consolidated.clone());

            // log consolidation to disk
            let append = LoggedUpdate {
                pid: pid,
                update: Update::Compact(consolidated),
                annotation: None, // TODO preserve root hoist
            };
            let bytes = serialize(&append, Infinite).unwrap();
            let log_reservation = self.log.reserve(bytes);

            let ret = unsafe { (*stack_ptr).cas(head, node) };

            if let Ok(new) = ret {
                // consolidation succeeded!
                log_reservation.complete();
                head = new;
                // TODO GC old stack (head)
            } else {
                log_reservation.abort();
            }
        }

        let materialized = self.t.materialize(partial_pages);

        Some((materialized, head))
    }

    /// Try to atomically append a `Materializer::PartialPage` to the page.
    pub fn append(&self,
                  pid: PageID,
                  old: *const stack::Node<*const M::PartialPage>,
                  new: M::PartialPage,
                  annotation: Option<M::Annotation>)
                  -> Result<*const stack::Node<*const M::PartialPage>,
                            *const stack::Node<*const M::PartialPage>> {
        let append = LoggedUpdate {
            pid: pid,
            update: Update::Append(vec![new.clone()]),
        };
        let bytes = serialize(&append, Infinite).unwrap();
        let log_reservation = self.log.reserve(bytes);
        let log_offset = log_reservation.log_id();

        let stack_ptr = self.inner.get(pid).unwrap();
        let result = unsafe { (*stack_ptr).cap(old, raw(new)) };

        if let Err(_ptr) = result {
            // TODO GC
            log_reservation.abort();
        } else {
            log_reservation.complete();
        }
        result
    }

    // TODO
    fn flush(&self, pid: PageID) {
        // iterate
        // flush all unflushed
        //   reserve flush
        //   append flush at LogID
        //   complete or abort
        // spin on CAS'ing stack
        // GC old flushes
    }

    // TODO
    fn page_out(&self, pid: PageID) {
        //   LRU selection algo
        self.flush(pid);
        // squish stack
        // CAS stack
    }

    // TODO
    fn squish_stack(&self, pid: PageID) {
        //   filter frags that have been flushed
        //   CAS stack with squished stack
    }

    // TODO
    fn page_in(&self, pid: PageID) {
        //   read stack
        //   scatter/gather reads for flushes
        //   spin on expand stack
    }
}

// TODO
// !. don't touch current write side
// 1. on recovery, just populate flushes
// appending, consolidating
//   append with mem-only address
//   put pid in unbounded flush FIFO (all flushed on shutdown, drop, etc...)
//
// async flushing
//   traverse flush FIFO
//   flush (don't page-out)

/// Points to either a memory location or a disk location to page-in data from.
#[derive(Clone, Debug, PartialEq)]
enum CacheEntry<A> {
    Resident(A),
    Flush(LogID),
    PartialFlush(LogID, LogID),
}

/// `LoggedUpdate` is for writing blocks of `Update`'s to disk
/// sequentially, to reduce IO during page reads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct LoggedUpdate<A>
    where A: Materializer
{
    pid: PageID,
    update: Update<A>,
    annotation: Option<A::Annotation>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Update<A>
    where A: Materializer
{
    Append(Vec<A::PartialPage>),
    Compact(Vec<A::PartialPage>),
    Del,
    Alloc,
}

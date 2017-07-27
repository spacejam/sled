use std::fmt::{self, Debug};
use std::ptr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use bincode::{Infinite, deserialize, serialize};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::*;

const MAX_FRAG_LEN: usize = 2;

pub trait PageMaterializer: Send + Sync {
    type MaterializedPage;
    type PartialPage: Serialize + DeserializeOwned;

    fn materialize(&self, Vec<Self::PartialPage>) -> Self::MaterializedPage;
    fn consolidate(&self, Vec<Self::PartialPage>) -> Vec<Self::PartialPage>;
}

pub struct PageCache<L: Log, PM>
    where PM: PageMaterializer + Sized,
          L: Log + Sized
{
    t: PM,
    inner: Radix<stack::Stack<*const PM::PartialPage>>,
    max_id: AtomicUsize,
    free: Stack<PageID>,
    log: Box<L>,
}

unsafe impl<L: Log, PM: PageMaterializer> Send for PageCache<L, PM> {}
unsafe impl<L: Log, PM: PageMaterializer> Sync for PageCache<L, PM> {}

impl<L: Log, PM: PageMaterializer> Debug for PageCache<L, PM> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str(&*format!("PageCache {{ max: {:?} free: {:?} }}\n",
                              self.max_id.load(SeqCst),
                              self.free))
    }
}

impl<PM> PageCache<LockFreeLog, PM>
    where PM: PageMaterializer,
          PM::PartialPage: Clone
{
    pub fn new(pm: PM, path: Option<String>) -> PageCache<LockFreeLog, PM> {
        let pc = PageCache {
            t: pm,
            inner: Radix::default(),
            max_id: AtomicUsize::new(0),
            free: Stack::default(),
            log: Box::new(LockFreeLog::start_system(path)),
        };

        // Recover from beginning of log. In the future when we
        // have PageCache snapshotting, we will want to first
        // load the latest snapshot, then use a later LogID
        // to recover from in the log.
        pc.recover_from_log(0);

        pc
    }

    /// Read updates from the log, apply them to our pagecache.
    fn recover_from_log(&self, from: LogID) {
        let mut last_good_id = 0;
        let mut free_pids = vec![];
        for (log_id, bytes) in self.log.iter_from(from) {
            if let Ok(append) = deserialize::<LoggedUpdate<PM::PartialPage>>(&*bytes) {
                last_good_id = log_id + bytes.len() as LogID + HEADER_LEN as LogID;
                match append.update {
                    Update::Append(appends) => {
                        let stack = self.inner.get(append.pid).unwrap();
                        for append in appends {
                            unsafe {
                                (*stack).push(raw(append));
                            }
                        }
                    }
                    Update::Compact(appends) => {
                        // TODO GC previous stack
                        let _prev = self.inner.del(append.pid);
                        let stack = raw(Stack::default());
                        self.inner.insert(append.pid, stack).unwrap();

                        for append in appends {
                            unsafe {
                                (*stack).push(raw(append));
                            }
                        }
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
    }

    pub fn allocate(&self) -> (PageID, *const stack::Node<*const PM::PartialPage>) {
        let pid = self.free.pop().unwrap_or_else(|| self.max_id.fetch_add(1, SeqCst));
        let stack = raw(Stack::default());
        self.inner.insert(pid, stack).unwrap();

        // write info to log
        let append: LoggedUpdate<PM::PartialPage> = LoggedUpdate {
            pid: pid,
            update: Update::Alloc,
        };
        let bytes = serialize(&append, Infinite).unwrap();
        self.log.write(bytes);

        (pid, ptr::null())
    }

    pub fn free(&self, pid: PageID) {
        // TODO epoch-based gc for reusing pid & freeing stack
        let stack_ptr = self.inner.del(pid);

        // write info to log
        let append: LoggedUpdate<PM::PartialPage> = LoggedUpdate {
            pid: pid,
            update: Update::Del,
        };
        let bytes = serialize(&append, Infinite).unwrap();
        self.log.write(bytes);

        // add pid to free stack to reduce fragmentation over time
        self.free.push(pid);

        unsafe {
            let ptrs = (*stack_ptr).pop_all();
            for ptr in ptrs {
                Box::from_raw(ptr as *mut Frag);
            }
        }
    }

    pub fn get(&self,
               pid: PageID)
               -> Option<(PM::MaterializedPage, *const stack::Node<*const PM::PartialPage>)> {
        let stack_ptr = self.inner.get(pid);
        if stack_ptr.is_none() {
            return None;
        }

        let stack_ptr = stack_ptr.unwrap();

        let mut head = unsafe { (*stack_ptr).head() };

        let stack_iter = StackIter::from_ptr(head);

        let mut partial_pages: Vec<PM::PartialPage> =
            unsafe { stack_iter.map(|ptr| (**ptr).clone()).collect() };
        partial_pages.reverse();
        let partial_pages = partial_pages;

        if partial_pages.len() > MAX_FRAG_LEN {
            let consolidated = self.t.consolidate(partial_pages.clone());

            let node = node_from_frag_vec(consolidated.clone());

            // log consolidation to disk
            let append = LoggedUpdate {
                pid: pid,
                update: Update::Compact(consolidated),
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

    pub fn append(&self,
                  pid: PageID,
                  old: *const stack::Node<*const PM::PartialPage>,
                  new: PM::PartialPage)
                  -> Result<*const stack::Node<*const PM::PartialPage>,
                            *const stack::Node<*const PM::PartialPage>> {
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
            log_reservation.abort();
        } else {
            log_reservation.complete();
        }
        self.log.make_stable(log_offset);

        // TODO GC
        result
    }
}

/// `LoggedUpdate` is for writing blocks of `Update`'s to disk
/// sequentially, to reduce IO during page reads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct LoggedUpdate<T> {
    pid: PageID,
    update: Update<T>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Update<T> {
    Append(Vec<T>),
    Compact(Vec<T>),
    Del,
    Alloc,
}

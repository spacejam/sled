use std::fmt::{self, Debug};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::ptr;

use bincode::{serialize, Infinite};
use serde::{Serialize, Deserialize};

use super::*;

const MAX_FRAG_LEN: usize = 7;

pub trait PageMaterializer {
    type MaterializedPage;
    type PartialPage: Serialize + Deserialize<'static>;

    fn materialize(&self, Vec<Self::PartialPage>) -> Self::MaterializedPage;
    fn consolidate(&self, Vec<Self::PartialPage>) -> Vec<Self::PartialPage>;
}

pub struct PageCache<PM>
    where PM: PageMaterializer
{
    t: PM,
    inner: Radix<stack::Stack<*const PM::PartialPage>>,
    max_id: AtomicUsize,
    free: Stack<PageID>,
    log: Option<Arc<Log>>,
}

impl<PM: PageMaterializer> Debug for PageCache<PM> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str(&*format!("PageCache {{ max: {:?} free: {:?} }}\n",
                              self.max_id.load(SeqCst),
                              self.free))
    }
}

impl<PM> PageCache<PM>
    where PM: PageMaterializer,
          PM::PartialPage: Clone
{
    pub fn new(pm: PM, log: Option<Arc<Log>>) -> PageCache<PM> {
        PageCache {
            t: pm,
            inner: Radix::default(),
            max_id: AtomicUsize::new(0),
            free: Stack::default(),
            log: log,
        }
    }

    pub fn allocate(&self) -> (PageID, *const stack::Node<*const PM::PartialPage>) {
        let pid = self.free.pop().unwrap_or_else(|| self.max_id.fetch_add(1, SeqCst));
        let stack = raw(Stack::default());
        self.inner.insert(pid, stack).unwrap();
        (pid, ptr::null())
    }

    pub fn free(&self, pid: PageID) {
        // TODO epoch-based gc for reusing pid & freeing stack
        if pid == 0 {
            panic!("freeing zero, which should always be left-most on a level");
        }
        let stack_ptr = self.inner.del(pid);
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

            let node = node_from_frag_vec(consolidated);

            let ret = unsafe { (*stack_ptr).cas(head, node) };

            if let Ok(new) = ret {
                // consolidation succeeded!
                head = new;
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
        let bytes = serialize(&new, Infinite).unwrap();
        let log = self.log.clone();
        let reservation = log.map(|l| l.reserve(bytes));

        let stack_ptr = self.inner.get(pid).unwrap();
        let res = unsafe { (*stack_ptr).cap(old, raw(new)) };

        if let Err(_ptr) = res {
            reservation.map(|r| r.abort());
        } else {
            reservation.map(|r| r.complete());
        }

        // TODO GC
        res
    }
}

/// `FragSet` is for writing blocks of `Frag`'s to disk
/// sequentially, to reduce IO during page reads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FragSet {
    pid: PageID,
    frags: Vec<Frag>,
    next: Option<LogID>,
}

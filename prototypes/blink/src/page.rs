/// Traversing pages, splits, merges

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use super::*;

pub enum SeekRes {
    ShortCircuitSome(Value),
    Node(tree::Node),
    ShortCircuitNone,
    Split(PageID),
    Merge, // TODO
}

#[derive(Debug, Clone)]
pub struct FragView {
    pub pid: PageID,
    pub head: Raw,
    pub stack: *const Stack<*const Frag>,
    pub depth: usize,
}

impl FragView {
    pub fn stack_len(&self) -> usize {
        unsafe { (*self.stack).len() }
    }

    pub fn stack_iter(&self) -> stack::StackIter<*const Frag> {
        unsafe { (*self.stack).into_iter() }
    }

    pub fn consolidate(&self) -> tree::Node {
        // if let Data::Index(ref mut ptrs) = parent.data {
        // let search =
        // ptrs.binary_search_by(|&(ref k, ref _v)| Bound::Inc(k.clone()).cmp(&lhs.lo));
        // if let Ok(idx) = search {
        // ptrs.remove(idx);
        // }
        // ptrs.push((lhs.lo.inner().unwrap(), lhs.id));
        // ptrs.push((rhs.lo.inner().unwrap(), rhs.id));
        // ptrs.sort_by(|a, b| a.0.cmp(&b.0));
        // }
        //
        unimplemented!()
    }

    pub fn should_split(&self) -> bool {
        unimplemented!()
    }

    /// returns child_split -> lhs, rhs, parent_frag
    pub fn split(&self,
                 new_id: PageID)
                 -> (*const Stack<*const Frag>, *const Stack<*const Frag>, ParentSplit) {
        // let parent_split = Frag::ParentSplit {
        // at: 1,
        // to: 1,
        // from: 1,
        // hi: 1,
        // };
        //
        unimplemented!()
    }
}

pub type Seek = (SeekRes, FragView);

pub struct ParentSplit {
    pub at: Vec<u8>,
    pub to: PageID,
    pub from: PageID,
    pub hi: Vec<u8>, // lets us stop traversing frags
}

pub enum Frag {
    Set(Key, Value),
    Del(Key),
    Base(tree::Node),
    ChildSplit {
        at: Vec<u8>,
        to: PageID, // TODO should this be physical?
    },
    ParentSplit(ParentSplit),
}

pub struct Pages {
    pub inner: Radix<Stack<*const Frag>>,
    max_id: AtomicUsize,
    free: Stack<PageID>,
}

impl Default for Pages {
    fn default() -> Pages {
        Pages {
            inner: Radix::default(),
            max_id: AtomicUsize::new(0),
            free: Stack::default(),
        }
    }
}

impl Pages {
    pub fn seek(&self, pid: PageID, key: Key) -> Seek {
        use self::Frag::*;
        use self::ParentSplit as PS;

        let stack_ptr = self.inner.get(pid).unwrap();

        // welcome to the danger zone
        unsafe {
            let (head, iter) = (*stack_ptr).iter_at_head();
            let mut depth = 0;
            let mut ret = None;

            // traverse stack until we find relevant frag, or base
            for frag_ptr in iter {
                depth += 1;
                if let Some(result) = match **frag_ptr {
                    // if we've traversed to a base node, we can return it
                    Base(ref node) => Some(SeekRes::Node(node.clone())),

                    // if we encounter a set or del for our key,
                    // we can short-circuit the request
                    Set(ref k, ref v) if k == &key => Some(SeekRes::ShortCircuitSome(v.clone())),
                    Del(ref k) if k == &key => Some(SeekRes::ShortCircuitNone),

                    // if we encounter a relevant split, pass it to the caller
                    ChildSplit { ref at, ref to } if at <= &key => Some(SeekRes::Split(*to)),
                    ParentSplit(PS { ref at, ref to, ref from, ref hi }) if at <= &key &&
                                                                            hi > &key => {
                        Some(SeekRes::Split(*to))
                    }
                    _ => None,
                } {
                    ret = Some(result);
                    break;
                }
            }
            let meta = FragView {
                pid: pid,
                head: head,
                stack: stack_ptr,
                depth: depth,
            };
            (ret.unwrap(), meta)
        }
    }

    pub fn cap(&self, pid: PageID, old: Raw, new: *const Frag) -> Result<(), Raw> {
        let stack_ptr = self.inner.get(pid).unwrap();
        unsafe { (*stack_ptr).cap(old, new) }
    }

    pub fn allocate(&self) -> PageID {
        // TODO free list/epoch gc
        let id = self.free.pop().unwrap_or_else(|| self.max_id.fetch_add(1, SeqCst));
        id
    }

    pub fn free(&self, pid: PageID) {
        // TODO epoch
        self.free.push(pid);
        let stack_ptr = self.inner.get(pid).unwrap();
        unsafe { (*stack_ptr).pop_all() };
    }

    pub fn insert(&self, pid: PageID, frag: *const Frag) -> Result<(), ()> {
        let stack = Stack::default();
        stack.push(frag);
        self.inner.insert(pid, raw(stack)).map(|_| ()).map_err(|_| ())
    }
}

// consolidation:
//  CAS head of stack with consolidated page
//  add head of stack to GC epoch if successful

// split:
//  new page, consolidated from right side
//  split -> consolidated left page
//  insert dangling right side into radix
//  CAS split -> left page into radix
//      if failed, insta-free right side & pid, don't retry
//  CAS update onto the parent index
//

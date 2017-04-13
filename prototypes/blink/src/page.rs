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

pub struct SeekMeta {
    pub pid: PageID,
    pub head: Raw,
    pub depth: usize,
}

pub type Seek = (SeekRes, SeekMeta);

pub enum Frag {
    Set(Key, Value),
    Del(Key),
    Base(tree::Node),
    ChildSplit {
        at: Vec<u8>,
        to: PageID, // TODO should this be physical?
    },
    ParentSplit {
        at: Vec<u8>,
        to: PageID,
        from: PageID,
        hi: Vec<u8>, // lets us stop traversing frags
    },
}

pub struct Pages {
    inner: Radix<Stack<*const Frag>>,
    max_id: AtomicUsize,
}

impl Default for Pages {
    fn default() -> Pages {
        Pages {
            inner: Radix::default(),
            max_id: AtomicUsize::new(0),
        }
    }
}

impl Pages {
    pub fn seek(&self, pid: PageID, key: Key) -> Seek {
        use self::Frag::*;

        let stack_ptr = self.inner.get(pid).unwrap();

        // welcome to the danger zone
        unsafe {
            let (head, iter) = (*stack_ptr).iter_at_head();
            let mut depth = 0;
            let mut ret = None;
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
                    ParentSplit { ref at, ref to, ref from, ref hi } if at <= &key && hi > &key => {
                        Some(SeekRes::Split(*to))
                    }
                    _ => None,
                } {
                    ret = Some(result);
                    break;
                }
            }
            let meta = SeekMeta {
                pid: pid,
                head: head,
                depth: depth,
            };
            (ret.unwrap(), meta)
        }
    }

    pub fn cap(&self, pid: PageID, old: Raw, new: *const Frag) -> Result<(), Raw> {
        let stack_ptr = self.inner.get(pid).unwrap();
        unsafe { (*stack_ptr).cap(old, new) }
    }

    pub fn allocate(&self) -> (Raw, PageID) {
        // TODO free list/epoch gc
        let id = self.max_id.fetch_add(1, SeqCst);
        let stack = Stack::default();
        let stack_ptr = raw(stack);
        let head = stack.head();
        self.inner.insert(id, stack_ptr).unwrap();
        (head, id)
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

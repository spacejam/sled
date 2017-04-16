/// Traversing pages, splits, merges

use std::collections::HashMap;
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
        use self::Frag::*;
        use self::ParentSplit as PS;

        let mut frags: Vec<Frag> =
            unsafe { self.stack_iter().map(|ptr| (**ptr).clone()).collect() };

        let mut base = frags.pop().unwrap().base().unwrap();
        println!("before, page is now {:?}", base.data);

        frags.reverse();

        for frag in frags {
            match frag {
                Set(ref k, ref v) => {
                    println!("trying to add to leaf");
                    base.set_leaf(k.clone(), v.clone()).unwrap();
                }
                ParentSplit(PS { at, to, from, hi }) => {
                    println!("trying to add index leaf");
                    base.parent_split(at, to, from, hi).unwrap();
                }
                Del(ref k) => {
                    println!("trying do remove from leaf");
                    base.del_leaf(k);
                }
                Base(_) => panic!("encountered base page in middle of chain"),
                ChildSplit { at, to } => {}
            }
        }

        println!("after, page is now {:?}", base.data);
        base
    }

    pub fn should_split(&self) -> bool {
        let base = self.consolidate();
        base.should_split()
    }

    /// returns child_split -> lhs, rhs, parent_frag
    pub fn split(&self,
                 new_id: PageID)
                 -> (*const Stack<*const Frag>, *const Stack<*const Frag>, ParentSplit) {
        let base = self.consolidate();
        let (lhs, rhs) = base.split(new_id.clone());
        let parent_split = ParentSplit {
            at: rhs.lo.clone(),
            to: new_id.clone(),
            from: lhs.id,
            hi: rhs.hi.clone(),
        };
        let child_split = Frag::ChildSplit {
            at: rhs.lo.inner().unwrap(),
            to: new_id,
        };
        let l = raw(Frag::Base(lhs));
        let r = raw(Frag::Base(rhs));
        let rs = Stack::default();
        rs.push(r);
        let ls = Stack::default();
        ls.push(l);
        ls.push(raw(child_split));

        (raw(ls), raw(rs), parent_split)
    }
}

pub type Seek = (SeekRes, FragView);

#[derive(Clone, Debug)]
pub struct ParentSplit {
    pub at: Bound,
    pub to: PageID,
    pub from: PageID,
    pub hi: Bound, // lets us stop traversing frags
}

#[derive(Clone, Debug)]
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

impl Frag {
    fn base(&self) -> Option<tree::Node> {
        match *self {
            Frag::Base(ref base) => Some(base.clone()),
            _ => None,
        }
    }
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
                    ParentSplit(PS { ref at, ref to, ref from, ref hi })
                        if at <= &Bound::Inc(key.clone()) &&
                           hi > &Bound::Inc(key.clone()) => Some(SeekRes::Split(*to)),
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

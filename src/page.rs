/// Traversing pages, splits, merges

use std::fmt::{self, Debug};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use super::*;

const MAX_FRAG_LEN: usize = 7;

pub struct Pages {
    pub inner: Radix<Page>,
    max_id: AtomicUsize,
    free: Stack<PageID>,
    log: Option<Arc<Log>>,
}

#[derive(Clone, Debug, PartialEq, RustcDecodable, RustcEncodable)]
pub struct ParentSplit {
    pub at: Bound,
    pub to: PageID,
}

#[derive(Clone, Debug, PartialEq, RustcDecodable, RustcEncodable)]
pub struct ChildSplit {
    pub at: Bound,
    pub to: PageID,
}

#[derive(Clone, Debug, PartialEq, RustcDecodable, RustcEncodable)]
pub enum Frag {
    Set(Key, Value),
    Del(Key),
    Base(tree::Node),
    ChildSplit(ChildSplit),
    ParentSplit(ParentSplit),
}

// TODO
// Merged
// LeftMerge(head: Raw, rhs: PageID, hi: Bound)
// ParentMerge(lhs: PageID, rhs: PageID)
// TxBegin(TxID), // in-mem
// TxCommit(TxID), // in-mem
// TxAbort(TxID), // in-mem
// Load, // should this be a swap operation on the data pointer?
// Flush {
//    annotation: Annotation,
//    highest_lsn: TxID,
// }, // in-mem
// PartialSwap(LogID), /* indicates part of page has been swapped out,
//                     * shows where to find it */

#[derive(Clone)]
pub struct PageView {
    head: Raw,
    page: *const Page,
    pub node: tree::Node,
    log: Option<Arc<Log>>,
}

impl PageView {
    pub fn cap(&mut self, frag: *const page::Frag) -> Result<Raw, Raw> {
        unsafe {
            let bytes = ops::to_binary(&*frag);
            let res = self.clone().log.map(|l| l.reserve(bytes.len()));
            let ret = (*self.page).cap(self.head, frag);
            if let Ok(new) = ret {
                res.map(|r| r.complete(&bytes));
                self.head = new;
                match *frag {
                    Frag::Set(ref k, ref v) => {
                        self.node.set_leaf(k.clone(), v.clone());
                    }
                    Frag::Del(ref k) => {
                        self.node.del_leaf(k);
                    }
                    Frag::ChildSplit(ref cs) => {
                        self.node.child_split(cs.clone());
                    }
                    Frag::ParentSplit(ref ps) => {
                        self.node.parent_split(ps.clone());
                    }
                    Frag::Base(_) => {
                        panic!("capped new Base in middle of frags stack");
                    }
                }
            } else {
                res.map(|r| r.abort());
            }
            ret
        }
    }

    pub fn cas(&mut self, new: Raw) -> Result<Raw, Raw> {
        // TODO add separated part to epoch
        unsafe {
            let ret = (*self.page).cas(self.head, new);
            if let Ok(new) = ret {
                self.head = new;
            }
            ret
        }
    }

    pub fn should_split(&self) -> bool {
        self.node.should_split()
    }

    pub fn maybe_consolidate(&mut self) {
        // try to consolidate if necessary
        let node = node_from_frag_vec(vec![Frag::Base(self.node.clone())]);

        let _ = self.cas(node);
    }

    /// returns child_split -> lhs, rhs, parent_split
    pub fn split(&self, new_id: PageID) -> (Raw, Frag, ParentSplit) {
        // print!("split(new_id: {}) (", new_id);
        let (lhs, rhs) = self.node.split(new_id.clone());

        let parent_split = ParentSplit {
            at: rhs.lo.clone(),
            to: new_id.clone(),
        };

        assert_eq!(lhs.hi.inner(), rhs.lo.inner());
        assert_eq!(lhs.hi.inner(), parent_split.at.inner());

        let child_split = Frag::ChildSplit(ChildSplit {
            at: rhs.lo.clone(),
            to: new_id,
        });

        let r = Frag::Base(rhs);
        let l = node_from_frag_vec(vec![child_split, Frag::Base(lhs)]);

        (l, r, parent_split)
    }
}

impl Frag {
    fn base(&self) -> Option<tree::Node> {
        match *self {
            Frag::Base(ref base) => Some(base.clone()),
            _ => None,
        }
    }
}

impl Default for Pages {
    fn default() -> Pages {
        Pages {
            inner: Radix::default(),
            max_id: AtomicUsize::new(0),
            free: Stack::default(),
            log: None,
        }
    }
}

impl Debug for Pages {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str(&*format!("Pages {{ max: {:?} free: {:?} }}\n",
                              self.max_id(),
                              self.free))
    }
}

impl Pages {
    pub fn get(&self, pid: PageID) -> PageView {
        let stack_ptr = self.inner.get(pid).unwrap();

        let head = unsafe { (*stack_ptr).head() };

        let stack_iter = StackIter::from_ptr(head);

        let (base, frags_len) = stack_iter.consolidated();

        let mut res = PageView {
            head: head,
            page: stack_ptr,
            node: base,
            log: self.log.clone(),
        };

        if frags_len > MAX_FRAG_LEN {
            res.maybe_consolidate();
        }

        res
    }

    pub fn cap(&self, pid: PageID, old: Raw, new: *const Frag) -> Result<Raw, Raw> {
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
        // let stack_ptr = self.inner.get(pid).unwrap();
        if pid == 0 {
            panic!("freeing zero");
        }
        let _stack_ptr = self.inner.del(pid);
        self.free.push(pid);
        // unsafe { (*stack_ptr).pop_all() };
    }

    pub fn insert(&self, pid: PageID, head: page::Frag) -> Result<(), ()> {
        let stack = Stack::from_vec(vec![raw(head)]);
        self.inner.insert(pid, raw(stack)).map(|_| ()).map_err(|_| ())
    }

    pub fn max_id(&self) -> PageID {
        self.max_id.load(SeqCst)
    }
}

impl<'a> StackIter<'a, *const Frag> {
    pub fn consolidated(self) -> (tree::Node, usize) {
        use self::Frag::*;

        let mut frags: Vec<Frag> = unsafe { self.map(|ptr| (**ptr).clone()).collect() };

        let mut base = frags.pop().unwrap().base().unwrap();

        let frags_len = frags.len();

        // we reverse our frags to start with the base node, and
        // apply frags in chronological order.
        for frag in frags.into_iter().rev() {
            match frag {
                Set(ref k, ref v) => {
                    if Bound::Inc(k.clone()) < base.hi {
                        base.set_leaf(k.clone(), v.clone());
                    } else {
                        panic!("tried to consolidate set at key <= hi")
                    }
                }
                ChildSplit(child_split) => {
                    base.child_split(child_split);
                }
                ParentSplit(parent_split) => {
                    base.parent_split(parent_split);
                }
                Del(ref k) => {
                    if Bound::Inc(k.clone()) < base.hi {
                        base.del_leaf(k);
                    } else {
                        panic!("tried to consolidate del at key <= hi")
                    }
                }
                Base(_) => panic!("encountered base page in middle of chain"),
            }
        }

        (base, frags_len)
    }
}

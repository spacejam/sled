/// Traversing pages, splits, merges

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use super::*;

const MAX_FRAG_LEN: usize = 7;

pub struct Pages {
    pub inner: Radix<Stack<*const Frag>>,
    max_id: AtomicUsize,
    free: Stack<PageID>,
}

#[derive(Clone, Debug)]
pub struct ParentSplit {
    pub at: Bound,
    pub to: PageID,
}

#[derive(Clone, Debug)]
pub struct LeftMerge {
    pub head: Raw,
    pub rhs: PageID,
    pub hi: Bound,
}

#[derive(Clone, Debug)]
pub struct ParentMerge {
    lhs: PageID,
    rhs: PageID,
}

#[derive(Clone, Debug)]
pub enum Frag {
    Set(Key, Value),
    Del(Key),
    Base(tree::Node),
    ParentSplit(ParentSplit),
    Merged,
    LeftMerge(LeftMerge),
    ParentMerge(ParentMerge),
}

#[derive(Debug, Clone)]
pub struct Frags {
    head: Raw,
    stack: *const Stack<*const Frag>,
    pub node: tree::Node,
}

impl Frags {
    pub fn cap(&mut self, frag: *const page::Frag) -> Result<Raw, Raw> {
        unsafe {
            let ret = (*self.stack).cap(self.head, frag);
            if let Ok(new) = ret {
                self.head = new;
                match *frag {
                    Frag::Set(ref k, ref v) => {
                        self.node.set_leaf(k.clone(), v.clone());
                    }
                    Frag::Del(ref k) => {
                        self.node.del_leaf(k);
                    }
                    Frag::ParentSplit(ref ps) => {
                        self.node.parent_split(ps.clone());
                    }
                    Frag::Base(_) => {
                        panic!("capped new Base in middle of frags stack");
                    }
                    Frag::Merged => unimplemented!(),
                    Frag::LeftMerge(ref lm) => unimplemented!(),
                    Frag::ParentMerge(ref pm) => unimplemented!(),
                }
            }
            ret
        }
    }

    pub fn cas(&mut self, new: Raw) -> Result<Raw, Raw> {
        // TODO add separated part to epoch
        unsafe {
            let ret = (*self.stack).cas(self.head, new);
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

        let res = self.cas(node);
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

        let r = Frag::Base(rhs);
        let l = node_from_frag_vec(vec![Frag::Base(lhs)]);

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
        }
    }
}

impl Debug for Pages {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str(&*format!("Pages {{ max: {:?} free: {:?} }}\n",
                              self.max_id(),
                              self.free));
        Ok(())
    }
}

impl Pages {
    pub fn consolidate_from_iter(stack_iter: StackIter<*const Frag>) -> (tree::Node, usize) {
        use self::Frag::*;

        let mut frags: Vec<Frag> = unsafe { stack_iter.map(|ptr| (**ptr).clone()).collect() };

        if frags.is_empty() {
            println!("frags: {:?}", frags);
        }
        let mut base = frags.pop().unwrap().base().unwrap();
        // println!("before, page is now {:?}", base.data);

        frags.reverse();

        let frags_len = frags.len();

        for frag in frags {
            // println!("\t{:?}", frag);
            match frag {
                Set(ref k, ref v) => {
                    if Bound::Inc(k.clone()) < base.hi {
                        base.set_leaf(k.clone(), v.clone()).unwrap();
                    } else {
                        panic!("tried to consolidate set at key <= hi")
                    }
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
                Merge => {}
            }
        }

        (base, frags_len)
    }

    pub fn get(&self, pid: PageID) -> Frags {
        let stack_ptr = self.inner.get(pid).unwrap();

        let (head, stack_iter) = unsafe { (*stack_ptr).iter_at_head() };

        let (base, frags_len) = Pages::consolidate_from_iter(stack_iter);

        let mut res = Frags {
            head: head,
            stack: stack_ptr,
            node: base,
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
        let stack_ptr = self.inner.del(pid);
        self.free.push(pid);
        // unsafe { (*stack_ptr).pop_all() };
    }

    pub fn insert(&self, pid: PageID, head: page::Frag) -> Result<(), ()> {
        let mut stack = Stack::from_vec(vec![raw(head)]);
        self.inner.insert(pid, raw(stack)).map(|_| ()).map_err(|_| ())
    }

    pub fn max_id(&self) -> PageID {
        self.max_id.load(SeqCst)
    }
}

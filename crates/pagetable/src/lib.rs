/// A simple wait-free, grow-only pagetable, assumes a dense keyspace.
extern crate sled_sync;

use std::sync::atomic::Ordering::SeqCst;

use sled_sync::{
    debug_delay, pin, unprotected, Atomic, Guard, Owned, Shared,
};

const FANFACTOR: usize = 18;
const FANOUT: usize = 1 << FANFACTOR;
const FAN_MASK: usize = FANOUT - 1;

pub type PageId = usize;

macro_rules! rep_no_copy {
    ($e:expr; $n:expr) => {{
        let mut v = Vec::with_capacity($n);

        for _ in 0..$n {
            v.push($e);
        }

        v
    }};
}

#[inline(always)]
fn split_fanout(i: usize) -> (usize, usize) {
    // right shift 32 on 32-bit pointer systems panics
    #[cfg(target_pointer_width = "64")]
    assert!(i <= 1 << (FANFACTOR * 2));

    let left = i >> FANFACTOR;
    let right = i & FAN_MASK;
    (left, right)
}

struct Node1<T: Send + 'static> {
    children: Vec<Atomic<Node2<T>>>,
}

struct Node2<T: Send + 'static> {
    children: Vec<Atomic<T>>,
}

impl<T: Send + 'static> Default for Node1<T> {
    fn default() -> Node1<T> {
        let children = rep_no_copy!(Atomic::null(); FANOUT);
        Node1 { children: children }
    }
}

impl<T: Send + 'static> Default for Node2<T> {
    fn default() -> Node2<T> {
        let children = rep_no_copy!(Atomic::null(); FANOUT);
        Node2 { children: children }
    }
}

/// A simple lock-free radix tree.
pub struct PageTable<T>
where
    T: 'static + Send + Sync,
{
    head: Atomic<Node1<T>>,
}

impl<T> Default for PageTable<T>
where
    T: 'static + Send + Sync,
{
    fn default() -> PageTable<T> {
        let head = Owned::new(Node1::default());
        PageTable {
            head: Atomic::from(head),
        }
    }
}

impl<T> PageTable<T>
where
    T: 'static + Send + Sync,
{
    /// Atomically swap the previous value in a tree with a new one.
    pub fn swap<'g>(
        &self,
        pid: PageId,
        new: Shared<'g, T>,
        guard: &'g Guard,
    ) -> Shared<'g, T> {
        let tip = traverse(self.head.load(SeqCst, guard), pid, guard);
        debug_delay();
        tip.swap(new, SeqCst, guard)
    }

    /// Compare and swap an old value to a new one.
    pub fn cas<'g>(
        &self,
        pid: PageId,
        old: Shared<'g, T>,
        new: Shared<'g, T>,
        guard: &'g Guard,
    ) -> Result<Shared<'g, T>, Shared<'g, T>> {
        debug_delay();
        let tip = traverse(self.head.load(SeqCst, guard), pid, guard);

        debug_delay();

        match tip.compare_and_set(old, new, SeqCst, guard) {
            Ok(_) => {
                if !old.is_null() {
                    unsafe {
                        let old_owned = old.into_owned();
                        guard.defer(move || old_owned);
                    }
                }
                Ok(new)
            }
            Err(e) => Err(e.current),
        }
    }

    /// Try to get a value from the tree.
    pub fn get<'g>(
        &self,
        pid: PageId,
        guard: &'g Guard,
    ) -> Option<Shared<'g, T>> {
        debug_delay();
        let tip = traverse(self.head.load(SeqCst, guard), pid, guard);
        if tip.load(SeqCst, guard).is_null() {
            return None;
        }
        let res = tip.load(SeqCst, guard);
        if res.is_null() {
            None
        } else {
            Some(res)
        }
    }

    /// Delete a value from the tree, returning the old value if it was set.
    pub fn del<'g>(
        &self,
        pid: PageId,
        guard: &'g Guard,
    ) -> Option<Shared<'g, T>> {
        debug_delay();
        let old = self.swap(pid, Shared::null(), guard);
        if !old.is_null() {
            unsafe {
                let old_owned = old.into_owned();
                guard.defer(move || old_owned);
            }
            Some(old)
        } else {
            None
        }
    }
}

fn traverse<'g, T: 'static + Send>(
    head: Shared<'g, Node1<T>>,
    k: PageId,
    guard: &'g Guard,
) -> &'g Atomic<T> {
    let (l1k, l2k) = split_fanout(k);

    debug_delay();
    let l1 = unsafe { &head.deref().children };

    debug_delay();
    let mut l2_ptr = l1[l1k].load(SeqCst, guard);

    if l2_ptr.is_null() {
        let next_child =
            Owned::new(Node2::default()).into_shared(guard);

        debug_delay();
        let ret = l1[l1k]
            .compare_and_set(l2_ptr, next_child, SeqCst, guard);

        match ret {
            Ok(_) => {
                // CAS worked
                l2_ptr = next_child;
            }
            Err(e) => {
                // another thread beat us, drop unused created
                // child and use what is already set
                l2_ptr = e.current;
            }
        }
    }

    debug_delay();
    let l2 = unsafe { &l2_ptr.deref().children };

    &l2[l2k]
}

impl<T> Drop for PageTable<T>
where
    T: 'static + Send + Sync,
{
    fn drop(&mut self) {
        unsafe {
            let head = self.head.load(SeqCst, unprotected()).as_raw();
            drop(Box::from_raw(head as *mut Node1<T>));
        }
    }
}

impl<T: Send + 'static> Drop for Node1<T> {
    fn drop(&mut self) {
        unsafe {
            let guard = pin();
            let children: Vec<*const Node2<T>> = self
                .children
                .iter()
                .map(|c| c.load(SeqCst, &guard).as_raw())
                .filter(|c| !c.is_null())
                .collect();

            for child in children {
                drop(Box::from_raw(child as *mut Node2<T>));
            }
        }
    }
}

impl<T: Send + 'static> Drop for Node2<T> {
    fn drop(&mut self) {
        unsafe {
            let guard = pin();

            let children: Vec<*const T> = self
                .children
                .iter()
                .map(|c| c.load(SeqCst, &guard).as_raw())
                .filter(|c| !c.is_null())
                .collect();

            for child in children {
                drop(Box::from_raw(child as *mut T));
            }
        }
    }
}

#[test]
fn test_split_fanout() {
    assert_eq!(split_fanout(0b11_1111_1111_1111_1111), (0, 0b11_1111_1111_1111_1111));
    assert_eq!(split_fanout(0b111_1111_1111_1111_1111), (0b1, 0b11_1111_1111_1111_1111));
}

#[test]
fn basic_functionality() {
    unsafe {
        let guard = pin();
        let rt = PageTable::default();
        let v1 = Owned::new(5).into_shared(&guard);
        rt.cas(0, Shared::null(), v1, &guard).unwrap();
        let ptr = rt.get(0, &guard).unwrap();
        assert_eq!(ptr.deref(), &5);
        rt.cas(0, ptr, Owned::new(6).into_shared(&guard), &guard)
            .unwrap();
        assert_eq!(rt.get(0, &guard).unwrap().deref(), &6);
        rt.del(0, &guard);
        assert!(rt.get(0, &guard).is_none());

        let k2 = 321 << FANFACTOR;
        let k3 = k2 + 1;

        let v2 = Owned::new(2).into_shared(&guard);
        rt.cas(k2, Shared::null(), v2, &guard).unwrap();
        assert_eq!(rt.get(k2, &guard).unwrap().deref(), &2);
        assert!(rt.get(k3, &guard).is_none());
        let v3 = Owned::new(3).into_shared(&guard);
        rt.cas(k3, Shared::null(), v3, &guard).unwrap();
        assert_eq!(rt.get(k3, &guard).unwrap().deref(), &3);
        assert_eq!(rt.get(k2, &guard).unwrap().deref(), &2);
    }
}

//! A simple wait-free, grow-only pagetable, assumes a dense keyspace.

use std::{
    alloc::{alloc_zeroed, Layout},
    convert::TryFrom,
    mem::{align_of, size_of},
    sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst},
};

use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Shared};

use crate::pagecache::debug::debug_delay;

#[allow(unused)]
#[doc(hidden)]
pub const PAGETABLE_NODE_SZ: usize = size_of::<Node1<()>>();

const FAN_FACTOR: u64 = 18;
const FAN_OUT: u64 = 1 << FAN_FACTOR;
const FAN_MASK: u64 = FAN_OUT - 1;

pub type PageId = u64;

struct Node1<T: Send + 'static> {
    children: [Atomic<Node2<T>>; FAN_OUT as usize],
}

struct Node2<T: Send + 'static> {
    children: [Atomic<T>; FAN_OUT as usize],
}

impl<T: Send + 'static> Node1<T> {
    fn new() -> Box<Self> {
        let size = size_of::<Self>();
        let align = align_of::<Self>();

        unsafe {
            let layout = Layout::from_size_align_unchecked(size, align);

            #[allow(clippy::cast_ptr_alignment)]
            let ptr = alloc_zeroed(layout) as *mut Self;

            Box::from_raw(ptr)
        }
    }
}

impl<T: Send + 'static> Node2<T> {
    fn new() -> Owned<Self> {
        let size = size_of::<Self>();
        let align = align_of::<Self>();

        unsafe {
            let layout = Layout::from_size_align_unchecked(size, align);

            #[allow(clippy::cast_ptr_alignment)]
            let ptr = alloc_zeroed(layout) as *mut Self;

            Owned::from_raw(ptr)
        }
    }
}

impl<T: Send + 'static> Drop for Node1<T> {
    fn drop(&mut self) {
        drop_iter(self.children.iter());
    }
}

impl<T: Send + 'static> Drop for Node2<T> {
    fn drop(&mut self) {
        drop_iter(self.children.iter());
    }
}

fn drop_iter<T>(iter: core::slice::Iter<'_, Atomic<T>>) {
    for child in iter {
        unsafe {
            let shared_child = child.load(Relaxed, &unprotected());
            if shared_child.as_raw().is_null() {
                // this does not leak because the PageTable is
                // assumed to be dense.
                break;
            }
            drop(shared_child.into_owned());
        }
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
    fn default() -> Self {
        let head = Node1::new();
        Self {
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
        let tip = traverse(self.head.load(Acquire, guard), pid, guard);
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
    ) -> std::result::Result<Shared<'g, T>, Shared<'g, T>> {
        debug_delay();
        let tip = traverse(self.head.load(Acquire, guard), pid, guard);

        debug_delay();

        let _ = tip
            .compare_and_set(old, new, Release, guard)
            .map_err(|e| e.current)?;

        if !old.is_null() {
            unsafe {
                guard.defer_destroy(old);
            }
        }
        Ok(new)
    }

    /// Try to get a value from the tree.
    pub fn get<'g>(
        &self,
        pid: PageId,
        guard: &'g Guard,
    ) -> Option<Shared<'g, T>> {
        debug_delay();
        let tip = traverse(self.head.load(Acquire, guard), pid, guard);

        let res = tip.load(Acquire, guard);
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
        if old.is_null() {
            None
        } else {
            unsafe {
                guard.defer_destroy(old);
            }
            Some(old)
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
    let mut l2_ptr = l1[l1k].load(Acquire, guard);

    if l2_ptr.is_null() {
        let next_child = Node2::new().into_shared(guard);

        debug_delay();
        let ret = l1[l1k].compare_and_set(l2_ptr, next_child, Release, guard);

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

#[inline(always)]
fn split_fanout(id: PageId) -> (usize, usize) {
    // right shift 32 on 32-bit pointer systems panics
    #[cfg(target_pointer_width = "64")]
    assert!(
        id <= 1 << (FAN_FACTOR * 2),
        "trying to access key of {}, which is \
         higher than 2 ^ {}",
        id,
        (FAN_FACTOR * 2)
    );

    let left = id >> FAN_FACTOR;
    let right = id & FAN_MASK;

    (safe_usize(left), safe_usize(right))
}

#[inline]
fn safe_usize(value: PageId) -> usize {
    usize::try_from(value).unwrap()
}

impl<T> Drop for PageTable<T>
where
    T: 'static + Send + Sync,
{
    fn drop(&mut self) {
        unsafe {
            let head = self.head.load(Relaxed, &unprotected()).into_owned();
            drop(head);
        }
    }
}

#[test]
fn test_split_fanout() {
    assert_eq!(
        split_fanout(0b11_1111_1111_1111_1111),
        (0, 0b11_1111_1111_1111_1111)
    );
    assert_eq!(
        split_fanout(0b111_1111_1111_1111_1111),
        (0b1, 0b11_1111_1111_1111_1111)
    );
}

#[test]
fn basic_functionality() {
    unsafe {
        let guard = crossbeam_epoch::pin();
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

        let k2 = 321 << FAN_FACTOR;
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

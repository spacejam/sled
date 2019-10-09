//! A simple wait-free, grow-only pagetable, assumes a dense keyspace.
#![allow(unsafe_code)]

use std::{
    alloc::{alloc_zeroed, Layout},
    convert::TryFrom,
    mem::{align_of, size_of},
    sync::atomic::{
        AtomicPtr,
        Ordering::{Acquire, Relaxed, Release},
    },
};

use crate::debug_delay;

#[allow(unused)]
#[doc(hidden)]
pub const PAGETABLE_NODE_SZ: usize = size_of::<Node1<()>>();

const FAN_FACTOR: u64 = 18;
const FAN_OUT: u64 = 1 << FAN_FACTOR;
const FAN_MASK: u64 = FAN_OUT - 1;

pub type PageId = u64;

struct Node1<T: Send + 'static> {
    children: [AtomicPtr<Node2<T>>; FAN_OUT as usize],
}

struct Node2<T: Send + 'static> {
    children: [AtomicPtr<T>; FAN_OUT as usize],
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

fn drop_iter<T>(iter: core::slice::Iter<'_, AtomicPtr<T>>) {
    for child in iter {
        let shared_child = child.load(Relaxed);
        if shared_child.is_null() {
            // this does not leak because the PageTable is
            // assumed to be dense.
            break;
        }
        unsafe {
            drop(Box::from_raw(shared_child));
        }
    }
}

/// A simple lock-free radix tree.
pub struct PageTable<T>
where
    T: 'static + Send + Sync,
{
    head: AtomicPtr<Node1<T>>,
}

impl<T> Default for PageTable<T>
where
    T: 'static + Send + Sync,
{
    fn default() -> Self {
        let head = Node1::new();
        Self { head: AtomicPtr::from(Box::into_raw(head)) }
    }
}

impl<T> PageTable<T>
where
    T: 'static + Send + Sync,
{
    /// # Panics
    ///
    /// will panic if the item is not null already,
    /// which represents a serious failure to
    /// properly handle lifecycles of pages in the
    /// using system.
    pub fn insert(&self, pid: PageId, item: T) {
        debug_delay();
        let tip = traverse(self.head.load(Acquire), pid);

        let old = tip.swap(Box::into_raw(Box::new(item)), Release);
        assert!(old.is_null());
    }

    /// Try to get a value from the tree.
    pub fn get<'g>(&self, pid: PageId) -> Option<&'g T> {
        debug_delay();
        let tip = traverse(self.head.load(Acquire), pid);

        let res = tip.load(Acquire);
        if res.is_null() {
            None
        } else {
            Some(unsafe { &*res })
        }
    }
}

fn traverse<'g, T: 'static + Send>(
    head: *mut Node1<T>,
    k: PageId,
) -> &'g AtomicPtr<T> {
    let (l1k, l2k) = split_fanout(k);

    debug_delay();
    let l1 = unsafe { &(*head).children };

    debug_delay();
    let mut l2_ptr = l1[l1k].load(Acquire);

    if l2_ptr.is_null() {
        let next_child = Box::into_raw(Node2::new());

        debug_delay();
        let ret =
            l1[l1k].compare_and_swap(std::ptr::null_mut(), next_child, Release);

        if ret == std::ptr::null_mut() {
            // success
            l2_ptr = next_child;
        } else {
            unsafe {
                drop(Box::from_raw(next_child));
            }
            l2_ptr = ret;
        }
    }

    debug_delay();
    let l2 = unsafe { &(*l2_ptr).children };

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
            let head = self.head.load(Relaxed);
            drop(Box::from_raw(head));
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

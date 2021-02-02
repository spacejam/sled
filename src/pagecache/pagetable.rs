//! A simple wait-free, grow-only pagetable, assumes a dense keyspace.
#![allow(unsafe_code)]

use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    convert::TryFrom,
    mem::{align_of, size_of},
    sync::atomic::{
        AtomicPtr, AtomicU64,
        Ordering::{Acquire, Relaxed, Release},
    },
};

use crate::{
    debug_delay,
    pagecache::{constants::MAX_PID_BITS, PageView, Pointer},
};

#[cfg(feature = "metrics")]
use crate::{Measure, M};

#[allow(unused)]
#[doc(hidden)]
pub const PAGETABLE_NODE_SZ: usize = size_of::<Node1>();

const NODE2_FAN_FACTOR: usize = 18;
const NODE1_FAN_OUT: usize = 1 << (MAX_PID_BITS - NODE2_FAN_FACTOR);
const NODE2_FAN_OUT: usize = 1 << NODE2_FAN_FACTOR;
const FAN_MASK: u64 = (NODE2_FAN_OUT - 1) as u64;

pub type PageId = u64;

struct Node1 {
    children: [AtomicPtr<Node2>; NODE1_FAN_OUT],
}

struct Node2 {
    children: [AtomicU64; NODE2_FAN_OUT],
}

impl Node1 {
    fn new() -> *mut Self {
        let size = size_of::<Self>();
        let align = align_of::<Self>();

        unsafe {
            let layout = Layout::from_size_align_unchecked(size, align);

            alloc_zeroed(layout) as *mut Self
        }
    }
}

impl Node2 {
    fn new() -> *mut Node2 {
        let size = size_of::<Self>();
        let align = align_of::<Self>();

        unsafe {
            let layout = Layout::from_size_align_unchecked(size, align);

            alloc_zeroed(layout) as *mut Self
        }
    }
}

impl Drop for Node1 {
    fn drop(&mut self) {
        for child in self.children.iter() {
            let shared_child = child.load(Relaxed);
            if shared_child.is_null() {
                // this does not leak because the PageTable is
                // assumed to be dense.
                break;
            }
            unsafe {
                std::ptr::drop_in_place(shared_child);
            }
        }
    }
}

impl Drop for Node2 {
    fn drop(&mut self) {
        for child in self.children.iter() {
            let shared_child = child.load(Relaxed);
            if shared_child == 0 {
                // this does not leak because the PageTable is
                // assumed to be dense.
                break;
            }
            todo!();
        }
    }
}

/// A simple lock-free radix tree.
pub struct PageTable {
    head: AtomicPtr<Node1>,
}

impl Default for PageTable {
    fn default() -> Self {
        let head = Node1::new();
        Self { head: AtomicPtr::from(head) }
    }
}

impl PageTable {
    /// # Panics
    ///
    /// will panic if the item is not null already,
    /// which represents a serious failure to
    /// properly handle lifecycles of pages in the
    /// using system.
    pub(crate) fn insert<'g>(
        &self,
        pid: PageId,
        item: Pointer,
    ) -> PageView<'g> {
        debug_delay();
        let tip = self.traverse(pid);

        let old = tip.swap(u64::from_le_bytes(item.0), Release);
        assert_eq!(old, 0);

        PageView { ptr: item, entry: tip, read: item.read() }
    }

    /// Try to get a value from the tree.
    ///
    /// # Panics
    ///
    /// Panics if the page has never been allocated.
    pub(crate) fn get<'g>(&self, pid: PageId) -> PageView<'g> {
        #[cfg(feature = "metrics")]
        let _measure = Measure::new(&M.get_pagetable);
        debug_delay();
        let tip = self.traverse(pid);

        debug_delay();
        let res = tip.load(Acquire);

        assert_ne!(res, 0, "tried to get pid {}", pid);

        let ptr = Pointer(res.to_le_bytes());
        PageView { ptr, entry: tip, read: ptr.read() }
    }

    pub(crate) fn contains_pid(&self, pid: PageId) -> bool {
        #[cfg(feature = "metrics")]
        let _measure = Measure::new(&M.get_pagetable);
        debug_delay();
        let tip = self.traverse(pid);

        debug_delay();
        let res = tip.load(Acquire);

        res != 0
    }

    fn traverse<'g>(&self, k: PageId) -> &'g AtomicU64 {
        let (l1k, l2k) = split_fanout(k);

        debug_delay();
        let head = self.head.load(Acquire);

        debug_delay();
        let l1 = unsafe { &(*head).children };

        debug_delay();
        let mut l2_ptr = l1[l1k].load(Acquire);

        if l2_ptr.is_null() {
            let next_child = Node2::new();

            debug_delay();
            let ret = l1[l1k].compare_exchange(
                std::ptr::null_mut(),
                next_child,
                Release,
                Acquire,
            );

            l2_ptr = match ret {
                Ok(next_child) => next_child,
                Err(current) => {
                    free(next_child);

                    current
                }
            };
        }

        debug_delay();
        let l2 = unsafe { &(*l2_ptr).children };

        &l2[l2k]
    }
}

#[inline]
fn split_fanout(id: PageId) -> (usize, usize) {
    // right shift 32 on 32-bit pointer systems panics
    #[cfg(target_pointer_width = "64")]
    assert!(
        id <= 1 << MAX_PID_BITS,
        "trying to access key of {}, which is \
         higher than 2 ^ {}",
        id,
        MAX_PID_BITS,
    );

    let left = id >> NODE2_FAN_FACTOR;
    let right = id & FAN_MASK;

    (safe_usize(left), safe_usize(right))
}

#[inline]
fn safe_usize(value: PageId) -> usize {
    usize::try_from(value).unwrap()
}

impl Drop for PageTable {
    fn drop(&mut self) {
        let head = self.head.load(Relaxed);
        unsafe {
            std::ptr::drop_in_place(head);
        }
        free(head);
    }
}

fn free<T>(item: *mut T) {
    let layout = Layout::new::<T>();
    unsafe {
        dealloc(item as *mut u8, layout);
    }
}

#[test]
fn fanout_functionality() {
    assert_eq!(
        split_fanout(0b11_1111_1111_1111_1111),
        (0, 0b11_1111_1111_1111_1111)
    );
    assert_eq!(
        split_fanout(0b111_1111_1111_1111_1111),
        (0b1, 0b11_1111_1111_1111_1111)
    );
}

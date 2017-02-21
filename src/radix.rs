// lock-free radix tree
// this is purpose-built for mapping PageID's to T's
// it supports optimistic mutation, without automatic retry
// goal: high pointer density with a dense address space
// it never deallocates space, eventually this will be addressed

use std::fmt::{self, Debug};
use std::ptr;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::thread;

use super::*;

const FANFACTOR: u64 = 6;
const FANOUT: u64 = 1 << FANFACTOR;
const FAN_MASK: u64 = FANOUT - 1;

#[inline(always)]
pub fn split_fanout(i: u64) -> (u64, u64) {
    let rem = i >> FANFACTOR;
    let first_6 = i & FAN_MASK;
    (first_6, rem)
}

#[derive(Clone)]
struct Node<T> {
    inner: Arc<AtomicPtr<T>>,
    children: Arc<Vec<AtomicPtr<Node<T>>>>,
}

impl<T> Default for Node<T> {
    fn default() -> Node<T> {
        let children = rep_no_copy!(AtomicPtr::new(ptr::null_mut()); FANOUT as usize);
        Node {
            inner: Arc::new(AtomicPtr::new(ptr::null_mut())),
            children: Arc::new(children),
        }
    }
}

impl<T> Drop for Node<T> {
    fn drop(&mut self) {
        for c in self.children.iter() {
            let mut ptr = c.load(Ordering::SeqCst);
            if !ptr.is_null() {
                let node = unsafe { Box::from_raw(ptr) };
            }
        }
    }
}

impl<T> Node<T> {
    fn cas(&self, pid: PageID, old: *mut T, new: *mut T) -> Result<(), ()> {
        if pid == 0 {
            // we've reached the CAS node
            if old == self.inner.compare_and_swap(old, new, Ordering::SeqCst) {
                return Ok(());
            } else {
                return Err(());
            }
        }
        let (first_six, remainder) = split_fanout(pid);
        let child_index = first_six as usize;
        let ref slot = self.children[child_index];
        let mut next_ptr = slot.load(Ordering::SeqCst);
        if next_ptr.is_null() {
            let child = Node::default();
            let child_ptr = Box::into_raw(Box::new(child));
            let ret = slot.compare_and_swap(next_ptr, child_ptr, Ordering::SeqCst);
            if ret == next_ptr {
                // CAS worked
                next_ptr = child_ptr;
            } else {
                // another thread beat us, drop child and use what they set
                let child = unsafe { Box::from_raw(child_ptr) };
                next_ptr = ret;
            }
        }

        let next = unsafe { Box::from_raw(next_ptr) };
        let ret = next.cas(remainder, old, new);
        mem::forget(next);
        ret
    }

    fn get(&self, pid: PageID) -> Option<*mut T> {
        if pid == 0 {
            // we've reached our Node
            let v = self.inner.load(Ordering::SeqCst);
            if v.is_null() {
                return None;
            } else {
                return Some(v);
            }
        }
        // we need to continue
        let (first_six, remainder) = split_fanout(pid);
        let next_ptr = self.children[first_six as usize].load(Ordering::SeqCst);
        if next_ptr.is_null() {
            return None;
        }
        let next = unsafe { Box::from_raw(next_ptr) };
        let ret = next.get(remainder);
        mem::forget(next);
        ret
    }
}

#[derive(Clone)]
pub struct Radix<T> {
    head: Arc<Node<T>>,
}

impl<T> Default for Radix<T> {
    fn default() -> Radix<T> {
        let head = Node::default();
        Radix { head: Arc::new(head) }
    }
}

impl<T> Radix<T> {
    pub fn insert(&self, pid: PageID, inner: *mut T) -> Result<(), ()> {
        self.head.cas(pid, ptr::null_mut(), inner)
    }

    pub fn cas(&self, pid: PageID, old: *mut T, new: *mut T) -> Result<(), ()> {
        self.head.cas(pid, old, new)
    }

    pub fn get(&self, pid: PageID) -> Option<*mut T> {
        self.head.get(pid)

    }

    pub fn del(&self, pid: PageID) -> Result<(), ()> {
        if let Some(cur) = self.get(pid) {
            self.cas(pid, cur, ptr::null_mut())
        } else {
            Err(())
        }
    }
}

#[test]
fn test_split_fanout() {
    let i = 0 + 0b111111;
    assert_eq!(split_fanout(i), (0b111111, 0));
}

#[test]
fn basic_functionality() {
    let rt = Radix::default();
    let one = Box::into_raw(Box::new(1));
    let two = Box::into_raw(Box::new(2));
    let three = Box::into_raw(Box::new(3));
    let four = Box::into_raw(Box::new(4));
    let five = Box::into_raw(Box::new(5));
    let six = Box::into_raw(Box::new(6));
    rt.insert(0, five).unwrap();
    assert_eq!(rt.get(0), Some(five));
    rt.cas(0, five, six).unwrap();
    assert_eq!(rt.get(0), Some(six));
    rt.del(0).unwrap();
    assert_eq!(rt.get(0), None);

    rt.insert(321, two).unwrap();
    assert_eq!(rt.get(321), Some(two));
    assert_eq!(rt.get(322), None);
    rt.insert(322, three).unwrap();
    assert_eq!(rt.get(322), Some(three));
    assert_eq!(rt.get(321), Some(two));
}

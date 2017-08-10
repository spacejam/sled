/// A simple lock-free radix tree, assumes a dense keyspace.

use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

use super::*;

const FANFACTOR: usize = 6;
const FANOUT: usize = 1 << FANFACTOR;
const FAN_MASK: usize = FANOUT - 1;

#[inline(always)]
fn split_fanout(i: usize) -> (usize, usize) {
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
        let children = rep_no_copy!(AtomicPtr::new(ptr::null_mut()); FANOUT);
        Node {
            inner: Arc::new(AtomicPtr::new(ptr::null_mut())),
            children: Arc::new(children),
        }
    }
}

impl<T> Drop for Node<T> {
    fn drop(&mut self) {
        for c in self.children.iter() {
            let ptr = c.load(Ordering::SeqCst);
            if !ptr.is_null() {
                unsafe { Box::from_raw(ptr) };
            }
        }
    }
}

/// A simple lock-free radix tree.
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
    /// Try to create a new item in the tree.
    pub fn insert(&self, pid: PageID, inner: *const T) -> Result<*const T, *const T> {
        self.cas(pid, ptr::null_mut(), inner)
    }

    /// Atomically swap the previous value in a tree with a new one.
    pub fn swap(&self, pid: PageID, new: *const T) -> *const T {
        let tip = traverse(&*self.head, pid, true);
        if tip.is_null() {
            // TODO is this desired?
            return ptr::null_mut();
        }

        unsafe { (*tip).inner.swap(new as *mut _, Ordering::SeqCst) }
    }

    /// Compare and swap an old value to a new one.
    pub fn cas(&self, pid: PageID, old: *const T, new: *const T) -> Result<*const T, *const T> {
        let tip = traverse(&*self.head, pid, true);
        if tip.is_null() {
            return Err(ptr::null_mut());
        }

        let res = unsafe {
            (*tip).inner.compare_and_swap(old as *mut _, new as *mut _, Ordering::SeqCst)
        };

        if old == res && !test_fail() {
            Ok(res)
        } else {
            Err(res)
        }
    }

    /// Try to get a value from the tree.
    pub fn get(&self, pid: PageID) -> Option<*const T> {
        let tip = traverse(&*self.head, pid, false);
        if tip.is_null() {
            return None;
        }
        let v = unsafe { (*tip).inner.load(Ordering::SeqCst) };
        if v.is_null() {
            None
        } else {
            Some(v)
        }
    }

    /// Delete a value from the tree, returning the old value.
    pub fn del(&self, pid: PageID) -> *const T {
        self.swap(pid, ptr::null_mut())
    }
}

#[inline(always)]
fn traverse<T>(ptr: *const Node<T>, pid: PageID, create_intermediate: bool) -> *const Node<T> {
    if pid == 0 {
        return ptr;
    }

    let (first_six, remainder) = split_fanout(pid);
    let child_index = first_six;
    let children = unsafe { &(*ptr).children };
    let mut next_ptr = children[child_index].load(Ordering::SeqCst);

    if next_ptr.is_null() {
        if !create_intermediate {
            return ptr::null_mut();
        }

        let child = Node::default();
        let child_ptr = Box::into_raw(Box::new(child));
        let ret = children[child_index].compare_and_swap(next_ptr, child_ptr, Ordering::SeqCst);
        if ret == next_ptr && !test_fail() {
            // CAS worked
            next_ptr = child_ptr;
        } else {
            // another thread beat us, drop unused created
            // child and use what is already set
            unsafe { Box::from_raw(child_ptr) };
            next_ptr = ret;
        }
    }

    traverse(next_ptr, remainder, create_intermediate)
}

#[test]
fn test_split_fanout() {
    assert_eq!(split_fanout(0b11_1111), (0b11_1111, 0));
    assert_eq!(split_fanout(0b111_1111), (0b11_1111, 0b1));
}

#[test]
fn basic_functionality() {
    let rt = Radix::default();
    let two = raw(2);
    let three = raw(3);
    let five = raw(5);
    let six = raw(6);
    rt.insert(0, five).unwrap();
    assert_eq!(rt.get(0), Some(five));
    rt.cas(0, five, six).unwrap();
    assert_eq!(rt.get(0), Some(six));
    assert_ne!(rt.del(0), ptr::null_mut());
    assert_eq!(rt.get(0), None);

    rt.insert(321, two).unwrap();
    assert_eq!(rt.get(321), Some(two));
    assert_eq!(rt.get(322), None);
    rt.insert(322, three).unwrap();
    assert_eq!(rt.get(322), Some(three));
    assert_eq!(rt.get(321), Some(two));
}

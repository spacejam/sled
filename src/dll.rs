#![allow(unsafe_code)]

use std::{cell::UnsafeCell, ptr};

use super::lru::CacheAccess;

/// A simple doubly linked list for use in the `Lru`
#[derive(Debug)]
pub(crate) struct Node {
    inner: UnsafeCell<CacheAccess>,
    next: *mut Node,
    prev: *mut Node,
}

impl std::ops::Deref for Node {
    type Target = CacheAccess;

    fn deref(&self) -> &CacheAccess {
        unsafe { &(*self.inner.get()) }
    }
}

impl std::ops::DerefMut for Node {
    fn deref_mut(&mut self) -> &mut CacheAccess {
        unsafe { &mut (*self.inner.get()) }
    }
}

impl Node {
    fn unwire(&mut self) {
        unsafe {
            if !self.prev.is_null() {
                (*self.prev).next = self.next;
            }

            if !self.next.is_null() {
                (*self.next).prev = self.prev;
            }
        }

        self.next = ptr::null_mut();
        self.prev = ptr::null_mut();
    }

    // This is a bit hacky but it's done
    // this way because we don't have a way
    // of mutating a key that is in a HashSet.
    //
    // This is safe to do because the hash
    // happens based on the PageId of the
    // CacheAccess, rather than the size
    // that we modify here.
    pub fn swap_sz(&self, new_sz: u8) -> u8 {
        unsafe { std::mem::replace(&mut (*self.inner.get()).sz, new_sz) }
    }
}

/// A simple non-cyclical doubly linked
/// list where items can be efficiently
/// removed from the middle, for the purposes
/// of backing an LRU cache.
pub struct DoublyLinkedList {
    head: *mut Node,
    tail: *mut Node,
    len: usize,
}

unsafe impl Send for DoublyLinkedList {}

impl Drop for DoublyLinkedList {
    fn drop(&mut self) {
        let mut cursor = self.head;
        while !cursor.is_null() {
            unsafe {
                let node = Box::from_raw(cursor);

                // don't need to check for cycles
                // because this Dll is non-cyclical
                cursor = node.prev;

                // this happens without the manual drop,
                // but we keep it for explicitness
                drop(node);
            }
        }
    }
}

impl Default for DoublyLinkedList {
    fn default() -> Self {
        Self { head: ptr::null_mut(), tail: ptr::null_mut(), len: 0 }
    }
}

impl DoublyLinkedList {
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn push_head(&mut self, item: CacheAccess) -> *mut Node {
        self.len += 1;

        let node = Node {
            inner: UnsafeCell::new(item),
            next: ptr::null_mut(),
            prev: self.head,
        };

        let ptr = Box::into_raw(Box::new(node));

        self.push_head_ptr(ptr);

        ptr
    }

    fn push_head_ptr(&mut self, ptr: *mut Node) {
        if !self.head.is_null() {
            unsafe {
                (*self.head).next = ptr;
                (*ptr).prev = self.head;
            }
        }

        if self.tail.is_null() {
            self.tail = ptr;
        }

        self.head = ptr;
    }

    #[cfg(test)]
    pub(crate) fn push_tail(&mut self, item: CacheAccess) {
        self.len += 1;

        let node = Node {
            inner: UnsafeCell::new(item),
            next: self.tail,
            prev: ptr::null_mut(),
        };

        let ptr = Box::into_raw(Box::new(node));

        if !self.tail.is_null() {
            unsafe {
                (*self.tail).prev = ptr;
            }
        }

        if self.head.is_null() {
            self.head = ptr;
        }

        self.tail = ptr;
    }

    pub(crate) fn promote(&mut self, ptr: *mut Node) {
        if self.head == ptr {
            return;
        }

        unsafe {
            if self.tail == ptr {
                self.tail = (*ptr).next;
            }

            if self.head == ptr {
                self.head = (*ptr).prev;
            }

            (*ptr).unwire();

            self.push_head_ptr(ptr);
        }
    }

    #[cfg(test)]
    pub(crate) fn pop_head(&mut self) -> Option<CacheAccess> {
        if self.head.is_null() {
            return None;
        }

        self.len -= 1;

        unsafe {
            let mut head = Box::from_raw(self.head);

            if self.head == self.tail {
                self.tail = ptr::null_mut();
            }

            self.head = head.prev;

            head.unwire();

            Some(**head)
        }
    }

    // NB: returns the Box<Node> instead of just the Option<CacheAccess>
    // because the LRU is a map to the Node as well, and if the LRU
    // accessed the map via PID, it would cause a use after free if
    // we had already freed the Node in this function.
    pub(crate) fn pop_tail(&mut self) -> Option<Box<Node>> {
        if self.tail.is_null() {
            return None;
        }

        self.len -= 1;

        unsafe {
            let mut tail: Box<Node> = Box::from_raw(self.tail);

            if self.head == self.tail {
                self.head = ptr::null_mut();
            }

            self.tail = tail.next;

            tail.unwire();

            Some(tail)
        }
    }

    #[cfg(test)]
    pub(crate) fn into_vec(mut self) -> Vec<CacheAccess> {
        let mut res = vec![];
        while let Some(val) = self.pop_head() {
            res.push(val);
        }
        res
    }
}

#[allow(unused_results)]
#[test]
fn basic_functionality() {
    let mut dll = DoublyLinkedList::default();
    dll.push_head(5.into());
    dll.push_tail(6.into());
    dll.push_head(4.into());
    dll.push_tail(7.into());
    dll.push_tail(8.into());
    dll.push_head(3.into());
    dll.push_tail(9.into());
    dll.push_head(2.into());
    dll.push_head(1.into());
    assert_eq!(dll.len(), 9);
    assert_eq!(
        dll.into_vec(),
        vec![
            1.into(),
            2.into(),
            3.into(),
            4.into(),
            5.into(),
            6.into(),
            7.into(),
            8.into(),
            9.into()
        ]
    );
}

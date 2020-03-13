#![allow(unsafe_code)]

use std::ptr;

use crate::PageId;

/// A simple doubly linked list for use in the `Lru`
#[derive(Debug)]
pub(crate) struct Node {
    inner: PageId,
    next: *mut Node,
    prev: *mut Node,
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

    pub(crate) fn push_head(&mut self, item: PageId) -> *mut Node {
        self.len += 1;

        let node = Node { inner: item, next: ptr::null_mut(), prev: self.head };

        let ptr = Box::into_raw(Box::new(node));

        self.push_head_ptr(ptr)
    }

    fn push_head_ptr(&mut self, ptr: *mut Node) -> *mut Node {
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

        ptr
    }

    #[cfg(test)]
    pub(crate) fn push_tail(&mut self, item: PageId) {
        self.len += 1;

        let node = Node { inner: item, next: self.tail, prev: ptr::null_mut() };

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

    pub(crate) fn promote(&mut self, ptr: *mut Node) -> *mut Node {
        if self.head == ptr {
            return ptr;
        }

        unsafe {
            if self.tail == ptr {
                self.tail = (*ptr).next;
            }

            if self.head == ptr {
                self.head = (*ptr).prev;
            }

            (*ptr).unwire();

            self.push_head_ptr(ptr)
        }
    }

    #[cfg(test)]
    pub(crate) fn pop_head(&mut self) -> Option<PageId> {
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

            Some(head.inner)
        }
    }

    pub(crate) fn pop_tail(&mut self) -> Option<PageId> {
        if self.tail.is_null() {
            return None;
        }

        self.len -= 1;

        unsafe {
            let mut tail = Box::from_raw(self.tail);

            if self.head == self.tail {
                self.head = ptr::null_mut();
            }

            self.tail = tail.next;

            tail.unwire();

            Some(tail.inner)
        }
    }

    #[cfg(test)]
    pub(crate) fn into_vec(mut self) -> Vec<PageId> {
        let mut res = vec![];
        while let Some(val) = self.pop_head() {
            res.push(val);
        }
        res
    }
}

#[allow(unused_results)]
#[test]
fn test_dll() {
    let mut dll = DoublyLinkedList::default();
    dll.push_head(5);
    dll.push_tail(6);
    dll.push_head(4);
    dll.push_tail(7);
    dll.push_tail(8);
    dll.push_head(3);
    dll.push_tail(9);
    dll.push_head(2);
    dll.push_head(1);
    assert_eq!(dll.len(), 9);
    assert_eq!(dll.into_vec(), vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

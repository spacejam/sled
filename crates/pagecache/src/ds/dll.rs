/// A simple doubly-linked list for use in the `Lru`
use std::ptr;

use super::*;

#[derive(Debug)]
pub struct Node {
    inner: PageID,
    next: *mut Node,
    prev: *mut Node,
}

impl Drop for Node {
    fn drop(&mut self) {
        if !self.prev.is_null() {
            unsafe {
                drop(Box::from_raw(self.prev));
            }
        }
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
}

pub struct Dll {
    head: *mut Node,
    tail: *mut Node,
    len: usize,
}

impl Drop for Dll {
    fn drop(&mut self) {
        if !self.head.is_null() {
            unsafe {
                drop(Box::from_raw(self.head));
            }
        }
    }
}

impl Default for Dll {
    fn default() -> Dll {
        Dll {
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
            len: 0,
        }
    }
}

impl Dll {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn push_head(&mut self, item: PageID) -> *mut Node {
        self.len += 1;

        let node = Node {
            inner: item,
            next: ptr::null_mut(),
            prev: self.head,
        };

        let ptr = Box::into_raw(Box::new(node));

        if !self.head.is_null() {
            unsafe {
                (*self.head).next = ptr;
            }
        }

        if self.tail.is_null() {
            self.tail = ptr;
        }

        self.head = ptr;

        ptr
    }

    #[cfg(test)]
    pub fn push_tail(&mut self, item: PageID) {
        self.len += 1;

        let node = Node {
            inner: item,
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

    pub fn promote(&mut self, ptr: *mut Node) -> *mut Node {
        if self.head == ptr {
            return ptr;
        }

        unsafe {
            let pid = self.pop_ptr(ptr);

            self.push_head(pid)
        }
    }

    #[cfg(test)]
    pub fn pop_head(&mut self) -> Option<PageID> {
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

    pub fn pop_tail(&mut self) -> Option<PageID> {
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

    pub unsafe fn pop_ptr(&mut self, ptr: *mut Node) -> PageID {
        self.len -= 1;

        let mut node = Box::from_raw(ptr);

        if self.tail == ptr {
            self.tail = node.next;
        }

        if self.head == ptr {
            self.head = node.prev;
        }

        node.unwire();

        node.inner
    }

    #[cfg(test)]
    pub fn into_vec(mut self) -> Vec<PageID> {
        let mut res = vec![];
        while let Some(val) = self.pop_head() {
            res.push(val);
        }
        res
    }
}

#[test]
fn test_dll() {
    let mut dll = Dll::default();
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

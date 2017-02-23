// lock-free stack
use std::fmt::{self, Debug};
use std::ptr;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

use std::thread;

use super::*;

pub struct Node<T> {
    inner: T,
    next: *mut Node<T>,
}

#[derive(Clone)]
pub struct Stack<T> {
    head: Arc<AtomicPtr<Node<T>>>,
}

impl<T> Default for Stack<T> {
    fn default() -> Stack<T> {
        Stack { head: Arc::new(AtomicPtr::new(ptr::null_mut())) }
    }
}

impl<T: Debug> Debug for Stack<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let mut ptr = self.head();
        formatter.write_str("Stack [");
        let mut written = false;
        while !ptr.is_null() {
            if written {
                formatter.write_str(", ");
            }
            unsafe {
                (*ptr).inner.fmt(formatter);
                ptr = (*ptr).next;
            }
            written = true;
        }
        formatter.write_str("]");
        Ok(())
    }
}

impl<T> Deref for Node<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T> Node<T> {
    pub fn next(&self) -> *mut Node<T> {
        self.next
    }
}

impl<T> Drop for Stack<T> {
    fn drop(&mut self) {
        let mut ptr = self.head();
        while !ptr.is_null() {
            let node = unsafe { Box::from_raw(ptr) };
            ptr = node.next;
        }
    }
}

impl<T> Stack<T> {
    pub fn try_push(&self, inner: T) -> Result<(), ()> {
        let cur = self.head();
        let mut node = Box::into_raw(Box::new(Node {
            inner: inner,
            next: cur,
        }));
        if cur == self.head.compare_and_swap(cur, node, Ordering::SeqCst) {
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn try_pop(&self) -> Result<Option<T>, ()> {
        let head_ptr = self.head();
        if head_ptr.is_null() {
            return Ok(None);
        }
        let node = unsafe { Box::from_raw(head_ptr) };
        let next_ptr = node.next;

        if head_ptr == self.head.compare_and_swap(head_ptr, next_ptr, Ordering::SeqCst) {
            Ok(Some(node.inner))
        } else {
            mem::forget(node);
            Err(())
        }
    }

    pub fn pop_all(&self) -> Vec<T> {
        let mut res = vec![];
        let mut node_ptr = self.head.swap(ptr::null_mut(), Ordering::SeqCst);
        while !node_ptr.is_null() {
            let node = unsafe { Box::from_raw(node_ptr) };
            node_ptr = node.next;
            res.push(node.inner);
        }
        res
    }

    pub fn head(&self) -> *mut Node<T> {
        self.head.load(Ordering::SeqCst)
    }
}

#[test]
fn basic_functionality() {
    let ll = Arc::new(Stack::default());
    assert_eq!(ll.try_pop(), Ok(None));
    ll.try_push(1).unwrap();
    let ll2 = ll.clone();
    let t = thread::spawn(move || {
        ll2.try_push(2).unwrap();
        ll2.try_push(3).unwrap();
        ll2.try_push(4).unwrap();
    });
    t.join();
    ll.try_push(5).unwrap();
    assert_eq!(ll.try_pop(), Ok(Some(5)));
    assert_eq!(ll.try_pop(), Ok(Some(4)));
    let ll3 = ll.clone();
    let t = thread::spawn(move || {
        assert_eq!(ll3.try_pop(), Ok(Some(3)));
        assert_eq!(ll3.try_pop(), Ok(Some(2)));
    });
    t.join();
    assert_eq!(ll.try_pop(), Ok(Some(1)));
    let ll4 = ll.clone();
    let t = thread::spawn(move || {
        assert_eq!(ll4.try_pop(), Ok(None));
    });
    t.join();
}

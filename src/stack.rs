// lock-free stack
use std::fmt::{self, Debug};
use std::ptr;
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

use {raw, test_fail};

pub struct Node<T> {
    inner: T,
    next: *const Node<T>,
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
        formatter.write_str("Stack(").unwrap();
        ptr.fmt(formatter).unwrap();
        formatter.write_str(") [").unwrap();
        let mut written = false;
        while !ptr.is_null() {
            if written {
                formatter.write_str(", ").unwrap();
            }
            unsafe {
                (*ptr).inner.fmt(formatter).unwrap();
                ptr = (*ptr).next;
            }
            written = true;
        }
        formatter.write_str("]").unwrap();
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
    pub fn next(&self) -> *const Node<T> {
        self.next
    }
}

impl<T> Drop for Stack<T> {
    fn drop(&mut self) {
        let mut ptr = self.head();
        while !ptr.is_null() {
            let node: Box<Node<T>> = unsafe { Box::from_raw(ptr as *mut _) };
            ptr = node.next;
        }
    }
}

impl<T> Stack<T> {
    pub fn from_raw(from: *const Node<T>) -> Stack<T> {
        Stack { head: Arc::new(AtomicPtr::new(from as *mut Node<T>)) }
    }

    pub fn from_vec(from: Vec<T>) -> Stack<T> {
        let stack = Stack::default();

        for item in from.into_iter().rev() {
            stack.push(item);
        }

        stack
    }

    pub fn push(&self, inner: T) {
        let mut head = self.head() as *mut _;
        let mut node = Box::into_raw(Box::new(Node {
            inner: inner,
            next: head,
        }));
        loop {
            let ret = self.head.compare_and_swap(head, node, Ordering::SeqCst);
            if head == ret {
                return;
            }
            head = ret;
            unsafe {
                (*node).next = head;
            }
        }
    }

    pub fn pop(&self) -> Option<T> {
        loop {
            let head_ptr = self.head() as *mut _;
            if head_ptr.is_null() {
                return None;
            }
            let node: Box<Node<T>> = unsafe { Box::from_raw(head_ptr) };
            let next_ptr = node.next;

            if head_ptr ==
               self.head.compare_and_swap(head_ptr, next_ptr as *mut _, Ordering::SeqCst) {
                return Some(node.inner);
            } else {
                mem::forget(node);
            }
        }
    }

    pub fn pop_all(&self) -> Vec<T> {
        let mut node_ptr = self.head.swap(ptr::null_mut(), Ordering::SeqCst);
        let mut res = vec![];
        while !node_ptr.is_null() {
            let node = unsafe { Box::from_raw(node_ptr) };
            node_ptr = node.next as *mut _;
            res.push(node.inner);
        }
        res
    }

    /// compare and push
    pub fn cap(&self, old: *const Node<T>, new: T) -> Result<*const Node<T>, *const Node<T>> {
        let node = Box::into_raw(Box::new(Node {
            inner: new,
            next: old,
        }));
        let res = self.head.compare_and_swap(old as *mut _, node as *mut _, Ordering::SeqCst);
        if old == res && !test_fail() {
            Ok(node)
        } else {
            Err(res)
        }
    }

    /// attempt consolidation
    pub fn cas(&self,
               old: *const Node<T>,
               new: *const Node<T>)
               -> Result<*const Node<T>, *const Node<T>> {
        let res = self.head.compare_and_swap(old as *mut _, new as *mut _, Ordering::SeqCst);
        if old == res && !test_fail() {
            Ok(new)
        } else {
            Err(res)
        }
    }

    pub fn iter_at_head(&self) -> (*const Node<T>, StackIter<T>) {
        let head = self.head();
        if head.is_null() {
            panic!("iter_at_head returning null head");
        }
        (head,
         StackIter {
            inner: head,
            marker: PhantomData,
        })
    }

    pub fn head(&self) -> *const Node<T> {
        self.head.load(Ordering::Acquire)
    }

    pub fn len(&self) -> usize {
        let mut len = 0;
        let mut head = self.head();
        while !head.is_null() {
            len += 1;
            head = unsafe { (*head).next };
        }
        len
    }
}

pub struct StackIter<'a, T: 'a> {
    inner: *const Node<T>,
    marker: PhantomData<&'a Node<T>>,
}

impl<'a, T: 'a> StackIter<'a, T> {
    pub fn from_ptr(ptr: *const Node<T>) -> StackIter<'a, T> {
        StackIter {
            inner: ptr,
            marker: PhantomData,
        }
    }
}

impl<'a, T> Iterator for StackIter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.inner.is_null() {
            None
        } else {
            unsafe {
                let ret = &(*self.inner).inner;
                self.inner = (*self.inner).next;
                Some(ret)
            }
        }
    }
}

impl<'a, T> IntoIterator for &'a Stack<T> {
    type Item = &'a T;
    type IntoIter = StackIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        StackIter {
            inner: self.head(),
            marker: PhantomData,
        }
    }
}

pub fn node_from_frag_vec<T>(from: Vec<T>) -> *const Node<T> {
    use std::ptr;
    let mut last = ptr::null();

    for item in from.into_iter().rev() {
        let node = raw(Node {
            inner: item,
            next: last,
        });
        last = node;
    }

    last
}

#[test]
fn basic_functionality() {
    use std::thread;

    let ll = Arc::new(Stack::default());
    assert_eq!(ll.pop(), None);
    ll.push(1);
    let ll2 = ll.clone();
    let t = thread::spawn(move || {
        ll2.push(2);
        ll2.push(3);
        ll2.push(4);
    });
    t.join().unwrap();
    ll.push(5);
    assert_eq!(ll.pop(), Some(5));
    assert_eq!(ll.pop(), Some(4));
    let ll3 = ll.clone();
    let t = thread::spawn(move || {
        assert_eq!(ll3.pop(), Some(3));
        assert_eq!(ll3.pop(), Some(2));
    });
    t.join().unwrap();
    assert_eq!(ll.pop(), Some(1));
    let ll4 = ll.clone();
    let t = thread::spawn(move || {
        assert_eq!(ll4.pop(), None);
    });
    t.join().unwrap();
}

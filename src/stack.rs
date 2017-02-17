// lock-free linked-list
use std::cell::{UnsafeCell, RefCell};
use std::fmt::{self, Debug};
use std::ptr;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use std::thread;

use super::*;

pub struct Node {
    id: LogID,
    header: AtomicUsize, /* header is used to track seal and users, to
                          * prevent accidental deallocation */
    next: *mut Node,
}

pub struct List {
    head: Arc<AtomicPtr<Node>>,
}

unsafe impl Send for List {}

impl Default for List {
    fn default() -> List {
        List { head: Arc::new(AtomicPtr::new(ptr::null_mut())) }
    }
}

impl Debug for List {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let mut ptr = self.head.load(Ordering::SeqCst);
        let mut ids = vec![];
        while !ptr.is_null() {
            let node = unsafe { Box::from_raw(ptr) };
            ids.push(node.id);
            ptr = node.next;
            mem::forget(node);
        }
        let debug = format!("List {:?}", ids);
        fmt::Debug::fmt(&debug, formatter)
    }
}

impl Drop for List {
    fn drop(&mut self) {
        let mut ptr = self.head.load(Ordering::SeqCst);
        while !ptr.is_null() {
            let node = unsafe { Box::from_raw(ptr) };
            ptr = node.next;
            drop(node);
        }
    }
}

impl List {
    fn append(&self, id: LogID) -> Result<(), ()> {
        let cur = self.head.load(Ordering::SeqCst);
        let mut node = Box::into_raw(Box::new(Node {
            id: id,
            header: AtomicUsize::new(0),
            next: cur,
        }));
        let old = self.head.compare_and_swap(cur, node, Ordering::SeqCst);
        if cur == old {
            Ok(())
        } else {
            Err(())
        }
    }

    fn remove(&self, id: LogID) -> Result<(), ()> {
        // find node
        // get next
        // back up, CAS
        Ok(())
    }

    fn head(&self) -> *mut Node {
        self.head.load(Ordering::SeqCst)
    }
}

#[test]
fn basic_functionality() {
    let ll = List::default();
    ll.append(1).unwrap();
    ll.append(2).unwrap();
    ll.append(3).unwrap();
    ll.append(4).unwrap();
    ll.append(5).unwrap();
    let h = ll.head();
    unsafe {
        assert_eq!((*h).id, 5);
    }
    println!("ll is {:?}", ll);
}

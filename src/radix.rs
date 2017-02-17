// lock-free radix tree
// this is purpose-built for mapping PageID's to LogID's
// it supports optimistic mutation, without automatic retry
// goal: high pointer density with a dense address space
// it never deallocates space
use std::fmt::{self, Debug};
use std::ptr;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::thread;

use super::*;

macro_rules! rep_no_copy {
    ($e:expr; $n:expr) => {
        {
            let mut v = Vec::with_capacity($n);
            for _ in 0..$n {
                v.push($e);
            }
            v
        }
    };
}

struct Node {
    log_id: AtomicUsize,
    children: Vec<AtomicPtr<Node>>,
}

impl Default for Node {
    fn default() -> Node {
        let children = rep_no_copy!(AtomicPtr::new(ptr::null_mut()); 1 << 6);
        Node {
            log_id: AtomicUsize::new(0),
            children: children,
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        for ref c in &self.children {
            let mut ptr = c.load(Ordering::SeqCst);
            if !ptr.is_null() {
                let node = unsafe { Box::from_raw(ptr) };
                drop(node);
            }
        }
    }
}

impl Node {
    fn cas(&self, pid: PageID, old_lid: LogID, new_lid: LogID) -> Result<(), ()> {
        if pid == 0 {
            // we've reached the CAS node
            if old_lid as usize ==
               self.log_id.compare_and_swap(old_lid as usize, new_lid as usize, Ordering::SeqCst) {
                return Ok(());
            } else {
                return Err(());
            }
        }
        let (first_six, remainder) = ops::split_six(pid);
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
                drop(child);
                next_ptr = ret;
            }
        }

        let next = unsafe { Box::from_raw(next_ptr) };
        let ret = next.cas(remainder, old_lid, new_lid);
        mem::forget(next);
        ret
    }

    fn get(&self, pid: PageID) -> Option<LogID> {
        if pid == 0 {
            // we've reached our Node
            let v = self.log_id.load(Ordering::SeqCst);
            if v == 0 {
                return None;
            } else {
                return Some(v as LogID);
            }
        }
        // we need to continue
        let (first_six, remainder) = ops::split_six(pid);
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

struct RTree {
    head: Arc<AtomicPtr<Node>>,
}

impl Default for RTree {
    fn default() -> RTree {
        let head = Node::default();
        let head_ptr = Box::into_raw(Box::new(head));
        RTree { head: Arc::new(AtomicPtr::new(head_ptr)) }
    }
}

impl Drop for RTree {
    fn drop(&mut self) {
        let mut ptr = self.head();
        if !ptr.is_null() {
            let node = unsafe { Box::from_raw(ptr) };
            drop(node);
        }
    }
}

impl RTree {
    fn insert(&self, pid: PageID, lid: LogID) -> Result<(), ()> {
        let head_ptr = self.head();
        let head = unsafe { Box::from_raw(head_ptr) };
        let ret = head.cas(pid, 0, lid);
        mem::forget(head);
        ret
    }

    fn cas(&self, pid: PageID, old_lid: LogID, new_lid: LogID) -> Result<(), ()> {
        let head_ptr = self.head();
        let head = unsafe { Box::from_raw(head_ptr) };
        let ret = head.cas(pid, old_lid, new_lid);
        mem::forget(head);
        ret
    }

    fn get(&self, pid: PageID) -> Option<LogID> {
        let head_ptr = self.head();
        let head = unsafe { Box::from_raw(head_ptr) };
        let ret = head.get(pid);
        mem::forget(head);
        ret

    }

    fn del(&self, pid: PageID) -> Result<(), ()> {
        let head_ptr = self.head();
        let head = unsafe { Box::from_raw(head_ptr) };
        let ret = if let Some(cur) = self.get(pid) {
            head.cas(pid, cur, 0)
        } else {
            Err(())
        };
        mem::forget(head);
        ret
    }

    fn head(&self) -> *mut Node {
        self.head.load(Ordering::SeqCst)
    }
}

#[test]
fn basic_functionality() {
    let rt = RTree::default();
    rt.insert(0, 5).unwrap();
    assert_eq!(rt.get(0), Some(5));
    rt.cas(0, 5, 6).unwrap();
    assert_eq!(rt.get(0), Some(6));
    rt.cas(0, 6, 0).unwrap();
    assert_eq!(rt.get(0), None);

    rt.cas(321, 0, 9).unwrap();
    assert_eq!(rt.get(321), Some(9));
    assert_eq!(rt.get(322), None);
    rt.cas(322, 0, 19).unwrap();
    assert_eq!(rt.get(322), Some(19));
    assert_eq!(rt.get(321), Some(9));
}

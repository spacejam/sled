/// A simple lock-free radix tree, assumes a dense keyspace.

use std::sync::atomic::Ordering::SeqCst;

use coco::epoch::{Atomic, Owned, Ptr, Scope, pin, unprotected};

use super::*;

const FANFACTOR: usize = 6;
const FANOUT: usize = 1 << FANFACTOR;
const FAN_MASK: usize = FANOUT - 1;

#[inline(always)]
fn split_fanout(i: usize) -> (usize, usize) {
    let rem = i >> FANFACTOR;
    let first = i & FAN_MASK;
    (first, rem)
}

struct Node<T: Send + 'static> {
    inner: Atomic<T>,
    children: Vec<Atomic<Node<T>>>,
}

impl<T: Send + 'static> Default for Node<T> {
    fn default() -> Node<T> {
        let children = rep_no_copy!(Atomic::null(); FANOUT);
        Node {
            inner: Atomic::null(),
            children: children,
        }
    }
}

impl<T: Send + 'static> Drop for Node<T> {
    fn drop(&mut self) {
        unsafe {
            pin(|scope| {
                let inner = self.inner.load(SeqCst, scope).as_raw();
                if !inner.is_null() {
                    drop(Box::from_raw(inner as *mut T));
                }

                let children: Vec<*const Node<T>> = self.children
                    .iter()
                    .map(|c| c.load(SeqCst, scope).as_raw())
                    .filter(|c| !c.is_null())
                    .collect();

                for child in children {
                    drop(Box::from_raw(child as *mut Node<T>));
                }
            })
        }
    }
}

/// A simple lock-free radix tree.
pub struct Radix<T>
    where T: 'static + Send + Sync
{
    head: Atomic<Node<T>>,
}

impl<T> Default for Radix<T>
    where T: 'static + Send + Sync
{
    fn default() -> Radix<T> {
        let head = Owned::new(Node::default());
        Radix {
            head: Atomic::from_owned(head),
        }
    }
}

impl<T> Drop for Radix<T>
    where T: 'static + Send + Sync
{
    fn drop(&mut self) {
        unsafe {
            unprotected(|scope| {
                let head = self.head.load(SeqCst, scope).as_raw();
                drop(Box::from_raw(head as *mut Node<T>));
            })
        }
    }
}

impl<T> Radix<T>
    where T: 'static + Send + Sync
{
    /// Try to create a new item in the tree.
    pub fn insert(&self, pid: PageID, item: T) -> Result<(), ()> {
        pin(|scope| {
            let new = Owned::new(item).into_ptr(scope);
            self.cas(pid, Ptr::null(), new, scope).map(|_| ()).map_err(
                |_| (),
            )
        })
    }

    /// Atomically swap the previous value in a tree with a new one.
    pub fn swap<'s>(&self, pid: PageID, new: Ptr<'s, T>, scope: &'s Scope) -> Ptr<'s, T> {
        let tip = traverse(self.head.load(SeqCst, scope), pid, true, scope);
        unsafe { tip.deref().inner.swap(new, SeqCst, scope) }
    }

    /// Compare and swap an old value to a new one.
    pub fn cas<'s>(
        &self,
        pid: PageID,
        old: Ptr<'s, T>,
        new: Ptr<'s, T>,
        scope: &'s Scope,
    ) -> Result<Ptr<'s, T>, Ptr<'s, T>> {
        let tip = traverse(self.head.load(SeqCst, scope), pid, true, scope);

        if test_fail() {
            // TODO
        }

        unsafe {
            match tip.deref().inner.compare_and_swap(old, new, SeqCst, scope) {
                Ok(()) => {
                    if !old.is_null() {
                        scope.defer_drop(old);
                    }
                    Ok(new)
                }
                Err(e) => Err(e),
            }
        }
    }

    /// Try to get a value from the tree.
    pub fn get<'s>(&self, pid: PageID, scope: &'s Scope) -> Option<Ptr<'s, T>> {
        let tip = traverse(self.head.load(SeqCst, scope), pid, false, scope);
        if tip.is_null() {
            return None;
        }
        let res = unsafe { tip.deref().inner.load(SeqCst, scope) };
        if res.is_null() { None } else { Some(res) }
    }

    /// Delete a value from the tree, returning the old value if it was set.
    pub fn del<'s>(&self, pid: PageID, scope: &'s Scope) -> Option<Ptr<'s, T>> {
        let old = self.swap(pid, Ptr::null(), scope);
        if !old.is_null() {
            unsafe { scope.defer_drop(old) };
            Some(old)
        } else {
            None
        }
    }
}

#[inline(always)]
fn traverse<'s, T: 'static + Send>(
    ptr: Ptr<'s, Node<T>>,
    pid: PageID,
    create_intermediate: bool,
    scope: &'s Scope,
) -> Ptr<'s, Node<T>> {
    if pid == 0 {
        return ptr;
    }

    let (first_bits, remainder) = split_fanout(pid);
    let child_index = first_bits;
    let children = unsafe { &ptr.deref().children };
    let mut next_ptr = children[child_index].load(SeqCst, scope);

    if next_ptr.is_null() {
        if !create_intermediate {
            return Ptr::null();
        }

        let next_child = Owned::new(Node::default()).into_ptr(scope);
        let ret = children[child_index].compare_and_swap(next_ptr, next_child, SeqCst, scope);
        if ret.is_ok() && !test_fail() {
            // CAS worked
            next_ptr = next_child;
        } else {
            // another thread beat us, drop unused created
            // child and use what is already set
            next_ptr = ret.unwrap_err();
        }
    }

    traverse(next_ptr, remainder, create_intermediate, scope)
}

#[test]
fn test_split_fanout() {
    assert_eq!(split_fanout(0b11_1111), (0b11_1111, 0));
    assert_eq!(split_fanout(0b111_1111), (0b11_1111, 0b1));
}

#[test]
fn basic_functionality() {
    pin(|scope| unsafe {
        let rt = Radix::default();
        rt.insert(0, 5).unwrap();
        let ptr = rt.get(0, scope).unwrap();
        assert_eq!(ptr.deref(), &5);
        rt.cas(0, ptr, Owned::new(6).into_ptr(scope), scope)
            .unwrap();
        assert_eq!(rt.get(0, scope).unwrap().deref(), &6);
        rt.del(0, scope);
        assert!(rt.get(0, scope).is_none());

        rt.insert(321, 2).unwrap();
        assert_eq!(rt.get(321, scope).unwrap().deref(), &2);
        assert!(rt.get(322, scope).is_none());
        rt.insert(322, 3).unwrap();
        assert_eq!(rt.get(322, scope).unwrap().deref(), &3);
        assert_eq!(rt.get(321, scope).unwrap().deref(), &2);
    })
}

/// A simple wait-free, grow-only radix tree, assumes a dense keyspace.

use std::sync::atomic::Ordering::SeqCst;

use epoch::{Atomic, Guard, Owned, Shared, pin, unprotected};

use {PageID, debug_delay};

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
            let guard = pin();
            let inner = self.inner.load(SeqCst, &guard).as_raw();
            if !inner.is_null() {
                drop(Box::from_raw(inner as *mut T));
            }

            let children: Vec<*const Node<T>> = self.children
                .iter()
                .map(|c| c.load(SeqCst, &guard).as_raw())
                .filter(|c| !c.is_null())
                .collect();

            for child in children {
                drop(Box::from_raw(child as *mut Node<T>));
            }
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
            head: Atomic::from(head),
        }
    }
}

impl<T> Drop for Radix<T>
    where T: 'static + Send + Sync
{
    fn drop(&mut self) {
        unsafe {
            let head = self.head.load(SeqCst, unprotected()).as_raw();
            drop(Box::from_raw(head as *mut Node<T>));
        }
    }
}

impl<T> Radix<T>
    where T: 'static + Send + Sync
{
    /// Try to create a new item in the tree.
    pub fn insert(&self, pid: PageID, item: T) -> Result<(), ()> {
        debug_delay();
        let guard = pin();
        let new = Owned::new(item).into_shared(&guard);
        self.cas(pid, Shared::null(), new, &guard)
            .map(|_| ())
            .map_err(|_| ())
    }

    /// Atomically swap the previous value in a tree with a new one.
    pub fn swap<'g>(
        &self,
        pid: PageID,
        new: Shared<'g, T>,
        guard: &'g Guard,
    ) -> Shared<'g, T> {
        debug_delay();
        let tip = traverse(self.head.load(SeqCst, guard), pid, true, guard);
        unsafe { tip.deref().inner.swap(new, SeqCst, guard) }
    }

    /// Compare and swap an old value to a new one.
    pub fn cas<'g>(
        &self,
        pid: PageID,
        old: Shared<'g, T>,
        new: Shared<'g, T>,
        guard: &'g Guard,
    ) -> Result<Shared<'g, T>, Shared<'g, T>> {
        debug_delay();
        let tip = traverse(self.head.load(SeqCst, guard), pid, true, guard);

        unsafe {
            match tip.deref().inner.compare_and_set(old, new, SeqCst, guard) {
                Ok(_) => {
                    if !old.is_null() {
                        guard.defer(move || old.into_owned());
                    }
                    Ok(new)
                }
                Err(e) => Err(e.current),
            }
        }
    }

    /// Try to get a value from the tree.
    pub fn get<'g>(
        &self,
        pid: PageID,
        guard: &'g Guard,
    ) -> Option<Shared<'g, T>> {
        debug_delay();
        let tip = traverse(self.head.load(SeqCst, guard), pid, false, guard);
        if tip.is_null() {
            return None;
        }
        let res = unsafe { tip.deref().inner.load(SeqCst, guard) };
        if res.is_null() { None } else { Some(res) }
    }

    /// Delete a value from the tree, returning the old value if it was set.
    pub fn del<'g>(
        &self,
        pid: PageID,
        guard: &'g Guard,
    ) -> Option<Shared<'g, T>> {
        debug_delay();
        let old = self.swap(pid, Shared::null(), guard);
        if !old.is_null() {
            unsafe {
                guard.defer(move || old.into_owned());
            }
            Some(old)
        } else {
            None
        }
    }
}

#[inline(always)]
fn traverse<'g, T: 'static + Send>(
    ptr: Shared<'g, Node<T>>,
    pid: PageID,
    create_intermediate: bool,
    guard: &'g Guard,
) -> Shared<'g, Node<T>> {
    if pid == 0 {
        return ptr;
    }

    let (first_bits, remainder) = split_fanout(pid);
    let child_index = first_bits;
    let children = unsafe { &ptr.deref().children };
    let mut next_ptr = children[child_index].load(SeqCst, guard);

    if next_ptr.is_null() {
        if !create_intermediate {
            return Shared::null();
        }

        let next_child = Owned::new(Node::default()).into_shared(guard);
        let ret = children[child_index].compare_and_set(
            next_ptr,
            next_child,
            SeqCst,
            guard,
        );
        if ret.is_ok() {
            // CAS worked
            next_ptr = next_child;
        } else {
            // another thread beat us, drop unused created
            // child and use what is already set
            next_ptr = ret.unwrap_err().current;
        }
    }

    traverse(next_ptr, remainder, create_intermediate, guard)
}

#[test]
fn test_split_fanout() {
    assert_eq!(split_fanout(0b11_1111), (0b11_1111, 0));
    assert_eq!(split_fanout(0b111_1111), (0b11_1111, 0b1));
}

#[test]
fn basic_functionality() {
    unsafe {
        let guard = pin();
        let rt = Radix::default();
        rt.insert(0, 5).unwrap();
        let ptr = rt.get(0, &guard).unwrap();
        assert_eq!(ptr.deref(), &5);
        rt.cas(0, ptr, Owned::new(6).into_shared(&guard), &guard)
            .unwrap();
        assert_eq!(rt.get(0, &guard).unwrap().deref(), &6);
        rt.del(0, &guard);
        assert!(rt.get(0, &guard).is_none());

        rt.insert(321, 2).unwrap();
        assert_eq!(rt.get(321, &guard).unwrap().deref(), &2);
        assert!(rt.get(322, &guard).is_none());
        rt.insert(322, 3).unwrap();
        assert_eq!(rt.get(322, &guard).unwrap().deref(), &3);
        assert_eq!(rt.get(321, &guard).unwrap().deref(), &2);
    }
}

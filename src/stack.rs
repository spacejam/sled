#![allow(unsafe_code)]

use std::{
    fmt::{self, Debug},
    ops::Deref,
    sync::atomic::Ordering::{Acquire, Release},
};

use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Shared};

use crate::debug_delay;

/// A node in the lock-free `Stack`.
#[derive(Debug)]
pub struct Node<T: Send + 'static> {
    pub(crate) inner: T,
    pub(crate) next: Atomic<Node<T>>,
}

impl<T: Send + 'static> Drop for Node<T> {
    fn drop(&mut self) {
        unsafe {
            let mut cursor = self.next.load(Acquire, unprotected());

            while !cursor.is_null() {
                // we carefully unset the next pointer here to avoid
                // a stack overflow when freeing long lists.
                let node = cursor.into_owned();
                cursor = node.next.swap(Shared::null(), Acquire, unprotected());
                drop(node);
            }
        }
    }
}

/// A simple lock-free stack, with the ability to atomically
/// append or entirely swap-out entries.
pub struct Stack<T: Send + 'static> {
    head: Atomic<Node<T>>,
}

impl<T: Send + 'static> Default for Stack<T> {
    fn default() -> Self {
        Self { head: Atomic::null() }
    }
}

impl<T: Send + 'static> Drop for Stack<T> {
    fn drop(&mut self) {
        unsafe {
            let curr = self.head.load(Acquire, unprotected());
            if !curr.as_raw().is_null() {
                drop(curr.into_owned());
            }
        }
    }
}

impl<T> Debug for Stack<T>
where
    T: Clone + Debug + Send + 'static + Sync,
{
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> Result<(), fmt::Error> {
        let guard = crossbeam_epoch::pin();
        let head = self.head(&guard);
        let iter = Iter::from_ptr(head, &guard);

        formatter.write_str("Stack [")?;
        let mut written = false;
        for node in iter {
            if written {
                formatter.write_str(", ")?;
            }
            formatter.write_str(&*format!("({:?}) ", &node))?;
            node.fmt(formatter)?;
            written = true;
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

impl<T: Send + 'static> Deref for Node<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T: Send + Sync + 'static> Stack<T> {
    /// Add an item to the stack, spinning until successful.
    pub(crate) fn push(&self, inner: T, guard: &Guard) {
        debug_delay();
        let node = Owned::new(Node { inner, next: Atomic::null() });

        unsafe {
            let node = node.into_shared(guard);

            loop {
                let head = self.head(guard);
                node.deref().next.store(head, Release);
                if self.head.compare_and_set(head, node, Release, guard).is_ok()
                {
                    return;
                }
            }
        }
    }

    /// Clears the stack and returns all items
    pub(crate) fn take_iter<'a>(
        &self,
        guard: &'a Guard,
    ) -> impl Iterator<Item = &'a T> {
        debug_delay();
        let node = self.head.swap(Shared::null(), Release, guard);

        let iter = Iter { inner: node, guard };

        if !node.is_null() {
            unsafe {
                guard.defer_destroy(node);
            }
        }

        iter
    }

    /// Pop the next item off the stack. Returns None if nothing is there.
    #[cfg(any(test, feature = "event_log"))]
    pub(crate) fn pop(&self, guard: &Guard) -> Option<T> {
        use std::ptr;
        use std::sync::atomic::Ordering::SeqCst;
        debug_delay();
        let mut head = self.head(guard);
        loop {
            match unsafe { head.as_ref() } {
                Some(h) => {
                    let next = h.next.load(Acquire, guard);
                    match self.head.compare_and_set(head, next, Release, guard)
                    {
                        Ok(_) => unsafe {
                            // we unset the next pointer before destruction
                            // to avoid double-frees.
                            h.next.store(Shared::default(), SeqCst);
                            guard.defer_destroy(head);
                            return Some(ptr::read(&h.inner));
                        },
                        Err(h) => head = h.current,
                    }
                }
                None => return None,
            }
        }
    }

    /// Returns the current head pointer of the stack, which can
    /// later be used as the key for cas and cap operations.
    pub(crate) fn head<'g>(&self, guard: &'g Guard) -> Shared<'g, Node<T>> {
        self.head.load(Acquire, guard)
    }
}

/// An iterator over nodes in a lock-free stack.
pub struct Iter<'a, T>
where
    T: Send + 'static + Sync,
{
    inner: Shared<'a, Node<T>>,
    guard: &'a Guard,
}

impl<'a, T> Iter<'a, T>
where
    T: 'a + Send + 'static + Sync,
{
    /// Creates a `Iter` from a pointer to one.
    pub(crate) fn from_ptr<'b>(
        ptr: Shared<'b, Node<T>>,
        guard: &'b Guard,
    ) -> Iter<'b, T> {
        Iter { inner: ptr, guard }
    }
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: Send + 'static + Sync,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        debug_delay();
        if self.inner.is_null() {
            None
        } else {
            unsafe {
                let ret = &self.inner.deref().inner;
                self.inner = self.inner.deref().next.load(Acquire, self.guard);
                Some(ret)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut size = 0;
        let mut cursor = self.inner;

        while !cursor.is_null() {
            unsafe {
                cursor = cursor.deref().next.load(Acquire, self.guard);
            }
            size += 1;
        }

        (size, Some(size))
    }
}

#[test]
#[cfg(not(miri))] // can't create threads
fn basic_functionality() {
    use crossbeam_epoch::pin;
    use crossbeam_utils::CachePadded;
    use std::sync::Arc;
    use std::thread;

    let guard = pin();
    let ll = Arc::new(Stack::default());
    assert_eq!(ll.pop(&guard), None);
    ll.push(CachePadded::new(1), &guard);
    let ll2 = Arc::clone(&ll);
    let t = thread::spawn(move || {
        let guard = pin();
        ll2.push(CachePadded::new(2), &guard);
        ll2.push(CachePadded::new(3), &guard);
        ll2.push(CachePadded::new(4), &guard);
        guard.flush();
    });
    t.join().unwrap();
    ll.push(CachePadded::new(5), &guard);
    assert_eq!(ll.pop(&guard), Some(CachePadded::new(5)));
    assert_eq!(ll.pop(&guard), Some(CachePadded::new(4)));
    let ll3 = Arc::clone(&ll);
    let t = thread::spawn(move || {
        let guard = pin();
        assert_eq!(ll3.pop(&guard), Some(CachePadded::new(3)));
        assert_eq!(ll3.pop(&guard), Some(CachePadded::new(2)));
        guard.flush();
    });
    t.join().unwrap();
    assert_eq!(ll.pop(&guard), Some(CachePadded::new(1)));
    let ll4 = Arc::clone(&ll);
    let t = thread::spawn(move || {
        let guard = pin();
        assert_eq!(ll4.pop(&guard), None);
        guard.flush();
    });
    t.join().unwrap();
    drop(ll);
    guard.flush();
    drop(guard);
}

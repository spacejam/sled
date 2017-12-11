// lock-free stack
use std::fmt::{self, Debug};
use std::ptr;
use std::ops::Deref;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};

use epoch::{Atomic, Owned, Shared, Guard, pin, unprotected};

use {debug_delay, test_fail};

#[derive(Debug)]
pub struct Node<T: Send + 'static> {
    inner: T,
    next: Atomic<Node<T>>,
}

impl<T: Send + 'static> Drop for Node<T> {
    fn drop(&mut self) {
        unsafe {
            let next = self.next.load(Relaxed, unprotected()).as_raw();
            if !next.is_null() {
                drop(Box::from_raw(next as *mut Node<T>));
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
    fn default() -> Stack<T> {
        Stack {
            head: Atomic::null(),
        }
    }
}

impl<T: Send + 'static> Drop for Stack<T> {
    fn drop(&mut self) {
        unsafe {
            let curr = self.head.load(Relaxed, unprotected()).as_raw();
            if !curr.is_null() {
                drop(Box::from_raw(curr as *mut Node<T>));
            }
        }
    }
}

impl<T> Debug for Stack<T>
    where T: Debug + Send + 'static + Sync
{
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let guard = pin();
        let head = self.head(&guard);
        let iter = StackIter::from_ptr(head, &guard);

        formatter.write_str("Stack [")?;
        let mut written = false;
        for node in iter {
            if written {
                formatter.write_str(", ")?;
            }
            formatter.write_str(&*format!("({:?}) ", &node as *const _))?;
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

impl<T: Send + 'static> Node<T> {
    pub fn next<'g>(&self, guard: &'g Guard) -> Shared<'g, Node<T>> {
        self.next.load(SeqCst, guard)
    }
}

impl<T: Send + 'static> Stack<T> {
    /// Add an item to the stack, spinning until successful.
    pub fn push(&self, inner: T) {
        debug_delay();
        let node = Owned::new(Node {
            inner: inner,
            next: Atomic::null(),
        });

        unsafe {
            let node = node.into_shared(unprotected());

            loop {
                let head = self.head(unprotected());
                node.deref().next.store(head, SeqCst);
                if self.head
                    .compare_and_set(head, node, SeqCst, unprotected())
                    .is_ok()
                {
                    return;
                }
            }
        }
    }

    /// Pop the next item off the stack. Returns None if nothing is there.
    pub fn pop(&self) -> Option<T> {
        debug_delay();
        let guard = pin();
        let mut head = self.head(&guard);
        loop {
            match unsafe { head.as_ref() } {
                Some(h) => {
                    let next = h.next.load(SeqCst, &guard);
                    match self.head.compare_and_set(
                        head,
                        next,
                        SeqCst,
                        &guard,
                    ) {
                        Ok(_) => unsafe {
                            guard.defer(move || {
                                let head: *mut Node<T> = Box::into_raw(head.into_owned().into());
                                drop(Vec::from_raw_parts(head, 0, 1));
                            });
                            return Some(ptr::read(&h.inner));
                        },
                        Err(h) => head = h.current,
                    }
                }
                None => return None,
            }
        }
    }

    /// compare and push
    pub fn cap<'g>(
        &self,
        old: Shared<Node<T>>,
        new: T,
        guard: &'g Guard,
    ) -> Result<Shared<'g, Node<T>>, Shared<'g, Node<T>>> {
        debug_delay();
        let node = Owned::new(Node {
            inner: new,
            next: Atomic::null(),
        });

        node.next.store(old, SeqCst);

        let node = node.into_shared(guard);

        let res = self.head.compare_and_set(old, node, SeqCst, guard);
        if res.is_err() {
            unsafe {
                node.deref().next.store(Shared::null(), SeqCst);
                guard.defer(move || node.into_owned());
            }
            Err(res.unwrap_err().current)
        } else if test_fail() {
            unimplemented!()
        } else {
            Ok(node)
        }
    }

    /// attempt consolidation
    pub fn cas<'g>(
        &self,
        old: Shared<'g, Node<T>>,
        new: Shared<'g, Node<T>>,
        guard: &'g Guard,
    ) -> Result<Shared<'g, Node<T>>, Shared<'g, Node<T>>> {
        debug_delay();
        let res = self.head.compare_and_set(old, new, SeqCst, guard);
        if res.is_ok() && !test_fail() {
            if !old.is_null() {
                unsafe { guard.defer(move || old.into_owned()) };
            }
            Ok(new)
        } else {
            if !new.is_null() {
                unsafe { guard.defer(move || new.into_owned()) };
            }

            Err(res.unwrap_err().current)
        }
    }

    /// Returns the current head pointer of the stack, which can
    /// later be used as the key for cas and cap operations.
    pub fn head<'g>(&self, guard: &'g Guard) -> Shared<'g, Node<T>> {
        self.head.load(SeqCst, guard)
    }
}

pub struct StackIter<'a, T>
    where T: 'a + Send + 'static + Sync
{
    inner: Shared<'a, Node<T>>,
    guard: &'a Guard,
}

impl<'a, T> StackIter<'a, T>
    where T: 'a + Send + 'static + Sync
{
    pub fn from_ptr<'b>(
        ptr: Shared<'b, Node<T>>,
        guard: &'b Guard,
    ) -> StackIter<'b, T> {
        StackIter {
            inner: ptr,
            guard: guard,
        }
    }
}

impl<'a, T> Iterator for StackIter<'a, T>
    where T: Send + 'static + Sync
{
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        debug_delay();
        if self.inner.is_null() {
            None
        } else {
            unsafe {
                let ret = &self.inner.deref().inner;
                self.inner = self.inner.deref().next.load(SeqCst, self.guard);
                Some(ret)
            }
        }
    }
}

pub fn node_from_frag_vec<T>(from: Vec<T>) -> Owned<Node<T>>
    where T: Send + 'static + Sync
{
    let mut last = None;

    for item in from.into_iter().rev() {
        let node = Owned::new(Node {
            inner: item,
            next: Atomic::null(),
        });

        if let Some(last) = last {
            node.next.store(last, SeqCst);
        }

        last = Some(node);
    }

    last.unwrap()
}

#[test]
fn basic_functionality() {
    use std::thread;
    use std::sync::Arc;

    let ll = Arc::new(Stack::default());
    assert_eq!(ll.pop(), None);
    ll.push(1);
    let ll2 = Arc::clone(&ll);
    let t = thread::spawn(move || {
        ll2.push(2);
        ll2.push(3);
        ll2.push(4);
    });
    t.join().unwrap();
    ll.push(5);
    assert_eq!(ll.pop(), Some(5));
    assert_eq!(ll.pop(), Some(4));
    let ll3 = Arc::clone(&ll);
    let t = thread::spawn(move || {
        assert_eq!(ll3.pop(), Some(3));
        assert_eq!(ll3.pop(), Some(2));
    });
    t.join().unwrap();
    assert_eq!(ll.pop(), Some(1));
    let ll4 = Arc::clone(&ll);
    let t = thread::spawn(move || {
        assert_eq!(ll4.pop(), None);
    });
    t.join().unwrap();
}

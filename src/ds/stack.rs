// lock-free stack
use std::fmt::{self, Debug};
use std::ptr;
use std::ops::Deref;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};

use coco::epoch::{Atomic, Owned, Ptr, Scope, pin, unprotected};

use test_fail;

#[derive(Debug)]
pub struct Node<T: Send + 'static> {
    inner: T,
    next: Atomic<Node<T>>,
}

impl<T: Send + 'static> Drop for Node<T> {
    fn drop(&mut self) {
        unsafe {
            unprotected(|scope| {
                let next = self.next.load(Relaxed, scope).as_raw();
                if !next.is_null() {
                    drop(Box::from_raw(next as *mut Node<T>));
                }
            })
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
            unprotected(|scope| {
                let curr = self.head.load(Relaxed, scope).as_raw();
                if !curr.is_null() {
                    drop(Box::from_raw(curr as *mut Node<T>));
                }
            })
        }
    }
}

impl<T> Debug for Stack<T>
    where T: Debug + Send + 'static + Sync
{
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        pin(|scope| {
            let head = self.head(scope);
            let iter = StackIter::from_ptr(head, scope);

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
        })
    }
}

impl<T: Send + 'static> Deref for Node<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T: Send + 'static> Node<T> {
    pub fn next<'s>(&self, scope: &'s Scope) -> Ptr<'s, Node<T>> {
        self.next.load(SeqCst, scope)
    }
}

impl<T: Send + 'static> Stack<T> {
    /// Add an item to the stack, spinning until successful.
    pub fn push(&self, inner: T) {
        let node = Owned::new(Node {
            inner: inner,
            next: Atomic::null(),
        });

        unsafe {
            unprotected(|scope| {
                let node = node.into_ptr(scope);

                loop {
                    let head = self.head(scope);
                    node.deref().next.store(head, SeqCst);
                    if self.head
                        .compare_and_swap(head, node, SeqCst, scope)
                        .is_ok()
                    {
                        return;
                    }
                }
            })
        }
    }

    /// Pop the next item off the stack. Returns None if nothing is there.
    pub fn pop(&self) -> Option<T> {
        pin(|scope| {
            let mut head = self.head(scope);
            loop {
                match unsafe { head.as_ref() } {
                    Some(h) => {
                        let next = h.next.load(SeqCst, scope);
                        match self.head.compare_and_swap(head, next, SeqCst, scope) {
                            Ok(()) => unsafe {
                                scope.defer_free(head);
                                return Some(ptr::read(&h.inner));
                            },
                            Err(h) => head = h,
                        }
                    }
                    None => return None,
                }
            }
        })
    }

    /// compare and push
    pub fn cap<'s>(
        &self,
        old: Ptr<Node<T>>,
        new: T,
        scope: &'s Scope,
    ) -> Result<Ptr<'s, Node<T>>, Ptr<'s, Node<T>>> {
        let node = Owned::new(Node {
            inner: new,
            next: Atomic::null(),
        });

        node.next.store(old, SeqCst);

        let node = node.into_ptr(scope);

        let res = self.head.compare_and_swap(old, node, SeqCst, scope);
        if res.is_err() {
            unsafe {
                node.deref().next.store(Ptr::null(), SeqCst);
                scope.defer_drop(node);
            }
            Err(res.unwrap_err())
        } else if test_fail() {
            unimplemented!()
        } else {
            Ok(node)
        }
    }

    /// attempt consolidation
    pub fn cas<'s>(
        &self,
        old: Ptr<'s, Node<T>>,
        new: Ptr<'s, Node<T>>,
        scope: &'s Scope,
    ) -> Result<Ptr<'s, Node<T>>, Ptr<'s, Node<T>>> {
        let res = self.head.compare_and_swap(old, new, SeqCst, scope);
        if res.is_ok() && !test_fail() {
            if !old.is_null() {
                unsafe { scope.defer_drop(old) };
            }
            Ok(new)
        } else {
            if !new.is_null() {
                unsafe { scope.defer_drop(new) };
            }

            Err(res.unwrap_err())
        }
    }

    /// Returns the current head pointer of the stack, which can
    /// later be used as the key for cas and cap operations.
    pub fn head<'s>(&self, scope: &'s Scope) -> Ptr<'s, Node<T>> {
        self.head.load(SeqCst, scope)
    }
}

pub struct StackIter<'a, T>
    where T: 'a + Send + 'static + Sync
{
    inner: Ptr<'a, Node<T>>,
    scope: &'a Scope,
}

impl<'a, T> StackIter<'a, T>
    where T: 'a + Send + 'static + Sync
{
    pub fn from_ptr<'b>(ptr: Ptr<'b, Node<T>>, scope: &'b Scope) -> StackIter<'b, T> {
        StackIter {
            inner: ptr,
            scope: scope,
        }
    }
}

impl<'a, T> Iterator for StackIter<'a, T>
    where T: Send + 'static + Sync
{
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.inner.is_null() {
            None
        } else {
            unsafe {
                let ret = &self.inner.deref().inner;
                self.inner = self.inner.deref().next.load(SeqCst, self.scope);
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
            node.next.store_owned(last, SeqCst);
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

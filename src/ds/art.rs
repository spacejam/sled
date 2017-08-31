/// An adaptive radix tree.
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::*;

use coco::epoch::{Atomic, Owned, Ptr, Scope, pin, unprotected};

use super::*;

const FANFACTOR: usize = 8;
const FANOUT: usize = 1 << FANFACTOR;
const FAN_MASK: usize = FANOUT - 1;

#[inline(always)]
fn split_fanout(i: usize) -> (u8, usize) {
    let rem = i >> FANFACTOR;
    let first = i & FAN_MASK;
    (first as u8, rem)
}

struct Node<T> {
    inner: Atomic<T>,
    children: Atomic<(Vec<Atomic<AtomicUsize>>, Vec<Atomic<Node<T>>>)>,
}

impl<T> Default for Node<T> {
    fn default() -> Node<T> {
        Node {
            inner: Atomic::null(),
            children: Atomic::new((
                rep_no_copy!(Atomic::new(AtomicUsize::new(0)); 1),
                rep_no_copy!(Atomic::null(); 1),
            )),
        }
    }
}

impl<T> Node<T> {
    fn upgrade(&self, scope: &Scope) {
        let children = self.children.load(Acquire, scope);
        let &(keys, ptrs) = children.deref();

        assert_eq!(keys.len(), ptrs.len());

        let next_len = match keys.len() {
            l if l == 1 => 4,
            l if l == 4 => 16,
            l if l == 16 => 48,
            l if l == 48 => 256,
            l => panic!("upgrade called on node with length {}", l),
        };

        let mut keys_clone = keys.clone();
        let mut ptrs_clone = ptrs.clone();


    }

    fn insert(&self, key: usize, val: T) -> Option<T> {
        pin(|scope| {})
    }

    fn claim(&self, key: u8, scope: &Scope) -> usize {
        loop {
            // try to claim spot in keys vec
            let children = self.children.load(Acquire, scope);
            let (keys, _ptrs) = *children;

            let predicate = |&&k| k.compare_and_swap(0, key as usize, SeqCst) == 0;

            if let Some(idx) = keys.position(predicate) {
                // claimed!
                return idx;
            }

            // if keys vec is full, try to upgrade
            self.upgrade(scope);
        }
    }
}

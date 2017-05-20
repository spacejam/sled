/// An in-memory b-link tree.

extern crate rand;

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

#[cfg(test)]
fn test_fail() -> bool {
    use rand::Rng;
    rand::thread_rng().gen::<bool>();
    // TODO when the time is right, return the gen'd bool
    false
}

#[cfg(not(test))]
#[inline(always)]
fn test_fail() -> bool {
    false
}

mod tree;
mod bound;
mod radix;
mod stack;
mod page;

use bound::Bound;
use radix::Radix;
use stack::{Stack, node_from_frag_vec};
use page::{Frag, Pages, FragView, SeekRes};

pub use tree::Tree;

type PageID = usize;
type Value = Vec<u8>;
type Key = Vec<u8>;
type Raw = *const stack::Node<*const page::Frag>;

#[inline(always)]
fn raw<T>(t: T) -> *const T {
    Box::into_raw(Box::new(t)) as *const T
}

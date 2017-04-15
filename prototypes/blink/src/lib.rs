/// An in-memory b-link tree.

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

mod tree;
mod bound;
mod radix;
mod stack;
mod page;

use bound::Bound;
use radix::Radix;
use stack::Stack;
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

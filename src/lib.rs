extern crate libc;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate time;
extern crate rand;
extern crate env_logger;
#[macro_use]
extern crate log as logger;

// atomic lock-free tree
pub use tree::Tree;
// lock-free pagecache
pub use page::{Materializer, PageCache};
// lock-free log-structured storage
pub use log::{HEADER_LEN, LockFreeLog, Log};
// lock-free stack
use stack::Stack;
// lock-free radix tree
pub use radix::Radix;
// general-purpose configuration
pub use config::Config;

use crc16::crc16_arr;

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
mod log;
mod crc16;
mod stack;
mod page;
mod radix;
mod config;

mod ops;

use bound::Bound;
use stack::{StackIter, node_from_frag_vec};
use tree::Frag;

type LogID = u64; // LogID == file position to simplify file mapping
type PageID = usize;

type Key = Vec<u8>;
type Value = Vec<u8>;

#[inline(always)]
fn raw<T>(t: T) -> *const T {
    Box::into_raw(Box::new(t)) as *const T
}

// get thread identifier
#[inline(always)]
fn tn() -> String {
    use std::thread;
    thread::current()
        .name()
        .unwrap_or("unknown")
        .to_owned()
}

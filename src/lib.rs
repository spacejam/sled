//! `rsdb` is a flash-sympathetic persistent lock-free B+ tree, pagecache, and log.
//!
//! ```
//! extern crate rsdb;
//!
//! let t = rsdb::Config::default().tree();
//!
//! t.set(b"yo!".to_vec(), b"v1".to_vec());
//! t.get(b"yo!");
//! t.cas(b"yo!".to_vec(), Some(b"v1".to_vec()), Some(b"v2".to_vec()));
//!
//! let mut iter = t.scan(b"a non-present key before yo!");
//! assert_eq!(iter.next(), Some((b"yo!".to_vec(), b"v2".to_vec())));
//! assert_eq!(iter.next(), None);
//!
//! t.del(b"yo!");
//! ```

#![deny(missing_docs)]
#![cfg_attr(test, deny(warnings))]
#![cfg_attr(test, feature(plugin))]
#![cfg_attr(test, plugin(clippy))]
#![cfg_attr(test, allow(inline_always))]

extern crate libc;
extern crate rayon;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate rand;
#[macro_use]
extern crate log as logger;
extern crate env_logger;
extern crate tempfile;
extern crate zstd;
extern crate time;

/// atomic lock-free tree
pub use tree::Tree;
/// lock-free pagecache
pub use page::{Materializer, PageCache};
/// lock-free log-structured storage
pub use log::{HEADER_LEN, LockFreeLog, Log};
/// lock-free stack
use stack::Stack;
/// lock-free radix tree
pub use radix::Radix;
/// general-purpose configuration
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
mod thread_cache;

mod ops;

use bound::Bound;
use page::CacheEntry;
use stack::{StackIter, node_from_frag_vec};
use thread_cache::ThreadCache;

type LogID = u64; // LogID == file position to simplify file mapping
type PageID = usize;

type Key = Vec<u8>;
type KeyRef<'a> = &'a [u8];
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

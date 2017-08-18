//! `rsdb` is a flash-sympathetic persistent lock-free B+ tree, pagecache, and log.
//!
//! # Tree
//!
//! ```
//! let t = rsdb::Config::default().tree();
//! t.set(b"yo!".to_vec(), b"v1".to_vec());
//! assert_eq!(t.get(b"yo!"), Some(b"v1".to_vec()));
//! t.cas(b"yo!".to_vec(), Some(b"v1".to_vec()), Some(b"v2".to_vec())).unwrap();
//! let mut iter = t.scan(b"a non-present key before yo!");
//! assert_eq!(iter.next(), Some((b"yo!".to_vec(), b"v2".to_vec())));
//! assert_eq!(iter.next(), None);
//! t.del(b"yo!");
//! ```
//!
//! # Working with the `PageCache`
//!
//! ```
//! extern crate rsdb;
//!
//! use rsdb::Materializer;
//!
//! pub struct TestMaterializer;
//!
//! impl Materializer for TestMaterializer {
//!     type MaterializedPage = String;
//!     type PartialPage = String;
//!     type Recovery = ();
//!
//!     fn materialize(&self, frags: &[String]) -> String {
//!         self.consolidate(frags).pop().unwrap()
//!     }
//!
//!     fn consolidate(&self, frags: &[String]) -> Vec<String> {
//!         let mut consolidated = String::new();
//!         for frag in frags.into_iter() {
//!             consolidated.push_str(&*frag);
//!         }
//!
//!         vec![consolidated]
//!     }
//!
//!     fn recover(&self, _: &String) -> Option<()> {
//!         None
//!     }
//! }
//!
//! fn main() {
//!     let path = "test_pagecache_doc.log";
//!     let conf = rsdb::Config::default().path(Some(path.to_owned()));
//!     let pc = rsdb::PageCache::new(TestMaterializer, conf.clone());
//!     let (id, key) = pc.allocate();
//!
//!     // The first item in a page should be set using replace, which
//!     // signals that this is the beginning of a new page history, and
//!     // that any previous items associated with this page should be
//!     // forgotten.
//!     let key = pc.replace(id, key, vec!["a".to_owned()]).unwrap();
//!     let key = pc.prepend(id, key, "b".to_owned()).unwrap();
//!     let _key = pc.prepend(id, key, "c".to_owned()).unwrap();
//!
//!     let (consolidated, _key) = pc.get(id).unwrap();
//!
//!     assert_eq!(consolidated, "abc".to_owned());
//!
//!     drop(pc);
//!     std::fs::remove_file(path).unwrap();
//! }
//! ```
//!
//! # Working with `Log`
//!
//! ```
//! use rsdb::Log;
//!
//! let log = rsdb::Config::default().log();
//! let first_offset = log.write(b"1".to_vec());
//! log.write(b"22".to_vec());
//! log.write(b"333".to_vec());
//!
//! // stick an abort in the middle, which should not be returned
//! let res = log.reserve(b"never_gonna_hit_disk".to_vec());
//! res.abort();
//!
//! log.write(b"4444".to_vec());
//! let last_offset = log.write(b"55555".to_vec());
//! log.make_stable(last_offset);
//! let mut iter = log.iter_from(first_offset);
//! assert_eq!(iter.next().unwrap().1, b"1".to_vec());
//! assert_eq!(iter.next().unwrap().1, b"22".to_vec());
//! assert_eq!(iter.next().unwrap().1, b"333".to_vec());
//! assert_eq!(iter.next().unwrap().1, b"4444".to_vec());
//! assert_eq!(iter.next().unwrap().1, b"55555".to_vec());
//! assert_eq!(iter.next(), None);
//! ```


#![deny(missing_docs)]
#![cfg_attr(test, deny(warnings))]
#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]
#![cfg_attr(feature="clippy", allow(inline_always))]

extern crate libc;
extern crate rayon;
extern crate crossbeam;
extern crate coco;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate rand;
#[macro_use]
extern crate log as _log;
extern crate tempfile;
extern crate time;
extern crate glob;
#[cfg(feature = "zstd")]
extern crate zstd;

/// atomic lock-free tree
pub use tree::Tree;
/// lock-free pagecache
pub use page::{Materializer, PageCache};
/// lock-free log-structured storage
pub use log::{HEADER_LEN, LockFreeLog, Log, LogRead};
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
mod crc64;
mod stack;
mod page;
mod radix;
mod config;
mod thread_cache;

use bound::Bound;
use page::CasKey;
use stack::{StackIter, node_from_frag_vec};
use thread_cache::ThreadCache;

type LogID = u64; // LogID == file position to simplify file mapping
type PageID = usize;

type Key = Vec<u8>;
type KeyRef<'a> = &'a [u8];
type Value = Vec<u8>;

// get thread identifier
#[inline(always)]
fn tn() -> String {
    use std::thread;
    thread::current().name().unwrap_or("unknown").to_owned()
}

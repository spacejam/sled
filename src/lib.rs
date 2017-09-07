//! `sled` is a flash-sympathetic persistent lock-free B+ tree, pagecache, and log.
//!
//! ```
//! let t = sled::Config::default().tree();
//! t.set(b"yo!".to_vec(), b"v1".to_vec());
//! assert_eq!(t.get(b"yo!"), Some(b"v1".to_vec()));
//! t.cas(b"yo!".to_vec(), Some(b"v1".to_vec()), Some(b"v2".to_vec())).unwrap();
//! let mut iter = t.scan(b"a non-present key before yo!");
//! assert_eq!(iter.next(), Some((b"yo!".to_vec(), b"v2".to_vec())));
//! assert_eq!(iter.next(), None);
//! t.del(b"yo!");
//! ```

#![deny(missing_docs)]
#![cfg_attr(test, deny(warnings))]
#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]
#![cfg_attr(feature="clippy", allow(inline_always))]

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate crossbeam;
extern crate coco;
extern crate bincode;
extern crate historian;
#[macro_use]
extern crate lazy_static;
#[cfg(feature = "log")]
#[macro_use]
extern crate log as _log;
extern crate libc;
#[cfg(feature = "rayon")]
extern crate rayon;
#[cfg(feature = "zstd")]
extern crate zstd;
#[cfg(test)]
extern crate rand;

/// atomic lock-free tree
pub use tree::{Tree, TreeIter};
/// lock-free pagecache
pub use page::{CasKey, Materializer, PageCache};
/// lock-free log-structured storage
pub use log::{HEADER_LEN, LockFreeLog, Log, LogRead};
pub use ds::{Radix, Stack};
/// general-purpose configuration
pub use config::Config;

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
mod log;
mod page;
mod config;
mod thread_cache;
mod hash;
mod ds;
mod metrics;

use metrics::Metrics;
use ds::{Lru, StackIter, node_from_frag_vec};
use hash::{crc16_arr, crc64};
use thread_cache::ThreadCache;

type LogID = u64; // LogID == file position to simplify file mapping
type PageID = usize;

type Key = Vec<u8>;
type KeyRef<'a> = &'a [u8];
type Value = Vec<u8>;

lazy_static! {
    /// A metric collector for all sled instances running in this
    /// process.
    pub static ref M: Metrics = Metrics::default();
}

// get thread identifier
#[inline(always)]
fn tn() -> String {
    use std::thread;
    thread::current().name().unwrap_or("unknown").to_owned()
}

fn clock() -> f64 {
    let u = uptime();
    (u.as_secs() * 1_000_000_000) as f64 + u.subsec_nanos() as f64
}

// not correct, since it starts counting at the first observance...
fn uptime() -> std::time::Duration {
    lazy_static! {
        static ref START: std::time::Instant = std::time::Instant::now();
    }

    START.elapsed()
}

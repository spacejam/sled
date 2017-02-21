#![allow(dead_code)]
extern crate libc;
extern crate rustc_serialize;
extern crate bincode;

// transactional kv with multi-key ops
pub use tx::TxStore;
// atomic lock-free tree
pub use tree::Tree;
// lock-free pagecache
pub use page::PageCache;
// lock-free log-structured storage
pub use log::Log;
// lock-free stack
pub use stack::Stack;
// lock-free radix tree
pub use radix::Radix;

use crc16::crc16_arr;
use map::CASMap;

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

mod tx;
mod tree;
mod log;
mod crc16;
mod stack;
mod map;
mod page;
mod rsdb;
mod radix;

pub mod ops;

// ID types
type PageID = u64;
type LogID = u64; // LogID == file position to simplify file mapping
type TxID = u64;
type Epoch = u64;

type Key = Vec<u8>;
type Value = Vec<u8>;
struct Tx;

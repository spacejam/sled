#![allow(dead_code)]
extern crate libc;
extern crate rustc_serialize;
extern crate bincode;

pub use log::Log;
pub use page::PT;
pub use stack::Stack;
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

mod log;
mod crc16;
mod stack;
mod map;
mod page;
mod rsdb;
mod radix;

pub mod ops;

type PageID = u64;
type LogID = u64; // LogID == file position to simplify file mapping
type TxID = u64;
type Epoch = u64;

const N_TX_THREADS: usize = 8;

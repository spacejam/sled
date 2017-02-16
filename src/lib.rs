extern crate libc;
extern crate rustc_serialize;
extern crate bincode;

use std::collections::HashMap;

mod log;
mod map;
mod page;
mod rsdb;
mod tree;
mod tx;

pub use rsdb::RSDB;

use map::CASMap;

type PageID = u64;
type LogID = u64; // LogID == position to simplify file mapping
type TxID = u64;
type Epoch = u64;

#[derive(Debug, Clone, Eq, PartialEq, RustcDecodable, RustcEncodable)]
#[repr(C)]
pub struct KV {
    k: Vec<u8>,
    v: Vec<u8>,
}

#[derive(Debug, Clone, Eq, PartialEq, RustcDecodable, RustcEncodable)]
#[repr(C)]
pub enum LogDelta {
    KV(KV),
    Merge {
        left: PageID,
        right: PageID,
    },
    Split {
        left: PageID,
        right: PageID,
    },
    FailedFlush, // on-disk only
}

#[derive(Debug, Clone, Eq, PartialEq, RustcDecodable, RustcEncodable)]
#[repr(C)]
pub enum Delta {
    KV(KV),
    Merge {
        left: PageID,
        right: PageID,
    },
    Split {
        left: PageID,
        right: PageID,
    },
    TxBegin(TxID), // in-mem
    TxCommit(TxID), // in-mem
    TxAbort(TxID), // in-mem
    Flush {
        pid: PageID,
        annotation: Annotation,
    }, // in-mem
    PartialSwap(LogID), /* indicates part of page has been swapped out,
                         * shows where to find it */
}

#[derive(Debug, Clone, Eq, PartialEq, RustcDecodable, RustcEncodable)]
#[repr(C)]
pub struct Page;

#[derive(Debug, Clone, Eq, PartialEq, RustcDecodable, RustcEncodable)]
#[repr(C)]
pub enum Data {
    Page(Page),
    Delta(Delta),
}

#[derive(Debug, Clone, Eq, PartialEq, RustcDecodable, RustcEncodable)]
#[repr(C)]
pub struct PD {
    data: Data,
    lid: LogID,
    pid: PageID,
}

#[derive(Debug, Clone, Eq, PartialEq, RustcDecodable, RustcEncodable)]
#[repr(C)]
pub struct LogPage;

#[derive(Debug, Clone, Eq, PartialEq, RustcDecodable, RustcEncodable)]
#[repr(C)]
pub struct Annotation;

#[derive(Debug, Clone, Eq, PartialEq, RustcDecodable, RustcEncodable)]
#[repr(C)]
pub enum LogData {
    Full(LogPage),
    Deltas(Vec<LogDelta>),
}

mod ops {
    use bincode::SizeLimit;
    use bincode::rustc_serialize::{encode, decode, DecodingResult};
    use rustc_serialize::{Encodable, Decodable};

    pub fn is_sealed(v: u32) -> bool {
        v >> 31 == 1
    }

    pub fn mk_sealed(v: u32) -> u32 {
        v | 1 << 31
    }

    pub fn n_writers(v: u32) -> u32 {
        v << 1 >> 25
    }

    pub fn incr_writers(v: u32) -> u32 {
        assert!(n_writers(v) != 127);
        v + (1 << 24)
    }

    pub fn decr_writers(v: u32) -> u32 {
        assert!(n_writers(v) != 0);
        v - (1 << 24)
    }

    pub fn offset(v: u32) -> u32 {
        v << 8 >> 8
    }

    pub fn bump_offset(v: u32, by: u32) -> u32 {
        assert!(by >> 24 == 0);
        v + by
    }

    pub fn to_binary<T: Encodable>(s: &T) -> Vec<u8> {
        encode(s, SizeLimit::Infinite).unwrap()
    }

    pub fn to_framed_binary<T: Encodable>(s: &T) -> Vec<u8> {
        let mut bytes = to_binary(s);
        let mut size = usize_to_array(bytes.len()).to_vec();
        let mut ret = Vec::with_capacity(bytes.len() + 4);
        ret.append(&mut size);
        ret.append(&mut bytes);
        ret
    }

    pub fn from_binary<T: Decodable>(encoded: Vec<u8>) -> DecodingResult<T> {
        decode(&encoded[..])
    }

    pub fn usize_to_array(u: usize) -> [u8; 4] {
        [(u >> 24) as u8, (u >> 16) as u8, (u >> 8) as u8, u as u8]
    }

    pub fn array_to_usize(ip: [u8; 4]) -> usize {
        ((ip[0] as usize) << 24) as usize + ((ip[1] as usize) << 16) as usize +
        ((ip[2] as usize) << 8) as usize + (ip[3] as usize)
    }
}

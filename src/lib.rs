#![allow(dead_code)]
extern crate libc;
extern crate rustc_serialize;
extern crate bincode;

pub use log::Log;
pub use page::PageCache;
pub use stack::Stack;

use crc16::crc16_arr;
use map::CASMap;

mod log;
mod crc16;
mod stack;
mod map;
mod page;
mod rsdb;
mod radix;

pub mod ops;

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

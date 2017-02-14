use std::io;

use super::*;

type RetVal = io::Result<Option<Vec<u8>>>;
type MultiRetVal = io::Result<Vec<Option<Vec<u8>>>>;

pub trait Tree {
    fn get(&self, k: Vec<u8>) -> RetVal;
    fn set(&self, k: Vec<u8>, v: Vec<u8>) -> RetVal;
    fn del(&self, k: Vec<u8>) -> RetVal;
    fn mget(&self, ks: Vec<Vec<u8>>) -> MultiRetVal;
}

#[derive(Debug, Clone, RustcDecodable, RustcEncodable)]
#[repr(C)]
struct TreePage {
    id: PageID,
    low_key: Vec<u8>,
    high_key: Vec<u8>,
    next: Option<PageID>,
    prev: Option<PageID>,
    children: Children,
}


#[derive(Debug, Clone, RustcDecodable, RustcEncodable)]
#[repr(C)]
enum Children {
    Data {
        kvs: Vec<KV>,
    },
    Index {
        seps: Vec<Vec<u8>>,
        ptrs: Vec<PageID>,
    },
}

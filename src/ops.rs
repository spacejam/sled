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

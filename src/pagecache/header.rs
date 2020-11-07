use super::*;

// This is the most writers in a single IO buffer
// that we have space to accommodate in the counter
// for writers in the IO buffer header.
pub(in crate::pagecache) const MAX_WRITERS: Header = 127;

pub(in crate::pagecache) type Header = u64;

// salt: 31 bits
// maxed: 1 bit
// seal: 1 bit
// n_writers: 7 bits
// offset: 24 bits

pub(crate) const fn is_maxed(v: Header) -> bool {
    v & (1 << 32) == 1 << 32
}

pub(crate) const fn mk_maxed(v: Header) -> Header {
    v | (1 << 32)
}

pub(crate) const fn is_sealed(v: Header) -> bool {
    v & (1 << 31) == 1 << 31
}

pub(crate) const fn mk_sealed(v: Header) -> Header {
    v | (1 << 31)
}

pub(crate) const fn n_writers(v: Header) -> Header {
    (v << 33) >> 57
}

#[inline]
pub(crate) fn incr_writers(v: Header) -> Header {
    assert_ne!(n_writers(v), MAX_WRITERS);
    v + (1 << 24)
}

#[inline]
pub(crate) fn decr_writers(v: Header) -> Header {
    assert_ne!(n_writers(v), 0);
    v - (1 << 24)
}

#[inline]
pub(crate) fn offset(v: Header) -> usize {
    let ret = (v << 40) >> 40;
    usize::try_from(ret).unwrap()
}

#[inline]
pub(crate) fn bump_offset(v: Header, by: usize) -> Header {
    assert_eq!(by >> 24, 0);
    v + (by as Header)
}

pub(crate) const fn bump_salt(v: Header) -> Header {
    (v + (1 << 33)) & 0xFFFF_FFFD_0000_0000
}

pub(crate) const fn salt(v: Header) -> Header {
    (v >> 33) << 33
}

#![allow(unsafe_code)]
#![allow(unused)]

use std::{
    alloc::{alloc, dealloc, Layout},
    convert::TryFrom,
    mem::size_of,
    num::NonZeroU64,
};

const ACTUAL_HEADER_SZ: usize = 21;
const ALIGNMENT: usize = 8;

fn aligned_boxed_slice(size: usize) -> Box<[u8]> {
    let size = size.max(size_of::<Header>());
    let layout = Layout::from_size_align(size, ALIGNMENT).unwrap();

    unsafe {
        let ptr = alloc(layout);
        let fat_ptr = fatten(ptr, size);
        let ret = Box::from_raw(fat_ptr);
        assert_eq!(ret.len(), size);
        ret
    }
}

/// <https://users.rust-lang.org/t/construct-fat-pointer-to-struct/29198/9>
#[allow(trivial_casts)]
#[inline]
fn fatten(data: *const u8, len: usize) -> *mut [u8] {
    // Requirements of slice::from_raw_parts.
    assert!(!data.is_null());
    assert!(isize::try_from(len).is_ok());

    let slice = unsafe { core::slice::from_raw_parts(data as *const (), len) };
    slice as *const [()] as *mut _
}

#[repr(C)]
#[derive(Debug)]
struct Header {
    // NB always lay out fields from largest to smallest
    // to properly pack the struct
    pub next: Option<NonZeroU64>,
    pub merging_child: Option<NonZeroU64>,
    pub children: u16,
    pub prefix_len: u8,
    pub merging: bool,
    pub is_index: bool,
}

pub(crate) struct InlineRecords(Box<[u8]>);

impl InlineRecords {
    fn new() -> InlineRecords {
        let mut boxed_slice = aligned_boxed_slice(ACTUAL_HEADER_SZ);
        boxed_slice[..size_of::<Header>()]
            .copy_from_slice(&[0; size_of::<Header>()]);
        InlineRecords(boxed_slice)
    }

    fn header(&mut self) -> &Header {
        unsafe { &*(self.0.as_ptr() as *mut Header) }
    }

    fn header_mut(&mut self) -> &mut Header {
        unsafe { &mut *(self.0.as_ptr() as *mut Header) }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple() {
        let mut ir = InlineRecords::new();
        let header: &mut Header = ir.header_mut();
        header.next = Some(NonZeroU64::new(5).unwrap());
        header.is_index = true;
        dbg!(header);
        println!("ir: {:?}", ir.0);
    }
}

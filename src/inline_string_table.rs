#![allow(unsafe_code)]
#![allow(unused)]

use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    convert::{TryFrom, TryInto},
    mem::{align_of, size_of},
    num::NonZeroU64,
    ops::{Deref, DerefMut, Index, IndexMut},
};

const ALIGNMENT: usize = align_of::<Header>();

// allocates space for a header struct at the beginning.
fn aligned_boxed_slice(size: usize) -> Box<[u8]> {
    let size = size + size_of::<Header>();
    let layout = Layout::from_size_align(size, ALIGNMENT).unwrap();

    unsafe {
        let ptr = alloc_zeroed(layout);
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
#[derive(Debug, Clone, Copy)]
pub(crate) struct Header {
    // NB always lay out fields from largest to smallest
    // to properly pack the struct
    pub next: Option<NonZeroU64>,
    pub merging_child: Option<NonZeroU64>,
    //fixed_key_length: Option<NonZeroU64>,
    pub children: u16,
    pub prefix_len: u8,
    pub merging: bool,
    pub is_index: bool,
}

pub(crate) struct InlineRecords(Box<[u8]>);

impl Deref for InlineRecords {
    type Target = Header;

    fn deref(&self) -> &Header {
        self.header()
    }
}

impl DerefMut for InlineRecords {
    fn deref_mut(&mut self) -> &mut Header {
        self.header_mut()
    }
}

impl Index<usize> for InlineRecords {
    type Output = [u8];

    // returns the bytes for an item's length + the item itself +
    fn index(&self, idx: usize) -> &[u8] {
        assert!(idx <= usize::from(self.children));
        let offsets_start = size_of::<Header>();
        let u64_size = size_of::<u64>();
        let offsets_end =
            offsets_start + (usize::from(self.children) * u64_size);
        let offsets = &self.0[offsets_start..offsets_end];
        let offset = &offsets[u64_size * idx..u64_size * (idx + 1)];

        let offset_slice = &offset[..u64_size];

        let offset =
            u64::from_le_bytes(offset_slice.try_into().unwrap()) as usize;

        let length_key_and_value = &self.0[offsets_end + offset..];

        let length_buf = &length_key_and_value[..u64_size];
        let length =
            u64::from_le_bytes(length_buf.try_into().unwrap()) as usize;

        let length =
            u64::from_le_bytes(offset_slice.try_into().unwrap()) as usize;

        let start = offset + u64_size;
        let end = start + length + u64_size;

        &length_key_and_value[start..end]
    }
}

impl InlineRecords {
    fn new(
        hi: &[u8],
        lo: &[u8],
        prefix_len: u8,
        items: &[(&[u8], u64)],
    ) -> InlineRecords {
        let offsets_and_lengths_size =
            (2 * size_of::<u64>()) * (2 + items.len());
        let keys_and_values_size =
            items.iter().map(|(k, _pid)| k.len()).sum::<usize>()
                + size_of::<u64>() * (2 + items.len());

        let boxed_slice = aligned_boxed_slice(
            offsets_and_lengths_size + keys_and_values_size,
        );

        let mut ret = InlineRecords(boxed_slice);
        ret.next = None;
        ret.merging_child = None;
        ret.children = u16::try_from(items.len()).unwrap();
        ret.prefix_len = prefix_len;
        ret.merging = false;
        ret.is_index = true;

        let data_buf = &mut ret.0[size_of::<Header>()..];
        let (offsets, lengths_keys_and_values) =
            data_buf.split_at_mut(offsets_and_lengths_size);

        let bounds = [(lo, 0), (hi, 0)];
        let iter = bounds.iter().chain(items.into_iter());

        let u64_size = size_of::<u64>();

        let mut kv_cursor = 0_usize;
        for (idx, (key, pid)) in iter.enumerate() {
            let offsets_cursor = idx * u64_size;
            offsets[offsets_cursor..offsets_cursor + u64_size]
                .copy_from_slice(&kv_cursor.to_le_bytes());

            lengths_keys_and_values[kv_cursor..kv_cursor + u64_size]
                .copy_from_slice(&(key.len() as u64).to_le_bytes());
            kv_cursor += u64_size;

            lengths_keys_and_values[kv_cursor..kv_cursor + key.len()]
                .copy_from_slice(key);
            kv_cursor += key.len();

            lengths_keys_and_values[kv_cursor..kv_cursor + u64_size]
                .copy_from_slice(&pid.to_le_bytes());
            kv_cursor += u64_size;
        }

        ret
    }

    fn header(&self) -> &Header {
        unsafe { &*(self.0.as_ptr() as *mut Header) }
    }

    fn header_mut(&mut self) -> &mut Header {
        unsafe { &mut *(self.0.as_ptr() as *mut Header) }
    }

    fn len(&self) -> usize {
        usize::from(self.children)
    }

    fn find(&self, key: &[u8]) -> Result<usize, usize> {
        todo!()
    }

    fn insert(&mut self, key: &[u8], pid: u64) {
        match self.find(&key[usize::from(self.prefix_len)..]) {
            Ok(offset) => {
                if self.is_index {
                    panic!("already contained key being merged into index");
                }
                todo!()
            }
            Err(prospective_offset) => {
                todo!()
            }
        }
    }

    fn get_lub(&self, key: &[u8]) -> u64 {
        assert!(key >= self.lo());
        todo!()
    }

    fn lo(&self) -> &[u8] {
        let ret = &self[0];
        &ret[..ret.len() - size_of::<u64>()]
    }

    fn hi(&self) -> &[u8] {
        let ret = &self[1];
        &ret[..ret.len() - size_of::<u64>()]
    }
}

fn varint_size(int: u64) -> u64 {
    if int <= 240 {
        1
    } else if int <= 2287 {
        2
    } else if int <= 67823 {
        3
    } else if int <= 0x00FF_FFFF {
        4
    } else if int <= 0xFFFF_FFFF {
        5
    } else if int <= 0x00FF_FFFF_FFFF {
        6
    } else if int <= 0xFFFF_FFFF_FFFF {
        7
    } else if int <= 0x00FF_FFFF_FFFF_FFFF {
        8
    } else {
        9
    }
}

fn serialize_varint_into(int: u64, buf: &mut [u8]) {
    if int <= 240 {
        buf[0] = u8::try_from(int).unwrap();
    } else if int <= 2287 {
        buf[0] = u8::try_from((int - 240) / 256 + 241).unwrap();
        buf[1] = u8::try_from((int - 240) % 256).unwrap();
    } else if int <= 67823 {
        buf[0] = 249;
        buf[1] = u8::try_from((int - 2288) / 256).unwrap();
        buf[2] = u8::try_from((int - 2288) % 256).unwrap();
    } else if int <= 0x00FF_FFFF {
        buf[0] = 250;
        let bytes = int.to_le_bytes();
        buf[1..4].copy_from_slice(&bytes[..3]);
    } else if int <= 0xFFFF_FFFF {
        buf[0] = 251;
        let bytes = int.to_le_bytes();
        buf[1..5].copy_from_slice(&bytes[..4]);
    } else if int <= 0x00FF_FFFF_FFFF {
        buf[0] = 252;
        let bytes = int.to_le_bytes();
        buf[1..6].copy_from_slice(&bytes[..5]);
    } else if int <= 0xFFFF_FFFF_FFFF {
        buf[0] = 253;
        let bytes = int.to_le_bytes();
        buf[1..7].copy_from_slice(&bytes[..6]);
    } else if int <= 0x00FF_FFFF_FFFF_FFFF {
        buf[0] = 254;
        let bytes = int.to_le_bytes();
        buf[1..8].copy_from_slice(&bytes[..7]);
    } else {
        buf[0] = 255;
        let bytes = int.to_le_bytes();
        buf[1..9].copy_from_slice(&bytes[..8]);
    };
}

fn deserialize_varint(buf: &[u8]) -> crate::Result<u64> {
    if buf.is_empty() {
        return Err(crate::Error::corruption(None));
    }
    let res = match buf[0] {
        0..=240 => u64::from(buf[0]),
        241..=248 => 240 + 256 * (u64::from(buf[0]) - 241) + u64::from(buf[1]),
        249 => 2288 + 256 * u64::from(buf[1]) + u64::from(buf[2]),
        other => {
            let sz = other as usize - 247;
            let mut aligned = [0; 8];
            aligned[..sz].copy_from_slice(&buf[1..=sz]);
            u64::from_le_bytes(aligned)
        }
    };
    Ok(res)
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

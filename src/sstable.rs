#![allow(unsafe_code)]
#![allow(unused)]

use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    cmp::Ordering::{Equal, Greater, Less},
    convert::{TryFrom, TryInto},
    fmt,
    mem::{align_of, size_of, ManuallyDrop},
    num::NonZeroU64,
    ops::{Deref, DerefMut, Index, IndexMut},
};

const ALIGNMENT: usize = align_of::<Header>();
const U64_SZ: usize = size_of::<u64>();

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
    lo_len: u64,
    hi_len: u64,
    //fixed_key_length: Option<NonZeroU64>,
    pub children: u16,
    pub prefix_len: u8,
    pub merging: bool,
    pub is_index: bool,
}

/// An immutable sorted string table
pub(crate) struct SSTable(ManuallyDrop<Box<[u8]>>);

impl Drop for SSTable {
    fn drop(&mut self) {
        let box_ptr = self.0.as_mut_ptr();
        let layout = Layout::from_size_align(self.0.len(), ALIGNMENT).unwrap();
        unsafe {
            dealloc(box_ptr, layout);
        }
    }
}

impl Deref for SSTable {
    type Target = Header;

    fn deref(&self) -> &Header {
        self.header()
    }
}

impl fmt::Debug for SSTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SSTable")
            .field("header", self.header())
            .field("lo", &self.lo())
            .field("hi", &self.hi())
            .field("items", &self.iter().collect::<crate::Map<_, _>>())
            .finish()
    }
}

impl DerefMut for SSTable {
    fn deref_mut(&mut self) -> &mut Header {
        self.header_mut()
    }
}

impl SSTable {
    pub fn new(
        lo: &[u8],
        hi: &[u8],
        prefix_len: u8,
        items: &[(&[u8], u64)],
    ) -> SSTable {
        let offsets_size = size_of::<u64>() * (2 + items.len());
        let keys_and_values_size = lo.len()
            + hi.len()
            + items.iter().map(|(k, _pid)| k.len()).sum::<usize>()
            + (2 * size_of::<u64>()) * (2 + items.len());

        println!("allocating size of {}", offsets_size + keys_and_values_size);

        let boxed_slice =
            aligned_boxed_slice(offsets_size + keys_and_values_size);

        let mut ret = SSTable(ManuallyDrop::new(boxed_slice));

        *ret.header_mut() = Header {
            next: None,
            merging_child: None,
            lo_len: lo.len() as u64,
            hi_len: hi.len() as u64,
            children: u16::try_from(items.len()).unwrap(),
            prefix_len: prefix_len,
            merging: false,
            is_index: true,
        };

        let data_buf = &mut ret.0[size_of::<Header>()..];
        let (offsets, lengths_keys_and_values) =
            data_buf.split_at_mut(offsets_size);

        let bounds = [(lo, 0), (hi, 0)];
        let iter = bounds.iter().chain(items.into_iter());

        let mut kv_cursor = 0_usize;
        for (idx, (key, pid)) in iter.enumerate() {
            let offsets_cursor = idx * U64_SZ;
            offsets[offsets_cursor..offsets_cursor + U64_SZ]
                .copy_from_slice(&kv_cursor.to_le_bytes());

            lengths_keys_and_values[kv_cursor..kv_cursor + U64_SZ]
                .copy_from_slice(&(key.len() as u64).to_le_bytes());
            kv_cursor += U64_SZ;

            lengths_keys_and_values[kv_cursor..kv_cursor + key.len()]
                .copy_from_slice(key);
            kv_cursor += key.len();

            lengths_keys_and_values[kv_cursor..kv_cursor + U64_SZ]
                .copy_from_slice(&pid.to_le_bytes());
            kv_cursor += U64_SZ;
            dbg!(kv_cursor);
        }

        ret
    }

    pub fn insert(&self, key: &[u8], pid: u64) -> SSTable {
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

    pub fn remove(&self, key: &[u8]) -> SSTable {
        let offset = self
            .find(&key[usize::from(self.prefix_len)..])
            .expect("called remove for non-present key");

        //

        //

        //
        todo!()
    }

    pub fn split(&self) -> (SSTable, SSTable) {
        todo!()
    }

    pub fn merge(&self, other: &SSTable) -> SSTable {
        todo!()
    }

    pub fn should_split(&self) -> bool {
        todo!()
    }

    pub fn should_merge(&self) -> bool {
        todo!()
    }

    fn header(&self) -> &Header {
        unsafe { &*(self.0.as_ptr() as *mut Header) }
    }

    fn header_mut(&mut self) -> &mut Header {
        unsafe { &mut *(self.0.as_mut_ptr() as *mut Header) }
    }

    pub fn len(&self) -> usize {
        usize::from(self.children)
    }

    fn len_internal(&self) -> usize {
        self.len() + 2
    }

    fn find(&self, key: &[u8]) -> Result<usize, usize> {
        let mut size = self.len();
        if size == 0 || key < self.index_child(0).0 {
            return Err(0);
        }
        let mut base = 0_usize;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            // mid is always in [0, size), that means mid is >= 0 and < size.
            // mid >= 0: by definition
            // mid < size: mid = size / 2 + size / 4 + size / 8 ...
            let l = self.index_child(mid).0;
            let cmp = crate::fastcmp(l, key);
            base = if cmp == Greater { base } else { mid };
            size -= half;
        }
        // base is always in [0, size) because base <= mid.
        let l = self.index_child(base).0;
        let cmp = crate::fastcmp(l, key);

        if cmp == Equal { Ok(base) } else { Err(base + (cmp == Less) as usize) }
    }

    fn get_lub(&self, key: &[u8]) -> u64 {
        assert!(key >= self.lo());
        match self.find(key) {
            Ok(idx) => self.index_child(idx).1,
            Err(idx) => self.index_child(idx - 1).1,
        }
    }

    fn iter(&self) -> impl Iterator<Item = (&[u8], u64)> {
        (2..)
            .take_while(move |idx| *idx < self.len_internal())
            .map(move |idx| self.index(idx))
    }

    fn lo(&self) -> &[u8] {
        self.index(0).0
    }

    fn hi(&self) -> &[u8] {
        self.index(1).0
    }

    fn index_child(&self, idx: usize) -> (&[u8], u64) {
        self.index(idx + 2)
    }

    fn index(&self, idx: usize) -> (&[u8], u64) {
        assert!(
            idx < self.len_internal(),
            "index {} is not less than internal length of {}",
            idx,
            self.len_internal()
        );

        let raw = self.index_raw_kv(idx);

        let pivot = raw.len() - U64_SZ;

        let (key, pid_bytes) = raw.split_at(pivot);

        (key, u64::from_le_bytes(pid_bytes.try_into().unwrap()))
    }

    fn index_raw_kv(&self, idx: usize) -> &[u8] {
        assert!(idx <= self.len_internal());
        let data_buf = &self.0[size_of::<Header>()..];
        let offsets_len = self.len_internal() * U64_SZ;
        let (offsets, items) = data_buf.split_at(offsets_len);

        let offset_buf = &offsets[U64_SZ * idx..U64_SZ * (idx + 1)];
        let kv_offset =
            u64::from_le_bytes(offset_buf.try_into().unwrap()) as usize;

        let item_buf = &items[kv_offset..];
        let len_buf = &item_buf[..U64_SZ];
        let key_len = u64::from_le_bytes(len_buf.try_into().unwrap()) as usize;
        let val_len = U64_SZ;

        let start = U64_SZ;
        let end = (2 * U64_SZ) + key_len;

        &item_buf[start..end]
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
        let mut ir =
            SSTable::new(&[1], &[7], 0, &[(&[1], 42), (&[6, 6, 6], 66)]);
        ir.next = Some(NonZeroU64::new(5).unwrap());
        ir.is_index = false;
        dbg!(ir.header());
        println!("ir: {:#?}", ir);
        assert_eq!(ir.get_lub(&[1]), 42);
        assert_eq!(ir.get_lub(&[2]), 42);
        assert_eq!(ir.get_lub(&[6]), 42);
        assert_eq!(ir.get_lub(&[7]), 66);
    }
}

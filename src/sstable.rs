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
    fixed_key_length: Option<NonZeroU64>,
    fixed_value_length: Option<NonZeroU64>,
    pub children: u16,
    offset_bytes: u8,
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
        items: &[(&[u8], &[u8])],
    ) -> SSTable {
        // determine if we need to use varints and offset
        // indirection tables, or if everything is equal
        // size we can skip this.
        let mut key_lengths = Vec::with_capacity(items.len());
        let mut value_lengths = Vec::with_capacity(items.len());

        let mut keys_equal_length = true;
        let mut values_equal_length = true;
        for (k, v) in items {
            key_lengths.push(k.len() as u64);
            if let Some(first_sz) = key_lengths.first() {
                keys_equal_length &= *first_sz == k.len() as u64;
            }
            value_lengths.push(v.len() as u64);
            if let Some(first_sz) = value_lengths.first() {
                values_equal_length &= *first_sz == v.len() as u64;
            }
        }

        let fixed_key_length = if keys_equal_length {
            if let Some(key_length) = key_lengths.first() {
                if *key_length > 0 {
                    Some(NonZeroU64::new(*key_length).unwrap())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let fixed_value_length = if values_equal_length {
            if let Some(value_length) = value_lengths.first() {
                if *value_length > 0 {
                    Some(NonZeroU64::new(*value_length).unwrap())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let key_storage_size = if let Some(key_length) = fixed_key_length {
            key_length.get() * (items.len() as u64)
        } else {
            let mut sum = 0;
            for key_length in &key_lengths {
                sum += key_length;
                sum += varint_size(*key_length);
            }
            sum
        };

        let value_storage_size = if let Some(value_length) = fixed_value_length
        {
            value_length.get() * (items.len() as u64)
        } else {
            let mut sum = 0;
            for value_length in &value_lengths {
                sum += value_length;
                sum += varint_size(*value_length);
            }
            sum
        };

        let (offsets_storage_size, offset_bytes) = if keys_equal_length
            && values_equal_length
        {
            (0, 0)
        } else {
            let max_offset_storage_size = (6 * items.len()) as u64;
            let max_total_item_storage_size =
                key_storage_size + value_storage_size + max_offset_storage_size;

            let bytes_per_offset: u8 = match max_total_item_storage_size {
                i if i < 256 => 1,
                i if i < (1 << 16) => 2,
                i if i < (1 << 24) => 3,
                i if i < (1 << 32) => 4,
                i if i < (1 << 40) => 5,
                i if i < (1 << 48) => 6,
                _ => unreachable!(),
            };

            (bytes_per_offset as u64 * items.len() as u64, bytes_per_offset)
        };

        let total_item_storage_size =
            key_storage_size + value_storage_size + offsets_storage_size;

        println!("allocating size of {}", total_item_storage_size);

        let boxed_slice = aligned_boxed_slice(
            usize::try_from(total_item_storage_size).unwrap(),
        );

        let mut ret = SSTable(ManuallyDrop::new(boxed_slice));

        *ret.header_mut() = Header {
            next: None,
            merging_child: None,
            lo_len: lo.len() as u64,
            hi_len: hi.len() as u64,
            fixed_key_length,
            fixed_value_length,
            offset_bytes,
            children: u16::try_from(items.len()).unwrap(),
            prefix_len: prefix_len,
            merging: false,
            is_index: true,
        };

        // we use either 0 or 1 offset tables.
        // - if keys and values are all equal lengths, no offset table is
        //   required
        // - if keys are equal length but values are not, we put an offset table
        //   at the beginning of the data buffer, then put each of the keys
        //   packed together, then varint-prefixed values which are addressed by
        //   the offset table
        // - if keys and values are both different lengths, we put an offset
        //   table at the beginning of the data buffer, then varint-prefixed
        //   keys followed inline with varint-prefixed values.
        //
        // So, there are 4 possible layouts:
        // 1. [fixed size keys] [fixed size values]
        //  - signified by fixed_key_length and fixed_value_length being Some
        // 2. [offsets] [fixed size keys] [variable values]
        //  - fixed_key_length: Some, fixed_value_length: None
        // 3. [offsets] [variable keys] [fixed-length values]
        //  - fixed_key_length: None, fixed_value_length: Some
        // 4. [offsets] [variable keys followed by variable values]
        //  - fixed_key_length: None, fixed_value_length: None

        let mut offset = 0_u64;
        for (idx, (k, v)) in items.iter().enumerate() {
            if !keys_equal_length || !values_equal_length {
                ret.set_offset(idx, usize::try_from(offset).unwrap());
            }
            if !keys_equal_length {
                offset += varint_size(k.len() as u64) + k.len() as u64;
            }
            if !values_equal_length {
                offset += varint_size(v.len() as u64) + v.len() as u64;
            }

            ret.key_buf_for_offset_mut(idx).copy_from_slice(k);

            ret.value_buf_for_offset_mut(idx).copy_from_slice(v);
        }

        ret
    }

    // returns the OPEN ENDED buffer where a key may be placed
    fn key_buf_for_offset_mut(&mut self, index: usize) -> &mut [u8] {
        match (self.fixed_key_length, self.fixed_value_length) {
            (Some(k_sz), Some(_)) | (Some(k_sz), None) => {
                let keys_buf = self.keys_buf_mut();
                &mut keys_buf[index * usize::try_from(k_sz.get()).unwrap()..]
            }
            (None, Some(_)) | (None, None) => {
                // find offset for key or combined kv offset
                let offset = self.offset(index);
                let keys_buf = self.keys_buf_mut();
                &mut keys_buf[offset..]
            }
        }
    }

    // returns the OPEN ENDED buffer where a value may be placed
    //
    // NB: it's important that this is only ever called after setting
    // the key and its varint length prefix, as this needs to be parsed
    // for case 4.
    fn value_buf_for_offset_mut(&mut self, index: usize) -> &mut [u8] {
        match (self.fixed_key_length, self.fixed_value_length) {
            (Some(_), Some(v_sz)) | (None, Some(v_sz)) => {
                let values_buf = self.values_buf_mut();
                &mut values_buf[index * usize::try_from(v_sz.get()).unwrap()..]
            }
            (Some(_), None) | (None, None) => {
                // find combined kv offset, skip key bytes
                let offset = self.offset(index);
                let values_buf = self.values_buf_mut();
                &mut values_buf[offset..]
            }
        }
    }

    fn offset(&self, index: usize) -> usize {
        let start = index * self.offset_bytes as usize;
        let end = start + self.offset_bytes as usize;
        let buf = &self.offsets_buf()[start..end];
        let mut le_usize_buf = [0u8; U64_SZ];
        le_usize_buf[end - start..].copy_from_slice(buf);
        usize::try_from(u64::from_le_bytes(le_usize_buf)).unwrap()
    }

    fn set_offset(&mut self, index: usize, offset: usize) {
        let offset_bytes = self.offset_bytes as usize;
        let mut buf = self.offset_buf_for_offset_mut(index);
        let bytes = &offset.to_le_bytes()[..offset_bytes];
        buf.copy_from_slice(bytes);
    }

    fn offset_buf_for_offset_mut(&mut self, index: usize) -> &mut [u8] {
        let start = index * self.offset_bytes as usize;
        let end = start + self.offset_bytes as usize;
        &mut self.offsets_buf_mut()[start..end]
    }

    fn keys_buf_mut(&mut self) -> &mut [u8] {
        todo!()
    }

    fn values_buf_mut(&mut self) -> &mut [u8] {
        todo!()
    }

    fn offsets_buf(&self) -> &[u8] {
        let offset_sz = self.children as usize * self.offset_bytes as usize;
        &self.data_buf()[..offset_sz]
    }

    fn offsets_buf_mut(&mut self) -> &mut [u8] {
        let offset_sz = self.children as usize * self.offset_bytes as usize;
        &mut self.data_buf_mut()[..offset_sz]
    }

    fn data_buf(&self) -> &[u8] {
        &self.0[size_of::<Header>()..]
    }

    fn data_buf_mut(&mut self) -> &mut [u8] {
        &mut self.0[size_of::<Header>()..]
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> SSTable {
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

    fn get_lub(&self, key: &[u8]) -> &[u8] {
        assert!(key >= self.lo());
        match self.find(key) {
            Ok(idx) => self.index_child(idx).1,
            Err(idx) => self.index_child(idx - 1).1,
        }
    }

    fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
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

    fn index_child(&self, idx: usize) -> (&[u8], &[u8]) {
        self.index(idx + 2)
    }

    fn index(&self, idx: usize) -> (&[u8], &[u8]) {
        assert!(
            idx < self.len_internal(),
            "index {} is not less than internal length of {}",
            idx,
            self.len_internal()
        );

        let raw = self.index_raw_kv(idx);

        let pivot = raw.len() - U64_SZ;

        let (key, pid_bytes) = raw.split_at(pivot);

        (key, pid_bytes)
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

// returns how many bytes the varint consumed
fn serialize_varint_into(int: u64, buf: &mut [u8]) -> usize {
    if int <= 240 {
        buf[0] = u8::try_from(int).unwrap();
        1
    } else if int <= 2287 {
        buf[0] = u8::try_from((int - 240) / 256 + 241).unwrap();
        buf[1] = u8::try_from((int - 240) % 256).unwrap();
        2
    } else if int <= 67823 {
        buf[0] = 249;
        buf[1] = u8::try_from((int - 2288) / 256).unwrap();
        buf[2] = u8::try_from((int - 2288) % 256).unwrap();
        3
    } else if int <= 0x00FF_FFFF {
        buf[0] = 250;
        let bytes = int.to_le_bytes();
        buf[1..4].copy_from_slice(&bytes[..3]);
        4
    } else if int <= 0xFFFF_FFFF {
        buf[0] = 251;
        let bytes = int.to_le_bytes();
        buf[1..5].copy_from_slice(&bytes[..4]);
        5
    } else if int <= 0x00FF_FFFF_FFFF {
        buf[0] = 252;
        let bytes = int.to_le_bytes();
        buf[1..6].copy_from_slice(&bytes[..5]);
        6
    } else if int <= 0xFFFF_FFFF_FFFF {
        buf[0] = 253;
        let bytes = int.to_le_bytes();
        buf[1..7].copy_from_slice(&bytes[..6]);
        7
    } else if int <= 0x00FF_FFFF_FFFF_FFFF {
        buf[0] = 254;
        let bytes = int.to_le_bytes();
        buf[1..8].copy_from_slice(&bytes[..7]);
        8
    } else {
        buf[0] = 255;
        let bytes = int.to_le_bytes();
        buf[1..9].copy_from_slice(&bytes[..8]);
        9
    }
}

// returns the deserialized varint, along with how many bytes
// were taken up by the varint
fn deserialize_varint(buf: &[u8]) -> crate::Result<(u64, usize)> {
    if buf.is_empty() {
        return Err(crate::Error::corruption(None));
    }
    let res = match buf[0] {
        0..=240 => (u64::from(buf[0]), 1),
        241..=248 => {
            let varint =
                240 + 256 * (u64::from(buf[0]) - 241) + u64::from(buf[1]);
            (varint, 2)
        }
        249 => {
            let varint = 2288 + 256 * u64::from(buf[1]) + u64::from(buf[2]);
            (varint, 3)
        }
        other => {
            let sz = other as usize - 247;
            let mut aligned = [0; 8];
            aligned[..sz].copy_from_slice(&buf[1..=sz]);
            let varint = u64::from_le_bytes(aligned);
            (varint, sz)
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

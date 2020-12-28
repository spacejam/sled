#![allow(unsafe_code)]

use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    cmp::Ordering::{Equal, Greater, Less},
    convert::{TryFrom, TryInto},
    fmt,
    mem::{align_of, size_of, ManuallyDrop},
    num::NonZeroU64,
    ops::{Bound, Deref, DerefMut},
};

use crate::{
    deserialize_varint, prefix, serialize_varint_into, varint_size, IVec,
};

const ALIGNMENT: usize = align_of::<Header>();

// allocates space for a header struct at the beginning.
pub(crate) fn aligned_boxed_slice(size: usize) -> Box<[u8]> {
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
#[derive(Clone)]
#[cfg_attr(feature = "testing", derive(PartialEq))]
pub(crate) struct Node(pub ManuallyDrop<Box<[u8]>>);

impl Drop for Node {
    fn drop(&mut self) {
        let box_ptr = self.0.as_mut_ptr();
        let layout = Layout::from_size_align(self.0.len(), ALIGNMENT).unwrap();
        unsafe {
            dealloc(box_ptr, layout);
        }
    }
}

impl Deref for Node {
    type Target = Header;

    fn deref(&self) -> &Header {
        self.header()
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("header", self.header())
            .field("lo", &self.lo())
            .field("hi", &self.hi())
            .field("items", &self.iter().collect::<crate::Map<_, _>>())
            .finish()
    }
}

impl DerefMut for Node {
    fn deref_mut(&mut self) -> &mut Header {
        self.header_mut()
    }
}

impl Node {
    pub unsafe fn from_raw(buf: &[u8]) -> Node {
        let mut boxed_slice = aligned_boxed_slice(buf.len());
        boxed_slice.copy_from_slice(buf);
        Node(ManuallyDrop::new(boxed_slice))
    }

    pub(crate) fn new(
        lo: &[u8],
        hi: &[u8],
        prefix_len: u8,
        items: &[(&[u8], &[u8])],
    ) -> Node {
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

        let (fixed_key_length, keys_equal_length) = if keys_equal_length {
            if let Some(key_length) = key_lengths.first() {
                if *key_length > 0 {
                    (Some(NonZeroU64::new(*key_length).unwrap()), true)
                } else {
                    (None, false)
                }
            } else {
                (None, false)
            }
        } else {
            (None, false)
        };

        let (fixed_value_length, values_equal_length) = if values_equal_length {
            if let Some(value_length) = value_lengths.first() {
                if *value_length > 0 {
                    (Some(NonZeroU64::new(*value_length).unwrap()), true)
                } else {
                    (None, false)
                }
            } else {
                (None, false)
            }
        } else {
            (None, false)
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

        let total_item_storage_size = hi.len() as u64
            + lo.len() as u64
            + key_storage_size
            + value_storage_size
            + offsets_storage_size;

        let boxed_slice = aligned_boxed_slice(
            usize::try_from(total_item_storage_size).unwrap(),
        );

        let mut ret = Node(ManuallyDrop::new(boxed_slice));

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

        if let Some(ref mut lo_buf) = ret.lo_mut() {
            lo_buf.copy_from_slice(lo);
        }
        if let Some(ref mut hi_buf) = ret.hi_mut() {
            hi_buf.copy_from_slice(hi);
        }

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

            let mut key_buf = ret.key_buf_for_offset_mut(idx);
            if !keys_equal_length {
                let varint_bytes =
                    serialize_varint_into(k.len() as u64, key_buf);
                key_buf = &mut key_buf[varint_bytes..];
            }
            key_buf[..k.len()].copy_from_slice(k);

            let mut value_buf = ret.value_buf_for_offset_mut(idx);
            if !values_equal_length {
                let varint_bytes =
                    serialize_varint_into(v.len() as u64, value_buf);
                value_buf = &mut value_buf[varint_bytes..];
            }
            value_buf[..v.len()].copy_from_slice(v);
        }

        ret
    }

    pub(crate) fn new_root(child_pid: u64) -> Node {
        Node::new(
            &[],
            &[],
            0,
            &[(prefix::empty().into(), &child_pid.to_le_bytes())],
        )
    }

    pub(crate) fn new_hoisted_root(left: u64, at: &[u8], right: u64) -> Node {
        Node::new(
            &[],
            &[],
            0,
            &[
                (prefix::empty().into(), &left.to_le_bytes()),
                (&at, &right.to_le_bytes()),
            ],
        )
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

    // returns the OPEN ENDED buffer where a key may be read
    fn key_buf_for_offset(&self, index: usize) -> &[u8] {
        match (self.fixed_key_length, self.fixed_value_length) {
            (Some(k_sz), Some(_)) | (Some(k_sz), None) => {
                let keys_buf = self.keys_buf();
                &keys_buf[index * usize::try_from(k_sz.get()).unwrap()..]
            }
            (None, Some(_)) | (None, None) => {
                // find offset for key or combined kv offset
                let offset = self.offset(index);
                let keys_buf = self.keys_buf();
                &keys_buf[offset..]
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
            (Some(_), None) => {
                // find combined kv offset
                let offset = self.offset(index);
                let values_buf = self.values_buf_mut();
                &mut values_buf[offset..]
            }
            (None, None) => {
                // find combined kv offset, skip key bytes
                let offset = self.offset(index);
                let values_buf = self.values_buf_mut();
                let slot_buf = &mut values_buf[offset..];
                let (val_len, varint_sz) =
                    deserialize_varint(slot_buf).unwrap();
                &mut slot_buf[usize::try_from(val_len).unwrap() + varint_sz..]
            }
        }
    }

    // returns the OPEN ENDED buffer where a value may be read
    //
    // NB: it's important that this is only ever called after setting
    // the key and its varint length prefix, as this needs to be parsed
    // for case 4.
    fn value_buf_for_offset(&self, index: usize) -> &[u8] {
        match (self.fixed_key_length, self.fixed_value_length) {
            (Some(_), Some(v_sz)) | (None, Some(v_sz)) => {
                let values_buf = self.values_buf();
                &values_buf[index * usize::try_from(v_sz.get()).unwrap()..]
            }
            (Some(_), None) => {
                // find combined kv offset
                let offset = self.offset(index);
                let values_buf = self.values_buf();
                &values_buf[offset..]
            }
            (None, None) => {
                // find combined kv offset, skip key bytes
                let offset = self.offset(index);
                let values_buf = self.values_buf();
                let slot_buf = &values_buf[offset..];
                let (val_len, varint_sz) =
                    deserialize_varint(slot_buf).unwrap();
                &slot_buf[usize::try_from(val_len).unwrap() + varint_sz..]
            }
        }
    }

    fn offset(&self, index: usize) -> usize {
        let start = index * self.offset_bytes as usize;
        let end = start + self.offset_bytes as usize;
        let buf = &self.offsets_buf()[start..end];
        let mut le_usize_buf = [0u8; size_of::<u64>()];
        le_usize_buf[..self.offset_bytes as usize].copy_from_slice(buf);
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
        let offset_sz = self.children as usize * self.offset_bytes as usize;
        &mut self.data_buf_mut()[offset_sz..]
    }

    fn keys_buf(&self) -> &[u8] {
        let offset_sz = self.children as usize * self.offset_bytes as usize;
        &self.data_buf()[offset_sz..]
    }

    fn values_buf_mut(&mut self) -> &mut [u8] {
        let offset_sz = self.children as usize * self.offset_bytes as usize;
        match (self.fixed_key_length, self.fixed_value_length) {
            (Some(fixed_key_length), Some(_))
            | (Some(fixed_key_length), None) => {
                let start = offset_sz
                    + (fixed_key_length.get() as usize
                        * self.children as usize);
                &mut self.data_buf_mut()[start..]
            }
            (None, Some(fixed_value_length)) => {
                let total_value_size =
                    fixed_value_length.get() as usize * self.children as usize;
                let mut data_buf = self.data_buf_mut();
                let start = data_buf.len() - total_value_size;
                &mut data_buf[start..]
            }
            (None, None) => &mut self.data_buf_mut()[offset_sz..],
        }
    }

    fn values_buf(&self) -> &[u8] {
        let offset_sz = self.children as usize * self.offset_bytes as usize;
        match (self.fixed_key_length, self.fixed_value_length) {
            (Some(fixed_key_length), Some(_))
            | (Some(fixed_key_length), None) => {
                let start = offset_sz
                    + (fixed_key_length.get() as usize
                        * self.children as usize);
                &self.data_buf()[start..]
            }
            (None, Some(fixed_value_length)) => {
                let total_value_size =
                    fixed_value_length.get() as usize * self.children as usize;
                let data_buf = self.data_buf();
                let start = data_buf.len() - total_value_size;
                &data_buf[start..]
            }
            (None, None) => &self.data_buf()[offset_sz..],
        }
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
        let start = (self.lo_len as usize)
            + (self.hi_len as usize)
            + size_of::<Header>();
        &self.0[start..]
    }

    fn data_buf_mut(&mut self) -> &mut [u8] {
        let start = (self.lo_len as usize)
            + (self.hi_len as usize)
            + size_of::<Header>();
        &mut self.0[start..]
    }

    pub(crate) fn insert(&self, key: &[u8], value: &[u8]) -> Node {
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
        testing_assert!(is_sorted(&index.keys));
    }

    pub(crate) fn remove(&self, key: &[u8]) -> Node {
        let offset = self
            .find(&key[usize::from(self.prefix_len)..])
            .expect("called remove for non-present key");

        //

        //

        //
        todo!()
    }

    pub(crate) fn split(&self) -> (Node, Node) {
        todo!()
    }

    pub(crate) fn receive_merge(&self, other: &Node) -> Node {
        todo!()
    }

    pub(crate) fn should_split(&self) -> bool {
        todo!()
    }

    pub(crate) fn should_merge(&self) -> bool {
        todo!()
    }

    fn header(&self) -> &Header {
        unsafe { &*(self.0.as_ptr() as *mut Header) }
    }

    fn header_mut(&mut self) -> &mut Header {
        unsafe { &mut *(self.0.as_mut_ptr() as *mut Header) }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn len(&self) -> usize {
        usize::from(self.children)
    }

    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        self.find(key).is_ok()
    }

    fn find(&self, key: &[u8]) -> Result<usize, usize> {
        let mut size = self.len();
        if size == 0 || key < self.index_key(0) {
            return Err(0);
        }
        let mut base = 0_usize;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            // mid is always in [0, size), that means mid is >= 0 and < size.
            // mid >= 0: by definition
            // mid < size: mid = size / 2 + size / 4 + size / 8 ...
            let l = self.index_key(mid);
            let cmp = crate::fastcmp(l, key);
            base = if cmp == Greater { base } else { mid };
            size -= half;
        }
        // base is always in [0, size) because base <= mid.
        let l = self.index_key(base);
        let cmp = crate::fastcmp(l, key);

        if cmp == Equal { Ok(base) } else { Err(base + (cmp == Less) as usize) }
    }

    pub(crate) fn can_merge_child(&self) -> bool {
        self.merging_child.is_none() && !self.merging
    }

    pub(crate) fn index_next_node(&self, key: &[u8]) -> (usize, u64) {
        assert!(key >= self.lo().unwrap_or(&[]));
        assert!(self.is_index);
        let idx = match self.find(key) {
            Ok(idx) => idx,
            Err(idx) => idx - 1,
        };
        (idx, self.index_pid(idx))
    }

    pub(crate) fn parent_split(&self, at: &[u8], to: u64) -> Option<Node> {
        assert!(self.is_index, "tried to attach a ParentSplit to a Leaf Node");

        let encoded_sep = &at[self.prefix_len as usize..];
        if self.contains_key(encoded_sep) {
            log::debug!(
                "parent_split skipped because \
                     parent already contains child \
                     at split point due to deep race"
            );
            return None;
        }

        Some(self.insert(encoded_sep, &to.to_le_bytes()))
    }

    pub(crate) fn iter_keys(&self) -> impl Iterator<Item = &[u8]> {
        (0..)
            .take_while(move |idx| *idx < self.len())
            .map(move |idx| self.index_key(idx))
    }

    pub(crate) fn iter_index_pids<'a>(
        &'a self,
    ) -> impl 'a + Iterator<Item = u64> {
        assert!(self.is_index);
        self.iter_values().map(move |pid_bytes| {
            u64::from_le_bytes(pid_bytes.try_into().unwrap())
        })
    }

    pub(crate) fn iter_values(&self) -> impl Iterator<Item = &[u8]> {
        (0..)
            .take_while(move |idx| *idx < self.len())
            .map(move |idx| self.index_value(idx))
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.iter_keys().zip(self.iter_values())
    }

    pub(crate) fn lo(&self) -> Option<&[u8]> {
        let start = size_of::<Header>();
        let end = start + self.lo_len as usize;
        if start == end { None } else { Some(&self.0[start..end]) }
    }

    fn lo_mut(&mut self) -> Option<&mut [u8]> {
        let start = size_of::<Header>();
        let end = start + self.lo_len as usize;
        if start == end { None } else { Some(&mut self.0[start..end]) }
    }

    pub(crate) fn hi(&self) -> Option<&[u8]> {
        let start = (self.lo_len as usize) + size_of::<Header>();
        let end = start + self.hi_len as usize;
        if start == end { None } else { Some(&self.0[start..end]) }
    }

    fn hi_mut(&mut self) -> Option<&mut [u8]> {
        let start = (self.lo_len as usize) + size_of::<Header>();
        let end = start + self.hi_len as usize;
        if start == end { None } else { Some(&mut self.0[start..end]) }
    }

    pub(crate) fn index_key(&self, idx: usize) -> &[u8] {
        assert!(
            idx < self.len(),
            "index {} is not less than internal length of {}",
            idx,
            self.len()
        );

        let buf = self.key_buf_for_offset(idx);

        let (start, end) = if let Some(fixed_key_length) = self.fixed_key_length
        {
            (0, fixed_key_length.get() as usize)
        } else {
            let (key_len, varint_sz) = deserialize_varint(buf).unwrap();
            let start = varint_sz;
            let end = start + usize::try_from(key_len).unwrap();
            (start, end)
        };

        &buf[start..end]
    }

    pub(crate) fn index_value(&self, idx: usize) -> &[u8] {
        assert!(
            idx < self.len(),
            "index {} is not less than internal length of {}",
            idx,
            self.len()
        );

        let buf = self.value_buf_for_offset(idx);

        let (start, end) =
            if let Some(fixed_value_length) = self.fixed_value_length {
                (0, fixed_value_length.get() as usize)
            } else {
                let (value_len, varint_sz) = deserialize_varint(buf).unwrap();
                let start = varint_sz;
                let end = start + usize::try_from(value_len).unwrap();
                (start, end)
            };

        &buf[start..end]
    }

    pub(crate) fn index_pid(&self, idx: usize) -> u64 {
        assert!(self.is_index);
        u64::from_le_bytes(self.index_value(idx).try_into().unwrap())
    }

    /// `node_kv_pair` returns either existing (node/key, value) pair or
    /// (node/key, none) where a node/key is node level encoded key.
    pub(crate) fn node_kv_pair<'a>(
        &'a self,
        key: &'a [u8],
    ) -> (&'a [u8], Option<&[u8]>) {
        assert!(Some(key) >= self.lo());
        if self.hi().is_some() {
            assert!(key < self.hi().unwrap());
        }
        if let Some((k, v)) = self.leaf_pair_for_key(key.as_ref()) {
            (k, Some(v.clone()))
        } else {
            let encoded_key = &key[self.prefix_len as usize..];
            let encoded_val = None;
            (encoded_key, encoded_val)
        }
    }

    /// `leaf_pair_for_key` finds an existing value pair for a given key.
    pub(crate) fn leaf_pair_for_key(
        &self,
        key: &[u8],
    ) -> Option<(&[u8], &[u8])> {
        assert!(!self.is_index, "leaf_pair_for_key called on index node");

        let suffix = &key[self.prefix_len as usize..];

        let search = self.find(key).ok();

        search.map(|idx| (self.index_key(idx), self.index_value(idx)))
    }

    pub(crate) fn contains_upper_bound(&self, bound: &Bound<IVec>) -> bool {
        if let Some(hi) = self.hi() {
            match bound {
                Bound::Excluded(bound) if hi >= &*bound => true,
                Bound::Included(bound) if hi > &*bound => true,
                _ => false,
            }
        } else {
            true
        }
    }

    pub(crate) fn contains_lower_bound(
        &self,
        bound: &Bound<IVec>,
        is_forward: bool,
    ) -> bool {
        if let Some(lo) = self.lo() {
            match bound {
                Bound::Excluded(bound)
                    if lo < &*bound || (is_forward && *bound == lo) =>
                {
                    true
                }
                Bound::Included(bound) if lo <= &*bound => true,
                Bound::Unbounded if !is_forward => self.hi().is_none(),
                _ => false,
            }
        } else {
            true
        }
    }

    fn prefix_decode(&self, key: &[u8]) -> IVec {
        prefix::decode(self.prefix(), key)
    }

    fn prefix_encode<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        assert!(self.lo().unwrap_or(&[]) <= key);
        if self.hi().is_some() {
            assert!(self.hi().unwrap() > key);
        }

        &key[self.prefix_len as usize..]
    }

    fn prefix(&self) -> &[u8] {
        &self.lo().unwrap_or(&[])[..self.prefix_len as usize]
    }

    pub(crate) fn successor(
        &self,
        bound: &Bound<IVec>,
    ) -> Option<(IVec, IVec)> {
        assert!(!self.is_index);

        // This encoding happens this way because
        // keys cannot be lower than the node's lo key.
        let predecessor_key = match bound {
            Bound::Unbounded => self.prefix_encode(&self.lo().unwrap_or(&[])),
            Bound::Included(b) | Bound::Excluded(b) => {
                let max = std::cmp::max(&**b, self.lo().unwrap_or(&[]));
                self.prefix_encode(max)
            }
        };

        let search = self.find(predecessor_key);

        let start = match search {
            Ok(start) => start,
            Err(start) if start < self.len() => start,
            _ => return None,
        };

        for (idx, k) in self.iter_keys().skip(start).enumerate() {
            match bound {
                Bound::Excluded(b) if b[self.prefix_len as usize..] == *k => {
                    // keep going because we wanted to exclude
                    // this key.
                    continue;
                }
                _ => {}
            }
            let decoded_key = self.prefix_decode(k);
            return Some((decoded_key, self.index_value(start + idx).into()));
        }

        None
    }

    pub(crate) fn predecessor(
        &self,
        bound: &Bound<IVec>,
    ) -> Option<(IVec, IVec)> {
        assert!(!self.is_index);

        // This encoding happens this way because
        // the rightmost (unbounded) node has
        // a hi key represented by the empty slice
        let successor_key = match bound {
            Bound::Unbounded => {
                if let Some(hi) = self.hi() {
                    Some(IVec::from(self.prefix_encode(hi)))
                } else {
                    None
                }
            }
            Bound::Included(b) => Some(IVec::from(self.prefix_encode(b))),
            Bound::Excluded(b) => {
                // we use manual prefix encoding here because
                // there is an assertion in `prefix_encode`
                // that asserts the key is within the node,
                // and maybe `b` is above the node.
                let encoded = &b[self.prefix_len as usize..];
                Some(IVec::from(encoded))
            }
        };

        let search = if let Some(successor_key) = successor_key {
            self.find(&*successor_key)
        } else if self.is_empty() {
            Err(0)
        } else {
            Ok(self.len() - 1)
        };

        let end = match search {
            Ok(end) => end,
            Err(end) if end > 0 => end - 1,
            _ => return None,
        };

        for (idx, k) in self.iter_keys().take(end + 1).enumerate().rev() {
            match bound {
                Bound::Excluded(b)
                    if b.len() >= self.prefix_len as usize
                        && b[self.prefix_len as usize..] == *k =>
                {
                    // keep going because we wanted to exclude
                    // this key.
                    continue;
                }
                _ => {}
            }
            let decoded_key = self.prefix_decode(k);

            return Some((decoded_key, self.index_value(idx).into()));
        }
        None
    }
}

#[cfg(test)]
mod test {
    use quickcheck::{Arbitrary, Gen};
    use rand::Rng;

    use super::*;

    #[test]
    fn simple() {
        let mut ir =
            Node::new(&[1], &[7], 0, &[(&[1], &[42]), (&[6, 6, 6], &[66])]);
        ir.next = Some(NonZeroU64::new(5).unwrap());
        ir.is_index = false;
        dbg!(ir.header());
        println!("ir: {:#?}", ir);
        assert_eq!(ir.get_lub(&[1]), &[42]);
        assert_eq!(ir.get_lub(&[2]), &[42]);
        assert_eq!(ir.get_lub(&[6]), &[42]);
        assert_eq!(ir.get_lub(&[7]), &[66]);
    }

    impl Arbitrary for Node {
        fn arbitrary<G: Gen>(g: &mut G) -> Node {
            todo!()
        }
    }

    fn prop_indexable(
        lo: Vec<u8>,
        hi: Vec<u8>,
        children: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> bool {
        let children_ref: Vec<(&[u8], &[u8])> =
            children.iter().map(|(k, v)| (k.as_ref(), v.as_ref())).collect();
        let ir = Node::new(&lo, &hi, 0, &children_ref);

        assert_eq!(ir.children as usize, children_ref.len());

        for (idx, (k, v)) in children_ref.iter().enumerate() {
            assert_eq!(ir.index_key(idx), *k);
            let value = ir.index_value(idx);
            assert_eq!(
                value, *v,
                "expected value index {} to have value {:?} but instead it was {:?}",
                idx, *v, value,
            );
        }
        true
    }

    quickcheck::quickcheck! {
        #[cfg_attr(miri, ignore)]
        fn indexable(lo: Vec<u8>, hi: Vec<u8>, children: Vec<(Vec<u8>, Vec<u8>)>) -> bool {
            prop_indexable(lo, hi, children)
        }
    }

    #[test]
    fn sstable_bug_00() {
        // postmortem: offsets were not being stored, and the slot buf was not
        // being considered correctly while writing or reading values in
        // shared slots.
        assert!(prop_indexable(
            vec![],
            vec![],
            vec![(vec![], vec![]), (vec![], vec![1]),]
        ));
    }

    #[test]
    fn sstable_bug_01() {
        // postmortem: hi and lo keys were not properly being accounted in the
        // inital allocation
        assert!(prop_indexable(vec![], vec![0], vec![],));
    }
}

#![allow(clippy::mut_mut)]
use std::{
    convert::{TryFrom, TryInto},
    iter::FromIterator,
    marker::PhantomData,
    num::NonZeroU64,
};

use crate::{
    node::{Index, Leaf},
    pagecache::{
        BatchManifest, MessageHeader, PageState, SegmentNumber, Snapshot,
    },
    Data, DiskPtr, Error, IVec, Link, Meta, Node, Result,
};

/// Items that may be serialized and deserialized
pub trait Serialize: Sized {
    /// Returns the buffer size required to hold
    /// the serialized bytes for this item.
    fn serialized_size(&self) -> u64;

    /// Serializees the item without allocating.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is not large enough.
    fn serialize_into(&self, buf: &mut &mut [u8]);

    /// Attempts to deserialize this type from some bytes.
    fn deserialize(buf: &mut &[u8]) -> Result<Self>;

    /// Returns owned serialized bytes.
    fn serialize(&self) -> Vec<u8> {
        let sz = self.serialized_size();
        let mut buf = vec![0; usize::try_from(sz).unwrap()];
        self.serialize_into(&mut buf.as_mut_slice());
        buf
    }
}

// Moves a reference to mutable bytes forward,
// sidestepping Rust's limitations in reasoning
// about lifetimes.
//
// ☑ Checked with Miri by Tyler on 2019-12-12
#[allow(unsafe_code)]
fn scoot(buf: &mut &mut [u8], amount: usize) {
    assert!(buf.len() >= amount);
    let len = buf.len();
    let ptr = buf.as_mut_ptr();
    let new_len = len - amount;

    unsafe {
        let new_ptr = ptr.add(amount);
        *buf = std::slice::from_raw_parts_mut(new_ptr, new_len);
    }
}

impl Serialize for BatchManifest {
    fn serialized_size(&self) -> u64 {
        8
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        buf[..8].copy_from_slice(&self.0.to_le_bytes());
        scoot(buf, 8);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        if buf.len() < 8 {
            return Err(Error::corruption(None));
        }

        let array = buf[..8].try_into().unwrap();
        *buf = &buf[8..];
        Ok(BatchManifest(i64::from_le_bytes(array)))
    }
}

impl Serialize for () {
    fn serialized_size(&self) -> u64 {
        0
    }

    fn serialize_into(&self, _: &mut &mut [u8]) {}

    fn deserialize(_: &mut &[u8]) -> Result<()> {
        Ok(())
    }
}

impl Serialize for MessageHeader {
    fn serialized_size(&self) -> u64 {
        1 + 4
            + self.segment_number.serialized_size()
            + self.pid.serialized_size()
            + self.len.serialized_size()
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        self.crc32.serialize_into(buf);
        self.kind.into().serialize_into(buf);
        self.len.serialize_into(buf);
        self.segment_number.serialize_into(buf);
        self.pid.serialize_into(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<MessageHeader> {
        Ok(MessageHeader {
            crc32: u32::deserialize(buf)?,
            kind: u8::deserialize(buf)?.into(),
            len: u64::deserialize(buf)?,
            segment_number: SegmentNumber(u64::deserialize(buf)?),
            pid: u64::deserialize(buf)?,
        })
    }
}

impl Serialize for IVec {
    fn serialized_size(&self) -> u64 {
        let len = self.len() as u64;
        len + len.serialized_size()
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        (self.len() as u64).serialize_into(buf);
        buf[..self.len()].copy_from_slice(self.as_ref());
        scoot(buf, self.len());
    }

    fn deserialize(buf: &mut &[u8]) -> Result<IVec> {
        let k_len = usize::try_from(u64::deserialize(buf)?)
            .expect("should never store items that rust can't natively index");
        let ret = &buf[..k_len];
        *buf = &buf[k_len..];
        Ok(ret.into())
    }
}

impl Serialize for u64 {
    fn serialized_size(&self) -> u64 {
        if *self <= 240 {
            1
        } else if *self <= 2287 {
            2
        } else if *self <= 67823 {
            3
        } else if *self <= 0x00FF_FFFF {
            4
        } else if *self <= 0xFFFF_FFFF {
            5
        } else if *self <= 0x00FF_FFFF_FFFF {
            6
        } else if *self <= 0xFFFF_FFFF_FFFF {
            7
        } else if *self <= 0x00FF_FFFF_FFFF_FFFF {
            8
        } else {
            9
        }
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        let sz = if *self <= 240 {
            buf[0] = u8::try_from(*self).unwrap();
            1
        } else if *self <= 2287 {
            buf[0] = u8::try_from((*self - 240) / 256 + 241).unwrap();
            buf[1] = u8::try_from((*self - 240) % 256).unwrap();
            2
        } else if *self <= 67823 {
            buf[0] = 249;
            buf[1] = u8::try_from((*self - 2288) / 256).unwrap();
            buf[2] = u8::try_from((*self - 2288) % 256).unwrap();
            3
        } else if *self <= 0x00FF_FFFF {
            buf[0] = 250;
            let bytes = self.to_le_bytes();
            buf[1..4].copy_from_slice(&bytes[..3]);
            4
        } else if *self <= 0xFFFF_FFFF {
            buf[0] = 251;
            let bytes = self.to_le_bytes();
            buf[1..5].copy_from_slice(&bytes[..4]);
            5
        } else if *self <= 0x00FF_FFFF_FFFF {
            buf[0] = 252;
            let bytes = self.to_le_bytes();
            buf[1..6].copy_from_slice(&bytes[..5]);
            6
        } else if *self <= 0xFFFF_FFFF_FFFF {
            buf[0] = 253;
            let bytes = self.to_le_bytes();
            buf[1..7].copy_from_slice(&bytes[..6]);
            7
        } else if *self <= 0x00FF_FFFF_FFFF_FFFF {
            buf[0] = 254;
            let bytes = self.to_le_bytes();
            buf[1..8].copy_from_slice(&bytes[..7]);
            8
        } else {
            buf[0] = 255;
            let bytes = self.to_le_bytes();
            buf[1..9].copy_from_slice(&bytes[..8]);
            9
        };

        scoot(buf, sz);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        if buf.is_empty() {
            return Err(Error::corruption(None));
        }
        let (res, scoot) = match buf[0] {
            0..=240 => (u64::from(buf[0]), 1),
            241..=248 => {
                (240 + 256 * (u64::from(buf[0]) - 241) + u64::from(buf[1]), 2)
            }
            249 => (2288 + 256 * u64::from(buf[1]) + u64::from(buf[2]), 3),
            other => {
                let sz = other as usize - 247;
                let mut aligned = [0; 8];
                aligned[..sz].copy_from_slice(&buf[1..=sz]);
                (u64::from_le_bytes(aligned), sz + 1)
            }
        };
        *buf = &buf[scoot..];
        Ok(res)
    }
}

impl Serialize for i64 {
    fn serialized_size(&self) -> u64 {
        8
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        buf[..8].copy_from_slice(&self.to_le_bytes());
        scoot(buf, 8);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        if buf.len() < 8 {
            return Err(Error::corruption(None));
        }

        let array = buf[..8].try_into().unwrap();
        *buf = &buf[8..];
        Ok(i64::from_le_bytes(array))
    }
}

impl Serialize for u32 {
    fn serialized_size(&self) -> u64 {
        4
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        buf[..4].copy_from_slice(&self.to_le_bytes());
        scoot(buf, 4);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        if buf.len() < 4 {
            return Err(Error::corruption(None));
        }

        let array = buf[..4].try_into().unwrap();
        *buf = &buf[4..];
        Ok(u32::from_le_bytes(array))
    }
}

impl Serialize for bool {
    fn serialized_size(&self) -> u64 {
        1
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        let byte = u8::from(*self);
        buf[0] = byte;
        scoot(buf, 1);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<bool> {
        if buf.is_empty() {
            return Err(Error::corruption(None));
        }
        let value = buf[0] != 0;
        *buf = &buf[1..];
        Ok(value)
    }
}

impl Serialize for u8 {
    fn serialized_size(&self) -> u64 {
        1
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        buf[0] = *self;
        scoot(buf, 1);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<u8> {
        if buf.is_empty() {
            return Err(Error::corruption(None));
        }
        let value = buf[0];
        *buf = &buf[1..];
        Ok(value)
    }
}

impl Serialize for Meta {
    fn serialized_size(&self) -> u64 {
        self.inner
            .iter()
            .map(|(k, v)| {
                (k.len() as u64).serialized_size()
                    + u64::try_from(k.len()).unwrap()
                    + v.serialized_size()
            })
            .sum()
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        serialize_2tuple_sequence(self.inner.iter(), buf);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        Ok(Meta { inner: deserialize_sequence(buf)? })
    }
}

impl Serialize for Link {
    fn serialized_size(&self) -> u64 {
        match self {
            Link::Set(key, value) => {
                1 + (key.len() as u64).serialized_size()
                    + (value.len() as u64).serialized_size()
                    + u64::try_from(key.len()).unwrap()
                    + u64::try_from(value.len()).unwrap()
            }
            Link::Del(key) => {
                1 + (key.len() as u64).serialized_size()
                    + u64::try_from(key.len()).unwrap()
            }
            Link::ParentMergeIntention(a) => 1 + a.serialized_size(),
            Link::ParentMergeConfirm | Link::ChildMergeCap => 1,
        }
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        match self {
            Link::Set(key, value) => {
                0_u8.serialize_into(buf);
                key.serialize_into(buf);
                value.serialize_into(buf);
            }
            Link::Del(key) => {
                1_u8.serialize_into(buf);
                key.serialize_into(buf);
            }
            Link::ParentMergeIntention(pid) => {
                2_u8.serialize_into(buf);
                pid.serialize_into(buf);
            }
            Link::ParentMergeConfirm => {
                3_u8.serialize_into(buf);
            }
            Link::ChildMergeCap => {
                4_u8.serialize_into(buf);
            }
        }
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        if buf.is_empty() {
            return Err(Error::corruption(None));
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        Ok(match discriminant {
            0 => Link::Set(IVec::deserialize(buf)?, IVec::deserialize(buf)?),
            1 => Link::Del(IVec::deserialize(buf)?),
            2 => Link::ParentMergeIntention(u64::deserialize(buf)?),
            3 => Link::ParentMergeConfirm,
            4 => Link::ChildMergeCap,
            _ => return Err(Error::corruption(None)),
        })
    }
}

fn shift_u64_opt(value: &Option<u64>) -> u64 {
    value.map(|s| s + 1).unwrap_or(0)
}

impl Serialize for Option<u64> {
    fn serialized_size(&self) -> u64 {
        shift_u64_opt(self).serialized_size()
    }
    fn serialize_into(&self, buf: &mut &mut [u8]) {
        shift_u64_opt(self).serialize_into(buf)
    }
    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        let shifted = u64::deserialize(buf)?;
        let unshifted = if shifted == 0 { None } else { Some(shifted - 1) };
        Ok(unshifted)
    }
}

impl Serialize for Option<NonZeroU64> {
    fn serialized_size(&self) -> u64 {
        (self.map(NonZeroU64::get).unwrap_or(0)).serialized_size()
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        (self.map(NonZeroU64::get).unwrap_or(0)).serialize_into(buf)
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        let underlying = u64::deserialize(buf)?;
        Ok(if underlying == 0 {
            None
        } else {
            Some(NonZeroU64::new(underlying).unwrap())
        })
    }
}

impl Serialize for Node {
    fn serialized_size(&self) -> u64 {
        2 + self.next.serialized_size()
            + self.merging_child.serialized_size()
            + self.lo.serialized_size()
            + self.hi.serialized_size()
            + self.data.serialized_size()
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        self.next.serialize_into(buf);
        self.merging_child.serialize_into(buf);
        self.merging.serialize_into(buf);
        self.prefix_len.serialize_into(buf);
        self.lo.serialize_into(buf);
        self.hi.serialize_into(buf);
        self.data.serialize_into(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        Ok(Node {
            next: Serialize::deserialize(buf)?,
            merging_child: Serialize::deserialize(buf)?,
            merging: bool::deserialize(buf)?,
            prefix_len: u8::deserialize(buf)?,
            lo: IVec::deserialize(buf)?,
            hi: IVec::deserialize(buf)?,
            data: Data::deserialize(buf)?,
        })
    }
}

impl Serialize for Option<i64> {
    fn serialized_size(&self) -> u64 {
        shift_i64_opt(self).serialized_size()
    }
    fn serialize_into(&self, buf: &mut &mut [u8]) {
        shift_i64_opt(self).serialize_into(buf)
    }
    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        Ok(unshift_i64_opt(i64::deserialize(buf)?))
    }
}

fn shift_i64_opt(value_opt: &Option<i64>) -> i64 {
    if let Some(value) = value_opt {
        if value.signum() == -1 {
            *value
        } else {
            value + 1
        }
    } else {
        0
    }
}

fn unshift_i64_opt(value: i64) -> Option<i64> {
    if value == 0 {
        None
    } else if value.signum() == -1 {
        Some(value)
    } else {
        Some(value - 1)
    }
}

impl Serialize for Snapshot {
    fn serialized_size(&self) -> u64 {
        self.stable_lsn.serialized_size()
            + self.active_segment.serialized_size()
            + self
                .pt
                .iter()
                .map(|page_state| page_state.serialized_size())
                .sum::<u64>()
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        self.stable_lsn.serialize_into(buf);
        self.active_segment.serialize_into(buf);
        for page_state in &self.pt {
            page_state.serialize_into(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        Ok(Snapshot {
            stable_lsn: Serialize::deserialize(buf)?,
            active_segment: Serialize::deserialize(buf)?,
            pt: deserialize_sequence(buf)?,
        })
    }
}

impl Serialize for Data {
    fn serialized_size(&self) -> u64 {
        match self {
            Data::Leaf(ref leaf) => {
                1_u64
                    + (leaf.keys.len() as u64).serialized_size()
                    + leaf
                        .keys
                        .iter()
                        .enumerate()
                        .map(|(idx, k)| {
                            let v = &leaf.values[idx];
                            (k.len() as u64).serialized_size()
                                + (v.len() as u64).serialized_size()
                                + k.len() as u64
                                + v.len() as u64
                        })
                        .sum::<u64>()
            }
            Data::Index(ref index) => {
                1_u64
                    + (index.keys.len() as u64).serialized_size()
                    + index
                        .keys
                        .iter()
                        .enumerate()
                        .map(|(idx, k)| {
                            let v = index.pointers[idx];
                            (k.len() as u64).serialized_size()
                                + v.serialized_size()
                                + k.len() as u64
                        })
                        .sum::<u64>()
            }
        }
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        match self {
            Data::Leaf(leaf) => {
                0_u8.serialize_into(buf);
                (leaf.keys.len() as u64).serialize_into(buf);
                for key in &leaf.keys {
                    key.serialize_into(buf);
                }
                for value in &leaf.values {
                    value.serialize_into(buf);
                }
            }
            Data::Index(index) => {
                1_u8.serialize_into(buf);
                (index.keys.len() as u64).serialize_into(buf);
                for key in &index.keys {
                    key.serialize_into(buf);
                }
                for value in &index.pointers {
                    value.serialize_into(buf);
                }
            }
        }
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Data> {
        if buf.is_empty() {
            return Err(Error::corruption(None));
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        let len = usize::try_from(u64::deserialize(buf)?).unwrap();
        Ok(match discriminant {
            0 => Data::Leaf(Leaf {
                keys: deserialize_bounded_sequence(buf, len)?,
                values: deserialize_bounded_sequence(buf, len)?,
            }),
            1 => Data::Index(Index {
                keys: deserialize_bounded_sequence(buf, len)?,
                pointers: deserialize_bounded_sequence(buf, len)?,
            }),
            _ => return Err(Error::corruption(None)),
        })
    }
}

impl Serialize for DiskPtr {
    fn serialized_size(&self) -> u64 {
        match self {
            DiskPtr::Inline(a) => 1 + a.serialized_size(),
            DiskPtr::Blob(a, b) => {
                1 + a.serialized_size() + b.serialized_size()
            }
        }
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        match self {
            DiskPtr::Inline(log_offset) => {
                0_u8.serialize_into(buf);
                log_offset.serialize_into(buf);
            }
            DiskPtr::Blob(log_offset, blob_lsn) => {
                1_u8.serialize_into(buf);
                log_offset.serialize_into(buf);
                blob_lsn.serialize_into(buf);
            }
        }
    }

    fn deserialize(buf: &mut &[u8]) -> Result<DiskPtr> {
        if buf.len() < 2 {
            return Err(Error::corruption(None));
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        Ok(match discriminant {
            0 => DiskPtr::Inline(u64::deserialize(buf)?),
            1 => DiskPtr::Blob(u64::deserialize(buf)?, i64::deserialize(buf)?),
            _ => return Err(Error::corruption(None)),
        })
    }
}

impl Serialize for PageState {
    fn serialized_size(&self) -> u64 {
        match self {
            PageState::Free(a, disk_ptr) => {
                1 + a.serialized_size() + disk_ptr.serialized_size()
            }
            PageState::Present { base, frags } => {
                1 + base.serialized_size()
                    + frags
                        .iter()
                        .map(|tuple| tuple.serialized_size())
                        .sum::<u64>()
            }
            _ => panic!("tried to serialize {:?}", self),
        }
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        match self {
            PageState::Free(lsn, disk_ptr) => {
                0_u8.serialize_into(buf);
                lsn.serialize_into(buf);
                disk_ptr.serialize_into(buf);
            }
            PageState::Present { base, frags } => {
                let frags_len: u8 = 1 + u8::try_from(frags.len())
                    .expect("should never have more than 255 frags");
                frags_len.serialize_into(buf);
                base.serialize_into(buf);
                serialize_3tuple_ref_sequence(frags.iter(), buf);
            }
            _ => panic!("tried to serialize {:?}", self),
        }
    }

    fn deserialize(buf: &mut &[u8]) -> Result<PageState> {
        if buf.is_empty() {
            return Err(Error::corruption(None));
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        Ok(match discriminant {
            0 => PageState::Free(
                i64::deserialize(buf)?,
                DiskPtr::deserialize(buf)?,
            ),
            len => PageState::Present {
                base: Serialize::deserialize(buf)?,
                frags: deserialize_bounded_sequence(buf, usize::from(len - 1))?,
            },
        })
    }
}

impl<A: Serialize, B: Serialize> Serialize for (A, B) {
    fn serialized_size(&self) -> u64 {
        self.0.serialized_size() + self.1.serialized_size()
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        self.0.serialize_into(buf);
        self.1.serialize_into(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<(A, B)> {
        let a = A::deserialize(buf)?;
        let b = B::deserialize(buf)?;
        Ok((a, b))
    }
}

impl<A: Serialize, B: Serialize, C: Serialize> Serialize for (A, B, C) {
    fn serialized_size(&self) -> u64 {
        self.0.serialized_size()
            + self.1.serialized_size()
            + self.2.serialized_size()
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        self.0.serialize_into(buf);
        self.1.serialize_into(buf);
        self.2.serialize_into(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<(A, B, C)> {
        let a = A::deserialize(buf)?;
        let b = B::deserialize(buf)?;
        let c = C::deserialize(buf)?;
        Ok((a, b, c))
    }
}

fn serialize_2tuple_sequence<'a, XS, A, B>(xs: XS, buf: &mut &mut [u8])
where
    XS: Iterator<Item = (&'a A, &'a B)>,
    A: Serialize + 'a,
    B: Serialize + 'a,
{
    for item in xs {
        item.0.serialize_into(buf);
        item.1.serialize_into(buf);
    }
}

fn serialize_3tuple_ref_sequence<'a, XS, A, B, C>(xs: XS, buf: &mut &mut [u8])
where
    XS: Iterator<Item = &'a (A, B, C)>,
    A: Serialize + 'a,
    B: Serialize + 'a,
    C: Serialize + 'a,
{
    for item in xs {
        item.0.serialize_into(buf);
        item.1.serialize_into(buf);
        item.2.serialize_into(buf);
    }
}

struct ConsumeSequence<'a, 'b, T> {
    buf: &'a mut &'b [u8],
    _t: PhantomData<T>,
    done: bool,
}

fn deserialize_sequence<T: Serialize, R>(buf: &mut &[u8]) -> Result<R>
where
    R: FromIterator<T>,
{
    let iter = ConsumeSequence { buf, _t: PhantomData, done: false };
    iter.collect()
}

fn deserialize_bounded_sequence<T: Serialize, R>(
    buf: &mut &[u8],
    bound: usize,
) -> Result<R>
where
    R: FromIterator<T>,
{
    let iter = ConsumeSequence { buf, _t: PhantomData, done: false };
    iter.take(bound).collect()
}

impl<'a, 'b, T> Iterator for ConsumeSequence<'a, 'b, T>
where
    T: Serialize,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Result<T>> {
        if self.done || self.buf.is_empty() {
            return None;
        }
        let item_res = T::deserialize(&mut self.buf);
        if item_res.is_err() {
            self.done = true;
        }
        Some(item_res)
    }
}

#[cfg(test)]
mod qc {
    use quickcheck::{Arbitrary, Gen};
    use rand::Rng;

    use super::*;
    use crate::pagecache::MessageKind;

    impl Arbitrary for MessageHeader {
        fn arbitrary<G: Gen>(g: &mut G) -> MessageHeader {
            MessageHeader {
                crc32: g.gen(),
                len: g.gen(),
                kind: MessageKind::arbitrary(g),
                segment_number: SegmentNumber(SpreadU64::arbitrary(g).0),
                pid: g.gen(),
            }
        }
    }

    impl Arbitrary for MessageKind {
        fn arbitrary<G: Gen>(g: &mut G) -> MessageKind {
            g.gen_range(0, 12).into()
        }
    }

    impl Arbitrary for Data {
        fn arbitrary<G: Gen>(g: &mut G) -> Data {
            if g.gen() {
                let keys = Arbitrary::arbitrary(g);
                let mut values = vec![];
                for _ in &keys {
                    values.push(Arbitrary::arbitrary(g))
                }
                Data::Index(Index { keys, pointers: values })
            } else {
                let keys = Arbitrary::arbitrary(g);
                let mut values = vec![];
                for _ in &keys {
                    values.push(Arbitrary::arbitrary(g))
                }
                Data::Leaf(Leaf { keys, values })
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Data>> {
            match self {
                Data::Index(ref index) => {
                    let index = index.clone();
                    Box::new(index.keys.shrink().map(move |keys| {
                        Data::Index(Index {
                            pointers: index
                                .pointers
                                .iter()
                                .take(keys.len())
                                .copied()
                                .collect(),
                            keys,
                        })
                    }))
                }
                Data::Leaf(ref leaf) => {
                    let leaf = leaf.clone();
                    Box::new(leaf.keys.shrink().map(move |keys| {
                        Data::Leaf(Leaf {
                            values: leaf
                                .values
                                .iter()
                                .take(keys.len())
                                .cloned()
                                .collect(),
                            keys,
                        })
                    }))
                }
            }
        }
    }

    impl Arbitrary for Node {
        fn arbitrary<G: Gen>(g: &mut G) -> Node {
            let next: Option<NonZeroU64> = Arbitrary::arbitrary(g);

            let merging_child: Option<NonZeroU64> = Arbitrary::arbitrary(g);

            Node {
                next,
                merging_child,
                merging: bool::arbitrary(g),
                prefix_len: u8::arbitrary(g),
                lo: IVec::arbitrary(g),
                hi: IVec::arbitrary(g),
                data: Data::arbitrary(g),
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Node>> {
            let data_shrinker = self.data.shrink().map({
                let node = self.clone();
                move |data| Node { data, ..node.clone() }
            });

            let hi_shrinker = self.hi.shrink().map({
                let node = self.clone();
                move |hi| Node { hi, ..node.clone() }
            });

            let lo_shrinker = self.lo.shrink().map({
                let node = self.clone();
                move |lo| Node { lo, ..node.clone() }
            });

            Box::new(data_shrinker.chain(hi_shrinker).chain(lo_shrinker))
        }
    }

    impl Arbitrary for Link {
        fn arbitrary<G: Gen>(g: &mut G) -> Link {
            let discriminant = g.gen_range(0, 5);
            match discriminant {
                0 => Link::Set(IVec::arbitrary(g), IVec::arbitrary(g)),
                1 => Link::Del(IVec::arbitrary(g)),
                2 => Link::ParentMergeIntention(u64::arbitrary(g)),
                3 => Link::ParentMergeConfirm,
                4 => Link::ChildMergeCap,
                _ => unreachable!("invalid choice"),
            }
        }
    }

    impl Arbitrary for IVec {
        fn arbitrary<G: Gen>(g: &mut G) -> IVec {
            let v: Vec<u8> = Arbitrary::arbitrary(g);
            v.into()
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = IVec>> {
            let v: Vec<u8> = self.to_vec();
            Box::new(v.shrink().map(IVec::from))
        }
    }

    impl Arbitrary for Meta {
        fn arbitrary<G: Gen>(g: &mut G) -> Meta {
            Meta { inner: Arbitrary::arbitrary(g) }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Meta>> {
            Box::new(self.inner.shrink().map(|inner| Meta { inner }))
        }
    }

    impl Arbitrary for DiskPtr {
        fn arbitrary<G: Gen>(g: &mut G) -> DiskPtr {
            if g.gen() {
                DiskPtr::Inline(g.gen())
            } else {
                DiskPtr::Blob(g.gen(), g.gen())
            }
        }
    }

    impl Arbitrary for PageState {
        fn arbitrary<G: Gen>(g: &mut G) -> PageState {
            if g.gen() {
                // don't generate 255 because we add 1 to this
                // number in PageState::serialize_into to account
                // for the base fragment
                let n = g.gen_range(0, 255);

                let base = (g.gen(), DiskPtr::arbitrary(g), g.gen());
                let frags = (0..n)
                    .map(|_| (g.gen(), DiskPtr::arbitrary(g), g.gen()))
                    .collect();
                PageState::Present { base, frags }
            } else {
                PageState::Free(g.gen(), DiskPtr::arbitrary(g))
            }
        }
    }

    impl Arbitrary for Snapshot {
        fn arbitrary<G: Gen>(g: &mut G) -> Snapshot {
            Snapshot {
                stable_lsn: g.gen(),
                active_segment: g.gen(),
                pt: Arbitrary::arbitrary(g),
            }
        }
    }

    #[derive(Debug, Clone)]
    struct SpreadI64(i64);

    impl Arbitrary for SpreadI64 {
        fn arbitrary<G: Gen>(g: &mut G) -> SpreadI64 {
            let uniform = g.gen::<i64>();
            let shift = g.gen_range(0, 64);
            SpreadI64(uniform >> shift)
        }
    }

    #[derive(Debug, Clone)]
    struct SpreadU64(u64);

    impl Arbitrary for SpreadU64 {
        fn arbitrary<G: Gen>(g: &mut G) -> SpreadU64 {
            let uniform = g.gen::<u64>();
            let shift = g.gen_range(0, 64);
            SpreadU64(uniform >> shift)
        }
    }

    fn prop_serialize<T>(item: &T) -> bool
    where
        T: Serialize + PartialEq + Clone + std::fmt::Debug,
    {
        let mut buf = vec![0; usize::try_from(item.serialized_size()).unwrap()];
        let buf_ref = &mut buf.as_mut_slice();
        item.serialize_into(buf_ref);
        assert_eq!(
            buf_ref.len(),
            0,
            "round-trip failed to consume produced bytes"
        );
        assert_eq!(buf.len() as u64, item.serialized_size());
        let deserialized = T::deserialize(&mut buf.as_slice()).unwrap();
        if *item == deserialized {
            true
        } else {
            eprintln!(
                "round-trip serialization failed. original:\n\n{:?}\n\n \
                 deserialized(serialized(original)):\n\n{:?}",
                item, deserialized
            );
            false
        }
    }

    quickcheck::quickcheck! {
        #[cfg_attr(miri, ignore)]
        fn bool(item: bool) -> bool {
            prop_serialize(&item)
        }

        #[cfg_attr(miri, ignore)]
        fn u8(item: u8) -> bool {
            prop_serialize(&item)
        }

        #[cfg_attr(miri, ignore)]
        fn i64(item: SpreadI64) -> bool {
            prop_serialize(&item.0)
        }

        #[cfg_attr(miri, ignore)]
        fn u64(item: SpreadU64) -> bool {
            prop_serialize(&item.0)
        }

        #[cfg_attr(miri, ignore)]
        fn disk_ptr(item: DiskPtr) -> bool {
            prop_serialize(&item)
        }

        #[cfg_attr(miri, ignore)]
        fn page_state(item: PageState) -> bool {
            prop_serialize(&item)
        }

        #[cfg_attr(miri, ignore)]
        fn meta(item: Meta) -> bool {
            prop_serialize(&item)
        }

        #[cfg_attr(miri, ignore)]
        fn snapshot(item: Snapshot) -> bool {
            prop_serialize(&item)
        }

        #[cfg_attr(miri, ignore)]
        fn node(item: Node) -> bool {
            prop_serialize(&item)
        }

        #[cfg_attr(miri, ignore)]
        fn data(item: Data) -> bool {
            prop_serialize(&item)
        }

        #[cfg_attr(miri, ignore)]
        fn link(item: Link) -> bool {
            prop_serialize(&item)
        }

        #[cfg_attr(miri, ignore)]
        fn msg_header(item: MessageHeader) -> bool {
            prop_serialize(&item)
        }
    }

    #[test]
    fn debug_node() {
        // color_backtrace::install();

        let node = Node {
            next: None,
            lo: vec![].into(),
            hi: vec![].into(),
            merging_child: None,
            merging: true,
            prefix_len: 0,
            data: Data::Index(Index::default()),
        };

        prop_serialize(&node);
    }
}

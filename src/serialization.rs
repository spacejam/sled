#![allow(clippy::mut_mut)]
use std::{
    convert::{TryFrom, TryInto},
    iter::FromIterator,
    marker::PhantomData,
    num::NonZeroU64,
};

use crate::{
    pagecache::{
        BatchManifest, HeapId, MessageHeader, PageState, SegmentNumber,
        Snapshot,
    },
    varint, DiskPtr, Error, IVec, Link, Meta, Node, Result,
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
        4 + 1
            + self.len.serialized_size()
            + self.segment_number.serialized_size()
            + self.pid.serialized_size()
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        self.crc32.serialize_into(buf);
        (self.kind as u8).serialize_into(buf);
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
        varint::size(*self) as u64
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        let sz = varint::serialize_into(*self, buf);

        scoot(buf, sz);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        if buf.is_empty() {
            return Err(Error::corruption(None));
        }
        let (res, scoot) = varint::deserialize(buf)?;
        *buf = &buf[scoot..];
        Ok(res)
    }
}

struct NonVarU64(u64);

impl Serialize for NonVarU64 {
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
        Ok(NonVarU64(u64::from_le_bytes(array)))
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

impl Serialize for HeapId {
    fn serialized_size(&self) -> u64 {
        16
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        NonVarU64(self.location).serialize_into(buf);
        self.original_lsn.serialize_into(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<HeapId> {
        Ok(HeapId {
            location: NonVarU64::deserialize(buf)?.0,
            original_lsn: i64::deserialize(buf)?,
        })
    }
}

impl Serialize for Meta {
    fn serialized_size(&self) -> u64 {
        let len_sz: u64 = (self.inner.len() as u64).serialized_size();
        let items_sz: u64 = self
            .inner
            .iter()
            .map(|(k, v)| {
                (k.len() as u64).serialized_size()
                    + k.len() as u64
                    + v.serialized_size()
            })
            .sum();

        len_sz + items_sz
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        (self.inner.len() as u64).serialize_into(buf);
        serialize_2tuple_sequence(self.inner.iter(), buf);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        let len = u64::deserialize(buf)?;
        let meta = Meta { inner: deserialize_bounded_sequence(buf, len)? };
        Ok(meta)
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
        if *value < 0 {
            *value
        } else {
            value + 1
        }
    } else {
        0
    }
}

const fn unshift_i64_opt(value: i64) -> Option<i64> {
    if value == 0 {
        return None;
    }
    let subtract = value > 0;
    Some(value - subtract as i64)
}

impl Serialize for Snapshot {
    fn serialized_size(&self) -> u64 {
        self.version.serialized_size()
            + self.stable_lsn.serialized_size()
            + self.active_segment.serialized_size()
            + (self.pt.len() as u64).serialized_size()
            + self.pt.iter().map(Serialize::serialized_size).sum::<u64>()
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        self.version.serialize_into(buf);
        self.stable_lsn.serialize_into(buf);
        self.active_segment.serialize_into(buf);
        (self.pt.len() as u64).serialize_into(buf);
        for page_state in &self.pt {
            page_state.serialize_into(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        Ok(Snapshot {
            version: Serialize::deserialize(buf)?,
            stable_lsn: Serialize::deserialize(buf)?,
            active_segment: Serialize::deserialize(buf)?,
            pt: {
                let len = u64::deserialize(buf)?;
                deserialize_bounded_sequence(buf, len)?
            },
        })
    }
}

impl Serialize for Node {
    fn serialized_size(&self) -> u64 {
        let size = self.rss();
        size.serialized_size() + size
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        assert!(self.overlay.is_empty());
        self.rss().serialize_into(buf);
        buf[..self.len()].copy_from_slice(self.as_ref());
        scoot(buf, self.len());
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Node> {
        if buf.is_empty() {
            return Err(Error::corruption(None));
        }
        let len = usize::try_from(u64::deserialize(buf)?).unwrap();

        #[allow(unsafe_code)]
        let sst = unsafe { Node::from_raw(&buf[..len]) };

        *buf = &buf[len..];
        Ok(sst)
    }
}

impl Serialize for DiskPtr {
    fn serialized_size(&self) -> u64 {
        match self {
            DiskPtr::Inline(a) => 1 + a.serialized_size(),
            DiskPtr::Heap(a, b) => {
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
            DiskPtr::Heap(log_offset, heap_id) => {
                1_u8.serialize_into(buf);
                log_offset.serialize_into(buf);
                heap_id.serialize_into(buf);
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
            1 => DiskPtr::Heap(
                Serialize::deserialize(buf)?,
                HeapId::deserialize(buf)?,
            ),
            _ => return Err(Error::corruption(None)),
        })
    }
}

impl Serialize for PageState {
    fn serialized_size(&self) -> u64 {
        match self {
            PageState::Free(a, disk_ptr) => {
                0_u64.serialized_size()
                    + a.serialized_size()
                    + disk_ptr.serialized_size()
            }
            PageState::Present { base, frags } => {
                (1 + frags.len() as u64).serialized_size()
                    + base.serialized_size()
                    + frags.iter().map(Serialize::serialized_size).sum::<u64>()
            }
            _ => panic!("tried to serialize {:?}", self),
        }
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        match self {
            PageState::Free(lsn, disk_ptr) => {
                0_u64.serialize_into(buf);
                lsn.serialize_into(buf);
                disk_ptr.serialize_into(buf);
            }
            PageState::Present { base, frags } => {
                (1 + frags.len() as u64).serialize_into(buf);
                base.serialize_into(buf);
                serialize_2tuple_ref_sequence(frags.iter(), buf);
            }
            _ => panic!("tried to serialize {:?}", self),
        }
    }

    fn deserialize(buf: &mut &[u8]) -> Result<PageState> {
        if buf.is_empty() {
            return Err(Error::corruption(None));
        }
        let len = u64::deserialize(buf)?;
        Ok(match len {
            0 => PageState::Free(
                i64::deserialize(buf)?,
                DiskPtr::deserialize(buf)?,
            ),
            _ => PageState::Present {
                base: Serialize::deserialize(buf)?,
                frags: deserialize_bounded_sequence(buf, len - 1)?,
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

fn serialize_2tuple_ref_sequence<'a, XS, A, B>(xs: XS, buf: &mut &mut [u8])
where
    XS: Iterator<Item = &'a (A, B)>,
    A: Serialize + 'a,
    B: Serialize + 'a,
{
    for item in xs {
        item.0.serialize_into(buf);
        item.1.serialize_into(buf);
    }
}

struct ConsumeSequence<'a, 'b, T> {
    buf: &'a mut &'b [u8],
    _t: PhantomData<T>,
    bound: u64,
}

fn deserialize_bounded_sequence<T: Serialize, R>(
    buf: &mut &[u8],
    bound: u64,
) -> Result<R>
where
    R: FromIterator<T>,
{
    let iter = ConsumeSequence { buf, _t: PhantomData, bound };
    iter.collect()
}

impl<'a, 'b, T> Iterator for ConsumeSequence<'a, 'b, T>
where
    T: Serialize,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Result<T>> {
        if self.bound == 0 || self.buf.is_empty() {
            return None;
        }
        let item_res = T::deserialize(self.buf);
        self.bound -= 1;
        if item_res.is_err() {
            self.bound = 0;
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

    impl Arbitrary for HeapId {
        fn arbitrary<G: Gen>(g: &mut G) -> HeapId {
            HeapId {
                location: SpreadU64::arbitrary(g).0,
                original_lsn: SpreadI64::arbitrary(g).0,
            }
        }
    }

    impl Arbitrary for MessageKind {
        fn arbitrary<G: Gen>(g: &mut G) -> MessageKind {
            g.gen_range(0, 12).into()
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
                DiskPtr::Heap(g.gen(), HeapId::arbitrary(g))
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

                let base = (g.gen(), DiskPtr::arbitrary(g));
                let frags =
                    (0..n).map(|_| (g.gen(), DiskPtr::arbitrary(g))).collect();
                PageState::Present { base, frags }
            } else {
                PageState::Free(g.gen(), DiskPtr::arbitrary(g))
            }
        }
    }

    impl Arbitrary for Snapshot {
        fn arbitrary<G: Gen>(g: &mut G) -> Snapshot {
            Snapshot {
                version: g.gen(),
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
                "\nround-trip serialization failed. original:\n\n{:?}\n\n \
                 deserialized(serialized(original)):\n\n{:?}\n",
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
        fn link(item: Link) -> bool {
            prop_serialize(&item)
        }

        #[cfg_attr(miri, ignore)]
        fn msg_header(item: MessageHeader) -> bool {
            prop_serialize(&item)
        }
    }
}

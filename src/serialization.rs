#![allow(clippy::mut_mut)]
use std::{
    convert::{TryFrom, TryInto},
    iter::FromIterator,
    marker::PhantomData,
};

use crate::{
    pagecache::{PageState, Snapshot},
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

impl Serialize for IVec {
    fn serialized_size(&self) -> u64 {
        8 + self.len() as u64
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
        8
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        buf[..8].copy_from_slice(&self.to_le_bytes());
        scoot(buf, 8);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        if buf.len() < 8 {
            return Err(Error::Corruption { at: DiskPtr::Inline(86) });
        }

        let array = buf[..8].try_into().unwrap();
        *buf = &buf[8..];
        Ok(u64::from_le_bytes(array))
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
            return Err(Error::Corruption { at: DiskPtr::Inline(103) });
        }

        let array = buf[..8].try_into().unwrap();
        *buf = &buf[8..];
        Ok(i64::from_le_bytes(array))
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
            return Err(Error::Corruption { at: DiskPtr::Inline(77) });
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
            return Err(Error::Corruption { at: DiskPtr::Inline(93) });
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
            .map(|(k, _)| 16 + u64::try_from(k.len()).unwrap())
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
                17 + u64::try_from(key.len()).unwrap()
                    + u64::try_from(value.len()).unwrap()
            }
            Link::Del(key) => 9 + u64::try_from(key.len()).unwrap(),
            Link::ParentMergeIntention(_) => 9,
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
            return Err(Error::Corruption { at: DiskPtr::Inline(210) });
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        Ok(match discriminant {
            0 => Link::Set(IVec::deserialize(buf)?, IVec::deserialize(buf)?),
            1 => Link::Del(IVec::deserialize(buf)?),
            2 => Link::ParentMergeIntention(u64::deserialize(buf)?),
            3 => Link::ParentMergeConfirm,
            4 => Link::ChildMergeCap,
            _ => return Err(Error::Corruption { at: DiskPtr::Inline(220) }),
        })
    }
}

impl Serialize for Node {
    fn serialized_size(&self) -> u64 {
        34 + self.lo.len() as u64
            + self.hi.len() as u64
            + self.data.serialized_size()
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        self.next.unwrap_or(0_u64).serialize_into(buf);
        self.merging_child.unwrap_or(0_u64).serialize_into(buf);
        self.merging.serialize_into(buf);
        self.prefix_len.serialize_into(buf);
        self.lo.serialize_into(buf);
        self.hi.serialize_into(buf);
        self.data.serialize_into(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        let next = u64::deserialize(buf)?;
        let merging_child = u64::deserialize(buf)?;
        Ok(Node {
            next: if next == 0 { None } else { Some(next) },
            merging_child: if merging_child == 0 {
                None
            } else {
                Some(merging_child)
            },
            merging: bool::deserialize(buf)?,
            prefix_len: u8::deserialize(buf)?,
            lo: IVec::deserialize(buf)?,
            hi: IVec::deserialize(buf)?,
            data: Data::deserialize(buf)?,
        })
    }
}

impl Serialize for Snapshot {
    fn serialized_size(&self) -> u64 {
        24 + self
            .pt
            .iter()
            .map(|(_, page_state)| 8 + page_state.serialized_size())
            .sum::<u64>()
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        self.last_lsn.serialize_into(buf);
        self.last_lid.serialize_into(buf);
        self.max_header_stable_lsn.serialize_into(buf);
        serialize_2tuple_sequence(self.pt.iter(), buf);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self> {
        Ok(Snapshot {
            last_lsn: { i64::deserialize(buf)? },
            last_lid: { u64::deserialize(buf)? },
            max_header_stable_lsn: { i64::deserialize(buf)? },
            pt: deserialize_sequence(buf)?,
        })
    }
}

impl Serialize for Data {
    fn serialized_size(&self) -> u64 {
        match self {
            Data::Index(ref pointers) => {
                1_u64
                    + pointers
                        .iter()
                        .map(|(k, _)| 16 + k.len() as u64)
                        .sum::<u64>()
            }
            Data::Leaf(ref items) => {
                1_u64
                    + items
                        .iter()
                        .map(|(k, v)| 16 + k.len() as u64 + v.len() as u64)
                        .sum::<u64>()
            }
        }
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        match self {
            Data::Leaf(items) => {
                0_u8.serialize_into(buf);
                serialize_2tuple_ref_sequence(items.iter(), buf);
            }
            Data::Index(items) => {
                1_u8.serialize_into(buf);
                serialize_2tuple_ref_sequence(items.iter(), buf);
            }
        }
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Data> {
        if buf.is_empty() {
            return Err(Error::Corruption { at: DiskPtr::Inline(108) });
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        Ok(match discriminant {
            0 => Data::Leaf(deserialize_sequence(buf)?),
            1 => Data::Index(deserialize_sequence(buf)?),
            _ => return Err(Error::Corruption { at: DiskPtr::Inline(115) }),
        })
    }
}

impl Serialize for DiskPtr {
    fn serialized_size(&self) -> u64 {
        match self {
            DiskPtr::Inline(_) => 9,
            DiskPtr::Blob(_, _) => 17,
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
        if buf.len() < 9 {
            return Err(Error::Corruption { at: DiskPtr::Inline(136) });
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        Ok(match discriminant {
            0 => DiskPtr::Inline(u64::deserialize(buf)?),
            1 => DiskPtr::Blob(u64::deserialize(buf)?, i64::deserialize(buf)?),
            _ => return Err(Error::Corruption { at: DiskPtr::Inline(666) }),
        })
    }
}

impl Serialize for PageState {
    fn serialized_size(&self) -> u64 {
        match self {
            PageState::Free(_, disk_ptr) => 9 + disk_ptr.serialized_size(),
            PageState::Present(items) => {
                1 + items
                    .iter()
                    .map(|tuple| tuple.serialized_size())
                    .sum::<u64>()
            }
        }
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        match self {
            PageState::Free(lsn, disk_ptr) => {
                0_u8.serialize_into(buf);
                lsn.serialize_into(buf);
                disk_ptr.serialize_into(buf);
            }
            PageState::Present(items) => {
                assert!(!items.is_empty());
                let items_len: u8 = u8::try_from(items.len())
                    .expect("should never have more than 255 frags");
                items_len.serialize_into(buf);
                serialize_3tuple_ref_sequence(items.iter(), buf);
            }
        }
    }

    fn deserialize(buf: &mut &[u8]) -> Result<PageState> {
        if buf.is_empty() {
            return Err(Error::Corruption { at: DiskPtr::Inline(402) });
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        Ok(match discriminant {
            0 => PageState::Free(
                i64::deserialize(buf)?,
                DiskPtr::deserialize(buf)?,
            ),
            len => {
                let items =
                    deserialize_bounded_sequence(buf, usize::from(len))?;
                PageState::Present(items)
            }
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

    impl Arbitrary for Data {
        fn arbitrary<G: Gen>(g: &mut G) -> Data {
            if g.gen() {
                Data::Index(Arbitrary::arbitrary(g))
            } else {
                Data::Leaf(Arbitrary::arbitrary(g))
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Data>> {
            match self {
                Data::Index(items) => Box::new(items.shrink().map(Data::Index)),
                Data::Leaf(items) => Box::new(items.shrink().map(Data::Leaf)),
            }
        }
    }

    impl Arbitrary for Node {
        fn arbitrary<G: Gen>(g: &mut G) -> Node {
            let next_raw: Option<u64> = Arbitrary::arbitrary(g);
            let next = next_raw.map(|v| std::cmp::max(v, 1));

            let merging_child_raw: Option<u64> = Arbitrary::arbitrary(g);
            let merging_child = merging_child_raw.map(|v| std::cmp::max(v, 1));

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
                _ => panic!("invalid choice"),
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
                // PageState must always have at least 1 if it present
                let n = std::cmp::max(1, g.gen::<u8>());

                let items = (0..n)
                    .map(|_| (g.gen(), DiskPtr::arbitrary(g), g.gen()))
                    .collect();
                PageState::Present(items)
            } else {
                PageState::Free(g.gen(), DiskPtr::arbitrary(g))
            }
        }
    }

    impl Arbitrary for Snapshot {
        fn arbitrary<G: Gen>(g: &mut G) -> Snapshot {
            Snapshot {
                last_lsn: g.gen(),
                last_lid: g.gen(),
                max_header_stable_lsn: g.gen(),
                pt: Arbitrary::arbitrary(g),
            }
        }
    }

    fn prop_serialize<T>(item: T) -> bool
    where
        T: Serialize + PartialEq + Clone + std::fmt::Debug,
    {
        let mut buf = vec![0; item.serialized_size() as usize];
        let buf_ref = &mut buf.as_mut_slice();
        item.serialize_into(buf_ref);
        assert_eq!(
            buf_ref.len(),
            0,
            "round-trip failed to consume produced bytes"
        );
        let deserialized = T::deserialize(&mut buf.as_slice()).unwrap();
        if item != deserialized {
            eprintln!(
                "round-trip serialization failed. original:\n\n{:?}\n\n \
                 deserialized(serialized(original)):\n\n{:?}",
                item, deserialized
            );
            false
        } else {
            true
        }
    }

    quickcheck::quickcheck! {
        fn bool(item: bool) -> bool {
            prop_serialize(item)
        }

        fn u8(item: u8) -> bool {
            prop_serialize(item)
        }

        fn i64(item: i64) -> bool {
            prop_serialize(item)
        }

        fn u64(item: u64) -> bool {
            prop_serialize(item)
        }

        fn disk_ptr(item: DiskPtr) -> bool {
            prop_serialize(item)
        }

        fn page_state(item: PageState) -> bool {
            prop_serialize(item)
        }

        fn meta(item: Meta) -> bool {
            prop_serialize(item)
        }

        fn snapshot(item: Snapshot) -> bool {
            prop_serialize(item)
        }

        fn node(item: Node) -> bool {
            prop_serialize(item)
        }

        fn data(item: Data) -> bool {
            prop_serialize(item)
        }

        fn link(item: Link) -> bool {
            prop_serialize(item)
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
            data: Data::Index(vec![]),
        };

        prop_serialize(node);
    }
}

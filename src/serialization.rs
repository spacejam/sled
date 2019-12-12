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
    /// creates the byte representation of the item
    fn serialize(&self) -> Vec<u8>;

    /// attempts to deserialize this type from some bytes
    fn deserialize(buf: &[u8]) -> Result<Self>;
}

impl Serialize for u64 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    fn deserialize(buf: &[u8]) -> Result<Self> {
        match buf.try_into() {
            Ok(array) => Ok(u64::from_le_bytes(array)),
            Err(_) => Err(Error::Corruption { at: DiskPtr::Inline(0) }),
        }
    }
}

impl Serialize for Meta {
    fn serialize(&self) -> Vec<u8> {
        let output_sz =
            self.inner.iter().map(|(k, _)| 16 + k.len()).sum::<usize>();

        let mut buf = Vec::with_capacity(output_sz);

        poo_2tuple_sequence(self.inner.iter(), &mut buf);

        buf
    }

    fn deserialize(mut buf: &[u8]) -> Result<Self> {
        Ok(Meta { inner: eat_sequence(&mut buf)? })
    }
}

impl Serialize for Link {
    fn serialize(&self) -> Vec<u8> {
        let mut buf;
        match self {
            Link::Set(key, value) => {
                buf = Vec::with_capacity(17 + key.len() + value.len());
                0_u8.poo(&mut buf);
                key.poo(&mut buf);
                value.poo(&mut buf);
            }
            Link::Del(key) => {
                buf = Vec::with_capacity(9 + key.len());
                1_u8.poo(&mut buf);
                key.poo(&mut buf);
            }
            Link::ParentMergeIntention(pid) => {
                buf = Vec::with_capacity(9);
                2_u8.poo(&mut buf);
                pid.poo(&mut buf);
            }
            Link::ParentMergeConfirm => {
                buf = Vec::with_capacity(1);
                3_u8.poo(&mut buf);
            }
            Link::ChildMergeCap => {
                buf = Vec::with_capacity(1);
                4_u8.poo(&mut buf);
            }
        }
        buf
    }

    fn deserialize(mut buf: &[u8]) -> Result<Self> {
        if buf.is_empty() {
            return Err(Error::Corruption { at: DiskPtr::Inline(0) });
        }
        let discriminant = buf[0];
        buf = &buf[1..];
        Ok(match discriminant {
            0 => Link::Set(IVec::eat(&mut buf)?, IVec::eat(&mut buf)?),
            1 => Link::Del(IVec::eat(&mut buf)?),
            2 => Link::ParentMergeIntention(u64::eat(&mut buf)?),
            3 => Link::ParentMergeConfirm,
            4 => Link::ChildMergeCap,
            _ => return Err(Error::Corruption { at: DiskPtr::Inline(0) }),
        })
    }
}

impl Serialize for Node {
    fn serialize(&self) -> Vec<u8> {
        let output_sz =
            34 + self.lo.len() + self.hi.len() + self.data.serialized_size();

        let mut buf = Vec::with_capacity(output_sz);

        self.next.unwrap_or(0).poo(&mut buf);
        self.merging_child.unwrap_or(0).poo(&mut buf);
        self.merging.poo(&mut buf);
        self.prefix_len.poo(&mut buf);
        self.lo.poo(&mut buf);
        self.hi.poo(&mut buf);
        self.data.poo(&mut buf);

        buf
    }

    fn deserialize(mut buf: &[u8]) -> Result<Self> {
        let next = u64::eat(&mut buf)?;
        let merging_child = u64::eat(&mut buf)?;
        Ok(Node {
            next: if next == 0 { None } else { Some(next) },
            merging_child: if merging_child == 0 {
                None
            } else {
                Some(merging_child)
            },
            merging: bool::eat(&mut buf)?,
            prefix_len: u8::eat(&mut buf)?,
            lo: IVec::eat(&mut buf)?,
            hi: IVec::eat(&mut buf)?,
            data: Data::eat(&mut buf)?,
        })
    }
}

impl Serialize for Snapshot {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![];
        self.last_lsn.poo(&mut buf);
        self.last_lid.poo(&mut buf);
        self.max_header_stable_lsn.poo(&mut buf);
        poo_2tuple_sequence(self.pt.iter(), &mut buf);
        buf
    }

    fn deserialize(mut buf: &[u8]) -> Result<Self> {
        Ok(Snapshot {
            last_lsn: { i64::eat(&mut buf)? },
            last_lid: { u64::eat(&mut buf)? },
            max_header_stable_lsn: { i64::eat(&mut buf)? },
            pt: eat_sequence(&mut buf)?,
        })
    }
}

/// This is a helper trait for driving the
/// Serialize functionality.
trait EatPoo: Sized {
    /// Creates the byte representation of the item.
    fn poo(&self, buf: &mut Vec<u8>);

    /// Attempts to deserialize this type from some bytes.
    /// The buf signature allows `eat` to push the beginning
    /// of an immutable slice forward across function calls.
    fn eat(buf: &mut &[u8]) -> Result<Self>;
}

impl EatPoo for IVec {
    fn eat(buf: &mut &[u8]) -> Result<IVec> {
        if buf.len() < 8 {
            return Err(Error::Corruption { at: DiskPtr::Inline(23) });
        }
        let k_len =
            usize::try_from(u64::from_le_bytes(buf[..8].try_into().unwrap()))
                .unwrap();
        *buf = &buf[8..];
        let ret = &buf[..k_len];
        *buf = &buf[k_len..];
        Ok(ret.into())
    }

    fn poo(&self, buf: &mut Vec<u8>) {
        let k_len = u64::try_from(self.len()).unwrap().to_le_bytes();
        buf.extend_from_slice(&k_len);
        buf.extend_from_slice(self);
    }
}

impl EatPoo for u64 {
    fn eat(buf: &mut &[u8]) -> Result<u64> {
        if buf.len() < 8 {
            return Err(Error::Corruption { at: DiskPtr::Inline(45) });
        }
        let value = u64::from_le_bytes(buf[..8].try_into().unwrap());
        *buf = &buf[8..];
        Ok(value)
    }

    fn poo(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl EatPoo for i64 {
    fn eat(buf: &mut &[u8]) -> Result<i64> {
        if buf.len() < 8 {
            return Err(Error::Corruption { at: DiskPtr::Inline(61) });
        }
        let value = i64::from_le_bytes(buf[..8].try_into().unwrap());
        *buf = &buf[8..];
        Ok(value)
    }

    fn poo(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl EatPoo for bool {
    fn eat(buf: &mut &[u8]) -> Result<bool> {
        if buf.is_empty() {
            return Err(Error::Corruption { at: DiskPtr::Inline(77) });
        }
        let value = buf[0] != 0;
        *buf = &buf[1..];
        Ok(value)
    }

    fn poo(&self, buf: &mut Vec<u8>) {
        let byte = u8::from(*self);
        buf.push(byte);
    }
}

impl EatPoo for u8 {
    fn eat(buf: &mut &[u8]) -> Result<u8> {
        if buf.is_empty() {
            return Err(Error::Corruption { at: DiskPtr::Inline(93) });
        }
        let value = buf[0];
        *buf = &buf[1..];
        Ok(value)
    }

    fn poo(&self, buf: &mut Vec<u8>) {
        buf.push(*self);
    }
}

impl EatPoo for Data {
    fn eat(buf: &mut &[u8]) -> Result<Data> {
        if buf.is_empty() {
            return Err(Error::Corruption { at: DiskPtr::Inline(108) });
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        Ok(match discriminant {
            0 => Data::Leaf(eat_sequence(buf)?),
            1 => Data::Index(eat_sequence(buf)?),
            _ => return Err(Error::Corruption { at: DiskPtr::Inline(115) }),
        })
    }

    fn poo(&self, buf: &mut Vec<u8>) {
        match self {
            Data::Leaf(items) => {
                0_u8.poo(buf);
                poo_2tuple_ref_sequence(items.iter(), buf);
            }
            Data::Index(items) => {
                1_u8.poo(buf);
                poo_2tuple_ref_sequence(items.iter(), buf);
            }
        }
    }
}

impl EatPoo for DiskPtr {
    fn eat(buf: &mut &[u8]) -> Result<DiskPtr> {
        if buf.len() < 9 {
            return Err(Error::Corruption { at: DiskPtr::Inline(136) });
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        Ok(match discriminant {
            0 => DiskPtr::Inline(u64::eat(buf)?),
            1 => DiskPtr::Blob(u64::eat(buf)?, i64::eat(buf)?),
            _ => return Err(Error::Corruption { at: DiskPtr::Inline(666) }),
        })
    }

    fn poo(&self, buf: &mut Vec<u8>) {
        match self {
            DiskPtr::Inline(log_offset) => {
                0_u8.poo(buf);
                log_offset.poo(buf);
            }
            DiskPtr::Blob(log_offset, blob_lsn) => {
                1_u8.poo(buf);
                log_offset.poo(buf);
                blob_lsn.poo(buf);
            }
        }
    }
}

impl EatPoo for PageState {
    fn eat(buf: &mut &[u8]) -> Result<PageState> {
        if buf.is_empty() {
            return Err(Error::Corruption { at: DiskPtr::Inline(0) });
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        Ok(match discriminant {
            0 => PageState::Free(i64::eat(buf)?, DiskPtr::eat(buf)?),
            len => {
                let items = eat_bounded_sequence(buf, usize::from(len))?;
                PageState::Present(items)
            }
        })
    }

    fn poo(&self, buf: &mut Vec<u8>) {
        match self {
            PageState::Free(lsn, disk_ptr) => {
                0_u8.poo(buf);
                lsn.poo(buf);
                disk_ptr.poo(buf);
            }
            PageState::Present(items) => {
                assert!(!items.is_empty());
                let items_len: u8 = u8::try_from(items.len())
                    .expect("should never have more than 255 frags");
                items_len.poo(buf);
                poo_3tuple_ref_sequence(items.iter(), buf);
            }
        }
    }
}

impl<A: EatPoo, B: EatPoo> EatPoo for (A, B) {
    fn eat(buf: &mut &[u8]) -> Result<(A, B)> {
        let a = A::eat(buf)?;
        let b = B::eat(buf)?;
        Ok((a, b))
    }

    fn poo(&self, buf: &mut Vec<u8>) {
        self.0.poo(buf);
        self.1.poo(buf);
    }
}

impl<A: EatPoo, B: EatPoo, C: EatPoo> EatPoo for (A, B, C) {
    fn eat(buf: &mut &[u8]) -> Result<(A, B, C)> {
        let a = A::eat(buf)?;
        let b = B::eat(buf)?;
        let c = C::eat(buf)?;
        Ok((a, b, c))
    }

    fn poo(&self, buf: &mut Vec<u8>) {
        self.0.poo(buf);
        self.1.poo(buf);
        self.2.poo(buf);
    }
}

fn poo_2tuple_ref_sequence<'a, XS, A, B>(xs: XS, buf: &mut Vec<u8>)
where
    XS: Iterator<Item = &'a (A, B)>,
    A: EatPoo + 'a,
    B: EatPoo + 'a,
{
    for item in xs {
        item.0.poo(buf);
        item.1.poo(buf);
    }
}

fn poo_2tuple_sequence<'a, XS, A, B>(xs: XS, buf: &mut Vec<u8>)
where
    XS: Iterator<Item = (&'a A, &'a B)>,
    A: EatPoo + 'a,
    B: EatPoo + 'a,
{
    for item in xs {
        item.0.poo(buf);
        item.1.poo(buf);
    }
}

fn poo_3tuple_ref_sequence<'a, XS, A, B, C>(xs: XS, buf: &mut Vec<u8>)
where
    XS: Iterator<Item = &'a (A, B, C)>,
    A: EatPoo + 'a,
    B: EatPoo + 'a,
    C: EatPoo + 'a,
{
    for item in xs {
        item.0.poo(buf);
        item.1.poo(buf);
        item.2.poo(buf);
    }
}

struct ConsumeSequence<'a, 'b, T> {
    buf: &'b mut &'a [u8],
    _t: PhantomData<T>,
    done: bool,
}

fn eat_sequence<'a, 'b, T: EatPoo, R>(buf: &'b mut &'a [u8]) -> Result<R>
where
    R: FromIterator<T>,
{
    let iter = ConsumeSequence { buf, _t: PhantomData, done: false };
    iter.collect()
}

fn eat_bounded_sequence<'a, 'b, T: EatPoo, R>(
    buf: &'b mut &'a [u8],
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
    T: EatPoo,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Result<T>> {
        if self.done || self.buf.is_empty() {
            return None;
        }
        let item_res = T::eat(&mut self.buf);
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
                // PageState must always have at least 1 if it's present
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

    fn prop_serializable<T>(item: T) -> bool
    where
        T: Serialize + PartialEq + Clone + std::fmt::Debug,
    {
        let serialized = item.serialize();
        let deserialized = T::deserialize(&serialized).unwrap();
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

    fn prop_serialize<T>(item: T) -> bool
    where
        T: EatPoo + PartialEq + Clone + std::fmt::Debug,
    {
        let mut buf = vec![];
        item.poo(&mut buf);
        let deserialized = T::eat(&mut buf.as_slice()).unwrap();
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
        fn disk_ptr(item: DiskPtr) -> bool {
            prop_serialize(item)
        }

        fn page_state(item: PageState) -> bool {
            prop_serialize(item)
        }

        fn meta(item: Meta) -> bool {
            prop_serializable(item)
        }

        fn snapshot(item: Snapshot) -> bool {
            prop_serializable(item)
        }

        fn node(item: Node) -> bool {
            prop_serializable(item)
        }

        fn link(item: Link) -> bool {
            prop_serializable(item)
        }
    }
}

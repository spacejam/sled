use std::{
    convert::{TryFrom, TryInto},
    iter::FromIterator,
    marker::PhantomData,
};

use crate::{
    pagecache::{PageState, Snapshot},
    Data, DiskPtr, Error, IVec, Link, Meta, Node, Result,
};

trait Serialize: Sized {
    /// creates the byte representation of the item
    fn write(&self, buf: &mut Vec<u8>);

    /// attempts to deserialize this type from some bytes
    fn consume(buf: &mut &[u8]) -> Result<Self>;
}

impl Serialize for IVec {
    fn consume<'a>(buf: &mut &'a [u8]) -> Result<IVec> {
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

    fn write(&self, buf: &mut Vec<u8>) {
        buf.reserve(8 + self.len());
        let k_len = u64::try_from(self.len()).unwrap().to_le_bytes();
        buf.extend_from_slice(&k_len);
        buf.extend_from_slice(self);
    }
}

impl Serialize for u64 {
    fn consume<'a>(buf: &mut &'a [u8]) -> Result<u64> {
        if buf.len() < 8 {
            return Err(Error::Corruption { at: DiskPtr::Inline(45) });
        }
        let value = u64::from_le_bytes(buf[..8].try_into().unwrap());
        *buf = &buf[8..];
        Ok(value)
    }

    fn write(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for i64 {
    fn consume<'a>(buf: &mut &'a [u8]) -> Result<i64> {
        if buf.len() < 8 {
            return Err(Error::Corruption { at: DiskPtr::Inline(61) });
        }
        let value = i64::from_le_bytes(buf[..8].try_into().unwrap());
        *buf = &buf[8..];
        Ok(value)
    }

    fn write(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for bool {
    fn consume<'a>(buf: &mut &'a [u8]) -> Result<bool> {
        if buf.is_empty() {
            return Err(Error::Corruption { at: DiskPtr::Inline(77) });
        }
        let value = if buf[0] == 0 { false } else { true };
        *buf = &buf[1..];
        Ok(value)
    }

    fn write(&self, buf: &mut Vec<u8>) {
        let byte = u8::from(*self);
        buf.push(byte);
    }
}

impl Serialize for u8 {
    fn consume<'a>(buf: &mut &'a [u8]) -> Result<u8> {
        if buf.is_empty() {
            return Err(Error::Corruption { at: DiskPtr::Inline(93) });
        }
        let value = buf[0];
        *buf = &buf[1..];
        Ok(value)
    }

    fn write(&self, buf: &mut Vec<u8>) {
        buf.push(*self);
    }
}

impl Serialize for Data {
    fn consume<'a>(buf: &mut &'a [u8]) -> Result<Data> {
        if buf.is_empty() {
            return Err(Error::Corruption { at: DiskPtr::Inline(108) });
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        Ok(match discriminant {
            0 => Data::Leaf(consume_sequence(buf)?),
            1 => Data::Index(consume_sequence(buf)?),
            _ => return Err(Error::Corruption { at: DiskPtr::Inline(115) }),
        })
    }

    fn write(&self, buf: &mut Vec<u8>) {
        match self {
            Data::Leaf(items) => {
                0_u8.write(buf);
                write_2tuple_ref_sequence(items.iter(), buf);
            }
            Data::Index(items) => {
                1_u8.write(buf);
                write_2tuple_ref_sequence(items.iter(), buf);
            }
        }
    }
}

impl Serialize for DiskPtr {
    fn consume<'a>(buf: &mut &'a [u8]) -> Result<DiskPtr> {
        if buf.len() < 9 {
            return Err(Error::Corruption { at: DiskPtr::Inline(136) });
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        Ok(match discriminant {
            0 => DiskPtr::Inline(u64::consume(buf)?),
            1 => DiskPtr::Blob(u64::consume(buf)?, i64::consume(buf)?),
            _ => return Err(Error::Corruption { at: DiskPtr::Inline(666) }),
        })
    }

    fn write(&self, buf: &mut Vec<u8>) {
        match self {
            DiskPtr::Inline(log_offset) => {
                buf.reserve(9);
                0_u8.write(buf);
                log_offset.write(buf);
            }
            DiskPtr::Blob(log_offset, blob_lsn) => {
                buf.reserve(17);
                1_u8.write(buf);
                log_offset.write(buf);
                blob_lsn.write(buf);
            }
        }
    }
}

impl Serialize for PageState {
    fn consume<'a>(buf: &mut &'a [u8]) -> Result<PageState> {
        if buf.len() < 1 {
            return Err(Error::Corruption { at: DiskPtr::Inline(0) });
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        Ok(match discriminant {
            0 => PageState::Free(i64::consume(buf)?, DiskPtr::consume(buf)?),
            len => {
                let items = consume_bounded_sequence(buf, usize::from(len))?;
                PageState::Present(items)
            }
        })
    }

    fn write(&self, buf: &mut Vec<u8>) {
        match self {
            PageState::Free(lsn, disk_ptr) => {
                buf.reserve(18);
                0_u8.write(buf);
                lsn.write(buf);
                disk_ptr.write(buf);
            }
            PageState::Present(items) => {
                assert!(!items.is_empty());
                let items_len: u8 = u8::try_from(items.len())
                    .expect("should never have more than 255 frags");
                items_len.write(buf);
                write_3tuple_ref_sequence(items.iter(), buf);
            }
        }
    }
}

impl<A: Serialize, B: Serialize> Serialize for (A, B) {
    fn consume<'a>(buf: &mut &'a [u8]) -> Result<(A, B)> {
        let a = A::consume(buf)?;
        let b = B::consume(buf)?;
        Ok((a, b))
    }

    fn write(&self, buf: &mut Vec<u8>) {
        self.0.write(buf);
        self.1.write(buf);
    }
}

impl<A: Serialize, B: Serialize, C: Serialize> Serialize for (A, B, C) {
    fn consume<'a>(buf: &mut &'a [u8]) -> Result<(A, B, C)> {
        let a = A::consume(buf)?;
        let b = B::consume(buf)?;
        let c = C::consume(buf)?;
        Ok((a, b, c))
    }

    fn write(&self, buf: &mut Vec<u8>) {
        self.0.write(buf);
        self.1.write(buf);
        self.2.write(buf);
    }
}

fn write_2tuple_ref_sequence<'a, XS, A, B>(xs: XS, buf: &mut Vec<u8>)
where
    XS: Iterator<Item = &'a (A, B)>,
    A: Serialize + 'a,
    B: Serialize + 'a,
{
    for item in xs {
        item.0.write(buf);
        item.1.write(buf);
    }
}

fn write_2tuple_sequence<'a, XS, A, B>(xs: XS, buf: &mut Vec<u8>)
where
    XS: Iterator<Item = (&'a A, &'a B)>,
    A: Serialize + 'a,
    B: Serialize + 'a,
{
    for item in xs {
        item.0.write(buf);
        item.1.write(buf);
    }
}

fn write_3tuple_ref_sequence<'a, XS, A, B, C>(xs: XS, buf: &mut Vec<u8>)
where
    XS: Iterator<Item = &'a (A, B, C)>,
    A: Serialize + 'a,
    B: Serialize + 'a,
    C: Serialize + 'a,
{
    for item in xs {
        item.0.write(buf);
        item.1.write(buf);
        item.2.write(buf);
    }
}

struct ConsumeSequence<'a, 'b, T> {
    buf: &'b mut &'a [u8],
    _t: PhantomData<T>,
    done: bool,
}

fn consume_sequence<'a, 'b, T: Serialize, R>(buf: &'b mut &'a [u8]) -> Result<R>
where
    R: FromIterator<T>,
{
    let iter = ConsumeSequence { buf, _t: PhantomData, done: false };
    iter.collect()
}

fn consume_bounded_sequence<'a, 'b, T: Serialize, R>(
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
    T: Serialize,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Result<T>> {
        if self.done || self.buf.is_empty() {
            return None;
        }
        let item_res = T::consume(&mut self.buf);
        if item_res.is_err() {
            self.done = true;
        }
        Some(item_res)
    }
}

/// Items that may be serialized and deserialized
pub trait Serializable: Sized {
    /// creates the byte representation of the item
    fn serialize(&self) -> Vec<u8>;

    /// attempts to deserialize this type from some bytes
    fn deserialize(buf: &[u8]) -> Result<Self>;
}

impl Serializable for u64 {
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

impl Serializable for i64 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    fn deserialize(buf: &[u8]) -> Result<Self> {
        match buf.try_into() {
            Ok(array) => Ok(i64::from_le_bytes(array)),
            Err(_) => Err(Error::Corruption { at: DiskPtr::Inline(0) }),
        }
    }
}

impl Serializable for Meta {
    fn serialize(&self) -> Vec<u8> {
        let output_sz =
            self.inner.iter().map(|(k, _)| 16 + k.len()).sum::<usize>();

        let mut buf = Vec::with_capacity(output_sz);

        write_2tuple_sequence(self.inner.iter(), &mut buf);

        buf
    }

    fn deserialize(mut buf: &[u8]) -> Result<Self> {
        Ok(Meta { inner: consume_sequence(&mut buf)? })
    }
}

impl Serializable for Link {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![];
        match self {
            Link::Set(key, value) => {
                buf.reserve(1 + key.len() + value.len());
                0_u8.write(&mut buf);
                key.write(&mut buf);
                value.write(&mut buf);
            }
            Link::Del(key) => {
                buf.reserve(1 + key.len());
                1_u8.write(&mut buf);
                key.write(&mut buf);
            }
            Link::ParentMergeIntention(pid) => {
                buf.reserve(9);
                2_u8.write(&mut buf);
                pid.write(&mut buf);
            }
            Link::ParentMergeConfirm => {
                3_u8.write(&mut buf);
            }
            Link::ChildMergeCap => {
                4_u8.write(&mut buf);
            }
        }
        buf
    }

    fn deserialize(mut buf: &[u8]) -> Result<Self> {
        if buf.len() < 1 {
            return Err(Error::Corruption { at: DiskPtr::Inline(0) });
        }
        let discriminant = buf[0];
        buf = &buf[1..];
        Ok(match discriminant {
            0 => Link::Set(IVec::consume(&mut buf)?, IVec::consume(&mut buf)?),
            1 => Link::Del(IVec::consume(&mut buf)?),
            2 => Link::ParentMergeIntention(u64::consume(&mut buf)?),
            3 => Link::ParentMergeConfirm,
            4 => Link::ChildMergeCap,
            _ => return Err(Error::Corruption { at: DiskPtr::Inline(0) }),
        })
    }
}

impl Serializable for Node {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![];

        self.next.unwrap_or(0).write(&mut buf);
        self.merging_child.unwrap_or(0).write(&mut buf);
        self.merging.write(&mut buf);
        self.prefix_len.write(&mut buf);
        self.lo.write(&mut buf);
        self.hi.write(&mut buf);
        self.data.write(&mut buf);

        buf
    }

    fn deserialize(mut buf: &[u8]) -> Result<Self> {
        let next = u64::consume(&mut buf)?;
        let merging_child = u64::consume(&mut buf)?;
        Ok(Node {
            next: if next == 0 { None } else { Some(next) },
            merging_child: if merging_child == 0 {
                None
            } else {
                Some(merging_child)
            },
            merging: bool::consume(&mut buf)?,
            prefix_len: u8::consume(&mut buf)?,
            lo: IVec::consume(&mut buf)?,
            hi: IVec::consume(&mut buf)?,
            data: Data::consume(&mut buf)?,
        })
    }
}

impl Serializable for Snapshot {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![];
        self.last_lsn.write(&mut buf);
        self.last_lid.write(&mut buf);
        self.max_header_stable_lsn.write(&mut buf);
        println!("writing {} pt entries", self.pt.len());
        write_2tuple_sequence(self.pt.iter(), &mut buf);
        buf
    }

    fn deserialize(mut buf: &[u8]) -> Result<Self> {
        Ok(Snapshot {
            last_lsn: { i64::consume(&mut buf)? },
            last_lid: { u64::consume(&mut buf)? },
            max_header_stable_lsn: { i64::consume(&mut buf)? },
            pt: {
                let pt: crate::FastMap8<_, _> = consume_sequence(&mut buf)?;
                println!("reading {} pt entries", pt.len());
                pt
            },
        })
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
            Node {
                next: Arbitrary::arbitrary(g),
                merging_child: Arbitrary::arbitrary(g),
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
    }

    impl Arbitrary for Meta {
        fn arbitrary<G: Gen>(g: &mut G) -> Meta {
            Meta { inner: Arbitrary::arbitrary(g) }
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
        T: Serializable + PartialEq + Clone + std::fmt::Debug,
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
        T: Serialize + PartialEq + Clone + std::fmt::Debug,
    {
        let mut buf = vec![];
        item.write(&mut buf);
        let deserialized = T::consume(&mut buf.as_slice()).unwrap();
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

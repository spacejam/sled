use std::{
    convert::TryFrom,
    fmt,
    hash::{Hash, Hasher},
    iter::FromIterator,
    ops::{Deref, DerefMut},
};

use crate::Arc;

const CUTOFF: usize = 22;

type Inner = [u8; CUTOFF];

/// A buffer that may either be inline or remote and protected
/// by an Arc
#[derive(Clone)]
pub struct IVec(IVecInner);

impl Default for IVec {
    fn default() -> Self {
        Self::from(&[])
    }
}

#[derive(Clone)]
enum IVecInner {
    Inline(u8, Inner),
    Remote(Arc<[u8]>),
    Subslice { base: Arc<[u8]>, offset: usize, len: usize },
}

impl Hash for IVec {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.deref().hash(state);
    }
}

const fn is_inline_candidate(length: usize) -> bool {
    length <= CUTOFF
}

impl IVec {
    /// Create a subslice of this `IVec` that shares
    /// the same backing data and reference counter.
    ///
    /// # Panics
    ///
    /// Panics if `self.len() - offset >= len`.
    ///
    /// # Examples
    /// ```
    /// # use sled::IVec;
    /// let iv = IVec::from(vec![1]);
    /// let subslice = iv.subslice(0, 1);
    /// assert_eq!(&subslice, &[1]);
    /// let subslice = subslice.subslice(0, 1);
    /// assert_eq!(&subslice, &[1]);
    /// let subslice = subslice.subslice(1, 0);
    /// assert_eq!(&subslice, &[]);
    /// let subslice = subslice.subslice(0, 0);
    /// assert_eq!(&subslice, &[]);
    ///
    /// let iv2 = IVec::from(vec![1, 2, 3]);
    /// let subslice = iv2.subslice(3, 0);
    /// assert_eq!(&subslice, &[]);
    /// let subslice = iv2.subslice(2, 1);
    /// assert_eq!(&subslice, &[3]);
    /// let subslice = iv2.subslice(1, 2);
    /// assert_eq!(&subslice, &[2, 3]);
    /// let subslice = iv2.subslice(0, 3);
    /// assert_eq!(&subslice, &[1, 2, 3]);
    /// let subslice = subslice.subslice(1, 2);
    /// assert_eq!(&subslice, &[2, 3]);
    /// let subslice = subslice.subslice(1, 1);
    /// assert_eq!(&subslice, &[3]);
    /// let subslice = subslice.subslice(1, 0);
    /// assert_eq!(&subslice, &[]);
    /// ```
    pub fn subslice(&self, slice_offset: usize, len: usize) -> Self {
        assert!(self.len().checked_sub(slice_offset).unwrap() >= len);

        let inner = match self.0 {
            IVecInner::Remote(ref base) => IVecInner::Subslice {
                base: base.clone(),
                offset: slice_offset,
                len,
            },
            IVecInner::Inline(_, old_inner) => {
                // old length already checked above in assertion
                let mut new_inner = Inner::default();
                new_inner[..len].copy_from_slice(
                    &old_inner[slice_offset..slice_offset + len],
                );

                IVecInner::Inline(u8::try_from(len).unwrap(), new_inner)
            }
            IVecInner::Subslice { ref base, ref offset, .. } => {
                IVecInner::Subslice {
                    base: base.clone(),
                    offset: offset + slice_offset,
                    len,
                }
            }
        };

        IVec(inner)
    }

    fn inline(slice: &[u8]) -> Self {
        assert!(is_inline_candidate(slice.len()));
        let mut data = Inner::default();
        data[..slice.len()].copy_from_slice(slice);
        Self(IVecInner::Inline(u8::try_from(slice.len()).unwrap(), data))
    }

    fn remote(arc: Arc<[u8]>) -> Self {
        Self(IVecInner::Remote(arc))
    }

    fn make_mut(&mut self) {
        match self.0 {
            IVecInner::Remote(ref mut buf) if Arc::strong_count(buf) != 1 => {
                self.0 = IVecInner::Remote(buf.to_vec().into());
            }
            IVecInner::Subslice { ref mut base, offset, len }
                if Arc::strong_count(base) != 1 =>
            {
                self.0 = IVecInner::Remote(
                    base[offset..offset + len].to_vec().into(),
                );
            }
            _ => {}
        }
    }
}

impl FromIterator<u8> for IVec {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = u8>,
    {
        let bs: Vec<u8> = iter.into_iter().collect();
        bs.into()
    }
}

impl From<Box<[u8]>> for IVec {
    fn from(b: Box<[u8]>) -> Self {
        if is_inline_candidate(b.len()) {
            Self::inline(&b)
        } else {
            Self::remote(Arc::from(b))
        }
    }
}

impl From<&[u8]> for IVec {
    fn from(slice: &[u8]) -> Self {
        if is_inline_candidate(slice.len()) {
            Self::inline(slice)
        } else {
            Self::remote(Arc::from(slice))
        }
    }
}

impl From<Arc<[u8]>> for IVec {
    fn from(arc: Arc<[u8]>) -> Self {
        if is_inline_candidate(arc.len()) {
            Self::inline(&arc)
        } else {
            Self::remote(arc)
        }
    }
}

impl From<&str> for IVec {
    fn from(s: &str) -> Self {
        Self::from(s.as_bytes())
    }
}

impl From<&IVec> for IVec {
    fn from(v: &Self) -> Self {
        v.clone()
    }
}

impl From<Vec<u8>> for IVec {
    fn from(v: Vec<u8>) -> Self {
        if is_inline_candidate(v.len()) {
            Self::inline(&v)
        } else {
            // rely on the Arc From specialization
            // for Vec<T>, which may improve
            // over time
            Self::remote(Arc::from(v))
        }
    }
}

impl std::borrow::Borrow<[u8]> for IVec {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

impl std::borrow::Borrow<[u8]> for &IVec {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

macro_rules! from_array {
    ($($s:expr),*) => {
        $(
            impl From<&[u8; $s]> for IVec {
                fn from(v: &[u8; $s]) -> Self {
                    Self::from(&v[..])
                }
            }
        )*
    }
}

from_array!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32
);

impl Into<Arc<[u8]>> for IVec {
    fn into(self) -> Arc<[u8]> {
        match self.0 {
            IVecInner::Inline(..) => Arc::from(self.as_ref()),
            IVecInner::Remote(arc) => arc,
            IVecInner::Subslice { .. } => self.deref().into(),
        }
    }
}

impl Deref for IVec {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}

impl AsRef<[u8]> for IVec {
    #[inline]
    #[allow(unsafe_code)]
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            IVecInner::Inline(sz, buf) => unsafe {
                buf.get_unchecked(..*sz as usize)
            },
            IVecInner::Remote(buf) => buf,
            IVecInner::Subslice { ref base, offset, len } => {
                &base[*offset..*offset + *len]
            }
        }
    }
}

impl DerefMut for IVec {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

impl AsMut<[u8]> for IVec {
    #[inline]
    #[allow(unsafe_code)]
    fn as_mut(&mut self) -> &mut [u8] {
        self.make_mut();

        match &mut self.0 {
            IVecInner::Inline(ref sz, ref mut buf) => unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr(), *sz as usize)
            },
            IVecInner::Remote(ref mut buf) => Arc::get_mut(buf).unwrap(),
            IVecInner::Subslice { ref mut base, offset, len } => {
                &mut Arc::get_mut(base).unwrap()[*offset..*offset + *len]
            }
        }
    }
}

impl Ord for IVec {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl PartialOrd for IVec {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: AsRef<[u8]>> PartialEq<T> for IVec {
    fn eq(&self, other: &T) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl PartialEq<[u8]> for IVec {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_ref() == other
    }
}

impl Eq for IVec {}

impl fmt::Debug for IVec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

#[test]
fn ivec_usage() {
    let iv1 = IVec::from(vec![1, 2, 3]);
    assert_eq!(iv1, vec![1, 2, 3]);
    let iv2 = IVec::from(&[4; 128][..]);
    assert_eq!(iv2, vec![4; 128]);
}

#[test]
fn boxed_slice_conversion() {
    let boite1: Box<[u8]> = Box::new([1, 2, 3]);
    let iv1: IVec = boite1.into();
    assert_eq!(iv1, vec![1, 2, 3]);
    let boite2: Box<[u8]> = Box::new([4; 128]);
    let iv2: IVec = boite2.into();
    assert_eq!(iv2, vec![4; 128]);
}

#[test]
#[should_panic]
fn subslice_usage_00() {
    let iv1 = IVec::from(vec![1, 2, 3]);
    let _subslice = iv1.subslice(0, 4);
}

#[test]
#[should_panic]
fn subslice_usage_01() {
    let iv1 = IVec::from(vec![1, 2, 3]);
    let _subslice = iv1.subslice(3, 1);
}

#[test]
fn ivec_as_mut_identity() {
    let initial = &[1];
    let mut iv = IVec::from(initial);
    assert_eq!(&*initial, &*iv);
    assert_eq!(&*initial, &mut *iv);
    assert_eq!(&*initial, iv.as_mut());
}

#[cfg(test)]
mod qc {
    use super::IVec;

    fn prop_identity(ivec: &IVec) -> bool {
        let mut iv2 = ivec.clone();

        if iv2 != ivec {
            println!("expected clone to equal original");
            return false;
        }

        if *ivec != *iv2 {
            println!("expected AsMut to equal original");
            return false;
        }

        if *ivec != iv2.as_mut() {
            println!("expected AsMut to equal original");
            return false;
        }

        true
    }

    quickcheck::quickcheck! {
        #[cfg_attr(miri, ignore)]
        fn bool(item: IVec) -> bool {
            prop_identity(&item)
        }
    }
}

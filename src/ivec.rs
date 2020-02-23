use std::{
    convert::TryFrom,
    fmt,
    hash::{Hash, Hasher},
    iter::FromIterator,
    ops::Deref,
    sync::Arc,
};

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
    fn inline(slice: &[u8]) -> Self {
        assert!(is_inline_candidate(slice.len()));

        let mut data = Inner::default();

        #[allow(unsafe_code)]
        unsafe {
            std::ptr::copy_nonoverlapping(
                slice.as_ptr(),
                data.as_mut_ptr(),
                slice.len(),
            );
        }

        Self(IVecInner::Inline(u8::try_from(slice.len()).unwrap(), data))
    }

    fn remote(arc: Arc<[u8]>) -> Self {
        Self(IVecInner::Remote(arc))
    }
}

impl FromIterator<u8> for IVec {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = u8>,
    {
        let bs: Box<[u8]> = iter.into_iter().collect();
        bs.into()
    }
}

impl From<Box<[u8]>> for IVec {
    fn from(b: Box<[u8]>) -> Self {
        if is_inline_candidate(b.len()) {
            Self::inline(&b)
        } else {
            // rely on the Arc From specialization
            // for Box<T>, which may improve
            // over time
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
        Self::remote(arc)
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

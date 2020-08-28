use std::{
    convert::TryFrom,
    fmt,
    hash::{Hash, Hasher},
    iter::FromIterator,
    ops::{Deref, DerefMut},
    io::{Read, Write, Result as IoResult},
};

use crate::{ Arc, arc };

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
    Remote { base: Arc<[u8]>, len: usize },
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
            IVecInner::Remote { ref base, .. } => IVecInner::Subslice {
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
    /// Allocate IVec with zero length and specified capacity
    pub fn with_capacity(cap: usize) -> Self {
        if is_inline_candidate(cap) {
            Self(IVecInner::Inline(0, [0; CUTOFF]))
        } else {
            Self(IVecInner::Remote{ base: arc::new_arc_buffer(cap), len: 0 })
        }
    }
    /// Create zero-length IVec with inline buffer
    pub fn new() -> Self {
        Self::with_capacity(CUTOFF)
    }

    /// Returns length of data
    pub fn len(&self) -> usize {
        // faster path, avoids deref
        match self.0 {
            IVecInner::Inline(len, ..) => len as usize,
            IVecInner::Remote { len, .. } => len,
            IVecInner::Subslice { len, .. } => len,
        }
    }

    /// Current buffer capacity, may be equal or greater than length
    pub fn capacity(&self) -> usize {
        match self.0 {
            IVecInner::Inline(..) => CUTOFF,
            IVecInner::Remote { ref base, .. } => base.as_ref().len(),
            IVecInner::Subslice { len, .. } => len,
        }
    }

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

    fn remote(base: Arc<[u8]>) -> Self {
        let len = base.len();
        Self(IVecInner::Remote{ base, len })
    }

    fn make_mut(&mut self) {
        match self.0 {
            IVecInner::Remote { ref mut base, len } if Arc::strong_count(base) != 1 => {
                self.0 = IVecInner::Remote { base: base.as_ref().into(), len };
            }
            IVecInner::Subslice { ref mut base, offset, len }
            if Arc::strong_count(base) != 1 =>
                {
                    let new_base: Arc<[u8]> = base[offset..offset + len].as_ref().into();
                    self.0 = IVecInner::Remote { base: new_base, len };
                }
            _ => {}
        }
    }

    /// Append single byte to IVec
    pub fn push(&mut self, v: u8) {
        self.push_data(1, |buf| { buf[0] = v } );
    }

    /// Extend IVec from slice of bytes
    pub fn extend_from_slice(&mut self, s: &[u8]) {
        self.push_data(s.len(), |buf| buf[..s.len()].copy_from_slice(s));
    }

    // push `more` bytes into ivec, call `f` to set data bytes
    fn push_data<F>(&mut self, more: usize, f: F)
        where F: FnOnce(&mut [u8])
    {
        match &mut self.0 {
            IVecInner::Inline(len, inner) => {
                let usize_len = *len as usize;
                let new_len = usize_len + more;
                if new_len <= CUTOFF {
                    f(&mut inner[usize_len..]);
                    *len = *len + (more as u8);
                } else {
                    self.0 = IVecInner::Remote {
                        base: arc::arc_buffer_from_slice(more, &inner[..usize_len], f),
                        len: new_len,
                    };
                }
            },
            IVecInner::Remote{ base, len } => {
                arc::ensure_arc_buffer_alloc(base, *len, more, f);
                *len = *len + more;
            },
            IVecInner::Subslice { ref mut base, offset, len } => {
                let slice = &base[*offset..*offset + *len];
                // always detach from slice
                self.0 = IVecInner::Remote {
                    base: arc::arc_buffer_from_slice(more, slice, f),
                    len: *len + more,
                };
            },
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
            IVecInner::Remote { base, len } => {
                if base.len() == len {
                    base
                } else {
                    Arc::from(&base[..len])
                }
            },
            IVecInner::Subslice { .. } => self.deref().into(),
        }
    }
}

impl Into<Vec<u8>> for IVec {
    fn into(self) -> Vec<u8> {
        Vec::from(self.as_ref())
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
            IVecInner::Remote{ base, len} => {
                &base[..*len]
            },
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
            IVecInner::Remote { ref mut base, ref len } => {
                &mut Arc::get_mut(base).unwrap()[..*len]
            },
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

impl Read for IVec {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let s = self.as_ref();
        let amt = buf.len().min(s.len());
        buf[..amt].copy_from_slice(&s[..amt]);
        *self = self.subslice(amt, s.len() - amt);
        Ok(amt)
    }
}

impl Write for IVec {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

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

#[test]
fn ivec_io_01() {
    let mut iv = IVec::new();
    let d = b"abcdef";
    assert_eq!(iv.write(&d[..3]).unwrap(), 3);
    assert_eq!(iv.write(&d[3..]).unwrap(), 3);
    let mut buf1 = [0; 2];
    let n = iv.read(&mut buf1).unwrap();
    assert_eq!(n, 2);

    assert_eq!(iv.len(), 4);
    assert_eq!(iv.capacity(), CUTOFF);

    let mut buf2 = [0; 3];
    let n = iv.read(&mut buf2).unwrap();
    assert_eq!(n, 3);
    assert_eq!(&buf2[..n], &d[2..5]);

    assert_eq!(iv.capacity(), CUTOFF);
    assert_eq!(iv.len(), 1);

    // check write after read
    assert_eq!(iv.write(b"xyzzy").unwrap(), 5);
    assert_eq!(iv.capacity(), CUTOFF);
    assert_eq!(iv.len(), 6);
    assert_eq!(iv, b"fxyzzy");
}

#[test]
fn ivec_io_02() {
    let mut iv = IVec::new();
    let t1 = b"1234567890abcdefghijkl";
    assert_eq!(t1.len(), CUTOFF);

    assert_eq!(iv.write(t1).unwrap(), CUTOFF);
    assert_eq!(iv.capacity(), CUTOFF);
    assert_eq!(iv.len(), CUTOFF);

    assert_eq!(CUTOFF.next_power_of_two(), 32);

    // check realloc internal -> remote
    assert_eq!(iv.write(b"X").unwrap(), 1);
    assert_eq!(&iv[CUTOFF-2..CUTOFF+1], b"klX");
    assert_eq!(iv.capacity(), 32);  // next power of 2 after CUTOFF
    assert_eq!(iv.len(), CUTOFF + 1);

    // check realloc remote -> remote
    assert_eq!(iv.write(t1).unwrap(), t1.len());
    assert_eq!(iv.capacity(), 64);
    assert_eq!(iv.len(), CUTOFF*2 + 1);
    assert_eq!(&iv[3..6], b"456");
    assert_eq!(&iv[iv.len() - 3..], b"jkl");

    // check realloc slice -> remote
    let mut ivs = iv.subslice(1, 40);
    assert_eq!(ivs.write(b"ZZT").unwrap(), 3);
    assert_eq!(ivs.len(), 43);
    assert_eq!(&iv[iv.len() - 3..], b"jkl"); // should not change
    assert_eq!(&ivs[ivs.len() - 3..], b"ZZT");
    assert_eq!(&ivs[0..3], b"234");

    // check single-byte push
    ivs.push(33);
    assert_eq!(ivs.len(), 44);
    assert_eq!(ivs[43], 33);
}

#[cfg(test)]
mod qc {
    use super::IVec;

    fn prop_identity(ivec: IVec) -> bool {
        let mut iv2 = ivec.clone();

        if iv2 != ivec {
            println!("expected clone to equal original");
            return false;
        }

        if &*ivec != &mut *iv2 {
            println!("expected AsMut to equal original");
            return false;
        }

        if &*ivec != iv2.as_mut() {
            println!("expected AsMut to equal original");
            return false;
        }

        true
    }

    quickcheck::quickcheck! {
        #[cfg_attr(miri, ignore)]
        fn bool(item: IVec) -> bool {
            prop_identity(item)
        }
    }
}

use super::*;

use std::{fmt, ops::Deref};

const INLINE_LEN_MASK: u8 = 0b0000_1111;
const KIND_MASK: u8 = 0b1111_0000;
const KIND_INLINE: u8 = 0b1000_0000;
const KIND_OWNED: u8 = 0b0100_0000;
const KIND_BORROWED: u8 = 0b0010_0000;

#[cfg(target_pointer_width = "64")]
const CUTOFF: usize = 15;
#[cfg(target_pointer_width = "64")]
type Inner = [u8; 16];

#[cfg(target_pointer_width = "32")]
const CUTOFF: usize = 7;
#[cfg(target_pointer_width = "32")]
type Inner = [u8; 8];

#[derive(Default, Ord, Eq, Serialize, Deserialize)]
pub(crate) struct IVec {
    #[serde(with = "ser")]
    data: Inner,
}

impl IVec {
    pub(crate) fn new(v: Vec<u8>) -> IVec {
        if v.len() <= CUTOFF {
            let sz = v.len() as u8;
            let tag = KIND_INLINE | sz;

            let mut data = [0u8; 16];
            data[CUTOFF] = tag;
            data[0..v.len()].copy_from_slice(&v[0..v.len()]);

            IVec { data }
        } else {
            let bs = v.into_boxed_slice();
            let ptr: *mut _ = Box::into_raw(bs);
            let mut data: [u8; 16] =
                unsafe { std::mem::transmute(ptr) };

            assert_eq!(
                data[CUTOFF], 0,
                "we incorrectly assumed that we could use \
                 the highest bits in the length field. please \
                 report this bug ASAP!"
            );

            data[CUTOFF] = KIND_OWNED;

            IVec { data }
        }
    }

    pub(crate) fn take(&mut self) -> IVec {
        assert_ne!(
            self.data[CUTOFF], KIND_BORROWED,
            "take called on Borrowed IVec"
        );
        let ret = IVec { data: self.data };
        if self.data[CUTOFF] == KIND_OWNED {
            self.data[CUTOFF] = KIND_BORROWED;
        }
        ret
    }

    pub(crate) fn borrow(&self) -> IVec {
        let mut ret = IVec { data: self.data };
        if ret.data[CUTOFF] == KIND_OWNED {
            ret.data[CUTOFF] = KIND_BORROWED;
        }
        ret
    }
}

impl From<Vec<u8>> for IVec {
    fn from(v: Vec<u8>) -> IVec {
        IVec::new(v)
    }
}

impl Deref for IVec {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        let tag = self.data[CUTOFF];
        let kind = tag & KIND_MASK;
        match kind {
            k if k == KIND_INLINE => {
                let base = self.data.as_ptr();
                let len = (tag & INLINE_LEN_MASK) as usize;
                unsafe { std::slice::from_raw_parts(base, len) }
            }
            k if (k == KIND_OWNED || k == KIND_BORROWED) => {
                let mut data: [u8; 16] = self.data;
                data[CUTOFF] = 0;

                unsafe {
                    let ptr: *mut [u8] = std::mem::transmute(data);
                    &*ptr
                }
            }
            other => {
                panic!("unknown kind {}", other);
            }
        }
    }
}

impl Clone for IVec {
    fn clone(&self) -> IVec {
        let ret: IVec = self.deref().to_vec().into();
        trace!(
            "cloned IVec pointing to {:?} to one pointing to {:?}",
            self.deref().as_ptr(),
            ret.deref().as_ptr(),
        );
        ret
    }
}

impl Drop for IVec {
    fn drop(&mut self) {
        let tag = self.data[CUTOFF];
        let kind = tag & KIND_MASK;
        match kind {
            k if k == KIND_OWNED => {
                let mut data = self.data.clone();
                data[CUTOFF] = 0;
                unsafe {
                    let ptr: *mut [u8] = std::mem::transmute(data);
                    let b: Box<[u8]> = Box::from_raw(ptr);
                    drop(b);
                }
            }
            _ => {
                // no owned remote storage
            }
        }
    }
}

impl PartialOrd for IVec {
    fn partial_cmp(
        &self,
        other: &IVec,
    ) -> Option<std::cmp::Ordering> {
        Some(self.deref().cmp(other.deref()))
    }
}

impl PartialEq for IVec {
    fn eq(&self, other: &IVec) -> bool {
        self.deref() == other.deref()
    }
}

impl<'a, T: AsRef<[u8]>> PartialEq<T> for IVec {
    fn eq(&self, other: &T) -> bool {
        self.deref() == other.as_ref()
    }
}

impl PartialEq<[u8]> for IVec {
    fn eq(&self, other: &[u8]) -> bool {
        self.deref() == other
    }
}

impl fmt::Debug for IVec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IVec( {:?} )", self.deref())
    }
}

pub(crate) mod ser {
    use super::{IVec, Inner, CUTOFF, KIND_BORROWED, KIND_OWNED};

    use std::ops::Deref;

    use serde::de::{Deserialize, Deserializer};
    use serde::ser::Serializer;

    pub(crate) fn serialize<S>(
        data: &Inner,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut data: Inner = *data;
        if data[CUTOFF] == KIND_OWNED {
            data[CUTOFF] = KIND_BORROWED;
        }

        let iv = IVec { data: data };
        let ivr: &[u8] = iv.deref();
        serializer.collect_seq(ivr)
    }

    pub(crate) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Inner, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = Vec::<u8>::deserialize(deserializer)?;
        let mut iv: IVec = v.into();
        let data: Inner = iv.data;
        iv.data[CUTOFF] = KIND_BORROWED;
        Ok(data)
    }
}

#[test]
fn ivec_usage() {
    let iv1: IVec = vec![1, 2, 3].into();
    assert_eq!(iv1, vec![1, 2, 3]);
    let iv2 = IVec::new(vec![4; 128]);
    assert_eq!(iv2, vec![4; 128]);
}

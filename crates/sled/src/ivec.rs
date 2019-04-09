use serde::{de::Deserializer, ser::Serializer};
use serde::{Deserialize, Serialize};
use std::{fmt, ops::Deref, result::Result as StdResult, sync::Arc};

const CUTOFF: usize = std::mem::size_of::<&[u8]>() - 1;
type Inner = [u8; CUTOFF];

/// A buffer that may either be inline or remote and protected
/// by an Arc
#[derive(Clone, Ord, Eq)]
pub enum IVec {
    /// An inlined small value
    Inline(u8, Inner),
    /// A heap-allocated value protected by an Arc
    Remote {
        /// The value protected by an Arc
        buf: Arc<[u8]>,
    },
}

impl Serialize for IVec {
    fn serialize<S: Serializer>(
        &self,
        serializer: S,
    ) -> StdResult<S::Ok, S::Error> {
        serde_bytes::serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for IVec {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> StdResult<Self, D::Error> {
        serde_bytes::deserialize(deserializer)
    }
}

impl IVec {
    pub(crate) fn new(v: &[u8]) -> IVec {
        if v.len() <= CUTOFF {
            let sz = v.len() as u8;

            let mut data: Inner = [0u8; CUTOFF];

            unsafe {
                std::ptr::copy_nonoverlapping(
                    v.as_ptr(),
                    data.as_mut_ptr(),
                    v.len(),
                );
            }

            IVec::Inline(sz, data)
        } else {
            IVec::Remote { buf: v.into() }
        }
    }

    #[inline]
    pub(crate) fn size_in_bytes(&self) -> u64 {
        if let IVec::Inline(..) = self {
            std::mem::size_of::<IVec>() as u64
        } else {
            let sz = std::mem::size_of::<IVec>() as u64;
            sz.saturating_add(self.len() as u64)
        }
    }
}

impl From<&[u8]> for IVec {
    fn from(v: &[u8]) -> IVec {
        IVec::new(v)
    }
}

impl From<&IVec> for IVec {
    fn from(v: &IVec) -> IVec {
        v.clone()
    }
}

impl From<Vec<u8>> for IVec {
    fn from(v: Vec<u8>) -> IVec {
        if v.len() <= CUTOFF {
            IVec::new(&v)
        } else {
            IVec::Remote {
                // rely on the Arc From specialization
                // for Vec<[T]>, which may improve
                // over time for T's that are Copy
                buf: v.into(),
            }
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
    fn as_ref(&self) -> &[u8] {
        match self {
            IVec::Inline(sz, buf) => unsafe {
                buf.get_unchecked(..*sz as usize)
            },
            IVec::Remote { buf } => buf,
        }
    }
}

impl PartialOrd for IVec {
    fn partial_cmp(&self, other: &IVec) -> Option<std::cmp::Ordering> {
        Some(self.as_ref().cmp(other.as_ref()))
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

impl fmt::Debug for IVec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

#[test]
fn ivec_usage() {
    let iv1: IVec = vec![1, 2, 3].into();
    assert_eq!(iv1, vec![1, 2, 3]);
    let iv2 = IVec::new(&[4; 128]);
    assert_eq!(iv2, vec![4; 128]);
}

use super::*;

use std::{fmt, ops::Deref, sync::Arc};

const CUTOFF: usize = std::mem::size_of::<&[u8]>() - 1;
type Inner = [u8; CUTOFF];

/// A buffer that may either be inline or remote and protected
/// by an Arc
#[derive(Clone, Ord, Eq, Serialize, Deserialize)]
pub enum IVec {
    /// An inlined small value
    Inline(u8, Inner),
    /// A heap-allocated value protected by an Arc
    Remote {
        #[serde(with = "ser")]
        /// The value protected by an Arc
        buf: Arc<[u8]>,
    },
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
        match self {
            IVec::Inline(sz, buf) => unsafe {
                buf.get_unchecked(..*sz as usize)
            },
            IVec::Remote { buf } => buf.as_ref(),
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
        self.deref().fmt(f)
    }
}

pub(crate) mod ser {
    use std::sync::Arc;

    use serde::de::{Deserializer, Visitor};
    use serde::ser::Serializer;

    pub(crate) fn serialize<S>(
        data: &Arc<[u8]>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&**data)
    }

    struct IVecVisitor;

    impl<'de> Visitor<'de> for IVecVisitor {
        type Value = Arc<[u8]>;

        fn expecting(
            &self,
            formatter: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            formatter.write_str("a borrowed byte array")
        }

        #[inline]
        fn visit_borrowed_bytes<E>(
            self,
            v: &'de [u8],
        ) -> Result<Arc<[u8]>, E> {
            Ok(v.into())
        }
    }

    pub(crate) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Arc<[u8]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(IVecVisitor)
    }
}

#[test]
fn ivec_usage() {
    let iv1: IVec = vec![1, 2, 3].into();
    assert_eq!(iv1, vec![1, 2, 3]);
    let iv2 = IVec::new(&[4; 128]);
    assert_eq!(iv2, vec![4; 128]);
}

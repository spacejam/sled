use std::cmp::Ordering;
use std::mem::size_of;

use super::*;

#[derive(Clone, Debug, Ord, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum Bound {
    Inclusive(Vec<u8>),
    Exclusive(Vec<u8>),
    Inf,
}

impl Bound {
    pub(crate) fn inner(&self) -> &[u8] {
        match *self {
            Bound::Inclusive(ref v) | Bound::Exclusive(ref v) => &*v,
            Bound::Inf => panic!("inner() called on Bound::Inf"),
        }
    }

    pub(crate) fn is_inf(&self) -> bool {
        if let Bound::Inf = self {
            true
        } else {
            false
        }
    }

    #[inline]
    pub(crate) fn size_in_bytes(&self) -> u64 {
        let self_sz = size_of::<Bound>() as u64;

        let inner_sz = match self {
            Bound::Inclusive(ref v) | Bound::Exclusive(ref v) => {
                (v.len() as u64)
                    .saturating_add(size_of::<Vec<u8>>() as u64)
            }
            Bound::Inf => 0u64,
        };

        self_sz.saturating_add(inner_sz)
    }
}

impl PartialOrd for Bound {
    fn partial_cmp(&self, other: &Bound) -> Option<Ordering> {
        use self::Bound::*;
        match *self {
            Inclusive(ref lhs) => match *other {
                Inf => Some(Ordering::Less),
                Inclusive(ref rhs) => Some(lhs.cmp(rhs)),
                Exclusive(ref rhs) => {
                    if lhs < rhs {
                        Some(Ordering::Less)
                    } else {
                        Some(Ordering::Greater)
                    }
                }
            },
            Exclusive(ref lhs) => match *other {
                Inf => Some(Ordering::Less),
                Inclusive(ref rhs) => {
                    if lhs <= rhs {
                        Some(Ordering::Less)
                    } else {
                        Some(Ordering::Greater)
                    }
                }
                Exclusive(ref rhs) => Some(lhs.cmp(rhs)),
            },
            Inf => match *other {
                Inf => Some(Ordering::Equal),
                _ => Some(Ordering::Greater),
            },
        }
    }
}

#[test]
fn test_bounds() {
    use self::Bound::*;
    assert_eq!(Inf, Inf);
    assert_eq!(Exclusive(vec![]), Exclusive(vec![]));
    assert_eq!(Inclusive(vec![]), Inclusive(vec![]));
    assert_eq!(Inclusive(b"hi".to_vec()), Inclusive(b"hi".to_vec()));
    assert_eq!(Exclusive(b"hi".to_vec()), Exclusive(b"hi".to_vec()));
    assert!(Inclusive(b"hi".to_vec()) > Exclusive(b"hi".to_vec()));
    assert!(Exclusive(b"hi".to_vec()) < Inclusive(b"hi".to_vec()));
    assert!(Inclusive(b"a".to_vec()) < Inclusive(b"z".to_vec()));
    assert!(Exclusive(b"a".to_vec()) < Exclusive(b"z".to_vec()));
    assert!(Inclusive(b"z".to_vec()) > Inclusive(b"a".to_vec()));
    assert!(Exclusive(b"z".to_vec()) > Exclusive(b"a".to_vec()));
    assert!(Inclusive(vec![]) < Inf);
    assert!(Exclusive(vec![]) < Inf);
    assert!(Inf > Inclusive(vec![0, 0, 0, 0, 0, 0, 136, 184]));
}

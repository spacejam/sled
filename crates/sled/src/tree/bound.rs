use std::cmp::Ordering;

#[derive(Clone, Debug, Ord, Eq, PartialEq, Serialize, Deserialize)]
pub enum Bound {
    Inclusive(Vec<u8>),
    Exclusive(Vec<u8>),
    Inf,
}

impl Bound {
    pub fn inner(&self) -> &[u8] {
        match *self {
            Bound::Inclusive(ref v) |
            Bound::Exclusive(ref v) => &*v,
            Bound::Inf => panic!("inner() called on Bound::Inf"),
        }
    }
}

impl PartialOrd for Bound {
    fn partial_cmp(&self, other: &Bound) -> Option<Ordering> {
        use self::Bound::*;
        match *self {
            Inclusive(ref lhs) => {
                match *other {
                    Inf => Some(Ordering::Less),
                    Inclusive(ref rhs) => Some(lhs.cmp(rhs)),
                    Exclusive(ref rhs) => {
                        if lhs < rhs {
                            Some(Ordering::Less)
                        } else {
                            Some(Ordering::Greater)
                        }
                    }
                }
            }
            Exclusive(ref lhs) => {
                match *other {
                    Inf => Some(Ordering::Less),
                    Inclusive(ref rhs) => {
                        if lhs <= rhs {
                            Some(Ordering::Less)
                        } else {
                            Some(Ordering::Greater)
                        }
                    }
                    Exclusive(ref rhs) => Some(lhs.cmp(rhs)),
                }
            }
            Inf => {
                match *other {
                    Inf => Some(Ordering::Equal),
                    _ => Some(Ordering::Greater),
                }
            }
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
    assert!(Inclusive(vec![]) < Inf);
    assert!(Exclusive(vec![]) < Inf);
    assert!(Inf > Inclusive(vec![0, 0, 0, 0, 0, 0, 136, 184]));
}

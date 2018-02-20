use std::cmp::Ordering;

#[derive(Clone, Debug, Ord, Eq, PartialEq, Serialize, Deserialize)]
pub enum Bound {
    Inc(Vec<u8>),
    Non(Vec<u8>),
    Inf,
}

impl Bound {
    pub fn inner(&self) -> &[u8] {
        match *self {
            Bound::Inc(ref v) |
            Bound::Non(ref v) => &*v,
            Bound::Inf => panic!("inner() called on Bound::Inf"),
        }
    }
}

impl PartialOrd for Bound {
    fn partial_cmp(&self, other: &Bound) -> Option<Ordering> {
        use self::Bound::*;
        match *self {
            Inc(ref lhs) => {
                match *other {
                    Inf => Some(Ordering::Less),
                    Inc(ref rhs) => Some(lhs.cmp(rhs)),
                    Non(ref rhs) => {
                        if lhs < rhs {
                            Some(Ordering::Less)
                        } else {
                            Some(Ordering::Greater)
                        }
                    }
                }
            }
            Non(ref lhs) => {
                match *other {
                    Inf => Some(Ordering::Less),
                    Inc(ref rhs) => {
                        if lhs <= rhs {
                            Some(Ordering::Less)
                        } else {
                            Some(Ordering::Greater)
                        }
                    }
                    Non(ref rhs) => Some(lhs.cmp(rhs)),
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
    assert_eq!(Non(vec![]), Non(vec![]));
    assert_eq!(Inc(vec![]), Inc(vec![]));
    assert_eq!(Inc(b"hi".to_vec()), Inc(b"hi".to_vec()));
    assert_eq!(Non(b"hi".to_vec()), Non(b"hi".to_vec()));
    assert!(Inc(b"hi".to_vec()) > Non(b"hi".to_vec()));
    assert!(Inc(vec![]) < Inf);
    assert!(Non(vec![]) < Inf);
    assert!(Inf > Inc(vec![0, 0, 0, 0, 0, 0, 136, 184]));
}

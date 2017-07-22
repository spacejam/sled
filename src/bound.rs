use std::cmp::Ordering;

/// Provides different values and utility functions for bounds. `Bound`s are e.g. used in trees to
/// set low and high values for leaves.
#[derive(Clone, Debug, Ord, Eq, PartialEq, Serialize, Deserialize)]
pub enum Bound {
    Inc(Vec<u8>),
    Non(Vec<u8>),
    Inf,
}

impl Bound {
    /// Returns the inner bounds.
    pub fn inner(&self) -> Option<Vec<u8>> {
        match self {
            &Bound::Inc(ref v) => Some(v.clone()),
            &Bound::Non(ref v) => Some(v.clone()),
            _ => None,
        }
    }
}

impl PartialOrd for Bound {
    /// Calculates and returns the order of bounds.
    fn partial_cmp(&self, other: &Bound) -> Option<Ordering> {
        use Bound::*;
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
                    Non(ref rhs) => Some(lhs.cmp(&rhs)),
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
    use Bound::*;
    assert!(Inf == Inf);
    assert!(Non(vec![]) == Non(vec![]));
    assert!(Inc(vec![]) == Inc(vec![]));
    assert!(Inc(b"hi".to_vec()) == Inc(b"hi".to_vec()));
    assert!(Non(b"hi".to_vec()) == Non(b"hi".to_vec()));
    assert!(Inc(b"hi".to_vec()) > Non(b"hi".to_vec()));
    assert!(Inc(vec![]) < Inf);
    assert!(Non(vec![]) < Inf);
    assert!(Inf > Inc(vec![0, 0, 0, 0, 0, 0, 136, 184]));
}

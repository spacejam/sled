use std::ops::{Bound, Deref};

use crate::{Measure, M};

use super::*;

#[cfg(any(test, feature = "lock_free_delays"))]
const MAX_LOOPS: usize = usize::max_value();

#[cfg(not(any(test, feature = "lock_free_delays")))]
const MAX_LOOPS: usize = 1_000_000;

fn possible_predecessor(s: &[u8]) -> Option<Vec<u8>> {
    let mut ret = s.to_vec();
    match ret.pop() {
        None => None,
        Some(i) if i == 0 => Some(ret),
        Some(i) => {
            ret.push(i - 1);
            ret.extend_from_slice(&[255; 4]);
            Some(ret)
        }
    }
}

macro_rules! iter_try {
    ($e:expr) => {
        match $e {
            Ok(item) => item,
            Err(e) => return Some(Err(e)),
        }
    };
}

/// An iterator over keys and values in a `Tree`.
pub struct Iter {
    pub(super) tree: Tree,
    pub(super) hi: Bound<IVec>,
    pub(super) lo: Bound<IVec>,
    pub(super) cached_node: Option<(PageId, Node)>,
    pub(super) going_forward: bool,
}

impl Iter {
    /// Iterate over the keys of this Tree
    pub fn keys(
        self,
    ) -> impl DoubleEndedIterator<Item = Result<IVec>> + Send + Sync {
        self.map(|r| r.map(|(k, _v)| k))
    }

    /// Iterate over the values of this Tree
    pub fn values(
        self,
    ) -> impl DoubleEndedIterator<Item = Result<IVec>> + Send + Sync {
        self.map(|r| r.map(|(_k, v)| v))
    }

    fn bounds_collapsed(&self) -> bool {
        match (&self.lo, &self.hi) {
            (Bound::Included(ref start), Bound::Included(ref end))
            | (Bound::Included(ref start), Bound::Excluded(ref end))
            | (Bound::Excluded(ref start), Bound::Included(ref end))
            | (Bound::Excluded(ref start), Bound::Excluded(ref end)) => {
                start > end
            }
            _ => false,
        }
    }

    fn low_key(&self) -> &[u8] {
        match self.lo {
            Bound::Unbounded => &[],
            Bound::Excluded(ref lo) | Bound::Included(ref lo) => lo.as_ref(),
        }
    }

    fn high_key(&self) -> &[u8] {
        const MAX_KEY: &[u8] = &[255; 1024 * 1024];
        match self.hi {
            Bound::Unbounded => MAX_KEY,
            Bound::Excluded(ref hi) | Bound::Included(ref hi) => hi.as_ref(),
        }
    }

    pub(crate) fn next_inner(&mut self) -> Option<<Self as Iterator>::Item> {
        let guard = pin();
        let (mut pid, mut node) = if let (true, Some((pid, node))) =
            (self.going_forward, self.cached_node.take())
        {
            (pid, node)
        } else {
            let view =
                iter_try!(self.tree.view_for_key(self.low_key(), &guard));
            (view.pid, view.deref().clone())
        };

        for _ in 0..MAX_LOOPS {
            if self.bounds_collapsed() {
                return None;
            }

            if !node.contains_upper_bound(&self.lo) {
                // node too low (maybe merged, maybe exhausted?)
                let view =
                    iter_try!(self.tree.view_for_key(self.low_key(), &guard));

                pid = view.pid;
                node = view.deref().clone();
                continue;
            } else if !node.contains_lower_bound(&self.lo, true) {
                // node too high (maybe split, maybe exhausted?)
                let seek_key = possible_predecessor(&node.lo)?;
                let view = iter_try!(self.tree.view_for_key(seek_key, &guard));
                pid = view.pid;
                node = view.deref().clone();
                continue;
            }

            if let Some((key, value)) = node.successor(&self.lo) {
                self.lo = Bound::Excluded(key.clone());
                self.cached_node = Some((pid, node));
                self.going_forward = true;

                match self.hi {
                    Bound::Unbounded => return Some(Ok((key, value))),
                    Bound::Included(ref h) if *h >= key => {
                        return Some(Ok((key, value)));
                    }
                    Bound::Excluded(ref h) if *h > key => {
                        return Some(Ok((key, value)));
                    }
                    _ => return None,
                }
            } else {
                if node.hi.is_empty() {
                    return None;
                }
                self.lo = Bound::Included(node.hi.clone());
                continue;
            }
        }
        panic!(
            "fucked up tree traversal next({:?}) on {:?}",
            self.lo, self.tree
        );
    }
}

impl Iterator for Iter {
    type Item = Result<(IVec, IVec)>;

    fn next(&mut self) -> Option<Self::Item> {
        let _measure = Measure::new(&M.tree_scan);
        let _cc = concurrency_control::read();
        self.next_inner()
    }

    fn last(mut self) -> Option<Self::Item> {
        self.next_back()
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        let _measure = Measure::new(&M.tree_reverse_scan);
        let guard = pin();
        let _cc = concurrency_control::read();

        let (mut pid, mut node) = if let (false, Some((pid, node))) =
            (self.going_forward, self.cached_node.take())
        {
            (pid, node)
        } else {
            let view =
                iter_try!(self.tree.view_for_key(self.high_key(), &guard));
            (view.pid, view.deref().clone())
        };

        for _ in 0..MAX_LOOPS {
            if self.bounds_collapsed() {
                return None;
            }

            if !node.contains_upper_bound(&self.hi) {
                // node too low (maybe merged, maybe exhausted?)
                let view =
                    iter_try!(self.tree.view_for_key(self.high_key(), &guard));

                pid = view.pid;
                node = view.deref().clone();
                continue;
            } else if !node.contains_lower_bound(&self.hi, false) {
                // node too high (maybe split, maybe exhausted?)
                let seek_key = possible_predecessor(&node.lo)?;
                let view = iter_try!(self.tree.view_for_key(seek_key, &guard));
                pid = view.pid;
                node = view.deref().clone();
                continue;
            }

            if let Some((key, value)) = node.predecessor(&self.hi) {
                self.hi = Bound::Excluded(key.clone());
                self.cached_node = Some((pid, node));
                self.going_forward = false;

                match self.lo {
                    Bound::Unbounded => return Some(Ok((key, value))),
                    Bound::Included(ref l) if *l <= key => {
                        return Some(Ok((key, value)));
                    }
                    Bound::Excluded(ref l) if *l < key => {
                        return Some(Ok((key, value)));
                    }
                    _ => return None,
                }
            } else {
                if node.lo.is_empty() {
                    return None;
                }
                self.hi = Bound::Excluded(node.lo.clone());
                continue;
            }
        }
        panic!(
            "fucked up tree traversal next_back({:?}) on {:?}",
            self.hi, self.tree
        );
    }
}

#[test]
fn test_possible_predecessor() {
    assert_eq!(possible_predecessor(b""), None);
    assert_eq!(possible_predecessor(&[0]), Some(vec![]));
    assert_eq!(possible_predecessor(&[0, 0]), Some(vec![0]));
    assert_eq!(
        possible_predecessor(&[0, 1]),
        Some(vec![0, 0, 255, 255, 255, 255])
    );
    assert_eq!(
        possible_predecessor(&[0, 2]),
        Some(vec![0, 1, 255, 255, 255, 255])
    );
    assert_eq!(possible_predecessor(&[1, 0]), Some(vec![1]));
    assert_eq!(
        possible_predecessor(&[1, 1]),
        Some(vec![1, 0, 255, 255, 255, 255])
    );
    assert_eq!(
        possible_predecessor(&[155]),
        Some(vec![154, 255, 255, 255, 255])
    );
}

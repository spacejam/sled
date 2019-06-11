use std::ops::Bound;

use pagecache::{Measure, M};

use super::*;

const MAX_LOOPS: usize = 10_000;

fn possible_predecessor(s: &[u8]) -> Option<Vec<u8>> {
    let mut ret = s.to_vec();
    match ret.pop() {
        None => None,
        Some(i) if i == 0 => Some(ret),
        Some(i) => {
            ret.push(i - 1);
            for _ in 0..4 {
                ret.push(255);
            }
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
pub struct Iter<'a> {
    pub(super) tree: &'a Tree,
    pub(super) hi: Bound<IVec>,
    pub(super) lo: Bound<IVec>,
    pub(super) cached_view: Option<View<'a>>,
    pub(super) tx: Result<Tx<'a, BLinkMaterializer, Frag>>,
    pub(super) going_forward: bool,
}

impl<'a> Iter<'a> {
    /// Iterate over the keys of this Tree
    pub fn keys(self) -> impl 'a + DoubleEndedIterator<Item = Result<IVec>> {
        self.map(|r| r.map(|(k, _v)| k))
    }

    /// Iterate over the values of this Tree
    pub fn values(self) -> impl 'a + DoubleEndedIterator<Item = Result<IVec>> {
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
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<(IVec, IVec)>;

    fn next(&mut self) -> Option<Self::Item> {
        let _measure = Measure::new(&M.tree_scan);

        let tx: &'a Tx<'a, _, _> = match self.tx {
            Ok(ref tx) => {
                let tx_ref = tx as *const Tx<'a, _, _>;
                unsafe { &*tx_ref as &'a Tx<'a, _, _> }
            }
            Err(ref e) => return Some(Err(e.clone())),
        };

        let mut view = match (self.going_forward, self.cached_view.take()) {
            (true, Some(view)) => view,
            _ => iter_try!(self.tree.view_for_key(self.low_key(), &tx)),
        };

        for _ in 0..MAX_LOOPS {
            if self.bounds_collapsed() {
                return None;
            }

            if !view.contains_upper_bound(&self.lo) {
                // view too low (maybe merged, maybe exhausted?)
                let next_pid = view.next?;
                if let Some(v) =
                    iter_try!(self.tree.view_for_pid(next_pid, &tx))
                {
                    view = v;
                }
                continue;
            } else if !view.contains_lower_bound(&self.lo, true) {
                // view too high (maybe split, maybe exhausted?)
                let seek_key = possible_predecessor(view.lo)?;
                view = iter_try!(self.tree.view_for_key(seek_key, &tx));
                continue;
            }

            if let Some((key, value)) =
                view.successor(&self.lo, &self.tree.context)
            {
                self.lo = Bound::Excluded(key.clone());
                self.cached_view = Some(view);
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
                if view.hi.is_empty() {
                    return None;
                }
                self.lo = Bound::Included(view.hi.clone());
                continue;
            }
        }
        panic!(
            "fucked up tree traversal next({:?}) on {:?}",
            self.lo, self.tree
        );
    }
}

impl<'a> DoubleEndedIterator for Iter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let _measure = Measure::new(&M.tree_reverse_scan);

        let tx: &'a Tx<'a, _, _> = match self.tx {
            Ok(ref tx) => {
                let tx_ref = tx as *const Tx<'a, _, _>;
                unsafe { &*tx_ref as &'a Tx<'a, _, _> }
            }
            Err(ref e) => return Some(Err(e.clone())),
        };

        let mut view = match (self.going_forward, self.cached_view.take()) {
            (false, Some(view)) => view,
            _ => iter_try!(self.tree.view_for_key(self.high_key(), &tx)),
        };

        for _ in 0..MAX_LOOPS {
            if self.bounds_collapsed() {
                return None;
            }

            if !view.contains_upper_bound(&self.hi) {
                // view too low (maybe merged, maybe exhausted?)
                let next_pid = view.next?;
                if let Some(v) =
                    iter_try!(self.tree.view_for_pid(next_pid, &tx))
                {
                    view = v;
                }
                continue;
            } else if !view.contains_lower_bound(&self.hi, false) {
                // view too high (maybe split, maybe exhausted?)
                let seek_key = possible_predecessor(view.lo)?;
                view = iter_try!(self.tree.view_for_key(seek_key, &tx));
                continue;
            }

            if let Some((key, value)) =
                view.predecessor(&self.hi, &self.tree.context)
            {
                self.hi = Bound::Excluded(key.clone());
                self.cached_view = Some(view);
                self.going_forward = false;

                match self.lo {
                    Bound::Unbounded => return Some(Ok((key, value))),
                    Bound::Included(ref l) if *l <= key => {
                        return Some(Ok((key, value)));
                    }
                    Bound::Excluded(ref l) if *l < key => {
                        return Some(Ok((key, value)));
                    }
                    _ => {
                        return None;
                    }
                }
            } else {
                if view.lo.is_empty() {
                    return None;
                }
                self.hi = Bound::Excluded(view.lo.clone());
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

use std::ops;

use pagecache::{Measure, M};

use super::*;

fn lower_bound_includes<'a>(
    lb: &ops::Bound<Vec<u8>>,
    item: &'a [u8],
) -> bool {
    match lb {
        ops::Bound::Included(ref start) => start.as_slice() <= item,
        ops::Bound::Excluded(ref start) => start.as_slice() < item,
        ops::Bound::Unbounded => true,
    }
}

fn upper_bound_includes<'a>(
    ub: &ops::Bound<Vec<u8>>,
    item: &'a [u8],
) -> bool {
    match ub {
        ops::Bound::Included(ref end) => item <= end.as_ref(),
        ops::Bound::Excluded(ref end) => item < end.as_ref(),
        ops::Bound::Unbounded => true,
    }
}

/// An iterator over keys and values in a `Tree`.
pub struct Iter<'a> {
    pub(super) tree: &'a Tree,
    pub(super) hi: ops::Bound<Vec<u8>>,
    pub(super) lo: ops::Bound<Vec<u8>>,
    pub(super) last_id: Option<PageId>,
    pub(super) last_key: Option<Key>,
    pub(super) broken: Option<Error>,
    pub(super) done: bool,
    pub(super) tx: Tx,
    pub(super) is_scan: bool,
    // TODO we have to refactor this in light of pages being deleted
}

impl<'a> Iter<'a> {
    /// Iterate over the keys of this Tree
    pub fn keys(
        self,
    ) -> impl 'a + DoubleEndedIterator<Item = Result<Vec<u8>>> {
        self.map(|r| r.map(|(k, _v)| k))
    }

    /// Iterate over the values of this Tree
    pub fn values(
        self,
    ) -> impl 'a + DoubleEndedIterator<Item = Result<IVec>> {
        self.map(|r| r.map(|(_k, v)| v))
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<(Vec<u8>, IVec)>;

    fn next(&mut self) -> Option<Self::Item> {
        let _measure = Measure::new(&M.tree_scan);

        if self.done {
            return None;
        } else if let Some(broken) = self.broken.take() {
            self.done = true;
            return Some(Err(broken));
        };

        let start_bound = &self.lo;
        let start: &[u8] = match start_bound {
            ops::Bound::Included(ref start)
            | ops::Bound::Excluded(ref start) => start.as_ref(),
            ops::Bound::Unbounded => b"",
        };

        let mut spins = 0;

        loop {
            spins += 1;
            debug_assert_ne!(
                spins, 100,
                "forward iterator stuck in loop \
                 looking for key after {:?} \
                 with start_bound {:?} \
                 on tree: \n \
                 {:?}",
                self.last_key, start, self.tree,
            );

            if self.last_id.is_none() {
                // initialize iterator based on valid bound
                let path_res =
                    self.tree.path_for_key(start, &self.tx);
                if let Err(e) = path_res {
                    error!("iteration failed: {:?}", e);
                    self.done = true;
                    return Some(Err(e));
                }

                let path = path_res.unwrap();

                let (last_frag, _tree_ptr) =
                    path.last().expect("path should never be empty");
                let last_node = last_frag.unwrap_base();
                self.last_id = Some(last_node.id);
            }

            let last_id = self.last_id.unwrap();

            let inclusive = match self.lo {
                ops::Bound::Unbounded | ops::Bound::Included(..) => {
                    true
                }
                ops::Bound::Excluded(..) => false,
            };

            let res = self
                .tree
                .context
                .pagecache
                .get(last_id, &self.tx)
                .map(|page_get| page_get.unwrap());

            if let Err(e) = res {
                error!("iteration failed: {:?}", e);
                self.done = true;
                return Some(Err(e));
            }

            // TODO (when implementing merge support) this could
            // be None if the node was removed since the last
            // iteration, and we need to just get the inner
            // node again...
            let (frag, _ptr) = res.unwrap();
            let node = frag.unwrap_base();
            let leaf =
                node.data.leaf_ref().expect("node should be a leaf");
            let prefix = &node.lo;

            let search = if inclusive && self.last_key.is_none() {
                leaf.binary_search_by(|&(ref k, ref _v)| {
                    prefix_cmp_encoded(k, start, prefix)
                })
                .ok()
                .or_else(|| {
                    binary_search_gt(leaf, |&(ref k, ref _v)| {
                        prefix_cmp_encoded(k, start, prefix)
                    })
                })
            } else {
                let last_key = &self.last_key;

                let search_key: &[u8] = if let Some(lk) = last_key {
                    lk.as_ref()
                } else {
                    start
                };

                binary_search_gt(leaf, |&(ref k, ref _v)| {
                    prefix_cmp_encoded(k, search_key, prefix)
                })
            };

            if let Some(idx) = search {
                let (k, v) = &leaf[idx];
                let decoded_k = prefix_decode(prefix, &k);

                if !upper_bound_includes(&self.hi, &*decoded_k) {
                    // we've overshot our bounds
                    return None;
                }

                self.last_key = Some(decoded_k.clone());

                let ret = Ok((decoded_k, v.clone()));
                return Some(ret);
            }

            if !node.hi.is_empty()
                && !upper_bound_includes(&self.hi, &node.hi)
            {
                // we've overshot our bounds
                return None;
            }

            // we need to seek to the right sibling to find a
            // key that is greater than our last key (or
            // the start bound)
            match node.next {
                Some(id) => self.last_id = Some(id),
                None => {
                    assert_eq!(
                        node.hi,
                        vec![],
                        "if a node has no right sibling, \
                         it must be the upper-bound node"
                    );
                    return None;
                }
            }
        }
    }
}

impl<'a> DoubleEndedIterator for Iter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let _measure = Measure::new(&M.tree_scan);

        if self.done {
            return None;
        } else if let Some(broken) = self.broken.take() {
            self.done = true;
            return Some(Err(broken));
        };

        // try to get a high key
        let end_bound = &self.hi;
        let start_bound = &self.lo;

        let (end, unbounded): (&[u8], bool) = if self.is_scan {
            match start_bound {
                ops::Bound::Included(ref start)
                | ops::Bound::Excluded(ref start) => {
                    (start.as_ref(), false)
                }
                ops::Bound::Unbounded => (&[255; 100], true),
            }
        } else {
            match end_bound {
                ops::Bound::Included(ref start)
                | ops::Bound::Excluded(ref start) => {
                    (start.as_ref(), false)
                }
                ops::Bound::Unbounded => (&[255; 100], true),
            }
        };

        let mut spins = 0;

        loop {
            spins += 1;
            debug_assert_ne!(
                spins, 100,
                "reverse iterator stuck in loop \
                 looking for key before {:?} \
                 with end_bound {:?} and \
                 start bound {:?} on tree: \n \
                 {:?}",
                self.last_key, end, start_bound, self.tree,
            );

            if self.last_id.is_none() {
                // initialize iterator based on valid bound

                let path_res = self.tree.path_for_key(end, &self.tx);
                if let Err(e) = path_res {
                    error!("iteration failed: {:?}", e);
                    self.done = true;
                    return Some(Err(e));
                }

                let path = path_res.unwrap();

                let (last_frag, _tree_ptr) =
                    path.last().expect("path should never be empty");
                let mut last_node = last_frag.unwrap_base();

                // (when hi is empty, it means it's the rightmost node)
                while unbounded && !last_node.hi.is_empty() {
                    // if we're unbounded, scan to the end
                    let res = self
                        .tree
                        .context
                        .pagecache
                        .get(last_node.next.unwrap(), &self.tx)
                        .map(|page_get| page_get.unwrap());

                    if let Err(e) = res {
                        error!("iteration failed: {:?}", e);
                        self.done = true;
                        return Some(Err(e));
                    }
                    let (frag, _ptr) = res.unwrap();
                    last_node = frag.unwrap_base();
                }

                self.last_id = Some(last_node.id);
            }

            let last_id = self.last_id.unwrap();

            let inclusive = match self.hi {
                ops::Bound::Unbounded | ops::Bound::Included(..) => {
                    true
                }
                ops::Bound::Excluded(..) => false,
            };

            let res = self
                .tree
                .context
                .pagecache
                .get(last_id, &self.tx)
                .map(|page_get| page_get.unwrap());

            if let Err(e) = res {
                error!("iteration failed: {:?}", e);
                self.done = true;
                return Some(Err(e));
            }

            // TODO (when implementing merge support) this could
            // be None if the node was removed since the last
            // iteration, and we need to just get the inner
            // node again...
            let (frag, _ptr) = res.unwrap();
            let node = frag.unwrap_base();
            let leaf =
                node.data.leaf_ref().expect("node should be a leaf");
            let prefix = &node.lo;
            let mut split_detected = false;
            let mut split_key: &[u8] = &[];

            let search = if inclusive && self.last_key.is_none() {
                if unbounded {
                    if leaf.is_empty() {
                        None
                    } else {
                        Some(leaf.len() - 1)
                    }
                } else {
                    binary_search_lub(leaf, |&(ref k, ref _v)| {
                        prefix_cmp_encoded(k, end, prefix)
                    })
                }
            } else {
                let last_key = &self.last_key;

                let search_key: &[u8] = if let Some(lk) = last_key {
                    lk.as_ref()
                } else {
                    end
                };

                if !node.hi.is_empty() && &*node.hi < search_key {
                    // the node has been split since we saw it,
                    // and we need to scan forward
                    split_detected = true;
                    split_key = search_key;

                    None
                } else {
                    binary_search_lt(leaf, |&(ref k, ref _v)| {
                        prefix_cmp_encoded(k, search_key, prefix)
                    })
                }
            };

            if let Some(idx) = search {
                let (k, v) = &leaf[idx];
                let decoded_k = prefix_decode(prefix, &k);

                if !self.is_scan
                    && !lower_bound_includes(&self.lo, &*decoded_k)
                {
                    // we've overshot our bounds
                    return None;
                }

                self.last_key = Some(decoded_k.clone());

                let ret = Ok((decoded_k, v.clone()));
                return Some(ret);
            }

            if !self.is_scan
                && !lower_bound_includes(&self.lo, &node.lo)
            {
                // we've overshot our bounds
                return None;
            }

            let mut next_node = if split_detected {
                // we need to skip ahead to get to the node
                // where our last key resided
                node
            } else {
                // we need to get the node to the left of ours by
                // guessing a key that might land on it, and then
                // fast-forwarding through the right child pointers
                // if we went too far to the left.
                let pred = possible_predecessor(prefix)?;
                match self.tree.path_for_key(pred, &self.tx) {
                    Err(e) => {
                        error!("next_back iteration failed: {:?}", e);
                        self.done = true;
                        return Some(Err(e));
                    }
                    Ok(path) => path.last().unwrap().0.unwrap_base(),
                }
            };

            // If we did not detect a split, we need to
            // seek until the node that points to our last one.
            // If we detected a split, we need to seek until
            // the new node that contains our last key.
            while (!split_detected
                && (next_node.next != Some(node.id))
                && next_node.lo < node.lo)
                || (split_detected && &*next_node.hi < split_key)
            {
                let res = self
                    .tree
                    .context
                    .pagecache
                    .get(next_node.next?, &self.tx)
                    .map(|page_get| page_get.unwrap());

                if let Err(e) = res {
                    error!("iteration failed: {:?}", e);
                    self.done = true;
                    return Some(Err(e));
                }
                let (frag, _ptr) = res.unwrap();
                next_node = frag.unwrap_base();
            }

            if split_detected && next_node.data.is_empty() {
                // we want to mark this node's lo key
                // as our last key to prevent infinite
                // search loops, by enforcing reverse
                // progress.
                self.last_key = Some(next_node.lo.to_vec());
            }

            if !split_detected {
                self.last_key = Some(node.lo.to_vec());
            }

            self.last_id = Some(next_node.id);
        }
    }
}

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

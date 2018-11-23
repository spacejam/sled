use pagecache::{Measure, M};

use super::*;

/// An iterator over keys and values in a `Tree`.
pub struct Iter<'a> {
    pub(super) id: PageId,
    pub(super) inner: &'a Tree,
    pub(super) last_key: Key,
    pub(super) inclusive: bool,
    pub(super) broken: Option<Error<()>>,
    pub(super) done: bool,
    pub(super) guard: Guard,
    // TODO we have to refactor this in light of pages being deleted
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<(Vec<u8>, PinnedValue), ()>;

    fn next(&mut self) -> Option<Self::Item> {
        let _measure = Measure::new(&M.tree_scan);

        if self.done {
            return None;
        } else if let Some(broken) = self.broken.take() {
            self.done = true;
            return Some(Err(broken));
        };

        loop {
            let pin_guard = self.guard.clone();
            let get_guard = self.guard.clone();

            let res = self
                .inner
                .pages
                .get(self.id, &get_guard)
                .map(|page_get| page_get.unwrap());

            if let Err(e) = res {
                error!("iteration failed: {:?}", e);
                self.done = true;
                return Some(Err(e.danger_cast()));
            }

            // TODO (when implementing merge support) this could
            // be None if the node was removed since the last
            // iteration, and we need to just get the inner
            // node again...
            let (frag, _ptr) = res.unwrap();
            let node = frag.unwrap_base();
            let leaf =
                node.data.leaf_ref().expect("node should be a leaf");
            let prefix = node.lo.inner();

            let search = if self.inclusive {
                self.inclusive = false;
                leaf.binary_search_by(|&(ref k, ref _v)| {
                    prefix_cmp_encoded(k, &self.last_key, prefix)
                }).ok()
                .or_else(|| {
                    binary_search_gt(leaf, |&(ref k, ref _v)| {
                        prefix_cmp_encoded(k, &self.last_key, prefix)
                    })
                })
            } else {
                binary_search_gt(leaf, |&(ref k, ref _v)| {
                    prefix_cmp_encoded(k, &self.last_key, prefix)
                })
            };

            if let Some(idx) = search {
                let (k, v) = &leaf[idx];
                let decoded_k = prefix_decode(prefix, &k);
                self.last_key = decoded_k.to_vec();
                let ret =
                    Ok((decoded_k, PinnedValue::new(&*v, pin_guard)));
                return Some(ret);
            }

            match node.next {
                Some(id) => self.id = id,
                None => {
                    assert_eq!(
                        node.hi,
                        Bound::Inf,
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

        loop {
            let pin_guard = self.guard.clone();
            let get_guard = self.guard.clone();

            let res = self
                .inner
                .pages
                .get(self.id, &get_guard)
                .map(|page_get| page_get.unwrap());

            if let Err(e) = res {
                error!("iteration failed: {:?}", e);
                self.done = true;
                return Some(Err(e.danger_cast()));
            }

            // TODO (when implementing merge support) this could
            // be None if the node was removed since the last
            // iteration, and we need to just get the inner
            // node again...
            let (frag, _ptr) = res.unwrap();
            let node = frag.unwrap_base();
            let leaf =
                node.data.leaf_ref().expect("node should be a leaf");
            let prefix = node.lo.inner();

            let search = if self.inclusive {
                self.inclusive = false;
                leaf.binary_search_by(|&(ref k, ref _v)| {
                    prefix_cmp_encoded(k, &self.last_key, prefix)
                }).ok()
                .or_else(|| {
                    binary_search_lt(leaf, |&(ref k, ref _v)| {
                        prefix_cmp_encoded(k, &self.last_key, prefix)
                    })
                })
            } else {
                binary_search_lt(leaf, |&(ref k, ref _v)| {
                    prefix_cmp_encoded(k, &self.last_key, prefix)
                })
            };

            if let Some(idx) = search {
                let (k, v) = &leaf[idx];
                let decoded_k = prefix_decode(prefix, &k);
                self.last_key = decoded_k.to_vec();
                let ret =
                    Ok((decoded_k, PinnedValue::new(&*v, pin_guard)));
                return Some(ret);
            }

            // we need to get the node to the right of ours by
            // guessing a key that might land on it, and then
            // fast-forwarding through the right child pointers
            // if we went too far to the left.
            let pred = possible_predecessor(prefix)?;
            let mut next_node =
                match self.inner.path_for_key(pred, &get_guard) {
                    Err(e) => {
                        error!("next_back iteration failed: {:?}", e);
                        self.done = true;
                        return Some(Err(e.danger_cast()));
                    }
                    Ok(path) => path.last().unwrap().0.unwrap_base(),
                };
            while next_node.next != Some(node.id)
                && next_node.lo < node.lo
            {
                let res = self
                    .inner
                    .pages
                    .get(next_node.next?, &get_guard)
                    .map(|page_get| page_get.unwrap());

                if let Err(e) = res {
                    error!("iteration failed: {:?}", e);
                    self.done = true;
                    return Some(Err(e.danger_cast()));
                }
                let (frag, _ptr) = res.unwrap();
                next_node = frag.unwrap_base();
            }

            self.id = next_node.id;
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

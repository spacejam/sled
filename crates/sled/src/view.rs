use super::*;

use std::{collections::HashSet, ops::Bound};

#[derive(Debug, Clone)]
pub(crate) struct View<'a> {
    pub(crate) pid: PageId,
    pub(crate) lo: &'a IVec,
    pub(crate) hi: &'a IVec,
    pub(crate) is_index: bool,
    pub(crate) next: Option<PageId>,
    pub(crate) ptr: TreePtr<'a>,
    frags: Vec<&'a Frag>,
    base_offset: usize,
    pub(crate) base_data: &'a Data,
    pub(crate) merging_child: Option<PageId>,
    pub(crate) merging: bool,
    min_children: usize,
    max_children: usize,
    size: u64,
}

impl<'a> View<'a> {
    pub(crate) fn new(
        pid: PageId,
        ptr: TreePtr<'a>,
        frags: Vec<&'a Frag>,
    ) -> View<'a> {
        let mut merging = false;
        let mut merging_child = None;
        let mut last_merge_confirmed = false;
        let mut min_children: isize = 0;
        let mut max_children: isize = 0;
        let mut size = 0;

        for (offset, frag) in frags.iter().enumerate() {
            size += frag.size_in_bytes();
            match frag {
                Frag::Base(node) => {
                    // NB if we re-add ParentSplit, we must
                    // handle the hi & next members differently
                    // here

                    min_children += node.data.len() as isize;
                    max_children += node.data.len() as isize;

                    if !last_merge_confirmed {
                        merging_child = merging_child.or(node.merging_child);
                    }

                    merging |= node.merging;

                    return View {
                        hi: &node.hi,
                        lo: &node.lo,
                        is_index: node.data.index_ref().is_some(),
                        next: node.next,
                        base_offset: offset,
                        base_data: &node.data,
                        pid,
                        ptr,
                        frags,
                        merging,
                        merging_child,
                        min_children: std::cmp::max(0, min_children) as usize,
                        max_children: std::cmp::max(0, max_children) as usize,
                        size,
                    };
                }
                Frag::ParentMergeIntention(pid) => {
                    if !last_merge_confirmed {
                        merging_child = merging_child.or_else(|| Some(*pid));
                    }
                }
                Frag::ParentMergeConfirm => {
                    if merging_child.is_none() {
                        last_merge_confirmed = true;
                    }
                }
                Frag::ChildMergeCap => {
                    assert!(!merging);
                    assert_eq!(offset, 0, "frags: {:?}", frags);
                    merging = true;
                }
                Frag::Del(..) => {
                    min_children -= 1;
                    max_children -= 1;
                }
                Frag::Set(..) | Frag::Merge(..) => max_children += 1,
            }
        }

        panic!("view was never initialized with a base")
    }

    pub(crate) fn contains_upper_bound(&self, bound: &Bound<IVec>) -> bool {
        match bound {
            Bound::Excluded(bound) if self.hi >= bound => true,
            Bound::Included(bound) if self.hi > bound => true,
            _ => self.hi.is_empty(),
        }
    }

    pub(crate) fn contains_lower_bound(
        &self,
        bound: &Bound<IVec>,
        is_forward: bool,
    ) -> bool {
        match bound {
            Bound::Excluded(bound)
                if self.lo < bound || (is_forward && bound == self.lo) =>
            {
                true
            }
            Bound::Included(bound) if self.lo <= bound => true,
            Bound::Unbounded if !is_forward => self.hi.is_empty(),
            _ => self.lo.is_empty(),
        }
    }

    fn keys(&self) -> Vec<&IVec> {
        let mut keys: HashSet<&IVec> = self
            .base_data
            .leaf_ref()
            .unwrap()
            .iter()
            .map(|(k, _v)| k)
            .collect();

        for offset in (0..self.base_offset).rev() {
            match self.frags[offset] {
                Frag::Set(k, _) => {
                    keys.insert(k);
                }
                Frag::Del(k) => {
                    keys.remove(k);
                }
                Frag::Merge(k, _) => {
                    keys.insert(k);
                }
                Frag::Base(_) => {
                    panic!(
                        "somehow hit 2 base nodes while \
                         searching for a successor"
                    );
                }
                Frag::ChildMergeCap => {
                    assert_eq!(offset, 0);
                }
                Frag::ParentMergeIntention(_) | Frag::ParentMergeConfirm => {
                    panic!(
                        "somehow hit parent merge \
                         frags while searching for a \
                         successor"
                    )
                }
            }
        }

        let mut keys: Vec<_> = keys.into_iter().collect();
        keys.sort_unstable_by(|a, b| prefix_cmp(&a, &b));

        keys
    }

    pub(crate) fn successor(
        &self,
        bound: &Bound<IVec>,
        config: &Config,
    ) -> Option<(IVec, IVec)> {
        assert!(!self.is_index);

        // This encoding happens this way because
        // keys cannot be lower than the node's lo key.
        let predecessor_key = match bound {
            Bound::Unbounded => prefix_encode(self.lo, self.lo),
            Bound::Included(b) => {
                let max = std::cmp::max(b, self.lo);
                prefix_encode(self.lo, max)
            }
            Bound::Excluded(b) => {
                let max = std::cmp::max(b, self.lo);
                prefix_encode(self.lo, max)
            }
        };

        let keys = self.keys();

        let successor_keys = keys.iter().filter(|k| {
            let ord = prefix_cmp(k, &predecessor_key);

            ord == std::cmp::Ordering::Greater
                || ord == std::cmp::Ordering::Equal
        });

        for encoded_key in successor_keys {
            let decoded_key = prefix_decode(self.lo, &encoded_key);

            if let Bound::Excluded(e) = bound {
                if e == &decoded_key {
                    // skip this excluded key
                    continue;
                }
            }

            // try to get this key until it works
            if let Some(value) = self.leaf_value_for_key(&decoded_key, config) {
                return Some((IVec::from(decoded_key), value));
            }
        }

        None
    }

    pub(crate) fn predecessor(
        &self,
        bound: &Bound<IVec>,
        config: &Config,
    ) -> Option<(IVec, IVec)> {
        assert!(!self.is_index);

        // This encoding happens this way because
        // the rightmost (unbounded) node has
        // a hi key represented by the empty slice
        let successor_key = match bound {
            Bound::Unbounded => {
                if self.hi.is_empty() {
                    prefix_encode(self.lo, &[255; 1024 * 1024])
                } else {
                    prefix_encode(self.lo, self.hi)
                }
            }
            Bound::Included(b) => {
                let min = if self.hi.is_empty() {
                    b
                } else {
                    std::cmp::min(b, self.hi)
                };
                prefix_encode(self.lo, min)
            }
            Bound::Excluded(b) => {
                let min = if self.hi.is_empty() {
                    b
                } else {
                    std::cmp::min(b, self.hi)
                };
                prefix_encode(self.lo, min)
            }
        };

        let keys = self.keys();

        let predecessor_keys = keys.iter().filter(|k| {
            let ord = prefix_cmp(k, &successor_key);

            ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal
        });

        for encoded_key in predecessor_keys.rev() {
            let decoded_key = prefix_decode(self.lo, &encoded_key);

            if let Bound::Excluded(e) = bound {
                if e == &decoded_key {
                    // skip this excluded key
                    continue;
                }
            }

            // try to get this key until it works
            if let Some(value) = self.leaf_value_for_key(&decoded_key, config) {
                return Some((IVec::from(decoded_key), value));
            }
        }

        None
    }

    pub(crate) fn leaf_value_for_key(
        &self,
        key: &[u8],
        config: &Config,
    ) -> Option<IVec> {
        assert!(!self.is_index);

        let mut merge_base = None;
        let mut merges = vec![];

        for frag in self.frags[..=self.base_offset].iter() {
            match frag {
                Frag::Set(k, val) if self.key_eq(k, key) => {
                    if merges.is_empty() {
                        return Some(val.clone());
                    } else {
                        merge_base = Some(val);
                        break;
                    }
                }
                Frag::Del(k) if self.key_eq(k, key) => {
                    // we should ignore "earlier" Frag's for
                    // this key, but still need to handle
                    // merges we encountered "after" this
                    // deletion
                    break;
                }
                Frag::Merge(k, val) if self.key_eq(k, key) => merges.push(val),
                Frag::Base(node) => {
                    let data = &node.data;
                    let items =
                        data.leaf_ref().expect("last_node should be a leaf");
                    let search = items
                        .binary_search_by(|&(ref k, ref _v)| {
                            prefix_cmp_encoded(k, key, &self.lo)
                        })
                        .ok();

                    let val = search.map(|idx| &items[idx].1);
                    if merges.is_empty() {
                        return val.cloned();
                    } else {
                        merge_base = val;
                    }
                }
                _ => {}
            }
        }

        if merges.is_empty() {
            None
        } else {
            let merge_fn_ptr = config
                .merge_operator
                .expect("must have a merge operator set");

            unsafe {
                let merge_fn: MergeOperator = std::mem::transmute(merge_fn_ptr);

                let mut ret = merge_fn(
                    key,
                    merge_base.map(|iv| &**iv),
                    &merges.pop().unwrap(),
                );
                                       ;
                for merge in merges.into_iter().rev() {
                    if let Some(v) = ret {
                        ret = merge_fn(key, Some(&*v), merge);
                    } else {
                        ret = merge_fn(key, None, merge);
                    }
                }

                ret.map(IVec::from)
            }
        }
    }

    #[inline]
    fn key_eq(&self, encoded: &[u8], not_encoded: &[u8]) -> bool {
        prefix_cmp_encoded(encoded, not_encoded, self.lo)
            == std::cmp::Ordering::Equal
    }

    pub(crate) fn index_next_node(&self, key: &[u8]) -> (usize, PageId) {
        assert!(self.is_index);
        let removed = self.removed_children();

        let items = self
            .base_data
            .index_ref()
            .expect("last_node should be a leaf");

        let search = binary_search_lub(items, |&(ref k, ref _v)| {
            prefix_cmp_encoded(k, key, &self.lo)
        });

        // This might be none if ord is Less and we're
        // searching for the empty key
        let mut index = search.expect("failed to traverse index");

        while removed.contains(&items[index].1) {
            index = index
                .checked_sub(1)
                .expect("leftmost child should never have been merged");
        }

        (index, items[index].1)
    }

    pub(crate) fn removed_children(&self) -> Vec<PageId> {
        self.frags[..=self.base_offset]
            .iter()
            .filter_map(|frag| match frag {
                Frag::ParentMergeIntention(child_pid) => Some(*child_pid),
                Frag::Base(node) => node.merging_child,
                _ => None,
            })
            .collect()
    }

    pub(crate) fn should_split(&self, max_sz: u64) -> bool {
        let size_checks = self.min_children > 4 && self.size > max_sz;
        let safety_checks = self.merging_child.is_none() && !self.merging;

        size_checks && safety_checks
    }

    pub(crate) fn should_merge(&self, min_sz: u64) -> bool {
        let size_checks = self.max_children < 2 && self.size < min_sz;
        let safety_checks = self.merging_child.is_none() && !self.merging;

        size_checks && safety_checks
    }

    pub(crate) fn can_merge_child(&self) -> bool {
        self.merging_child.is_none() && !self.merging
    }

    pub(crate) fn compact(&self, config: &Config) -> Node {
        let mut merged = self.frags[self.base_offset].unwrap_base().clone();
        for offset in (0..self.base_offset).rev() {
            let frag = self.frags[offset];
            merged.apply(frag, config.merge_operator);
        }

        assert_eq!(merged.merging_child, self.merging_child);
        assert_eq!(merged.hi, self.hi);
        assert_eq!(merged.lo, self.lo);

        merged
    }

    pub(crate) fn split(&self, config: &Config) -> (Node, Node) {
        let mut lhs = self.compact(config);
        let rhs = lhs.split();

        lhs.data.drop_gte(&rhs.lo, &lhs.lo);
        lhs.hi = rhs.lo.clone();

        // intentionally make this the end to make
        // any issues pop out with setting it
        // correctly after the split.
        lhs.next = None;

        (lhs, rhs)
    }
}

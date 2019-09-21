use std::{fmt, ops::Bound};

use crate::Lazy;

use super::*;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Node {
    pub(crate) data: Data,
    pub(crate) next: Option<PageId>,
    pub(crate) lo: IVec,
    pub(crate) hi: IVec,
    pub(crate) merging_child: Option<PageId>,
    pub(crate) merging: bool,
}

impl Debug for Node {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        let data = self.data.fmt_keys(&self.lo);

        write!(
            f,
            "Node {{ \
             lo: {:?} \
             hi: {:?} \
             next: {:?} \
             merging_child: {:?} \
             merging: {} \
             data: {:?} }}",
            self.lo, self.hi, self.next, self.merging_child, self.merging, data
        )
    }
}

impl Node {
    pub(crate) fn apply(&mut self, frag: &Frag) {
        use self::Frag::*;

        assert!(
            !self.merging,
            "somehow a frag was applied to a node after it was merged"
        );

        match *frag {
            Set(ref k, ref v) => {
                // (when hi is empty, it means it's unbounded)
                if self.hi.is_empty()
                    || prefix_cmp_encoded(k, &self.hi, &self.lo)
                        == std::cmp::Ordering::Less
                {
                    self.set_leaf(k.clone(), v.clone());
                } else {
                    panic!(
                        "tried to consolidate set at key <= hi.\
                         Set({:?}, {:?}) to node {:?}",
                        k, v, self
                    )
                }
            }
            Del(ref k) => {
                // (when hi is empty, it means it's unbounded)
                if self.hi.is_empty()
                    || prefix_cmp_encoded(k, &self.hi, &self.lo)
                        == std::cmp::Ordering::Less
                {
                    self.del_leaf(k);
                } else {
                    panic!("tried to consolidate del at key <= hi")
                }
            }
            Base(_) => panic!("trying to apply a Base to frag {:?}", self),
            ParentMergeIntention(pid) => {
                assert!(
                    self.merging_child.is_none(),
                    "trying to merge {:?} into node {:?} which \
                     is already merging another child",
                    frag,
                    self
                );
                self.merging_child = Some(pid);
            }
            ParentMergeConfirm => {
                assert!(self.merging_child.is_some());
                let merged_child = self.merging_child.take().expect(
                    "we should have a specific \
                     child that was merged if this \
                     frag appears here",
                );
                self.data.parent_merge_confirm(merged_child);
            }
            ChildMergeCap => {
                self.merging = true;
            }
        }
    }

    pub(crate) fn set_leaf(&mut self, key: IVec, val: IVec) {
        if let Data::Leaf(ref mut records) = self.data {
            let search = records.binary_search_by(|(k, _)| prefix_cmp(k, &key));
            match search {
                Ok(idx) => records[idx] = (key, val),
                Err(idx) => records.insert(idx, (key, val)),
            }
        } else {
            panic!("tried to Set a value to an index");
        }
    }

    pub(crate) fn parent_split(&mut self, at: &[u8], to: PageId) -> bool {
        if let Data::Index(ref mut ptrs) = self.data {
            let encoded_sep = prefix_encode(&self.lo, at);
            match ptrs.binary_search_by(|a| prefix_cmp(&a.0, &encoded_sep)) {
                Ok(_) => {
                    debug!(
                        "parent_split skipped because \
                         parent already contains child at split point \
                         due to deep race"
                    );
                    return false;
                }
                Err(idx) => ptrs.insert(idx, (encoded_sep, to)),
            }
        } else {
            panic!("tried to attach a ParentSplit to a Leaf chain");
        }

        true
    }

    pub(crate) fn del_leaf(&mut self, key: &IVec) {
        if let Data::Leaf(ref mut records) = self.data {
            let search = records
                .binary_search_by(|&(ref k, ref _v)| prefix_cmp(k, &*key));
            if let Ok(idx) = search {
                let _removed = records.remove(idx);
            }
        } else {
            panic!("tried to attach a Del to an Index chain");
        }
    }

    pub(crate) fn split(mut self) -> (Self, Self) {
        let (split, right_data) = self.data.split(&self.lo);
        let rhs = Self {
            data: right_data,
            next: self.next,
            lo: split,
            hi: self.hi.clone(),
            merging_child: None,
            merging: false,
        };

        self.data.drop_gte(&rhs.lo, &self.lo);
        self.hi = rhs.lo.clone();

        // intentionally make this the end to make
        // any issues pop out with setting it
        // correctly after the split.
        self.next = None;

        (self, rhs)
    }

    pub(crate) fn receive_merge(&self, rhs: &Self) -> Self {
        let mut merged = self.clone();
        merged.hi = rhs.hi.clone();
        merged.data.receive_merge(
            rhs.lo.as_ref(),
            merged.lo.as_ref(),
            &rhs.data,
        );
        merged.next = rhs.next;
        merged
    }

    pub(crate) fn contains_upper_bound(&self, bound: &Bound<IVec>) -> bool {
        match bound {
            Bound::Excluded(bound) if self.hi >= *bound => true,
            Bound::Included(bound) if self.hi > *bound => true,
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
                if self.lo < *bound || (is_forward && *bound == self.lo) =>
            {
                true
            }
            Bound::Included(bound) if self.lo <= *bound => true,
            Bound::Unbounded if !is_forward => self.hi.is_empty(),
            _ => self.lo.is_empty(),
        }
    }

    pub(crate) fn successor(
        &self,
        bound: &Bound<IVec>,
    ) -> Option<(IVec, IVec)> {
        assert!(!self.data.is_index());

        // This encoding happens this way because
        // keys cannot be lower than the node's lo key.
        let predecessor_key = match bound {
            Bound::Unbounded => prefix_encode(&self.lo, &self.lo),
            Bound::Included(b) => {
                let max = std::cmp::max(b, &self.lo);
                prefix_encode(&self.lo, max)
            }
            Bound::Excluded(b) => {
                let max = std::cmp::max(b, &self.lo);
                prefix_encode(&self.lo, max)
            }
        };

        let records = self.data.leaf_ref().unwrap();
        let search =
            records.binary_search_by(|(k, _)| prefix_cmp(k, &predecessor_key));

        let idx = match search {
            Ok(idx) => idx,
            Err(idx) if idx < records.len() => idx,
            _ => return None,
        };

        for (k, v) in &records[idx..] {
            match bound {
                Bound::Excluded(b)
                    if prefix_cmp_encoded(k, b, &self.lo)
                        == std::cmp::Ordering::Equal =>
                {
                    // keep going because we wanted to exclude
                    // this key.
                    continue;
                }
                _ => {}
            }
            let decoded_key = prefix_decode(&self.lo, &k);
            return Some((IVec::from(decoded_key), v.clone()));
        }

        None
    }

    pub(crate) fn predecessor(
        &self,
        bound: &Bound<IVec>,
    ) -> Option<(IVec, IVec)> {
        static MAX_IVEC: Lazy<IVec, fn() -> IVec> = Lazy::new(init_max_ivec);

        fn init_max_ivec() -> IVec {
            let mut base = vec![255; 1024 * 1024];
            base[0] = 0;
            IVec::from(base)
        }

        assert!(!self.data.is_index());

        // This encoding happens this way because
        // the rightmost (unbounded) node has
        // a hi key represented by the empty slice
        let successor_key = match bound {
            Bound::Unbounded => {
                if self.hi.is_empty() {
                    MAX_IVEC.clone()
                } else {
                    prefix_encode(&self.lo, &self.hi)
                }
            }
            Bound::Included(b) => {
                let min = if self.hi.is_empty() {
                    b
                } else {
                    std::cmp::min(b, &self.hi)
                };
                prefix_encode(&self.lo, min)
            }
            Bound::Excluded(b) => {
                let min = if self.hi.is_empty() {
                    b
                } else {
                    std::cmp::min(b, &self.hi)
                };
                prefix_encode(&self.lo, min)
            }
        };

        let records = self.data.leaf_ref().unwrap();
        let search =
            records.binary_search_by(|(k, _)| prefix_cmp(k, &successor_key));

        let idx = match search {
            Ok(idx) => idx,
            Err(idx) if idx > 0 => idx - 1,
            _ => return None,
        };

        for (k, v) in records[0..=idx].iter().rev() {
            match bound {
                Bound::Excluded(b)
                    if prefix_cmp_encoded(k, b, &self.lo)
                        == std::cmp::Ordering::Equal =>
                {
                    // keep going because we wanted to exclude
                    // this key.
                    continue;
                }
                _ => {}
            }
            let decoded_key = prefix_decode(&self.lo, &k);
            return Some((IVec::from(decoded_key), v.clone()));
        }
        None
    }

    /// leaf_pair_for_key finds an existing value pair for a given key.
    pub(crate) fn leaf_pair_for_key(
        &self,
        key: &[u8],
    ) -> Option<(&IVec, &IVec)> {
        assert!(!self.data.is_index());

        let records = self.data.leaf_ref().unwrap();

        let common_prefix = key
            .iter()
            .zip(self.lo.iter())
            .take(u8::max_value() as usize)
            .take_while(|(a, b)| a == b)
            .count() as u8;

        let search = records
            .binary_search_by(|&(ref k, ref _v)| {
                if k[0] > common_prefix {
                    Ordering::Less
                } else if k[0] < common_prefix {
                    Ordering::Greater
                } else {
                    k[1..].cmp(&key[common_prefix as usize..])
                }
            })
            .ok();

        search.map(|idx| (&records[idx].0, &records[idx].1))
    }

    /// node_kv_pair returns either existing (node/key, value) pair or
    /// (node/key, none) where a node/key is node level encoded key.
    pub fn node_kv_pair(&self, key: &[u8]) -> (IVec, Option<IVec>) {
        if let Some((k, v)) = self.leaf_pair_for_key(key.as_ref()) {
            (k.clone(), Some(v.clone()))
        } else {
            let encoded_key = prefix_encode(&self.lo, key.as_ref());
            let encoded_val = None;
            (encoded_key, encoded_val)
        }
    }

    pub(crate) fn should_split(&self) -> bool {
        let threshold = if cfg!(any(test, feature = "lock_free_delays")) {
            2
        } else if self.data.is_index() {
            256
        } else {
            16
        };

        let size_checks = self.data.len() > threshold;
        let safety_checks = self.merging_child.is_none() && !self.merging;

        size_checks && safety_checks
    }

    pub(crate) fn should_merge(&self) -> bool {
        let threshold = if cfg!(any(test, feature = "lock_free_delays")) {
            1
        } else if self.data.is_index() {
            64
        } else {
            4
        };

        let size_checks = self.data.len() < threshold;
        let safety_checks = self.merging_child.is_none() && !self.merging;

        size_checks && safety_checks
    }

    pub(crate) fn can_merge_child(&self) -> bool {
        self.merging_child.is_none() && !self.merging
    }

    pub(crate) fn index_next_node(&self, key: &[u8]) -> (usize, PageId) {
        let records = self
            .data
            .index_ref()
            .expect("index_next_node called on leaf");

        let common_prefix = key
            .iter()
            .zip(self.lo.iter())
            .take(u8::max_value() as usize)
            .take_while(|(a, b)| a == b)
            .count() as u8;

        let search = binary_search_lub(records, |&(ref k, ref _v)| {
            if k[0] > common_prefix {
                Ordering::Less
            } else if k[0] < common_prefix {
                Ordering::Greater
            } else {
                k[1..].cmp(&key[common_prefix as usize..])
            }
        });

        let index = search.expect("failed to traverse index");

        (index, records[index].1)
    }
}

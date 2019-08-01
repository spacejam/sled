use std::{fmt, mem::size_of, ops::Bound};

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

impl fmt::Debug for Node {
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
    #[inline]
    pub(crate) fn size_in_bytes(&self) -> u64 {
        let self_sz = size_of::<Self>() as u64;
        let lo_sz = self.lo.size_in_bytes();
        let hi_sz = self.hi.size_in_bytes();
        let data_sz = self.data.size_in_bytes();

        self_sz
            .saturating_add(lo_sz)
            .saturating_add(hi_sz)
            .saturating_add(data_sz)
    }

    pub(crate) fn apply(&mut self, frag: &Frag, merge_operator: Option<usize>) {
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
            Merge(ref k, ref v) => {
                // (when hi is empty, it means it's unbounded)
                if self.hi.is_empty()
                    || prefix_cmp_encoded(k, &self.hi, &self.lo)
                        == std::cmp::Ordering::Less
                {
                    let merge_fn_ptr =
                        merge_operator.expect("must have a merge operator set");
                    unsafe {
                        let merge_fn: MergeOperator =
                            std::mem::transmute(merge_fn_ptr);
                        self.merge_leaf(k.clone(), v.clone(), merge_fn);
                    }
                } else {
                    panic!("tried to consolidate set at key <= hi")
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

    pub(crate) fn merge_leaf(
        &mut self,
        key: IVec,
        val: IVec,
        merge_fn: MergeOperator,
    ) {
        if let Data::Leaf(ref mut records) = self.data {
            let search = records.binary_search_by(|(k, _)| prefix_cmp(k, &key));

            let decoded_k = prefix_decode(&self.lo, &key);

            match search {
                Ok(idx) => {
                    let new =
                        merge_fn(&*decoded_k, Some(&records[idx].1), &val);
                    if let Some(new) = new {
                        records[idx] = (key, new.into());
                    } else {
                        records.remove(idx);
                    }
                }
                Err(idx) => {
                    let new = merge_fn(&*decoded_k, None, &val);
                    if let Some(new) = new {
                        records.insert(idx, (key, new.into()));
                    }
                }
            }
        } else {
            panic!("tried to Merge a value to an index");
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
                records.remove(idx);
            }
        } else {
            panic!("tried to attach a Del to an Index chain");
        }
    }

    pub(crate) fn split(mut self) -> (Node, Node) {
        let (split, right_data) = self.data.split(&self.lo);
        let rhs = Node {
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

    pub(crate) fn receive_merge(&self, rhs: &Node) -> Node {
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
            Bound::Excluded(bound) if &self.hi >= bound => true,
            Bound::Included(bound) if &self.hi > bound => true,
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
                if &self.lo < bound || (is_forward && bound == &self.lo) =>
            {
                true
            }
            Bound::Included(bound) if &self.lo <= bound => true,
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
            Err(idx) => idx,
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
        assert!(!self.data.is_index());

        lazy_static::lazy_static! {
            static ref MAX_IVEC: IVec = {
                let mut base = vec![255; 1024 * 1024];
                base[0] = 0;
                IVec::from(base)
            };
        }

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
            Ok(idx) if idx > 0 => idx - 1,
            Err(idx) if idx > 0 => idx - 1,
            _ => return None,
        };

        if let Some((k, v)) = records.get(idx) {
            let decoded_key = prefix_decode(&self.lo, &k);
            Some((IVec::from(decoded_key), v.clone()))
        } else {
            None
        }
    }

    pub(crate) fn leaf_value_for_key(&self, key: &[u8]) -> Option<&IVec> {
        assert!(!self.data.is_index());

        let records = self.data.leaf_ref().unwrap();
        let search = records
            .binary_search_by(|&(ref k, ref _v)| {
                prefix_cmp_encoded(k, key, &self.lo)
            })
            .ok();

        search.map(|idx| &records[idx].1)
    }

    pub(crate) fn should_split(&self, max_sz: u64) -> bool {
        let size_checks = self.data.len() > 4 && self.size_in_bytes() > max_sz;
        let safety_checks = self.merging_child.is_none() && !self.merging;

        size_checks && safety_checks
    }

    pub(crate) fn should_merge(&self, min_sz: u64) -> bool {
        let size_checks = self.data.len() < 2 && self.size_in_bytes() < min_sz;
        let safety_checks = self.merging_child.is_none() && !self.merging;

        size_checks && safety_checks
    }

    pub(crate) fn can_merge_child(&self) -> bool {
        self.merging_child.is_none() && !self.merging
    }

    pub(crate) fn index_next_node(&self, key: &[u8]) -> (usize, PageId) {
        assert!(self.data.is_index());

        let records = self.data.index_ref().unwrap();

        let search = binary_search_lub(records, |&(ref k, ref _v)| {
            prefix_cmp_encoded(k, key, &self.lo)
        });

        // This might be none if ord is Less and we're
        // searching for the empty key
        let index = search.expect("failed to traverse index");

        (index, records[index].1)
    }
}

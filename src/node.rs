use std::ops::Bound;

use super::*;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Node {
    pub(crate) data: Data,
    pub(crate) next: Option<PageId>,
    pub(crate) lo: IVec,
    pub(crate) hi: IVec,
    pub(crate) merging_child: Option<PageId>,
    pub(crate) merging: bool,
    prefix_len: u8,
}

impl Node {
    fn prefix_decode(&self, key: &[u8]) -> IVec {
        prefix::decode(self.prefix(), key)
    }

    fn prefix_encode<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        assert!(&*self.lo <= key);
        if !self.hi.is_empty() {
            assert!(&*self.hi > key);
        }

        &key[self.prefix_len as usize..]
    }

    fn prefix(&self) -> &[u8] {
        &self.lo[..self.prefix_len as usize]
    }

    pub(crate) fn apply(&mut self, frag: &Frag) {
        use self::Frag::*;

        assert!(
            !self.merging,
            "somehow a frag was applied to a node after it was merged"
        );

        match *frag {
            Set(ref k, ref v) => {
                self.set_leaf(k.clone(), v.clone());
            }
            Del(ref k) => {
                self.del_leaf(k);
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
            let search = records.binary_search_by_key(&&key, |(k, _)| k);
            match search {
                Ok(idx) => records[idx] = (key, val),
                Err(idx) => records.insert(idx, (key, val)),
            }
        } else {
            panic!("tried to Set a value to an index");
        }
    }

    pub(crate) fn del_leaf(&mut self, key: &IVec) {
        if let Data::Leaf(ref mut records) = self.data {
            let search = records.binary_search_by_key(&key, |(k, _)| k);
            if let Ok(idx) = search {
                records.remove(idx);
            }
        } else {
            panic!("tried to attach a Del to an Index chain");
        }
    }

    pub(crate) fn parent_split(&mut self, at: &[u8], to: PageId) -> bool {
        if let Data::Index(ref mut ptrs) = self.data {
            let encoded_sep = &at[self.prefix_len as usize..];
            match ptrs.binary_search_by_key(&encoded_sep, |(k, _)| k) {
                Ok(_) => {
                    debug!(
                        "parent_split skipped because \
                         parent already contains child at split point \
                         due to deep race"
                    );
                    return false;
                }
                Err(idx) => ptrs.insert(idx, (IVec::from(encoded_sep), to)),
            }
        } else {
            panic!("tried to attach a ParentSplit to a Leaf chain");
        }

        true
    }

    pub(crate) fn split(mut self) -> (Node, Node) {
        fn split_inner<T>(
            xs: &mut Vec<(IVec, T)>,
            lhs_prefix: &[u8],
            rhs_max: &[u8],
        ) -> (IVec, u8, Vec<(IVec, T)>)
        where
            T: Clone + Ord,
        {
            let rhs = xs.split_off(xs.len() / 2 + 1);
            let rhs_min = &rhs[0].0;

            // When splitting, the prefix can only grow or stay the
            // same size, because all keys already shared the same
            // prefix before. Here we figure out if we can shave
            // off additional bytes in the key.
            let max_additional = u8::max_value() - lhs_prefix.len() as u8;
            let rhs_additional_prefix_len = rhs_min
                .iter()
                .zip(rhs_max.iter())
                .take_while(|(a, b)| a == b)
                .take(max_additional as usize)
                .count();

            let split_point: IVec = prefix::decode(lhs_prefix, &rhs_min);
            assert!(!split_point.is_empty());

            let mut rhs_data = Vec::with_capacity(rhs.len());

            for (k, v) in rhs {
                let k: IVec = if rhs_additional_prefix_len > 0 {
                    // shave off additional prefixed bytes
                    IVec::from(&k[rhs_additional_prefix_len..])
                } else {
                    k.clone()
                };
                rhs_data.push((k, v.clone()));
            }

            (split_point, rhs_additional_prefix_len as u8, rhs_data)
        }

        let prefix = &self.lo[..self.prefix_len as usize];
        let rhs_max = &self.hi;
        let (split, rhs_additional_prefix_len, right_data) = match self.data {
            Data::Index(ref mut ptrs) => {
                let (split, prefix_len, rhs) =
                    split_inner(ptrs, prefix, rhs_max);
                (split, prefix_len, Data::Index(rhs))
            }
            Data::Leaf(ref mut items) => {
                let (split, prefix_len, rhs) =
                    split_inner(items, prefix, rhs_max);
                (split, prefix_len, Data::Leaf(rhs))
            }
        };

        let rhs = Node {
            data: right_data,
            next: self.next,
            lo: split.clone(),
            hi: self.hi.clone(),
            merging_child: None,
            merging: false,
            prefix_len: self.prefix_len + rhs_additional_prefix_len,
        };

        self.hi = split;

        // intentionally make this the end to make
        // any issues pop out with setting it
        // correctly after the split.
        self.next = None;

        if self.hi.is_empty() {
            assert_eq!(self.prefix_len, 0);
        }

        assert!(!(self.lo.is_empty() && self.hi.is_empty()));
        assert!(!(self.lo.is_empty() && (self.prefix_len > 0)));
        assert!(!(rhs.lo.is_empty() && rhs.hi.is_empty()));
        assert!(!(rhs.lo.is_empty() && (rhs.prefix_len > 0)));

        (self, rhs)
    }

    pub(crate) fn receive_merge(&self, rhs: &Node) -> Node {
        fn receive_merge_inner<T>(
            old_prefix: &[u8],
            new_prefix_len: usize,
            lhs_data: &mut Vec<(IVec, T)>,
            rhs_data: &[(IVec, T)],
        ) where
            T: Clone,
        {
            // When merging, the prefix can only shrink or
            // stay the same length. Here we figure out if
            // we need to add previous prefixed bytes.
            for (k, v) in rhs_data {
                let k = if new_prefix_len != old_prefix.len() {
                    prefix::reencode(old_prefix, k, new_prefix_len)
                } else {
                    k.clone()
                };
                lhs_data.push((k, v.clone()));
            }
        }

        let mut merged = self.clone();

        let new_prefix_len = rhs
            .hi
            .iter()
            .zip(self.lo.iter())
            .take_while(|(a, b)| a == b)
            .take(u8::max_value() as usize)
            .count();

        match (&mut merged.data, &rhs.data) {
            (Data::Index(ref mut lh_ptrs), Data::Index(ref rh_ptrs)) => {
                receive_merge_inner(
                    rhs.prefix(),
                    new_prefix_len,
                    lh_ptrs,
                    rh_ptrs.as_ref(),
                );
            }
            (Data::Leaf(ref mut lh_items), Data::Leaf(ref rh_items)) => {
                receive_merge_inner(
                    rhs.prefix(),
                    new_prefix_len,
                    lh_items,
                    rh_items.as_ref(),
                );
            }
            _ => panic!("Can't merge incompatible Data!"),
        }

        merged.hi = rhs.hi.clone();
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
            Bound::Unbounded => self.prefix_encode(&self.lo),
            Bound::Included(b) => {
                let max = std::cmp::max(b, &self.lo);
                self.prefix_encode(max)
            }
            Bound::Excluded(b) => {
                let max = std::cmp::max(b, &self.lo);
                self.prefix_encode(max)
            }
        };

        let records = self.data.leaf_ref().unwrap();
        let search = records.binary_search_by_key(&predecessor_key, |(k, _)| k);

        let idx = match search {
            Ok(idx) => idx,
            Err(idx) if idx < records.len() => idx,
            _ => return None,
        };

        for (k, v) in &records[idx..] {
            match bound {
                Bound::Excluded(b) if b[self.prefix_len as usize..] == **k => {
                    // keep going because we wanted to exclude
                    // this key.
                    continue;
                }
                _ => {}
            }
            let decoded_key = self.prefix_decode(&k);
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
            let base = vec![255; 1024 * 1024];
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
                    IVec::from(self.prefix_encode(&self.hi))
                }
            }
            Bound::Included(b) => {
                let min = if self.hi.is_empty() {
                    b
                } else {
                    std::cmp::min(b, &self.hi)
                };
                IVec::from(self.prefix_encode(min))
            }
            Bound::Excluded(b) => {
                let min = if self.hi.is_empty() {
                    b
                } else {
                    std::cmp::min(b, &self.hi)
                };
                let encoded = &min[self.prefix_len as usize..];
                IVec::from(encoded)
            }
        };

        let records = self.data.leaf_ref().unwrap();
        let search = records.binary_search_by_key(&&successor_key, |(k, _)| k);

        let idx = match search {
            Ok(idx) => idx,
            Err(idx) if idx > 0 => idx - 1,
            _ => return None,
        };

        for (k, v) in records[0..=idx].iter().rev() {
            match bound {
                Bound::Excluded(b)
                    if b.len() >= self.prefix_len as usize
                        && b[self.prefix_len as usize..] == **k =>
                {
                    // keep going because we wanted to exclude
                    // this key.
                    continue;
                }
                _ => {}
            }
            let decoded_key = self.prefix_decode(&k);
            return Some((IVec::from(decoded_key), v.clone()));
        }
        None
    }

    /// leaf_pair_for_key finds an existing value pair for a given key.
    pub(crate) fn leaf_pair_for_key(
        &self,
        key: &[u8],
    ) -> Option<(&IVec, &IVec)> {
        let records = self
            .data
            .leaf_ref()
            .expect("leaf_pair_for_key called on index node");

        let suffix = &key[self.prefix_len as usize..];

        let search = records.binary_search_by_key(&suffix, |(k, _)| k).ok();

        search.map(|idx| (&records[idx].0, &records[idx].1))
    }

    /// node_kv_pair returns either existing (node/key, value) pair or
    /// (node/key, none) where a node/key is node level encoded key.
    pub fn node_kv_pair(&self, key: &[u8]) -> (IVec, Option<IVec>) {
        if let Some((k, v)) = self.leaf_pair_for_key(key.as_ref()) {
            (k.clone(), Some(v.clone()))
        } else {
            let encoded_key = IVec::from(&key[self.prefix_len as usize..]);
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

        let suffix = &key[self.prefix_len as usize..];

        let search =
            binary_search_lub(records, |(k, _)| k.as_ref().cmp(suffix));

        let index = search.expect("failed to traverse index");

        (index, records[index].1)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Data {
    Index(Vec<(IVec, PageId)>),
    Leaf(Vec<(IVec, IVec)>),
}

impl Default for Data {
    fn default() -> Data {
        Data::Leaf(vec![])
    }
}

impl Data {
    pub(crate) fn len(&self) -> usize {
        match *self {
            Data::Index(ref ptrs) => ptrs.len(),
            Data::Leaf(ref items) => items.len(),
        }
    }

    pub(crate) fn parent_merge_confirm(&mut self, merged_child_pid: PageId) {
        match self {
            Data::Index(ref mut ptrs) => {
                let idx = ptrs
                    .iter()
                    .position(|(_k, c)| *c == merged_child_pid)
                    .unwrap();
                let _ = ptrs.remove(idx);
            }
            _ => panic!("parent_merge_confirm called on leaf data"),
        }
    }

    pub(crate) fn leaf_ref(&self) -> Option<&Vec<(IVec, IVec)>> {
        match *self {
            Data::Index(_) => None,
            Data::Leaf(ref items) => Some(items),
        }
    }

    pub(crate) fn index_ref(&self) -> Option<&Vec<(IVec, PageId)>> {
        match *self {
            Data::Index(ref ptrs) => Some(ptrs),
            Data::Leaf(_) => None,
        }
    }

    pub(crate) fn is_index(&self) -> bool {
        if let Data::Index(..) = self {
            true
        } else {
            false
        }
    }

    pub(crate) fn is_leaf(&self) -> bool {
        if let Data::Leaf(..) = self {
            true
        } else {
            false
        }
    }
}

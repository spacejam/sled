use std::{num::NonZeroU64, ops::Bound};

use super::*;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct Node {
    pub(crate) prefix_len: u8,
    pub(crate) next: Option<NonZeroU64>,
    pub(crate) merging_child: Option<NonZeroU64>,
    pub(crate) merging: bool,
    pub(crate) lo: IVec,
    pub(crate) hi: IVec,
    pub(crate) data: Data,
}

impl Node {
    pub(crate) fn new_hoisted_root(
        left: PageId,
        at: IVec,
        right: PageId,
    ) -> Node {
        Node {
            data: Data::Index(Index {
                keys: vec![prefix::empty().into(), at],
                pointers: vec![left, right],
            }),
            ..Node::default()
        }
    }

    pub(crate) fn new_root(child_pid: PageId) -> Node {
        Node {
            data: Data::Index(Index {
                keys: vec![prefix::empty().into()],
                pointers: vec![child_pid],
            }),
            ..Node::default()
        }
    }

    pub(crate) fn rss(&self) -> u64 {
        std::mem::size_of::<Node>() as u64
            + self.lo.len() as u64
            + self.hi.len() as u64
            + self.data.rss()
    }

    fn prefix_decode(&self, key: &[u8]) -> IVec {
        prefix::decode(self.prefix(), key)
    }

    fn prefix_encode<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        assert!(*self.lo <= *key);
        if !self.hi.is_empty() {
            assert!(*self.hi > *key);
        }

        &key[self.prefix_len as usize..]
    }

    fn prefix(&self) -> &[u8] {
        &self.lo[..self.prefix_len as usize]
    }

    pub(crate) fn apply(&mut self, link: &Link) {
        use self::Link::*;

        assert!(
            !self.merging,
            "somehow a link was applied to a node after it was merged"
        );

        match *link {
            Set(ref k, ref v) => {
                self.set_leaf(k.clone(), v.clone());
            }
            Del(ref k) => {
                self.del_leaf(k);
            }
            ParentMergeIntention(pid) => {
                assert!(
                    self.merging_child.is_none(),
                    "trying to merge {:?} into node {:?} which \
                     is already merging another child",
                    link,
                    self
                );
                self.merging_child = Some(NonZeroU64::new(pid).unwrap());
            }
            ParentMergeConfirm => {
                assert!(self.merging_child.is_some());
                let merged_child = self
                    .merging_child
                    .take()
                    .expect(
                        "we should have a specific \
                     child that was merged if this \
                     link appears here",
                    )
                    .get();
                self.data.parent_merge_confirm(merged_child);
            }
            ChildMergeCap => {
                self.merging = true;
            }
        }
    }

    pub(crate) fn set_leaf(&mut self, key: IVec, val: IVec) {
        if !self.hi.is_empty() {
            assert!(*key < self.hi[self.prefix_len as usize..]);
        }
        if let Data::Leaf(ref mut leaf) = self.data {
            let search = leaf.keys.binary_search_by(|k| fastcmp(k, &key));
            match search {
                Ok(idx) => leaf.values[idx] = val,
                Err(idx) => {
                    leaf.keys.insert(idx, key);
                    leaf.values.insert(idx, val);
                }
            }
            testing_assert!(is_sorted(&leaf.keys));
        } else {
            panic!("tried to Set a value to an index");
        }
    }

    pub(crate) fn del_leaf(&mut self, key: &IVec) {
        if let Data::Leaf(ref mut leaf) = self.data {
            let search = leaf.keys.binary_search_by(|k| fastcmp(k, key));
            if let Ok(idx) = search {
                leaf.keys.remove(idx);
                leaf.values.remove(idx);
            }
            testing_assert!(is_sorted(&leaf.keys));
        } else {
            panic!("tried to attach a Del to an Index chain");
        }
    }

    pub(crate) fn parent_split(&mut self, at: &[u8], to: PageId) -> bool {
        if let Data::Index(ref mut index) = self.data {
            let encoded_sep = &at[self.prefix_len as usize..];
            match index.keys.binary_search_by(|k| fastcmp(k, encoded_sep)) {
                Ok(_) => {
                    debug!(
                        "parent_split skipped because \
                         parent already contains child \
                         at split point due to deep race"
                    );
                    return false;
                }
                Err(idx) => {
                    index.keys.insert(idx, IVec::from(encoded_sep));
                    index.pointers.insert(idx, to)
                }
            }
            testing_assert!(is_sorted(&index.keys));
        } else {
            panic!("tried to attach a ParentSplit to a Leaf chain");
        }

        true
    }

    pub(crate) fn split(mut self) -> (Node, Node) {
        fn split_inner<T>(
            keys: &mut Vec<IVec>,
            values: &mut Vec<T>,
            old_prefix: &[u8],
            old_hi: &[u8],
            suffix_truncation: bool,
        ) -> (IVec, u8, Vec<IVec>, Vec<T>)
        where
            T: Clone + Ord,
        {
            let split_point = keys.len() / 2 + 1;
            let right_keys = keys.split_off(split_point);
            let right_values = values.split_off(split_point);
            let right_min = &right_keys[0];
            let left_max = &keys.last().unwrap();

            let splitpoint_length = if suffix_truncation {
                // we can only perform suffix truncation when
                // choosing the split points for leaf nodes.
                // split points bubble up into indexes, but
                // an important invariant is that for indexes
                // the first item always matches the lo key,
                // otherwise ranges would be permanently
                // inaccessible by falling into the gap
                // during a split.
                right_min
                    .iter()
                    .zip(left_max.iter())
                    .take_while(|(a, b)| a == b)
                    .count()
                    + 1
            } else {
                right_min.len()
            };

            let split_point: IVec =
                prefix::decode(old_prefix, &right_min[..splitpoint_length]);

            assert!(!split_point.is_empty());

            let new_prefix_len = old_hi
                .iter()
                .zip(split_point.iter())
                .take_while(|(a, b)| a == b)
                .take(u8::max_value() as usize)
                .count();

            assert!(
                new_prefix_len >= old_prefix.len(),
                "new prefix length {} should be greater than \
                 or equal to the old prefix length {}",
                new_prefix_len,
                old_prefix.len()
            );

            let mut right_keys_data = Vec::with_capacity(right_keys.len());

            for k in right_keys {
                let k: IVec = if new_prefix_len == old_prefix.len() {
                    k.clone()
                } else {
                    // shave off additional prefixed bytes
                    prefix::reencode(old_prefix, &k, new_prefix_len)
                };
                right_keys_data.push(k);
            }

            testing_assert!(is_sorted(&right_keys_data));

            (
                split_point,
                u8::try_from(new_prefix_len).unwrap(),
                right_keys_data,
                right_values,
            )
        }

        let prefixed_lo = &self.lo[..self.prefix_len as usize];
        let prefixed_hi = &self.hi;
        let (split, right_prefix_len, right_data) = match self.data {
            Data::Index(ref mut index) => {
                let (split, right_prefix_len, right_keys, right_values) =
                    split_inner(
                        &mut index.keys,
                        &mut index.pointers,
                        prefixed_lo,
                        prefixed_hi,
                        false,
                    );

                (
                    split,
                    right_prefix_len,
                    Data::Index(Index {
                        keys: right_keys,
                        pointers: right_values,
                    }),
                )
            }
            Data::Leaf(ref mut leaf) => {
                let (split, right_prefix_len, right_keys, right_values) =
                    split_inner(
                        &mut leaf.keys,
                        &mut leaf.values,
                        prefixed_lo,
                        prefixed_hi,
                        true,
                    );

                (
                    split,
                    right_prefix_len,
                    Data::Leaf(Leaf { keys: right_keys, values: right_values }),
                )
            }
        };

        let right = Node {
            data: right_data,
            next: self.next,
            lo: split.clone(),
            hi: self.hi.clone(),
            merging_child: None,
            merging: false,
            prefix_len: right_prefix_len,
        };

        self.hi = split;

        let new_prefix_len = self
            .hi
            .iter()
            .zip(self.lo.iter())
            .take_while(|(a, b)| a == b)
            .take(u8::max_value() as usize)
            .count();

        if new_prefix_len != self.prefix_len as usize {
            match self.data {
                Data::Index(ref mut index) => {
                    for k in &mut index.keys {
                        *k = prefix::reencode(prefixed_lo, k, new_prefix_len);
                    }
                }
                Data::Leaf(ref mut leaf) => {
                    for k in &mut leaf.keys {
                        *k = prefix::reencode(prefixed_lo, k, new_prefix_len);
                    }
                }
            }
        }

        self.prefix_len = u8::try_from(new_prefix_len).unwrap();

        // intentionally make this the end to make
        // any issues pop out with setting it
        // correctly after the split.
        self.next = None;

        if self.hi.is_empty() {
            assert_eq!(self.prefix_len, 0);
        }

        assert!(!(self.lo.is_empty() && self.hi.is_empty()));
        assert!(!(self.lo.is_empty() && (self.prefix_len > 0)));
        assert!(self.lo.len() >= self.prefix_len as usize);
        assert!(self.hi.len() >= self.prefix_len as usize);
        assert!(!(right.lo.is_empty() && right.hi.is_empty()));
        assert!(!(right.lo.is_empty() && (right.prefix_len > 0)));
        assert!(right.lo.len() >= right.prefix_len as usize);
        assert!(right.hi.len() >= right.prefix_len as usize);

        if !self.lo.is_empty() && !self.hi.is_empty() {
            assert!(self.lo < self.hi, "{:?}", self);
        }

        if !right.lo.is_empty() && !right.hi.is_empty() {
            assert!(right.lo < right.hi, "{:?}", right);
        }

        (self, right)
    }

    pub(crate) fn receive_merge(&self, right: &Node) -> Node {
        fn receive_merge_inner<T>(
            old_prefix: &[u8],
            new_prefix_len: usize,
            left_keys: &mut Vec<IVec>,
            left_values: &mut Vec<T>,
            right_keys: &[IVec],
            right_values: &[T],
        ) where
            T: Debug + Clone + PartialOrd,
        {
            // When merging, the prefix should only shrink or
            // stay the same length. Here we figure out if
            // we need to add previous prefixed bytes.

            for (k, v) in right_keys.iter().zip(right_values.iter()) {
                let k = if new_prefix_len == old_prefix.len() {
                    k.clone()
                } else {
                    prefix::reencode(old_prefix, k, new_prefix_len)
                };
                left_keys.push(k);
                left_values.push(v.clone());
            }
            testing_assert!(
                is_sorted(left_keys),
                "should have been sorted: {:?}",
                left_keys
            );
        }

        let mut merged = self.clone();

        let new_prefix_len = right
            .hi
            .iter()
            .zip(self.lo.iter())
            .take_while(|(a, b)| a == b)
            .take(u8::max_value() as usize)
            .count();

        if new_prefix_len != merged.prefix_len as usize {
            match merged.data {
                Data::Index(ref mut index) => {
                    for k in &mut index.keys {
                        *k = prefix::reencode(self.prefix(), k, new_prefix_len);
                    }
                }
                Data::Leaf(ref mut leaf) => {
                    for k in &mut leaf.keys {
                        *k = prefix::reencode(self.prefix(), k, new_prefix_len);
                    }
                }
            }
        }

        merged.prefix_len = u8::try_from(new_prefix_len).unwrap();

        match (&mut merged.data, &right.data) {
            (Data::Index(ref mut left_index), Data::Index(ref right_index)) => {
                receive_merge_inner(
                    right.prefix(),
                    new_prefix_len,
                    &mut left_index.keys,
                    &mut left_index.pointers,
                    right_index.keys.as_ref(),
                    right_index.pointers.as_ref(),
                );
            }
            (Data::Leaf(ref mut left_leaf), Data::Leaf(ref right_leaf)) => {
                receive_merge_inner(
                    right.prefix(),
                    new_prefix_len,
                    &mut left_leaf.keys,
                    &mut left_leaf.values,
                    right_leaf.keys.as_ref(),
                    right_leaf.values.as_ref(),
                );
            }
            _ => panic!("Can't merge incompatible Data!"),
        }

        merged.hi = right.hi.clone();
        merged.next = right.next;
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
            Bound::Included(b) | Bound::Excluded(b) => {
                let max = std::cmp::max(b, &self.lo);
                self.prefix_encode(max)
            }
        };

        let leaf = self.data.leaf_ref().unwrap();
        let search =
            leaf.keys.binary_search_by(|k| fastcmp(k, predecessor_key));

        let start = match search {
            Ok(start) => start,
            Err(start) if start < leaf.keys.len() => start,
            _ => return None,
        };

        for (idx, k) in leaf.keys[start..].iter().enumerate() {
            match bound {
                Bound::Excluded(b) if b[self.prefix_len as usize..] == **k => {
                    // keep going because we wanted to exclude
                    // this key.
                    continue;
                }
                _ => {}
            }
            let decoded_key = self.prefix_decode(k);
            return Some((decoded_key, leaf.values[start + idx].clone()));
        }

        None
    }

    pub(crate) fn predecessor(
        &self,
        bound: &Bound<IVec>,
    ) -> Option<(IVec, IVec)> {
        assert!(!self.data.is_index());

        // This encoding happens this way because
        // the rightmost (unbounded) node has
        // a hi key represented by the empty slice
        let successor_key = match bound {
            Bound::Unbounded => {
                if self.hi.is_empty() {
                    None
                } else {
                    Some(IVec::from(self.prefix_encode(&self.hi)))
                }
            }
            Bound::Included(b) => Some(IVec::from(self.prefix_encode(b))),
            Bound::Excluded(b) => {
                // we use manual prefix encoding here because
                // there is an assertion in `prefix_encode`
                // that asserts the key is within the node,
                // and maybe `b` is above the node.
                let encoded = &b[self.prefix_len as usize..];
                Some(IVec::from(encoded))
            }
        };

        let leaf = self.data.leaf_ref().unwrap();
        let search = if let Some(successor_key) = successor_key {
            leaf.keys.binary_search_by(|k| fastcmp(k, &successor_key))
        } else if leaf.keys.is_empty() {
            Err(0)
        } else {
            Ok(leaf.keys.len() - 1)
        };

        let end = match search {
            Ok(end) => end,
            Err(end) if end > 0 => end - 1,
            _ => return None,
        };

        for (idx, k) in leaf.keys[0..=end].iter().enumerate().rev() {
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
            let decoded_key = self.prefix_decode(k);

            return Some((decoded_key, leaf.values[idx].clone()));
        }
        None
    }

    /// `leaf_pair_for_key` finds an existing value pair for a given key.
    pub(crate) fn leaf_pair_for_key(
        &self,
        key: &[u8],
    ) -> Option<(&IVec, &IVec)> {
        let leaf = self
            .data
            .leaf_ref()
            .expect("leaf_pair_for_key called on index node");

        let suffix = &key[self.prefix_len as usize..];

        let search = leaf.keys.binary_search_by(|k| fastcmp(k, suffix)).ok();

        search.map(|idx| (&leaf.keys[idx], &leaf.values[idx]))
    }

    /// `node_kv_pair` returns either existing (node/key, value) pair or
    /// (node/key, none) where a node/key is node level encoded key.
    pub fn node_kv_pair(&self, key: &[u8]) -> (IVec, Option<IVec>) {
        assert!(key >= self.lo.as_ref());
        if !self.hi.is_empty() {
            assert!(key < self.hi.as_ref());
        }
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
        let index =
            self.data.index_ref().expect("index_next_node called on leaf");

        let suffix = &key[self.prefix_len as usize..];

        let search = binary_search_lub(suffix, &index.keys);

        let pos = search.expect("failed to traverse index");

        (pos, index.pointers[pos])
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Data {
    Index(Index),
    Leaf(Leaf),
}

impl Default for Data {
    fn default() -> Data {
        Data::Leaf(Leaf::default())
    }
}

impl Data {
    pub(crate) fn rss(&self) -> u64 {
        match self {
            Data::Index(ref index) => {
                index.keys.iter().map(|k| k.len() + 8).sum::<usize>() as u64
            }
            Data::Leaf(ref leaf) => leaf
                .keys
                .iter()
                .zip(leaf.values.iter())
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>() as u64,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match *self {
            Data::Index(ref index) => index.keys.len(),
            Data::Leaf(ref leaf) => leaf.keys.len(),
        }
    }

    pub(crate) fn parent_merge_confirm(&mut self, merged_child_pid: PageId) {
        match self {
            Data::Index(ref mut index) => {
                let idx = index
                    .pointers
                    .iter()
                    .position(|c| *c == merged_child_pid)
                    .unwrap();
                index.keys.remove(idx);
                index.pointers.remove(idx);
            }
            _ => panic!("parent_merge_confirm called on leaf data"),
        }
    }

    pub(crate) fn leaf_ref(&self) -> Option<&Leaf> {
        match *self {
            Data::Index(_) => None,
            Data::Leaf(ref leaf) => Some(leaf),
        }
    }

    pub(crate) fn index_ref(&self) -> Option<&Index> {
        match *self {
            Data::Index(ref index) => Some(index),
            Data::Leaf(_) => None,
        }
    }

    pub(crate) fn is_index(&self) -> bool {
        if let Data::Index(..) = self { true } else { false }
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) struct Leaf {
    pub(crate) keys: Vec<IVec>,
    pub(crate) values: Vec<IVec>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) struct Index {
    pub(crate) keys: Vec<IVec>,
    pub(crate) pointers: Vec<PageId>,
}

#[cfg(feature = "lock_free_delays")]
fn is_sorted<T: PartialOrd>(xs: &[T]) -> bool {
    xs.windows(2).all(|pair| pair[0] <= pair[1])
}

#[test]
fn merge_uneven_nodes() {
    let left = Node {
        data: Data::Leaf(Leaf {
            keys: vec![vec![230, 126, 1, 0].into()],
            values: vec![vec![].into()],
        }),
        next: NonZeroU64::new(1),
        lo: vec![230, 125, 1, 0].into(),
        hi: vec![230, 134, 0, 0].into(),
        merging_child: None,
        merging: false,
        prefix_len: 0,
    };

    let right = Node {
        data: Data::Leaf(Leaf {
            keys: vec![vec![134, 0, 0].into()],
            values: vec![vec![].into()],
        }),
        next: None,
        lo: vec![230, 134, 0, 0].into(),
        hi: vec![230, 147, 0, 0].into(),
        merging_child: None,
        merging: false,
        prefix_len: 1,
    };

    left.receive_merge(&right);
}

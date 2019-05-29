use std::{fmt, mem::size_of};

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
            Base(_) => panic!("encountered base page in middle of chain"),
            ParentMergeIntention(pid) => {
                assert!(self.merging_child.is_none());
                self.merging_child = Some(pid);
            }
            ParentMergeConfirm => {
                assert!(self.merging_child.is_some());
                self.merging_child = None;
            }
            ChildMergeCap => {
                assert!(!self.merging);
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

    pub(crate) fn parent_split(&mut self, at: &[u8], to: PageId) {
        if let Data::Index(ref mut ptrs) = self.data {
            let encoded_sep = prefix_encode(&self.lo, at);
            match ptrs.binary_search_by(|a| prefix_cmp(&a.0, &encoded_sep)) {
                Ok(_) => panic!("must not have found ptr"),
                Err(idx) => ptrs.insert(idx, (encoded_sep, to)),
            }
        } else {
            panic!("tried to attach a ParentSplit to a Leaf chain");
        }
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

    pub(crate) fn split(&self) -> Node {
        let (split, right_data) = self.data.split(&self.lo);
        Node {
            data: right_data,
            next: self.next,
            lo: split,
            hi: self.hi.clone(),
            merging_child: None,
            merging: false,
        }
    }

    pub(crate) fn receive_merge(&self, rhs: &Node) -> Node {
        let mut merged = self.clone();
        merged.hi = rhs.hi.clone();
        merged.data.receive_merge(
            rhs.lo.as_ref(),
            merged.lo.as_ref(),
            &rhs.data,
        );
        merged
    }
}

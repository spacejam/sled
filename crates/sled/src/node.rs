use std::mem::size_of;

use super::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Node {
    pub(crate) id: PageId,
    pub(crate) data: Data,
    pub(crate) next: Option<PageId>,
    pub(crate) lo: Vec<u8>,
    pub(crate) hi: Vec<u8>,
}

impl Node {
    #[inline]
    pub(crate) fn size_in_bytes(&self) -> u64 {
        let self_sz = size_of::<Node>() as u64;
        let lo_sz = (size_of::<Vec<u8>>() + self.lo.len()) as u64;
        let hi_sz = (size_of::<Vec<u8>>() + self.hi.len()) as u64;
        let data_sz = self.data.size_in_bytes();

        self_sz
            .saturating_add(lo_sz)
            .saturating_add(hi_sz)
            .saturating_add(data_sz)
    }

    pub(crate) fn apply(
        &mut self,
        frag: &Frag,
        merge_operator: Option<usize>,
    ) {
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
                    panic!("tried to consolidate set at key <= hi")
                }
            }
            Merge(ref k, ref v) => {
                // (when hi is empty, it means it's unbounded)
                if self.hi.is_empty()
                    || prefix_cmp_encoded(k, &self.hi, &self.lo)
                        == std::cmp::Ordering::Less
                {
                    let merge_fn_ptr = merge_operator
                        .expect("must have a merge operator set");
                    unsafe {
                        let merge_fn: MergeOperator =
                            std::mem::transmute(merge_fn_ptr);
                        self.merge_leaf(
                            k.clone(),
                            v.clone(),
                            merge_fn,
                        );
                    }
                } else {
                    panic!("tried to consolidate set at key <= hi")
                }
            }
            ChildSplit(ref child_split) => {
                self.child_split(child_split);
            }
            ParentSplit(ref parent_split) => {
                self.parent_split(parent_split);
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
            Base(_, _) => {
                panic!("encountered base page in middle of chain")
            }
            Counter(_) => unimplemented!(),
        }
    }

    pub(crate) fn set_leaf(&mut self, key: Key, val: Value) {
        if let Data::Leaf(ref mut records) = self.data {
            let search =
                records.binary_search_by(|&(ref k, ref _v)| {
                    prefix_cmp(k, &*key)
                });
            if let Ok(idx) = search {
                records[idx] = (key, val);
            } else {
                records.push((key, val));
                records.sort_unstable_by(|a, b| {
                    prefix_cmp(&*a.0, &*b.0)
                });
            }
        } else {
            panic!("tried to Set a value to an index");
        }
    }

    pub(crate) fn merge_leaf(
        &mut self,
        key: Key,
        val: Value,
        merge_fn: MergeOperator,
    ) {
        if let Data::Leaf(ref mut records) = self.data {
            let search =
                records.binary_search_by(|&(ref k, ref _v)| {
                    prefix_cmp(k, &*key)
                });

            let decoded_k = prefix_decode(&self.lo, &key);
            if let Ok(idx) = search {
                let new = merge_fn(
                    &*decoded_k,
                    Some(&records[idx].1),
                    &val,
                );
                if let Some(new) = new {
                    records[idx] = (key, new);
                } else {
                    records.remove(idx);
                }
            } else {
                let new = merge_fn(&*decoded_k, None, &val);
                if let Some(new) = new {
                    records.push((key, new));
                    records.sort_unstable_by(|a, b| {
                        prefix_cmp(&*a.0, &*b.0)
                    });
                }
            }
        } else {
            panic!("tried to Merge a value to an index");
        }
    }

    pub(crate) fn child_split(&mut self, cs: &ChildSplit) {
        self.data.drop_gte(&cs.at, &self.lo);
        self.hi = cs.at.clone();
        self.next = Some(cs.to);
    }

    pub(crate) fn parent_split(&mut self, ps: &ParentSplit) {
        if let Data::Index(ref mut ptrs) = self.data {
            let encoded_sep = prefix_encode(&self.lo, &ps.at);
            ptrs.push((encoded_sep, ps.to));
            ptrs.sort_unstable_by(|a, b| prefix_cmp(&*a.0, &*b.0));
        } else {
            panic!("tried to attach a ParentSplit to a Leaf chain");
        }
    }

    pub(crate) fn del_leaf(&mut self, key: KeyRef<'_>) {
        if let Data::Leaf(ref mut records) = self.data {
            let search =
                records.binary_search_by(|&(ref k, ref _v)| {
                    prefix_cmp(k, &*key)
                });
            if let Ok(idx) = search {
                records.remove(idx);
            }
        } else {
            panic!("tried to attach a Del to an Index chain");
        }
    }

    pub(crate) fn should_split(&self, max_sz: u64) -> bool {
        self.data.len() > 2 && self.size_in_bytes() > max_sz
    }

    pub(crate) fn split(&self, id: PageId) -> Node {
        let (split, right_data) = self.data.split(&self.lo);
        Node {
            id,
            data: right_data,
            next: self.next,
            lo: split,
            hi: self.hi.clone(),
        }
    }
}

use super::*;

pub(crate) enum Nav<'a> {
    Restart,
    Right(PageId),
    Down(PageId),
    Value(Option<&'a IVec>),
}

#[derive(Clone)]
pub(crate) struct View<'a> {
    pub(crate) pid: PageId,
    pub(crate) lo: &'a [u8],
    pub(crate) hi: &'a [u8],
    pub(crate) is_index: bool,
    pub(crate) next: Option<PageId>,
    pub(crate) ptr: TreePtr<'a>,
    frags: Vec<&'a Frag>,
    base_offset: usize,
    pub(crate) base_data: &'a Data,
}

impl<'a> View<'a> {
    pub(crate) fn new(
        pid: PageId,
        ptr: TreePtr<'a>,
        frags: Vec<&'a Frag>,
    ) -> View<'a> {
        let mut view = View {
            pid,
            ptr,
            frags,
            lo: &[],
            hi: &[],
            is_index: false,
            base_offset: 0,
            base_data: unsafe { std::mem::uninitialized() },
            next: None,
        };

        for (offset, frag) in view.frags.iter().enumerate() {
            match frag {
                Frag::Base(node) => {
                    if view.hi.is_empty() {
                        view.hi = node.hi.as_ref();
                        view.next = node.next;
                    }
                    view.lo = node.lo.as_ref();
                    view.is_index = node.data.index_ref().is_some();
                    view.base_offset = offset;
                    view.base_data = &node.data;
                    break;
                }
                _ => {}
            }
        }

        view
    }

    pub(crate) fn key_range(
        &self,
    ) -> Option<impl std::ops::RangeBounds<std::ops::Bound<&[u8]>>> {
        if self.is_index {
            return None;
        }

        let lo = if self.lo.is_empty() {
            std::ops::Bound::Unbounded
        } else {
            std::ops::Bound::Included(self.lo)
        };

        let hi = if self.hi.is_empty() {
            std::ops::Bound::Unbounded
        } else {
            std::ops::Bound::Excluded(self.hi)
        };

        Some(lo..hi)
    }

    pub(crate) fn nav_key(&self, key: &[u8]) -> Nav {
        unimplemented!()
    }

    pub(crate) fn nav_predecessor(&self, key: &[u8]) -> Nav {
        unimplemented!()
    }

    pub(crate) fn nav_successor(&self, key: &[u8]) -> Nav {
        unimplemented!()
    }

    pub(crate) fn is_free(&self) -> bool {
        self.frags.is_empty()
    }

    pub(crate) fn leaf_value_for_key(
        &self,
        key: &[u8],
        config: &Config,
    ) -> Option<IVec> {
        assert!(!self.is_index);

        let mut merge_base = None;
        let mut merges = vec![];

        for frag in self.frags[..self.base_offset + 1].iter() {
            match frag {
                Frag::Set(k, val) if self.key_eq(k, key) => {
                    if merges.is_empty() {
                        return Some(val.clone());
                    } else {
                        merge_base = Some(val);
                        break;
                    }
                }
                Frag::Del(k) if self.key_eq(k, key) => return None,
                Frag::Merge(k, val) if self.key_eq(k, key) => merges.push(val),
                Frag::Base(node) => {
                    let data = &node.data;
                    let items =
                        data.leaf_ref().expect("last_node should be a leaf");
                    let search = items
                        .binary_search_by(|&(ref k, ref _v)| {
                            prefix_cmp_encoded(k, key.as_ref(), &node.lo)
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

    pub(crate) fn index_next_node(&self, key: &[u8]) -> PageId {
        assert!(self.is_index);

        for frag in self.frags[..self.base_offset + 1].iter() {
            match frag {
                Frag::Set(..) => unimplemented!(),
                Frag::Del(..) => unimplemented!(),
                Frag::Merge(..) => unimplemented!(),
                Frag::Base(node) => {
                    let data = &node.data;
                    let items =
                        data.index_ref().expect("last_node should be a leaf");
                    let search =
                        binary_search_lub(items, |&(ref k, ref _v)| {
                            prefix_cmp_encoded(k, key.as_ref(), &node.lo)
                        });

                    // This might be none if ord is Less and we're
                    // searching for the empty key
                    let index = search.expect("failed to traverse index");

                    return items[index].1;
                }
            }
        }
        panic!("no index found")
    }

    pub(crate) fn predecessor(&self, key: &[u8]) -> Option<&IVec> {
        unimplemented!()
    }

    pub(crate) fn should_split(&self, max_sz: u64) -> bool {
        let children = self.base_data.len();
        children > 2 && self.size_in_bytes() > max_sz
    }

    pub(crate) fn compact(&self, config: &Config) -> Node {
        let mut lhs = self.frags[self.base_offset].unwrap_base().clone();
        for offset in (0..self.base_offset).rev() {
            let frag = self.frags[offset];
            lhs.apply(frag, config.merge_operator);
        }
        lhs
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

    #[inline]
    pub(crate) fn size_in_bytes(&self) -> u64 {
        // TODO needs to better account for
        // sizes that don't actually fall under
        // a merge threshold once we support one.
        self.frags[..self.base_offset + 1]
            .iter()
            .map(|f| f.size_in_bytes())
            .sum()
    }
}

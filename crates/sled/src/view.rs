use super::*;

pub(crate) struct View<'a> {
    pub(crate) pid: PageId,
    pub(crate) lo: &'a [u8],
    pub(crate) hi: &'a [u8],
    pub(crate) is_index: bool,
    pub(crate) next: Option<PageId>,
    pub(crate) ptr: TreePtr<'a>,
    frags: Vec<&'a Frag>,
    base_offset: usize,
    base_data: &'a Data,
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
                Frag::ChildSplit(cs) => {
                    view.next = Some(cs.to);
                    view.hi = cs.at.as_ref();
                }
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

    pub(crate) fn is_free(&self) -> bool {
        self.frags.is_empty()
    }

    pub(crate) fn leaf_value_for_key(&self, key: &[u8]) -> Option<&IVec> {
        assert!(!self.is_index);

        for frag in self.frags[..self.base_offset + 1].iter() {
            match frag {
                Frag::Set(k, val) if k == key => return Some(val),
                Frag::Del(k) if k == key => return None,
                Frag::Merge(_key, _val) => unimplemented!(),
                Frag::ChildSplit(cs) => {
                    assert!(&*cs.at > key);
                }
                Frag::ParentSplit(_ps) => unimplemented!(),
                Frag::Base(node) => {
                    let data = &node.data;
                    let items =
                        data.leaf_ref().expect("last_node should be a leaf");
                    let search = items
                        .binary_search_by(|&(ref k, ref _v)| {
                            prefix_cmp_encoded(k, key.as_ref(), &node.lo)
                        })
                        .ok();

                    return search.map(|idx| &items[idx].1);
                }
                _ => {}
            }
        }
        None
    }

    pub(crate) fn index_next_node(&self, key: &[u8]) -> PageId {
        assert!(self.is_index);

        for frag in self.frags[..self.base_offset + 1].iter() {
            match frag {
                Frag::Set(key, val) => unimplemented!(),
                Frag::Del(key) => unimplemented!(),
                Frag::Merge(_key, _val) => unimplemented!(),
                Frag::ChildSplit(cs) => {
                    assert!(&*cs.at > key);
                }
                Frag::ParentSplit(ps) => {}
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

    pub(crate) fn should_split(&self, max_sz: u64) -> bool {
        self.size_in_bytes() > max_sz
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
            .take_while(|f| !f.is_child_split())
            .map(|f| f.size_in_bytes())
            .sum()
    }
}

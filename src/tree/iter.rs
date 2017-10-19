use super::*;

/// An iterator over keys and values in a `Tree`.
pub struct Iter<'a> {
    pub(super) id: PageID,
    pub(super) inner: &'a PageCache<BLinkMaterializer, Frag, PageID>,
    pub(super) last_key: Bound,
    // TODO we have to refactor this in light of pages being deleted
}

impl<'a> Iterator for Iter<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        let start = clock();
        loop {
            let (frag, _cas_key) = self.inner.get(self.id).unwrap();
            let (node, _is_root) = frag.base().unwrap();
            // TODO this could be None if the node was removed since the last
            // iteration, and we need to just get the inner node again...
            for (ref k, ref v) in node.data.leaf().unwrap() {
                if Bound::Inc(k.clone()) > self.last_key {
                    self.last_key = Bound::Inc(k.to_vec());
                    let ret = Some((k.clone(), v.clone()));
                    M.tree_scan.measure(clock() - start);
                    return ret;
                }
            }
            if node.next.is_none() {
                M.tree_scan.measure(clock() - start);
                return None;
            }
            self.id = node.next.unwrap();
        }
    }
}

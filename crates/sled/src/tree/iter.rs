use super::*;

use epoch::pin;

/// An iterator over keys and values in a `Tree`.
pub struct Iter<'a> {
    pub(super) id: PageID,
    pub(super) inner:
        &'a PageCache<BLinkMaterializer, Frag, Vec<(PageID, PageID)>>,
    pub(super) last_key: Bound,
    pub(super) broken: Option<Error<()>>,
    pub(super) done: bool,
    // TODO we have to refactor this in light of pages being deleted
}

impl<'a> Iterator for Iter<'a> {
    type Item = DbResult<(Vec<u8>, Vec<u8>), ()>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        } else if let Some(broken) = self.broken.take() {
            self.done = true;
            return Some(Err(broken));
        };

        let guard = pin();
        loop {
            let res = self.inner.get(self.id, &guard);
            if res.is_err() {
                // TODO this could be None if the node was removed since the last
                // iteration, and we need to just get the inner node again...
                error!("iteration failed: {:?}", res);
                self.done = true;
                return Some(Err(res.unwrap_err().danger_cast()));
            }

            let (frag, _cas_key) = res.unwrap().unwrap();
            let (node, _is_root) = frag.base().unwrap();
            let prefix = node.lo.inner();
            for (ref k, ref v) in node.data.leaf().unwrap() {
                let decoded_k = prefix_decode(prefix, k);
                if Bound::Inclusive(decoded_k.clone()) > self.last_key {
                    self.last_key = Bound::Inclusive(decoded_k.to_vec());
                    let ret = Ok((decoded_k, v.clone()));
                    return Some(ret);
                }
            }
            if node.next.is_none() {
                return None;
            }
            self.id = node.next.unwrap();
        }
    }
}

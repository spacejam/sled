use super::*;

use pagecache::pin;

/// An iterator over keys and values in a `Tree`.
pub struct Iter<'a> {
    pub(super) id: PageId,
    pub(super) inner:
        &'a PageCache<BLinkMaterializer, Frag, Vec<(PageId, PageId)>>,
    pub(super) last_key: Bound,
    pub(super) broken: Option<Error<()>>,
    pub(super) done: bool,
    // TODO we have to refactor this in light of pages being deleted
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>), ()>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        } else if let Some(broken) = self.broken.take() {
            self.done = true;
            return Some(Err(broken));
        };

        let guard = pin();
        loop {
            let res = self
                .inner
                .get(self.id, &guard)
                .map(|page_get| page_get.unwrap());

            if let Err(e) = res {
                error!("iteration failed: {:?}", e);
                self.done = true;
                return Some(Err(e.danger_cast()));
            }

            // TODO(when implementing merge support) this could
            // be None if the node was removed since the last
            // iteration, and we need to just get the inner
            // node again...
            let ptr = res.unwrap();
            let node: &Node = ptr.unwrap_base_ptr();

            let prefix = node.lo.inner();
            for (ref k, ref v) in
                node.data.leaf().expect("node should be a leaf")
            {
                let decoded_k = prefix_decode(prefix, k);
                if Bound::Inclusive(decoded_k.clone()) > self.last_key
                {
                    self.last_key =
                        Bound::Inclusive(decoded_k.to_vec());
                    let ret = Ok((decoded_k, v.clone()));
                    return Some(ret);
                }
            }
            match node.next {
                Some(id) => self.id = id,
                None => return None,
            }
        }
    }
}

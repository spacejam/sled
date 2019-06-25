use std::collections::HashMap;

use super::*;

/// A batch of updates that will
/// be applied atomically to the
/// Tree.
pub struct Batch<'a> {
    pub(super) tree: &'a Tree,
    pub(super) writes: HashMap<IVec, Option<IVec>>,
}

impl<'a> Batch<'a> {
    /// Set a key to a new value
    pub fn set<K, V>(&mut self, key: K, value: V)
    where
        IVec: From<K>,
        IVec: From<V>,
    {
        self.writes.insert(IVec::from(key), Some(IVec::from(value)));
    }

    /// Remove a key
    pub fn del<K>(&mut self, key: K)
    where
        IVec: From<K>,
    {
        self.writes.insert(IVec::from(key), None);
    }

    /// Atomically apply the `Batch`
    pub fn apply(self) -> Result<()> {
        let peg = self.tree.context.pin_log()?;
        let cc = self.tree.concurrency_control.write().unwrap();
        for (k, v_opt) in self.writes.into_iter() {
            if let Some(v) = v_opt {
                self.tree.set_inner(k, v)?;
            } else {
                self.tree.del_inner(k)?;
            }
        }
        drop(cc);

        // when the peg drops, it ensures all updates
        // written to the log since its creation are
        // recovered atomically
        peg.seal_batch()
    }
}

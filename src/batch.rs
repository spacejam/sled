#![allow(unused_results)]

use std::collections::HashMap;

use super::*;

/// A batch of updates that will
/// be applied atomically to the
/// Tree.
#[derive(Debug, Default, Clone)]
pub struct Batch {
    pub(crate) writes: HashMap<IVec, Option<IVec>>,
}

impl Batch {
    /// Set a key to a new value
    pub fn insert<K, V>(&mut self, key: K, value: V)
    where
        IVec: From<K> + From<V>,
    {
        self.writes.insert(IVec::from(key), Some(IVec::from(value)));
    }

    /// Remove a key
    pub fn remove<K>(&mut self, key: K)
    where
        IVec: From<K>,
    {
        self.writes.insert(IVec::from(key), None);
    }
}

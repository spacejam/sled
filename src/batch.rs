#![allow(unused_results)]

use std::collections::HashMap;

use super::*;

/// A batch of updates that will
/// be applied atomically to the
/// Tree.
///
/// # Examples
///
/// ```
/// use sled::{Batch, open};
///
/// # let _ = std::fs::remove_dir_all("batch_db");
/// let db = open("batch_db").unwrap();
/// db.insert("key_0", "val_0").unwrap();
///
/// let mut batch = Batch::default();
/// batch.insert("key_a", "val_a");
/// batch.insert("key_b", "val_b");
/// batch.insert("key_c", "val_c");
/// batch.remove("key_0");
///
/// db.apply_batch(batch).unwrap();
/// // key_0 no longer exists, and key_a, key_b, and key_c
/// // now do exist.
/// # let _ = std::fs::remove_dir_all("batch_db");
/// ```
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

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
/// # let _ = std::fs::remove_dir_all("batch_db_2");
/// let db = open("batch_db_2").unwrap();
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
/// # let _ = std::fs::remove_dir_all("batch_db_2");
/// ```
#[derive(Debug, Default, Clone)]
pub struct Batch {
    pub(crate) writes: HashMap<IVec, Option<IVec>>,
}

impl Batch {
    /// Set a key to a new value
    pub fn insert<K: Into<IVec>, V: Into<IVec>>(&mut self, key: K, value: V) {
        self.writes.insert(key.into(), Some(value.into()));
    }

    /// Remove a key
    pub fn remove<K: Into<IVec>>(&mut self, key: K) {
        self.writes.insert(key.into(), None);
    }
}

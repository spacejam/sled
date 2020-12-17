#![allow(unused_results)]

use super::*;

/// A batch of updates that will
/// be applied atomically to the
/// Tree.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use sled::{Batch, open};
///
/// # let _ = std::fs::remove_dir_all("batch_db_2");
/// let db = open("batch_db_2")?;
/// db.insert("key_0", "val_0")?;
///
/// let mut batch = Batch::default();
/// batch.insert("key_a", "val_a");
/// batch.insert("key_b", "val_b");
/// batch.insert("key_c", "val_c");
/// batch.remove("key_0");
///
/// db.apply_batch(batch)?;
/// // key_0 no longer exists, and key_a, key_b, and key_c
/// // now do exist.
/// # let _ = std::fs::remove_dir_all("batch_db_2");
/// # Ok(()) }
/// ```
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Batch {
    pub(crate) writes: Map<IVec, Option<IVec>>,
}

impl Batch {
    /// Set a key to a new value
    pub fn insert<K, V>(&mut self, key: K, value: V)
    where
        K: Into<IVec>,
        V: Into<IVec>,
    {
        self.writes.insert(key.into(), Some(value.into()));
    }

    /// Remove a key
    pub fn remove<K>(&mut self, key: K)
    where
        K: Into<IVec>,
    {
        self.writes.insert(key.into(), None);
    }

    /// Get a value if it is present in the `Batch`.
    /// `Some(None)` means it's present as a deletion.
    pub fn get<K: AsRef<[u8]>>(&self, k: K) -> Option<Option<&IVec>> {
        let inner = self.writes.get(k.as_ref())?;
        Some(inner.as_ref())
    }
}

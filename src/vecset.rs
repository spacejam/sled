use serde::{Deserialize, Serialize};

/// A set built on top of `Vec` and binary search,
/// for use when calling `nth` to iterate over a set
/// is too expensive, and the set size is expected to be
/// ~10 or less most of the time.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VecSet<T> {
    inner: Vec<T>,
}

impl<T> VecSet<T>
where
    T: Ord + Eq,
{
    /// Insert a new item
    pub fn insert(&mut self, item: T) -> bool {
        if let Err(insert_idx) = self.inner.binary_search(&item) {
            self.inner.insert(insert_idx, item);
            true
        } else {
            false
        }
    }

    /// Removes an item by value
    pub fn remove(&mut self, item: &T) -> Option<T> {
        if let Ok(at) = self.inner.binary_search(item) {
            Some(self.inner.remove(at))
        } else {
            None
        }
    }

    /// Returns `true` if the item is present
    pub fn contains(&self, item: &T) -> bool {
        self.inner.binary_search(item).is_ok()
    }

    /// Returns `true` if the set is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the size of the set
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Indexes into the underlying buffer
    pub fn get(&self, idx: usize) -> Option<&T> {
        self.inner.get(idx)
    }

    /// Iterate over the contents of this set
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.inner.iter()
    }
}

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

/// Database abstraction layer which can be used to insert, read and update values.
/// Uses a tree implementation for data access under the hood.
///
/// An ESL implementation keeps track of the inserts and deletes.
pub struct DB {
    tree: Arc<Tree>,
    esl: Arc<AtomicUsize>,
}

impl DB {
    /// Creates a new `DB` instance.
    pub fn open() -> DB {
        let tree = Tree::new();
        DB {
            tree: Arc::new(tree),
            esl: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Sets a key-value entry.
    pub fn insert(&self, k: Key, v: Value) {
        self.esl.fetch_add(1, Ordering::SeqCst);
        self.tree.set(k, v);
    }

    /// Return a value based on the key.
    pub fn read(&self, k: Key) -> Option<Value> {
        self.tree.get(&*k)
    }

    /// Delete a value based on the key.
    pub fn del(&self, k: Key) {
        self.esl.fetch_add(1, Ordering::SeqCst);
        self.tree.del(&*k);
    }
}

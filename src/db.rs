use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

pub struct DB {
    tree: Arc<Tree>,
    esl: Arc<AtomicUsize>,
}

impl DB {
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> DB {
        let tree = Tree::new(path);
        DB {
            tree: Arc::new(tree),
            esl: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn insert(&self, k: Key, v: Value) {
        self.esl.fetch_add(1, Ordering::SeqCst);
        self.tree.set(k, v);
    }

    pub fn read(&self, k: Key) -> Option<Value> {
        self.tree.get(&*k)
    }

    pub fn del(&self, k: Key) {
        self.esl.fetch_add(1, Ordering::SeqCst);
        self.tree.del(&*k);
    }
}

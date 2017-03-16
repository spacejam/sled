use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

pub struct DB {
    // tx table maps from TxID to Tx chain
    tx_table: Arc<Radix<Stack<Tx>>>,
    tree: Arc<Tree>,
    // end_stable_log is the highest LSN that may be flushed
    lsn: Arc<AtomicUsize>,
    esl: Arc<AtomicUsize>,
}

impl DB {
    pub fn open() -> DB {
        let tree = Tree::open();
        let esl = tree.esl.clone();
        DB {
            tx_table: Arc::new(Radix::default()),
            tree: Arc::new(tree),
            lsn: Arc::new(AtomicUsize::new(esl.load(Ordering::SeqCst))),
            esl: esl,
        }
    }

    pub fn insert(&self, k: Key, v: Value) {
        self.esl.fetch_add(1, Ordering::SeqCst);
        self.tree.insert(k, v);
    }

    pub fn read(&self, k: Key) -> Option<Value> {
        self.tree.read(k)
    }

    pub fn delete(&self, k: Key) {
        self.esl.fetch_add(1, Ordering::SeqCst);
        self.tree.delete(k);
    }
}

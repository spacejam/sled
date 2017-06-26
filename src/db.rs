use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

struct Tx;

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
        let tree = Tree::new();
        DB {
            tx_table: Arc::new(Radix::default()),
            tree: Arc::new(tree),
            lsn: Arc::new(AtomicUsize::new(0)),
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

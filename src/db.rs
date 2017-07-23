use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

pub struct DB {
    tree: Arc<Tree>,
    esl: Arc<AtomicUsize>,
}

impl DB {
    pub fn open() -> DB {
        let tree = Tree::new();
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

#[test]
fn basic_functionality() {
    let db = DB::open();
    let esl = &db.esl;
    assert_eq!(esl.load(Ordering::SeqCst), 0);

    db.insert(vec![1], vec![42]);
    assert_eq!(esl.load(Ordering::SeqCst), 1);

    let result = db.read(vec![1]);
    assert_eq!(result, Some(vec![42]));

    db.del(vec![1]);
    assert_eq!(esl.load(Ordering::SeqCst), 2);

    let result = db.read(vec![1]);
    assert_eq!(result, None);
}

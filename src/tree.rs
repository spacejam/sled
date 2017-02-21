use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

// stories
// 1. allocate new page for root of tree
// 1. insert keys
// 1. deallocate page at end of epoch
// 1. flush, should flush none
// 1. bump ESL, flush, should flush all under
// 1. perform page consolidation after 6 deltas
// 1. split page, install index as root node
// 1. after it's big, trigger merge
// 1. close & recover
// 1. checkpoint pagetable
// 1. recover from checkpoint

const ROOT: PageID = 0;

pub struct Tree {
    // pager is our pagecache
    pager: Arc<Pager>,
    // end_stable_log is the highest LSN that may be flushed
    esl: Arc<AtomicUsize>,
}

impl Tree {
    pub fn open() -> Tree {
        let pt = Pager::open();
        let esl = pt.esl.clone();
        Tree {
            pager: Arc::new(pt),
            esl: esl,
        }
    }

    pub fn insert(&self, k: Key, v: Value) {}

    pub fn read(&self, k: Key) -> Option<Value> {
        let mut path = vec![];
        let mut page_cursor = ROOT;

        while let Some(stack_ptr) = self.pager.read(page_cursor) {
            // page level
            path.push(page_cursor);

            let stack = unsafe { Box::from_raw(stack_ptr) };
            let mut chain_cursor = stack.head();
            mem::forget(stack);

            while !chain_cursor.is_null() {
                // delta chain level
                let node = unsafe { Box::from_raw(chain_cursor) };
                match node.data {
                    Data::Leaf(ref key_record_pairs) => {}
                    Data::Index(ref key_ptr_pairs) => {}
                }

                mem::forget(node);
            }
            return None;
        }
        None
    }

    pub fn update(&self, k: Key, v: Value) {}

    pub fn delete(&self, k: Key) {}
}

#[test]
fn basic() {
    let t = Tree::open();
    t.insert(b"k1".to_vec(), b"v1".to_vec());
    assert_eq!(t.read(b"k1".to_vec()), Some(b"v1".to_vec()));
    t.update(b"k1".to_vec(), b"v2".to_vec());
    assert_eq!(t.read(b"k1".to_vec()), Some(b"v2".to_vec()));
    t.delete(b"k1".to_vec());
    assert_eq!(t.read(b"k1".to_vec()), None);
}

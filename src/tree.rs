use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

// stories
// 1. allocate new page for root of tree
// 1. insert keys
// 1. flush, should flush none
// 1. bump ESL, flush, should flush all under
// 1. perform page consolidation after 6 deltas
// 1. split page, install index as root node
// 1. after it's big, trigger merge
// 1. close & recover
// 1. checkpoint pagetable
// 1. recover from checkpoint

pub struct Tree {
    // pages is our pagecache
    pages: Arc<PageCache>,
    // end_stable_log is the highest LSN that may be flushed
    esl: Arc<AtomicUsize>,
}

impl Tree {
    pub fn open() -> Tree {
        let pt = PageCache::open();
        let esl = pt.esl.clone();
        Tree {
            pages: Arc::new(pt),
            esl: esl,
        }
    }

    pub fn insert(&self, k: Key, v: Value) {}

    pub fn update(&self, k: Key, v: Value) {}
    pub fn delete(&self, k: Key) {}
}

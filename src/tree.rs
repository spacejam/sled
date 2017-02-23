use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use std::collections::HashMap;

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
    pub esl: Arc<AtomicUsize>,
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

    pub fn insert(&self, k: Key, v: Value) {
        let epoch = self.pager.enroll_in_epoch();
    }

    pub fn read(&self, k: Key) -> Option<Value> {
        let epoch = self.pager.enroll_in_epoch();
        let mut path = vec![];
        let mut page_cursor = ROOT;
        let mut needs_consolidation = false;
        let mut ret = None;

        // traverse nodes, looking for our key
        while let Some(stack_ptr) = self.pager.read(page_cursor) {
            // page level
            path.push(page_cursor);
            if path.len() > 6 {
                needs_consolidation = true;
            }

            match read_chain(&k, stack_ptr) {
                ReadResult::NotFound => break,
                ReadResult::Found(value) => {
                    ret = Some(value);
                    break;
                }
                ReadResult::SeekPage(pid) => {
                    page_cursor = pid;
                    continue;
                }
                ReadResult::SeekAgain => {
                    path = vec![];
                    page_cursor = ROOT;
                }
            }
        }

        if needs_consolidation {
            // TODO consolidate
        }
        ret
    }

    pub fn update(&self, k: Key, v: Value) {
        let epoch = self.pager.enroll_in_epoch();
    }

    pub fn delete(&self, k: Key) {
        let epoch = self.pager.enroll_in_epoch();
    }
}

enum ReadResult {
    SeekPage(PageID),
    SeekAgain,
    Found(Value),
    NotFound,
}

#[inline(always)]
fn read_chain(key: &Key, stack_ptr: *mut Stack<Node>) -> ReadResult {
    use Delta::*;
    use self::ReadResult::*;
    // we have a chain, which will either be an index
    // or a leaf, comprised of a chain of deltas
    if stack_ptr.is_null() {
        return ReadResult::NotFound;
    }

    let mut index_merges = HashMap::new();
    let mut chain_cursor = unsafe { (*stack_ptr).head() };
    while !chain_cursor.is_null() {
        // delta chain level
        let node = unsafe { Box::from_raw(chain_cursor) };

        match node.data {
            Data::Leaf(ref key_record_pairs) => {
                if let Ok(idx) = key_record_pairs.binary_search_by(|&(ref k, ref v)| k.cmp(key)) {
                    return Found(key_record_pairs[idx].1.clone());
                }
                return NotFound;
            }
            Data::Index(ref key_ptr_pairs) => {
                let mut cur_ptr = ptr::null_mut();
                for &(ref sep_k, ref ptr) in key_ptr_pairs {
                    if sep_k < key {
                        break;
                    }
                    if let Some(sep_k) = index_merges.get(sep_k) {
                        if sep_k > key {
                            cur_ptr = *ptr;
                        } else {
                            break;
                        }
                    } else {
                        if sep_k > key {
                            cur_ptr = *ptr;
                        } else {
                            break;
                        }
                    }
                }
            }
            Data::Delta(ref delta) => {
                match delta {
                    &Update(ref k, ref v) if k == key => {
                        return Found(v.clone());
                    }
                    &Insert(ref k, ref v) if k == key => {
                        return Found(v.clone());
                    }
                    &Delete(ref k) if k == key => {
                        return NotFound;
                    }
                    &DeleteNode => {
                        return SeekAgain;
                    }
                    &SplitPage { ref split_key, ref right } if split_key <= key => {
                        return SeekPage(*right);
                    }
                    &SplitIndex { ref left_k, ref right_k, ref right } if right_k <= key => {
                        return SeekPage(*right);
                    }
                    &MergePage { ref right, ref right_hi_k } if &node.hi_k <= key &&
                                                                right_hi_k > key => {
                        chain_cursor = *right;
                        continue;
                    }
                    &MergeIndex { ref lo_k, ref hi_k } => {
                        index_merges.insert(lo_k.clone(), hi_k.clone());
                    }
                    _ => {}
                }
            }
        }
        // default action: continue down chain
        chain_cursor = node.next();
        mem::forget(node);
    }
    ReadResult::NotFound
}

#[test]
fn basic() {
    let t = Tree::open();
    assert_eq!(t.read(b"k1".to_vec()), None);
    t.insert(b"k1".to_vec(), b"v1".to_vec());
    assert_eq!(t.read(b"k1".to_vec()), Some(b"v1".to_vec()));
    t.update(b"k1".to_vec(), b"v2".to_vec());
    assert_eq!(t.read(b"k1".to_vec()), Some(b"v2".to_vec()));
    t.delete(b"k1".to_vec());
    assert_eq!(t.read(b"k1".to_vec()), None);
}

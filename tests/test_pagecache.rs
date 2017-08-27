extern crate rsdb;
extern crate coco;
extern crate rand;
extern crate quickcheck;

use std::collections::HashMap;

use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};
use rand::{Rng, thread_rng};
use coco::epoch::Ptr;

use rsdb::*;

type PageID = usize;

#[derive(Clone)]
pub struct TestMaterializer;

impl Materializer for TestMaterializer {
    type PageFrag = String;
    type Recovery = ();

    fn merge(&self, frags: &[&String]) -> String {
        let mut consolidated = String::new();
        for frag in frags.into_iter() {
            consolidated.push_str(&*frag);
        }

        consolidated
    }

    fn recover(&self, _: &String) -> Option<()> {
        None
    }
}

#[test]
fn test_cache() {
    let path = "test_pagecache_caching";
    let conf = Config::default().cache_capacity(50).cache_bits(1).path(
        Some(
            path.to_owned(),
        ),
    );

    let mut pc = PageCache::new(TestMaterializer, conf.clone());
    pc.recover();

    let mut keys = HashMap::new();
    for _ in 0..10 {
        let (id, key) = pc.allocate();
        let key = pc.set(id, key, "a".to_owned()).unwrap();
        keys.insert(id, key);
    }

    for i in 0..100 {
        let id = i as usize % 10;
        let key = keys.get(&id).cloned().unwrap().unwrap();
        let key = pc.merge(id, key, "b".to_owned()).unwrap();
        keys.insert(id, key);
    }

    pc.__delete_all_files();
}

#[test]
fn basic_recovery() {
    let path = "test_pagecache_recovery";
    let snapshot_path = "test_pagecache.snapshot";
    let conf = Config::default()
        .flush_every_ms(None)
        .path(Some(path.to_owned()))
        .snapshot_path(Some(snapshot_path.to_owned()));

    let mut pc = PageCache::new(TestMaterializer, conf.clone());
    pc.recover();
    let (id, key) = pc.allocate();
    let key = pc.set(id, key, "a".to_owned()).unwrap().unwrap();
    let key = pc.merge(id, key, "b".to_owned()).unwrap().unwrap();
    let _key = pc.merge(id, key, "c".to_owned()).unwrap().unwrap();
    let (consolidated, _) = pc.get(id).unwrap();
    assert_eq!(consolidated, "abc".to_owned());
    drop(pc);

    let mut pc2 = PageCache::new(TestMaterializer, conf.clone());
    pc2.recover();
    let (consolidated2, key) = pc2.get(id).unwrap();
    assert_eq!(consolidated, consolidated2);

    pc2.merge(id, key, "d".to_owned()).unwrap().unwrap();
    drop(pc2);

    let mut pc3 = PageCache::new(TestMaterializer, conf.clone());
    pc3.recover();
    let (consolidated3, _key) = pc3.get(id).unwrap();
    assert_eq!(consolidated3, "abcd".to_owned());
    pc3.free(id);
    drop(pc3);

    let mut pc4 = PageCache::new(TestMaterializer, conf.clone());
    pc4.recover();
    let res = pc4.get(id);
    assert!(res.is_none());
    pc4.__delete_all_files();
}

#[derive(Debug, Clone)]
enum Op {
    Set(PageID, String, bool),
    Merge(PageID, String, bool),
    Get(PageID),
    Free(PageID),
    Allocate,
    Restart,
}

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        if g.gen_weighted_bool(10) {
            return Op::Restart;
        }

        let choice = g.gen_range(0, 5);

        match choice {
            0 => {
                Op::Set(
                    g.gen::<u8>() as PageID,
                    g.gen_ascii_chars().take(1).collect(),
                    g.gen::<bool>(),
                )
            }
            1 => {
                Op::Merge(
                    g.gen::<u8>() as PageID,
                    g.gen_ascii_chars().take(1).collect(),
                    g.gen::<bool>(),
                )
            }
            2 => Op::Get(g.gen::<u8>() as PageID),
            3 => Op::Free(g.gen::<u8>() as PageID),
            4 => Op::Allocate,
            _ => panic!("impossible choice"),
        }
    }
}

#[derive(Debug, Clone)]
struct OpVec {
    ops: Vec<Op>,
}

impl Arbitrary for OpVec {
    fn arbitrary<G: Gen>(g: &mut G) -> OpVec {
        let mut ops = vec![];
        for _ in 0..g.gen_range(1, 50) {
            let op = Op::arbitrary(g);
            ops.push(op);

        }
        OpVec {
            ops: ops,
        }
    }

    fn shrink(&self) -> Box<Iterator<Item = OpVec>> {
        let mut smaller = vec![];
        for i in 0..self.ops.len() {
            let mut clone = self.clone();
            clone.ops.remove(i);
            smaller.push(clone);
        }

        Box::new(smaller.into_iter())
    }
}

fn prop_pagecache_works(ops: OpVec, cache_fixup_threshold: u8) -> bool {
    use self::Op::*;
    let nonce: String = thread_rng().gen_ascii_chars().take(10).collect();
    let path = format!("quickcheck_pagecache_works_{}", nonce);
    let config = Config::default()
        .cache_fixup_threshold(cache_fixup_threshold as usize)
        .path(Some(path));

    let mut pc = PageCache::new(TestMaterializer, config.clone());
    pc.recover();

    let mut reference: HashMap<PageID, (String, CasKey<String>)> = HashMap::new();

    let bad_addr = 1 << std::mem::align_of::<String>().trailing_zeros();
    let bad_ptr = unsafe { Ptr::from_raw(bad_addr as *mut _) };

    for op in ops.ops.into_iter() {
        match op {
            Set(pid, c, good_key) => {
                let present = reference.contains_key(&pid);

                if present {
                    let (ref mut existing, ref mut old_key) = reference.get(&pid).cloned().unwrap();

                    if good_key {
                        let res = pc.merge(pid, old_key.clone(), c.clone());
                        let new_key = res.unwrap().unwrap();
                        *old_key = new_key;
                        existing.push_str(&*c);
                    } else {
                        let res = pc.merge(pid, bad_ptr.into(), c);
                        assert!(res.unwrap().is_err());
                    }
                } else {
                    let res = pc.set(pid, bad_ptr.into(), c.clone());
                    assert!(res.is_none());
                }
            }
            Merge(pid, c, good_key) => {
                let present = reference.contains_key(&pid);

                if present {
                    let (ref mut existing, ref mut old_key) = reference.get(&pid).cloned().unwrap();

                    if good_key {
                        let res = pc.merge(pid, old_key.clone(), c.clone());
                        let new_key = res.unwrap().unwrap();
                        *old_key = new_key;
                        existing.push_str(&*c);
                    } else {
                        let res = pc.merge(pid, bad_ptr.into(), c);
                        assert!(res.unwrap().is_err());
                    }
                } else {
                    let res = pc.merge(pid, bad_ptr.into(), c.clone());
                    assert!(res.is_none());
                }
            }
            Get(pid) => {
                let r = reference.get(&pid).cloned();
                let a = pc.get(pid);
                if let Some((ref s, _)) = r {
                    if s.is_empty() {
                        assert_eq!(a, None);
                    } else {
                        assert_eq!(r, a);
                    }
                } else {
                    assert_eq!(r, a);
                }
            }
            Free(pid) => {
                pc.free(pid);
                reference.remove(&pid);
            }
            Allocate => {
                let (pid, key) = pc.allocate();
                reference.insert(pid, (String::new(), key));
            }
            Restart => {
                drop(pc);
                pc = PageCache::new(TestMaterializer, config.clone());
                pc.recover();
            }
        }
    }

    pc.__delete_all_files();

    true
}

#[test]
fn quickcheck_pagecache_works() {
    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 1))
        .tests(50)
        .max_tests(100)
        .quickcheck(prop_pagecache_works as fn(OpVec, u8) -> bool);
}

#[test]
fn test_pagecache_bug_1() {
    // postmortem: this happened because `PageCache::page_in` assumed
    // at least one update had been stored for a retrieved page.
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![Allocate, Get(0)],
        },
        0,
    );
}

#[test]
fn test_pagecache_bug_2() {
    // postmortem: historically needed to "seed" a page by writing
    // a compacting base to it. changed the snapshot and page-in code
    // to allow a merge being the first update to hit a page.
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![Allocate, Restart, Merge(0, "K".to_owned(), false)],
        },
        0,
    );
}

#[test]
fn test_pagecache_bug_3() {
    // postmortem: this was a mismatch in semantics in the test harness itself
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![Allocate, Get(0)],
        },
        0,
    );
}

#[test]
fn test_pagecache_bug_4() {
    // postmortem: previously this caused a panic, we shouldn't break
    // when the user asks us to mutate non-existant pages!
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![Merge(98, "i".to_owned(), false)],
        },
        0,
    );
}

#[test]
fn test_pagecache_bug_5() {
    // postmortem: this was a mismatch in semantics in the test harness itself
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![
                Merge(132, "n".to_owned(), false),
                Set(132, "1".to_owned(), false),
            ],
        },
        0,
    );
}

#[test]
#[ignore]
fn test_pagecache_bug_() {
    // postmortem: TEMPLATE
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![],
        },
        0,
    );
}

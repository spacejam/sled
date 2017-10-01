extern crate sled;
extern crate coco;
extern crate rand;
extern crate quickcheck;

use std::collections::HashMap;
use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};

use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};
use coco::epoch::Ptr;

use sled::*;

type PageID = usize;

#[derive(Clone)]
pub struct TestMaterializer;

impl Materializer for TestMaterializer {
    type PageFrag = Vec<usize>;
    type Recovery = ();

    fn merge(&self, frags: &[&Vec<usize>]) -> Vec<usize> {
        let mut consolidated = vec![];
        for &frag in frags.iter() {
            let mut frag = frag.clone();
            consolidated.append(&mut frag);
        }

        consolidated
    }

    fn recover(&self, _: &Vec<usize>) -> Option<()> {
        None
    }
}

#[test]
fn pagecache_caching() {
    let conf = Config::default().cache_capacity(40).cache_bits(0);

    let mut pc = PageCache::new(TestMaterializer, conf.clone());
    pc.recover();

    let mut keys = HashMap::new();
    for _ in 0..2 {
        let (id, key) = pc.allocate();
        let key = pc.set(id, key, vec![0]).unwrap();
        keys.insert(id, key);
    }

    for i in 0..1000 {
        let id = i as usize % 2;
        let (_, key) = pc.get(id).unwrap();
        let key = pc.merge(id, key, vec![i]).unwrap();
        keys.insert(id, key);
    }
}

#[test]
fn basic_pagecache_recovery() {
    let conf = Config::default().flush_every_ms(None);

    let mut pc = PageCache::new(TestMaterializer, conf.clone());
    pc.recover();
    let (id, key) = pc.allocate();
    let key = pc.set(id, key, vec![1]).unwrap();
    let key = pc.merge(id, key, vec![2]).unwrap();
    let _key = pc.merge(id, key, vec![3]).unwrap();
    let (consolidated, _) = pc.get(id).unwrap();
    assert_eq!(consolidated, vec![1, 2, 3]);
    drop(pc);

    let mut pc2 = PageCache::new(TestMaterializer, conf.clone());
    pc2.recover();
    let (consolidated2, key) = pc2.get(id).unwrap();
    assert_eq!(consolidated, consolidated2);

    pc2.merge(id, key, vec![4]).unwrap();
    drop(pc2);

    let mut pc3 = PageCache::new(TestMaterializer, conf.clone());
    pc3.recover();
    let (consolidated3, _key) = pc3.get(id).unwrap();
    assert_eq!(consolidated3, vec![1, 2, 3, 4]);
    pc3.free(id);
    drop(pc3);

    let mut pc4 = PageCache::new(TestMaterializer, conf.clone());
    pc4.recover();
    let res = pc4.get(id);
    assert!(res.is_none());
}

#[derive(Debug, Clone)]
enum Op {
    Set(PageID, usize),
    Merge(PageID, usize),
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

        static COUNTER: AtomicUsize = ATOMIC_USIZE_INIT;
        COUNTER.compare_and_swap(0, 1, Ordering::SeqCst);

        let pid = (g.gen::<u8>() % 8) as PageID;

        match choice {
            0 => Op::Set(pid, COUNTER.fetch_add(1, Ordering::Relaxed)),
            1 => Op::Merge(pid, COUNTER.fetch_add(1, Ordering::Relaxed)),
            2 => Op::Get(pid),
            3 => Op::Free(pid),
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

        let mut clone = self.clone();
        let mut lowered = false;
        for op in clone.ops.iter_mut() {
            match *op {
                Op::Set(ref mut pid, _) |
                Op::Merge(ref mut pid, _) |
                Op::Get(ref mut pid) |
                Op::Free(ref mut pid) if *pid > 0 => {
                    lowered = true;
                    *pid -= 1;
                }
                _ => {}
            }
        }
        if lowered {
            smaller.push(clone);
        }


        Box::new(smaller.into_iter())
    }
}

fn prop_pagecache_works(ops: OpVec, cache_fixup_threshold: u8) -> bool {
    use self::Op::*;
    let config = Config::default()
        .io_buf_size(1000)
        .flush_every_ms(Some(1))
        .cache_bits(0)
        .cache_capacity(40)
        .cache_fixup_threshold(cache_fixup_threshold as usize);

    let mut pc = PageCache::new(TestMaterializer, config.clone());
    pc.recover();

    let mut reference: HashMap<PageID, Vec<usize>> = HashMap::new();

    let bad_addr = 1 << std::mem::align_of::<Vec<usize>>().trailing_zeros();
    let bad_ptr = unsafe { Ptr::from_raw(bad_addr as *mut _) };

    for op in ops.ops.into_iter() {
        match op {
            Set(pid, c) => {
                let present = reference.contains_key(&pid);

                if present {
                    let ref mut existing = reference.get_mut(&pid).unwrap();
                    let old_key = if existing.is_empty() {
                        Ptr::null().into()
                    } else {
                        let (_, old_key) = pc.get(pid).unwrap();
                        old_key
                    };

                    pc.set(pid, old_key.clone(), vec![c]).unwrap();
                    existing.clear();
                    existing.push(c);
                } else {
                    let res = pc.set(pid, bad_ptr.into(), vec![c]);
                    assert_eq!(res, Err(None));
                }
            }
            Merge(pid, c) => {
                let present = reference.contains_key(&pid);

                if present {
                    let ref mut existing = reference.get_mut(&pid).unwrap();
                    let old_key = if existing.is_empty() {
                        Ptr::null().into()
                    } else {
                        let (_, old_key) = pc.get(pid).unwrap();
                        old_key
                    };

                    pc.merge(pid, old_key.clone(), vec![c]).unwrap();
                    existing.push(c);
                } else {
                    let res = pc.merge(pid, bad_ptr.into(), vec![c]);
                    assert_eq!(res, Err(None));
                }
            }
            Get(pid) => {
                let r = reference.get(&pid).cloned();
                let a = pc.get(pid).map(|(a, _)| a);
                if let Some(ref s) = r {
                    if s.is_empty() {
                        assert_eq!(a, None);
                    } else {
                        assert_eq!(r, a);
                        let values = a.unwrap();
                        values.iter().fold(0, |acc, cur| {
                            if *cur <= acc {
                                panic!("out of order page fragments in page!");
                            }
                            *cur
                        });
                    }
                } else {
                    assert_eq!(None, a);
                }
            }
            Free(pid) => {
                pc.free(pid);
                reference.remove(&pid);
            }
            Allocate => {
                let (pid, _key) = pc.allocate();
                reference.insert(pid, vec![]);
            }
            Restart => {
                drop(pc);
                pc = PageCache::new(TestMaterializer, config.clone());
                pc.recover();
            }
        }
    }

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
fn pagecache_bug_1() {
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
fn pagecache_bug_2() {
    // postmortem: historically needed to "seed" a page by writing
    // a compacting base to it. changed the snapshot and page-in code
    // to allow a merge being the first update to hit a page.
    // portmortem 2:
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![Allocate, Restart, Merge(0, 1)],
        },
        0,
    );
}

#[test]
fn pagecache_bug_3() {
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
fn pagecache_bug_4() {
    // postmortem: previously this caused a panic, we shouldn't break
    // when the user asks us to mutate non-existant pages!
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![Merge(98, 1)],
        },
        0,
    );
}

#[test]
fn pagecache_bug_5() {
    // postmortem: this was a mismatch in semantics in the test harness itself
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![Merge(132, 1), Set(132, 1)],
        },
        0,
    );
}

#[test]
fn pagecache_bug_6() {
    // postmortem: the test wasn't actually recording changes to the reference page...
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![Allocate, Set(0, 53), Set(0, 54)],
        },
        0,
    );
}

#[test]
fn pagecache_bug_7() {
    // postmortem: the test wasn't correctly recording the replacement effect of a set
    // in the reference page
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![Allocate, Merge(0, 201), Set(0, 208), Get(0)],
        },
        0,
    );
}

#[test]
fn pagecache_bug_8() {
    // postmortem: page_in messed up the stack ordering when storing a merged stack
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![
                Allocate,
                Set(0, 188),
                Allocate,
                Merge(1, 196),
                Merge(1, 198),
                Merge(1, 200),
                Merge(0, 201),
                Get(1),
            ],
        },
        0,
    );
}

#[test]
fn pagecache_bug_9() {
    // postmortem: this started failing in the giant refactor for log structured storage,
    // and was possibly fixed by properly handling intervals in mark_interval
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![
                Allocate,
                Allocate,
                Merge(1, 208),
                Merge(1, 211),
                Merge(0, 212),
                Set(0, 213),
                Set(1, 214),
            ],
        },
        0,
    );
}

fn _pagecache_bug_() {
    // postmortem: TEMPLATE
    // portmortem 2: ...
    // use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![],
        },
        0,
    );
}

extern crate sled;
extern crate crossbeam_epoch as epoch;
extern crate rand;
extern crate quickcheck;

use std::collections::HashMap;
use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};

use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};
use epoch::{Shared, pin};

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
    let conf = Config::default()
        .cache_capacity(40)
        .cache_bits(0)
        .flush_every_ms(None)
        .snapshot_after_ops(1_000_000)
        .io_buf_size(5000);

    let mut pc = PageCache::new(TestMaterializer, conf.clone());
    pc.recover();

    let guard = pin();
    let mut keys = HashMap::new();

    for _ in 0..2 {
        let (id, key) = pc.allocate(&guard);
        let key = pc.replace(id, key, vec![0], &guard).unwrap();
        keys.insert(id, key);
    }

    for i in 0..1000 {
        let id = i as usize % 2;
        let (_, key) = pc.get(id, &guard).unwrap();
        let key = pc.link(id, key, vec![i], &guard).unwrap();
        keys.insert(id, key);
    }
}

#[test]
fn pagecache_strange_crash_1() {
    let conf = Config::default()
        .cache_capacity(40)
        .cache_bits(0)
        .flush_every_ms(None)
        .snapshot_after_ops(1_000_000)
        .io_buf_size(5000);

    {
        let mut pc = PageCache::new(TestMaterializer, conf.clone());
        pc.recover();

        let guard = pin();
        let mut keys = HashMap::new();
        for _ in 0..2 {
            let (id, key) = pc.allocate(&guard);
            let key = pc.replace(id, key, vec![0], &guard).unwrap();
            keys.insert(id, key);
        }

        for i in 0..1000 {
            let id = i as usize % 2;
            let (_, key) = pc.get(id, &guard).unwrap();
            let key = pc.link(id, key, vec![i], &guard).unwrap();
            keys.insert(id, key);
        }
    }
    println!("!!!!!!!!!!!!!!!!!!!!! recovering !!!!!!!!!!!!!!!!!!!!!!");
    let mut pc = PageCache::new(TestMaterializer, conf.clone());
    pc.recover();
    // TODO test no eaten lsn's on recovery
    // TODO test that we don't skip multiple segments ahead on recovery (confusing Lsn & Lid)
}

#[test]
fn pagecache_strange_crash_2() {
    for x in 0..10 {
        let guard = pin();
        let conf = Config::default()
            .cache_capacity(40)
            .cache_bits(0)
            .flush_every_ms(None)
            .snapshot_after_ops(1_000_000)
            .io_buf_size(5000);

        println!("!!!!!!!!!!!!!!!!!!!!! {} !!!!!!!!!!!!!!!!!!!!!!", x);
        let mut pc = PageCache::new(TestMaterializer, conf.clone());
        pc.recover();

        let mut keys = HashMap::new();
        for _ in 0..2 {
            let (id, key) = pc.allocate(&guard);
            let key = pc.replace(id, key, vec![0], &guard).unwrap();
            keys.insert(id, key);
        }

        for i in 0..1000 {
            let id = i as usize % 2;
            println!("------ beginning op on pid {} ------", id);
            let (_, key) = pc.get(id, &guard).unwrap();
            println!("got key {:?} for pid {}", key, id);
            assert!(!key.is_null());
            let key_res = pc.link(id, key, vec![i], &guard);
            if key_res.is_err() {
                println!("failed linking pid {}", id);
            }
            let key = key_res.unwrap();
            keys.insert(id, key);
        }
    }
}

#[test]
fn basic_pagecache_recovery() {
    let conf = Config::default().flush_every_ms(None).io_buf_size(200);

    let mut pc = PageCache::new(TestMaterializer, conf.clone());

    let guard = pin();
    pc.recover();
    let (id, key) = pc.allocate(&guard);
    let key = pc.replace(id, key, vec![1], &guard).unwrap();
    let key = pc.link(id, key, vec![2], &guard).unwrap();
    let _key = pc.link(id, key, vec![3], &guard).unwrap();
    let (consolidated, _) = pc.get(id, &guard).unwrap();
    assert_eq!(consolidated, vec![1, 2, 3]);
    drop(pc);

    let mut pc2 = PageCache::new(TestMaterializer, conf.clone());
    pc2.recover();
    let (consolidated2, key) = pc2.get(id, &guard).unwrap();
    assert_eq!(consolidated, consolidated2);

    pc2.link(id, key, vec![4], &guard).unwrap();
    drop(pc2);

    let mut pc3 = PageCache::new(TestMaterializer, conf.clone());
    pc3.recover();
    let (consolidated3, _key) = pc3.get(id, &guard).unwrap();
    assert_eq!(consolidated3, vec![1, 2, 3, 4]);
    pc3.free(id);
    drop(pc3);

    let mut pc4 = PageCache::new(TestMaterializer, conf.clone());
    pc4.recover();
    let res = pc4.get(id, &guard);
    assert!(res.is_none());
}

#[derive(Debug, Clone)]
enum Op {
    Replace(PageID, usize),
    Link(PageID, usize),
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
            0 => Op::Replace(pid, COUNTER.fetch_add(1, Ordering::Relaxed)),
            1 => Op::Link(pid, COUNTER.fetch_add(1, Ordering::Relaxed)),
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
                Op::Replace(ref mut pid, _) |
                Op::Link(ref mut pid, _) |
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
    let bad_ptr = Shared::from(bad_addr as *const _);

    for op in ops.ops.into_iter() {
        match op {
            Replace(pid, c) => {
                let present = reference.contains_key(&pid);

                if present {
                    let ref mut existing = reference.get_mut(&pid).unwrap();
                    {
                        let guard = pin();
                        let old_key = if existing.is_empty() {
                            Shared::null().into()
                        } else {
                            let (_, old_key) = pc.get(pid, &guard).unwrap();
                            old_key
                        };

                        pc.replace(pid, old_key.clone(), vec![c], &guard)
                            .unwrap();
                    }
                    existing.clear();
                    existing.push(c);
                } else {
                    let guard = pin();
                    let res =
                        pc.replace(pid, bad_ptr.into(), vec![c], &guard);
                    assert!(res.unwrap_err().is_none());
                }
            }
            Link(pid, c) => {
                let present = reference.contains_key(&pid);

                if present {
                    let ref mut existing = reference.get_mut(&pid).unwrap();
                    {
                        let guard = pin();
                        let old_key = if existing.is_empty() {
                            Shared::null().into()
                        } else {
                            let (_, old_key) = pc.get(pid, &guard).unwrap();
                            old_key
                        };

                        pc.link(pid, old_key.clone(), vec![c], &guard).unwrap();
                    }
                    existing.push(c);
                } else {
                    let guard = pin();
                    let res = pc.link(pid, bad_ptr, vec![c], &guard);
                    assert!(res.unwrap_err().is_none());
                }
            }
            Get(pid) => {
                let guard = pin();
                let r = reference.get(&pid).cloned();
                let a = pc.get(pid, &guard).map(|(a, _)| a);
                if let Some(ref s) = r {
                    if s.is_empty() {
                        assert_eq!(a, None);
                    } else {
                        assert_eq!(r, a);
                        let values = a.unwrap();
                        values.iter().fold(0, |acc, cur| {
                            if *cur <= acc {
                                panic!(
                                    "out of order page fragments in page!"
                                );
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
                let guard = pin();
                let (pid, _key) = pc.allocate(&guard);
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
    // to allow a link being the first update to hit a page.
    // portmortem 2:
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![Allocate, Restart, Link(0, 1)],
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
            ops: vec![Link(98, 1)],
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
            ops: vec![Link(132, 1), Replace(132, 1)],
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
            ops: vec![Allocate, Replace(0, 53), Replace(0, 54)],
        },
        0,
    );
}

#[test]
fn pagecache_bug_7() {
    // postmortem: the test wasn't correctly recording the replacement effect of a replace
    // in the reference page
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![Allocate, Link(0, 201), Replace(0, 208), Get(0)],
        },
        0,
    );
}

#[test]
fn pagecache_bug_8() {
    // postmortem: page_in messed up the stack ordering when storing a linked stack
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![
                Allocate,
                Replace(0, 188),
                Allocate,
                Link(1, 196),
                Link(1, 198),
                Link(1, 200),
                Link(0, 201),
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
                Link(1, 208),
                Link(1, 211),
                Link(0, 212),
                Replace(0, 213),
                Replace(1, 214),
            ],
        },
        0,
    );
}

#[test]
fn pagecache_bug_10() {
    // postmortem: the segment was marked free before it
    // was actually full, because the pids inside were
    // rewritten.
    use Op::*;
    prop_pagecache_works(
        OpVec {
            ops: vec![
                Allocate,
                Replace(0, 425),
                Free(0),
                Allocate,
                Link(1, 427),
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

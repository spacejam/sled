extern crate sled;
extern crate crossbeam_epoch as epoch;
extern crate rand;
extern crate quickcheck;

use std::collections::HashMap;
use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};
use std::sync::Arc;

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
        .io_buf_size(20000)
        .build();

    let pc = PageCache::start(TestMaterializer, conf.clone());

    let guard = pin();
    let mut keys = HashMap::new();

    for _ in 0..2 {
        let id = pc.allocate(&guard);
        let key = pc.replace(id, Shared::null(), vec![0], &guard).unwrap();
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
        .io_buf_size(20000)
        .build();

    {
        let pc = PageCache::start(TestMaterializer, conf.clone());

        let guard = pin();
        let mut keys = HashMap::new();
        for _ in 0..2 {
            let id = pc.allocate(&guard);
            let key = pc.replace(id, Shared::null(), vec![0], &guard).unwrap();
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
    let _pc = PageCache::start(TestMaterializer, conf.clone());
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
            .io_buf_size(20000)
            .build();

        println!("!!!!!!!!!!!!!!!!!!!!! {} !!!!!!!!!!!!!!!!!!!!!!", x);
        conf.verify_snapshot(Arc::new(TestMaterializer));

        let pc = PageCache::start(TestMaterializer, conf.clone());
        pc.recovered_state();

        let mut keys = HashMap::new();
        for _ in 0..2 {
            let id = pc.allocate(&guard);
            let key = pc.replace(id, Shared::null(), vec![0], &guard).unwrap();
            keys.insert(id, key);
        }

        for i in 0..1000 {
            let id = i as usize % 2;
            // println!("------ beginning op on pid {} ------", id);
            let (_, key) = pc.get(id, &guard).unwrap();
            // println!("got key {:?} for pid {}", key, id);
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
    let conf = Config::default()
        .flush_every_ms(None)
        .io_buf_size(1000)
        .build();

    let pc = PageCache::start(TestMaterializer, conf.clone());

    let guard = pin();
    let id = pc.allocate(&guard);
    let key = pc.replace(id, Shared::null(), vec![1], &guard).unwrap();
    let key = pc.link(id, key, vec![2], &guard).unwrap();
    let _key = pc.link(id, key, vec![3], &guard).unwrap();
    let (consolidated, _) = pc.get(id, &guard).unwrap();
    assert_eq!(consolidated, vec![1, 2, 3]);
    drop(pc);

    let pc2 = PageCache::start(TestMaterializer, conf.clone());
    let (consolidated2, key) = pc2.get(id, &guard).unwrap();
    assert_eq!(consolidated, consolidated2);

    pc2.link(id, key, vec![4], &guard).unwrap();
    drop(pc2);

    let pc3 = PageCache::start(TestMaterializer, conf.clone());
    let (consolidated3, _key) = pc3.get(id, &guard).unwrap();
    assert_eq!(consolidated3, vec![1, 2, 3, 4]);
    pc3.free(id);
    drop(pc3);

    let pc4 = PageCache::start(TestMaterializer, conf.clone());
    let res = pc4.get(id, &guard);
    assert!(res.is_free());
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
        for _ in 0..g.gen_range(1, 100) {
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

enum P {
    Free,
    Allocated,
    Unallocated,
    Present(Vec<usize>),
}

impl P {
    fn is_free(&self) -> bool {
        match *self {
            P::Free => true,
            _ => false,
        }
    }

    fn is_unallocated(&self) -> bool {
        match *self {
            P::Unallocated => true,
            _ => false,
        }
    }

    fn is_allocated(&self) -> bool {
        match *self {
            P::Allocated => true,
            _ => false,
        }
    }

    fn is_present(&self) -> bool {
        match *self {
            P::Present(_) => true,
            _ => false,
        }
    }

    fn entries(&self) -> &Vec<usize> {
        match *self {
            P::Present(ref entries) => entries,
            _ => panic!("entries called on non-present page"),
        }
    }
}

fn prop_pagecache_works(ops: OpVec) -> bool {
    use self::Op::*;
    let config = Config::default()
        .io_buf_size(1000)
        .flush_every_ms(None)
        // FIXME uncomment .flush_every_ms(Some(1))
        .cache_bits(0)
        .cache_capacity(40)
        .cache_fixup_threshold(1)
        .build();

    let mut pc = PageCache::start(TestMaterializer, config.clone());

    let mut reference: HashMap<PageID, P> = HashMap::new();

    // TODO use returned pointers, cleared on restart, with caching set to
    // a large amount, to test linkage.
    // println!("{:?}", ops);
    for op in ops.ops.into_iter() {
        let guard = pin();
        match op {
            Replace(pid, c) => {
                println!("replacing pid {}", pid);
                let get = pc.get(pid, &guard);
                let ref_get = reference.entry(pid).or_insert(P::Unallocated);

                match ref_get {
                    &mut P::Allocated => {
                        pc.replace(pid, Shared::null(), vec![c], &guard)
                            .unwrap();
                        *ref_get = P::Present(vec![c]);
                    }
                    &mut P::Present(ref mut existing) => {
                        let (_, old_key) = get.unwrap();
                        pc.replace(pid, old_key.clone(), vec![c], &guard)
                            .unwrap();
                        existing.clear();
                        existing.push(c);
                    }
                    &mut P::Free => {
                        assert_eq!(get, sled::PageGet::Free);
                    }
                    &mut P::Unallocated => {
                        assert_eq!(get, sled::PageGet::Unallocated);
                    }
                }
            }
            Link(pid, c) => {
                let get = pc.get(pid, &guard);
                let ref_get = reference.entry(pid).or_insert(P::Unallocated);

                match ref_get {
                    &mut P::Allocated => {
                        pc.link(pid, Shared::null(), vec![c], &guard).unwrap();
                        *ref_get = P::Present(vec![c]);
                    }
                    &mut P::Present(ref mut existing) => {
                        let (_, old_key) = get.unwrap();
                        pc.link(pid, old_key.clone(), vec![c], &guard).unwrap();
                        existing.push(c);
                    }
                    &mut P::Free => {
                        assert_eq!(get, sled::PageGet::Free);
                    }
                    &mut P::Unallocated => {
                        assert_eq!(get, sled::PageGet::Unallocated);
                    }
                }
            }
            Get(pid) => {
                let get = pc.get(pid, &guard);

                match reference.get(&pid) {
                    Some(&P::Allocated) => {
                        assert!(get.is_allocated());
                    }
                    Some(&P::Present(ref existing)) => {
                        let (val, _ptr) = get.unwrap();
                        assert_eq!(existing, &val);
                        val.iter().fold(0, |acc, cur| {
                            if *cur <= acc {
                                panic!("out of order page fragments in page!");
                            }
                            *cur
                        });
                    }
                    Some(&P::Free) => {
                        assert!(get.is_free());
                    }
                    Some(&P::Unallocated) |
                    None => {
                        assert_eq!(get, sled::PageGet::Unallocated);
                    }
                }
            }
            Free(pid) => {
                pc.free(pid);
                let get = pc.get(pid, &guard);

                match reference.get(&pid) {
                    Some(&P::Allocated) |
                    Some(&P::Present(_)) |
                    Some(&P::Free) => {
                        reference.insert(pid, P::Free);
                        assert!(get.is_free())
                    }
                    Some(&P::Unallocated) |
                    None => assert!(get.is_unallocated()),
                }
            }
            Allocate => {
                let pid = pc.allocate(&guard);
                println!("allocated pid {}", pid);
                reference.insert(pid, P::Allocated);
                let get = pc.get(pid, &guard);
                assert!(get.is_allocated());
            }
            Restart => {
                drop(pc);

                config.verify_snapshot(Arc::new(TestMaterializer));

                pc = PageCache::start(TestMaterializer, config.clone());
            }
        }
    }

    true
}

#[test]
fn quickcheck_pagecache_works() {
    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 1))
        .tests(1000)
        .max_tests(1000000)
        .quickcheck(prop_pagecache_works as fn(OpVec) -> bool);
}

#[test]
fn pagecache_bug_01() {
    // postmortem: this happened because `PageCache::page_in` assumed
    // at least one update had been stored for a retrieved page.
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![Allocate, Get(0)],
    });
}

#[test]
fn pagecache_bug_02() {
    // postmortem: historically needed to "seed" a page by writing
    // a compacting base to it. changed the snapshot and page-in code
    // to allow a link being the first update to hit a page.
    // portmortem 2:
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![Allocate, Restart, Link(0, 1)],
    });
}

#[test]
fn pagecache_bug_3() {
    // postmortem: this was a mismatch in semantics in the test harness itself
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![Allocate, Get(0)],
    });
}

#[test]
fn pagecache_bug_4() {
    // postmortem: previously this caused a panic, we shouldn't break
    // when the user asks us to mutate non-existant pages!
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![Link(98, 1)],
    });
}

#[test]
fn pagecache_bug_5() {
    // postmortem: this was a mismatch in semantics in the test harness itself
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![Link(132, 1), Replace(132, 1)],
    });
}

#[test]
fn pagecache_bug_6() {
    // postmortem: the test wasn't actually recording changes to the reference page...
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![Allocate, Replace(0, 53), Replace(0, 54)],
    });
}

#[test]
fn pagecache_bug_7() {
    // postmortem: the test wasn't correctly recording the replacement effect of a replace
    // in the reference page
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![Allocate, Link(0, 201), Replace(0, 208), Get(0)],
    });
}

#[test]
fn pagecache_bug_8() {
    // postmortem: page_in messed up the stack ordering when storing a linked stack
    use Op::*;
    prop_pagecache_works(OpVec {
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
    });
}

#[test]
fn pagecache_bug_9() {
    // postmortem: this started failing in the giant refactor for log structured storage,
    // and was possibly fixed by properly handling intervals in mark_interval
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![
            Allocate,
            Allocate,
            Link(1, 208),
            Link(1, 211),
            Link(0, 212),
            Replace(0, 213),
            Replace(1, 214),
        ],
    });
}

#[test]
fn pagecache_bug_10() {
    // postmortem: the segment was marked free before it
    // was actually full, because the pids inside were
    // rewritten.
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![Allocate, Replace(0, 425), Free(0), Allocate, Link(1, 427)],
    });
}

#[test]
fn pagecache_bug_11() {
    // postmortem: failed to completely back-out an experiment
    // to tombstone allocations, which removed a check.
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![Free(0)],
    });
}

#[test]
fn pagecache_bug_12() {
    // postmortem: refactor to add Free tombstones changed
    // the model.
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![Allocate, Free(0), Replace(0, 66)],
    });
}

#[test]
fn pagecache_bug_13() {
    // postmortem: Free tombstones mean that the page table may
    // already have an entry for a newly-allocated page.
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![Allocate, Free(0), Free(0), Restart, Allocate, Allocate],
    });
}

#[test]
fn pagecache_bug_14() {
    // postmortem: Free tombstones are a little weird with the Lru.
    // Make sure they don't get paged out.
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![
            Allocate,
            Allocate,
            Replace(0, 955),
            Replace(1, 956),
            Restart,
            Replace(1, 962),
            Free(1),
            Get(0),
        ],
    });
}

#[test]
fn pagecache_bug_15() {
    // postmortem: non-idempotent PageCache::free.
    // fixed by deduplicating the free list on recovery.
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![
            Allocate,
            Free(0),
            Free(0),
            Restart,
            Allocate,
            Replace(0, 8485),
            Allocate,
            Restart,
            Get(0),
        ],
    });
}

#[test]
fn pagecache_bug_16() {
    // postmortem: non-idempotent PageCache::free.
    // did not check if a Free tombstone was present,
    // just if the stack was present.
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![
            Allocate,
            Free(0),
            Free(0),
            Free(0),
            Allocate,
            Link(0, 5622),
            Allocate,
            Restart,
            Get(0),
        ],
    });
}

#[test]
fn pagecache_bug_17() {
    // postmortem:
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Link(0, 205),
            Replace(0, 207),
            Link(1, 208),
            Link(2, 213),
            Replace(3, 215),
            Replace(2, 216),
            Replace(3, 218),
            Free(1),
            Link(4, 220),
            Restart,
            Allocate,
            Allocate,
            Replace(5, 221),
            Free(4),
            Free(3),
            Link(2, 222),
            Link(4, 223),
            Replace(1, 224),
            Link(0, 226),
            Restart,
            Allocate,
            Allocate,
            Link(0, 227),
            Link(0, 228),
            Replace(4, 229),
            Free(1),
            Free(0),
            Link(3, 230),
            Link(1, 231),
            Replace(2, 232),
            Replace(4, 233),
            Replace(2, 236),
            Allocate,
            Replace(3, 240),
            Link(3, 241),
            Replace(1, 242),
        ],
    });
}

#[test]
fn pagecache_bug_18() {
    // postmortem: added page replacement information to
    // the segment that a page was being added to.
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![Allocate, Link(0, 273), Free(0), Restart, Restart],
    });
}

#[test]
fn pagecache_bug_19() {
    // postmortem: was double-clearing removals when tracking
    // removed pages in segments, before doing the recovery
    // check to see if the snapshot already includes updates
    // for that lsn.
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Link(2, 147),
            Allocate,
            Allocate,
            Link(4, 152),
            Replace(4, 153),
            Link(4, 154),
            Link(3, 155),
            Replace(1, 156),
            Link(5, 157),
            Link(0, 158),
            Link(3, 159),
            Link(6, 160),
            Free(4),
            Free(6),
            Replace(3, 161),
            Replace(6, 162),
            Free(1),
            Free(2),
            Replace(0, 163),
            Link(5, 164),
            Replace(5, 165),
            Free(5),
            Free(0),
            Link(2, 166),
            Link(2, 167),
            Link(2, 168),
            Replace(3, 169),
            Restart,
            Restart,
        ],
    });
}

#[test]
fn pagecache_bug_20() {
    // postmortem: failed to handle Unallocated nodes properly
    // in refactored test model.
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![Free(0), Replace(0, 17)],
    });
}

#[test]
fn pagecache_bug_21() {
    // postmortem: test model marked unused pids as Unallocated
    // instead of Free during recovery.
    use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![Allocate, Free(0), Restart, Allocate, Restart, Get(0)],
    });
}

fn _pagecache_bug_() {
    // postmortem: TEMPLATE
    // portmortem 2: ...
    // use Op::*;
    prop_pagecache_works(OpVec {
        ops: vec![],
    });
}

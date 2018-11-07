extern crate pagecache;
extern crate quickcheck;
extern crate rand;
extern crate tests;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT},
        Arc,
    },
    thread,
};

use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};
use rand::Rng;

use pagecache::{
    pin, ConfigBuilder, Materializer, PageCache, PageGet, PagePtr,
};

type PageId = usize;

#[derive(Clone)]
pub struct TestMaterializer;

impl Materializer for TestMaterializer {
    type PageFrag = Vec<usize>;
    type Recovery = ();

    fn new(
        _config: pagecache::Config,
        _recovery: &Option<()>,
    ) -> TestMaterializer {
        TestMaterializer
    }

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

    fn size_in_bytes(&self, frag: &Vec<usize>) -> usize {
        std::mem::size_of::<Vec<usize>>() + frag.len()
    }
}

#[test]
fn pagecache_caching() {
    let config = ConfigBuilder::new()
        .temporary(true)
        .cache_capacity(40)
        .cache_bits(0)
        .flush_every_ms(None)
        .snapshot_after_ops(1_000_000)
        .io_buf_size(20000)
        .build();

    let pc: PageCache<TestMaterializer, _, _> =
        PageCache::start(config.clone()).unwrap();

    let guard = pin();
    let mut keys = HashMap::new();

    for _ in 0..2 {
        let id = pc.allocate(&guard).unwrap();
        let key = pc
            .replace(id, PagePtr::allocated(), vec![0], &guard)
            .unwrap();
        keys.insert(id, key);
    }

    for i in 0..1000 {
        let id = i as usize % 2;
        let (_, key) = pc.get(id, &guard).unwrap().unwrap();
        let key = pc.link(id, key, vec![i], &guard).unwrap();
        keys.insert(id, key);
    }
}

#[test]
fn parallel_pagecache() {
    const N_THREADS: usize = 10;
    const N_PER_THREAD: usize = 1000;

    let config = ConfigBuilder::new()
        .temporary(true)
        .io_bufs(3)
        .blink_node_split_size(500)
        .flush_every_ms(None)
        .snapshot_after_ops(100_000_000)
        .io_buf_size(8_000)
        .page_consolidation_threshold(3)
        .print_profile_on_drop(true)
        .build();

    macro_rules! par {
        ($t:ident, $f:expr) => {
            let mut threads = vec![];
            for tn in 0..N_THREADS {
                let tree = $t.clone();
                let thread = thread::Builder::new()
                    .name(format!("t({})", tn))
                    .spawn(move || {
                        for i in (tn * N_PER_THREAD)
                            ..((tn + 1) * N_PER_THREAD)
                        {
                            $f(&*tree, i);
                        }
                    }).expect("should be able to spawn thread");
                threads.push(thread);
            }
            while let Some(thread) = threads.pop() {
                if let Err(e) = thread.join() {
                    panic!("thread failure: {:?}", e);
                }
            }
        };
    }

    println!("========== sets ==========");
    let p: Arc<PageCache<TestMaterializer, Vec<usize>, ()>> =
        Arc::new(PageCache::start(config.clone()).unwrap());

    par!{p, |pc: &PageCache<_, _, _>, _i: usize| {
        let guard = pin();
        let id = pc.allocate(&guard).unwrap();
        let _key = pc
            .replace(id, PagePtr::allocated(), vec![id], &guard)
            .unwrap();
        let (ptr, _key): (&Vec<usize>, _) =
                           pc.get(id, &guard)
                             .expect("we should read what we just wrote")
                             .unwrap();
        assert_eq!(ptr[0], id);
    }};

    drop(p);

    println!("========== gets ==========");
    let p: Arc<PageCache<TestMaterializer, Vec<usize>, ()>> =
        Arc::new(PageCache::start(config.clone()).unwrap());

    par!{p, |pc: &PageCache<_, _, _>, i: usize| {
        let guard = pin();
        let (ptr, _key): (&Vec<usize>, _) =
                           pc.get(i, &guard)
                             .expect("we should read what we just wrote")
                             .unwrap();
        assert_eq!(*ptr, vec![i]);
    }};

    drop(p);

    println!("========== links ==========");
    let p: Arc<PageCache<TestMaterializer, Vec<usize>, ()>> =
        Arc::new(PageCache::start(config.clone()).unwrap());

    par!{p, |pc: &PageCache<_, _, _>, i: usize| {
        let mut success = 0;

        while success <= 10 {
            let guard = pin();
            let (_ptr, key) = pc.get(i, &guard)
                .expect("we should read what we just wrote")
                .unwrap();
            match pc.link(i, key, vec![success], &guard) {
                Err(_) => {},
                Ok(_) => {
                    success += 1;
                }
            }
        }
        let guard = pin();
        let (ptr, _key): (&Vec<usize>, _) =
                           pc.get(i, &guard)
                             .expect("we should read what we just wrote")
                             .unwrap();
        assert_eq!(*ptr, vec![i, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }};

    drop(p);

    println!("========== gets ==========");
    let p: Arc<PageCache<TestMaterializer, Vec<usize>, ()>> =
        Arc::new(PageCache::start(config.clone()).unwrap());

    par!{p, |pc: &PageCache<_, _, _>, i: usize| {
        let guard = pin();
        let (ptr, _key): (&Vec<usize>, _) =
                           pc.get(i, &guard)
                             .expect("we should read what we just wrote")
                             .unwrap();
        assert_eq!(*ptr, vec![i, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }};
}

#[test]
fn pagecache_strange_crash_1() {
    let config = ConfigBuilder::new()
        .temporary(true)
        .cache_capacity(40)
        .cache_bits(0)
        .flush_every_ms(None)
        .snapshot_after_ops(1_000_000)
        .io_buf_size(20000)
        .build();

    {
        let pc: PageCache<TestMaterializer, _, _> =
            PageCache::start(config.clone()).unwrap();

        let guard = pin();
        let mut keys = HashMap::new();
        for _ in 0..2 {
            let id = pc.allocate(&guard).unwrap();
            let key = pc
                .replace(id, PagePtr::allocated(), vec![0], &guard)
                .unwrap();
            keys.insert(id, key);
        }

        for i in 0..1000 {
            let id = i as usize % 2;
            let (_, key) = pc.get(id, &guard).unwrap().unwrap();
            let key = pc.link(id, key, vec![i], &guard).unwrap();
            keys.insert(id, key);
        }
    }
    let _pc: PageCache<TestMaterializer, _, _> =
        PageCache::start(config.clone()).unwrap();
    // TODO test no eaten lsn's on recovery
    // TODO test that we don't skip multiple segments ahead on recovery (confusing Lsn & Lid)
}

#[test]
fn pagecache_strange_crash_2() {
    for _ in 0..10 {
        let guard = pin();
        let config = ConfigBuilder::new()
            .temporary(true)
            .cache_capacity(40)
            .cache_bits(0)
            .flush_every_ms(None)
            .snapshot_after_ops(1_000_000)
            .io_buf_size(20000)
            .build();

        config.verify_snapshot::<TestMaterializer, _, _>().unwrap();

        let pc: PageCache<TestMaterializer, _, _> =
            PageCache::start(config.clone()).unwrap();

        let mut keys = HashMap::new();
        for _ in 0..2 {
            let id = pc.allocate(&guard).unwrap();
            let key = pc
                .replace(id, PagePtr::allocated(), vec![0], &guard)
                .unwrap();
            keys.insert(id, key);
        }

        for i in 0..1000 {
            let id = i as usize % 2;
            let (_, key) = pc.get(id, &guard).unwrap().unwrap();
            assert!(!key.is_allocated());
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
    let config = ConfigBuilder::new()
        .temporary(true)
        .flush_every_ms(None)
        .io_buf_size(1000)
        .build();

    let pc: PageCache<TestMaterializer, _, _> =
        PageCache::start(config.clone()).unwrap();

    let guard = pin();
    let id = pc.allocate(&guard).unwrap();
    let key = pc
        .replace(id, PagePtr::allocated(), vec![1], &guard)
        .unwrap();
    let key = pc.link(id, key, vec![2], &guard).unwrap();
    let _key = pc.link(id, key, vec![3], &guard).unwrap();
    let (cv1_ref, _key) = pc.get(id, &guard).unwrap().unwrap();
    let cv1 = cv1_ref.clone();
    assert_eq!(cv1, vec![1, 2, 3]);
    drop(pc);

    let pc2: PageCache<TestMaterializer, _, _> =
        PageCache::start(config.clone()).unwrap();
    let (cv2_ref, consolidated2) =
        pc2.get(id, &guard).unwrap().unwrap();
    let cv2 = cv2_ref.clone();
    assert_eq!(cv1, cv2);

    pc2.link(id, consolidated2, vec![4], &guard).unwrap();
    drop(pc2);

    let pc3: PageCache<TestMaterializer, _, _> =
        PageCache::start(config.clone()).unwrap();
    let (cv3_ref, consolidated3) =
        pc3.get(id, &guard).unwrap().unwrap();
    let cv3 = cv3_ref.clone();
    assert_eq!(cv3, vec![1, 2, 3, 4]);
    pc3.free(id, consolidated3, &guard).unwrap();
    drop(pc3);

    let pc4: PageCache<TestMaterializer, _, _> =
        PageCache::start(config.clone()).unwrap();
    let res = pc4.get(id, &guard).unwrap();
    assert!(res.is_free());
}

#[derive(Debug, Clone)]
enum Op {
    Replace(PageId, usize),
    Link(PageId, usize),
    Get(PageId),
    Free(PageId),
    Allocate,
    Restart,
}

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        if g.gen_bool(1. / 10.) {
            return Op::Restart;
        }

        let choice = g.gen_range(0, 5);

        static COUNTER: AtomicUsize = ATOMIC_USIZE_INIT;
        COUNTER.compare_and_swap(0, 1, Ordering::SeqCst);

        let pid = (g.gen::<u8>() % 8) as PageId;

        match choice {
            0 => Op::Replace(
                pid,
                COUNTER.fetch_add(1, Ordering::Relaxed),
            ),
            1 => {
                Op::Link(pid, COUNTER.fetch_add(1, Ordering::Relaxed))
            }
            2 => Op::Get(pid),
            3 => Op::Free(pid),
            4 => Op::Allocate,
            _ => panic!("impossible choice"),
        }
    }

    fn shrink(&self) -> Box<Iterator<Item = Op>> {
        let mut shrunk = false;
        let mut op = self.clone();
        match op {
            Op::Replace(ref mut pid, _)
            | Op::Link(ref mut pid, _)
            | Op::Get(ref mut pid)
            | Op::Free(ref mut pid)
                if *pid > 0 =>
            {
                *pid -= 1;
                shrunk = true;
            }
            _ => {}
        }
        if shrunk {
            Box::new(vec![op.clone()].into_iter())
        } else {
            Box::new(vec![].into_iter())
        }
    }
}

#[derive(Debug)]
enum P {
    Free,
    Allocated,
    Unallocated,
    Present(Vec<usize>),
}

fn prop_pagecache_works(ops: Vec<Op>, flusher: bool) -> bool {
    // tests::setup_logger();
    use self::Op::*;
    let config = ConfigBuilder::new()
        .temporary(true)
        .io_buf_size(1000)
        .flush_every_ms(if flusher { Some(1) } else { None })
        .cache_capacity(40)
        .cache_bits(0)
        .build();

    let mut pc: PageCache<TestMaterializer, _, _> =
        PageCache::start(config.clone()).unwrap();

    let mut reference: HashMap<PageId, P> = HashMap::new();

    // TODO use returned pointers, cleared on restart, with caching set to
    // a large amount, to test linkage.
    for op in ops.into_iter() {
        let guard = pin();
        match op {
            Replace(pid, c) => {
                let get = pc.get(pid, &guard).unwrap();
                let ref_get =
                    reference.entry(pid).or_insert(P::Unallocated);

                match ref_get {
                    &mut P::Allocated => {
                        assert_eq!(get, PageGet::Allocated);
                        pc.replace(
                            pid,
                            PagePtr::allocated(),
                            vec![c],
                            &guard,
                        ).unwrap();
                        *ref_get = P::Present(vec![c]);
                    }
                    &mut P::Present(ref mut existing) => {
                        let (v, old_key) = get.unwrap();
                        assert_eq!(v, existing);
                        pc.replace(
                            pid,
                            old_key.clone(),
                            vec![c],
                            &guard,
                        ).unwrap();
                        existing.clear();
                        existing.push(c);
                    }
                    &mut P::Free => {
                        assert!(get.is_free());
                    }
                    &mut P::Unallocated => {
                        assert_eq!(get, PageGet::Unallocated);
                    }
                }
            }
            Link(pid, c) => {
                let get = pc.get(pid, &guard).unwrap();
                let ref_get =
                    reference.entry(pid).or_insert(P::Unallocated);

                match ref_get {
                    &mut P::Allocated => {
                        pc.link(
                            pid,
                            PagePtr::allocated(),
                            vec![c],
                            &guard,
                        ).unwrap();
                        *ref_get = P::Present(vec![c]);
                    }
                    &mut P::Present(ref mut existing) => {
                        let (_, old_key) = get.unwrap();
                        pc.link(
                            pid,
                            old_key.clone(),
                            vec![c],
                            &guard,
                        ).unwrap();
                        existing.push(c);
                    }
                    &mut P::Free => {
                        assert!(get.is_free());
                    }
                    &mut P::Unallocated => {
                        assert_eq!(get, PageGet::Unallocated);
                    }
                }
            }
            Get(pid) => {
                let get = pc.get(pid, &guard).unwrap();

                match reference.get(&pid) {
                    Some(&P::Allocated) => {
                        assert!(get.is_allocated());
                    }
                    Some(&P::Present(ref existing)) => {
                        let (val, _cache_entry) = get.unwrap();

                        assert_eq!(existing, val);
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
                    Some(&P::Unallocated) | None => {
                        assert_eq!(get, PageGet::Unallocated);
                    }
                }
            }
            Free(pid) => {
                let pre_get = pc.get(pid, &guard).unwrap();

                match pre_get {
                    PageGet::Materialized(_, ptr) => {
                        pc.free(pid, ptr, &guard).unwrap()
                    }
                    PageGet::Allocated => pc
                        .free(pid, PagePtr::allocated(), &guard)
                        .unwrap(),
                    _ => {}
                }

                let get = pc.get(pid, &guard).unwrap();

                match reference.get(&pid) {
                    Some(&P::Allocated) | Some(&P::Present(_))
                    | Some(&P::Free) => {
                        reference.insert(pid, P::Free);
                        assert!(get.is_free())
                    }
                    Some(&P::Unallocated) | None => {
                        assert!(get.is_unallocated())
                    }
                }
            }
            Allocate => {
                let pid = pc.allocate(&guard).unwrap();
                reference.insert(pid, P::Allocated);
                let get = pc.get(pid, &guard);
                assert!(get.unwrap().is_allocated());
            }
            Restart => {
                drop(pc);

                config
                    .verify_snapshot::<TestMaterializer, _, _>()
                    .unwrap();

                pc = PageCache::start(config.clone()).unwrap();
            }
        }
        guard.flush();
        drop(guard);
    }

    true
}

#[test]
#[cfg(not(target_os = "fuchsia"))]
#[ignore]
fn quickcheck_pagecache_works() {
    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 100))
        .tests(1000)
        .max_tests(1000000)
        .quickcheck(
            prop_pagecache_works as fn(Vec<Op>, bool) -> bool,
        );
}

#[test]
fn pagecache_bug_01() {
    // postmortem: this happened because `PageCache::page_in` assumed
    // at least one update had been stored for a retrieved page.
    use Op::*;
    prop_pagecache_works(vec![Allocate, Get(0)], true);
}

#[test]
fn pagecache_bug_02() {
    // postmortem: historically needed to "seed" a page by writing
    // a compacting base to it. changed the snapshot and page-in code
    // to allow a link being the first update to hit a page.
    // portmortem 2:
    use Op::*;
    prop_pagecache_works(vec![Allocate, Restart, Link(0, 1)], true);
}

#[test]
fn pagecache_bug_3() {
    // postmortem: this was a mismatch in semantics in the test harness itself
    use Op::*;
    prop_pagecache_works(vec![Allocate, Get(0)], true);
}

#[test]
fn pagecache_bug_4() {
    // postmortem: previously this caused a panic, we shouldn't break
    // when the user asks us to mutate non-existant pages!
    use Op::*;
    prop_pagecache_works(vec![Link(98, 1)], true);
}

#[test]
fn pagecache_bug_5() {
    // postmortem: this was a mismatch in semantics in the test harness itself
    use Op::*;
    prop_pagecache_works(vec![Link(132, 1), Replace(132, 1)], true);
}

#[test]
fn pagecache_bug_6() {
    // postmortem: the test wasn't actually recording changes to the reference page...
    use Op::*;
    prop_pagecache_works(
        vec![Allocate, Replace(0, 53), Replace(0, 54)],
        true,
    );
}

#[test]
fn pagecache_bug_7() {
    // postmortem: the test wasn't correctly recording the replacement effect of a replace
    // in the reference page
    use Op::*;
    prop_pagecache_works(
        vec![Allocate, Link(0, 201), Replace(0, 208), Get(0)],
        true,
    );
}

#[test]
fn pagecache_bug_8() {
    // postmortem: page_in messed up the stack ordering when storing a linked stack
    use Op::*;
    prop_pagecache_works(
        vec![
            Allocate,
            Replace(0, 188),
            Allocate,
            Link(1, 196),
            Link(1, 198),
            Link(1, 200),
            Link(0, 201),
            Get(1),
        ],
        true,
    );
}

#[test]
fn pagecache_bug_9() {
    // postmortem: this started failing in the giant refactor for log structured storage,
    // and was possibly fixed by properly handling intervals in mark_interval
    use Op::*;
    prop_pagecache_works(
        vec![
            Allocate,
            Allocate,
            Link(1, 208),
            Link(1, 211),
            Link(0, 212),
            Replace(0, 213),
            Replace(1, 214),
        ],
        true,
    );
}

#[test]
fn pagecache_bug_10() {
    // postmortem: the segment was marked free before it
    // was actually full, because the pids inside were
    // rewritten.
    use Op::*;
    prop_pagecache_works(
        vec![
            Allocate,
            Replace(0, 425),
            Free(0),
            Allocate,
            Link(1, 427),
        ],
        true,
    );
}

#[test]
fn pagecache_bug_11() {
    // postmortem: failed to completely back-out an experiment
    // to tombstone allocations, which removed a check.
    use Op::*;
    prop_pagecache_works(vec![Free(0)], true);
}

#[test]
fn pagecache_bug_12() {
    // postmortem: refactor to add Free tombstones changed
    // the model.
    use Op::*;
    prop_pagecache_works(
        vec![Allocate, Free(0), Replace(0, 66)],
        true,
    );
}

#[test]
fn pagecache_bug_13() {
    // postmortem: Free tombstones mean that the page table may
    // already have an entry for a newly-allocated page.
    use Op::*;
    prop_pagecache_works(
        vec![Allocate, Free(0), Free(0), Restart, Allocate, Allocate],
        true,
    );
}

#[test]
fn pagecache_bug_14() {
    // postmortem: Free tombstones are a little weird with the Lru.
    // Make sure they don't get paged out.
    use Op::*;
    prop_pagecache_works(
        vec![
            Allocate,
            Allocate,
            Replace(0, 955),
            Replace(1, 956),
            Restart,
            Replace(1, 962),
            Free(1),
            Get(0),
        ],
        true,
    );
}

#[test]
fn pagecache_bug_15() {
    // postmortem: non-idempotent PageCache::free.
    // fixed by deduplicating the free list on recovery.
    use Op::*;
    prop_pagecache_works(
        vec![
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
        true,
    );
}

#[test]
fn pagecache_bug_16() {
    // postmortem: non-idempotent PageCache::free.
    // did not check if a Free tombstone was present,
    // just if the stack was present.
    use Op::*;
    prop_pagecache_works(
        vec![
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
        true,
    );
}

#[test]
fn pagecache_bug_17() {
    // postmortem:
    use Op::*;
    prop_pagecache_works(
        vec![
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
        true,
    );
}

#[test]
fn pagecache_bug_18() {
    // postmortem: added page replacement information to
    // the segment that a page was being added to.
    use Op::*;
    prop_pagecache_works(
        vec![Allocate, Link(0, 273), Free(0), Restart, Restart],
        true,
    );
}

#[test]
fn pagecache_bug_19() {
    // postmortem: was double-clearing removals when tracking
    // removed pages in segments, before doing the recovery
    // check to see if the snapshot already includes updates
    // for that lsn.
    use Op::*;
    prop_pagecache_works(
        vec![
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
        true,
    );
}

#[test]
fn pagecache_bug_20() {
    // postmortem: failed to handle Unallocated nodes properly
    // in refactored test model.
    use Op::*;
    prop_pagecache_works(vec![Free(0), Replace(0, 17)], true);
}

#[test]
fn pagecache_bug_21() {
    // postmortem: test model marked unused pids as Unallocated
    // instead of Free during recovery.
    use Op::*;
    prop_pagecache_works(
        vec![Allocate, Free(0), Restart, Allocate, Restart, Get(0)],
        true,
    );
}

#[test]
fn pagecache_bug_22() {
    // postmortem: was initializing the SA with segments that were not
    // necessarily populated beyond the initial header.
    use Op::*;
    prop_pagecache_works(
        vec![
            Allocate,
            Allocate,
            Allocate,
            Link(2, 12879),
            Allocate,
            Replace(3, 12881),
            Allocate,
            Allocate,
            Link(2, 12882),
            Allocate,
            Link(5, 12883),
            Link(5, 12884),
            Replace(2, 12886),
            Free(0),
            Link(5, 12888),
            Link(2, 12889),
            Replace(4, 12890),
            Free(1),
            Restart,
            Allocate,
            Allocate,
            Allocate,
            Link(1, 12891),
            Free(2),
            Free(4),
            Link(1, 12893),
            Free(7),
            Free(0),
            Replace(3, 12894),
            Replace(3, 12895),
            Allocate,
            Free(1),
            Link(6, 12897),
            Restart,
            Link(6, 12898),
            Replace(6, 12899),
            Free(3),
            Allocate,
            Allocate,
            Allocate,
            Replace(2, 12903),
            Replace(6, 12904),
            Free(5),
            Link(7, 12905),
            Replace(4, 12906),
            Link(6, 12907),
            Allocate,
            Free(2),
            Replace(6, 12909),
            Restart,
            Replace(1, 12910),
            Restart,
            Free(6),
        ],
        true,
    );
}

#[test]
fn pagecache_bug_23() {
    // postmortem: mishandling of segment safety buffers
    // by using the wrong index in the check for lsn's of
    // segments linking together properly. also improperly
    // handled segments that were pushed to the free list
    // by ensure_safe_free_distance, which were then removed
    // because they were the tip, effectively negating
    // their utility. fix: add bool to everything in the
    // free list signifying if they were pushed from
    // ensure_safe_free_distance or not, and only
    // perform file truncation if not.
    use Op::*;
    prop_pagecache_works(
        vec![
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Replace(4, 4461),
            Replace(1, 4463),
            Replace(0, 4464),
            Replace(1, 4465),
            Link(2, 4466),
            Replace(2, 4467),
            Replace(0, 4468),
            Replace(4, 4469),
            Link(3, 4470),
            Free(2),
            Link(1, 4471),
            Free(5),
            Replace(1, 4472),
            Allocate,
            Replace(2, 4473),
            Link(6, 4474),
            Replace(1, 4476),
            Restart,
            Allocate,
            Free(6),
            Replace(0, 4478),
            Replace(2, 4480),
            Free(3),
            Replace(5, 4481),
            Link(5, 4482),
            Link(5, 4483),
            Link(5, 4485),
            Replace(2, 4486),
            Replace(1, 4487),
            Allocate,
            Free(6),
            Link(0, 4490),
            Allocate,
            Allocate,
            Link(4, 4491),
            Free(5),
            Link(0, 4492),
            Replace(0, 4493),
            Link(0, 4494),
            Free(1),
            Allocate,
            Free(3),
            Replace(4, 4501),
            Restart,
            Link(0, 4503),
            Link(4, 4504),
            Link(0, 4505),
            Allocate,
            Replace(6, 4507),
            Restart,
            Replace(5, 4512),
            Link(4, 4513),
            Allocate,
            Allocate,
            Free(2),
            Replace(4, 4515),
            Link(0, 4516),
            Replace(6, 4517),
            Free(4),
            Link(0, 4518),
            Replace(5, 4519),
            Replace(3, 4520),
            Restart,
            Replace(3, 4522),
        ],
        true,
    );
}

#[test]
fn pagecache_bug_24() {
    // postmortem:
    use Op::*;
    prop_pagecache_works(
        vec![
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Replace(0, 4744),
            Link(1, 4745),
            Replace(4, 4746),
            Replace(1, 4747),
            Allocate,
            Replace(7, 4748),
            Replace(1, 4749),
            Link(4, 4750),
            Link(0, 4751),
            Replace(6, 4752),
            Replace(7, 4753),
            Link(6, 4754),
            Replace(4, 4755),
            Free(7),
            Replace(4, 4756),
            Free(6),
            Replace(0, 4758),
            Replace(2, 4762),
            Link(4, 4763),
            Replace(2, 4765),
            Free(4),
            Allocate,
            Free(1),
            Allocate,
            Allocate,
            Link(4, 4772),
            Replace(7, 4774),
            Replace(4, 4775),
            Replace(0, 4780),
            Link(7, 4781),
            Replace(2, 4782),
            Replace(0, 4783),
            Free(6),
            Link(0, 4785),
            Link(2, 4786),
            Replace(3, 4787),
            Restart,
            Replace(0, 4788),
            Link(0, 4789),
            Replace(2, 4790),
            Link(0, 4791),
            Free(3),
            Replace(5, 4792),
            Allocate,
            Replace(7, 4793),
            Allocate,
            Allocate,
            Free(7),
            Allocate,
            Replace(0, 4794),
            Replace(4, 4795),
            Link(4, 4796),
            Allocate,
            Allocate,
            Link(4, 4797),
            Replace(3, 4798),
            Restart,
            Link(1, 4799),
            Link(5, 4800),
            Free(0),
            Restart,
            Replace(0, 4802),
        ],
        true,
    );
}

#[test]
fn pagecache_bug_25() {
    // postmortem:
    use Op::*;
    prop_pagecache_works(
        vec![
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Link(2, 769),
            Replace(4, 770),
            Link(0, 772),
            Replace(1, 773),
            Replace(1, 774),
            Replace(1, 778),
            Replace(1, 779),
            Replace(0, 780),
            Link(5, 781),
            Link(1, 782),
            Link(2, 783),
            Link(5, 784),
            Replace(5, 786),
            Link(0, 787),
            Link(1, 788),
            Link(0, 789),
            Allocate,
            Free(5),
            Replace(6, 790),
            Free(6),
            Allocate,
            Link(2, 793),
            Free(0),
            Replace(2, 795),
            Free(2),
            Allocate,
            Allocate,
            Link(5, 796),
            Free(1),
            Replace(4, 798),
            Allocate,
            Replace(5, 800),
            Allocate,
            Free(0),
            Replace(2, 801),
            Link(7, 802),
            Allocate,
            Free(2),
            Allocate,
            Link(3, 803),
            Allocate,
            Link(6, 805),
            Restart,
            Replace(7, 806),
            Allocate,
            Replace(7, 807),
            Allocate,
            Free(4),
            Link(6, 808),
            Allocate,
            Restart,
            Allocate,
            Link(2, 809),
            Replace(6, 810),
            Free(1),
            Link(0, 811),
            Link(0, 812),
            Link(6, 813),
            Free(5),
            Allocate,
            Allocate,
            Allocate,
            Restart,
            Link(1, 815),
            Replace(6, 816),
            Free(3),
            Restart,
            Link(3, 825),
        ],
        true,
    );
}

#[test]
fn pagecache_bug_26() {
    // postmortem: an implementation of make_stable was accidentally
    // returning early before doing any stabilization.
    use Op::*;
    prop_pagecache_works(
        vec![
            Allocate,
            Allocate,
            Allocate,
            Allocate,
            Link(3, 9261),
            Replace(2, 9263),
            Restart,
            Link(3, 9266),
            Link(3, 9268),
            Get(2),
            Free(3),
        ],
        true,
    );
}

#[test]
fn pagecache_bug_27() {
    // postmortem: was using a bad pointer for recovery
    use Op::*;
    prop_pagecache_works(
        vec![Allocate, Free(0), Restart, Allocate],
        true,
    );
}

#[test]
fn pagecache_bug_28() {
    // postmortem: should not be using pointers in the free page list
    // at all, because segments can be relocated during segment compaction.
    use Op::*;
    prop_pagecache_works(
        vec![Allocate, Free(0), Restart, Free(0), Allocate],
        true,
    );
}

fn _pagecache_bug_() {
    // postmortem: TEMPLATE
    // portmortem 2: ...
    // use Op::*;
    prop_pagecache_works(vec![], true);
}

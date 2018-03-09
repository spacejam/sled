extern crate quickcheck;
extern crate rand;
extern crate sled;
extern crate pagecache;

use std::collections::BTreeMap;
use std::thread;
use std::sync::Arc;

use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};

use sled::*;
use pagecache::ConfigBuilder;

const N_THREADS: usize = 5;
const N_PER_THREAD: usize = 300;
const N: usize = N_THREADS * N_PER_THREAD; // NB N should be multiple of N_THREADS
const SPACE: usize = N;

#[inline(always)]
fn kv(i: usize) -> Vec<u8> {
    let i = i % SPACE;
    let k = [(i >> 16) as u8, (i >> 8) as u8, i as u8];
    k.to_vec()
}

#[test]
fn parallel_tree_ops() {
    macro_rules! par {
        ($t:ident, $f:expr) => {
            let mut threads = vec![];
            for tn in 0..N_THREADS {
                let sz = N / N_THREADS;
                let tree = $t.clone();
                let thread = thread::Builder::new()
                    .name(format!("t({})", tn))
                    .spawn(move || {
                        for i in (tn * sz)..((tn + 1) * sz) {
                            let k = kv(i);
                            $f(&*tree, k);
                        }
                    })
                    .unwrap();
                threads.push(thread);
            }
            while let Some(thread) = threads.pop() {
                thread.join().unwrap();
            }
        };
    }

    println!("========== initial sets ==========");
    let config = ConfigBuilder::new().temporary(true).blink_fanout(2).build();
    let t = Arc::new(sled::Tree::start(config).unwrap());
    par!{t, |tree: &Tree, k: Vec<u8>| {
        assert_eq!(tree.get(&*k), Ok(None));
        tree.set(k.clone(), k.clone()).unwrap();
        assert_eq!(tree.get(&*k), Ok(Some(k)));
    }};

    println!("========== reading sets ==========");
    par!{t, |tree: &Tree, k: Vec<u8>| {
        if tree.get(&*k.clone()) != Ok(Some(k.clone())) {
            println!("{}", tree.key_debug_str(&*k.clone()));
            panic!("expected key {:?} not found", k);
        }
    }};

    println!("========== CAS test ==========");
    par!{t, |tree: &Tree, k: Vec<u8>| {
        let k1 = k.clone();
        let mut k2 = k.clone();
        k2.reverse();
        tree.cas(k1.clone(), Some(k1), Some(k2)).unwrap();
    }};

    par!{t, |tree: &Tree, k: Vec<u8>| {
        let k1 = k.clone();
        let mut k2 = k.clone();
        k2.reverse();
        assert_eq!(tree.get(&*k1), Ok(Some(k2)));
    }};

    println!("========== deleting ==========");
    par!{t, |tree: &Tree, k: Vec<u8>| {
        tree.del(&*k).unwrap();
    }};

    par!{t, |tree: &Tree, k: Vec<u8>| {
        assert_eq!(tree.get(&*k), Ok(None));
    }};
}

#[test]
fn tree_subdir() {
    let config = ConfigBuilder::new()
        .path("test_tree_subdir/test_subdir".to_owned())
        .build();
    let t = sled::Tree::start(config).unwrap();

    t.set(vec![1], vec![1]).unwrap();

    drop(t);

    let config = ConfigBuilder::new()
        .path("test_tree_subdir/test_subdir".to_owned())
        .build();
    let t = sled::Tree::start(config).unwrap();

    let res = t.get(&*vec![1]);

    drop(t);

    std::fs::remove_dir_all("test_tree_subdir").unwrap();

    assert_eq!(res, Ok(Some(vec![1])));
}

#[test]
fn tree_iterator() {
    println!("========== iterator ==========");
    let config = ConfigBuilder::new()
        .temporary(true)
        .blink_fanout(2)
        .flush_every_ms(None)
        .build();
    let t = sled::Tree::start(config).unwrap();
    for i in 0..N_PER_THREAD {
        let k = kv(i);
        t.set(k.clone(), k).unwrap();
    }

    for (i, (k, v)) in t.iter().map(|res| res.unwrap()).enumerate() {
        let should_be = kv(i);
        assert_eq!(should_be, k);
        assert_eq!(should_be, v);
    }

    for (i, (k, v)) in t.scan(b"").map(|res| res.unwrap()).enumerate() {
        let should_be = kv(i);
        assert_eq!(should_be, k);
        assert_eq!(should_be, v);
    }

    let half_way = N_PER_THREAD / 2;
    let half_key = kv(half_way);
    let mut tree_scan = t.scan(&*half_key);
    assert_eq!(tree_scan.next(), Some(Ok((half_key.clone(), half_key))));

    let first_key = kv(0);
    let mut tree_scan = t.scan(&*first_key);
    assert_eq!(tree_scan.next(), Some(Ok((first_key.clone(), first_key))));

    let last_key = kv(N_PER_THREAD - 1);
    let mut tree_scan = t.scan(&*last_key);
    assert_eq!(tree_scan.next(), Some(Ok((last_key.clone(), last_key))));
    assert_eq!(tree_scan.next(), None);
}

#[test]
fn recover_tree() {
    println!("========== recovery ==========");
    let config = ConfigBuilder::new()
        .temporary(true)
        .blink_fanout(2)
        .io_buf_size(5000)
        .flush_every_ms(None)
        .snapshot_after_ops(100)
        .build();
    let t = sled::Tree::start(config.clone()).unwrap();
    for i in 0..N_PER_THREAD {
        let k = kv(i);
        t.set(k.clone(), k).unwrap();
    }
    drop(t);

    let t = sled::Tree::start(config.clone()).unwrap();
    for i in 0..config.blink_fanout << 1 {
        let k = kv(i);
        assert_eq!(t.get(&*k), Ok(Some(k.clone())));
        t.del(&*k).unwrap();
    }
    drop(t);

    let t = sled::Tree::start(config.clone()).unwrap();
    for i in 0..config.blink_fanout << 1 {
        let k = kv(i);
        assert_eq!(t.get(&*k), Ok(None));
    }
}

#[derive(Debug, Clone)]
enum Op {
    Set(u8, u8),
    Merge(u8, u8),
    Get(u8),
    Del(u8),
    Cas(u8, u8, u8),
    Scan(u8, usize),
    Restart,
}

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        if g.gen_weighted_bool(10) {
            return Op::Restart;
        }

        let choice = g.gen_range(0, 6);

        match choice {
            0 => Op::Set(g.gen::<u8>(), g.gen::<u8>()),
            1 => Op::Merge(g.gen::<u8>(), g.gen::<u8>()),
            2 => Op::Get(g.gen::<u8>()),
            3 => Op::Del(g.gen::<u8>()),
            4 => Op::Cas(g.gen::<u8>(), g.gen::<u8>(), g.gen::<u8>()),
            5 => Op::Scan(g.gen::<u8>(), g.gen_range::<usize>(0, 40)),
            _ => panic!("impossible choice"),
        }
    }
}

fn bytes_to_u16(v: &[u8]) -> u16 {
    assert_eq!(v.len(), 2);
    ((v[0] as u16) << 8) + v[1] as u16
}

fn u16_to_bytes(u: u16) -> Vec<u8> {
    vec![(u >> 8) as u8, u as u8]
}

// just adds up values as if they were u16's
fn test_merge_operator(
    _k: &[u8],
    old: Option<&[u8]>,
    to_merge: &[u8],
) -> Option<Vec<u8>> {
    let base = old.unwrap_or(&[0, 0]);
    let base_n = bytes_to_u16(base);
    let new_n = base_n + to_merge[0] as u16;
    let ret = u16_to_bytes(new_n);
    Some(ret)
}

fn prop_tree_matches_btreemap(
    ops: Vec<Op>,
    blink_fanout: u8,
    snapshot_after: u8,
    flusher: bool,
) -> bool {

    use self::Op::*;
    let config = ConfigBuilder::new()
        .temporary(true)
        .snapshot_after_ops(snapshot_after as usize + 1)
        .flush_every_ms(if flusher { Some(1) } else { None })
        .io_buf_size(10000)
        .blink_fanout(blink_fanout as usize + 2)
        .cache_capacity(40)
        .merge_operator(test_merge_operator)
        .build();

    let mut tree = sled::Tree::start(config.clone()).unwrap();
    let mut reference = BTreeMap::new();

    for op in ops.into_iter() {
        match op {
            Set(k, v) => {
                tree.set(vec![k], vec![0, v]).unwrap();
                reference.insert(k, v as u16);
            }
            Merge(k, v) => {
                tree.merge(vec![k], vec![v]).unwrap();
                let mut entry = reference.entry(k).or_insert(0u16);
                *entry += v as u16;
            }
            Get(k) => {
                let res1 =
                    tree.get(&*vec![k]).unwrap().map(|v| bytes_to_u16(&*v));
                let res2 = reference.get(&k).cloned();
                assert_eq!(res1, res2);
            }
            Del(k) => {
                tree.del(&*vec![k]).unwrap();
                reference.remove(&k);
            }
            Cas(k, old, new) => {
                let tree_old = tree.get(&*vec![k]).unwrap();
                if tree_old == Some(vec![0, old]) {
                    tree.set(vec![k], vec![0, new]).unwrap();
                }

                let ref_old = reference.get(&k).cloned();
                if ref_old == Some(old as u16) {
                    reference.insert(k, new as u16);
                }
            }
            Scan(k, len) => {
                let mut tree_iter = tree.scan(&*vec![k]).take(len).map(|res| {
                    let (ref tk, ref tv) = res.unwrap();
                    (tk[0], tv.clone())
                });
                let ref_iter = reference
                    .iter()
                    .filter(|&(ref rk, _rv)| **rk >= k)
                    .take(len)
                    .map(|(ref rk, ref rv)| (**rk, **rv));
                for r in ref_iter {
                    assert_eq!(
                        Some((r.0, u16_to_bytes(r.1))),
                        tree_iter.next()
                    );
                }
            }
            Restart => {
                drop(tree);
                tree = sled::Tree::start(config.clone()).unwrap();
            }
        }
    }

    true
}

#[test]
fn quickcheck_tree_matches_btreemap() {
    // use fewer tests for travis OSX builds that stall out all the time
    #[cfg(target_os = "macos")]
    let n_tests = 100;

    #[cfg(not(target_os = "macos"))]
    let n_tests = 500;

    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 100))
        .tests(n_tests)
        .max_tests(10000)
        .quickcheck(
            prop_tree_matches_btreemap as fn(Vec<Op>, u8, u8, bool) -> bool,
        );
}

#[test]
fn tree_bug_01() {
    // postmortem:
    // this was a bug in the snapshot recovery, where
    // it led to max_id dropping by 1 after a restart.
    // postmortem 2:
    // we were stalling here because we had a new log with stable of
    // SEG_HEADER_LEN, but when we iterated over it to create a new
    // snapshot (snapshot every 1 set in Config), we iterated up until
    // that offset. make_stable requires our stable offset to be >=
    // the provided one, to deal with 0.
    use Op::*;
    prop_tree_matches_btreemap(
        vec![Set(32, 9), Set(195, 13), Restart, Set(164, 147)],

        0,
        0,
        true,
    );

}

#[test]
fn tree_bug_2() {
    // postmortem:
    // this was a bug in the way that the `Materializer`
    // was fed data, possibly out of order, if recover
    // in the pagecache had to run over log entries
    // that were later run through the same `Materializer`
    // then the second time (triggered by a snapshot)
    // would not pick up on the importance of seeing
    // the new root set.
    use Op::*;
    prop_tree_matches_btreemap(
        vec![Restart, Set(215, 121), Restart, Set(216, 203), Scan(210, 4)],

        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_3() {
    // postmortem: the tree was not persisting and recovering root hoists
    // postmortem 2: when refactoring the log storage, we failed to restart
    // log writing in the proper location.
    use Op::*;
    prop_tree_matches_btreemap(
        vec![
            Set(113, 204),
            Set(119, 205),
            Set(166, 88),
            Set(23, 44),
            Restart,
            Set(226, 192),
            Set(189, 186),
            Restart,
            Scan(198, 11),
        ],
        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_4() {
    // postmortem: pagecache was failing to replace the LogID list
    // when it encountered a new Update::Compact.
    // postmortem 2: after refactoring log storage, we were not properly
    // setting the log tip, and the beginning got clobbered after writing
    // after a restart.
    use Op::*;
    prop_tree_matches_btreemap(
        vec![
            Set(158, 31),
            Set(111, 134),
            Set(230, 187),
            Set(169, 58),
            Set(131, 10),
            Set(108, 246),
            Set(127, 155),
            Restart,
            Set(59, 119),
        ],
        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_5() {
    // postmortem: during recovery, the segment accountant was failing to properly set the file's
    // tip.
    use Op::*;
    prop_tree_matches_btreemap(
        vec![
            Set(231, 107),
            Set(251, 42),
            Set(80, 81),
            Set(178, 130),
            Set(150, 232),
            Restart,
            Set(98, 78),
            Set(0, 45),
        ],
        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_6() {
    // postmortem: after reusing segments, we were failing to checksum reads performed while
    // iterating over rewritten segment buffers, and using former garbage data. fix: use the
    // crc that's there for catching torn writes with high probability, AND zero out buffers.
    use Op::*;
    prop_tree_matches_btreemap(
        vec![
            Set(162, 8),
            Set(59, 192),
            Set(238, 83),
            Set(151, 231),
            Restart,
            Set(30, 206),
            Set(150, 146),
            Set(18, 34),
        ],
        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_7() {
    // postmortem: the segment accountant was not fully recovered, and thought that it could
    // reuse a particular segment that wasn't actually empty yet.
    use Op::*;
    prop_tree_matches_btreemap(
        vec![
            Set(135, 22),
            Set(41, 36),
            Set(101, 31),
            Set(111, 35),
            Restart,
            Set(47, 36),
            Set(79, 114),
            Set(64, 9),
            Scan(196, 25),
        ],
        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_8() {
    // postmortem: failed to properly recover the state in the segment accountant
    // that tracked the previously issued segment.
    use Op::*;
    prop_tree_matches_btreemap(
        vec![
            Set(145, 151),
            Set(155, 148),
            Set(131, 170),
            Set(163, 60),
            Set(225, 126),
            Restart,
            Set(64, 237),
            Set(102, 205),
            Restart,
        ],
        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_9() {
    // postmortem: was failing to load existing snapshots on initialization. would
    // encounter uninitialized segments at the log tip and overwrite the first segment
    // (indexed by LSN of 0) in the segment accountant ordering, skipping over
    // important updates.
    use Op::*;
    prop_tree_matches_btreemap(
        vec![
            Set(189, 36),
            Set(254, 194),
            Set(132, 50),
            Set(91, 221),
            Set(126, 6),
            Set(199, 183),
            Set(71, 125),
            Scan(67, 16),
            Set(190, 16),
            Restart,
        ],
        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_10() {
    // postmortem: after reusing a segment, but not completely writing a segment,
    // we were hitting an old LSN and violating an assert, rather than just ending.
    use Op::*;
    prop_tree_matches_btreemap(
        vec![
            Set(152, 163),
            Set(105, 191),
            Set(207, 217),
            Set(128, 19),
            Set(106, 22),
            Scan(20, 24),
            Set(14, 150),
            Set(80, 43),
            Set(174, 134),
            Set(20, 150),
            Set(13, 171),
            Restart,
            Scan(240, 25),
            Scan(77, 37),
            Set(153, 232),
            Del(2),
            Set(227, 169),
            Get(232),
            Cas(247, 151, 70),
            Set(78, 52),
            Get(16),
            Del(78),
            Cas(201, 93, 196),
            Set(172, 84),
        ],

        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_11() {
    // postmortem: a stall was happening because LSNs and LogIDs were being
    // conflated in calls to make_stable. A higher LogID than any LSN was
    // being created, then passed in.
    use Op::*;
    prop_tree_matches_btreemap(
        vec![
            Set(38, 148),
            Set(176, 175),
            Set(82, 88),
            Set(164, 85),
            Set(139, 74),
            Set(73, 23),
            Cas(34, 67, 151),
            Set(115, 133),
            Set(249, 138),
            Restart,
            Set(243, 6),
        ],
        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_12() {
    // postmortem: was not checking that a log entry's LSN matches its position as
    // part of detecting tears / partial rewrites.
    use Op::*;
    prop_tree_matches_btreemap(
        vec![
            Set(118, 156),
            Set(8, 63),
            Set(165, 110),
            Set(219, 108),
            Set(91, 61),
            Set(18, 98),
            Scan(73, 6),
            Set(240, 108),
            Cas(71, 28, 189),
            Del(199),
            Restart,
            Set(30, 140),
            Scan(118, 13),
            Get(180),
            Cas(115, 151, 116),
            Restart,
            Set(31, 95),
            Cas(79, 153, 225),
            Set(34, 161),
            Get(213),
            Set(237, 215),
            Del(52),
            Set(56, 78),
            Scan(141, 2),
            Cas(228, 114, 170),
            Get(231),
            Get(223),
            Del(167),
            Restart,
            Scan(240, 31),
            Del(54),
            Del(2),
            Set(117, 165),
            Set(223, 50),
            Scan(69, 4),
            Get(156),
            Set(214, 72),
        ],
        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_13() {
    // postmortem: failed root hoists were being improperly recovered before the
    // following free was done on their page, but we treated the written node as
    // if it were a successful completed root hoist.
    use Op::*;
    prop_tree_matches_btreemap(
        vec![
            Set(42, 10),
            Set(137, 220),
            Set(183, 129),
            Set(91, 145),
            Set(126, 26),
            Set(255, 67),
            Set(69, 18),
            Restart,
            Set(24, 92),
            Set(193, 17),
            Set(3, 143),
            Cas(50, 13, 84),
            Restart,
            Set(191, 116),
            Restart,
            Del(165),
        ],
        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_14() {
    // postmortem: after adding prefix compression, we were not
    // handling re-inserts and deletions properly
    use Op::*;
    prop_tree_matches_btreemap(
        vec![
            Set(107, 234),
            Set(7, 245),
            Set(40, 77),
            Set(171, 244),
            Set(173, 16),
            Set(171, 176),
            Scan(93, 33),
        ],
        1,
        0,
        true,
    );
}

#[test]
fn tree_bug_15() {
    // postmortem: was not sorting keys properly when binary searching for them
    use Op::*;
    prop_tree_matches_btreemap(
        vec![
            Set(102, 165),
            Set(91, 191),
            Set(141, 228),
            Set(188, 124),
            Del(141),
            Scan(101, 26),
        ],
        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_16() {
    // postmortem: the test merge function was not properly adding numbers.
    use Op::*;
    prop_tree_matches_btreemap(
        vec![Merge(247, 162), Scan(209, 31)],
        0,
        0,
        false,
    );
}

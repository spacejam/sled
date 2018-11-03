extern crate pagecache;
extern crate quickcheck;
extern crate rand;
extern crate sled;
extern crate tests;

use std::sync::Arc;
use std::thread;

use pagecache::ConfigBuilder;
use sled::*;
use tests::tree::{
    prop_tree_matches_btreemap, Key,
    Op::{self, *},
};

use quickcheck::{QuickCheck, StdGen};

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
                    }).unwrap();
                threads.push(thread);
            }
            while let Some(thread) = threads.pop() {
                thread.join().unwrap();
            }
        };
    }

    println!("========== initial sets ==========");
    let config = ConfigBuilder::new()
        .temporary(true)
        .blink_node_split_size(0)
        .build();
    let t = Arc::new(sled::Tree::start(config).unwrap());
    par!{t, |tree: &Tree, k: Vec<u8>| {
        assert_eq!(tree.get(&*k), Ok(None));
        tree.set(k.clone(), k.clone()).unwrap();
        assert_eq!(tree.get(&*k).unwrap().unwrap(), k);
    }};

    println!("========== reading sets ==========");
    par!{t, |tree: &Tree, k: Vec<u8>| {
        if tree.get(&*k).unwrap().unwrap() != k {
            println!("{}", tree.key_debug_str(&*k.clone()));
            panic!("expected key {:?} not found", k);
        }
    }};

    println!("========== CAS test ==========");
    par!{t, |tree: &Tree, k: Vec<u8>| {
        let k1 = k.clone();
        let mut k2 = k.clone();
        k2.reverse();
        tree.cas(k1.clone(), Some(&*k1), Some(k2)).unwrap();
    }};

    par!{t, |tree: &Tree, k: Vec<u8>| {
        let k1 = k.clone();
        let mut k2 = k.clone();
        k2.reverse();
        assert_eq!(tree.get(&*k1).unwrap().unwrap(), k2);
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
        .path("/tmp/test_tree_subdir/test_subdir".to_owned())
        .build();
    let t = sled::Tree::start(config).unwrap();

    t.set(vec![1], vec![1]).unwrap();

    drop(t);

    let config = ConfigBuilder::new()
        .path("/tmp/test_tree_subdir/test_subdir".to_owned())
        .build();
    let t = sled::Tree::start(config).unwrap();

    let res = t.get(&*vec![1]);

    drop(t);

    std::fs::remove_dir_all("/tmp/test_tree_subdir").unwrap();

    assert_eq!(res.unwrap().unwrap(), vec![1_u8]);
}

#[test]
fn tree_iterator() {
    println!("========== iterator ==========");
    let config = ConfigBuilder::new()
        .temporary(true)
        .blink_node_split_size(0)
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
        assert_eq!(should_be, &*v);
    }

    for (i, (k, v)) in t.scan(b"").map(|res| res.unwrap()).enumerate()
    {
        let should_be = kv(i);
        assert_eq!(should_be, k);
        assert_eq!(should_be, &*v);
    }

    let half_way = N_PER_THREAD / 2;
    let half_key = kv(half_way);
    let mut tree_scan = t.scan(&*half_key);
    let r1 = tree_scan.next().unwrap().unwrap();
    assert_eq!((r1.0, &*r1.1), (half_key.clone(), &*half_key));

    let first_key = kv(0);
    let mut tree_scan = t.scan(&*first_key);
    let r2 = tree_scan.next().unwrap().unwrap();
    assert_eq!((r2.0, &*r2.1), (first_key.clone(), &*first_key));

    let last_key = kv(N_PER_THREAD - 1);
    let mut tree_scan = t.scan(&*last_key);
    let r3 = tree_scan.next().unwrap().unwrap();
    assert_eq!((r3.0, &*r3.1), (last_key.clone(), &*last_key));
    assert_eq!(tree_scan.next(), None);
}

#[test]
fn recover_tree() {
    println!("========== recovery ==========");
    let config = ConfigBuilder::new()
        .temporary(true)
        .blink_node_split_size(0)
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
    for i in 0..4 {
        let k = kv(i as usize);
        assert_eq!(t.get(&*k).unwrap().unwrap(), k);
        t.del(&*k).unwrap();
    }
    drop(t);

    let t = sled::Tree::start(config.clone()).unwrap();
    for i in 0..4 {
        let k = kv(i as usize);
        assert_eq!(t.get(&*k), Ok(None));
    }
}

#[test]
#[cfg(not(target_os = "fuchsia"))]
#[ignore]
fn quickcheck_tree_matches_btreemap() {
    // use fewer tests for travis OSX builds that stall out all the time
    #[cfg(target_os = "macos")]
    let n_tests = 100;

    #[cfg(not(target_os = "macos"))]
    let n_tests = 5;

    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 1000))
        .tests(n_tests)
        .max_tests(100000)
        .quickcheck(
            prop_tree_matches_btreemap
                as fn(Vec<Op>, u8, u8, bool) -> bool,
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
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![32]), 9),
            Set(Key(vec![195]), 13),
            Restart,
            Set(Key(vec![164]), 147),
        ],
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
    prop_tree_matches_btreemap(
        vec![
            Restart,
            Set(Key(vec![215]), 121),
            Restart,
            Set(Key(vec![216]), 203),
            Scan(Key(vec![210]), 4),
        ],
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
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![113]), 204),
            Set(Key(vec![119]), 205),
            Set(Key(vec![166]), 88),
            Set(Key(vec![23]), 44),
            Restart,
            Set(Key(vec![226]), 192),
            Set(Key(vec![189]), 186),
            Restart,
            Scan(Key(vec![198]), 11),
        ],
        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_4() {
    // postmortem: pagecache was failing to replace the LogId list
    // when it encountered a new Update::Compact.
    // postmortem 2: after refactoring log storage, we were not properly
    // setting the log tip, and the beginning got clobbered after writing
    // after a restart.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![158]), 31),
            Set(Key(vec![111]), 134),
            Set(Key(vec![230]), 187),
            Set(Key(vec![169]), 58),
            Set(Key(vec![131]), 10),
            Set(Key(vec![108]), 246),
            Set(Key(vec![127]), 155),
            Restart,
            Set(Key(vec![59]), 119),
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
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![231]), 107),
            Set(Key(vec![251]), 42),
            Set(Key(vec![80]), 81),
            Set(Key(vec![178]), 130),
            Set(Key(vec![150]), 232),
            Restart,
            Set(Key(vec![98]), 78),
            Set(Key(vec![0]), 45),
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
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![162]), 8),
            Set(Key(vec![59]), 192),
            Set(Key(vec![238]), 83),
            Set(Key(vec![151]), 231),
            Restart,
            Set(Key(vec![30]), 206),
            Set(Key(vec![150]), 146),
            Set(Key(vec![18]), 34),
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
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![135]), 22),
            Set(Key(vec![41]), 36),
            Set(Key(vec![101]), 31),
            Set(Key(vec![111]), 35),
            Restart,
            Set(Key(vec![47]), 36),
            Set(Key(vec![79]), 114),
            Set(Key(vec![64]), 9),
            Scan(Key(vec![196]), 25),
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
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![145]), 151),
            Set(Key(vec![155]), 148),
            Set(Key(vec![131]), 170),
            Set(Key(vec![163]), 60),
            Set(Key(vec![225]), 126),
            Restart,
            Set(Key(vec![64]), 237),
            Set(Key(vec![102]), 205),
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
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![189]), 36),
            Set(Key(vec![254]), 194),
            Set(Key(vec![132]), 50),
            Set(Key(vec![91]), 221),
            Set(Key(vec![126]), 6),
            Set(Key(vec![199]), 183),
            Set(Key(vec![71]), 125),
            Scan(Key(vec![67]), 16),
            Set(Key(vec![190]), 16),
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
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![152]), 163),
            Set(Key(vec![105]), 191),
            Set(Key(vec![207]), 217),
            Set(Key(vec![128]), 19),
            Set(Key(vec![106]), 22),
            Scan(Key(vec![20]), 24),
            Set(Key(vec![14]), 150),
            Set(Key(vec![80]), 43),
            Set(Key(vec![174]), 134),
            Set(Key(vec![20]), 150),
            Set(Key(vec![13]), 171),
            Restart,
            Scan(Key(vec![240]), 25),
            Scan(Key(vec![77]), 37),
            Set(Key(vec![153]), 232),
            Del(Key(vec![2])),
            Set(Key(vec![227]), 169),
            Get(Key(vec![232])),
            Cas(Key(vec![247]), 151, 70),
            Set(Key(vec![78]), 52),
            Get(Key(vec![16])),
            Del(Key(vec![78])),
            Cas(Key(vec![201]), 93, 196),
            Set(Key(vec![172]), 84),
        ],
        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_11() {
    // postmortem: a stall was happening because LSNs and LogIds were being
    // conflated in calls to make_stable. A higher LogId than any LSN was
    // being created, then passed in.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![38]), 148),
            Set(Key(vec![176]), 175),
            Set(Key(vec![82]), 88),
            Set(Key(vec![164]), 85),
            Set(Key(vec![139]), 74),
            Set(Key(vec![73]), 23),
            Cas(Key(vec![34]), 67, 151),
            Set(Key(vec![115]), 133),
            Set(Key(vec![249]), 138),
            Restart,
            Set(Key(vec![243]), 6),
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
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![118]), 156),
            Set(Key(vec![8]), 63),
            Set(Key(vec![165]), 110),
            Set(Key(vec![219]), 108),
            Set(Key(vec![91]), 61),
            Set(Key(vec![18]), 98),
            Scan(Key(vec![73]), 6),
            Set(Key(vec![240]), 108),
            Cas(Key(vec![71]), 28, 189),
            Del(Key(vec![199])),
            Restart,
            Set(Key(vec![30]), 140),
            Scan(Key(vec![118]), 13),
            Get(Key(vec![180])),
            Cas(Key(vec![115]), 151, 116),
            Restart,
            Set(Key(vec![31]), 95),
            Cas(Key(vec![79]), 153, 225),
            Set(Key(vec![34]), 161),
            Get(Key(vec![213])),
            Set(Key(vec![237]), 215),
            Del(Key(vec![52])),
            Set(Key(vec![56]), 78),
            Scan(Key(vec![141]), 2),
            Cas(Key(vec![228]), 114, 170),
            Get(Key(vec![231])),
            Get(Key(vec![223])),
            Del(Key(vec![167])),
            Restart,
            Scan(Key(vec![240]), 31),
            Del(Key(vec![54])),
            Del(Key(vec![2])),
            Set(Key(vec![117]), 165),
            Set(Key(vec![223]), 50),
            Scan(Key(vec![69]), 4),
            Get(Key(vec![156])),
            Set(Key(vec![214]), 72),
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
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![42]), 10),
            Set(Key(vec![137]), 220),
            Set(Key(vec![183]), 129),
            Set(Key(vec![91]), 145),
            Set(Key(vec![126]), 26),
            Set(Key(vec![255]), 67),
            Set(Key(vec![69]), 18),
            Restart,
            Set(Key(vec![24]), 92),
            Set(Key(vec![193]), 17),
            Set(Key(vec![3]), 143),
            Cas(Key(vec![50]), 13, 84),
            Restart,
            Set(Key(vec![191]), 116),
            Restart,
            Del(Key(vec![165])),
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
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![107]), 234),
            Set(Key(vec![7]), 245),
            Set(Key(vec![40]), 77),
            Set(Key(vec![171]), 244),
            Set(Key(vec![173]), 16),
            Set(Key(vec![171]), 176),
            Scan(Key(vec![93]), 33),
        ],
        1,
        0,
        true,
    );
}

#[test]
fn tree_bug_15() {
    // postmortem: was not sorting keys properly when binary searching for them
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![102]), 165),
            Set(Key(vec![91]), 191),
            Set(Key(vec![141]), 228),
            Set(Key(vec![188]), 124),
            Del(Key(vec![141])),
            Scan(Key(vec![101]), 26),
        ],
        0,
        0,
        true,
    );
}

#[test]
fn tree_bug_16() {
    // postmortem: the test merge function was not properly adding numbers.
    prop_tree_matches_btreemap(
        vec![Merge(Key(vec![247]), 162), Scan(Key(vec![209]), 31)],
        0,
        0,
        false,
    );
}

#[test]
fn tree_bug_17() {
    // postmortem: we were creating a copy of a node leaf during iteration
    // before accidentally putting it into a PinnedValue, despite the
    // fact that it was not actually part of the node's actual memory!
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![194, 215, 103, 0, 138, 11, 248, 131]), 70),
            Scan(Key(vec![]), 30),
        ],
        0,
        0,
        false,
    );
}

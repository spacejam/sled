extern crate quickcheck;
extern crate rand;
extern crate rsdb;

use std::collections::BTreeMap;
use std::thread;
use std::sync::Arc;

use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};
use rand::{Rng, thread_rng};

use rsdb::*;

const SPACE: usize = N;
const N_THREADS: usize = 5;
const N_PER_THREAD: usize = 300;
const N: usize = N_THREADS * N_PER_THREAD; // NB N should be multiple of N_THREADS

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

#[inline(always)]
fn kv(i: usize) -> Vec<u8> {
    let i = i % SPACE;
    let k = [(i >> 16) as u8, (i >> 8) as u8, i as u8];
    k.to_vec()
}

#[test]
fn parallel_ops() {
    println!("========== initial sets ==========");
    let conf = Config::default().blink_fanout(2);
    let t = Arc::new(conf.tree());
    par!{t, |tree: &Tree, k: Vec<u8>| {
        assert_eq!(tree.get(&*k), None);
        tree.set(k.clone(), k.clone());
        assert_eq!(tree.get(&*k), Some(k));
    }};

    println!("========== reading sets ==========");
    par!{t, |tree: &Tree, k: Vec<u8>| {
        if tree.get(&*k.clone()) != Some(k.clone()) {
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
        assert_eq!(tree.get(&*k1), Some(k2));
    }};

    println!("========== deleting ==========");
    par!{t, |tree: &Tree, k: Vec<u8>| {
        tree.del(&*k);
    }};
    par!{t, |tree: &Tree, k: Vec<u8>| {
        assert_eq!(tree.get(&*k), None);
    }};
}

#[test]
fn iterator() {
    println!("========== iterator ==========");
    let t = Config::default()
        .blink_fanout(2)
        .flush_every_ms(None)
        .tree();
    for i in 0..N_PER_THREAD {
        let k = kv(i);
        t.set(k.clone(), k);
    }

    for (i, (k, v)) in t.iter().enumerate() {
        let should_be = kv(i);
        assert_eq!(should_be, k);
        assert_eq!(should_be, v);
    }

    for (i, (k, v)) in t.scan(b"").enumerate() {
        let should_be = kv(i);
        assert_eq!(should_be, k);
        assert_eq!(should_be, v);
    }

    let half_way = N_PER_THREAD / 2;
    let half_key = kv(half_way);
    let mut tree_scan = t.scan(&*half_key);
    assert_eq!(tree_scan.next(), Some((half_key.clone(), half_key)));

    let first_key = kv(0);
    let mut tree_scan = t.scan(&*first_key);
    assert_eq!(tree_scan.next(), Some((first_key.clone(), first_key)));

    let last_key = kv(N_PER_THREAD - 1);
    let mut tree_scan = t.scan(&*last_key);
    assert_eq!(tree_scan.next(), Some((last_key.clone(), last_key)));
    assert_eq!(tree_scan.next(), None);
}

#[test]
fn recover_tree() {
    println!("========== recovery ==========");
    let path = "test_tree.log";
    let conf = Config::default()
        .blink_fanout(2)
        .flush_every_ms(None)
        .snapshot_after_ops(100)
        .path(Some(path.to_owned()));
    let t = conf.tree();
    for i in 0..N_PER_THREAD {
        let k = kv(i);
        t.set(k.clone(), k);
    }
    drop(t);

    let t = conf.tree();
    for i in 0..conf.get_blink_fanout() << 1 {
        let k = kv(i);
        assert_eq!(t.get(&*k), Some(k.clone()));
        t.del(&*k);
    }
    drop(t);

    let t = conf.tree();
    for i in 0..conf.get_blink_fanout() << 1 {
        let k = kv(i);
        assert_eq!(t.get(&*k), None);
    }
    t.__delete_all_files();
}

#[derive(Debug, Clone)]
enum Op {
    Set(u8, u8),
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

        let choice = g.gen_range(0, 5);

        match choice {
            0 => Op::Set(g.gen::<u8>(), g.gen::<u8>()),
            1 => Op::Get(g.gen::<u8>()),
            2 => Op::Del(g.gen::<u8>()),
            3 => Op::Cas(g.gen::<u8>(), g.gen::<u8>(), g.gen::<u8>()),
            4 => Op::Scan(g.gen::<u8>(), g.gen_range::<usize>(0, 40)),
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

fn prop_tree_matches_btreemap(ops: OpVec, blink_fanout: u8, snapshot_after: u8) -> bool {
    use self::Op::*;
    let nonce: String = thread_rng().gen_ascii_chars().take(10).collect();
    let path = format!("quickcheck_tree_matches_btreemap_{}", nonce);
    let config = Config::default()
        .snapshot_after_ops(snapshot_after as usize + 1)
        .blink_fanout(blink_fanout as usize + 1)
        .cache_capacity(1_000_000_000)
        .path(Some(path));
    let mut tree = config.tree();
    let mut reference = BTreeMap::new();

    for op in ops.ops.into_iter() {
        match op {
            Set(k, v) => {
                tree.set(vec![k], vec![v]);
                reference.insert(k, v);
            }
            Get(k) => {
                let res1 = tree.get(&*vec![k]).map(|v_vec| v_vec[0]);
                let res2 = reference.get(&k).cloned();
                assert_eq!(res1, res2);
            }
            Del(k) => {
                tree.del(&*vec![k]);
                reference.remove(&k);
            }
            Cas(k, old, new) => {
                let tree_old = tree.get(&*vec![k]);
                if tree_old == Some(vec![old]) {
                    tree.set(vec![k], vec![new]);
                }

                let ref_old = reference.get(&k).cloned();
                if ref_old == Some(old) {
                    reference.insert(k, new);
                }
            }
            Scan(k, len) => {
                let tree_iter = tree.scan(&*vec![k]).take(len).map(|(ref tk, ref tv)| {
                    (tk[0], tv[0])
                });
                let ref_iter = reference
                    .iter()
                    .filter(|&(ref rk, _rv)| **rk >= k)
                    .take(len)
                    .map(|(ref rk, ref rv)| (**rk, **rv));
                for (t, r) in tree_iter.zip(ref_iter) {
                    assert_eq!(t, r);
                }
            }
            Restart => {
                drop(tree);
                tree = config.tree();
            }
        }
    }

    tree.__delete_all_files();

    true
}

#[test]
fn quickcheck_tree_matches_btreemap() {
    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 1))
        .tests(50)
        .max_tests(100)
        .quickcheck(prop_tree_matches_btreemap as fn(OpVec, u8, u8) -> bool);
}

#[test]
fn test_snapshot_bug_1() {
    // postmortem:
    // this was a bug in the snapshot recovery, where
    // it led to max_id dropping by 1 after a restart.
    use Op::*;
    prop_tree_matches_btreemap(
        OpVec {
            ops: vec![Set(32, 9), Set(195, 13), Restart, Set(164, 147)],
        },
        0,
        0,
    );

}

#[test]
fn test_snapshot_bug_2() {
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
        OpVec {
            ops: vec![Restart, Set(215, 121), Restart, Set(216, 203), Scan(210, 4)],
        },
        0,
        0,
    );
}

#[test]
fn test_snapshot_bug_3() {
    // postmortem: the tree was not persisting and recovering root hoists
    use Op::*;
    prop_tree_matches_btreemap(
        OpVec {
            ops: vec![
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
        },
        0,
        0,
    );
}

#[test]
fn test_snapshot_bug_4() {
    // postmortem: pagecache was failing to replace the LogID list
    // when it encountered a new Update::Compact.
    use Op::*;
    prop_tree_matches_btreemap(
        OpVec {
            ops: vec![
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
        },
        0,
        0,
    );
}

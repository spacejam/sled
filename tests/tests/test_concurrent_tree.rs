#![cfg(target_os = "linux")]
extern crate deterministic;
extern crate quickcheck;
extern crate sled;
extern crate tests;

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use deterministic::{set_seed, spawn::spawn_with_random_prio};
use quickcheck::{QuickCheck, StdGen};
use sled::*;
use tests::tree::{
    Key,
    Op::{self, *},
};

fn bytes_to_u16(v: &[u8]) -> u16 {
    assert_eq!(v.len(), 2);
    ((v[0] as u16) << 8) + v[1] as u16
}

fn u16_to_bytes(u: u16) -> Vec<u8> {
    let ret = vec![(u >> 8) as u8, u as u8];
    ret
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

fn run_ops(
    ops: Vec<Op>,
    tree: Arc<Tree>,
    reference: Arc<RwLock<BTreeMap<Key, u16>>>,
) {
    for op in ops.into_iter() {
        match op {
            Set(k, v) => {
                let mut r = reference.write().unwrap();
                tree.set(k.0.clone(), vec![0, v]).unwrap();
                r.insert(k.clone(), v as u16);
            }
            Merge(k, v) => {
                let mut r = reference.write().unwrap();
                tree.merge(k.0.clone(), vec![v]).unwrap();
                let mut entry = r.entry(k).or_insert(0u16);
                *entry += v as u16;
            }
            Get(k) => {
                let r = reference.read().unwrap();
                let res1 = tree
                    .get(&*k.0)
                    .unwrap()
                    .map(|v| bytes_to_u16(&v));
                let res2 = r.get(&k).cloned();
                assert_eq!(res1, res2);
            }
            Del(k) => {
                let mut r = reference.write().unwrap();
                tree.del(&*k.0).unwrap();
                r.remove(&k);
            }
            Cas(k, old, new) => {
                let mut r = reference.write().unwrap();
                let tree_old = tree.get(&*k.0).unwrap();
                if let Some(old_tree) = tree_old {
                    if old_tree == &*vec![0, old] {
                        tree.set(k.0.clone(), vec![0, new]).unwrap();
                    }
                }

                let ref_old = r.get(&k).cloned();
                if ref_old == Some(old as u16) {
                    r.insert(k, new as u16);
                }
            }
            Scan(k, len) => {
                let mut r = reference.write().unwrap();
                let mut tree_iter = tree
                    .scan(&*k.0)
                    .take(len)
                    .map(|res| res.unwrap());
                let ref_iter = r
                    .iter()
                    .filter(|&(ref rk, _rv)| **rk >= k)
                    .take(len)
                    .map(|(ref rk, ref rv)| (rk.0.clone(), **rv));

                for r in ref_iter {
                    let tree_next = tree_iter.next().unwrap();
                    let lhs = (tree_next.0, &*tree_next.1);
                    let rhs = (r.0.clone(), &*u16_to_bytes(r.1));
                    assert_eq!(
                        lhs, rhs,
                        "expected iteration over the Tree \
                         to match our BTreeMap model"
                    );
                }
            }
            Restart => {
                panic!("hit restart from within thread!");
            }
        }
    }
}

fn prop_concurrent_tree_matches_btreemap(
    ops: Vec<Op>,
    deterministic_seed: usize,
    threads_gen: u8,
) -> bool {
    let threads = std::cmp::max(threads_gen, 8) as usize;

    set_seed(deterministic_seed);

    let config = ConfigBuilder::new()
        .temporary(true)
        .snapshot_after_ops(100_000)
        .flush_every_ms(None)
        .io_buf_size(10_000)
        .blink_node_split_size(0)
        .cache_capacity(40)
        .cache_bits(0)
        .merge_operator(test_merge_operator)
        .build();

    let mut tree =
        Arc::new(sled::Tree::start(config.clone()).unwrap());
    let reference: Arc<RwLock<BTreeMap<Key, u16>>> =
        Arc::new(RwLock::new(BTreeMap::new()));

    println!("ops: {:?}", ops);
    let epochs = ops.split(|o| o.is_restart());

    // perform a restart after each sequence of epochs
    for epoch in epochs {
        if epoch.is_empty() {
            // restart
            drop(tree);
            tree =
                Arc::new(sled::Tree::start(config.clone()).unwrap());
        }

        let ops_per_thread = epoch.len() / threads;
        let op_chunks =
            epoch.chunks(std::cmp::max(1, ops_per_thread));

        // split ops amongst threads and run them
        let mut threads = vec![];
        for op_chunk in op_chunks {
            let tree = tree.clone();
            let reference = reference.clone();
            let ops = op_chunk.to_vec();
            let t = spawn_with_random_prio(move || {
                run_ops(ops, tree, reference);
            });

            threads.push(t);
        }

        for t in threads.into_iter() {
            t.join().expect("thread should finish without panicking");
        }

        // restart
        drop(tree);
        tree = Arc::new(sled::Tree::start(config.clone()).unwrap());
    }

    true
}

#[test]
#[ignore]
fn quickcheck_concurrent_tree_matches_btreemap() {
    let n_tests = 10;

    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 1000))
        .tests(n_tests)
        .max_tests(100)
        .quickcheck(
            prop_concurrent_tree_matches_btreemap
                as fn(Vec<Op>, usize, u8) -> bool,
        );
}

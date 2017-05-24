extern crate blink;

use std::mem;
use std::thread;
use std::sync::Arc;

use blink::*;

const SPACE: usize = N;
const N_THREADS: usize = 5;
const N_PER_THREAD: usize = 10000;
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
                        $f(&tree, k);
                    }
                })
                .unwrap();
            threads.push(thread);
        }
        while let Some(thread) = threads.pop() {
            thread.join();
        }
    };
}

#[inline(always)]
fn kv(i: usize) -> Vec<u8> {
    let i = i % SPACE;
    let k = [// (i >> 56) as u8,
             // (i >> 48) as u8,
             // (i >> 40) as u8,
             // (i >> 32) as u8,
             // (i >> 24) as u8,
             (i >> 16) as u8,
             (i >> 8) as u8,
             i as u8];
    k.to_vec()
}

#[test]
fn it_works() {
    println!("========== initial sets ==========");
    let t = Tree::new();
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
        tree.cas(k1.clone(), Some(k1), k2).unwrap();
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

// TODO quickcheck splits, reads, writes interleaved

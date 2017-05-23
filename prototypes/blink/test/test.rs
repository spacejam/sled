extern crate blink;

use std::mem;
use std::thread;
use std::sync::Arc;

use blink::*;

const N: usize = 70;
const SPACE: usize = 7000;
const N_THREADS: usize = 7;

#[test]
fn it_works() {
    // TODO crashes when we mod over 256 space, with old test code
    #[inline(always)]
    fn kv(i: usize) -> (Vec<u8>, Vec<u8>) {
        let i = i % SPACE;
        let k = [// (i >> 56) as u8,
                 // (i >> 48) as u8,
                 // (i >> 40) as u8,
                 // (i >> 32) as u8,
                 // (i >> 24) as u8,
                 // (i >> 16) as u8,
                 (i >> 8) as u8,
                 i as u8];
        (k.to_vec(), k.to_vec())
    }
    let t = Tree::new();
    let mut threads = vec![];
    for tn in 0..N_THREADS {
        let sz = N / N_THREADS;
        let tree = t.clone();
        let thread = thread::Builder::new()
            .name(format!("t({})", tn))
            .spawn(move || {
                for i in (tn * sz)..((tn + 1) * sz) {
                    let (k, v) = kv((i));
                    if SPACE >= N {
                        assert_eq!(tree.get(&*k), None);
                    }
                    tree.set(k.to_vec(), v.to_vec());
                    assert_eq!(tree.get(&*k), Some(v));
                }
            })
            .unwrap();
        threads.push(thread);
    }
    while let Some(thread) = threads.pop() {
        thread.join();
    }
    let tree = t.clone();
    for i in 0..N {
        let (k, v) = kv((i));
        if tree.get(&*k) != Some(v) {
            println!("{}", tree.key_debug_str(&*k));
            println!("{:?}", tree);
            panic!("expected key {:?} not found", k);
        }
    }
    for i in 0..N / 2 {
        let (k, _v) = kv((i));
        tree.del(&*k);
    }
    for i in N / 2..N {
        let (k, _v) = kv((i));
        tree.del(&*k);
    }
    for i in 0..N {
        let (k, _v) = kv((i));
        assert_eq!(tree.get(&*k), None);
    }
    for i in 0..N {
        let (k, v) = kv((i));
        tree.set(k.to_vec(), v.to_vec());
        assert_eq!(tree.get(&*k), Some(v));
    }
    for i in 0..N / 2 {
        let (k, v1) = kv((i));
        let (_k, v2) = kv((i + 1));
        tree.cas(k.clone(), Some(v1), v2.clone()).unwrap();
    }
    for i in N / 2..N {
        let (k, v1) = kv((i));
        let (_k, v2) = kv((i + 1));
        tree.cas(k.clone(), Some(v1), v2.clone()).unwrap();
    }
    for i in 0..N {
        let (k, v1) = kv((i));
        let (_k, v2) = kv((i + 1));
        assert_eq!(tree.get(&*k), Some(v2));
    }
    for i in 0..N {
        let (k, _v) = kv((i));
        tree.del(&*k);
        assert_eq!(tree.get(&*k), None);
    }
    for i in 0..N {
        let (k, _v) = kv((i));
        assert_eq!(tree.get(&*k), None);
    }
}

// TODO quickcheck splits, reads, writes interleaved

extern crate blink;

use std::mem;

use blink::*;

const N: usize = 70000;
const SPACE: usize = 70000;

#[test]
fn it_works() {
    // TODO crashes when we mod over 256 space, with old test code
    #[inline(always)]
    fn kv(i: usize) -> (Vec<u8>, Vec<u8>) {
        let i = i % SPACE;
        let k = [(i >> 56) as u8,
                 (i >> 48) as u8,
                 (i >> 40) as u8,
                 (i >> 32) as u8,
                 (i >> 24) as u8,
                 (i >> 16) as u8,
                 (i >> 8) as u8,
                 i as u8];
        (k.to_vec(), k.to_vec())
    }
    let tree = Tree::new();
    for i in 0..N {
        let (k, v) = kv((i));
        assert_eq!(tree.get(&*k), None);
        tree.set(k.to_vec(), v.to_vec());
        assert_eq!(tree.get(&*k), Some(v));
    }
    for i in 0..N {
        let (k, v) = kv((i));
        assert_eq!(tree.get(&*k), Some(v));
    }
    for i in 0..N {
        let (k, _v) = kv((i));
        tree.del(&*k);
        assert_eq!(tree.get(&*k), None);
    }
    for i in 0..N {
        let (k, v) = kv((i));
        tree.set(k.to_vec(), v.to_vec());
        assert_eq!(tree.get(&*k), Some(v));
    }
    for i in 0..N {
        let (k, v1) = kv((i));
        let (_k, v2) = kv((i + 1));
        tree.cas(k.clone(), Some(v1), v2.clone()).unwrap();
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

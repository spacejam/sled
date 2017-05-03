extern crate blink;

use std::mem;

use blink::*;

const N: usize = 70000;
const SPACE: usize = 256;

#[test]
fn it_works() {
    // TODO crashes when we mod over 256 space
    #[inline(always)]
    fn kv(i: usize) -> (Vec<u8>, Vec<u8>) {
        let i = i % 256;
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
        // assert_eq!(tree.get(&*k), None);
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
    }
    for i in 0..N {
        let (k, v) = kv((i));
        tree.set(k.to_vec(), v.to_vec());
    }
    for i in 0..N {
        let (k, _v) = kv((i));
        tree.del(&*k);
    }
    for i in 0..N {
        let (k, _v) = kv((i));
        assert_eq!(tree.get(&*k), None);
    }
}

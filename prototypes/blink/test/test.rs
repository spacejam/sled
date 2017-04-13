extern crate blink;

use std::mem;

use blink::*;

#[test]
fn it_works() {
    #[inline(always)]
    fn kv(i: usize) -> (Vec<u8>, Vec<u8>) {
        let k: [u8; 8] = unsafe { mem::transmute(i) };
        (k.to_vec(), k.to_vec())
    }
    let tree = Tree::new();
    for i in 0..1000 {
        let (k, v) = kv(i);
        tree.set(k.to_vec(), v.to_vec());
    }
    for i in 0..1000 {
        let (k, v) = kv(i);
        assert_eq!(tree.get(&*k), Some(v));
    }
    for i in 0..1000 {
        let (k, _v) = kv(i);
        tree.del(&*k);
    }
    for i in 0..1000 {
        let (k, _v) = kv(i);
        assert_eq!(tree.get(&*k), None);
    }
}

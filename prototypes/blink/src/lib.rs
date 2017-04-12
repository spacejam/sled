/// An in-memory b-link tree.

// notes
// * during all traversals, merges or splits may be encountered
// * if a partial SMO is encountered, complete it

macro_rules! rep_no_copy {
    ($e:expr; $n:expr) => {
        {
            let mut v = Vec::with_capacity($n);
            for _ in 0..$n {
                v.push($e);
            }
            v
        }
    };
}

mod tree;
mod bound;
mod radix;
mod stack;

use tree::Tree;
use bound::Bound;
use radix::Radix;
use stack::Stack;

type PageID = usize;

#[inline(always)]
fn raw<T>(t: T) -> *const T {
    Box::into_raw(Box::new(t)) as *const T
}
// general traversal hazards:
// encounter merged nodes

#[test]
fn test_bounds() {
    use Bound::*;
    assert!(Inf == Inf);
    assert!(Non(vec![]) == Non(vec![]));
    assert!(Inc(vec![]) == Inc(vec![]));
    assert!(Inc(b"hi".to_vec()) == Inc(b"hi".to_vec()));
    assert!(Non(b"hi".to_vec()) == Non(b"hi".to_vec()));
    assert!(Inc(b"hi".to_vec()) > Non(b"hi".to_vec()));
    assert!(Inc(vec![]) < Inf);
    assert!(Non(vec![]) < Inf);
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::*;

    #[test]
    fn it_works() {
        #[inline(always)]
        fn kv(i: usize) -> (Vec<u8>, Vec<u8>) {
            let k: [u8; 8] = unsafe { mem::transmute(i) };
            (k.to_vec(), k.to_vec())
        }
        let tree = Tree::new();
        for i in 0..100000 {
            let (k, v) = kv(i);
            tree.set(k.to_vec(), v.to_vec());
        }
        for i in 0..100000 {
            let (k, v) = kv(i);
            assert_eq!(tree.get(&*k), Some(v));
        }
        for i in 0..100000 {
            let (k, _v) = kv(i);
            tree.del(&*k);
        }
        for i in 0..100000 {
            let (k, _v) = kv(i);
            assert_eq!(tree.get(&*k), None);
        }
    }
}

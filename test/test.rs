#![feature(test)]
extern crate test;
extern crate rsdb;

use test::Bencher;

use rsdb::RSDB;

#[test]
fn it_works() {
    let mut db = RSDB::new("/tmp/rsdb").unwrap();
    db.set(b"k1", b"v1").unwrap();
    assert!(db.get(b"k1").unwrap().unwrap() == b"v1")
}

#[bench]
fn bench_set(b: &mut Bencher) {
    let mut db = RSDB::new("/tmp/rsdb").unwrap();
    b.iter(|| db.set(b"k1", b"v1").unwrap());
}

#[test]
fn atomic_stuff() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let a = AtomicUsize::new(0);
    let c1 = a.load(Ordering::SeqCst) as u32;

    fn is_sealed(v: u32) -> bool {
        v >> 31 == 1
    }

    fn mk_sealed(v: u32) -> u32 {
        v | 1 << 31
    }

    fn n_writers(v: u32) -> u32 {
        v << 1 >> 25
    }

    fn incr_writers(v: u32) -> u32 {
        v + (1 << 24)
    }

    fn decr_writers(v: u32) -> u32 {
        v - (1 << 24)
    }

    fn offset(v: u32) -> u32 {
        v << 8 >> 8
    }

    fn bump_offset(v: u32, by: u32) -> u32 {
        assert!(by >> 24 == 0);
        v + by
    }

    // check seal
    println!("sealed: {:?}", is_sealed(c1));
    let c2 = mk_sealed(c1);
    println!("sealed: {:?}", is_sealed(c2));
    println!("v: {:#b}", c2);

    println!("writers: {:?}", n_writers(c2));
    let c3 = incr_writers(c2);
    println!("writers: {:?}", n_writers(c3));
    println!("sealed: {:?}", is_sealed(c3));
    println!("v: {:#b}", c3);
    let c4 = incr_writers(incr_writers(incr_writers(c3)));
    println!("writers: {:?}", n_writers(c4));
    println!("sealed: {:?}", is_sealed(c4));
    println!("v: {:#b}", c4);

    // let c5 = bump_offset(c4, 1 << 23);
    let c5 = bump_offset(c4, 13213213);
    println!("writers: {:?}", n_writers(c5));
    println!("sealed: {:?}", is_sealed(c5));
    println!("offset: {:?}", offset(c5));
    println!("v: {:#b}", c5);

    let c6 = decr_writers(decr_writers(decr_writers(decr_writers(c5))));
    println!("writers: {:?}", n_writers(c6));
    println!("sealed: {:?}", is_sealed(c6));
    println!("offset: {:?}", offset(c6));
    println!("v: {:#b}", c6);


}

#[test]
fn logging_works() {
    // write several items to log
    // verify content
}

#[test]
fn paging_works() {
    // create a page
    // write delta kvs to it
    // page out
    // page in
    // verify content
}

#[test]
fn split_works() {
    // write lots
    // verify
}

#[test]
fn merge_delta_works() {
    // write lots of deltas
    // verify that merges took place
}

#[test]
fn txn_works() {
    // perform multicore txns that should conflict
    // verify safety
}

#[test]
fn tree_works() {
    // open
    // set
    // close
    // get
    // verify
}

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

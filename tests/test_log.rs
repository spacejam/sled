#![cfg(test)]
#![allow(unused)]

extern crate quickcheck;
extern crate rand;
extern crate rsdb;

use std::thread;
use std::sync::Arc;

use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};
use rand::Rng;

use rsdb::{Config, HEADER_LEN, LockFreeLog, Log};

#[derive(Debug, Clone)]
enum Op {
    Write(Vec<u8>),
    WriteReservation(Vec<u8>),
    AbortReservation(Vec<u8>),
    Stabilize,
    Complete,
    Abort,
    Restart,
}

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        if g.gen_weighted_bool(2) {
            Op::Abort
        } else {
            Op::Complete
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
        for _ in 0..g.gen_range(1, 100) {
            let op = Op::arbitrary(g);
            ops.push(op);

        }
        OpVec { ops: ops }
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

fn prop_read_stable(ops: OpVec) -> bool {
    use self::Op::*;
    let log = Config::default().log();
    for op in ops.ops.into_iter() {
        match op {
            Write(buf) => {
                log.write(buf);
            }
            WriteReservation(ref buf) => {}
            AbortReservation(ref buf) => {}
            Stabilize => {}
            Complete => {}
            Abort => {}
            Restart => {}
        }
    }
    true
}

#[test]
#[ignore]
fn qc_merge_converges() {
    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 1))
        .tests(10)
        .max_tests(10)
        .quickcheck(prop_read_stable as fn(OpVec) -> bool);
}


#[test]
#[ignore]
fn more_reservations_than_buffers() {
    let log = Config::default().log();
    let mut reservations = vec![];
    for _ in 0..log.config().get_io_bufs() + 1 {
        reservations.push(log.reserve(vec![0; log.config().get_io_buf_size() - HEADER_LEN]))
    }
    for res in reservations.into_iter().rev() {
        // abort in reverse order
        res.abort();
    }
}

#[test]
#[ignore]
fn non_contiguous_flush() {
    let conf = Config::default();
    let log = conf.log();
    let res1 = log.reserve(vec![0; conf.get_io_buf_size() - HEADER_LEN]);
    let res2 = log.reserve(vec![0; conf.get_io_buf_size() - HEADER_LEN]);
    let id = res2.log_id();
    res2.abort();
    res1.abort();
    log.make_stable(id);
}

#[test]
fn concurrent_logging() {
    // TODO linearize res bufs, verify they are correct
    let log = Arc::new(Config::default().log());
    let iobs2 = log.clone();
    let iobs3 = log.clone();
    let iobs4 = log.clone();
    let iobs5 = log.clone();
    let iobs6 = log.clone();
    let log7 = log.clone();
    let t1 = thread::Builder::new()
        .name("c1".to_string())
        .spawn(move || {
            for i in 0..5_000 {
                let buf = vec![1; i % 8192];
                log.write(buf);
            }
        })
        .unwrap();
    let t2 = thread::Builder::new()
        .name("c2".to_string())
        .spawn(move || {
            for i in 0..5_000 {
                let buf = vec![2; i % 8192];
                iobs2.write(buf);
            }
        })
        .unwrap();
    let t3 = thread::Builder::new()
        .name("c3".to_string())
        .spawn(move || {
            for i in 0..5_000 {
                let buf = vec![3; i % 8192];
                iobs3.write(buf);
            }
        })
        .unwrap();
    let t4 = thread::Builder::new()
        .name("c4".to_string())
        .spawn(move || {
            for i in 0..5_000 {
                let buf = vec![4; i % 8192];
                iobs4.write(buf);
            }
        })
        .unwrap();
    let t5 = thread::Builder::new()
        .name("c5".to_string())
        .spawn(move || {
            for i in 0..5_000 {
                let buf = vec![5; i % 8192];
                iobs5.write(buf);
            }
        })
        .unwrap();

    let t6 = thread::Builder::new()
        .name("c6".to_string())
        .spawn(move || {
            for i in 0..5_000 {
                let buf = vec![6; i % 8192];
                let res = iobs6.reserve(buf);
                let id = res.log_id();
                res.complete();
                iobs6.make_stable(id);
            }
        })
        .unwrap();


    t1.join().unwrap();
    t2.join().unwrap();
    t3.join().unwrap();
    t4.join().unwrap();
    t5.join().unwrap();
    t6.join().unwrap();
}

fn test_write(log: &LockFreeLog) {
    let data_bytes = b"yoyoyoyo";
    let res = log.reserve(data_bytes.to_vec());
    let id = res.log_id();
    res.complete();
    log.make_stable(id);
    let read_buf = log.read(id).unwrap().unwrap();
    assert_eq!(read_buf, data_bytes);
}

fn test_abort(log: &LockFreeLog) {
    let res = log.reserve(vec![0; 5]);
    let id = res.log_id();
    res.abort();
    log.make_stable(id);
    match log.read(id) {
        Ok(Err(_)) => (), // good
        _ => {
            panic!("sucessfully read an aborted request! BAD! SAD!");
        }
    }
}

#[test]
fn test_log_aborts() {
    let log = Config::default().log();
    test_write(&log);
    test_abort(&log);
    test_write(&log);
    test_abort(&log);
    test_write(&log);
    test_abort(&log);
}

#[test]
fn test_hole_punching() {
    let log = Config::default().log();

    let data_bytes = b"yoyoyoyo";
    let res = log.reserve(data_bytes.to_vec());
    let id = res.log_id();
    res.complete();
    log.make_stable(id);
    log.read(id).unwrap().unwrap();

    log.punch_hole(id);

    assert_eq!(log.read(id).unwrap(), Err(data_bytes.len()));
}

#[test]
fn test_log_iterator() {
    println!("making stable.");
    let log = Config::default().log();
    let first_offset = log.write(b"1".to_vec());
    log.write(b"22".to_vec());
    log.write(b"333".to_vec());

    // stick an abort in the middle, which should not be returned
    let res = log.reserve(b"never_gonna_hit_disk".to_vec());
    res.abort();

    log.write(b"4444".to_vec());
    let last_offset = log.write(b"55555".to_vec());
    log.make_stable(last_offset);
    let mut iter = log.iter_from(first_offset);
    assert_eq!(iter.next(), Some((0, b"1".to_vec())));
    assert_eq!(iter.next(), Some((8, b"22".to_vec())));
    assert_eq!(iter.next(), Some((17, b"333".to_vec())));
    assert_eq!(iter.next(), Some((54, b"4444".to_vec())));
    assert_eq!(iter.next(), Some((65, b"55555".to_vec())));
    assert_eq!(iter.next(), None);
}

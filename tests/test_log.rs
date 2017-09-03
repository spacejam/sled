#![cfg(test)]
#![allow(unused)]

extern crate quickcheck;
extern crate rand;
extern crate rsdb;

use std::fs;
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};

use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};
use rand::{Rng, thread_rng};

use rsdb::{Config, HEADER_LEN, LockFreeLog, Log, LogRead};

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
        .spawn(move || for i in 0..5_000 {
            let buf = vec![1; i % 8192];
            log.write(buf);
        })
        .unwrap();
    let t2 = thread::Builder::new()
        .name("c2".to_string())
        .spawn(move || for i in 0..5_000 {
            let buf = vec![2; i % 8192];
            iobs2.write(buf);
        })
        .unwrap();
    let t3 = thread::Builder::new()
        .name("c3".to_string())
        .spawn(move || for i in 0..5_000 {
            let buf = vec![3; i % 8192];
            iobs3.write(buf);
        })
        .unwrap();
    let t4 = thread::Builder::new()
        .name("c4".to_string())
        .spawn(move || for i in 0..5_000 {
            let buf = vec![4; i % 8192];
            iobs4.write(buf);
        })
        .unwrap();
    let t5 = thread::Builder::new()
        .name("c5".to_string())
        .spawn(move || for i in 0..5_000 {
            let buf = vec![5; i % 8192];
            iobs5.write(buf);
        })
        .unwrap();

    let t6 = thread::Builder::new()
        .name("c6".to_string())
        .spawn(move || for i in 0..5_000 {
            let buf = vec![6; i % 8192];
            let res = iobs6.reserve(buf);
            let id = res.log_id();
            res.complete();
            iobs6.make_stable(id);
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
        Ok(LogRead::Flush(_, _)) => panic!("sucessfully read an aborted request! BAD! SAD!"),
        _ => (), // good
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

#[cfg(feature = "libc")]
#[cfg(target_os = "linux")]
#[test]
fn test_hole_punching() {
    use std::os::linux::fs::MetadataExt;

    let conf = Config::default();
    let log = conf.log();

    let id1 = log.write(b"first".to_vec());
    let id2 = log.write(b"second".to_vec());

    log.make_stable(id2);

    let blocks_before = {
        let cf = conf.cached_file();
        let f = cf.borrow();
        f.metadata().unwrap().st_blocks();
    };

    assert_eq!(log.read(id1).unwrap().unwrap(), b"first".to_vec());

    let mut iter = log.iter_from(0);
    assert_eq!(iter.next().unwrap().1, b"first".to_vec());
    assert_eq!(iter.next().unwrap().1, b"second".to_vec());
    assert_eq!(iter.next(), None);

    log.punch_hole(id1);

    let mut iter = log.iter_from(0);
    assert_eq!(iter.next().unwrap().1, b"second".to_vec());
    assert_eq!(iter.next(), None);

    assert!(log.read(id1).unwrap().is_zeroed());
}

#[test]
fn test_log_iterator() {
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
    assert_eq!(iter.next().unwrap().1, b"1".to_vec());
    assert_eq!(iter.next().unwrap().1, b"22".to_vec());
    assert_eq!(iter.next().unwrap().1, b"333".to_vec());
    assert_eq!(iter.next().unwrap().1, b"4444".to_vec());
    assert_eq!(iter.next().unwrap().1, b"55555".to_vec());
    assert_eq!(iter.next(), None);
}

#[derive(Debug, Clone)]
enum Op {
    Write(Vec<u8>),
    WriteReservation(Vec<u8>),
    AbortReservation(Vec<u8>),
    Read(u64),
    PunchHole(u64),
    Restart,
}

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        if g.gen_weighted_bool(7) {
            return Op::Restart;
        }

        static COUNTER: AtomicUsize = ATOMIC_USIZE_INIT;

        let mut incr =
            || vec![COUNTER.fetch_add(1, Ordering::Relaxed) as u8; thread_rng().gen_range(0, 2)];

        let choice = g.gen_range(0, 5);

        match choice {
            0 => Op::Write(incr()),
            1 => Op::WriteReservation(incr()),
            2 => Op::AbortReservation(incr()),
            3 => Op::Read(g.gen_range(0, 15)),
            4 => Op::PunchHole(g.gen_range(0, 15)),
            _ => panic!("impossible choice"),
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
        OpVec {
            ops: ops,
        }
    }

    fn shrink(&self) -> Box<Iterator<Item = OpVec>> {
        let mut smaller = vec![];
        for i in 0..self.ops.len() {
            let mut clone = self.clone();
            clone.ops.remove(i);
            smaller.push(clone);
        }

        // reduce write sizes
        let mut clone = self.clone();
        let mut lowered = false;
        for mut op in clone.ops.iter_mut() {
            match *op {
                Op::WriteReservation(ref mut buf) |
                Op::AbortReservation(ref mut buf) |
                Op::Write(ref mut buf) if buf.len() > 0 => {
                    buf.clear();
                }
                _ => {}
            }
        }
        if lowered {
            smaller.push(clone);
        }

        // shrink read offsets
        let mut clone = self.clone();
        let mut lowered = false;
        for mut op in clone.ops.iter_mut() {
            match *op {
                Op::Read(ref mut lid) if *lid > 0 => {
                    lowered = true;
                    *lid -= 1;
                }
                _ => {}
            }
        }
        if lowered {
            smaller.push(clone);
        }

        Box::new(smaller.into_iter())
    }
}

fn prop_log_works(ops: OpVec) -> bool {
    use self::Op::*;
    let config = Config::default();

    let mut log = config.log();
    let mut reference: Vec<(u64, Option<Vec<u8>>)> = vec![];

    for op in ops.ops.into_iter() {
        match op {
            PunchHole(lid) => {
                if reference.len() <= lid as usize {
                    continue;
                }
                {
                    let (lid, ref mut expected) = reference[lid as usize];
                    log.make_stable(lid);
                    if expected.take().is_some() {
                        log.punch_hole(lid);
                    }
                }
                if lid > 0 {
                    // we want to remove this record if the previous record is
                    // also zeroed, as during reads, zeroed records become
                    // indistinguishable.
                    let should_remove = {
                        let (lid, ref expected) = reference[(lid - 1) as usize];
                        if expected.is_none() { true } else { false }
                    };
                    if should_remove {
                        reference.remove(lid as usize);
                    }
                }
            }
            Read(lid) => {
                if reference.len() <= lid as usize {
                    continue;
                }
                let (lid, ref expected) = reference[lid as usize];
                log.make_stable(lid);
                let read = log.read(lid).unwrap();
                if expected.is_none() {
                    assert!(read.is_zeroed());
                } else {
                    let flush = read.flush();
                    assert_eq!(flush, *expected);
                }
            }
            Write(buf) => {
                let lid = log.write(buf.clone());
                reference.push((lid, Some(buf)));
            }
            WriteReservation(buf) => {
                let lid = log.reserve(buf.clone()).complete();
                reference.push((lid, Some(buf)));
            }
            AbortReservation(buf) => {
                let lid = log.reserve(buf).abort();
                reference.push((lid, None));
            }
            Restart => {
                drop(log);
                log = config.log();
            }
        }
    }
    true
}

#[test]
fn quickcheck_log_works() {
    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 1))
        .tests(50)
        .max_tests(100)
        .quickcheck(prop_log_works as fn(OpVec) -> bool);
}

#[test]
fn test_log_bug_1() {
    // postmortem: test was not stabilizing its buffers before reading
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![
            AbortReservation(vec![]),
            WriteReservation(vec![34]),
            Write(vec![35]),
            WriteReservation(vec![36]),
            Read(3),
        ],
    });
}

#[test]
fn test_log_bug_2() {
    // postmortem: the logic that scans through zeroed entries
    // until the next readable byte hit the end of the file.
    // fix: when something hits the end of the file, return
    // LogRead::Zeroed.
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![AbortReservation(vec![46]), Read(0)],
    });
}

#[test]
fn test_log_bug_3() {
    // postmortem: the skip-ahead logic for continuing to
    // the next valid log entry when reading a zeroed entry
    // was being triggered on valid entries of size 0.
    // fix: don't skip-ahead for empty valid entries.
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![Write(vec![]), Read(0)],
    });
}

#[test]
fn test_log_bug_4() {
    // postmortem: was calling punch_hole on lids that
    // were still only present in the log's IO buffer
    // fix: ensure that callers of punch_hole first
    // call make_stable.
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![Write(vec![]), PunchHole(0)],
    });
}

#[test]
fn test_log_bug_5() {
    // postmortem: on non-linux systems, punch_hole
    // was not zeroing out the bytes in a record.
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![
            Write(vec![]),
            PunchHole(0),
            Write(vec![44]),
            PunchHole(1),
            Read(0),
        ],
    });
}

fn _test_log_bug_() {
    // postmortem: TEMPLATE
    // use Op::*;
    prop_log_works(OpVec {
        ops: vec![],
    });
}

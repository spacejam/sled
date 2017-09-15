#![cfg(test)]
#![allow(unused)]

extern crate quickcheck;
extern crate rand;
extern crate sled;

#[cfg(target_os = "linux")]
extern crate libc;

use std::fs;
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};

use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};
use rand::{Rng, thread_rng};

use sled::{Config, HEADER_LEN, LockFreeLog, Log, LogRead};

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

#[test]
fn test_log_iterator() {
    let conf = Config::default();
    let log = conf.log();
    let first_offset = log.write(b"".to_vec());
    log.write(b"1".to_vec());
    log.write(b"22".to_vec());
    log.write(b"333".to_vec());

    // stick an abort in the middle, which should not be returned
    {
        let res = log.reserve(b"never_gonna_hit_disk".to_vec());
        res.abort();
    }

    log.write(b"4444".to_vec());
    let last_offset = log.write(b"55555".to_vec());
    log.make_stable(last_offset);

    drop(log);

    let log = conf.log();

    let mut iter = log.iter_from(first_offset);
    assert_eq!(iter.next().unwrap().2, b"".to_vec());
    assert_eq!(iter.next().unwrap().2, b"1".to_vec());
    assert_eq!(iter.next().unwrap().2, b"22".to_vec());
    assert_eq!(iter.next().unwrap().2, b"333".to_vec());
    assert_eq!(iter.next().unwrap().2, b"4444".to_vec());
    assert_eq!(iter.next().unwrap().2, b"55555".to_vec());
    assert_eq!(iter.next(), None);
}

#[derive(Debug, Clone)]
enum Op {
    Write(Vec<u8>),
    WriteReservation(Vec<u8>),
    AbortReservation(Vec<u8>),
    Read(u64),
    Restart,
    Truncate(u64),
}

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        static COUNTER: AtomicUsize = ATOMIC_USIZE_INIT;
        static LEN: AtomicUsize = ATOMIC_USIZE_INIT;

        let mut incr = || {
            let len = thread_rng().gen_range(0, 2);
            LEN.fetch_add(len + HEADER_LEN, Ordering::Relaxed);
            vec![COUNTER.fetch_add(1, Ordering::Relaxed) as u8; len]
        };

        if g.gen_weighted_bool(7) {
            return Op::Restart;
        }

        if g.gen_weighted_bool(50) {
            let len = LEN.load(Ordering::Relaxed);
            return Op::Truncate(thread_rng().gen_range(0, len as u64));
        }

        let choice = g.gen_range(0, 4);

        match choice {
            0 => Op::Write(incr()),
            1 => Op::WriteReservation(incr()),
            2 => Op::AbortReservation(incr()),
            3 => Op::Read(g.gen_range(0, 15)),
            _ => panic!("impossible choice"),
        }
    }
}

#[test]
#[ignore]
fn test_segments_connect() {}

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

        // shrink offsets
        let mut clone = self.clone();
        let mut lowered = false;
        for mut op in clone.ops.iter_mut() {
            match *op {
                Op::Read(ref mut lid) if *lid > 0 => {
                    lowered = true;
                    *lid -= 1;
                }
                Op::Truncate(ref mut len) if *len > 0 => {
                    lowered = true;
                    *len -= 1;
                }
                _ => {}
            }
        }
        if lowered {
            smaller.push(clone);
        }

        // drop elements
        for i in 0..self.ops.len() {
            let mut clone = self.clone();
            clone.ops.remove(i);
            smaller.push(clone);
        }

        Box::new(smaller.into_iter())
    }
}

fn prop_log_works(ops: OpVec) -> bool {
    use self::Op::*;
    let config = Config::default().io_buf_size(1024 * 8);

    let mut tip = 0;
    let mut log = config.log();
    let mut reference: Vec<(u64, Option<Vec<u8>>, usize)> = vec![];

    for op in ops.ops.into_iter() {
        match op {
            Read(lid) => {
                if reference.len() <= lid as usize {
                    continue;
                }
                let (lid, ref expected, _len) = reference[lid as usize];
                log.flush();
                let read_res = log.read(lid);
                if expected.is_none() || tip as u64 <= lid {
                    assert!(read_res.is_err() || !read_res.unwrap().is_flush());
                } else {
                    let flush = read_res.unwrap().flush();
                    assert_eq!(flush, *expected);
                }
            }
            Write(buf) => {
                let lid = log.write(buf.clone());
                let len = buf.len();
                tip = lid as usize + len + HEADER_LEN;
                reference.push((lid, Some(buf), len));
            }
            WriteReservation(buf) => {
                let lid = log.reserve(buf.clone()).complete();
                let len = buf.len();
                tip = lid as usize + len + HEADER_LEN;
                reference.push((lid, Some(buf), len));
            }
            AbortReservation(buf) => {
                let len = buf.len();
                let lid = log.reserve(buf).abort();
                tip = lid as usize + len + HEADER_LEN;
                reference.push((lid, None, len));
            }
            Restart => {
                drop(log);
                log = config.log();
            }
            Truncate(new_len) => {
                if tip as u64 <= new_len {
                    // we are testing data loss, not rogue extensions
                    continue;
                }

                drop(log);

                #[cfg(target_os = "linux")]
                {
                    let path = config.get_path();

                    let mut sz_total = 0;
                    for &mut (lid, ref mut expected, sz) in &mut reference {
                        let tip = lid as usize + sz + HEADER_LEN;
                        if new_len < tip as u64 {
                            *expected = None;
                        }
                    }

                    use std::os::unix::io::AsRawFd;

                    let cached_f = config.cached_file();
                    let f = cached_f.borrow_mut();
                    use libc::ftruncate;
                    let fd = f.as_raw_fd();

                    unsafe {
                        ftruncate(fd, new_len as i64);
                    }
                }

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
        .max_tests(1000)
        .quickcheck(prop_log_works as fn(OpVec) -> bool);
}

#[test]
fn test_log_bug_01() {
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
fn test_log_bug_7() {
    // postmortem: test alignment
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![
            WriteReservation(vec![]),
            WriteReservation(vec![]),
            AbortReservation(vec![]),
            Write(vec![12]),
            AbortReservation(vec![]),
            Truncate(24),
            WriteReservation(vec![19]),
            Write(vec![20]),
            Read(4),
        ],
    });
}

#[test]
fn test_log_bug_9() {
    // postmortem: test was mishandling truncation, which should test loss, not extension
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![
            Truncate(99),
            WriteReservation(vec![234]),
            Truncate(8),
            Read(0),
        ],
    });
}

#[test]
fn test_log_bug_10() {
    // postmortem: test alignment
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![
            Write(vec![231]),
            AbortReservation(vec![232]),
            WriteReservation(vec![235]),
            AbortReservation(vec![236]),
            WriteReservation(vec![]),
            Write(vec![]),
            WriteReservation(vec![]),
            AbortReservation(vec![240]),
            Truncate(14),
            AbortReservation(vec![242]),
            WriteReservation(vec![243]),
            Write(vec![245]),
            Write(vec![]),
            Write(vec![249]),
            WriteReservation(vec![250]),
            Read(7),
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

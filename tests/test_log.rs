#![cfg_attr(test, allow(unused))]

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

use sled::{Config, Log, LogRead, MSG_HEADER_LEN, SEG_HEADER_LEN,
           SEG_TRAILER_LEN, SegmentMode};

type Lsn = isize;
type LogID = u64;


#[test]
#[ignore]
fn more_log_reservations_than_buffers() {
    let config = Config::default().segment_mode(SegmentMode::Linear).build();
    let log = config.log();
    let mut reservations = vec![];
    for _ in 0..config.get_io_bufs() + 1 {
        reservations.push(log.reserve(
            vec![0; config.get_io_buf_size() - MSG_HEADER_LEN],
        ))
    }
    for res in reservations.into_iter().rev() {
        // abort in reverse order
        res.abort();
    }
}

#[test]
fn non_contiguous_log_flush() {
    let conf = Config::default()
        .segment_mode(SegmentMode::Linear)
        .io_buf_size(1000)
        .build();
    let log = conf.log();

    let seg_overhead = SEG_HEADER_LEN + SEG_TRAILER_LEN;
    let buf_len = ((conf.get_io_buf_size() - seg_overhead) /
                       conf.get_min_items_per_segment()) -
        MSG_HEADER_LEN;

    let res1 = log.reserve(vec![0; buf_len]);
    let res2 = log.reserve(vec![0; buf_len]);
    let id = res2.lid();
    let lsn = res2.lsn();
    res2.abort();
    res1.abort();
    log.make_stable(lsn);
}

#[test]
fn concurrent_logging() {
    // TODO linearize res bufs, verify they are correct
    for i in 0..10 {
        let conf = Config::default()
            .segment_mode(SegmentMode::Linear)
            .io_buf_size(1000)
            .flush_every_ms(Some(50))
            .build();
        let log = Arc::new(conf.log());
        let iobs2 = log.clone();
        let iobs3 = log.clone();
        let iobs4 = log.clone();
        let iobs5 = log.clone();
        let iobs6 = log.clone();
        let log7 = log.clone();

        let seg_overhead = SEG_HEADER_LEN + SEG_TRAILER_LEN;
        let buf_len = ((conf.get_io_buf_size() - seg_overhead) /
                           conf.get_min_items_per_segment()) -
            MSG_HEADER_LEN;

        let t1 = thread::Builder::new()
            .name("c1".to_string())
            .spawn(move || for i in 0..1_000 {
                let buf = vec![1; i % buf_len];
                log.write(buf);
            })
            .unwrap();

        let t2 = thread::Builder::new()
            .name("c2".to_string())
            .spawn(move || for i in 0..1_000 {
                let buf = vec![2; i % buf_len];
                iobs2.write(buf);
            })
            .unwrap();

        let t3 = thread::Builder::new()
            .name("c3".to_string())
            .spawn(move || for i in 0..1_000 {
                let buf = vec![3; i % buf_len];
                iobs3.write(buf);
            })
            .unwrap();

        let t4 = thread::Builder::new()
            .name("c4".to_string())
            .spawn(move || for i in 0..1_000 {
                let buf = vec![4; i % buf_len];
                iobs4.write(buf);
            })
            .unwrap();
        let t5 = thread::Builder::new()
            .name("c5".to_string())
            .spawn(move || for i in 0..1_000 {
                let buf = vec![5; i % buf_len];
                iobs5.write(buf);
            })
            .unwrap();

        let t6 = thread::Builder::new()
            .name("c6".to_string())
            .spawn(move || for i in 0..1_000 {
                let buf = vec![6; i % buf_len];
                let (lsn, _lid) = iobs6.write(buf);
                // println!("+");
                iobs6.make_stable(lsn);
                // println!("-");
            })
            .unwrap();

        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();
        t4.join().unwrap();
        t5.join().unwrap();
        t6.join().unwrap();
    }
}

fn write(log: &Log) {
    let data_bytes = b"yoyoyoyo";
    let (lsn, lid) = log.write(data_bytes.to_vec());
    let (_, read_buf, _) = log.read(lsn, lid).unwrap().unwrap();
    assert_eq!(read_buf, data_bytes);
}

fn abort(log: &Log) {
    let res = log.reserve(vec![0; 5]);
    let (lsn, lid) = res.abort();
    match log.read(lsn, lid) {
        Ok(LogRead::Flush(_, _, _)) => {
            panic!("sucessfully read an aborted request! BAD! SAD!")
        }
        _ => (), // good
    }
}

#[test]
fn log_aborts() {
    let log = Config::default().log();
    write(&log);
    abort(&log);
    write(&log);
    abort(&log);
    write(&log);
    abort(&log);
}

#[test]
fn log_iterator() {
    let conf = Config::default()
        .segment_mode(SegmentMode::Linear)
        .io_buf_size(1000)
        .build();
    let log = conf.log();
    let (first_lsn, _) = log.write(b"".to_vec());
    log.write(b"1".to_vec());
    log.write(b"22".to_vec());
    log.write(b"333".to_vec());

    // stick an abort in the middle, which should not be
    // returned
    {
        let res = log.reserve(b"never_gonna_hit_disk".to_vec());
        res.abort();
    }

    log.write(b"4444".to_vec());
    let (last_lsn, _) = log.write(b"55555".to_vec());
    log.make_stable(last_lsn);

    drop(log);

    let log = conf.log();

    let mut iter = log.iter_from(first_lsn);
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

        // ensure len never is loaded at 0, as it will crash
        // the call to gen_range below if low >= high
        LEN.compare_and_swap(0, 1, Ordering::SeqCst);

        let mut incr = || {
            let len = thread_rng().gen_range(0, 2);
            LEN.fetch_add(len + MSG_HEADER_LEN, Ordering::Relaxed);
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
fn snapshot_with_out_of_order_buffers() {
    let conf = Config::default()
        .segment_mode(SegmentMode::Linear)
        .io_buf_size(100)
        .io_bufs(2)
        .snapshot_after_ops(5)
        .build();

    let len = conf.get_io_buf_size() - SEG_HEADER_LEN - SEG_TRAILER_LEN -
        MSG_HEADER_LEN;

    let log = conf.log();

    for i in 0..4 {
        let buf = vec![i as u8; len];
        let (lsn, _lid) = log.write(buf);
        log.make_stable(lsn);
    }

    {
        // erase the third segment trailer to represent
        // an out-of-order segment write before
    }

    drop(log);

    let log = conf.log();

    // start iterating just past the first segment header
    let mut iter = log.iter_from(SEG_HEADER_LEN as Lsn);

    for i in 0..conf.get_io_bufs() * 2 {
        let expected = vec![i as u8; len];
        let (_lsn, _lid, buf) = iter.next().unwrap();
        assert_eq!(expected, buf);
    }
}

#[test]
fn multi_segment_log_iteration() {
    // ensure segments are being linked
    // ensure trailers are valid
    let conf = Config::default()
        .segment_mode(SegmentMode::Linear)
        .io_buf_size(1000)
        .build();

    let seg_overhead = SEG_HEADER_LEN + SEG_TRAILER_LEN;
    let len = ((conf.get_io_buf_size() - seg_overhead) /
                   conf.get_min_items_per_segment()) -
        MSG_HEADER_LEN;

    let log = conf.log();

    for i in 0..conf.get_io_bufs() * 2 {
        let buf = vec![i as u8; len];
        log.write(buf);
    }

    drop(log);

    let log = conf.log();

    // start iterating just past the first segment header
    let mut iter = log.iter_from(SEG_HEADER_LEN as Lsn);

    for i in 0..conf.get_io_bufs() * 2 {
        let expected = vec![i as u8; len];
        let (_lsn, _lid, buf) = iter.next().unwrap();
        assert_eq!(expected, buf);
    }
}

#[derive(Debug, Clone)]
struct OpVec {
    ops: Vec<Op>,
}

impl Arbitrary for OpVec {
    fn arbitrary<G: Gen>(g: &mut G) -> OpVec {
        let mut ops = vec![];
        for _ in 0..g.gen_range(1, 50) {
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
    let config = Config::default()
        .io_buf_size(1024 * 8)
        .flush_every_ms(Some(1))
        // .flush_every_ms(None) // FIXME rm line
        .segment_mode(SegmentMode::Linear)
        .build();
    // println!("testing {:?}", ops);

    let mut tip = 0;
    let mut log = config.log();
    let mut reference: Vec<(Lsn, LogID, Option<Vec<u8>>, usize)> = vec![];

    for op in ops.ops.into_iter() {
        match op {
            Read(lid) => {
                if reference.len() <= lid as usize {
                    continue;
                }
                let (lsn, lid, ref expected, _len) = reference[lid as usize];
                // log.make_stable(lid);
                let read_res = log.read(lsn, lid);
                // println!( "expected {:?} read_res {:?} tip {} lid {}", expected, read_res, tip, lid);
                if expected.is_none() || tip as u64 <= lid {
                    assert!(read_res.is_err() || !read_res.unwrap().is_flush());
                } else {
                    let flush = read_res.unwrap().flush().map(|f| f.1);
                    assert_eq!(flush, *expected);
                }
            }
            Write(buf) => {
                let len = buf.len();
                let (lsn, lid) = log.write(buf.clone());
                tip = lid as usize + len + MSG_HEADER_LEN;
                reference.push((lsn, lid, Some(buf), len));
            }
            WriteReservation(buf) => {
                let len = buf.len();
                let (lsn, lid) = log.write(buf.clone());
                tip = lid as usize + len + MSG_HEADER_LEN;
                reference.push((lsn, lid, Some(buf), len));
            }
            AbortReservation(buf) => {
                let len = buf.len();
                let res = log.reserve(buf.clone());
                let lsn = res.lsn();
                let lid = res.lid();
                res.abort();
                tip = lid as usize + len + MSG_HEADER_LEN;
                reference.push((lsn, lid, None, len));
            }
            Restart => {
                drop(log);

                // on recovery, we will rewind over any aborted tip entries
                while !reference.is_empty() {
                    let should_pop = if reference.last().unwrap().2.is_none() {
                        true
                    } else {
                        false
                    };
                    if should_pop {
                        reference.pop();
                    } else {
                        break;
                    }
                }
                log = config.log();
            }
            Truncate(new_len) => {
                #[cfg(target_os = "linux")]
                {
                    if tip as u64 <= new_len {
                        // we are testing data loss, not rogue extensions
                        continue;
                    }

                    tip = new_len as usize;

                    drop(log);

                    let path = config.get_path();

                    let mut sz_total = 0;
                    for &mut (lsn, lid, ref mut expected, sz) in
                        &mut reference
                    {
                        let tip = lid as usize + sz + MSG_HEADER_LEN;
                        if new_len < tip as u64 {
                            *expected = None;
                        }
                    }

                    while !reference.is_empty() {
                        if reference.last().unwrap().2.is_none() {
                            reference.pop();
                        } else {
                            break;
                        }
                    }

                    use std::os::unix::io::AsRawFd;

                    {
                        let f = config.file();
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
    }
    true
}

#[test]
fn quickcheck_log_works() {
    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 1))
        .tests(1000)
        .max_tests(10000)
        .quickcheck(prop_log_works as fn(OpVec) -> bool);
}

#[test]
fn log_bug_01() {
    // postmortem: test was not stabilizing its buffers before
    // reading
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
fn log_bug_2() {
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
fn log_bug_3() {
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
fn log_bug_7() {
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
fn log_bug_9() {
    // postmortem: test was mishandling truncation, which
    // should test loss, not extension
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
fn log_bug_10() {
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

#[test]
fn log_bug_11() {
    // postmortem: was miscalculating the initial disk offset
    // on startup
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![Write(vec![]), Restart],
    });
}

#[test]
fn log_bug_12() {
    // postmortem: after a torn page is encountered, we were
    // just reusing the tip as the next start location. this
    // breaks recovery. fix: traverse the segment until we
    // encounter the last valid entry.
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![
            Write(vec![]),
            Truncate(20),
            AbortReservation(vec![]),
            Read(0),
        ],
    });
}

#[test]
fn log_bug_13() {
    // postmortem: was not recording the proper highest lsn on recovery.
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![
            WriteReservation(vec![35]),
            Restart,
            AbortReservation(vec![36]),
            Read(0),
        ],
    });
}

#[test]
fn log_bug_14a() {
    // postmortem: was not simulating the "rewind" behavior of the
    // log to replace aborted flushes at the log tip properly.
    // postmortem 2:
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![AbortReservation(vec![12]), Restart, Write(vec![]), Read(0)],
    });
}

#[test]
fn log_bug_14b() {
    // postmortem: config was being improperly copied, causing temp
    // files to be deleted while dangling refs persisted, causing
    // the original data file to be unlinked and then divergent file
    // descriptors ended up being used. resulted in weird stuff like
    // reads not returning recent writes. Lesson: don't go around
    // FinalConfig!
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![Restart, Write(vec![]), Read(0)],
    });
}

#[test]
fn log_bug_15() {
    // postmortem: was not properly clearing previously
    // overwritten writes during truncation
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![Write(vec![]), Truncate(0), Write(vec![]), Read(0)],
    });
}

#[test]
fn log_bug_16() {
    // postmortem: a bug in recovery created by the massive log overhaul
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![
            Write(vec![189]),
            Truncate(1),
            Write(vec![206]),
            Restart,
            Write(vec![208]),
            Read(0),
        ],
    });
}

#[test]
fn log_bug_17() {
    // postmortem: this was a transient failure caused by failing to stabilize
    // an update before later reading it.
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![
            Write(vec![]),
            Read(7),
            Write(vec![81]),
            Read(14),
            Read(9),
            Read(14),
            Read(0),
            Read(8),
            Write(vec![82]),
            Read(6),
        ],
    });
}

#[test]
fn log_bug_18() {
    // postmortem: this was a recovery bug
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![Restart],
    });
}

#[test]
fn log_bug_19() {
    // postmortem: this was stalling in make_stable
    // postmortem 2: SA recovery skipped the first segment because we
    // were not properly adding the empty tip to the free list.
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![
            Restart,
            Restart,
            Write(vec![]),
            AbortReservation(vec![]),
            Write(vec![47]),
            Restart,
            Read(2),
        ],
    });
}

#[test]
fn log_bug_20() {
    // postmortem: message header length was not being included when
    // calculating the starting log offsets.
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![
            WriteReservation(vec![]),
            Restart,
            WriteReservation(vec![2]),
            Read(1),
        ],
    });
}

#[test]
fn log_bug_21() {
    // postmortem: message header length was not being included when
    // calculating the starting log offsets.
    use Op::*;
    prop_log_works(OpVec {
        ops: vec![
            WriteReservation(vec![1]),
            Restart,
            WriteReservation(vec![2]),
            Read(1),
        ],
    });
}

fn _log_bug_() {
    // postmortem: TEMPLATE
    // use Op::*;
    prop_log_works(OpVec {
        ops: vec![],
    });
}

mod common;

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};
use rand::{thread_rng, Rng};

use sled::*;

use sled::{
    pin, BatchManifest, DiskPtr, Log, LogKind, LogRead, Lsn, PageId,
    SegmentMode, Serialize, MAX_MSG_HEADER_LEN, MINIMUM_ITEMS_PER_SEGMENT,
    SEG_HEADER_LEN,
};

const PID: PageId = 0;
const REPLACE: LogKind = LogKind::Replace;

#[test]
fn log_writebatch() -> crate::Result<()> {
    common::setup_logger();
    let config =
        Config::new().temporary(true).segment_mode(SegmentMode::Linear);
    let log = config.open_raw_log().unwrap();

    let guard = pin();

    log.reserve(REPLACE, PID, &IVec::from(b"1"), &guard)?.complete()?;
    log.reserve(REPLACE, PID, &IVec::from(b"2"), &guard)?.complete()?;
    log.reserve(REPLACE, PID, &IVec::from(b"3"), &guard)?.complete()?;
    log.reserve(REPLACE, PID, &IVec::from(b"4"), &guard)?.complete()?;
    log.reserve(REPLACE, PID, &IVec::from(b"5"), &guard)?.complete()?;

    // simulate a torn batch by
    // writing an LSN higher than
    // is possible to recover into
    // a batch manifest before
    // some writes.
    let mut batch_res =
        log.reserve(REPLACE, PID, &BatchManifest::default(), &guard)?;
    log.reserve(REPLACE, PID, &IVec::from(b"6"), &guard)?.complete()?;
    log.reserve(REPLACE, PID, &IVec::from(b"7"), &guard)?.complete()?;
    log.reserve(REPLACE, PID, &IVec::from(b"8"), &guard)?.complete()?;
    log.reserve(REPLACE, PID, &IVec::from(b"9"), &guard)?.complete()?;
    batch_res.mark_writebatch(Lsn::max_value() / 2);
    batch_res.complete()?;
    log.reserve(REPLACE, PID, &IVec::from(b"10"), &guard)?.complete()?;

    drop(log);
    let log = config.open_raw_log().unwrap();

    let mut iter = log.iter_from(0);

    assert!(iter.next().is_some());
    assert!(iter.next().is_some());
    assert!(iter.next().is_some());
    assert!(iter.next().is_some());
    assert!(iter.next().is_some());
    assert_eq!(iter.next(), None);

    Ok(())
}

#[test]
fn more_log_reservations_than_buffers() -> Result<()> {
    let config = Config::new()
        .temporary(true)
        .segment_mode(SegmentMode::Linear)
        .segment_size(128);

    let log = config.open_raw_log().unwrap();
    let mut reservations = vec![];

    let total_seg_overhead = SEG_HEADER_LEN;
    let big_msg_overhead = MAX_MSG_HEADER_LEN + total_seg_overhead;
    let big_msg_sz = config.segment_size - big_msg_overhead;

    for _ in 0..=30 {
        let guard = pin();
        reservations.push(
            log.reserve(REPLACE, PID, &IVec::from(vec![0; big_msg_sz]), &guard)
                .unwrap(),
        )
    }
    for res in reservations.into_iter().rev() {
        // abort in reverse order
        res.abort()?;
    }

    log.flush()?;
    Ok(())
}

#[test]
fn non_contiguous_log_flush() -> Result<()> {
    let config = Config::new()
        .temporary(true)
        .segment_mode(SegmentMode::Linear)
        .segment_size(1024);

    let log = config.open_raw_log().unwrap();

    let seg_overhead = SEG_HEADER_LEN;
    let buf_len = (config.segment_size / MINIMUM_ITEMS_PER_SEGMENT)
        - (MAX_MSG_HEADER_LEN + seg_overhead);

    let guard = pin();
    let res1 = log
        .reserve(REPLACE, PID, &IVec::from(vec![0; buf_len]), &guard)
        .unwrap();
    let res2 = log
        .reserve(REPLACE, PID, &IVec::from(vec![0; buf_len]), &guard)
        .unwrap();
    let lsn = res2.lsn();
    res2.abort()?;
    res1.abort()?;
    log.make_stable(lsn)?;
    Ok(())
}

#[test]
fn concurrent_logging() {
    common::setup_logger();
    // TODO linearize res bufs, verify they are correct
    for _ in 0..10 {
        let config = Config::new()
            .temporary(true)
            .segment_mode(SegmentMode::Linear)
            .flush_every_ms(Some(50))
            .segment_size(256);

        let log = Arc::new(config.open_raw_log().unwrap());
        let iobs2 = log.clone();
        let iobs3 = log.clone();
        let iobs4 = log.clone();
        let iobs5 = log.clone();
        let iobs6 = log.clone();

        let seg_overhead = SEG_HEADER_LEN;
        let buf_len = (config.segment_size / MINIMUM_ITEMS_PER_SEGMENT)
            - (MAX_MSG_HEADER_LEN + seg_overhead);

        let t1 = thread::Builder::new()
            .name("c1".to_string())
            .spawn(move || {
                for i in 0..1_000 {
                    let buf = IVec::from(vec![1; i % buf_len]);
                    let guard = pin();
                    log.reserve(REPLACE, PID, &buf, &guard)
                        .unwrap()
                        .complete()
                        .unwrap();
                }
            })
            .unwrap();

        let t2 = thread::Builder::new()
            .name("c2".to_string())
            .spawn(move || {
                for i in 0..1_000 {
                    let buf = IVec::from(vec![2; i % buf_len]);
                    let guard = pin();
                    iobs2
                        .reserve(REPLACE, PID, &buf, &guard)
                        .unwrap()
                        .complete()
                        .unwrap();
                }
            })
            .unwrap();

        let t3 = thread::Builder::new()
            .name("c3".to_string())
            .spawn(move || {
                for i in 0..1_000 {
                    let buf = IVec::from(vec![3; i % buf_len]);
                    let guard = pin();
                    iobs3
                        .reserve(REPLACE, PID, &buf, &guard)
                        .unwrap()
                        .complete()
                        .unwrap();
                }
            })
            .unwrap();

        let t4 = thread::Builder::new()
            .name("c4".to_string())
            .spawn(move || {
                for i in 0..1_000 {
                    let buf = IVec::from(vec![4; i % buf_len]);
                    let guard = pin();
                    iobs4
                        .reserve(REPLACE, PID, &buf, &guard)
                        .unwrap()
                        .complete()
                        .unwrap();
                }
            })
            .unwrap();
        let t5 = thread::Builder::new()
            .name("c5".to_string())
            .spawn(move || {
                for i in 0..1_000 {
                    let guard = pin();
                    let buf = IVec::from(vec![5; i % buf_len]);
                    iobs5
                        .reserve(REPLACE, PID, &buf, &guard)
                        .unwrap()
                        .complete()
                        .unwrap();
                }
            })
            .unwrap();

        let t6 = thread::Builder::new()
            .name("c6".to_string())
            .spawn(move || {
                for i in 0..1_000 {
                    let buf = IVec::from(vec![6; i % buf_len]);
                    let guard = pin();
                    let (lsn, _lid) = iobs6
                        .reserve(REPLACE, PID, &buf, &guard)
                        .unwrap()
                        .complete()
                        .unwrap();
                    iobs6.make_stable(lsn).unwrap();
                }
            })
            .unwrap();

        t6.join().unwrap();
        t5.join().unwrap();
        t4.join().unwrap();
        t3.join().unwrap();
        t2.join().unwrap();
        t1.join().unwrap();
    }
}

#[test]
fn concurrent_log_404() {
    common::setup_logger();

    let config = Config::new()
        .temporary(true)
        .segment_mode(SegmentMode::Linear)
        .flush_every_ms(Some(50))
        .segment_size(1024);

    let log_arc = Arc::new(config.open_raw_log().unwrap());

    const ITERATIONS: i32 = 5000;
    const NUM_THREADS: i32 = 6;

    static SHARED_COUNTER: AtomicUsize = AtomicUsize::new(0);
    let mut handles = Vec::new();
    for t in 0..NUM_THREADS {
        let log = log_arc.clone();
        let h = thread::Builder::new()
            .name(format!("t_{}", t))
            .spawn(move || {
                for _ in 0..ITERATIONS {
                    let current = SHARED_COUNTER.load(Ordering::SeqCst);
                    let raw_value = current as u64;
                    let guard = pin();
                    let res = log
                        .reserve(REPLACE, current as PageId, &raw_value, &guard)
                        .unwrap();
                    match SHARED_COUNTER.compare_and_swap(
                        current,
                        current + 1,
                        Ordering::SeqCst,
                    ) {
                        // If the current value was returned, then CAS succeeded
                        // on AtomicUsize
                        c if c == current => {
                            res.complete().expect("reservation complete panic");
                        }
                        // Any other value is an error
                        _ => {
                            res.abort().expect("reservation abort panic");
                        }
                    }
                }
            })
            .unwrap();
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap();
    }

    drop(log_arc);

    let log = config.open_raw_log().unwrap();
    let mut iter = log.iter_from(SEG_HEADER_LEN as Lsn);
    let successfuls = SHARED_COUNTER.load(Ordering::SeqCst);
    for i in 0..successfuls {
        let (kind, pid, lsn, ptr, _sz) = iter.next().unwrap_or_else(|| {
            panic!(
                "expected {} messages, but failed to read number {}",
                successfuls,
                i - 1
            )
        });

        let msg = log.read(pid, lsn, ptr).unwrap().into_data().unwrap();
        assert_eq!(&msg, &(i as u64).serialize());
        assert_eq!(kind, LogKind::Replace);
    }
    // Assert that there is nothing left in the log.
    assert_eq!(iter.next(), None);
}

fn write(log: &Log) {
    let data_bytes = IVec::from(b"yoyoyoyo");
    let guard = pin();
    let (lsn, ptr) = log
        .reserve(REPLACE, PID, &data_bytes, &guard)
        .unwrap()
        .complete()
        .unwrap();
    let read_buf = log.read(PID, lsn, ptr).unwrap().into_data().unwrap();
    assert_eq!(
        *read_buf,
        *data_bytes.serialize(),
        "after writing data, it should be readable"
    );
}

fn abort(log: &Log) {
    let guard = pin();
    let res = log.reserve(REPLACE, PID, &IVec::from(&[0; 5]), &guard).unwrap();
    let (lsn, ptr) = res.abort().unwrap();
    match log.read(PID, lsn, ptr) {
        Ok(LogRead::Canceled(_)) => {}
        other => {
            panic!(
                "expected to successfully read \
                 aborted log message, instead read {:?}",
                other
            );
        }
    }
}

#[test]
fn log_aborts() {
    let config =
        Config::new().temporary(true).segment_mode(SegmentMode::Linear);
    let log = config.open_raw_log().unwrap();
    write(&log);
    abort(&log);
    write(&log);
    abort(&log);
    write(&log);
    abort(&log);
}

#[test]
fn log_iterator() {
    let config = Config::new()
        .temporary(true)
        .segment_mode(SegmentMode::Linear)
        .segment_size(1024);

    let log = config.open_raw_log().unwrap();
    let guard = pin();
    let (first_lsn, _) = log
        .reserve(REPLACE, PID, &IVec::from(b""), &guard)
        .unwrap()
        .complete()
        .unwrap();
    log.reserve(REPLACE, PID, &IVec::from(b"1"), &guard)
        .unwrap()
        .complete()
        .unwrap();
    log.reserve(REPLACE, PID, &IVec::from(b"22"), &guard)
        .unwrap()
        .complete()
        .unwrap();
    log.reserve(REPLACE, PID, &IVec::from(b"333"), &guard)
        .unwrap()
        .complete()
        .unwrap();

    // stick an abort in the middle, which should not be
    // returned
    {
        let guard = pin();
        let res = log
            .reserve(REPLACE, PID, &IVec::from(b"never_gonna_hit_disk"), &guard)
            .unwrap();
        res.abort().unwrap();
    }

    let guard = pin();
    log.reserve(REPLACE, PID, &IVec::from(b"4444"), &guard)
        .unwrap()
        .complete()
        .unwrap();
    let (last_lsn, _) = log
        .reserve(REPLACE, PID, &IVec::from(b"55555"), &guard)
        .unwrap()
        .complete()
        .unwrap();
    log.make_stable(last_lsn).unwrap();

    drop(log);

    let log = config.open_raw_log().unwrap();

    let mut iter = log.iter_from(first_lsn);
    assert!(iter.next().is_some());
    assert!(iter.next().is_some());
    assert!(iter.next().is_some());
    assert!(iter.next().is_some());
    assert!(iter.next().is_some());
    assert!(iter.next().is_some());
    assert_eq!(iter.next(), None);
}

#[test]
#[cfg(not(target_os = "fuchsia"))]
fn log_chunky_iterator() {
    common::setup_logger();
    let mut threads = vec![];
    for tn in 0..100 {
        let thread = thread::spawn(move || {
            let config = Config::new()
                .temporary(true)
                .segment_mode(SegmentMode::Linear)
                .segment_size(128);

            let log = config.open_raw_log().unwrap();

            let mut reference = vec![];

            let max_valid_size =
                config.segment_size - (MAX_MSG_HEADER_LEN + SEG_HEADER_LEN);

            for i in 0..1000 {
                let len = thread_rng().gen_range(0, max_valid_size * 2);
                let item = thread_rng().gen::<u8>();
                let buf = IVec::from(vec![item; len]);
                let abort = thread_rng().gen::<bool>();

                let pid = (tn * 10000) + i;

                let guard = pin();

                if abort {
                    let res = log
                        .reserve(REPLACE, pid, &buf, &guard)
                        .expect("should be able to reserve");
                    res.abort().unwrap();
                } else {
                    let (lsn, lid) = log
                        .reserve(REPLACE, pid, &buf, &guard)
                        .expect("should be able to write reservation")
                        .complete()
                        .unwrap();
                    reference.push((REPLACE, pid, lsn, lid, buf));
                }
            }

            let mut ref_iter = reference.clone().into_iter();
            for _ in log.iter_from(SEG_HEADER_LEN as Lsn) {
                assert!(ref_iter.next().is_some());
            }

            // recover and restart
            drop(log);
            let log = config.open_raw_log().unwrap();

            let mut log_iter = log.iter_from(SEG_HEADER_LEN as Lsn);
            for _ in reference.clone().into_iter() {
                assert!(log_iter.next().is_some());
            }
        });
        threads.push(thread);
    }
    for thread in threads.into_iter() {
        thread.join().unwrap();
    }
}

#[test]
fn multi_segment_log_iteration() -> Result<()> {
    common::setup_logger();
    // ensure segments are being linked
    // ensure trailers are valid
    let config = Config::new()
        .temporary(true)
        .segment_mode(SegmentMode::Linear)
        .segment_size(512);

    let total_seg_overhead = SEG_HEADER_LEN;
    let big_msg_overhead = MAX_MSG_HEADER_LEN + total_seg_overhead;
    let big_msg_sz = (config.segment_size - big_msg_overhead) / 64;

    let log = config.open_raw_log().unwrap();

    for i in 0..48 {
        let buf = IVec::from(vec![i as u8; big_msg_sz * i]);
        let guard = pin();
        log.reserve(REPLACE, i as PageId, &buf, &guard)
            .unwrap()
            .complete()
            .unwrap();
    }
    log.flush()?;

    drop(log);

    let log = config.open_raw_log().unwrap();

    // start iterating just past the first segment header
    let mut iter = log.iter_from(SEG_HEADER_LEN as Lsn);

    for _ in 0..48 {
        iter.next().expect("expected to read another message");
    }

    Ok(())
}

#[derive(Debug, Clone)]
enum Op {
    Write(Vec<u8>),
    AbortReservation(Vec<u8>),
    Read(u64),
    Restart,
    Truncate(u64),
}

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        static LEN: AtomicUsize = AtomicUsize::new(0);

        // ensure len never is loaded at 0, as it will crash
        // the call to gen_range below if low >= high
        LEN.compare_and_swap(0, 1, Ordering::SeqCst);

        let incr = || {
            let len = thread_rng().gen_range(0, 2);
            LEN.fetch_add(len + MAX_MSG_HEADER_LEN, Ordering::Relaxed);
            vec![COUNTER.fetch_add(1, Ordering::Relaxed) as u8; len]
        };

        if g.gen_bool(1. / 7.) {
            return Op::Restart;
        }

        if g.gen_bool(1. / 50.) {
            let len = LEN.load(Ordering::Relaxed);
            return Op::Truncate(thread_rng().gen_range(0, len as u64));
        }

        let choice = g.gen_range(0, 4);

        match choice {
            0 => Op::Write(incr()),
            1 => Op::Write(incr()),
            2 => Op::AbortReservation(incr()),
            3 => Op::Read(g.gen_range(0, 15)),
            _ => panic!("impossible choice"),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Op>> {
        let mut op = self.clone();
        let mut shrunk = false;
        match op {
            Op::Read(ref mut lid) if *lid > 0 => {
                shrunk = true;
                *lid -= 1;
            }
            Op::Truncate(ref mut len) if *len > 0 => {
                shrunk = true;
                *len -= 1;
            }
            _ => {}
        }
        if shrunk {
            Box::new(vec![op].into_iter())
        } else {
            Box::new(vec![].into_iter())
        }
    }
}

fn prop_log_works(ops: Vec<Op>, flusher: bool) -> bool {
    common::setup_logger();

    use self::Op::*;
    let config = Config::new()
        .temporary(true)
        .flush_every_ms(if flusher { Some(1) } else { None })
        .segment_mode(SegmentMode::Linear)
        .segment_size(8192);

    let mut tip = 0;
    let mut log = config.open_raw_log().unwrap();
    let mut reference: Vec<(Lsn, DiskPtr, Option<Vec<u8>>, usize)> = vec![];

    for op in ops.into_iter() {
        match op {
            Read(id) => {
                if reference.len() <= id as usize {
                    continue;
                }
                let (lsn, ptr, ref expected, _len) = reference[id as usize];
                // log.make_stable(lid);
                let read_res = log.read(PID, lsn, ptr);
                // println!( "expected {:?} read_res {:?} tip {} lid {}",
                // expected, read_res, tip, lid);
                if expected.is_none() || tip as u64 <= ptr.lid() {
                    assert!(
                        read_res.is_err() || !read_res.unwrap().is_successful()
                    );
                } else {
                    let flush = read_res.unwrap().into_data();
                    assert_eq!(
                        flush,
                        *expected,
                        "read unexpected value at lid {}. reference: {:?}",
                        ptr.lid(),
                        reference
                    );
                }
            }
            Write(buf) => {
                let len = buf.len();
                let guard = pin();
                let (lsn, ptr) = log
                    .reserve(REPLACE, PID, &IVec::from(&*buf), &guard)
                    .unwrap()
                    .complete()
                    .unwrap();
                tip = ptr.lid() as usize + len + MAX_MSG_HEADER_LEN;
                reference.push((
                    lsn,
                    ptr,
                    Some(IVec::from(&*buf).serialize()),
                    len,
                ));
            }
            AbortReservation(buf) => {
                let len = buf.len();
                let guard = pin();
                let res = log
                    .reserve(REPLACE, PID, &IVec::from(buf), &guard)
                    .unwrap();
                let lsn = res.lsn();
                let lid = res.lid();
                let ptr = res.pointer();
                res.abort().unwrap();
                tip = lid as usize + len + MAX_MSG_HEADER_LEN;
                reference.push((lsn, ptr, None, len));
            }
            Restart => {
                drop(log);

                // on recovery, we will rewind over any aborted tip entries
                while !reference.is_empty() {
                    let should_pop = reference.last().unwrap().2.is_none();
                    if should_pop {
                        reference.pop();
                    } else {
                        break;
                    }
                }
                log = config.open_raw_log().unwrap();
            }
            #[allow(unused_variables)]
            Truncate(new_len) => {
                #[cfg(target_os = "linux")]
                {
                    if tip as u64 <= new_len {
                        // we are testing data loss, not rogue extensions
                        continue;
                    }

                    tip = new_len as usize;

                    drop(log);

                    for &mut (_lsn, ptr, ref mut expected, sz) in &mut reference
                    {
                        let tip = ptr.lid() as usize + sz + MAX_MSG_HEADER_LEN;
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

                    #[cfg(feature = "failpoints")]
                    config.truncate_corrupt(new_len);

                    log = config.open_raw_log().unwrap();
                }
            }
        }
    }
    true
}

#[test]
fn quickcheck_log_works() {
    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 100))
        .tests(100)
        .max_tests(10000)
        .quickcheck(prop_log_works as fn(Vec<Op>, bool) -> bool);
}

#[test]
fn log_bug_00() {
    // postmortem:
    use self::Op::*;
    prop_log_works(vec![Write(vec![66]), Read(0)], true);
}

#[test]
fn log_bug_01() {
    // postmortem: test was not stabilizing its buffers before
    // reading
    use self::Op::*;
    prop_log_works(
        vec![
            AbortReservation(vec![]),
            Write(vec![34]),
            Write(vec![35]),
            Write(vec![36]),
            Read(3),
        ],
        true,
    );
}

#[test]
fn log_bug_02() {
    // postmortem: the logic that scans through zeroed entries
    // until the next readable byte hit the end of the file.
    // fix: when something hits the end of the file, return
    // LogRead::Zeroed.
    use self::Op::*;
    prop_log_works(vec![AbortReservation(vec![46]), Read(0)], true);
}

#[test]
fn log_bug_03() {
    // postmortem: the skip-ahead logic for continuing to
    // the next valid log entry when reading a zeroed entry
    // was being triggered on valid entries of size 0.
    // fix: don't skip-ahead for empty valid entries.
    use self::Op::*;
    prop_log_works(vec![Write(vec![]), Read(0)], true);
}

#[test]
fn log_bug_07() {
    // postmortem: test alignment
    use self::Op::*;
    prop_log_works(
        vec![
            Write(vec![]),
            Write(vec![]),
            AbortReservation(vec![]),
            Write(vec![12]),
            AbortReservation(vec![]),
            Truncate(24),
            Write(vec![19]),
            Write(vec![20]),
            Read(4),
        ],
        true,
    );
}

#[test]
fn log_bug_09() {
    // postmortem: test was mishandling truncation, which
    // should test loss, not extension
    use self::Op::*;
    prop_log_works(
        vec![Truncate(99), Write(vec![234]), Truncate(8), Read(0)],
        true,
    );
}

#[test]
fn log_bug_10() {
    // postmortem: test alignment
    use self::Op::*;
    prop_log_works(
        vec![
            Write(vec![231]),
            AbortReservation(vec![232]),
            Write(vec![235]),
            AbortReservation(vec![236]),
            Write(vec![]),
            Write(vec![]),
            Write(vec![]),
            AbortReservation(vec![240]),
            Truncate(14),
            AbortReservation(vec![242]),
            Write(vec![243]),
            Write(vec![245]),
            Write(vec![]),
            Write(vec![249]),
            Write(vec![250]),
            Read(7),
        ],
        true,
    );
}

#[test]
fn log_bug_11() {
    // postmortem: was miscalculating the initial disk offset
    // on startup
    use self::Op::*;
    prop_log_works(vec![Write(vec![]), Restart], true);
}

#[test]
fn log_bug_12() {
    // postmortem: after a torn page is encountered, we were
    // just reusing the tip as the next start location. this
    // breaks recovery. fix: traverse the segment until we
    // encounter the last valid entry.
    use self::Op::*;
    prop_log_works(
        vec![Write(vec![]), Truncate(20), AbortReservation(vec![]), Read(0)],
        true,
    );
}

#[test]
fn log_bug_13() {
    // postmortem: was not recording the proper highest lsn on recovery.
    use self::Op::*;
    prop_log_works(
        vec![Write(vec![35]), Restart, AbortReservation(vec![36]), Read(0)],
        true,
    );
}

#[test]
fn log_bug_14a() {
    // postmortem: was not simulating the "rewind" behavior of the
    // log to replace aborted flushes at the log tip properly.
    // postmortem 2:
    use self::Op::*;
    prop_log_works(
        vec![AbortReservation(vec![12]), Restart, Write(vec![]), Read(0)],
        true,
    );
}

#[test]
fn log_bug_14b() {
    // postmortem: config was being improperly copied, causing temp
    // files to be deleted while dangling refs persisted, causing
    // the original data file to be unlinked and then divergent file
    // descriptors ended up being used. resulted in weird stuff like
    // reads not returning recent writes. Lesson: don't go around
    // FinalConfig!
    use self::Op::*;
    prop_log_works(vec![Restart, Write(vec![]), Read(0)], true);
}

#[test]
fn log_bug_15() {
    // postmortem: was not properly clearing previously
    // overwritten writes during truncation
    use self::Op::*;
    prop_log_works(
        vec![Write(vec![]), Truncate(0), Write(vec![]), Read(0)],
        true,
    );
}

#[test]
fn log_bug_16() {
    // postmortem: a bug in recovery created by the massive log overhaul
    use self::Op::*;
    prop_log_works(
        vec![
            Write(vec![189]),
            Truncate(1),
            Write(vec![206]),
            Restart,
            Write(vec![208]),
            Read(0),
        ],
        true,
    );
}

#[test]
fn log_bug_17() {
    // postmortem: this was a transient failure caused by failing to stabilize
    // an update before later reading it.
    use self::Op::*;
    prop_log_works(
        vec![
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
        true,
    );
}

#[test]
fn log_bug_18() {
    // postmortem: this was a recovery bug
    use self::Op::*;
    prop_log_works(vec![Restart], true);
}

#[test]
fn log_bug_19() {
    // postmortem: this was stalling in make_stable
    // postmortem 2: SA recovery skipped the first segment because we
    // were not properly adding the empty tip to the free list.
    use self::Op::*;
    prop_log_works(
        vec![
            Restart,
            Restart,
            Write(vec![]),
            AbortReservation(vec![]),
            Write(vec![47]),
            Restart,
            Read(2),
        ],
        true,
    );
}

#[test]
fn log_bug_20() {
    // postmortem: message header length was not being included when
    // calculating the starting log offsets.
    use self::Op::*;
    prop_log_works(vec![Write(vec![]), Restart, Write(vec![2]), Read(1)], true);
}

#[test]
fn log_bug_21() {
    // postmortem: message header length was not being included when
    // calculating the starting log offsets.
    use self::Op::*;
    prop_log_works(
        vec![Write(vec![1]), Restart, Write(vec![2]), Read(1)],
        true,
    );
}

#[test]
fn log_bug_22() {
    // postmortem:
    use self::Op::*;
    prop_log_works(
        vec![Read(10), Write(vec![75]), Write(vec![]), Write(vec![77])],
        true,
    );
}

#[test]
fn log_bug_23() {
    // postmortem:
    use self::Op::*;
    prop_log_works(vec![AbortReservation(vec![230]), Restart], false);
}

#[test]
fn log_bug_24() {
    // postmortem:
    use self::Op::*;
    prop_log_works(
        vec![
            AbortReservation(vec![107]),
            Restart,
            AbortReservation(vec![109]),
            Write(vec![110]),
            Write(vec![111]),
            Write(vec![112]),
            Restart,
            AbortReservation(vec![]),
            Write(vec![114]),
            Read(1),
        ],
        false,
    );
}

#[test]
fn log_bug_25() {
    // postmortem: we were making assertions
    // about invalid segments during our initial
    // segment scan
    use self::Op::*;
    prop_log_works(vec![Restart], false);
}

fn _log_bug_() {
    // postmortem: TEMPLATE
    // use self::Op::*;
    prop_log_works(vec![], true);
}

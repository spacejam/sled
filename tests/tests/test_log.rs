#![cfg_attr(test, allow(unused))]

use std::mem::size_of;
use {
    lazy_static::lazy_static,
    pagecache::{
        ConfigBuilder, DiskPtr, Log, LogRead, SegmentMode,
        MINIMUM_ITEMS_PER_SEGMENT, MSG_HEADER_LEN, SEG_HEADER_LEN,
        SEG_TRAILER_LEN,
    },
    quickcheck::{Arbitrary, Gen, QuickCheck, StdGen},
    rand::{thread_rng, Rng},
    std::{
        fs,
        sync::{
            atomic::{AtomicUsize, Ordering},
            {Arc, Mutex},
        },
        thread,
    },
};

type Lsn = i64;
type LogId = u64;

#[test]
#[ignore]
fn more_log_reservations_than_buffers() {
    let config = ConfigBuilder::new()
        .temporary(true)
        .segment_mode(SegmentMode::Linear)
        .build();
    let log = Log::start_raw_log(config.clone()).unwrap();
    let mut reservations = vec![];

    let total_seg_overhead = SEG_HEADER_LEN + SEG_TRAILER_LEN;
    let big_msg_overhead = MSG_HEADER_LEN + total_seg_overhead;
    let big_msg_sz = config.io_buf_size - big_msg_overhead;

    for _ in 0..=config.io_bufs {
        reservations.push(log.reserve(&vec![0; big_msg_sz]).unwrap())
    }
    for res in reservations.into_iter().rev() {
        // abort in reverse order
        res.abort();
    }
}

#[test]
fn non_contiguous_log_flush() {
    let config = ConfigBuilder::new()
        .temporary(true)
        .segment_mode(SegmentMode::Linear)
        .io_buf_size(1000)
        .build();
    let log = Log::start_raw_log(config.clone()).unwrap();

    let seg_overhead = std::cmp::max(SEG_HEADER_LEN, SEG_TRAILER_LEN);
    let buf_len = (config.io_buf_size / MINIMUM_ITEMS_PER_SEGMENT)
        - (MSG_HEADER_LEN + seg_overhead);

    let res1 = log.reserve(&vec![0; buf_len]).unwrap();
    let res2 = log.reserve(&vec![0; buf_len]).unwrap();
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
        let config = ConfigBuilder::new()
            .temporary(true)
            .segment_mode(SegmentMode::Linear)
            .io_buf_size(1000)
            .flush_every_ms(Some(50))
            .build();
        let log =
            Arc::new(Log::start_raw_log(config.clone()).unwrap());
        let iobs2 = log.clone();
        let iobs3 = log.clone();
        let iobs4 = log.clone();
        let iobs5 = log.clone();
        let iobs6 = log.clone();
        let log7 = log.clone();

        let seg_overhead =
            std::cmp::max(SEG_HEADER_LEN, SEG_TRAILER_LEN);
        let buf_len = (config.io_buf_size
            / MINIMUM_ITEMS_PER_SEGMENT)
            - (MSG_HEADER_LEN + seg_overhead);

        let t1 = thread::Builder::new()
            .name("c1".to_string())
            .spawn(move || {
                for i in 0..1_000 {
                    let buf = vec![1; i % buf_len];
                    log.write(buf);
                }
            })
            .unwrap();

        let t2 = thread::Builder::new()
            .name("c2".to_string())
            .spawn(move || {
                for i in 0..1_000 {
                    let buf = vec![2; i % buf_len];
                    iobs2.write(buf);
                }
            })
            .unwrap();

        let t3 = thread::Builder::new()
            .name("c3".to_string())
            .spawn(move || {
                for i in 0..1_000 {
                    let buf = vec![3; i % buf_len];
                    iobs3.write(buf);
                }
            })
            .unwrap();

        let t4 = thread::Builder::new()
            .name("c4".to_string())
            .spawn(move || {
                for i in 0..1_000 {
                    let buf = vec![4; i % buf_len];
                    iobs4.write(buf);
                }
            })
            .unwrap();
        let t5 = thread::Builder::new()
            .name("c5".to_string())
            .spawn(move || {
                for i in 0..1_000 {
                    let buf = vec![5; i % buf_len];
                    iobs5.write(buf);
                }
            })
            .unwrap();

        let t6 = thread::Builder::new()
            .name("c6".to_string())
            .spawn(move || {
                for i in 0..1_000 {
                    let buf = vec![6; i % buf_len];
                    let (lsn, _lid) = iobs6.write(buf).unwrap();
                    iobs6.make_stable(lsn).unwrap();
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
}

#[test]
fn concurrent_logging_404() {
    let config = ConfigBuilder::new()
        .temporary(true)
        .segment_mode(SegmentMode::Linear)
        .io_buf_size(1000)
        .flush_every_ms(Some(50))
        .build();
    let log_arc =
        Arc::new(Log::start_raw_log(config.clone()).unwrap());

    const ITERATIONS: i32 = 5000;
    const NUM_THREADS: i32 = 6;

    static SHARED_COUNTER: AtomicUsize = AtomicUsize::new(0);
    let mut handles = Vec::new();
    for t in 0..NUM_THREADS {
        let log = log_arc.clone();
        let h = thread::Builder::new()
            .name(format!("t_{}", t))
            .spawn(move || {
                for i in 0..ITERATIONS {
                    let current =
                        SHARED_COUNTER.load(Ordering::SeqCst);
                    let raw_value: [u8; size_of::<usize>()] =
                        unsafe { std::mem::transmute(current + 1) };
                    let res = log.reserve(&raw_value).unwrap();
                    match SHARED_COUNTER.compare_and_swap(
                        current,
                        current + 1,
                        Ordering::SeqCst,
                    ) {
                        // If the current value was returned, then CAS succeeded on AtomicUsize
                        c if c == current => {
                            res.complete()
                                .expect("reservation complete panic");
                        }
                        // Any other value is an error
                        _ => {
                            res.abort()
                                .expect("reservation abort panic");
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

    let log = Log::start_raw_log(config).unwrap();
    let mut iter = log.iter_from(SEG_HEADER_LEN as Lsn);
    let successfuls = SHARED_COUNTER.load(Ordering::SeqCst);
    for i in 0..successfuls {
        let val = iter.next().expect("expected some log entry");
        let mut raw_value = [0_u8; size_of::<usize>()];
        raw_value.copy_from_slice(&*val.2);

        let read: usize = unsafe { std::mem::transmute(raw_value) };
        assert_eq!(
            i + 1,
            read,
            "failed to read expected value at log coordinate {}",
            val.1
        );
    }
    // Assert that there is nothing left in the log.
    assert_eq!(iter.next(), None);
}

fn write(log: &Log) {
    let data_bytes = b"yoyoyoyo";
    let (lsn, ptr) = log.write(data_bytes).unwrap();
    let read_buf = log.read(lsn, ptr).unwrap().into_data().unwrap();
    assert_eq!(
        read_buf, data_bytes,
        "after writing data, it should be readable"
    );
}

fn abort(log: &Log) {
    let res = log.reserve(&[0; 5]).unwrap();
    let (lsn, ptr) = res.abort().unwrap();
    match log.read(lsn, ptr) {
        Ok(LogRead::Failed(_, _)) => {}
        other => {
            panic!(
                "expected to successfully read \
                 aborted log message, instead read {:?}",
                other
            );
        }
        _ => (), // good
    }
}

#[test]
fn log_aborts() {
    let config = ConfigBuilder::new()
        .temporary(true)
        .segment_mode(SegmentMode::Linear)
        .build();
    let log = Log::start_raw_log(config).unwrap();
    write(&log);
    abort(&log);
    write(&log);
    abort(&log);
    write(&log);
    abort(&log);
}

#[test]
fn log_iterator() {
    let config = ConfigBuilder::new()
        .temporary(true)
        .segment_mode(SegmentMode::Linear)
        .io_buf_size(1000)
        .build();
    let log = Log::start_raw_log(config.clone()).unwrap();
    let (first_lsn, _) = log.write(b"").unwrap();
    log.write(b"1");
    log.write(b"22");
    log.write(b"333");

    // stick an abort in the middle, which should not be
    // returned
    {
        let res = log.reserve(b"never_gonna_hit_disk").unwrap();
        res.abort().unwrap();
    }

    log.write(b"4444");
    let (last_lsn, _) = log.write(b"55555").unwrap();
    log.make_stable(last_lsn).unwrap();

    drop(log);

    let log = Log::start_raw_log(config).unwrap();

    let mut iter = log.iter_from(first_lsn);
    assert_eq!(iter.next().unwrap().2, b"");
    assert_eq!(iter.next().unwrap().2, b"1");
    assert_eq!(iter.next().unwrap().2, b"22");
    assert_eq!(iter.next().unwrap().2, b"333");
    assert_eq!(iter.next().unwrap().2, b"4444");
    assert_eq!(iter.next().unwrap().2, b"55555");
    assert_eq!(iter.next(), None);
}

#[test]
#[cfg(not(target_os = "fuchsia"))]
fn log_chunky_iterator() {
    let mut threads = vec![];
    for _ in 0..100 {
        let thread = thread::spawn(|| {
            let config = ConfigBuilder::new()
                .temporary(true)
                .segment_mode(SegmentMode::Linear)
                .io_buf_size(100)
                .build();

            let log = Log::start_raw_log(config.clone()).unwrap();

            let mut reference = vec![];

            let max_valid_size = config.io_buf_size
                - (MSG_HEADER_LEN + SEG_HEADER_LEN + SEG_TRAILER_LEN);

            for i in 0..1000 {
                let len =
                    thread_rng().gen_range(0, max_valid_size * 2);
                let item = thread_rng().gen::<u8>();
                let buf = vec![item; len];
                let abort = thread_rng().gen::<bool>();

                if abort {
                    if let Ok(res) = log.reserve(&buf) {
                        res.abort().unwrap();
                    } else {
                        assert!(len > max_valid_size);
                    }
                } else {
                    let (lsn, lid) = log.write(buf.clone()).expect(
                        "should be able to write reservation",
                    );
                    reference.push((lsn, lid, buf));
                }
            }

            let mut ref_iter = reference.clone().into_iter();
            for t in log.iter_from(SEG_HEADER_LEN as Lsn) {
                assert_eq!(ref_iter.next(), Some(t));
            }

            // recover and restart
            drop(log);
            let log = Log::start_raw_log(config).unwrap();

            let mut log_iter = log.iter_from(SEG_HEADER_LEN as Lsn);
            for r in reference.clone().into_iter() {
                assert_eq!(Some(r), log_iter.next());
            }
        });
        threads.push(thread);
    }
    for thread in threads.into_iter() {
        thread.join().unwrap();
    }
}

#[test]
#[ignore]
fn snapshot_with_out_of_order_buffers() {
    let config = ConfigBuilder::new()
        .temporary(true)
        .segment_mode(SegmentMode::Linear)
        .io_buf_size(100)
        .io_bufs(2)
        .snapshot_after_ops(5)
        .build();

    let len = config.io_buf_size
        - SEG_HEADER_LEN
        - SEG_TRAILER_LEN
        - MSG_HEADER_LEN;

    let log = Log::start_raw_log(config.clone()).unwrap();

    for i in 0..4 {
        let buf = vec![i as u8; len];
        let (lsn, _lid) = log.write(buf).unwrap();
        log.make_stable(lsn).unwrap();
    }

    {
        // erase the third segment trailer to represent
        // an out-of-order segment write before
    }

    drop(log);

    let log = Log::start_raw_log(config.clone()).unwrap();

    // start iterating just past the first segment header
    let mut iter = log.iter_from(SEG_HEADER_LEN as Lsn);

    for i in 0..config.io_bufs * 2 {
        let expected = vec![i as u8; len];
        let (_lsn, _lid, buf) = iter.next().unwrap();
        assert_eq!(expected, buf);
    }
}

#[test]
fn multi_segment_log_iteration() {
    tests::setup_logger();
    // ensure segments are being linked
    // ensure trailers are valid
    let config = ConfigBuilder::new()
        .temporary(true)
        .segment_mode(SegmentMode::Linear)
        .io_buf_size(500)
        .build();

    let total_seg_overhead = SEG_HEADER_LEN + SEG_TRAILER_LEN;
    let big_msg_overhead = MSG_HEADER_LEN + total_seg_overhead;
    let big_msg_sz = (config.io_buf_size - big_msg_overhead) / 64;

    let log = Log::start_raw_log(config.clone()).unwrap();

    for i in 0..config.io_bufs * 16 {
        let buf = vec![i as u8; big_msg_sz * i];
        log.write(buf).unwrap();
    }
    log.flush();

    drop(log);

    let log = Log::start_raw_log(config.clone()).unwrap();

    // start iterating just past the first segment header
    println!("creating iterator");
    let mut iter = log.iter_from(SEG_HEADER_LEN as Lsn);
    println!("using iterator");

    for i in 0..config.io_bufs * 16 {
        let expected = vec![i as u8; big_msg_sz * i];
        let (_lsn, _lid, buf) = iter.next().expect("");
        assert_eq!(expected, buf);
    }
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

        let mut incr = || {
            let len = thread_rng().gen_range(0, 2);
            LEN.fetch_add(len + MSG_HEADER_LEN, Ordering::Relaxed);
            vec![COUNTER.fetch_add(1, Ordering::Relaxed) as u8; len]
        };

        if g.gen_bool(1. / 7.) {
            return Op::Restart;
        }

        if g.gen_bool(1. / 50.) {
            let len = LEN.load(Ordering::Relaxed);
            return Op::Truncate(
                thread_rng().gen_range(0, len as u64),
            );
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

    fn shrink(&self) -> Box<Iterator<Item = Op>> {
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
    tests::setup_logger();

    /*
    lazy_static! {
        // forces quickcheck to run one thread at a time
        static ref M: Mutex<()> = Mutex::new(());
    }

    let _lock =
        M.lock().expect("our test lock should not be poisoned");
    */

    println!("ops: {:?}", ops);
    use self::Op::*;
    let config = ConfigBuilder::new()
        .temporary(true)
        .io_buf_size(8192)
        .flush_every_ms(if flusher { Some(1) } else { None })
        .segment_mode(SegmentMode::Linear)
        .build();

    let mut tip = 0;
    let mut log = Log::start_raw_log(config.clone()).unwrap();
    let mut reference: Vec<(Lsn, DiskPtr, Option<Vec<u8>>, usize)> =
        vec![];

    for op in ops.into_iter() {
        match op {
            Read(lid) => {
                if reference.len() <= lid as usize {
                    continue;
                }
                let (lsn, ptr, ref expected, _len) =
                    reference[lid as usize];
                // log.make_stable(lid);
                let read_res = log.read(lsn, ptr);
                // println!( "expected {:?} read_res {:?} tip {} lid {}", expected, read_res, tip, lid);
                if expected.is_none() || tip as u64 <= ptr.lid() {
                    assert!(
                        read_res.is_err()
                            || !read_res.unwrap().is_successful()
                    );
                } else {
                    let flush = read_res.unwrap().into_data();
                    assert_eq!(flush, *expected);
                }
            }
            Write(buf) => {
                let len = buf.len();
                let (lsn, ptr) = log.write(buf.clone()).unwrap();
                tip = ptr.lid() as usize + len + MSG_HEADER_LEN;
                reference.push((lsn, ptr, Some(buf), len));
            }
            AbortReservation(buf) => {
                let len = buf.len();
                let res = log.reserve(&buf).unwrap();
                let lsn = res.lsn();
                let lid = res.lid();
                let ptr = res.ptr();
                res.abort().unwrap();
                tip = lid as usize + len + MSG_HEADER_LEN;
                reference.push((lsn, ptr, None, len));
            }
            Restart => {
                drop(log);

                println!("\nrestarting\n");

                // on recovery, we will rewind over any aborted tip entries
                while !reference.is_empty() {
                    let should_pop =
                        reference.last().unwrap().2.is_none();
                    if should_pop {
                        reference.pop();
                    } else {
                        break;
                    }
                }
                log = Log::start_raw_log(config.clone()).unwrap();
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
                    for &mut (lsn, ptr, ref mut expected, sz) in
                        &mut reference
                    {
                        let tip =
                            ptr.lid() as usize + sz + MSG_HEADER_LEN;
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

                    config.truncate_corrupt(new_len);

                    log = Log::start_raw_log(config.clone()).unwrap();
                }
            }
        }
    }
    true
}

#[test]
#[ignore]
fn quickcheck_log_works() {
    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 100))
        .tests(1000)
        .max_tests(10000)
        .quickcheck(prop_log_works as fn(Vec<Op>, bool) -> bool);
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
fn log_bug_2() {
    // postmortem: the logic that scans through zeroed entries
    // until the next readable byte hit the end of the file.
    // fix: when something hits the end of the file, return
    // LogRead::Zeroed.
    use self::Op::*;
    prop_log_works(vec![AbortReservation(vec![46]), Read(0)], true);
}

#[test]
fn log_bug_3() {
    // postmortem: the skip-ahead logic for continuing to
    // the next valid log entry when reading a zeroed entry
    // was being triggered on valid entries of size 0.
    // fix: don't skip-ahead for empty valid entries.
    use self::Op::*;
    prop_log_works(vec![Write(vec![]), Read(0)], true);
}

#[test]
fn log_bug_7() {
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
fn log_bug_9() {
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
        vec![
            Write(vec![]),
            Truncate(20),
            AbortReservation(vec![]),
            Read(0),
        ],
        true,
    );
}

#[test]
fn log_bug_13() {
    // postmortem: was not recording the proper highest lsn on recovery.
    use self::Op::*;
    prop_log_works(
        vec![
            Write(vec![35]),
            Restart,
            AbortReservation(vec![36]),
            Read(0),
        ],
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
        vec![
            AbortReservation(vec![12]),
            Restart,
            Write(vec![]),
            Read(0),
        ],
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
    prop_log_works(
        vec![Write(vec![]), Restart, Write(vec![2]), Read(1)],
        true,
    );
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
        vec![
            Read(10),
            Write(vec![75]),
            Write(vec![]),
            Write(vec![77]),
        ],
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

fn _log_bug_() {
    // postmortem: TEMPLATE
    // use self::Op::*;
    prop_log_works(vec![], true);
}

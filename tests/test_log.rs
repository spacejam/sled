mod common;

use rand::{thread_rng, Rng};

use sled::*;

use sled::{
    pin, BatchManifest, Log, LogKind, LogRead, Lsn, PageId, SEG_HEADER_LEN,
};

const PID: PageId = 4;
const REPLACE: LogKind = LogKind::Replace;

#[test]
#[ignore = "adding 50 causes the flush to never return"]
fn log_writebatch() -> crate::Result<()> {
    common::setup_logger();
    let config = Config::new().temporary(true);
    let db = config.open()?;
    let log = &db.context.pagecache.log;

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
    let batch_res =
        log.reserve(REPLACE, PID, &BatchManifest::default(), &guard)?;
    log.reserve(REPLACE, PID, &IVec::from(b"6"), &guard)?.complete()?;
    log.reserve(REPLACE, PID, &IVec::from(b"7"), &guard)?.complete()?;
    log.reserve(REPLACE, PID, &IVec::from(b"8"), &guard)?.complete()?;
    let last_res = log.reserve(REPLACE, PID, &IVec::from(b"9"), &guard)?;
    let last_res_lsn = last_res.lsn();
    last_res.complete()?;
    batch_res.mark_writebatch(last_res_lsn + 50)?;
    log.reserve(REPLACE, PID, &IVec::from(b"10"), &guard)?.complete()?;

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
    let config = Config::new().temporary(true).segment_size(256);

    let db = config.open()?;
    let log = &db.context.pagecache.log;
    let mut reservations = vec![];

    let total_seg_overhead = SEG_HEADER_LEN;
    let big_msg_overhead = MAX_MSG_HEADER_LEN + total_seg_overhead;
    let big_msg_sz = config.segment_size - big_msg_overhead;

    for _ in 0..256 * 30 {
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
    let config = Config::new().temporary(true).segment_size(1024);

    let db = config.open()?;
    let log = &db.context.pagecache.log;

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
#[cfg(not(miri))] // can't create threads
fn concurrent_logging() -> Result<()> {
    use std::thread;

    common::setup_logger();
    for _ in 0..10 {
        let config = Config::new()
            .temporary(true)
            .flush_every_ms(Some(50))
            .segment_size(256);

        let db = config.open()?;

        let db2 = db.clone();
        let db3 = db.clone();
        let db4 = db.clone();
        let db5 = db.clone();
        let db6 = db.clone();

        let seg_overhead = SEG_HEADER_LEN;
        let buf_len = (config.segment_size / MINIMUM_ITEMS_PER_SEGMENT)
            - (MAX_MSG_HEADER_LEN + seg_overhead);

        let t1 = thread::Builder::new()
            .name("c1".to_string())
            .spawn(move || {
                let log = &db.context.pagecache.log;
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
                let log = &db2.context.pagecache.log;
                for i in 0..1_000 {
                    let buf = IVec::from(vec![2; i % buf_len]);
                    let guard = pin();
                    log.reserve(REPLACE, PID, &buf, &guard)
                        .unwrap()
                        .complete()
                        .unwrap();
                }
            })
            .unwrap();

        let t3 = thread::Builder::new()
            .name("c3".to_string())
            .spawn(move || {
                let log = &db3.context.pagecache.log;
                for i in 0..1_000 {
                    let buf = IVec::from(vec![3; i % buf_len]);
                    let guard = pin();
                    log.reserve(REPLACE, PID, &buf, &guard)
                        .unwrap()
                        .complete()
                        .unwrap();
                }
            })
            .unwrap();

        let t4 = thread::Builder::new()
            .name("c4".to_string())
            .spawn(move || {
                let log = &db4.context.pagecache.log;
                for i in 0..1_000 {
                    let buf = IVec::from(vec![4; i % buf_len]);
                    let guard = pin();
                    log.reserve(REPLACE, PID, &buf, &guard)
                        .unwrap()
                        .complete()
                        .unwrap();
                }
            })
            .unwrap();
        let t5 = thread::Builder::new()
            .name("c5".to_string())
            .spawn(move || {
                let log = &db5.context.pagecache.log;
                for i in 0..1_000 {
                    let guard = pin();
                    let buf = IVec::from(vec![5; i % buf_len]);
                    log.reserve(REPLACE, PID, &buf, &guard)
                        .unwrap()
                        .complete()
                        .unwrap();
                }
            })
            .unwrap();

        let t6 = thread::Builder::new()
            .name("c6".to_string())
            .spawn(move || {
                let log = &db6.context.pagecache.log;
                for i in 0..1_000 {
                    let buf = IVec::from(vec![6; i % buf_len]);
                    let guard = pin();
                    let (lsn, _lid) = log
                        .reserve(REPLACE, PID, &buf, &guard)
                        .unwrap()
                        .complete()
                        .unwrap();
                    log.make_stable(lsn).unwrap();
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

    Ok(())
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
    let config = Config::new().temporary(true);
    let db = config.open().unwrap();
    let log = &db.context.pagecache.log;
    write(&log);
    abort(&log);
    write(&log);
    abort(&log);
    write(&log);
    abort(&log);
}

#[test]
#[cfg_attr(any(target_os = "fuchsia", miri), ignore)]
fn log_chunky_iterator() {
    common::setup_logger();
    let config =
        Config::new().flush_every_ms(None).temporary(true).segment_size(256);

    let db = config.open().unwrap();
    let log = &db.context.pagecache.log;

    let mut reference = vec![];

    let max_valid_size =
        config.segment_size - (MAX_MSG_HEADER_LEN + SEG_HEADER_LEN);

    for i in PID..1000 {
        let len = thread_rng().gen_range(0, max_valid_size * 2);
        let item = thread_rng().gen::<u8>();
        let buf = IVec::from(vec![item; len]);
        let abort = thread_rng().gen::<bool>();

        let pid = 10000 + i;

        let guard = pin();

        if abort {
            let res = log
                .reserve(REPLACE, pid, &buf, &guard)
                .expect("should be able to reserve");
            res.abort().unwrap();
        } else {
            let res = log
                .reserve(REPLACE, pid, &buf, &guard)
                .expect("should be able to write reservation");
            let ptr = res.pointer();
            let (lsn, _) = res.complete().unwrap();
            reference.push((REPLACE, pid, lsn, ptr));
        }
    }

    for (_, pid, lsn, ptr) in reference.into_iter() {
        assert!(log.read(pid, lsn, ptr).is_ok());
    }
}

#[test]
fn multi_segment_log_iteration() -> Result<()> {
    common::setup_logger();
    // ensure segments are being linked
    // ensure trailers are valid
    let config = Config::new().temporary(true).segment_size(512);

    let total_seg_overhead = SEG_HEADER_LEN;
    let big_msg_overhead = MAX_MSG_HEADER_LEN + total_seg_overhead;
    let big_msg_sz = (config.segment_size - big_msg_overhead) / 64;

    let db = config.open().unwrap();
    let log = &db.context.pagecache.log;

    for i in 0..48 {
        let buf = IVec::from(vec![i as u8; big_msg_sz * i]);
        let guard = pin();
        log.reserve(REPLACE, i as PageId, &buf, &guard)
            .unwrap()
            .complete()
            .unwrap();
    }
    log.flush()?;

    // start iterating just past the first segment header
    let mut iter = log.iter_from(SEG_HEADER_LEN as Lsn);

    for _ in 0..48 {
        iter.next().expect("expected to read another message");
    }

    Ok(())
}

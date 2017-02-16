use std::thread;

use rsdb::{Log, LogData, ops};

#[test]
fn basic_functionality() {
    // TODO linearize res bufs, verify they are correct
    let log = Log::start_system();
    let iobs2 = log.clone();
    let iobs3 = log.clone();
    let iobs4 = log.clone();
    let iobs5 = log.clone();
    let iobs6 = log.clone();
    let t1 = thread::spawn(move || {
        for i in 0..5_000 {
            let buf = vec![1; i % 8192];
            log.write(buf);
        }
    });
    let t2 = thread::spawn(move || {
        for i in 0..5_000 {
            let buf = vec![2; i % 8192];
            iobs2.write(buf);
        }
    });
    let t3 = thread::spawn(move || {
        for i in 0..5_000 {
            let buf = vec![3; i % 8192];
            iobs3.write(buf);
        }
    });
    let t4 = thread::spawn(move || {
        for i in 0..5_000 {
            let buf = vec![4; i % 8192];
            iobs4.write(buf);
        }
    });
    let t5 = thread::spawn(move || {
        for i in 0..5_000 {
            let buf = vec![5; i % 8192];
            iobs5.write(buf);
        }
    });
    let t6 = thread::spawn(move || {
        for i in 0..5_000 {
            let buf = vec![6; i % 8192];
            iobs6.write(buf);
        }
    });
    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();
    t6.join();
}

fn test_delta(log: &Log) {
    let deltablock = LogData::Deltas(vec![]);
    let data_bytes = ops::to_binary(&deltablock);
    let res = log.reserve(data_bytes.len());
    let id = res.log_id();
    res.complete(data_bytes);
    log.make_stable(id);
    let read = log.read(id).unwrap();
    assert_eq!(read, deltablock);
}

fn test_abort(log: &Log) {
    let res = log.reserve(5);
    let id = res.log_id();
    res.abort();
    log.make_stable(id);
    match log.read(id) {
        Err(_) => (), // good
        Ok(_) => {
            panic!("sucessfully read an aborted request! BAD! SAD!");
        }
    }
}

#[test]
fn log_rt() {
    let log = Log::start_system();
    test_delta(&log);
    test_abort(&log);
    test_delta(&log);
    test_abort(&log);
    test_delta(&log);
    test_abort(&log);
}

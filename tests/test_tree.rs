mod common;
mod tree;

use std::sync::{Arc, Barrier};
use std::thread;

use log::{debug, warn};
use quickcheck::{QuickCheck, StdGen};

use sled::Transactional;
use sled::*;

use tree::{
    prop_tree_matches_btreemap, Key,
    Op::{self, *},
};

const N_THREADS: usize = 10;
const N_PER_THREAD: usize = 100;
const N: usize = N_THREADS * N_PER_THREAD; // NB N should be multiple of N_THREADS
const SPACE: usize = N;

const INTENSITY: usize = 5;

fn kv(i: usize) -> Vec<u8> {
    let i = i % SPACE;
    let k = [(i >> 16) as u8, (i >> 8) as u8, i as u8];
    k.to_vec()
}

#[test]
fn concurrent_tree_ops() {
    common::setup_logger();

    for i in 0..INTENSITY {
        debug!("beginning test {}", i);

        let config = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .snapshot_after_ops(100_000_000)
            .segment_size(256);

        macro_rules! par {
            ($t:ident, $f:expr) => {
                let mut threads = vec![];
                for tn in 0..N_THREADS {
                    let tree = $t.clone();
                    let thread = thread::Builder::new()
                        .name(format!("t(thread: {} test: {})", tn, i))
                        .spawn(move || {
                            for i in
                                (tn * N_PER_THREAD)..((tn + 1) * N_PER_THREAD)
                            {
                                let k = kv(i);
                                $f(&*tree, k);
                            }
                        })
                        .expect("should be able to spawn thread");
                    threads.push(thread);
                }
                while let Some(thread) = threads.pop() {
                    if let Err(e) = thread.join() {
                        panic!("thread failure: {:?}", e);
                    }
                }
            };
        }

        debug!("========== initial sets test {} ==========", i);
        let t = Arc::new(config.open().unwrap());
        par! {t, |tree: &Tree, k: Vec<u8>| {
            assert_eq!(tree.get(&*k), Ok(None));
            tree.insert(&k, k.clone()).expect("we should write successfully");
            assert_eq!(tree.get(&*k).unwrap(), Some(k.clone().into()),
                "failed to read key {:?} that we just wrote from tree {:?}",
                k, tree);
        }};

        let n_scanned = t.iter().count();
        if n_scanned != N {
            warn!(
                "WARNING: test {} only had {} keys present \
                 in the DB BEFORE restarting. expected {}",
                i, n_scanned, N,
            );
        }

        drop(t);
        let t =
            Arc::new(config.open().expect("should be able to restart Tree"));

        let n_scanned = t.iter().count();
        if n_scanned != N {
            warn!(
                "WARNING: test {} only had {} keys present \
                 in the DB AFTER restarting. expected {}",
                i, n_scanned, N,
            );
        }

        debug!("========== reading sets in test {} ==========", i);
        par! {t, |tree: &Tree, k: Vec<u8>| {
            if let Some(v) =  tree.get(&*k).unwrap() {
                if v != k {
                    panic!("expected key {:?} not found", k);
                }
            } else {
                panic!("could not read key {:?}, which we \
                       just wrote to tree {:?}", k, tree);
            }
        }};

        drop(t);
        let t =
            Arc::new(config.open().expect("should be able to restart Tree"));

        debug!("========== CAS test in test {} ==========", i);
        par! {t, |tree: &Tree, k: Vec<u8>| {
            let k1 = k.clone();
            let mut k2 = k.clone();
            k2.reverse();
            tree.compare_and_swap(&k1, Some(&*k1), Some(k2)).unwrap().unwrap();
        }};

        drop(t);
        let t =
            Arc::new(config.open().expect("should be able to restart Tree"));

        par! {t, |tree: &Tree, k: Vec<u8>| {
            let k1 = k.clone();
            let mut k2 = k.clone();
            k2.reverse();
            assert_eq!(tree.get(&*k1).unwrap().unwrap().to_vec(), k2);
        }};

        drop(t);
        let t =
            Arc::new(config.open().expect("should be able to restart Tree"));

        debug!("========== deleting in test {} ==========", i);
        par! {t, |tree: &Tree, k: Vec<u8>| {
            tree.remove(&*k).unwrap();
        }};

        drop(t);
        let t =
            Arc::new(config.open().expect("should be able to restart Tree"));

        par! {t, |tree: &Tree, k: Vec<u8>| {
            assert_eq!(tree.get(&*k), Ok(None));
        }};
    }
}

#[test]
fn concurrent_tree_iter() -> Result<()> {
    common::setup_logger();

    const N_FORWARD: usize = INTENSITY;
    const N_REVERSE: usize = INTENSITY;

    let config = Config::new().temporary(true).flush_every_ms(None);

    let t = config.open().unwrap();

    const INDELIBLE: [&[u8]; 16] = [
        &[0u8],
        &[1u8],
        &[2u8],
        &[3u8],
        &[4u8],
        &[5u8],
        &[6u8],
        &[7u8],
        &[8u8],
        &[9u8],
        &[10u8],
        &[11u8],
        &[12u8],
        &[13u8],
        &[14u8],
        &[15u8],
    ];

    for item in &INDELIBLE {
        t.insert(item.to_vec(), item.to_vec())?;
    }

    let barrier = Arc::new(Barrier::new(N_FORWARD + N_REVERSE + 2));

    let mut threads: Vec<thread::JoinHandle<Result<()>>> = vec![];

    for i in 0..N_FORWARD {
        let t = thread::Builder::new()
            .name(format!("forward({})", i))
            .spawn({
                let t = t.clone();
                let barrier = barrier.clone();
                move || {
                    barrier.wait();
                    for _ in 0..100 {
                        let expected = INDELIBLE.iter();
                        let mut keys = t.iter().keys();

                        for expect in expected {
                            loop {
                                let k = keys.next().unwrap()?;
                                assert!(
                                    &*k <= *expect,
                                    "witnessed key is {:?} but we expected \
                                     one <= {:?}, so we overshot due to a \
                                     concurrent modification",
                                    k,
                                    expect,
                                );
                                if &*k == *expect {
                                    break;
                                }
                            }
                        }
                    }

                    Ok(())
                }
            })
            .unwrap();
        threads.push(t);
    }

    for i in 0..N_REVERSE {
        let t = thread::Builder::new()
            .name(format!("reverse({})", i))
            .spawn({
                let t = t.clone();
                let barrier = barrier.clone();
                move || {
                    barrier.wait();
                    for _ in 0..100 {
                        let expected = INDELIBLE.iter().rev();
                        let mut keys = t.iter().keys().rev();

                        for expect in expected {
                            loop {
                                if let Some(Ok(k)) = keys.next() {
                                    assert!(
                                    &*k >= *expect,
                                    "witnessed key is {:?} but we expected \
                                     one >= {:?}, so we overshot due to a \
                                     concurrent modification\n{:?}",
                                    k,
                                    expect,
                                    *t,
                                );
                                    if &*k == *expect {
                                        break;
                                    }
                                } else {
                                    panic!("undershot key on tree: \n{:?}", *t);
                                }
                            }
                        }
                    }

                    Ok(())
                }
            })
            .unwrap();

        threads.push(t);
    }

    let inserter = thread::Builder::new()
        .name("inserter".into())
        .spawn({
            let t = t.clone();
            let barrier = barrier.clone();
            move || {
                barrier.wait();

                for i in 0..(16 * 16 * 8) {
                    let major = i / (16 * 8);
                    let minor = i % 16;

                    let mut base = INDELIBLE[major].to_vec();
                    base.push(minor as u8);
                    t.insert(base.clone(), base.clone())?;
                }

                Ok(())
            }
        })
        .unwrap();

    threads.push(inserter);

    let deleter = thread::Builder::new()
        .name("deleter".into())
        .spawn({
            let t = t.clone();
            let barrier = barrier.clone();
            move || {
                barrier.wait();

                for i in 0..(16 * 16 * 8) {
                    let major = i / (16 * 8);
                    let minor = i % 16;

                    let mut base = INDELIBLE[major].to_vec();
                    base.push(minor as u8);
                    t.remove(&base)?;
                }

                Ok(())
            }
        })
        .unwrap();

    threads.push(deleter);

    for thread in threads.into_iter() {
        thread.join().expect("thread should not have crashed")?;
    }

    Ok(())
}

#[test]
fn concurrent_tree_transactions() -> TransactionResult<()> {
    common::setup_logger();

    let config = Config::new().temporary(true).flush_every_ms(None);
    let db = config.open().unwrap();

    db.insert(b"k1", b"cats").unwrap();
    db.insert(b"k2", b"dogs").unwrap();

    let mut threads: Vec<std::thread::JoinHandle<TransactionResult<()>>> =
        vec![];

    const N_WRITERS: usize = 30;
    const N_READERS: usize = 5;

    let barrier = Arc::new(Barrier::new(N_WRITERS + N_READERS));

    for _ in 0..N_WRITERS {
        let db = db.clone();
        let barrier = barrier.clone();
        let thread = std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..100 {
                db.transaction(|db| {
                    let v1 = db.remove(b"k1")?.unwrap();
                    let v2 = db.remove(b"k2")?.unwrap();

                    db.insert(b"k1", v2)?;
                    db.insert(b"k2", v1)?;

                    Ok(())
                })?;
            }
            Ok(())
        });
        threads.push(thread);
    }

    for _ in 0..N_READERS {
        let db = db.clone();
        let barrier = barrier.clone();
        let thread = std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..1000 {
                db.transaction(|db| {
                    let v1 = db.get(b"k1")?.unwrap();
                    let v2 = db.get(b"k2")?.unwrap();

                    let mut results = vec![v1, v2];
                    results.sort();

                    assert_eq!([&results[0], &results[1]], [b"cats", b"dogs"]);

                    Ok(())
                })?;
            }
            Ok(())
        });
        threads.push(thread);
    }

    for thread in threads.into_iter() {
        thread.join().unwrap()?;
    }

    let v1 = db.get(b"k1")?.unwrap();
    let v2 = db.get(b"k2")?.unwrap();
    assert_eq!([v1, v2], [b"cats", b"dogs"]);

    Ok(())
}

#[test]
fn many_tree_transactions() -> TransactionResult<()> {
    common::setup_logger();

    let config = Config::new().temporary(true).flush_every_ms(None);
    let db = Arc::new(config.open().unwrap());
    let t1 = db.open_tree(b"1")?;
    let t2 = db.open_tree(b"2")?;
    let t3 = db.open_tree(b"3")?;
    let t4 = db.open_tree(b"4")?;
    let t5 = db.open_tree(b"5")?;
    let t6 = db.open_tree(b"6")?;
    let t7 = db.open_tree(b"7")?;
    let t8 = db.open_tree(b"8")?;
    let t9 = db.open_tree(b"9")?;

    (&t1, &t2, &t3, &t4, &t5, &t6, &t7, &t8, &t9).transaction(|trees| {
        trees.0.insert("hi", "there")?;
        trees.8.insert("ok", "thanks")?;
        Ok(())
    })
}

#[test]
fn tree_subdir() {
    let mut parent_path = std::env::temp_dir();
    parent_path.push("test_tree_subdir");

    let _ = std::fs::remove_dir_all(&parent_path);

    let mut path = parent_path.clone();
    path.push("test_subdir");

    let config = Config::new().path(&path);

    let t = config.open().unwrap();

    t.insert(&[1], vec![1]).unwrap();

    drop(t);

    let config = Config::new().path(&path);

    let t = config.open().unwrap();

    let res = t.get(&*vec![1]);

    assert_eq!(res.unwrap().unwrap(), vec![1_u8]);

    drop(t);

    std::fs::remove_dir_all(&parent_path).unwrap();
}

#[test]
fn tree_small_keys_iterator() {
    let config = Config::new().temporary(true).flush_every_ms(None);
    let t = config.open().unwrap();
    for i in 0..N_PER_THREAD {
        let k = kv(i);
        t.insert(&k, k.clone()).unwrap();
    }

    for (i, (k, v)) in t.iter().map(|res| res.unwrap()).enumerate() {
        let should_be = kv(i);
        assert_eq!(should_be, &*k);
        assert_eq!(should_be, &*v);
    }

    for (i, (k, v)) in t.iter().map(|res| res.unwrap()).enumerate() {
        let should_be = kv(i);
        assert_eq!(should_be, &*k);
        assert_eq!(should_be, &*v);
    }

    let half_way = N_PER_THREAD / 2;
    let half_key = kv(half_way);
    let mut tree_scan = t.range(&*half_key..);
    let r1 = tree_scan.next().unwrap().unwrap();
    assert_eq!((r1.0.as_ref(), &*r1.1), (half_key.as_ref(), &*half_key));

    let first_key = kv(0);
    let mut tree_scan = t.range(&*first_key..);
    let r2 = tree_scan.next().unwrap().unwrap();
    assert_eq!((r2.0.as_ref(), &*r2.1), (first_key.as_ref(), &*first_key));

    let last_key = kv(N_PER_THREAD - 1);
    let mut tree_scan = t.range(&*last_key..);
    let r3 = tree_scan.next().unwrap().unwrap();
    assert_eq!((r3.0.as_ref(), &*r3.1), (last_key.as_ref(), &*last_key));
    assert_eq!(tree_scan.next(), None);
}

#[test]
fn tree_big_keys_iterator() {
    fn kv(i: usize) -> Vec<u8> {
        let k = [(i >> 16) as u8, (i >> 8) as u8, i as u8];

        let mut base = vec![0; u8::max_value() as usize];
        base.extend_from_slice(&k);
        base
    }

    let config = Config::new().temporary(true).flush_every_ms(None);

    let t = config.open().unwrap();
    for i in 0..N_PER_THREAD {
        let k = kv(i);
        t.insert(&k, k.clone()).unwrap();
    }

    for (i, (k, v)) in t.iter().map(|res| res.unwrap()).enumerate() {
        let should_be = kv(i);
        assert_eq!(should_be, &*k, "{:#?}", t);
        assert_eq!(should_be, &*v);
    }

    for (i, (k, v)) in t.iter().map(|res| res.unwrap()).enumerate() {
        let should_be = kv(i);
        assert_eq!(should_be, &*k);
        assert_eq!(should_be, &*v);
    }

    let half_way = N_PER_THREAD / 2;
    let half_key = kv(half_way);
    let mut tree_scan = t.range(&*half_key..);
    let r1 = tree_scan.next().unwrap().unwrap();
    assert_eq!((r1.0.as_ref(), &*r1.1), (half_key.as_ref(), &*half_key));

    let first_key = kv(0);
    let mut tree_scan = t.range(&*first_key..);
    let r2 = tree_scan.next().unwrap().unwrap();
    assert_eq!((r2.0.as_ref(), &*r2.1), (first_key.as_ref(), &*first_key));

    let last_key = kv(N_PER_THREAD - 1);
    let mut tree_scan = t.range(&*last_key..);
    let r3 = tree_scan.next().unwrap().unwrap();
    assert_eq!((r3.0.as_ref(), &*r3.1), (last_key.as_ref(), &*last_key));
    assert_eq!(tree_scan.next(), None);
}

#[test]
fn tree_subscriptions_and_keyspaces() -> Result<()> {
    let config = Config::new().temporary(true).flush_every_ms(None);

    let db = config.open().unwrap();

    let t1 = db.open_tree(b"1".to_vec())?;
    let mut s1 = t1.watch_prefix(b"".to_vec());

    let t2 = db.open_tree(b"2".to_vec())?;
    let mut s2 = t2.watch_prefix(b"".to_vec());

    t1.insert(b"t1_a", b"t1_a".to_vec())?;
    t2.insert(b"t2_a", b"t2_a".to_vec())?;

    assert_eq!(s1.next().unwrap().key(), b"t1_a");
    assert_eq!(s2.next().unwrap().key(), b"t2_a");

    let guard = pin();
    guard.flush();
    drop(guard);

    drop(db);
    drop(t1);
    drop(t2);

    let db = config.open().unwrap();

    let t1 = db.open_tree(b"1".to_vec())?;
    let mut s1 = t1.watch_prefix(b"".to_vec());

    let t2 = db.open_tree(b"2".to_vec())?;
    let mut s2 = t2.watch_prefix(b"".to_vec());

    assert!(db.is_empty());
    assert_eq!(t1.len(), 1);
    assert_eq!(t2.len(), 1);

    t1.insert(b"t1_b", b"t1_b".to_vec())?;
    t2.insert(b"t2_b", b"t2_b".to_vec())?;

    assert_eq!(s1.next().unwrap().key(), b"t1_b");
    assert_eq!(s2.next().unwrap().key(), b"t2_b");

    let guard = pin();
    guard.flush();
    drop(guard);

    drop(db);
    drop(t1);
    drop(t2);

    let db = config.open().unwrap();

    let t1 = db.open_tree(b"1".to_vec())?;
    let t2 = db.open_tree(b"2".to_vec())?;

    assert!(db.is_empty());
    assert_eq!(t1.len(), 2);
    assert_eq!(t2.len(), 2);

    db.drop_tree(b"1")?;
    db.drop_tree(b"2")?;

    assert_eq!(t1.get(b""), Err(Error::CollectionNotFound(b"1".to_vec())));

    assert_eq!(t2.get(b""), Err(Error::CollectionNotFound(b"2".to_vec())));

    let guard = pin();
    guard.flush();
    drop(guard);

    drop(db);
    drop(t1);
    drop(t2);

    let db = config.open().unwrap();

    let t1 = db.open_tree(b"1".to_vec())?;
    let t2 = db.open_tree(b"2".to_vec())?;

    assert!(db.is_empty());
    assert_eq!(t1.len(), 0);
    assert_eq!(t2.len(), 0);

    Ok(())
}

#[test]
fn tree_range() {
    common::setup_logger();

    let config = Config::new().temporary(true).flush_every_ms(None);
    let t = config.open().unwrap();

    t.insert(b"0", vec![0]).unwrap();
    t.insert(b"1", vec![10]).unwrap();
    t.insert(b"2", vec![20]).unwrap();
    t.insert(b"3", vec![30]).unwrap();
    t.insert(b"4", vec![40]).unwrap();
    t.insert(b"5", vec![50]).unwrap();

    let start: &[u8] = b"2";
    let end: &[u8] = b"4";
    let mut r = t.range(start..end);
    assert_eq!(r.next().unwrap().unwrap().0, b"2");
    assert_eq!(r.next().unwrap().unwrap().0, b"3");
    assert_eq!(r.next(), None);

    let start = b"2".to_vec();
    let end = b"4".to_vec();
    let mut r = t.range(start..end).rev();
    assert_eq!(r.next().unwrap().unwrap().0, b"3");
    assert_eq!(r.next().unwrap().unwrap().0, b"2");
    assert_eq!(r.next(), None);

    let start = b"2".to_vec();
    let mut r = t.range(start..);
    assert_eq!(r.next().unwrap().unwrap().0, b"2");
    assert_eq!(r.next().unwrap().unwrap().0, b"3");
    assert_eq!(r.next().unwrap().unwrap().0, b"4");
    assert_eq!(r.next().unwrap().unwrap().0, b"5");
    assert_eq!(r.next(), None);

    let start = b"2".to_vec();
    let mut r = t.range(..=start).rev();
    assert_eq!(
        r.next().unwrap().unwrap().0,
        b"2",
        "failed to find 2 in tree {:?}",
        t
    );
    assert_eq!(r.next().unwrap().unwrap().0, b"1");
    assert_eq!(r.next().unwrap().unwrap().0, b"0");
    assert_eq!(r.next(), None);
}

#[test]
fn recover_tree() {
    common::setup_logger();

    let config = Config::new()
        .temporary(true)
        .flush_every_ms(None)
        .snapshot_after_ops(N_PER_THREAD as u64)
        .segment_size(4096);

    let t = config.open().unwrap();
    for i in 0..N_PER_THREAD {
        let k = kv(i);
        t.insert(&k, k.clone()).unwrap();
    }
    drop(t);

    let t = config.open().unwrap();
    for i in 0..N_PER_THREAD {
        let k = kv(i as usize);
        assert_eq!(t.get(&*k).unwrap().unwrap(), k);
        t.remove(&*k).unwrap();
    }
    drop(t);

    let t = config.open().unwrap();
    for i in 0..N_PER_THREAD {
        let k = kv(i as usize);
        assert_eq!(t.get(&*k), Ok(None));
    }
}

#[test]
fn tree_import_export() -> Result<()> {
    common::setup_logger();

    let config_1 = Config::new().temporary(true);
    let config_2 = Config::new().temporary(true);

    let db = config_1.open()?;
    for db_id in 0..N_THREADS {
        let tree_id = format!("tree_{}", db_id);
        let tree = db.open_tree(tree_id.as_bytes())?;
        for i in 0..N_THREADS {
            let k = kv(i);
            tree.insert(&k, k.clone()).unwrap();
        }
    }

    let checksum_a = db.checksum().unwrap();

    drop(db);

    let exporter = config_1.open()?;
    let importer = config_2.open()?;

    let export = exporter.export();
    importer.import(export);

    drop(exporter);
    drop(config_1);
    drop(importer);

    let db = config_2.open()?;

    let checksum_b = db.checksum().unwrap();
    assert_eq!(checksum_a, checksum_b);

    for db_id in 0..N_THREADS {
        let tree_id = format!("tree_{}", db_id);
        let tree = db.open_tree(tree_id.as_bytes())?;

        for i in 0..N_THREADS {
            let k = kv(i as usize);
            assert_eq!(tree.get(&*k).unwrap().unwrap(), k);
            tree.remove(&*k).unwrap();
        }
    }

    let checksum_c = db.checksum().unwrap();

    drop(db);

    let db = config_2.open()?;
    for db_id in 0..N_THREADS {
        let tree_id = format!("tree_{}", db_id);
        let tree = db.open_tree(tree_id.as_bytes())?;

        for i in 0..N_THREADS {
            let k = kv(i as usize);
            assert_eq!(tree.get(&*k), Ok(None));
        }
    }

    let checksum_d = db.checksum().unwrap();
    assert_eq!(checksum_c, checksum_d);

    Ok(())
}

#[test]
#[cfg(not(target_os = "fuchsia"))]
fn quickcheck_tree_matches_btreemap() {
    let n_tests = 100;

    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 1000))
        .tests(n_tests)
        .max_tests(n_tests * 10)
        .quickcheck(
            prop_tree_matches_btreemap as fn(Vec<Op>, u8, bool, bool) -> bool,
        );
}

#[test]
fn tree_bug_01() {
    // postmortem:
    // this was a bug in the snapshot recovery, where
    // it led to max_id dropping by 1 after a restart.
    // postmortem 2:
    // we were stalling here because we had a new log with stable of
    // SEG_HEADER_LEN, but when we iterated over it to create a new
    // snapshot (snapshot every 1 set in Config), we iterated up until
    // that offset. make_stable requires our stable offset to be >=
    // the provided one, to deal with 0.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![32]), 9),
            Set(Key(vec![195]), 13),
            Restart,
            Set(Key(vec![164]), 147),
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_02() {
    // postmortem:
    // this was a bug in the way that the `Materializer`
    // was fed data, possibly out of order, if recover
    // in the pagecache had to run over log entries
    // that were later run through the same `Materializer`
    // then the second time (triggered by a snapshot)
    // would not pick up on the importance of seeing
    // the new root set.
    // portmortem 2: when refactoring iterators, failed
    // to account for node.hi being empty on the infinity
    // shard
    prop_tree_matches_btreemap(
        vec![
            Restart,
            Set(Key(vec![215]), 121),
            Restart,
            Set(Key(vec![216]), 203),
            Scan(Key(vec![210]), 4),
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_3() {
    // postmortem: the tree was not persisting and recovering root hoists
    // postmortem 2: when refactoring the log storage, we failed to restart
    // log writing in the proper location.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![113]), 204),
            Set(Key(vec![119]), 205),
            Set(Key(vec![166]), 88),
            Set(Key(vec![23]), 44),
            Restart,
            Set(Key(vec![226]), 192),
            Set(Key(vec![189]), 186),
            Restart,
            Scan(Key(vec![198]), 11),
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_4() {
    // postmortem: pagecache was failing to replace the LogId list
    // when it encountered a new Update::Compact.
    // postmortem 2: after refactoring log storage, we were not properly
    // setting the log tip, and the beginning got clobbered after writing
    // after a restart.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![158]), 31),
            Set(Key(vec![111]), 134),
            Set(Key(vec![230]), 187),
            Set(Key(vec![169]), 58),
            Set(Key(vec![131]), 10),
            Set(Key(vec![108]), 246),
            Set(Key(vec![127]), 155),
            Restart,
            Set(Key(vec![59]), 119),
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_5() {
    // postmortem: during recovery, the segment accountant was failing to
    // properly set the file's tip.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![231]), 107),
            Set(Key(vec![251]), 42),
            Set(Key(vec![80]), 81),
            Set(Key(vec![178]), 130),
            Set(Key(vec![150]), 232),
            Restart,
            Set(Key(vec![98]), 78),
            Set(Key(vec![0]), 45),
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_6() {
    // postmortem: after reusing segments, we were failing to checksum reads
    // performed while iterating over rewritten segment buffers, and using
    // former garbage data. fix: use the crc that's there for catching torn
    // writes with high probability, AND zero out buffers.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![162]), 8),
            Set(Key(vec![59]), 192),
            Set(Key(vec![238]), 83),
            Set(Key(vec![151]), 231),
            Restart,
            Set(Key(vec![30]), 206),
            Set(Key(vec![150]), 146),
            Set(Key(vec![18]), 34),
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_7() {
    // postmortem: the segment accountant was not fully recovered, and thought
    // that it could reuse a particular segment that wasn't actually empty
    // yet.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![135]), 22),
            Set(Key(vec![41]), 36),
            Set(Key(vec![101]), 31),
            Set(Key(vec![111]), 35),
            Restart,
            Set(Key(vec![47]), 36),
            Set(Key(vec![79]), 114),
            Set(Key(vec![64]), 9),
            Scan(Key(vec![196]), 25),
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_8() {
    // postmortem: failed to properly recover the state in the segment
    // accountant that tracked the previously issued segment.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![145]), 151),
            Set(Key(vec![155]), 148),
            Set(Key(vec![131]), 170),
            Set(Key(vec![163]), 60),
            Set(Key(vec![225]), 126),
            Restart,
            Set(Key(vec![64]), 237),
            Set(Key(vec![102]), 205),
            Restart,
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_9() {
    // postmortem: was failing to load existing snapshots on initialization.
    // would encounter uninitialized segments at the log tip and overwrite
    // the first segment (indexed by LSN of 0) in the segment accountant
    // ordering, skipping over important updates.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![189]), 36),
            Set(Key(vec![254]), 194),
            Set(Key(vec![132]), 50),
            Set(Key(vec![91]), 221),
            Set(Key(vec![126]), 6),
            Set(Key(vec![199]), 183),
            Set(Key(vec![71]), 125),
            Scan(Key(vec![67]), 16),
            Set(Key(vec![190]), 16),
            Restart,
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_10() {
    // postmortem: after reusing a segment, but not completely writing a
    // segment, we were hitting an old LSN and violating an assert, rather
    // than just ending.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![152]), 163),
            Set(Key(vec![105]), 191),
            Set(Key(vec![207]), 217),
            Set(Key(vec![128]), 19),
            Set(Key(vec![106]), 22),
            Scan(Key(vec![20]), 24),
            Set(Key(vec![14]), 150),
            Set(Key(vec![80]), 43),
            Set(Key(vec![174]), 134),
            Set(Key(vec![20]), 150),
            Set(Key(vec![13]), 171),
            Restart,
            Scan(Key(vec![240]), 25),
            Scan(Key(vec![77]), 37),
            Set(Key(vec![153]), 232),
            Del(Key(vec![2])),
            Set(Key(vec![227]), 169),
            Get(Key(vec![232])),
            Cas(Key(vec![247]), 151, 70),
            Set(Key(vec![78]), 52),
            Get(Key(vec![16])),
            Del(Key(vec![78])),
            Cas(Key(vec![201]), 93, 196),
            Set(Key(vec![172]), 84),
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_11() {
    // postmortem: a stall was happening because LSNs and LogIds were being
    // conflated in calls to make_stable. A higher LogId than any LSN was
    // being created, then passed in.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![38]), 148),
            Set(Key(vec![176]), 175),
            Set(Key(vec![82]), 88),
            Set(Key(vec![164]), 85),
            Set(Key(vec![139]), 74),
            Set(Key(vec![73]), 23),
            Cas(Key(vec![34]), 67, 151),
            Set(Key(vec![115]), 133),
            Set(Key(vec![249]), 138),
            Restart,
            Set(Key(vec![243]), 6),
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_12() {
    // postmortem: was not checking that a log entry's LSN matches its position
    // as part of detecting tears / partial rewrites.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![118]), 156),
            Set(Key(vec![8]), 63),
            Set(Key(vec![165]), 110),
            Set(Key(vec![219]), 108),
            Set(Key(vec![91]), 61),
            Set(Key(vec![18]), 98),
            Scan(Key(vec![73]), 6),
            Set(Key(vec![240]), 108),
            Cas(Key(vec![71]), 28, 189),
            Del(Key(vec![199])),
            Restart,
            Set(Key(vec![30]), 140),
            Scan(Key(vec![118]), 13),
            Get(Key(vec![180])),
            Cas(Key(vec![115]), 151, 116),
            Restart,
            Set(Key(vec![31]), 95),
            Cas(Key(vec![79]), 153, 225),
            Set(Key(vec![34]), 161),
            Get(Key(vec![213])),
            Set(Key(vec![237]), 215),
            Del(Key(vec![52])),
            Set(Key(vec![56]), 78),
            Scan(Key(vec![141]), 2),
            Cas(Key(vec![228]), 114, 170),
            Get(Key(vec![231])),
            Get(Key(vec![223])),
            Del(Key(vec![167])),
            Restart,
            Scan(Key(vec![240]), 31),
            Del(Key(vec![54])),
            Del(Key(vec![2])),
            Set(Key(vec![117]), 165),
            Set(Key(vec![223]), 50),
            Scan(Key(vec![69]), 4),
            Get(Key(vec![156])),
            Set(Key(vec![214]), 72),
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_13() {
    // postmortem: failed root hoists were being improperly recovered before the
    // following free was done on their page, but we treated the written node as
    // if it were a successful completed root hoist.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![42]), 10),
            Set(Key(vec![137]), 220),
            Set(Key(vec![183]), 129),
            Set(Key(vec![91]), 145),
            Set(Key(vec![126]), 26),
            Set(Key(vec![255]), 67),
            Set(Key(vec![69]), 18),
            Restart,
            Set(Key(vec![24]), 92),
            Set(Key(vec![193]), 17),
            Set(Key(vec![3]), 143),
            Cas(Key(vec![50]), 13, 84),
            Restart,
            Set(Key(vec![191]), 116),
            Restart,
            Del(Key(vec![165])),
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_14() {
    // postmortem: after adding prefix compression, we were not
    // handling re-inserts and deletions properly
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![107]), 234),
            Set(Key(vec![7]), 245),
            Set(Key(vec![40]), 77),
            Set(Key(vec![171]), 244),
            Set(Key(vec![173]), 16),
            Set(Key(vec![171]), 176),
            Scan(Key(vec![93]), 33),
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_15() {
    // postmortem: was not sorting keys properly when binary searching for them
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![102]), 165),
            Set(Key(vec![91]), 191),
            Set(Key(vec![141]), 228),
            Set(Key(vec![188]), 124),
            Del(Key(vec![141])),
            Scan(Key(vec![101]), 26),
        ],
        0,
        true,
        false,
    );
}

#[test]
fn tree_bug_16() {
    // postmortem: the test merge function was not properly adding numbers.
    prop_tree_matches_btreemap(
        vec![Merge(Key(vec![247]), 162), Scan(Key(vec![209]), 31)],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_17() {
    // postmortem: we were creating a copy of a node leaf during iteration
    // before accidentally putting it into a PinnedValue, despite the
    // fact that it was not actually part of the node's actual memory!
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![194, 215, 103, 0, 138, 11, 248, 131]), 70),
            Scan(Key(vec![]), 30),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_18() {
    // postmortem: when implementing get_gt and get_lt, there were some
    // issues with getting order comparisons correct.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![]), 19),
            Set(Key(vec![78]), 98),
            Set(Key(vec![255]), 224),
            Set(Key(vec![]), 131),
            Get(Key(vec![255])),
            GetGt(Key(vec![89])),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_19() {
    // postmortem: we were not seeking properly to the next node
    // when we hit a half-split child and were using get_lt
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![]), 138),
            Set(Key(vec![68]), 113),
            Set(Key(vec![155]), 73),
            Set(Key(vec![50]), 220),
            Set(Key(vec![]), 247),
            GetLt(Key(vec![100])),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_20() {
    // postmortem: we were not seeking forward during get_gt
    // if path_for_key reached a leaf that didn't include
    // a key for our
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![]), 10),
            Set(Key(vec![56]), 42),
            Set(Key(vec![138]), 27),
            Set(Key(vec![155]), 73),
            Set(Key(vec![]), 251),
            GetGt(Key(vec![94])),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_21() {
    // postmortem: more split woes while implementing get_lt
    // postmortem 2: failed to properly account for node hi key
    // being empty in the view predecessor function
    // postmortem 3: when rewriting Iter, failed to account for
    // direction of iteration
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![176]), 163),
            Set(Key(vec![]), 229),
            Set(Key(vec![169]), 121),
            Set(Key(vec![]), 58),
            GetLt(Key(vec![176])),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_22() {
    // postmortem: inclusivity wasn't being properly flipped off after
    // the first result during iteration
    // postmortem 2: failed to properly check bounds while iterating
    prop_tree_matches_btreemap(
        vec![
            Merge(Key(vec![]), 155),
            Merge(Key(vec![56]), 251),
            Scan(Key(vec![]), 2),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_23() {
    // postmortem: when rewriting CRC handling code, mis-sized the blob crc
    prop_tree_matches_btreemap(
        vec![Set(Key(vec![6; 5120]), 92), Restart, Scan(Key(vec![]), 35)],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_24() {
    // postmortem: get_gt diverged with the Iter impl
    prop_tree_matches_btreemap(
        vec![
            Merge(Key(vec![]), 193),
            Del(Key(vec![])),
            Del(Key(vec![])),
            Set(Key(vec![]), 55),
            Set(Key(vec![]), 212),
            Merge(Key(vec![]), 236),
            Del(Key(vec![])),
            Set(Key(vec![]), 192),
            Del(Key(vec![])),
            Set(Key(vec![94]), 115),
            Merge(Key(vec![62]), 34),
            GetGt(Key(vec![])),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_25() {
    // postmortem: was not accounting for merges when traversing
    // the frag chain and a Del was encountered
    prop_tree_matches_btreemap(
        vec![Del(Key(vec![])), Merge(Key(vec![]), 84), Get(Key(vec![]))],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_26() {
    // postmortem:
    prop_tree_matches_btreemap(
        vec![
            Merge(Key(vec![]), 194),
            Merge(Key(vec![62]), 114),
            Merge(Key(vec![80]), 202),
            Merge(Key(vec![]), 169),
            Set(Key(vec![]), 197),
            Del(Key(vec![])),
            Del(Key(vec![])),
            Set(Key(vec![]), 215),
            Set(Key(vec![]), 164),
            Merge(Key(vec![]), 150),
            GetGt(Key(vec![])),
            GetLt(Key(vec![80])),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_27() {
    // postmortem: was not accounting for the fact that deletions reduce the
    // chances of being able to split successfully.
    prop_tree_matches_btreemap(
        vec![
            Del(Key(vec![])),
            Merge(
                Key(vec![
                    74, 117, 68, 37, 89, 16, 84, 130, 133, 78, 74, 59, 44, 109,
                    34, 5, 36, 74, 131, 100, 79, 86, 87, 107, 87, 27, 1, 85,
                    53, 112, 89, 75, 67, 78, 58, 121, 0, 105, 8, 117, 79, 40,
                    94, 123, 83, 72, 78, 23, 23, 35, 50, 77, 59, 75, 54, 92,
                    89, 12, 27, 48, 64, 21, 42, 97, 45, 28, 122, 13, 4, 32, 51,
                    25, 26, 18, 65, 12, 54, 104, 106, 80, 75, 91, 111, 9, 5,
                    130, 43, 40, 3, 72, 0, 58, 92, 64, 112, 97, 75, 130, 11,
                    135, 19, 107, 40, 17, 25, 49, 48, 119, 82, 54, 35, 113, 91,
                    68, 12, 118, 123, 62, 108, 88, 67, 43, 33, 119, 132, 124,
                    1, 62, 133, 110, 25, 62, 129, 117, 117, 107, 123, 94, 127,
                    80, 0, 116, 101, 9, 9, 54, 134, 70, 66, 79, 50, 124, 115,
                    85, 42, 120, 24, 15, 81, 100, 72, 71, 40, 58, 22, 6, 34,
                    54, 69, 110, 18, 74, 111, 80, 52, 90, 44, 4, 29, 84, 95,
                    21, 25, 10, 10, 60, 18, 78, 23, 21, 114, 92, 96, 17, 127,
                    53, 86, 2, 60, 104, 8, 132, 44, 115, 6, 25, 80, 46, 12, 20,
                    44, 67, 136, 127, 50, 55, 70, 41, 90, 16, 10, 44, 32, 24,
                    106, 13, 104,
                ]),
                219,
            ),
            Merge(Key(vec![]), 71),
            Del(Key(vec![])),
            Set(Key(vec![0]), 146),
            Merge(Key(vec![13]), 155),
            Merge(Key(vec![]), 14),
            Del(Key(vec![])),
            Set(Key(vec![]), 150),
            Set(
                Key(vec![
                    13, 8, 3, 6, 9, 14, 3, 13, 7, 12, 13, 7, 13, 13, 1, 13, 5,
                    4, 3, 2, 6, 16, 17, 10, 0, 16, 12, 0, 16, 1, 0, 15, 15, 4,
                    1, 6, 9, 9, 11, 16, 7, 6, 10, 1, 11, 10, 4, 9, 9, 14, 4,
                    12, 16, 10, 15, 2, 1, 8, 4,
                ]),
                247,
            ),
            Del(Key(vec![154])),
            Del(Key(vec![])),
            Del(Key(vec![
                0, 24, 24, 31, 40, 23, 10, 30, 16, 41, 30, 23, 14, 25, 21, 19,
                18, 7, 17, 41, 11, 5, 14, 42, 11, 22, 4, 8, 4, 38, 33, 31, 3,
                30, 40, 22, 40, 39, 5, 40, 1, 41, 11, 26, 25, 33, 12, 38, 4,
                35, 30, 42, 19, 26, 23, 22, 39, 18, 29, 4, 1, 24, 14, 38, 0,
                36, 27, 11, 27, 34, 16, 15, 38, 0, 20, 37, 22, 31, 12, 26, 16,
                4, 22, 25, 4, 34, 4, 33, 37, 28, 18, 4, 41, 15, 8, 16, 27, 3,
                20, 26, 40, 31, 15, 15, 17, 15, 5, 13, 22, 37, 7, 13, 35, 14,
                6, 28, 21, 26, 13, 35, 1, 10, 8, 34, 23, 27, 29, 8, 14, 42, 36,
                31, 34, 12, 31, 24, 5, 8, 11, 36, 29, 24, 38, 8, 12, 18, 22,
                36, 21, 28, 11, 24, 0, 41, 37, 39, 42, 25, 13, 41, 27, 8, 24,
                22, 30, 17, 2, 4, 20, 33, 5, 24, 33, 6, 29, 5, 0, 17, 9, 20,
                26, 15, 23, 22, 16, 23, 16, 1, 20, 0, 28, 16, 34, 30, 19, 5,
                36, 40, 28, 6, 39,
            ])),
            Merge(Key(vec![]), 50),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_28() {
    // postmortem:
    prop_tree_matches_btreemap(
        vec![
            Del(Key(vec![])),
            Set(Key(vec![]), 65),
            Del(Key(vec![])),
            Del(Key(vec![])),
            Merge(Key(vec![]), 50),
            Merge(Key(vec![]), 2),
            Del(Key(vec![197])),
            Merge(Key(vec![5]), 146),
            Set(Key(vec![222]), 224),
            Merge(Key(vec![149]), 60),
            Scan(Key(vec![178]), 18),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_29() {
    // postmortem: tree merge and split thresholds caused an infinite
    // loop while performing updates
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![]), 142),
            Merge(
                Key(vec![
                    45, 47, 6, 67, 16, 12, 62, 35, 69, 80, 49, 61, 29, 82, 9,
                    47, 25, 78, 47, 64, 29, 74, 45, 0, 37, 44, 21, 82, 55, 44,
                    31, 60, 86, 18, 45, 67, 55, 21, 35, 46, 25, 51, 5, 32, 33,
                    36, 1, 81, 28, 28, 79, 76, 80, 89, 80, 62, 8, 85, 50, 15,
                    4, 11, 76, 72, 73, 47, 30, 50, 85, 67, 84, 13, 82, 84, 78,
                    70, 42, 83, 8, 7, 50, 77, 85, 37, 47, 82, 86, 46, 30, 27,
                    5, 39, 70, 26, 59, 16, 6, 34, 56, 40, 40, 67, 16, 61, 63,
                    56, 64, 31, 15, 81, 84, 19, 61, 66, 3, 7, 40, 56, 13, 40,
                    64, 50, 88, 47, 88, 50, 63, 65, 79, 62, 1, 44, 59, 27, 12,
                    60, 3, 36, 89, 45, 18, 4, 68, 48, 61, 30, 48, 26, 84, 49,
                    3, 74, 51, 53, 30, 57, 50, 35, 74, 59, 30, 73, 19, 30, 82,
                    78, 3, 5, 62, 17, 48, 29, 67, 52, 45, 61, 74, 52, 29, 61,
                    63, 11, 89, 76, 34, 8, 50, 75, 42, 12, 5, 55, 0, 59, 44,
                    68, 26, 76, 37, 50, 53, 73, 53, 76, 57, 40, 30, 52, 0, 41,
                    21, 8, 79, 79, 38, 37, 50, 56, 43, 9, 85, 21, 60, 64, 13,
                    54, 60, 83, 1, 2, 37, 75, 42, 0, 83, 81, 80, 87, 12, 15,
                    75, 55, 41, 59, 9, 80, 66, 27, 65, 26, 48, 29, 37, 38, 9,
                    76, 31, 39, 35, 22, 73, 59, 28, 33, 35, 63, 78, 17, 22, 82,
                    12, 60, 49, 26, 54, 19, 60, 29, 39, 37, 10, 50, 12, 19, 29,
                    1, 74, 12, 5, 38, 49, 41, 19, 88, 3, 27, 77, 81, 72, 42,
                    71, 86, 82, 11, 79, 40, 35, 26, 35, 64, 4, 33, 87, 31, 84,
                    81, 74, 31, 49, 0, 29, 73, 14, 55, 78, 21, 23, 20, 83, 48,
                    89, 88, 62, 64, 73, 7, 20, 70, 81, 64, 3, 79, 38, 75, 13,
                    40, 29, 82, 40, 14, 66, 56, 54, 52, 37, 14, 67, 8, 37, 1,
                    5, 73, 14, 35, 63, 48, 46, 22, 84, 71, 2, 60, 63, 88, 14,
                    15, 69, 88, 2, 43, 57, 43, 52, 18, 78, 75, 75, 74, 13, 35,
                    50, 35, 17, 13, 64, 82, 55, 32, 14, 57, 35, 77, 65, 22, 40,
                    27, 39, 80, 23, 20, 41, 50, 48, 22, 84, 37, 59, 45, 64, 10,
                    3, 69, 56, 24, 4, 25, 76, 65, 47, 52, 64, 88, 3, 23, 37,
                    16, 56, 69, 71, 27, 87, 65, 74, 23, 82, 41, 60, 78, 75, 22,
                    51, 15, 57, 80, 46, 73, 7, 1, 36, 64, 0, 56, 83, 74, 62,
                    73, 81, 68, 71, 63, 31, 5, 23, 11, 15, 39, 2, 10, 23, 18,
                    74, 3, 43, 25, 68, 54, 11, 21, 14, 58, 10, 73, 0, 66, 28,
                    73, 25, 40, 55, 56, 33, 81, 67, 43, 35, 65, 38, 21, 48, 81,
                    4, 77, 68, 51, 38, 36, 49, 43, 33, 51, 28, 43, 60, 71, 78,
                    48, 49, 76, 21, 0, 72, 0, 32, 78, 12, 87, 5, 80, 62, 40,
                    85, 26, 70, 58, 56, 78, 7, 53, 30, 16, 22, 12, 23, 37, 83,
                    45, 33, 41, 83, 78, 87, 44, 0, 65, 51, 3, 8, 72, 38, 14,
                    24, 64, 77, 45, 5, 1, 7, 27, 82, 7, 6, 70, 25, 67, 22, 8,
                    30, 76, 41, 11, 14, 1, 65, 85, 60, 80, 0, 30, 31, 79, 43,
                    89, 33, 84, 22, 7, 67, 45, 39, 74, 75, 12, 61, 19, 71, 66,
                    83, 57, 38, 45, 21, 18, 37, 54, 36, 14, 54, 63, 81, 12, 7,
                    10, 39, 16, 40, 10, 7, 81, 45, 12, 22, 20, 29, 85, 40, 41,
                    72, 79, 58, 50, 41, 59, 64, 41, 32, 56, 35, 8, 60, 17, 14,
                    89, 17, 7, 48, 6, 35, 9, 34, 54, 6, 44, 87, 76, 50, 1, 67,
                    70, 15, 8, 4, 45, 67, 86, 32, 69, 3, 88, 85, 72, 66, 21,
                    89, 11, 77, 1, 50, 75, 56, 41, 74, 6, 4, 51, 65, 39, 50,
                    45, 56, 3, 19, 80, 86, 55, 48, 81, 17, 3, 89, 7, 9, 63, 58,
                    80, 39, 34, 85, 55, 71, 41, 55, 8, 63, 38, 51, 47, 49, 83,
                    2, 73, 22, 39, 18, 45, 77, 56, 80, 54, 13, 23, 81, 54, 15,
                    48, 57, 83, 71, 41, 32, 64, 1, 9, 46, 27, 16, 21, 7, 28,
                    55, 17, 71, 68, 17, 74, 46, 38, 84, 3, 12, 71, 63, 16, 23,
                    48, 12, 29, 28, 5, 21, 61, 14, 77, 66, 62, 57, 18, 30, 63,
                    14, 41, 37, 30, 73, 16, 12, 74, 8, 82, 67, 53, 10, 5, 37,
                    36, 39, 52, 37, 72, 76, 21, 35, 40, 42, 55, 47, 50, 41, 19,
                    40, 86, 26, 54, 23, 74, 46, 66, 59, 80, 26, 81, 61, 80, 88,
                    55, 40, 30, 45, 7, 46, 21, 3, 20, 46, 63, 18, 9, 34, 67, 9,
                    19, 52, 53, 29, 69, 78, 65, 39, 71, 40, 38, 57, 80, 27, 34,
                    30, 27, 55, 8, 65, 31, 37, 33, 25, 39, 46, 9, 83, 6, 27,
                    28, 61, 9, 21, 58, 21, 10, 69, 24, 5, 31, 32, 44, 26, 84,
                    73, 73, 9, 64, 26, 21, 85, 12, 39, 81, 38, 49, 24, 35, 3,
                    88, 15, 15, 76, 64, 70, 9, 30, 51, 26, 16, 70, 60, 15, 7,
                    54, 36, 32, 9, 10, 18, 66, 19, 25, 77, 46, 51, 51, 14, 41,
                    56, 65, 41, 87, 26, 10, 2, 73, 2, 71, 26, 56, 10, 68, 15,
                    53, 10, 43, 15, 22, 45, 2, 15, 16, 69, 80, 83, 18, 22, 70,
                    77, 52, 48, 24, 17, 40, 56, 22, 17, 3, 36, 46, 37, 41, 22,
                    0, 41, 45, 14, 15, 73, 18, 42, 34, 5, 87, 6, 2, 7, 58, 3,
                    86, 87, 7, 79, 88, 33, 30, 48, 3, 66, 27, 34, 58, 48, 71,
                    40, 1, 46, 84, 32, 63, 79, 0, 21, 71, 1, 59, 39, 77, 51,
                    14, 20, 58, 83, 19, 0, 2, 2, 57, 73, 79, 42, 59, 33, 50,
                    15, 11, 48, 25, 14, 39, 36, 88, 71, 28, 45, 15, 59, 39, 60,
                    78, 18, 18, 45, 50, 29, 66, 86, 5, 76, 85, 55, 17, 28, 8,
                    39, 75, 33, 9, 73, 71, 59, 56, 57, 86, 6, 75, 26, 43, 68,
                    34, 82, 88, 76, 17, 86, 63, 2, 38, 63, 13, 44, 8, 25, 0,
                    63, 54, 73, 52, 3, 72,
                ]),
                9,
            ),
            Set(Key(vec![]), 35),
            Set(
                Key(vec![
                    165, 64, 99, 55, 152, 102, 148, 35, 59, 10, 198, 191, 71,
                    129, 170, 155, 7, 106, 171, 93, 126,
                ]),
                212,
            ),
            Del(Key(vec![])),
            Merge(Key(vec![]), 177),
            Merge(
                Key(vec![
                    20, 55, 154, 104, 10, 68, 64, 3, 31, 78, 232, 227, 169,
                    161, 13, 50, 16, 239, 87, 0, 9, 85, 248, 32, 156, 106, 11,
                    18, 57, 13, 177, 36, 69, 176, 101, 92, 119, 38, 218, 26, 4,
                    154, 185, 135, 75, 167, 101, 107, 206, 76, 153, 213, 70,
                    52, 205, 95, 55, 116, 242, 68, 77, 90, 249, 142, 93, 135,
                    118, 127, 116, 121, 235, 183, 215, 2, 118, 193, 146, 185,
                    4, 129, 167, 164, 178, 105, 149, 47, 73, 121, 95, 23, 216,
                    153, 23, 108, 141, 190, 250, 121, 98, 229, 33, 106, 89,
                    117, 122, 145, 47, 242, 81, 88, 141, 38, 177, 170, 167, 56,
                    24, 196, 61, 97, 83, 91, 202, 181, 75, 112, 3, 169, 61, 17,
                    100, 81, 111, 178, 122, 176, 95, 185, 169, 146, 239, 40,
                    168, 32, 170, 34, 172, 89, 59, 188, 170, 186, 61, 7, 177,
                    230, 130, 155, 208, 171, 82, 153, 20, 72, 74, 111, 147,
                    178, 164, 157, 71, 114, 216, 40, 85, 91, 20, 145, 149, 95,
                    36, 114, 24, 129, 144, 229, 14, 133, 77, 92, 139, 167, 48,
                    18, 178, 4, 15, 171, 171, 88, 74, 104, 157, 2, 121, 13,
                    141, 6, 107, 118, 228, 147, 152, 28, 206, 128, 102, 150, 1,
                    129, 84, 171, 119, 110, 198, 72, 100, 166, 153, 98, 66,
                    128, 79, 41, 126,
                ]),
                103,
            ),
            Del(Key(vec![])),
            Merge(
                Key(vec![
                    117, 48, 90, 153, 149, 191, 229, 73, 3, 6, 73, 52, 73, 186,
                    42, 53, 94, 17, 61, 11, 153, 118, 219, 188, 184, 89, 13,
                    124, 138, 40, 238, 9, 46, 45, 38, 115, 153, 106, 166, 56,
                    134, 206, 140, 57, 95, 244, 27, 135, 43, 13, 143, 137, 56,
                    122, 243, 205, 52, 116, 130, 35, 80, 167, 58, 93,
                ]),
                8,
            ),
            Set(Key(vec![145]), 43),
            GetLt(Key(vec![229])),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_30() {
    // postmortem:
    prop_tree_matches_btreemap(
        vec![
            Merge(Key(vec![]), 241),
            Set(Key(vec![20]), 146),
            Merge(
                Key(vec![
                    60, 38, 29, 57, 35, 71, 15, 46, 7, 27, 76, 84, 27, 25, 90,
                    30, 37, 63, 11, 24, 27, 28, 94, 93, 82, 68, 69, 61, 46, 86,
                    11, 86, 63, 34, 90, 71, 92, 87, 38, 48, 40, 78, 9, 37, 26,
                    36, 60, 4, 2, 38, 32, 73, 86, 43, 52, 79, 11, 43, 59, 21,
                    60, 40, 80, 94, 69, 44, 4, 73, 59, 16, 16, 22, 88, 41, 13,
                    21, 91, 33, 49, 91, 20, 79, 23, 61, 53, 63, 58, 62, 49, 10,
                    71, 72, 27, 55, 53, 39, 91, 82, 86, 38, 41, 1, 54, 3, 77,
                    15, 93, 31, 49, 29, 82, 7, 17, 58, 42, 12, 49, 67, 62, 46,
                    20, 27, 61, 32, 58, 9, 17, 19, 28, 44, 41, 34, 94, 11, 50,
                    73, 1, 50, 48, 8, 88, 33, 40, 51, 15, 35, 2, 36, 37, 30,
                    37, 83, 71, 91, 32, 0, 69, 28, 64, 30, 72, 63, 39, 7, 89,
                    0, 21, 51, 92, 80, 13, 57, 7, 53, 94, 26, 2, 63, 18, 23,
                    89, 34, 83, 55, 32, 75, 81, 27, 11, 5, 63, 0, 75, 12, 39,
                    9, 13, 20, 25, 57, 94, 75, 59, 46, 84, 80, 61, 24, 31, 7,
                    68, 93, 12, 94, 6, 94, 27, 33, 81, 19, 3, 78, 3, 14, 22,
                    36, 49, 61, 51, 79, 43, 35, 58, 54, 65, 72, 36, 87, 3, 3,
                    25, 75, 82, 58, 75, 76, 29, 89, 1, 16, 64, 63, 85, 0, 47,
                ]),
                11,
            ),
            Merge(Key(vec![25]), 245),
            Merge(Key(vec![119]), 152),
            Scan(Key(vec![]), 31),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_31() {
    // postmortem:
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![1]), 212),
            Set(Key(vec![12]), 174),
            Set(Key(vec![]), 182),
            Set(
                Key(vec![
                    12, 55, 46, 38, 40, 34, 44, 32, 19, 15, 28, 49, 35, 40, 55,
                    35, 61, 9, 62, 18, 3, 58,
                ]),
                86,
            ),
            Scan(Key(vec![]), -18),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_32() {
    // postmortem: the MAX_IVEC that predecessor used in reverse
    // iteration was setting the first byte to 0 even though we
    // no longer perform per-key prefix encoding.
    prop_tree_matches_btreemap(
        vec![Set(Key(vec![57]), 141), Scan(Key(vec![]), -40)],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_33() {
    // postmortem: the split point was being incorrectly
    // calculated when using the simplified prefix technique.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![]), 91),
            Set(Key(vec![1]), 216),
            Set(Key(vec![85, 25]), 78),
            Set(Key(vec![85]), 43),
            GetLt(Key(vec![])),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_34() {
    // postmortem: a safety check was too aggressive when
    // finding predecessors using the new simplified prefix
    // encoding technique.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![9, 212]), 100),
            Set(Key(vec![9]), 63),
            Set(Key(vec![5]), 100),
            Merge(Key(vec![]), 16),
            Set(Key(vec![9, 70]), 188),
            Scan(Key(vec![]), -40),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_35() {
    // postmortem: prefix lengths were being incorrectly
    // handled on splits.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![207]), 29),
            Set(Key(vec![192]), 218),
            Set(Key(vec![121]), 167),
            Set(Key(vec![189]), 40),
            Set(Key(vec![85]), 197),
            Set(Key(vec![185]), 58),
            Set(Key(vec![84]), 97),
            Set(Key(vec![23]), 34),
            Set(Key(vec![47]), 162),
            Set(Key(vec![39]), 92),
            Set(Key(vec![46]), 173),
            Set(Key(vec![33]), 202),
            Set(Key(vec![8]), 113),
            Set(Key(vec![17]), 228),
            Set(Key(vec![8, 49]), 217),
            Set(Key(vec![6]), 192),
            Set(Key(vec![5]), 47),
            Set(Key(vec![]), 5),
            Set(Key(vec![0]), 103),
            Set(Key(vec![1]), 230),
            Set(Key(vec![0, 229]), 117),
            Set(Key(vec![]), 112),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_36() {
    // postmortem: suffix truncation caused
    // regions to be permanently inaccessible
    // when applied to split points on index
    // nodes.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![152]), 65),
            Set(Key(vec![]), 227),
            Set(Key(vec![101]), 23),
            Merge(Key(vec![254]), 97),
            Set(Key(vec![254, 5]), 207),
            Scan(Key(vec![]), -30),
        ],
        0,
        false,
        false,
    );
}

#[test]
fn tree_bug_37() {
    // postmortem: suffix truncation was so
    // aggressive that it would cut into
    // the prefix in the lo key sometimes.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![]), 82),
            Set(Key(vec![2, 0]), 40),
            Set(Key(vec![2, 0, 0]), 49),
            Set(Key(vec![1]), 187),
            Scan(Key(vec![]), 33),
        ],
        0,
        false,
        false,
    );
}

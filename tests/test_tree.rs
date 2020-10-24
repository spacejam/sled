mod common;
mod tree;

use std::{sync::Arc, time::Duration};

#[allow(unused_imports)]
use log::{debug, warn};

use quickcheck::{QuickCheck, StdGen};

use sled::Transactional;
use sled::{transaction::*, *};

use tree::{
    prop_tree_matches_btreemap, Key,
    Op::{self, *},
};

const N_THREADS: usize = 10;
const N_PER_THREAD: usize = 100;
const N: usize = N_THREADS * N_PER_THREAD; // NB N should be multiple of N_THREADS
const SPACE: usize = N;

#[allow(dead_code)]
const INTENSITY: usize = 5;

fn kv(i: usize) -> Vec<u8> {
    let i = i % SPACE;
    let k = [(i >> 16) as u8, (i >> 8) as u8, i as u8];
    k.to_vec()
}

#[test]
#[cfg_attr(miri, ignore)]
fn very_large_reverse_tree_iterator() {
    let mut a = vec![255; 1024 * 1024];
    a.push(0);
    let mut b = vec![255; 1024 * 1024];
    b.push(1);

    let db = Config::new()
        .temporary(true)
        .flush_every_ms(Some(1))
        .segment_size(256)
        .open()
        .unwrap();

    db.insert(a, "").unwrap();
    db.insert(b, "").unwrap();

    assert_eq!(db.iter().rev().count(), 2);
}

#[test]
#[cfg(all(target_os = "linux", not(miri)))]
fn test_varied_compression_ratios() {
    // tests for the compression issue reported in #938 by @Mrmaxmeier.

    let low_entropy = vec![0u8; 64 << 10]; // 64k zeroes
    let high_entropy = {
        // 64mb random
        use std::fs::File;
        use std::io::Read;
        let mut buf = vec![0u8; 64 << 20];
        File::open("/dev/urandom").unwrap().read_exact(&mut buf).unwrap();
        buf
    };

    let tree = sled::Config::default()
        .use_compression(true)
        .path("compression_db_test")
        .open()
        .unwrap();

    tree.insert(b"low  entropy", &low_entropy[..]).unwrap();
    tree.insert(b"high entropy", &high_entropy[..]).unwrap();

    println!("reloading database...");
    drop(tree);
    let tree = sled::Config::default()
        .use_compression(true)
        .path("compression_db_test")
        .open()
        .unwrap();
    drop(tree);

    let _ = std::fs::remove_dir_all("compression_db_test");
}

#[test]
#[cfg(not(miri))] // can't create threads
fn concurrent_tree_pops() -> sled::Result<()> {
    use std::thread;

    let db = sled::Config::new().temporary(true).open()?;

    // Insert values 0..5
    for x in 0u32..5 {
        db.insert(x.to_be_bytes(), &[])?;
    }

    let mut threads = vec![];

    // Pop 5 values using multiple threads
    for _ in 0..5 {
        let db = db.clone();
        threads.push(thread::spawn(move || {
            db.pop_min().unwrap().unwrap();
        }));
    }

    for thread in threads.into_iter() {
        thread.join().unwrap();
    }

    assert!(
        db.is_empty(),
        "elements left in database: {:?}",
        db.iter().collect::<Vec<_>>()
    );

    Ok(())
}

#[test]
#[cfg(not(miri))] // can't create threads
fn concurrent_tree_ops() {
    use std::thread;

    common::setup_logger();

    for i in 0..INTENSITY {
        debug!("beginning test {}", i);

        let config = Config::new()
            .temporary(true)
            .flush_every_ms(Some(1))
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
            let mut k2 = k;
            k2.reverse();
            tree.compare_and_swap(&k1, Some(&*k1), Some(k2)).unwrap().unwrap();
        }};

        drop(t);
        let t =
            Arc::new(config.open().expect("should be able to restart Tree"));

        par! {t, |tree: &Tree, k: Vec<u8>| {
            let k1 = k.clone();
            let mut k2 = k;
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
#[cfg(not(miri))] // can't create threads
fn concurrent_tree_iter() -> Result<()> {
    use std::sync::Barrier;
    use std::thread;

    common::setup_logger();

    const N_FORWARD: usize = INTENSITY;
    const N_REVERSE: usize = INTENSITY;

    let config = Config::new().temporary(true).flush_every_ms(Some(1));

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
#[cfg(not(miri))] // can't create threads
fn concurrent_tree_transactions() -> TransactionResult<()> {
    use std::sync::Barrier;

    common::setup_logger();

    let config = Config::new()
        .temporary(true)
        .flush_every_ms(Some(1))
        .use_compression(true);
    let db = config.open().unwrap();

    db.insert(b"k1", b"cats").unwrap();
    db.insert(b"k2", b"dogs").unwrap();

    let mut threads: Vec<std::thread::JoinHandle<TransactionResult<()>>> =
        vec![];

    const N_WRITERS: usize = 30;
    const N_READERS: usize = 5;
    const N_SUBSCRIBERS: usize = 5;

    let barrier = Arc::new(Barrier::new(N_WRITERS + N_READERS + N_SUBSCRIBERS));

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

    for _ in 0..N_SUBSCRIBERS {
        let db = db.clone();
        let barrier = barrier.clone();
        let thread = std::thread::spawn(move || {
            barrier.wait();
            let sub = db.watch_prefix(b"k1");
            drop(db);

            while sub.next_timeout(Duration::from_millis(100)).is_ok() {}
            drop(sub);

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
fn tree_flush_in_transaction() {
    let config = sled::Config::new().temporary(true);
    let db = config.open().unwrap();
    let tree = db.open_tree(b"a").unwrap();

    tree.transaction::<_, _, sled::transaction::TransactionError>(|tree| {
        tree.insert(b"k1", b"cats")?;
        tree.insert(b"k2", b"dogs")?;
        tree.flush();
        Ok(())
    })
    .unwrap();
}

#[test]
fn incorrect_multiple_db_transactions() -> TransactionResult<()> {
    common::setup_logger();

    let db1 =
        Config::new().temporary(true).flush_every_ms(Some(1)).open().unwrap();
    let db2 =
        Config::new().temporary(true).flush_every_ms(Some(1)).open().unwrap();

    let result: TransactionResult<()> =
        (&*db1, &*db2).transaction::<_, ()>(|_| Ok(()));

    assert!(result.is_err());

    Ok(())
}

#[test]
fn many_tree_transactions() -> TransactionResult<()> {
    common::setup_logger();

    let config = Config::new().temporary(true).flush_every_ms(Some(1));
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
fn batch_outside_of_transaction() -> TransactionResult<()> {
    common::setup_logger();

    let config = Config::new().temporary(true).flush_every_ms(Some(1));
    let db = config.open().unwrap();

    let t1 = db.open_tree(b"1")?;

    let mut b1 = Batch::default();
    b1.insert(b"k1", b"v1");
    b1.insert(b"k2", b"v2");

    t1.transaction(|tree| {
        tree.apply_batch(&b1)?;
        Ok(())
    })?;

    assert_eq!(t1.get(b"k1")?, Some(b"v1".into()));
    assert_eq!(t1.get(b"k2")?, Some(b"v2".into()));
    Ok(())
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
#[cfg_attr(miri, ignore)]
fn tree_small_keys_iterator() {
    let config = Config::new().temporary(true).flush_every_ms(Some(1));
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
#[cfg_attr(miri, ignore)]
fn tree_big_keys_iterator() {
    fn kv(i: usize) -> Vec<u8> {
        let k = [(i >> 16) as u8, (i >> 8) as u8, i as u8];

        let mut base = vec![0; u8::max_value() as usize];
        base.extend_from_slice(&k);
        base
    }

    let config = Config::new().temporary(true).flush_every_ms(Some(1));

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
fn tree_subscribers_and_keyspaces() -> Result<()> {
    let config = Config::new().temporary(true).flush_every_ms(Some(1));

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

    assert_eq!(t1.get(b""), Err(Error::CollectionNotFound(b"1".into())));

    assert_eq!(t2.get(b""), Err(Error::CollectionNotFound(b"2".into())));

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

    let config = Config::new().temporary(true).flush_every_ms(Some(1));
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
#[cfg_attr(miri, ignore)]
fn recover_tree() {
    common::setup_logger();

    let config = Config::new()
        .temporary(true)
        .flush_every_ms(Some(1))
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
fn create_tree() {
    common::setup_logger();

    let path = "create_exclusive_db";
    let _ = std::fs::remove_dir_all(path);

    {
        let config = Config::new().create_new(true).path(path);
        config.open().unwrap();
    }

    let config = Config::new().create_new(true).path(path);
    config.open().unwrap_err();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg_attr(miri, ignore)]
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
#[cfg_attr(any(target_os = "fuchsia", miri), ignore)]
fn quickcheck_tree_matches_btreemap() {
    let n_tests = if cfg!(windows) { 25 } else { 100 };

    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 1000))
        .tests(n_tests)
        .max_tests(n_tests * 10)
        .quickcheck(
            prop_tree_matches_btreemap
                as fn(Vec<Op>, bool, bool, u8, u8) -> bool,
        );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_00() {
    // postmortem:
    prop_tree_matches_btreemap(vec![Restart], false, false, 0, 0);
}

#[test]
#[cfg_attr(miri, ignore)]
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_03() {
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_04() {
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_05() {
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_06() {
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_07() {
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_08() {
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_09() {
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        true,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_16() {
    // postmortem: the test merge function was not properly adding numbers.
    prop_tree_matches_btreemap(
        vec![Merge(Key(vec![247]), 162), Scan(Key(vec![209]), 31)],
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_17() {
    // postmortem: we were creating a copy of a node leaf during iteration
    // before accidentally putting it into a PinnedValue, despite the
    // fact that it was not actually part of the node's actual memory!
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![194, 215, 103, 0, 138, 11, 248, 131]), 70),
            Scan(Key(vec![]), 30),
        ],
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_23() {
    // postmortem: when rewriting CRC handling code, mis-sized the blob crc
    prop_tree_matches_btreemap(
        vec![Set(Key(vec![6; 5120]), 92), Restart, Scan(Key(vec![]), 35)],
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_25() {
    // postmortem: was not accounting for merges when traversing
    // the frag chain and a Del was encountered
    prop_tree_matches_btreemap(
        vec![Del(Key(vec![])), Merge(Key(vec![]), 84), Get(Key(vec![]))],
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_32() {
    // postmortem: the MAX_IVEC that predecessor used in reverse
    // iteration was setting the first byte to 0 even though we
    // no longer perform per-key prefix encoding.
    prop_tree_matches_btreemap(
        vec![Set(Key(vec![57]), 141), Scan(Key(vec![]), -40)],
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
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
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_38() {
    // postmortem: Free pages were not being initialized in the
    // pagecache properly.
    for _ in 0..10 {
        prop_tree_matches_btreemap(
            vec![
                Set(Key(vec![193]), 73),
                Merge(Key(vec![117]), 216),
                Set(Key(vec![221]), 176),
                GetLt(Key(vec![123])),
                Restart,
            ],
            false,
            false,
            0,
            0,
        );
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_39() {
    // postmortem:
    for _ in 0..100 {
        prop_tree_matches_btreemap(
            vec![
                Set(
                    Key(vec![
                        67, 48, 34, 254, 61, 189, 196, 127, 26, 185, 244, 63,
                        60, 63, 246, 194, 243, 177, 218, 210, 153, 126, 124,
                        47, 160, 242, 157, 2, 51, 34, 88, 41, 44, 65, 58, 211,
                        245, 74, 192, 101, 222, 68, 196, 250, 127, 231, 102,
                        177, 246, 105, 190, 144, 113, 148, 71, 72, 149, 246,
                        38, 95, 106, 42, 83, 65, 84, 73, 148, 34, 95, 88, 57,
                        232, 219, 227, 74, 14, 5, 124, 106, 57, 244, 50, 81,
                        93, 145, 111, 40, 190, 127, 227, 17, 242, 165, 194,
                        171, 60, 6, 255, 176, 143, 131, 164, 217, 18, 123, 19,
                        246, 183, 29, 0, 6, 39, 175, 57, 134, 166, 231, 47,
                        254, 158, 163, 178, 78, 240, 108, 157, 72, 135, 34,
                        236, 103, 192, 109, 31, 2, 72, 128, 242, 4, 113, 109,
                        224, 120, 61, 169, 226, 131, 210, 33, 181, 91, 91, 197,
                        223, 127, 26, 94, 158, 55, 57, 3, 184, 15, 30, 2, 222,
                        39, 29, 12, 42, 14, 166, 176, 28, 13, 246, 11, 186, 8,
                        247, 113, 253, 102, 227, 68, 111, 227, 238, 54, 150,
                        11, 57, 155, 4, 75, 179, 17, 172, 42, 22, 199, 44, 242,
                        211, 0, 39, 243, 221, 114, 86, 145, 22, 226, 108, 32,
                        248, 42, 49, 191, 112, 1, 69, 101, 112, 251, 243, 252,
                        83, 140, 132, 165,
                    ]),
                    250,
                ),
                Del(Key(vec![
                    11, 77, 168, 37, 181, 169, 239, 146, 240, 211, 7, 115, 197,
                    119, 46, 80, 240, 92, 221, 108, 208, 247, 221, 129, 108,
                    13, 36, 21, 93, 11, 243, 103, 188, 39, 126, 77, 29, 32,
                    206, 175, 199, 245, 71, 96, 221, 7, 68, 64, 45, 78, 68,
                    193, 73, 13, 60, 13, 28, 167, 147, 7, 90, 11, 206, 44, 84,
                    243, 3, 77, 122, 87, 7, 125, 184, 6, 178, 59,
                ])),
                Merge(Key(vec![176]), 123),
                Restart,
                Merge(
                    Key(vec![
                        93, 43, 181, 76, 63, 247, 227, 15, 17, 239, 9, 252,
                        181, 53, 65, 74, 22, 18, 71, 64, 115, 58, 110, 30, 13,
                        177, 31, 47, 124, 14, 0, 157, 200, 194, 92, 215, 21,
                        36, 239, 204, 18, 88, 216, 149, 18, 208, 187, 188, 32,
                        76, 35, 12, 142, 157, 38, 186, 245, 63, 2, 230, 13, 79,
                        160, 86, 32, 170, 239, 151, 25, 180, 170, 201, 22, 211,
                        238, 208, 24, 139, 5, 44, 38, 48, 243, 38, 249, 36, 43,
                        200, 52, 244, 166, 0, 29, 114, 10, 18, 253, 253, 130,
                        223, 37, 8, 109, 228, 0, 122, 192, 16, 68, 231, 37,
                        230, 249, 180, 214, 101, 17,
                    ]),
                    176,
                ),
                Set(
                    Key(vec![
                        153, 217, 142, 179, 255, 74, 1, 20, 254, 1, 38, 28, 66,
                        244, 81, 101, 210, 58, 18, 107, 12, 116, 74, 188, 95,
                        56, 248, 9, 204, 128, 24, 239, 143, 83, 83, 213, 17,
                        32, 135, 73, 217, 8, 241, 44, 57, 131, 107, 139, 122,
                        32, 194, 225, 136, 148, 227, 196, 196, 121, 97, 81, 74,
                    ]),
                    42,
                ),
                Set(Key(vec![]), 160),
                GetLt(Key(vec![
                    244, 145, 243, 120, 149, 64, 125, 161, 98, 205, 205, 107,
                    191, 119, 83, 42, 92, 119, 25, 198, 47, 123, 26, 224, 190,
                    98, 144, 238, 74, 36, 76, 186, 226, 153, 69, 217, 109, 214,
                    201, 104, 148, 107, 132, 219, 37, 109, 98, 172, 70, 160,
                    177, 115, 194, 80, 76, 60, 148, 176, 191, 84, 109, 35, 51,
                    107, 157, 11, 233, 126, 71, 183, 215, 116, 72, 235, 218,
                    171, 233, 181, 53, 253, 104, 231, 138, 166, 40,
                ])),
                Set(
                    Key(vec![
                        37, 160, 29, 162, 43, 212, 2, 100, 236, 24, 2, 82, 58,
                        38, 81, 137, 89, 55, 164, 83,
                    ]),
                    64,
                ),
                Get(Key(vec![
                    15, 53, 101, 33, 156, 199, 212, 82, 2, 64, 136, 70, 235,
                    72, 170, 188, 180, 200, 109, 231, 6, 13, 30, 70, 4, 132,
                    133, 101, 82, 187, 78, 241, 157, 49, 156, 3, 17, 167, 216,
                    209, 7, 174, 112, 186, 170, 189, 85, 99, 119, 52, 39, 38,
                    151, 108, 203, 42, 63, 255, 216, 234, 34, 2, 80, 168, 122,
                    70, 20, 11, 220, 106, 49, 110, 165, 170, 149, 163,
                ])),
                GetLt(Key(vec![])),
                Merge(Key(vec![136]), 135),
                Cas(Key(vec![177]), 159, 209),
                Cas(Key(vec![101]), 143, 240),
                Set(Key(vec![226, 62, 34, 63, 172, 96, 162]), 43),
                Merge(
                    Key(vec![
                        48, 182, 144, 255, 137, 100, 2, 139, 69, 111, 159, 133,
                        234, 147, 118, 231, 155, 74, 73, 98, 58, 36, 35, 21,
                        50, 42, 71, 25, 200, 5, 4, 198, 158, 41, 88, 75, 153,
                        254, 248, 213, 0, 89, 43, 160, 58, 206, 88, 107, 57,
                        208, 119, 34, 80, 166, 112, 13, 241, 46, 172, 115, 179,
                        42, 59, 200, 225, 125, 65, 18, 173, 77, 27, 129, 228,
                        68, 53, 175, 61, 230, 27, 136, 131, 171, 64, 79, 125,
                        149, 52, 80,
                    ]),
                    105,
                ),
                Merge(
                    Key(vec![
                        126, 109, 165, 43, 2, 82, 97, 81, 59, 78, 243, 142, 37,
                        105, 109, 178, 25, 73, 50, 103, 107, 129, 213, 193,
                        158, 16, 63, 108, 160, 204, 78, 83, 2, 43, 66, 2, 18,
                        11, 147, 47, 106, 106, 141, 82, 65, 101, 99, 171, 178,
                        68, 106, 7, 190, 159, 105, 132, 155, 240, 155, 95, 66,
                        254, 239, 202, 168, 26, 207, 213, 116, 215, 141, 77, 7,
                        245, 174, 144, 39, 28,
                    ]),
                    122,
                ),
                Del(Key(vec![
                    13, 152, 171, 90, 130, 131, 232, 51, 173, 103, 255, 225,
                    156, 192, 146, 141, 94, 84, 39, 171, 152, 114, 133, 20,
                    125, 68, 57, 27, 33, 175, 37, 164, 40,
                ])),
                Scan(Key(vec![]), -34),
                Set(Key(vec![]), 85),
                Merge(Key(vec![112]), 104),
                Restart,
                Restart,
                Del(Key(vec![237])),
                Set(
                    Key(vec![
                        53, 79, 71, 234, 187, 78, 206, 117, 48, 84, 162, 101,
                        132, 137, 43, 144, 234, 23, 116, 13, 28, 184, 174, 241,
                        181, 201, 131, 156, 7, 103, 135, 17, 168, 249, 7, 120,
                        74, 8, 192, 134, 109, 54, 175, 130, 145, 206, 185, 49,
                        144, 133, 226, 244, 42, 126, 176, 232, 96, 56, 70, 56,
                        159, 127, 35, 39, 185, 114, 182, 41, 50, 93, 61,
                    ]),
                    144,
                ),
                Merge(
                    Key(vec![
                        10, 58, 6, 62, 17, 15, 26, 29, 79, 34, 77, 12, 93, 65,
                        87, 71, 19, 57, 25, 40, 53, 73, 57, 2, 81, 49, 67, 62,
                        78, 14, 34, 70, 86, 49, 86, 84, 16, 33, 24, 7, 87, 49,
                        58, 50, 13, 14, 35, 46, 7, 39, 76, 51, 21, 76, 9, 53,
                        45, 21, 71, 48, 16, 73, 68, 1, 63, 34, 12, 42, 11, 85,
                        79, 19, 11, 77, 90, 0, 62, 56, 37, 33, 10, 69, 20, 64,
                        15, 51, 64, 90, 69, 15, 7, 41, 53, 71, 52, 21, 45, 45,
                        49, 3, 59, 15, 90, 7, 12, 62, 30, 81,
                    ]),
                    131,
                ),
                Get(Key(vec![
                    79, 28, 48, 41, 5, 70, 54, 56, 36, 32, 59, 15, 26, 42, 61,
                    23, 53, 6, 71, 44, 61, 65, 4, 17, 23, 15, 65, 64, 46, 66,
                    27, 63, 51, 44, 35, 1, 8, 70, 7, 1, 13, 10, 40, 6, 36, 64,
                    68, 52, 8, 0, 46, 53, 48, 32, 9, 52, 69, 41, 8, 57, 27, 31,
                    79, 27, 12, 70, 72, 33, 6, 22, 47, 37, 11, 38, 32, 7, 31,
                    37, 45, 23, 74, 22, 46, 1, 3, 74, 72, 56, 52, 65, 78, 28,
                    5, 68, 30, 36, 5, 43, 7, 2, 48, 75, 16, 53, 31, 40, 9, 3,
                    49, 71, 70, 20, 24, 6, 23, 76, 49, 21, 12, 60, 54, 43, 7,
                    79, 74, 62, 53, 20, 46, 11, 74, 29, 31, 43, 20, 27, 22, 22,
                    15, 59, 12, 21, 61, 11, 8, 28, 5, 78, 70, 22, 11, 36, 62,
                    56, 44, 49, 25, 39, 37, 24, 72, 65, 67, 22, 48, 16, 50, 5,
                    10, 13, 36, 65, 29, 3, 26, 74, 15, 73, 78, 36, 14, 36, 30,
                    42, 19, 73, 65, 75, 2, 25, 1, 32, 38, 43, 58, 19, 37, 37,
                    48, 23, 72, 77, 34, 24, 1, 4, 42, 11, 68, 54, 23, 34, 0,
                    48, 20, 20, 23, 61, 65, 72, 64, 24, 63, 3, 21, 48, 63, 57,
                    40, 36, 46, 48, 8, 20, 62, 7, 69, 35, 79, 38, 45, 74, 7,
                    16, 48, 59, 56, 31, 13, 13,
                ])),
                Del(Key(vec![176, 58, 119])),
                Get(Key(vec![241])),
                Get(Key(vec![160])),
                Cas(Key(vec![]), 166, 235),
                Set(
                    Key(vec![
                        64, 83, 151, 149, 100, 93, 5, 18, 91, 58, 84, 156, 127,
                        108, 99, 168, 54, 51, 169, 185, 174, 101, 178, 148, 28,
                        91, 25, 138, 14, 133, 170, 97, 138, 180, 157, 131, 174,
                        22, 91, 108, 59, 165, 52, 28, 17, 175, 44, 95, 112, 38,
                        141, 46, 124, 49, 116, 55, 39, 109, 73, 181, 104, 86,
                        81, 150, 95, 149, 69, 110, 110, 102, 22, 62, 180, 60,
                        87, 127, 127, 136, 12, 139, 109, 165, 34, 181, 158,
                        156, 102, 38, 6, 149, 183, 69, 129, 98, 161, 175, 82,
                        51, 47, 93, 136, 16, 118, 65, 152, 139, 8, 30, 10, 100,
                        47, 13, 47, 179, 87, 19, 109, 78, 116, 20, 111, 89, 28,
                        0, 86, 39, 139, 7, 111, 40, 145, 155, 107, 45, 36, 90,
                        143, 154, 135, 36, 13, 98, 61, 150, 65, 128, 16, 52,
                        100, 128, 11, 5, 49, 143, 56, 78, 48, 62, 86, 50, 86,
                        41, 153, 53, 139, 89, 164, 33, 136, 83, 182, 53, 132,
                        144, 177, 105, 104, 55, 9, 174, 30, 65, 76, 33, 163,
                        172, 80, 169, 175, 54, 165, 173, 109, 24, 70, 25, 158,
                        135, 76, 130, 76, 9, 56, 20, 13, 133, 33, 168, 160,
                        153, 43, 80, 58, 56, 171, 28, 97, 122, 162, 32, 164,
                        11, 112, 177, 63, 47, 25, 0, 66, 87, 169, 118, 173, 27,
                        154, 79, 72, 107, 140, 126, 150, 60, 174, 184, 111,
                        155, 22, 32, 185, 149, 95, 60, 146, 165, 103, 34, 131,
                        91, 92, 85, 6, 102, 172, 131, 178, 141, 76, 84, 121,
                        49, 19, 66, 127, 45, 23, 159, 33, 138, 47, 36, 106, 39,
                        83, 164, 83, 16, 126, 126, 118, 84, 171,
                    ]),
                    143,
                ),
                Scan(Key(vec![165]), -26),
                Get(Key(vec![])),
                Del(Key(vec![])),
                Set(
                    Key(vec![
                        197, 224, 20, 219, 111, 246, 70, 138, 190, 237, 9, 202,
                        187, 160, 47, 10, 231, 14, 2, 131, 30, 202, 95, 48, 44,
                        21, 192, 155, 172, 51, 101, 155, 73, 5, 22, 140, 137,
                        11, 37, 79, 79, 92, 25, 107, 82, 145, 39, 45, 155, 136,
                        242, 8, 43, 71, 28, 70, 94, 79, 151, 20, 144, 53, 100,
                        196, 74, 140, 27, 224, 59, 1, 143, 136, 132, 85, 114,
                        166, 103, 242, 156, 183, 168, 148, 2, 33, 29, 201, 7,
                        96, 13, 33, 102, 172, 21, 96, 27, 1, 86, 149, 150, 119,
                        208, 118, 148, 51, 143, 54, 245, 89, 216, 145, 145, 72,
                        105, 51, 19, 14, 15, 18, 34, 16, 101, 172, 133, 32,
                        173, 106, 157, 15, 48, 194, 27, 55, 204, 110, 145, 99,
                        9, 37, 195, 206, 13, 246, 161, 100, 222, 235, 184, 12,
                        64, 103, 50, 158, 242, 163, 198, 61, 224, 130, 226,
                        187, 158, 175, 135, 54, 110, 33, 9, 59, 127, 135, 47,
                        204, 109, 105, 0, 161, 48, 247, 140, 101, 141, 81, 157,
                        80, 135, 228, 102, 44, 74, 53, 121, 116, 17, 56, 26,
                        112,
                    ]),
                    22,
                ),
                Set(Key(vec![110]), 222),
                Set(Key(vec![94]), 5),
                GetGt(Key(vec![
                    181, 161, 96, 186, 128, 24, 232, 74, 149, 3, 129, 98, 220,
                    25, 111, 111, 163, 244, 229, 137, 159, 137, 13, 12, 97,
                    150, 6, 88, 76, 77, 31, 36, 57, 54, 82, 85, 119, 250, 187,
                    163, 132, 73, 194, 129, 149, 176, 62, 118, 166, 50, 200,
                    28, 158, 184, 28, 139, 74, 87, 144, 87, 1, 73, 37, 46, 226,
                    91, 102, 13, 67, 195, 64, 189, 90, 190, 163, 216, 171, 22,
                    69, 234, 57, 134, 96, 198, 179, 115, 43, 160, 104, 252,
                    105, 192, 91, 211, 176, 171, 252, 236, 202, 158, 250, 186,
                    134, 154, 82, 17, 113, 175, 13, 125, 185, 101, 38, 236,
                    155, 30, 110, 11, 33, 198, 114, 184, 84, 91, 67, 125, 55,
                    188, 124, 242, 89, 124, 69, 18, 26, 137, 34, 33, 201, 58,
                    252, 134, 33, 131, 126, 136, 168, 20, 32, 237, 10, 57, 158,
                    149, 102, 62, 10, 98, 106, 10, 93, 78, 240, 205, 38, 186,
                    97, 104, 204, 14, 34, 100, 179, 161, 135, 136, 194, 99,
                ])),
                Merge(Key(vec![95]), 253),
                GetLt(Key(vec![99])),
                Merge(Key(vec![]), 124),
                Get(Key(vec![61])),
                Restart,
            ],
            false,
            false,
            0,
            0,
        );
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_40() {
    // postmortem: deletions of non-existant keys were
    // being persisted despite being unneccessary.
    prop_tree_matches_btreemap(
        vec![Del(Key(vec![99; 111222333]))],
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_41() {
    // postmortem: indexing of values during
    // iteration was incorrect.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![]), 131),
            Set(Key(vec![17; 1]), 214),
            Set(Key(vec![4; 1]), 202),
            Set(Key(vec![24; 1]), 79),
            Set(Key(vec![26; 1]), 235),
            Scan(Key(vec![]), 19),
        ],
        false,
        false,
        0,
        0,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_42() {
    // postmortem: during refactoring, accidentally
    // messed up the index selection for merge destinations.
    for _ in 0..100 {
        prop_tree_matches_btreemap(
            vec![
                Merge(Key(vec![]), 112),
                Set(Key(vec![110; 1]), 153),
                Set(Key(vec![15; 1]), 100),
                Del(Key(vec![110; 1])),
                GetLt(Key(vec![148; 1])),
            ],
            false,
            false,
            0,
            0,
        );
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_43() {
    // postmortem: when changing the PageState to always
    // include a base node, we did not account for this
    // in the tag + size compressed value. This was not
    // caught by the quickcheck tests because PageState's
    // Arbitrary implementation would ensure that at least
    // one frag was present, which was the invariant before
    // the base was extracted away from the vec of frags.
    prop_tree_matches_btreemap(
        vec![
            Set(Key(vec![241; 1]), 199),
            Set(Key(vec![]), 198),
            Set(Key(vec![72; 108]), 175),
            GetLt(Key(vec![])),
            Restart,
            Restart,
        ],
        false,
        false,
        0,
        52,
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_44() {
    // postmortem: off-by-one bug related to LSN recovery
    // where 1 was added to the index when the recovered
    // LSN was actually divisible by the segment size
    assert!(prop_tree_matches_btreemap(
        vec![
            Merge(Key(vec![]), 97),
            Merge(Key(vec![]), 41),
            Merge(Key(vec![]), 241),
            Set(Key(vec![21; 1]), 24),
            Del(Key(vec![])),
            Set(Key(vec![]), 145),
            Set(Key(vec![151; 1]), 187),
            Get(Key(vec![])),
            Restart,
            Set(Key(vec![]), 151),
            Restart,
        ],
        false,
        false,
        0,
        0,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_bug_45() {
    // postmortem: recovery was not properly accounting for
    // the possibility of a segment to be maxed out, similar
    // to bug 44.
    for _ in 0..10 {
        assert!(prop_tree_matches_btreemap(
            vec![
                Merge(Key(vec![206; 77]), 225),
                Set(Key(vec![88; 190]), 40),
                Set(Key(vec![162; 1]), 213),
                Merge(Key(vec![186; 1]), 175),
                Set(Key(vec![105; 16]), 111),
                Cas(Key(vec![]), 75, 252),
                Restart
            ],
            false,
            true,
            0,
            210
        ))
    }
}

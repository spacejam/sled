mod common;
mod tree;

use std::{
    io,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        Arc, Barrier,
    },
};

#[allow(unused_imports)]
use log::{debug, warn};

use quickcheck::{Gen, QuickCheck};

// use sled::Transactional;
// use sled::transaction::*;
use sled::{Config, Db as SledDb, InlineArray};

type Db = SledDb<3>;

use tree::{
    prop_tree_matches_btreemap,
    Op::{self},
};

const N_THREADS: usize = 32;
const N_PER_THREAD: usize = 10_000;
const N: usize = N_THREADS * N_PER_THREAD; // NB N should be multiple of N_THREADS
const SPACE: usize = N;

#[allow(dead_code)]
const INTENSITY: usize = 10;

fn kv(i: usize) -> InlineArray {
    let i = i % SPACE;
    let k = [(i >> 16) as u8, (i >> 8) as u8, i as u8];
    (&k).into()
}

#[test]
#[cfg_attr(miri, ignore)]
fn monotonic_inserts() {
    common::setup_logger();

    let db: Db = Config::tmp().unwrap().flush_every_ms(None).open().unwrap();

    for len in [1_usize, 16, 32, 1024].iter() {
        for i in 0_usize..*len {
            let mut k = vec![];
            for c in 0_usize..i {
                k.push((c % 256) as u8);
            }
            db.insert(&k, &[]).unwrap();
        }

        let count = db.iter().count();
        assert_eq!(count, *len as usize);

        let count2 = db.iter().rev().count();
        assert_eq!(count2, *len as usize);

        db.clear().unwrap();
    }

    for len in [1_usize, 16, 32, 1024].iter() {
        for i in (0_usize..*len).rev() {
            let mut k = vec![];
            for c in (0_usize..i).rev() {
                k.push((c % 256) as u8);
            }
            db.insert(&k, &[]).unwrap();
        }

        let count3 = db.iter().count();
        assert_eq!(count3, *len as usize);

        let count4 = db.iter().rev().count();
        assert_eq!(count4, *len as usize);

        db.clear().unwrap();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn fixed_stride_inserts() {
    // this is intended to test the fixed stride key omission optimization
    common::setup_logger();

    let db: Db = Config::tmp().unwrap().flush_every_ms(None).open().unwrap();

    let mut expected = std::collections::HashSet::new();
    for k in 0..4096_u16 {
        db.insert(&k.to_be_bytes(), &[]).unwrap();
        expected.insert(k.to_be_bytes().to_vec());
    }

    let mut count = 0_u16;
    for kvr in db.iter() {
        let (k, _) = kvr.unwrap();
        assert_eq!(&k, &count.to_be_bytes());
        count += 1;
    }
    assert_eq!(count, 4096, "tree: {:?}", db);
    assert_eq!(db.len(), 4096);

    let count = db.iter().rev().count();
    assert_eq!(count, 4096);

    for k in 0..4096_u16 {
        db.insert(&k.to_be_bytes(), &[1]).unwrap();
    }

    let count = db.iter().count();
    assert_eq!(count, 4096);

    let count = db.iter().rev().count();
    assert_eq!(count, 4096);
    assert_eq!(db.len(), 4096);

    for k in 0..4096_u16 {
        db.remove(&k.to_be_bytes()).unwrap();
    }

    let count = db.iter().count();
    assert_eq!(count, 0);

    let count = db.iter().rev().count();
    assert_eq!(count, 0);
    assert_eq!(db.len(), 0);
    assert!(db.is_empty().unwrap());
}

#[test]
#[cfg_attr(miri, ignore)]
fn sequential_inserts() {
    common::setup_logger();

    let db: Db = Config::tmp().unwrap().flush_every_ms(None).open().unwrap();

    for len in [1, 16, 32, u16::MAX].iter() {
        for i in 0..*len {
            db.insert(&i.to_le_bytes(), &[]).unwrap();
        }

        let count = db.iter().count();
        assert_eq!(count, *len as usize);

        let count2 = db.iter().rev().count();
        assert_eq!(count2, *len as usize);
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn reverse_inserts() {
    common::setup_logger();

    let db: Db = Config::tmp().unwrap().flush_every_ms(None).open().unwrap();

    for len in [1, 16, 32, u16::MAX].iter() {
        for i in 0..*len {
            let i2 = u16::MAX - i;
            db.insert(&i2.to_le_bytes(), &[]).unwrap();
        }

        let count = db.iter().count();
        assert_eq!(count, *len as usize);

        let count2 = db.iter().rev().count();
        assert_eq!(count2, *len as usize);
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn very_large_reverse_tree_iterator() {
    let mut a = vec![255; 1024 * 1024];
    a.push(0);
    let mut b = vec![255; 1024 * 1024];
    b.push(1);

    let db: Db = Config::tmp().unwrap().flush_every_ms(Some(1)).open().unwrap();

    db.insert(a, "").unwrap();
    db.insert(b, "").unwrap();

    assert_eq!(db.iter().rev().count(), 2);
}

#[test]
#[cfg(all(target_os = "linux", not(miri)))]
fn varied_compression_ratios() {
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

    let tree: Db =
        Config::default().path("compression_db_test").open().unwrap();

    tree.insert(b"low  entropy", &low_entropy[..]).unwrap();
    tree.insert(b"high entropy", &high_entropy[..]).unwrap();

    println!("reloading database...");
    drop(tree);
    let tree: Db =
        Config::default().path("compression_db_test").open().unwrap();
    drop(tree);

    let _ = std::fs::remove_dir_all("compression_db_test");
}

#[test]
fn test_pop_first() -> io::Result<()> {
    let config = sled::Config::tmp().unwrap();
    let db: sled::Db<4> = config.open()?;
    db.insert(&[0], vec![0])?;
    db.insert(&[1], vec![10])?;
    db.insert(&[2], vec![20])?;
    db.insert(&[3], vec![30])?;
    db.insert(&[4], vec![40])?;
    db.insert(&[5], vec![50])?;

    assert_eq!(&db.pop_first()?.unwrap().0, &[0]);
    assert_eq!(&db.pop_first()?.unwrap().0, &[1]);
    assert_eq!(&db.pop_first()?.unwrap().0, &[2]);
    assert_eq!(&db.pop_first()?.unwrap().0, &[3]);
    assert_eq!(&db.pop_first()?.unwrap().0, &[4]);
    assert_eq!(&db.pop_first()?.unwrap().0, &[5]);
    assert_eq!(db.pop_first()?, None);
    /*
     */
    Ok(())
}

#[test]
fn test_pop_last_in_range() -> io::Result<()> {
    let config = sled::Config::tmp().unwrap();
    let db: sled::Db<4> = config.open()?;

    let data = vec![
        (b"key 1", b"value 1"),
        (b"key 2", b"value 2"),
        (b"key 3", b"value 3"),
    ];

    for (k, v) in data {
        db.insert(k, v).unwrap();
    }

    let r1 = db.pop_last_in_range(b"key 1".as_ref()..=b"key 3").unwrap();
    assert_eq!(Some((b"key 3".into(), b"value 3".into())), r1);

    let r2 = db.pop_last_in_range(b"key 1".as_ref()..b"key 3").unwrap();
    assert_eq!(Some((b"key 2".into(), b"value 2".into())), r2);

    let r3 = db.pop_last_in_range(b"key 4".as_ref()..).unwrap();
    assert!(r3.is_none());

    let r4 = db.pop_last_in_range(b"key 2".as_ref()..=b"key 3").unwrap();
    assert!(r4.is_none());

    let r5 = db.pop_last_in_range(b"key 0".as_ref()..=b"key 3").unwrap();
    assert_eq!(Some((b"key 1".into(), b"value 1".into())), r5);

    let r6 = db.pop_last_in_range(b"key 0".as_ref()..=b"key 3").unwrap();
    assert!(r6.is_none());
    Ok(())
}

#[test]
fn test_interleaved_gets_sets() {
    common::setup_logger();
    let db: Db =
        Config::tmp().unwrap().cache_capacity_bytes(1024).open().unwrap();

    let done = Arc::new(AtomicBool::new(false));

    std::thread::scope(|scope| {
        let db_2 = db.clone();
        let done = &done;
        scope.spawn(move || {
            for v in 0..500_000_u32 {
                db_2.insert(v.to_be_bytes(), &[42u8; 4096][..])
                    .expect("failed to insert");
                if v % 10_000 == 0 {
                    log::trace!("WRITING: {}", v);
                    db_2.flush().unwrap();
                }
            }
            done.store(true, SeqCst);
        });
        scope.spawn(move || {
            while !done.load(SeqCst) {
                for v in (0..500_000_u32).rev() {
                    db.get(v.to_be_bytes()).expect("Fatal error?");
                    if v % 10_000 == 0 {
                        log::trace!("READING: {}", v)
                    }
                }
            }
        });
    });
}

#[test]
#[cfg(not(miri))] // can't create threads
fn concurrent_tree_pops() -> std::io::Result<()> {
    use std::thread;

    let db: Db = Config::tmp().unwrap().open()?;

    // Insert values 0..5
    for x in 0u32..5 {
        db.insert(x.to_be_bytes(), &[])?;
    }

    let mut threads = vec![];

    // Pop 5 values using multiple threads
    let barrier = Arc::new(Barrier::new(5));
    for _ in 0..5 {
        let barrier = barrier.clone();
        let db: Db = db.clone();
        threads.push(thread::spawn(move || {
            barrier.wait();
            db.pop_first().unwrap().unwrap();
        }));
    }

    for thread in threads.into_iter() {
        thread.join().unwrap();
    }

    assert!(
        db.is_empty().unwrap(),
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

        let config = Config::tmp()
            .unwrap()
            .flush_every_ms(Some(1))
            .cache_capacity_bytes(1024);

        macro_rules! par {
            ($t:ident, $f:expr) => {
                let mut threads = vec![];

                let flusher_barrier = Arc::new(Barrier::new(N_THREADS));
                for tn in 0..N_THREADS {
                    let tree = $t.clone();
                    let barrier = flusher_barrier.clone();
                    let thread = thread::Builder::new()
                        .name(format!("t(thread: {} flusher)", tn))
                        .spawn(move || {
                            tree.flush().unwrap();
                            barrier.wait();
                        })
                        .expect("should be able to spawn thread");
                    threads.push(thread);
                }

                let barrier = Arc::new(Barrier::new(N_THREADS));

                for tn in 0..N_THREADS {
                    let tree = $t.clone();
                    let barrier = barrier.clone();
                    let thread = thread::Builder::new()
                        .name(format!("t(thread: {} test: {})", tn, i))
                        .spawn(move || {
                            barrier.wait();
                            for i in
                                (tn * N_PER_THREAD)..((tn + 1) * N_PER_THREAD)
                            {
                                let k = kv(i);
                                $f(&tree, k);
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
        let t: Db = config.open().unwrap();
        par! {t, move |tree: &Db, k: InlineArray| {
            assert_eq!(tree.get(&*k).unwrap(), None);
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
        let t: Db = config.open().expect("should be able to restart Db");

        let n_scanned = t.iter().count();
        if n_scanned != N {
            warn!(
                "WARNING: test {} only had {} keys present \
                 in the DB AFTER restarting. expected {}",
                i, n_scanned, N,
            );
        }

        debug!("========== reading sets in test {} ==========", i);
        par! {t, move |tree: &Db, k: InlineArray| {
            if let Some(v) =  tree.get(&*k).unwrap() {
                if v != k {
                    panic!("expected key {:?} not found", k);
                }
            } else {
                panic!(
                    "could not read key {:?}, which we \
                    just wrote to tree {:?}", k, tree
               );
            }
        }};

        drop(t);
        let t: Db = config.open().expect("should be able to restart Db");

        debug!("========== CAS test in test {} ==========", i);
        par! {t, move |tree: &Db, k: InlineArray| {
            let k1 = k.clone();
            let mut k2 = k;
            k2.make_mut().reverse();
            tree.compare_and_swap(&k1, Some(&*k1), Some(k2)).unwrap().unwrap();
        }};

        drop(t);
        let t: Db = config.open().expect("should be able to restart Db");

        par! {t, move |tree: &Db, k: InlineArray| {
            let k1 = k.clone();
            let mut k2 = k;
            k2.make_mut().reverse();
            assert_eq!(tree.get(&*k1).unwrap().unwrap(), k2);
        }};

        drop(t);
        let t: Db = config.open().expect("should be able to restart Db");

        debug!("========== deleting in test {} ==========", i);
        par! {t, move |tree: &Db, k: InlineArray| {
            tree.remove(&*k).unwrap().unwrap();
        }};

        drop(t);
        let t: Db = config.open().expect("should be able to restart Db");

        par! {t, move |tree: &Db, k: InlineArray| {
            assert_eq!(tree.get(&*k).unwrap(), None);
        }};
    }
}

#[test]
#[cfg(not(miri))] // can't create threads
fn concurrent_tree_iter() -> io::Result<()> {
    use std::sync::Barrier;
    use std::thread;

    common::setup_logger();

    const N_FORWARD: usize = INTENSITY;
    const N_REVERSE: usize = INTENSITY;
    const N_INSERT: usize = INTENSITY;
    const N_DELETE: usize = INTENSITY;

    // items that are expected to always be present at their expected
    // order, regardless of other inserts or deletes.
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

    let config = Config::tmp()
        .unwrap()
        .cache_capacity_bytes(1024 * 1024 * 1024)
        .flush_every_ms(Some(1));

    let t: Db = config.open().unwrap();

    let mut threads: Vec<thread::JoinHandle<io::Result<()>>> = vec![];

    let flusher_barrier = Arc::new(Barrier::new(N_THREADS));
    for tn in 0..N_THREADS {
        let tree = t.clone();
        let barrier = flusher_barrier.clone();
        let thread = thread::Builder::new()
            .name(format!("t(thread: {} flusher)", tn))
            .spawn(move || {
                tree.flush().unwrap();
                barrier.wait();
                Ok(())
            })
            .expect("should be able to spawn thread");
        threads.push(thread);
    }

    for item in &INDELIBLE {
        t.insert(item, item.to_vec())?;
    }

    let barrier =
        Arc::new(Barrier::new(N_FORWARD + N_REVERSE + N_INSERT + N_DELETE));

    static I: AtomicUsize = AtomicUsize::new(0);

    for i in 0..N_FORWARD {
        let t: Db = t.clone();
        let barrier = barrier.clone();

        let thread = thread::Builder::new()
            .name(format!("forward({})", i))
            .spawn(move || {
                I.fetch_add(1, SeqCst);
                barrier.wait();
                for _ in 0..1024 {
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
                I.fetch_sub(1, SeqCst);

                Ok(())
            })
            .unwrap();
        threads.push(thread);
    }

    for i in 0..N_REVERSE {
        let t: Db = t.clone();
        let barrier = barrier.clone();

        let thread = thread::Builder::new()
            .name(format!("reverse({})", i))
            .spawn(move || {
                I.fetch_add(1, SeqCst);
                barrier.wait();
                for _ in 0..1024 {
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
                                    t,
                                );
                                if &*k == *expect {
                                    break;
                                }
                            } else {
                                panic!("undershot key on tree: \n{:?}", t);
                            }
                        }
                    }
                }
                I.fetch_sub(1, SeqCst);

                Ok(())
            })
            .unwrap();

        threads.push(thread);
    }

    for i in 0..N_INSERT {
        let t: Db = t.clone();
        let barrier = barrier.clone();

        let thread = thread::Builder::new()
            .name(format!("insert({})", i))
            .spawn(move || {
                barrier.wait();

                while I.load(SeqCst) != 0 {
                    for i in 0..(16 * 16 * 8) {
                        let major = i / (16 * 8);
                        let minor = i % 16;

                        let mut base = INDELIBLE[major].to_vec();
                        base.push(minor as u8);
                        t.insert(base.clone(), base.clone())?;
                    }
                }

                Ok(())
            })
            .unwrap();

        threads.push(thread);
    }

    for i in 0..N_DELETE {
        let t: Db = t.clone();
        let barrier = barrier.clone();

        let thread = thread::Builder::new()
            .name(format!("deleter({})", i))
            .spawn(move || {
                barrier.wait();

                while I.load(SeqCst) != 0 {
                    for i in 0..(16 * 16 * 8) {
                        let major = i / (16 * 8);
                        let minor = i % 16;

                        let mut base = INDELIBLE[major].to_vec();
                        base.push(minor as u8);
                        t.remove(&base)?;
                    }
                }

                Ok(())
            })
            .unwrap();

        threads.push(thread);
    }

    for thread in threads.into_iter() {
        thread.join().expect("thread should not have crashed")?;
    }

    t.check_error().expect("Db should have no set error");

    dbg!(t.stats());

    Ok(())
}

/*
#[test]
#[cfg(not(miri))] // can't create threads
fn concurrent_tree_transactions() -> TransactionResult<()> {
    use std::sync::Barrier;

    common::setup_logger();

    let config = Config::new()
        .temporary(true)
        .flush_every_ms(Some(1))
    let db: Db = config.open().unwrap();

    db.insert(b"k1", b"cats").unwrap();
    db.insert(b"k2", b"dogs").unwrap();

    let mut threads: Vec<std::thread::JoinHandle<TransactionResult<()>>> =
        vec![];

    const N_WRITERS: usize = 30;
    const N_READERS: usize = 5;
    const N_SUBSCRIBERS: usize = 5;

    let barrier = Arc::new(Barrier::new(N_WRITERS + N_READERS + N_SUBSCRIBERS));

    for _ in 0..N_WRITERS {
        let db: Db = db.clone();
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
        let db: Db = db.clone();
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
        let db: Db = db.clone();
        let barrier = barrier.clone();
        let thread = std::thread::spawn(move || {
            barrier.wait();
            let mut sub = db.watch_prefix(b"k1");
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
    let config = sled::Config::tmp().unwrap();
    let db: Db = config.open().unwrap();
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
        Config::tmp().unwrap().flush_every_ms(Some(1)).open().unwrap();
    let db2 =
        Config::tmp().unwrap().flush_every_ms(Some(1)).open().unwrap();

    let result: TransactionResult<()> =
        (&*db1, &*db2).transaction::<_, ()>(|_| Ok(()));

    assert!(result.is_err());

    Ok(())
}

#[test]
fn many_tree_transactions() -> TransactionResult<()> {
    common::setup_logger();

    let config = Config::tmp().unwrap().flush_every_ms(Some(1));
    let db: Db = Arc::new(config.open().unwrap());
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

    let config = Config::tmp().unwrap().flush_every_ms(Some(1));
    let db: Db = config.open().unwrap();

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
*/

#[test]
fn tree_subdir() {
    let mut parent_path = std::env::temp_dir();
    parent_path.push("test_tree_subdir");

    let _ = std::fs::remove_dir_all(&parent_path);

    let mut path = parent_path.clone();
    path.push("test_subdir");

    let config = Config::new().path(&path);

    let t: Db = config.open().unwrap();

    t.insert(&[1], vec![1]).unwrap();

    drop(t);

    let config = Config::new().path(&path);

    let t: Db = config.open().unwrap();

    let res = t.get(&*vec![1]);

    assert_eq!(res.unwrap().unwrap(), vec![1_u8]);

    drop(t);

    std::fs::remove_dir_all(&parent_path).unwrap();
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_small_keys_iterator() {
    let config = Config::tmp().unwrap().flush_every_ms(Some(1));
    let t: Db = config.open().unwrap();
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
    assert!(tree_scan.next().is_none());
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_big_keys_iterator() {
    fn kv(i: usize) -> Vec<u8> {
        let k = [(i >> 16) as u8, (i >> 8) as u8, i as u8];

        let mut base = vec![0; u8::MAX as usize];
        base.extend_from_slice(&k);
        base
    }

    let config = Config::tmp().unwrap().flush_every_ms(Some(1));

    let t: Db = config.open().unwrap();
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
    assert!(tree_scan.next().is_none());
}

/*
#[test]
fn tree_subscribers_and_keyspaces() -> io::Result<()> {
    let config = Config::tmp().unwrap().flush_every_ms(Some(1));

    let db: Db = config.open().unwrap();

    let t1 = db.open_tree(b"1")?;
    let mut s1 = t1.watch_prefix(b"");

    let t2 = db.open_tree(b"2")?;
    let mut s2 = t2.watch_prefix(b"");

    t1.insert(b"t1_a", b"t1_a".to_vec())?;
    t2.insert(b"t2_a", b"t2_a".to_vec())?;

    assert_eq!(s1.next().unwrap().iter().next().unwrap().1, b"t1_a");
    assert_eq!(s2.next().unwrap().iter().next().unwrap().1, b"t2_a");

    drop(db);
    drop(t1);
    drop(t2);

    let db: Db = config.open().unwrap();

    let t1 = db.open_tree(b"1")?;
    let mut s1 = t1.watch_prefix(b"");

    let t2 = db.open_tree(b"2")?;
    let mut s2 = t2.watch_prefix(b"");

    assert!(db.is_empty());
    assert_eq!(t1.len(), 1);
    assert_eq!(t2.len(), 1);

    t1.insert(b"t1_b", b"t1_b".to_vec())?;
    t2.insert(b"t2_b", b"t2_b".to_vec())?;

    assert_eq!(s1.next().unwrap().iter().next().unwrap().1, b"t1_b");
    assert_eq!(s2.next().unwrap().iter().next().unwrap().1, b"t2_b");

    drop(db);
    drop(t1);
    drop(t2);

    let db: Db = config.open().unwrap();

    let t1 = db.open_tree(b"1")?;
    let t2 = db.open_tree(b"2")?;

    assert!(db.is_empty());
    assert_eq!(t1.len(), 2);
    assert_eq!(t2.len(), 2);

    db.drop_tree(b"1")?;
    db.drop_tree(b"2")?;

    assert_eq!(t1.get(b""), Err(Error::CollectionNotFound));

    assert_eq!(t2.get(b""), Err(Error::CollectionNotFound));

    let guard = pin();
    guard.flush();
    drop(guard);

    drop(db);
    drop(t1);
    drop(t2);

    let db: Db = config.open().unwrap();

    let t1 = db.open_tree(b"1")?;
    let t2 = db.open_tree(b"2")?;

    assert!(db.is_empty());
    assert_eq!(t1.len(), 0);
    assert_eq!(t2.len(), 0);

    Ok(())
}
*/

#[test]
fn tree_range() {
    common::setup_logger();

    let config = Config::tmp().unwrap().flush_every_ms(Some(1));
    let t: sled::Db<7> = config.open().unwrap();

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
    assert!(r.next().is_none());

    let start = b"2".to_vec();
    let end = b"4".to_vec();
    let mut r = t.range(start..end).rev();
    assert_eq!(r.next().unwrap().unwrap().0, b"3");
    assert_eq!(r.next().unwrap().unwrap().0, b"2");
    assert!(r.next().is_none());

    let start = b"2".to_vec();
    let mut r = t.range(start..);
    assert_eq!(r.next().unwrap().unwrap().0, b"2");
    assert_eq!(r.next().unwrap().unwrap().0, b"3");
    assert_eq!(r.next().unwrap().unwrap().0, b"4");
    assert_eq!(r.next().unwrap().unwrap().0, b"5");
    assert!(r.next().is_none());

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
    assert!(r.next().is_none());
}

#[test]
#[cfg_attr(miri, ignore)]
fn recover_tree() {
    common::setup_logger();

    let config = Config::tmp().unwrap().flush_every_ms(Some(1));

    let t: sled::Db<7> = config.open().unwrap();
    for i in 0..N_PER_THREAD {
        let k = kv(i);
        t.insert(&k, k.clone()).unwrap();
    }
    drop(t);

    let t: sled::Db<7> = config.open().unwrap();
    for i in 0..N_PER_THREAD {
        let k = kv(i as usize);
        assert_eq!(t.get(&*k).unwrap().unwrap(), k);
        t.remove(&*k).unwrap();
    }
    drop(t);

    println!("---------------- recovering a (hopefully) empty db ----------------------");

    let t: sled::Db<7> = config.open().unwrap();
    for i in 0..N_PER_THREAD {
        let k = kv(i as usize);
        assert!(
            t.get(&*k).unwrap().is_none(),
            "expected key {:?} to have been deleted",
            i
        );
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_gc() {
    const FANOUT: usize = 7;

    common::setup_logger();

    let config = Config::tmp().unwrap().flush_every_ms(None);

    let t: sled::Db<FANOUT> = config.open().unwrap();

    for i in 0..N {
        let k = kv(i);
        t.insert(&k, k.clone()).unwrap();
    }

    for _ in 0..100 {
        t.flush().unwrap();
    }

    let size_on_disk_after_inserts = t.size_on_disk().unwrap();

    for i in 0..N {
        let k = kv(i);
        t.insert(&k, k.clone()).unwrap();
    }

    for _ in 0..100 {
        t.flush().unwrap();
    }

    let size_on_disk_after_rewrites = t.size_on_disk().unwrap();

    for i in 0..N {
        let k = kv(i);
        assert_eq!(t.get(&*k).unwrap(), Some(k.clone().into()), "{k:?}");
        t.remove(&*k).unwrap();
    }

    for _ in 0..100 {
        t.flush().unwrap();
    }

    let size_on_disk_after_deletes = t.size_on_disk().unwrap();

    t.check_error().expect("Db should have no set error");

    let stats = t.stats();

    dbg!(stats);

    assert!(
        stats.cache.heap.allocator.objects_allocated >= (N / FANOUT) as u64,
        "{stats:?}"
    );
    assert!(
        stats.cache.heap.allocator.objects_freed
            >= (stats.cache.heap.allocator.objects_allocated / 2) as u64,
        "{stats:?}"
    );
    assert!(
        stats.cache.heap.allocator.heap_slots_allocated >= (N / FANOUT) as u64,
        "{stats:?}"
    );
    assert!(
        stats.cache.heap.allocator.heap_slots_freed
            >= (stats.cache.heap.allocator.heap_slots_allocated / 2) as u64,
        "{stats:?}"
    );

    // TODO test this after we implement file truncation
    // let expected_max_size = size_on_disk_after_inserts / 100;
    // assert!(size_on_disk_after_deletes <= expected_max_size);

    println!(
        "after writing {N} items and removing them, disk size went \
        from {}kb after inserts to {}kb after rewriting to {}kb after deletes",
        size_on_disk_after_inserts / 1024,
        size_on_disk_after_rewrites / 1024,
        size_on_disk_after_deletes / 1024,
    );
}

/*
#[test]
fn create_exclusive() {
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
*/

#[test]
fn contains_tree() {
    let db: Db = Config::tmp().unwrap().flush_every_ms(None).open().unwrap();
    let tree_one = db.open_tree("tree 1").unwrap();
    let tree_two = db.open_tree("tree 2").unwrap();

    drop(tree_one);
    drop(tree_two);

    assert_eq!(false, db.contains_tree("tree 3").unwrap());
    assert_eq!(true, db.contains_tree("tree 1").unwrap());
    assert_eq!(true, db.contains_tree("tree 2").unwrap());

    assert!(db.drop_tree("tree 1").unwrap());
    assert_eq!(false, db.contains_tree("tree 1").unwrap());
}

#[test]
#[cfg_attr(miri, ignore)]
fn tree_import_export() -> io::Result<()> {
    common::setup_logger();

    let config_1 = Config::tmp().unwrap();
    let config_2 = Config::tmp().unwrap();

    let db: Db = config_1.open()?;
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

    let exporter: Db = config_1.open()?;
    let importer: Db = config_2.open()?;

    let export = exporter.export();
    importer.import(export);

    drop(exporter);
    drop(config_1);
    drop(importer);

    let db: Db = config_2.open()?;

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

    let db: Db = config_2.open()?;
    for db_id in 0..N_THREADS {
        let tree_id = format!("tree_{}", db_id);
        let tree = db.open_tree(tree_id.as_bytes())?;

        for i in 0..N_THREADS {
            let k = kv(i as usize);
            assert_eq!(tree.get(&*k).unwrap(), None);
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
        .gen(Gen::new(100))
        .tests(n_tests)
        .max_tests(n_tests * 10)
        .quickcheck(
            prop_tree_matches_btreemap as fn(Vec<Op>, bool, i32, usize) -> bool,
        );
}

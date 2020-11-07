mod common;

use std::convert::TryFrom;
use std::env::{self, VarError};
use std::mem::size_of;
use std::process::{exit, Child, Command, ExitStatus};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use rand::Rng;

use sled::Config;

use common::cleanup;

const TEST_ENV_VAR: &str = "SLED_CRASH_TEST";
const N_TESTS: usize = 100;
const CYCLE: usize = 256;
const BATCH_SIZE: u32 = 8;
const SEGMENT_SIZE: usize = 1024;

// test names, also used as dir names
const RECOVERY_DIR: &str = "crash_recovery";
const BATCHES_DIR: &str = "crash_batches";
const ITER_DIR: &str = "crash_iter";
const TX_DIR: &str = "crash_tx";

const CRASH_CHANCE: u32 = 250;

fn main() {
    // Don't actually run this harness=false test under miri, as it requires spawning and killing
    // child processes.
    if cfg!(miri) {
        return;
    }

    common::setup_logger();

    match env::var(TEST_ENV_VAR) {
        Err(VarError::NotPresent) => {
            test_crash_recovery();
            test_crash_batches();
            concurrent_crash_iter();
            concurrent_crash_transactions();
        }

        Ok(ref s) if s == RECOVERY_DIR => run(),
        Ok(ref s) if s == BATCHES_DIR => run_batches(),
        Ok(ref s) if s == ITER_DIR => run_iter(),
        Ok(ref s) if s == TX_DIR => run_tx(),

        Ok(_) | Err(_) => panic!("invalid crash test case"),
    }
}

/// Verifies that the keys in the tree are correctly recovered.
/// Panics if they are incorrect.
/// Returns the key that should be resumed at, and the current cycle value.
fn verify(tree: &sled::Tree) -> (u32, u32) {
    // key 0 should always be the highest value, as that's where we increment
    // at some point, it might go down by one
    // it should never return, or go down again after that
    let mut iter = tree.iter();
    let highest = match iter.next() {
        Some(Ok((_k, v))) => slice_to_u32(&*v),
        Some(Err(e)) => panic!("{:?}", e),
        None => return (0, 0),
    };

    let highest_vec = u32_to_vec(highest);

    // find how far we got
    let mut contiguous: u32 = 0;
    let mut lowest = 0;
    for res in iter {
        let (_k, v) = res.unwrap();
        if v[..4] == highest_vec[..4] {
            contiguous += 1;
        } else {
            let expected = if highest == 0 {
                CYCLE as u32 - 1
            } else {
                (highest - 1) % CYCLE as u32
            };
            let actual = slice_to_u32(&*v);
            assert_eq!(expected, actual);
            lowest = actual;
            break;
        }
    }

    // ensure nothing changes after this point
    let low_beginning = u32_to_vec(contiguous + 1);

    for res in tree.range(&*low_beginning..) {
        let (k, v): (sled::IVec, _) = res.unwrap();
        assert_eq!(
            slice_to_u32(&*v),
            lowest,
            "expected key {} to have value {}, instead it had value {} in db: {:?}",
            slice_to_u32(&*k),
            lowest,
            slice_to_u32(&*v),
            tree
        );
    }

    tree.verify_integrity().unwrap();

    (contiguous, highest)
}

fn u32_to_vec(u: u32) -> Vec<u8> {
    let buf: [u8; size_of::<u32>()] = u.to_be_bytes();
    buf.to_vec()
}

fn slice_to_u32(b: &[u8]) -> u32 {
    let mut buf = [0u8; size_of::<u32>()];
    buf.copy_from_slice(&b[..size_of::<u32>()]);

    u32::from_be_bytes(buf)
}

fn spawn_killah() {
    thread::spawn(|| {
        let runtime = rand::thread_rng().gen_range(0, 60);
        thread::sleep(Duration::from_millis(runtime));
        exit(9);
    });
}

fn run_inner(config: Config) {
    let crash_during_initialization = rand::thread_rng().gen_bool(0.1);

    if crash_during_initialization {
        spawn_killah();
    }

    let tree = config.open().unwrap();

    if !crash_during_initialization {
        spawn_killah();
    }

    let (key, highest) = verify(&tree);

    let mut hu = ((highest as usize) * CYCLE) + key as usize;
    assert_eq!(hu % CYCLE, key as usize);
    assert_eq!(hu / CYCLE, highest as usize);

    loop {
        hu += 1;

        if hu / CYCLE >= CYCLE {
            hu = 0;
        }

        let key = u32_to_vec((hu % CYCLE) as u32);

        let mut value = u32_to_vec((hu / CYCLE) as u32);
        let additional_len = rand::thread_rng().gen_range(0, SEGMENT_SIZE / 3);
        value.append(&mut vec![0u8; additional_len]);

        tree.insert(&key, value).unwrap();
    }
}

/// Verifies that the keys in the tree are correctly recovered (i.e., equal).
/// Panics if they are incorrect.
fn verify_batches(tree: &sled::Tree) -> u32 {
    let mut iter = tree.iter();
    let first_value = match iter.next() {
        Some(Ok((_k, v))) => slice_to_u32(&*v),
        Some(Err(e)) => panic!("{:?}", e),
        None => return 0,
    };
    for key in 0..BATCH_SIZE {
        let res = tree.get(u32_to_vec(key));
        let option = res.unwrap();
        let v = match option {
            Some(v) => v,
            None => panic!(
                "expected key {} to have a value, instead it was missing in db: {:?}",
                key, tree
            ),
        };
        let value = slice_to_u32(&*v);
        assert_eq!(
            first_value, value,
            "expected key {} to have value {}, instead it had value {} in db: {:?}",
            key, first_value, value, tree
        );
    }

    tree.verify_integrity().unwrap();

    first_value
}

fn run_batches_inner(db: sled::Db) {
    fn do_batch(i: u32, db: &sled::Db) {
        let mut rng = rand::thread_rng();
        let base_value = u32_to_vec(i);

        let mut batch = sled::Batch::default();
        if rng.gen_bool(0.1) {
            for key in 0..BATCH_SIZE {
                batch.remove(u32_to_vec(key));
            }
        } else {
            for key in 0..BATCH_SIZE {
                let mut value = base_value.clone();
                let additional_len = rng.gen_range(0, SEGMENT_SIZE / 3);
                value.append(&mut vec![0u8; additional_len]);

                batch.insert(u32_to_vec(key), value);
            }
        }
        db.apply_batch(batch).unwrap();
    }

    let mut i = verify_batches(&db);
    i += 1;
    do_batch(i, &db);

    loop {
        i += 1;
        do_batch(i, &db);
    }
}

fn run() {
    let config = Config::new()
        .cache_capacity(128 * 1024 * 1024)
        .flush_every_ms(Some(1))
        .path(RECOVERY_DIR.to_string())
        .segment_size(SEGMENT_SIZE);

    if let Err(e) = thread::spawn(|| run_inner(config)).join() {
        println!("worker thread failed: {:?}", e);
        std::process::exit(15);
    }
}

fn run_batches() {
    let crash_during_initialization = rand::thread_rng().gen_ratio(1, 10);

    if crash_during_initialization {
        spawn_killah();
    }

    let config = Config::new()
        .cache_capacity(128 * 1024 * 1024)
        .flush_every_ms(Some(1))
        .path(BATCHES_DIR.to_string())
        .segment_size(SEGMENT_SIZE);

    let db = config.open().unwrap();
    // let db2 = db.clone();

    let t1 = thread::spawn(|| run_batches_inner(db));
    let t2 = thread::spawn(|| {}); // run_batches_inner(db2));

    if !crash_during_initialization {
        spawn_killah();
    }

    if let Err(e) = t1.join().and_then(|_| t2.join()) {
        println!("worker thread failed: {:?}", e);
        std::process::exit(15);
    }
}

fn run_child_process(test_name: &str) -> Child {
    let bin = env::current_exe().expect("could not get test binary path");

    env::set_var(TEST_ENV_VAR, test_name);

    Command::new(bin)
        .env(TEST_ENV_VAR, test_name)
        .env("SLED_CRASH_CHANCE", CRASH_CHANCE.to_string())
        .spawn()
        .unwrap_or_else(|_| {
            panic!("could not spawn child process for {} test", test_name)
        })
}

fn handle_child_exit_status(dir: &str, status: ExitStatus) {
    let code = status.code();

    if code.is_none() || code.unwrap() != 9 {
        cleanup(dir);
        panic!("{} test child exited abnormally", dir);
    }
}

fn handle_child_wait_err(dir: &str, e: std::io::Error) {
    cleanup(dir);

    panic!("error waiting for {} test child: {}", dir, e);
}

fn test_crash_recovery() {
    let dir = RECOVERY_DIR;
    cleanup(dir);

    for _ in 0..N_TESTS {
        let mut child = run_child_process(dir);

        child
            .wait()
            .map(|status| handle_child_exit_status(dir, status))
            .map_err(|e| handle_child_wait_err(dir, e))
            .unwrap();
    }

    cleanup(dir);
}

fn test_crash_batches() {
    let dir = BATCHES_DIR;
    cleanup(dir);

    for _ in 0..N_TESTS {
        let mut child = run_child_process(dir);

        child
            .wait()
            .map(|status| handle_child_exit_status(dir, status))
            .map_err(|e| handle_child_wait_err(dir, e))
            .unwrap();
    }

    cleanup(dir);
}

fn concurrent_crash_iter() {
    let dir = ITER_DIR;
    cleanup(dir);

    for _ in 0..N_TESTS {
        let mut child = run_child_process(dir);

        child
            .wait()
            .map(|status| handle_child_exit_status(dir, status))
            .map_err(|e| handle_child_wait_err(dir, e))
            .unwrap();
    }

    cleanup(dir);
}

fn concurrent_crash_transactions() {
    let dir = TX_DIR;
    cleanup(dir);

    for _ in 0..N_TESTS {
        let mut child = run_child_process(dir);

        child
            .wait()
            .map(|status| handle_child_exit_status(dir, status))
            .map_err(|e| handle_child_wait_err(dir, e))
            .unwrap();
    }

    cleanup(dir);
}

fn run_iter() {
    common::setup_logger();

    const N_FORWARD: usize = 50;
    const N_REVERSE: usize = 50;

    let config = Config::new().path(ITER_DIR).flush_every_ms(Some(1));

    let t = config.open().unwrap();
    t.verify_integrity().unwrap();

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
        t.insert(*item, *item).unwrap();
    }

    let barrier = Arc::new(Barrier::new(N_FORWARD + N_REVERSE + 2));
    let mut threads = vec![];

    for i in 0..N_FORWARD {
        let t = thread::Builder::new()
            .name(format!("forward({})", i))
            .spawn({
                let t = t.clone();
                let barrier = barrier.clone();
                move || {
                    barrier.wait();
                    loop {
                        let expected = INDELIBLE.iter();
                        let mut keys = t.iter().keys();

                        for expect in expected {
                            loop {
                                let k = keys.next().unwrap().unwrap();
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
                    loop {
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

                loop {
                    for i in 0..(16 * 16 * 8) {
                        let major = i / (16 * 8);
                        let minor = i % 16;

                        let mut base = INDELIBLE[major].to_vec();
                        base.push(minor as u8);
                        t.insert(base.clone(), base.clone()).unwrap();
                    }
                }
            }
        })
        .unwrap();

    threads.push(inserter);

    let deleter = thread::Builder::new()
        .name("deleter".into())
        .spawn({
            move || {
                barrier.wait();

                loop {
                    for i in 0..(16 * 16 * 8) {
                        let major = i / (16 * 8);
                        let minor = i % 16;

                        let mut base = INDELIBLE[major].to_vec();
                        base.push(minor as u8);
                        t.remove(&base).unwrap();
                    }
                }
            }
        })
        .unwrap();

    spawn_killah();

    threads.push(deleter);

    for thread in threads.into_iter() {
        thread.join().expect("thread should not have crashed");
    }
}

fn run_tx() {
    common::setup_logger();

    let config = Config::new().flush_every_ms(Some(1)).path(TX_DIR);
    let db = config.open().unwrap();
    db.verify_integrity().unwrap();

    db.insert(b"k1", b"cats").unwrap();
    db.insert(b"k2", b"dogs").unwrap();
    db.insert(b"id", &0_u64.to_le_bytes()).unwrap();

    let mut threads = vec![];

    const N_WRITERS: usize = 50;
    const N_READERS: usize = 5;

    let barrier = Arc::new(Barrier::new(N_WRITERS + N_READERS));

    for _ in 0..N_WRITERS {
        let db = db.clone();
        let barrier = barrier.clone();
        let thread = std::thread::spawn(move || {
            barrier.wait();
            loop {
                db.transaction::<_, _, ()>(|db| {
                    let v1 = db.remove(b"k1").unwrap().unwrap();
                    let v2 = db.remove(b"k2").unwrap().unwrap();

                    db.insert(b"id", &db.generate_id().unwrap().to_le_bytes())
                        .unwrap();

                    db.insert(b"k1", v2).unwrap();
                    db.insert(b"k2", v1).unwrap();
                    Ok(())
                })
                .unwrap();
            }
        });
        threads.push(thread);
    }

    for _ in 0..N_READERS {
        let db = db.clone();
        let barrier = barrier.clone();
        let thread = std::thread::spawn(move || {
            barrier.wait();
            let mut last_id = 0;
            loop {
                let read_id = db
                    .transaction::<_, _, ()>(|db| {
                        let v1 = db.get(b"k1").unwrap().unwrap();
                        let v2 = db.get(b"k2").unwrap().unwrap();
                        let id = u64::from_le_bytes(
                            TryFrom::try_from(
                                &*db.get(b"id").unwrap().unwrap(),
                            )
                            .unwrap(),
                        );

                        let mut results = vec![v1, v2];
                        results.sort();

                        assert_eq!(
                            [&results[0], &results[1]],
                            [b"cats", b"dogs"]
                        );

                        Ok(id)
                    })
                    .unwrap();
                assert!(read_id >= last_id);
                last_id = read_id;
            }
        });
        threads.push(thread);
    }

    spawn_killah();

    for thread in threads.into_iter() {
        thread.join().expect("threads should not crash");
    }

    let v1 = db.get(b"k1").unwrap().unwrap();
    let v2 = db.get(b"k2").unwrap().unwrap();
    assert_eq!([v1, v2], [b"cats", b"dogs"]);
}

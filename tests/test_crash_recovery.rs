#![cfg(target_os = "linux")]

mod common;

use std::fs;
use std::mem::size_of;
use std::path::Path;
use std::thread;
use std::time::Duration;

use rand::Rng;

use sled::Config;

const N_TESTS: usize = 100;
const CYCLE: usize = 256;
const BATCH_SIZE: u32 = 8;

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
        if &v[..4] == &highest_vec[..4] {
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

    (contiguous, highest)
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
                key,
                tree
            ),
        };
        let value = slice_to_u32(&*v);
        assert_eq!(
            first_value,
            value,
            "expected key {} to have value {}, instead it had value {} in db: {:?}",
            key,
            first_value,
            value,
            tree
        );
    }

    first_value
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
        let runtime = rand::thread_rng().gen_range(0, 200);
        thread::sleep(Duration::from_millis(runtime));
        unsafe {
            libc::raise(9);
        }
    });
}

fn run(config: Config) {
    common::setup_logger();

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
        let additional_len = rand::thread_rng().gen_range(0, 1000);
        value.append(&mut vec![0u8; additional_len]);

        tree.insert(&key, value).unwrap();
    }
}

fn run_batches(config: Config) {
    fn do_batch(i: u32, tree: &sled::Tree) {
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
                let additional_len = rng.gen_range(0, 1000);
                value.append(&mut vec![0u8; additional_len]);

                batch.insert(u32_to_vec(key), value);
            }
        }
        tree.apply_batch(batch).unwrap();
    }

    common::setup_logger();
    let crash_during_initialization = rand::thread_rng().gen_bool(0.1);

    if crash_during_initialization {
        spawn_killah();
    }

    let tree = config.open().unwrap();

    let mut i = verify_batches(&tree);
    i += 1;
    do_batch(i, &tree);

    if !crash_during_initialization {
        spawn_killah();
    }

    loop {
        i += 1;
        do_batch(i, &tree);
    }
}

fn run_without_snapshot(dir: &str) {
    let config = Config::new()
        .cache_capacity(128 * 1024 * 1024)
        .flush_every_ms(Some(100))
        .path(dir.to_string())
        .snapshot_after_ops(1 << 56)
        .segment_size(1024);

    match thread::spawn(|| run(config)).join() {
        Err(e) => {
            println!("worker thread failed: {:?}", e);
            std::process::exit(15);
        }
        _ => {}
    }
}

fn run_with_snapshot(dir: &str) {
    let config = Config::new()
        .cache_capacity(128 * 1024 * 1024)
        .flush_every_ms(Some(100))
        .path(dir.to_string())
        .snapshot_after_ops(5000)
        .segment_size(1024);

    match thread::spawn(|| run(config)).join() {
        Err(e) => {
            println!("worker thread failed: {:?}", e);
            std::process::exit(15);
        }
        _ => {}
    }
}

fn run_batches_without_snapshot(dir: &str) {
    let config = Config::new()
        .cache_capacity(128 * 1024 * 1024)
        .flush_every_ms(Some(100))
        .path(dir.to_string())
        .snapshot_after_ops(1 << 56)
        .segment_size(1024);

    match thread::spawn(|| run_batches(config)).join() {
        Err(e) => {
            println!("worker thread failed: {:?}", e);
            std::process::exit(15);
        }
        _ => {}
    }
}

fn run_batches_with_snapshot(dir: &str) {
    let config = Config::new()
        .cache_capacity(128 * 1024 * 1024)
        .flush_every_ms(Some(100))
        .path(dir.to_string())
        .snapshot_after_ops(5000)
        .segment_size(1024);

    match thread::spawn(|| run_batches(config)).join() {
        Err(e) => {
            println!("worker thread failed: {:?}", e);
            std::process::exit(15);
        }
        _ => {}
    }
}

#[test]
fn test_crash_recovery_with_runtime_snapshot() {
    let dir = "test_crashes_with_snapshot";
    cleanup(dir);
    for _ in 0..N_TESTS {
        let child = unsafe { libc::fork() };
        if child == 0 {
            run_with_snapshot(dir)
        } else {
            let mut status = 0;
            unsafe {
                libc::waitpid(child, &mut status as *mut libc::c_int, 0);
            }
            if status != 9 {
                cleanup(dir);
                panic!("child exited abnormally");
            }
        }
    }
    cleanup(dir);
}

#[test]
fn test_crash_recovery_no_runtime_snapshot() {
    let dir = "test_crashes";
    cleanup(dir);
    for _ in 0..N_TESTS {
        let child = unsafe { libc::fork() };
        if child == 0 {
            run_without_snapshot(dir)
        } else {
            let mut status = 0;
            unsafe {
                libc::waitpid(child, &mut status as *mut libc::c_int, 0);
            }
            if status != 9 {
                cleanup(dir);
                panic!("child exited abnormally");
            }
        }
    }
    cleanup(dir);
}

#[test]
fn test_crash_batches_with_runtime_snapshot() {
    let dir = "test_batches_with_snapshot";
    cleanup(dir);
    for _ in 0..N_TESTS {
        let child = unsafe { libc::fork() };
        if child == 0 {
            run_batches_with_snapshot(dir)
        } else {
            let mut status = 0;
            unsafe {
                libc::waitpid(child, &mut status as *mut libc::c_int, 0);
            }
            if status != 9 {
                cleanup(dir);
                panic!("child exited abnormally");
            }
        }
    }
    cleanup(dir);
}

#[test]
fn test_crash_batches_no_runtime_snapshot() {
    let dir = "test_batches";
    cleanup(dir);
    for _ in 0..N_TESTS {
        let child = unsafe { libc::fork() };
        if child == 0 {
            run_batches_without_snapshot(dir)
        } else {
            let mut status = 0;
            unsafe {
                libc::waitpid(child, &mut status as *mut libc::c_int, 0);
            }
            if status != 9 {
                cleanup(dir);
                panic!("child exited abnormally");
            }
        }
    }
    cleanup(dir);
}

fn cleanup(dir: &str) {
    let dir = Path::new(dir);
    if dir.exists() {
        fs::remove_dir_all(dir).unwrap();
    }
}

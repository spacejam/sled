#![cfg(all(
    not(target_os = "fuchsia"),
    not(target_os = "android"),
    not(target_os = "windows"),
    not(target_os = "macos")
))]

extern crate libc;
extern crate pagecache;
extern crate rand;
extern crate sled;
extern crate tests;

use std::fs;
use std::mem::size_of;
use std::path::Path;
use std::thread;
use std::time::Duration;

use rand::Rng;

use pagecache::{Config, ConfigBuilder};

const CYCLE: usize = 256;

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
            "expected key {} to have value {}, instead it had value {}",
            slice_to_u32(&*k),
            lowest,
            slice_to_u32(&*v)
        );
    }

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
        let runtime = rand::thread_rng().gen_range(0, 200);
        thread::sleep(Duration::from_millis(runtime));
        unsafe {
            libc::raise(9);
        }
    });
}

fn run(config: Config) {
    // tests::setup_logger();
    let crash_during_initialization = rand::thread_rng().gen_bool(0.1);

    if crash_during_initialization {
        spawn_killah();
    }

    let tree = sled::Db::start(config).unwrap();

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
        let additional_len = rand::thread_rng().gen_range(0, 100_000);
        value.append(&mut vec![0u8; additional_len]);

        tree.set(&key, value).unwrap();
    }
}

fn run_without_snapshot() {
    let config = ConfigBuilder::new()
        .io_bufs(2)
        .blink_node_split_size(1024)
        .page_consolidation_threshold(10)
        .cache_bits(6)
        .cache_capacity(128 * 1024 * 1024)
        .flush_every_ms(Some(100))
        // drop io_buf_size to 1<<16, then 1<<17 to tease out
        // low hanging fruit more quickly
        .io_buf_size(100_000) // 1<<16 is 65k but might cause stalling
        .path("test_crashes".to_string())
        .snapshot_after_ops(1 << 56)
        .build();

    match thread::spawn(|| run(config)).join() {
        Err(e) => {
            println!("worker thread failed: {:?}", e);
            std::process::exit(15);
        }
        _ => {}
    }
}

fn run_with_snapshot() {
    let config = ConfigBuilder::new()
        .io_bufs(2)
        .blink_node_split_size(1024)
        .page_consolidation_threshold(10)
        .cache_bits(6)
        .cache_capacity(128 * 1024 * 1024)
        .flush_every_ms(Some(100))
        // drop io_buf_size to 1<<16, then 1<<17 to tease out
        // low hanging fruit more quickly
        .io_buf_size(100_000) // 1<<16 is 65k but might cause stalling
        .path("test_crashes_with_snapshot".to_string())
        .snapshot_after_ops(1 << 10)
        .build();

    match thread::spawn(|| run(config)).join() {
        Err(e) => {
            println!("worker thread failed: {:?}", e);
            std::process::exit(15);
        }
        _ => {}
    }
}

#[test]
#[ignore]
fn test_crash_recovery_with_runtime_snapshot() {
    cleanup_with_snapshots();
    for _ in 0..100 {
        let child = unsafe { libc::fork() };
        if child == 0 {
            run_with_snapshot()
        } else {
            let mut status = 0;
            unsafe {
                libc::waitpid(child, &mut status as *mut libc::c_int, 0);
            }
            if status != 9 {
                cleanup();
                panic!("child exited abnormally");
            }
        }
    }
    cleanup_with_snapshots();
}

#[test]
#[ignore]
fn test_crash_recovery_no_runtime_snapshot() {
    cleanup();
    for _ in 0..100 {
        let child = unsafe { libc::fork() };
        if child == 0 {
            run_without_snapshot()
        } else {
            let mut status = 0;
            unsafe {
                libc::waitpid(child, &mut status as *mut libc::c_int, 0);
            }
            if status != 9 {
                cleanup();
                panic!("child exited abnormally");
            }
        }
    }
    cleanup();
}

fn cleanup_with_snapshots() {
    let dir = Path::new("test_crashes_with_snapshot");
    if dir.exists() {
        fs::remove_dir_all(dir).unwrap();
    }
}

fn cleanup() {
    let dir = Path::new("test_crashes");
    if dir.exists() {
        fs::remove_dir_all(dir).unwrap();
    }
}

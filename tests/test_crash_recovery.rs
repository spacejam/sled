mod common;

use std::env::{self, VarError};
use std::mem::size_of;
use std::process::{exit, Child, Command, ExitStatus};
use std::thread;
use std::time::Duration;

use rand::Rng;

use sled::Config;

use common::cleanup;

const TEST_ENV_VAR: &str = "SLED_CRASH_TEST";
const N_TESTS: usize = 100;
const CYCLE: usize = 256;
const BATCH_SIZE: u32 = 8;

// test names, also used as dir names
const RECOVERY_WITH_SNAPSHOT: &str = "crash_recovery_with_runtime_snapshot";
const RECOVERY_NO_SNAPSHOT: &str = "crash_recovery_no_runtime_snapshot";

fn main() {
    match env::var(TEST_ENV_VAR) {
        Err(VarError::NotPresent) => {
            test_crash_recovery_with_runtime_snapshot();
            test_crash_recovery_no_runtime_snapshot();
        }

        Ok(ref s) if s == RECOVERY_WITH_SNAPSHOT => run_with_snapshot(s),
        Ok(ref s) if s == RECOVERY_NO_SNAPSHOT => run_without_snapshot(s),

        Ok(_) | Err(_) => panic!("invalid crash test case"),
    }
}

fn test_crash_recovery_with_runtime_snapshot() {
    let dir = RECOVERY_WITH_SNAPSHOT;
    cleanup(dir);

    for _ in 0..N_TESTS {
        let mut child = run_child_process(RECOVERY_WITH_SNAPSHOT);

        child.wait().map(|status| handle_child_exit_status(dir, status));
    }

    cleanup(dir);
}

fn test_crash_recovery_no_runtime_snapshot() {
    let dir = RECOVERY_NO_SNAPSHOT;
    cleanup(dir);

    for _ in 0..N_TESTS {
        let mut child = run_child_process(RECOVERY_NO_SNAPSHOT);

        child
            .wait()
            .map(|status| handle_child_exit_status(dir, status))
            .map_err(|e| handle_child_wait_err(dir, e));
    }

    cleanup(dir);
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

fn run_child_process(test_name: &str) -> Child {
    let bin = env::current_exe().expect("could not get test binary path");

    env::set_var(TEST_ENV_VAR, test_name);

    Command::new(bin).env(TEST_ENV_VAR, test_name).spawn().expect(&format!(
        "could not spawn child process for {} test",
        test_name
    ))
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
        exit(9);
    });
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

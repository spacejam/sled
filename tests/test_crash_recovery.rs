mod common;
mod crash_tests;

use std::alloc::{Layout, System};
use std::env::{self, VarError};
use std::process::Command;
use std::thread;

use common::cleanup;

const TEST_ENV_VAR: &str = "SLED_CRASH_TEST";
const N_TESTS: usize = 100;

const TESTS: [&str; 7] = [
    crash_tests::SEQUENTIAL_WRITES_DIR,
    crash_tests::BATCHES_DIR,
    crash_tests::ITER_DIR,
    crash_tests::TX_DIR,
    crash_tests::METADATA_STORE_DIR,
    crash_tests::HEAP_DIR,
    crash_tests::OBJECT_CACHE_DIR,
];

const CRASH_CHANCE: u32 = 250;

#[global_allocator]
static ALLOCATOR: ShredAllocator = ShredAllocator;

#[derive(Default, Debug, Clone, Copy)]
struct ShredAllocator;

unsafe impl std::alloc::GlobalAlloc for ShredAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        assert!(layout.size() < 1_000_000_000);
        let ret = System.alloc(layout);
        assert_ne!(ret, std::ptr::null_mut());
        std::ptr::write_bytes(ret, 0xa1, layout.size());
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        std::ptr::write_bytes(ptr, 0xde, layout.size());
        System.dealloc(ptr, layout)
    }
}

fn main() {
    // Don't actually run this harness=false test under miri, as it requires
    // spawning and killing child processes.
    if cfg!(miri) {
        return;
    }

    common::setup_logger();

    match env::var(TEST_ENV_VAR) {
        Err(VarError::NotPresent) => {
            let filtered: Vec<&'static str> =
                if let Some(filter) = std::env::args().nth(1) {
                    TESTS
                        .iter()
                        .filter(|name| name.contains(&filter))
                        .cloned()
                        .collect()
                } else {
                    TESTS.to_vec()
                };

            let filtered_len = filtered.len();

            println!();
            println!(
                "running {} test{}",
                filtered.len(),
                if filtered.len() == 1 { "" } else { "s" },
            );

            let mut tests = vec![];
            for test_name in filtered.into_iter() {
                let test = thread::spawn(move || {
                    let res =
                        std::panic::catch_unwind(|| supervisor(test_name));
                    println!(
                        "test {} ... {}",
                        test_name,
                        if res.is_ok() { "ok" } else { "panicked" }
                    );
                    res.unwrap();
                });
                tests.push((test_name, test));
            }

            for (test_name, test) in tests.into_iter() {
                test.join().expect(test_name);
            }

            println!();
            println!(
                "test result: ok. {} passed; {} filtered out",
                filtered_len,
                TESTS.len() - filtered_len,
            );
            println!();
        }

        Ok(ref s) if s == crash_tests::SEQUENTIAL_WRITES_DIR => {
            crash_tests::run_crash_sequential_writes()
        }
        Ok(ref s) if s == crash_tests::BATCHES_DIR => {
            crash_tests::run_crash_batches()
        }
        Ok(ref s) if s == crash_tests::ITER_DIR => {
            crash_tests::run_crash_iter()
        }
        Ok(ref s) if s == crash_tests::TX_DIR => crash_tests::run_crash_tx(),
        Ok(ref s) if s == crash_tests::METADATA_STORE_DIR => {
            crash_tests::run_crash_metadata_store()
        }
        Ok(ref s) if s == crash_tests::HEAP_DIR => {
            crash_tests::run_crash_heap()
        }
        Ok(ref s) if s == crash_tests::OBJECT_CACHE_DIR => {
            crash_tests::run_crash_object_cache()
        }
        Ok(other) => panic!("invalid crash test case: {other}"),
        Err(e) => panic!("env var {TEST_ENV_VAR} unable to be read: {e:?}"),
    }
}

fn run_child_process(dir: &str) {
    let bin = env::current_exe().expect("could not get test binary path");

    unsafe {
        env::set_var(TEST_ENV_VAR, dir);
    }

    let status_res = Command::new(bin)
        .env(TEST_ENV_VAR, dir)
        .env("SLED_CRASH_CHANCE", CRASH_CHANCE.to_string())
        .spawn()
        .unwrap_or_else(|_| {
            panic!("could not spawn child process for {} test", dir)
        })
        .wait();

    match status_res {
        Ok(status) => {
            let code = status.code();

            if code.is_none() || code.unwrap() != 9 {
                cleanup(dir);
                panic!("{} test child exited abnormally", dir);
            }
        }
        Err(e) => {
            cleanup(dir);
            panic!("error waiting for {} test child: {}", dir, e);
        }
    }
}

fn supervisor(dir: &str) {
    cleanup(dir);

    for _ in 0..N_TESTS {
        run_child_process(dir);
    }

    cleanup(dir);
}

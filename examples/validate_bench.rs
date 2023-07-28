use std::alloc::{Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Barrier;
use std::thread::scope;
use std::time::{Duration, Instant};

use bloodstone::{open_default, BloodStone};

const N_WRITES_PER_THREAD: u32 = 1024 * 1024;
const MAX_CONCURRENCY: u32 = 16;

fn bloodstone_gets(bloodstone: &BloodStone) {
    let factory = || bloodstone.clone();

    let f = |bloodstone: BloodStone| {
        let start = 0;
        let end = N_WRITES_PER_THREAD * MAX_CONCURRENCY;
        for i in start..end {
            let k: &[u8] = &i.to_be_bytes();
            let opt = bloodstone.get(k).unwrap();
            if opt.is_none() {
                panic!("failed to get key {}", i);
            }
        }
    };

    for concurrency in [1, 2, 3, 4, 8, MAX_CONCURRENCY as _] {
        let get_stone_elapsed = execute_lockstep_concurrent(factory, f, concurrency);

        println!(
            "{} gets/s with concurrency of {concurrency}, {:?} total reads sled 1.0",
            (N_WRITES_PER_THREAD * MAX_CONCURRENCY * concurrency as u32) as u64 * 1_000_000_u64
                / u64::try_from(get_stone_elapsed.as_micros().max(1)).unwrap_or(u64::MAX),
            get_stone_elapsed
        );
    }
}

fn sled_gets(sled: &sled::Db) {
    let factory = || sled.clone();

    let f = |sled: sled::Db| {
        let start = 0;
        let end = N_WRITES_PER_THREAD * MAX_CONCURRENCY;
        for i in start..end {
            let k: &[u8] = &i.to_be_bytes();
            sled.get(k).unwrap().unwrap();
        }
    };

    for concurrency in [1, 2, 3, 4, 8, MAX_CONCURRENCY as _] {
        let get_stone_elapsed = execute_lockstep_concurrent(factory, f, concurrency);

        println!(
            "{} gets/s with concurrency of {concurrency}, {:?} total reads sled 0.34",
            (N_WRITES_PER_THREAD * MAX_CONCURRENCY * concurrency as u32) as u64 * 1_000_000_u64
                / u64::try_from(get_stone_elapsed.as_micros().max(1)).unwrap_or(u64::MAX),
            get_stone_elapsed
        );
    }
}

fn execute_lockstep_concurrent<State: Send, Factory: FnMut() -> State, F: Sync + Fn(State)>(
    mut factory: Factory,
    f: F,
    concurrency: usize,
) -> Duration {
    let barrier = &Barrier::new(concurrency + 1);
    let f = &f;

    scope(|s| {
        let mut threads = vec![];

        for _ in 0..concurrency {
            let state = factory();

            let thread = s.spawn(move || {
                barrier.wait();
                f(state);
            });

            threads.push(thread);
        }

        barrier.wait();
        let get_stone = Instant::now();

        for thread in threads.into_iter() {
            thread.join().unwrap();
        }

        get_stone.elapsed()
    })
}

#[derive(Debug, Clone, Copy)]
struct Stats {
    disk_space: u64,
    allocated_memory: usize,
    freed_memory: usize,
    resident_memory: usize,
}

fn main() {
    println!("new sled storage:");
    // sled 1.0 / bloodstone
    let new_stats = {
        let stone = open_default("timing_test.bloodstone").unwrap();

        bloodstone_gets(&stone);
    };

    // sled 0.34 / old

    println!("old sled storage:");
    let old_stats = {
        let sled = sled::open("timing_test.sled").unwrap();
        sled_gets(&sled);
    };

    /*
    let scan = Instant::now();
    let count = stone.iter().count();
    assert_eq!(count as u64, N_WRITES_PER_THREAD);
    let scan_elapsed = scan.elapsed();
    println!(
        "{} scanned items/s, total {:?}",
        (N_WRITES_PER_THREAD * 1_000_000) / u64::try_from(scan_elapsed.as_micros().max(1)).unwrap_or(u64::MAX),
        scan_elapsed
    );
    */

    /*
    let scan_rev = Instant::now();
    let count = stone.range(..).rev().count();
    assert_eq!(count as u64, N_WRITES_PER_THREAD);
    let scan_rev_elapsed = scan_rev.elapsed();
    println!(
        "{} reverse-scanned items/s, total {:?}",
        (N_WRITES_PER_THREAD * 1_000_000) / u64::try_from(scan_rev_elapsed.as_micros().max(1)).unwrap_or(u64::MAX),
        scan_rev_elapsed
    );
    */
}

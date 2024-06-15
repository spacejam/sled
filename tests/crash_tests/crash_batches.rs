use std::alloc::{Layout, System};
use std::env::{self, VarError};
use std::mem::size_of;
use std::process::{exit, Child, Command, ExitStatus};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use rand::Rng;

use super::*;

const CACHE_SIZE: usize = 1024 * 1024;
const TEST_ENV_VAR: &str = "SLED_CRASH_TEST";
const N_TESTS: usize = 100;
const CYCLE: usize = 256;
const BATCH_SIZE: u32 = 8;
const SEGMENT_SIZE: usize = 1024;

/// Verifies that the keys in the tree are correctly recovered (i.e., equal).
/// Panics if they are incorrect.
fn verify_batches(tree: &Db) -> u32 {
    let mut iter = tree.iter();
    let first_value = match iter.next() {
        Some(Ok((_k, v))) => slice_to_u32(&*v),
        Some(Err(e)) => panic!("{:?}", e),
        None => return 0,
    };

    // we now expect all items in the batch to be present and to have the same value

    for key in 0..BATCH_SIZE {
        let res = tree.get(u32_to_vec(key));
        let option = res.unwrap();
        let v = match option {
            Some(v) => v,
            None => panic!(
                "expected key {} to have a value, instead it was missing in db with keys: {}",
                key,
                tree_to_string(&tree)
            ),
        };
        let value = slice_to_u32(&*v);
        // FIXME BUG 1 count 2
        // assertion `left == right` failed: expected key 0 to have value 62003, instead it had value 62375 in db with keys:
        // {0:62003, 1:62003, 2:62003, 3:62003, 4:62003, 5:62003, 6:62003, 7:62003,
        // Human: iterating shows correct value, but first get did not
        //
        // expected key 1 to have value 1, instead it had value 29469 in db with keys:
        // {0:1, 1:29469, 2:29469, 3:29469, 4:29469, 5:29469, 6:29469, 7:29469,
        // Human: 0 didn't get included in later syncs
        //
        //  expected key 0 to have value 59485, instead it had value 59484 in db with keys:
        //  {0:59485, 1:59485, 2:59485, 3:59485, 4:59485, 5:59485, 6:59485, 7:59485,
        //  Human: had key N during first check, then N + 1 in iteration
        assert_eq!(
            first_value, value,
            "expected key {} to have value {}, instead it had value {}. second get: {:?}. db iter: {}. third get: {:?}",
            key, first_value, value,
            slice_to_u32(&*tree.get(u32_to_vec(key)).unwrap().unwrap()),
            tree_to_string(&tree),
            slice_to_u32(&*tree.get(u32_to_vec(key)).unwrap().unwrap()),
        );
    }

    first_value
}

fn run_batches_inner(db: Db) {
    fn do_batch(i: u32, db: &Db) {
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
                let additional_len = rng.gen_range(0..SEGMENT_SIZE / 3);
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

pub fn run_crash_batches() {
    let crash_during_initialization = rand::thread_rng().gen_ratio(1, 10);

    if crash_during_initialization {
        spawn_killah();
    }

    let config = Config::new()
        .cache_capacity_bytes(CACHE_SIZE)
        .flush_every_ms(None) // TODO NOCOMMIT Some(1))
        .path(BATCHES_DIR);

    let db = config.open().expect("couldn't open batch db");
    let db2 = db.clone();

    let t1 = thread::spawn(|| run_batches_inner(db));
    let t2 = thread::spawn(move || loop {
        db2.flush().unwrap();
    }); // run_batches_inner(db2));

    if !crash_during_initialization {
        spawn_killah();
    }

    if let Err(e) = t1.join().and_then(|_| t2.join()) {
        println!("worker thread failed: {:?}", e);
        std::process::exit(15);
    }
}

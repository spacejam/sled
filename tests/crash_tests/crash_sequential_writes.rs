use std::thread;

use super::*;

const CACHE_SIZE: usize = 1024 * 1024;
const CYCLE: usize = 256;
const SEGMENT_SIZE: usize = 1024;

/// Verifies that the keys in the tree are correctly recovered.
/// Panics if they are incorrect.
/// Returns the key that should be resumed at, and the current cycle value.
fn verify(tree: &Db) -> (u32, u32) {
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
    let mut lowest_with_high_value = 0;

    for res in iter {
        let (k, v) = res.unwrap();
        if v[..4] == highest_vec[..4] {
            contiguous += 1;
        } else {
            let expected = if highest == 0 {
                CYCLE as u32 - 1
            } else {
                (highest - 1) % CYCLE as u32
            };
            let actual = slice_to_u32(&*v);
            // FIXME BUG 2
            // thread '<unnamed>' panicked at tests/test_crash_recovery.rs:159:13:
            // assertion `left == right` failed
            //   left: 139
            //  right: 136
            assert_eq!(
                expected,
                actual,
                "tree failed assertion with iterated values: {}, k: {:?} v: {:?} expected: {} highest: {}",
                tree_to_string(&tree),
                k,
                v,
                expected,
                highest
            );
            lowest_with_high_value = actual;
            break;
        }
    }

    // ensure nothing changes after this point
    let low_beginning = u32_to_vec(contiguous + 1);

    for res in tree.range(&*low_beginning..) {
        let (k, v): (sled::InlineArray, _) = res.unwrap();
        assert_eq!(
            slice_to_u32(&*v),
            lowest_with_high_value,
            "expected key {} to have value {}, instead it had value {} in db: {:?}",
            slice_to_u32(&*k),
            lowest_with_high_value,
            slice_to_u32(&*v),
            tree
        );
    }

    (contiguous, highest)
}

fn run_inner(config: Config) {
    let crash_during_initialization = rand::thread_rng().gen_bool(0.1);

    if crash_during_initialization {
        spawn_killah();
    }

    let tree = config.open().expect("couldn't open db");

    if !crash_during_initialization {
        spawn_killah();
    }

    println!("~~~~~~~~~~~~~~~~~~");
    let (key, highest) = verify(&tree);
    dbg!(key, highest);

    let mut hu = ((highest as usize) * CYCLE) + key as usize;
    assert_eq!(hu % CYCLE, key as usize);
    assert_eq!(hu / CYCLE, highest as usize);

    loop {
        let key = u32_to_vec((hu % CYCLE) as u32);

        //dbg!(hu, hu % CYCLE);

        let mut value = u32_to_vec((hu / CYCLE) as u32);
        let additional_len = rand::thread_rng().gen_range(0..SEGMENT_SIZE / 3);
        value.append(&mut vec![0u8; additional_len]);

        tree.insert(&key, value).unwrap();

        hu += 1;

        if hu / CYCLE >= CYCLE {
            hu = 0;
        }
    }
}

pub fn run_crash_sequential_writes() {
    let path = std::path::Path::new(CRASH_DIR).join(SEQUENTIAL_WRITES_DIR);
    let config = Config::new()
        .cache_capacity_bytes(CACHE_SIZE)
        .flush_every_ms(Some(1))
        .path(path);

    if let Err(e) = thread::spawn(|| run_inner(config)).join() {
        println!("worker thread failed: {:?}", e);
        std::process::exit(15);
    }
}

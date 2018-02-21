#[macro_use]
extern crate lazy_static;
extern crate quickcheck;
extern crate fail;
extern crate rand;
extern crate sled;
extern crate pagecache;

use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;

use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};

use sled::*;

#[derive(Debug, Clone)]
enum Op {
    Set,
    Del(u8),
    Restart,
    FailPoint(&'static str),
}

use Op::*;

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        let fail_points = vec![
            "initial allocation",
            "initial allocation post",
            "zero segment",
            "zero segment post",
            "zero garbage segment",
            "zero garbage segment post",
            "buffer write",
            "buffer write post",
            "write_config bytes",
            "write_config crc",
            "write_config post",
            "trailer write",
            "trailer write post",
            "snap write",
            "snap write len",
            "snap write crc",
            "snap write post",
            "snap write mv",
            "snap write mv post",
            "snap write rm old",
        ];

        if g.gen_weighted_bool(30) {
            return FailPoint((*g.choose(&fail_points).unwrap()));
        }

        if g.gen_weighted_bool(10) {
            return Restart;
        }

        let choice = g.gen_range(0, 2);

        match choice {
            0 => Set,
            1 => Del(g.gen::<u8>()),
            _ => panic!("impossible choice"),
        }
    }

    fn shrink(&self) -> Box<Iterator<Item = Op>> {
        match *self {
            Op::Del(ref lid) if *lid > 0 => Box::new(
                vec![Op::Del(*lid - 1)].into_iter(),
            ),
            _ => Box::new(vec![].into_iter()),
        }
    }
}

fn v(b: &Vec<u8>) -> u16 {
    assert_eq!(b.len(), 2);
    ((b[0] as u16) << 8) + b[1] as u16
}

fn prop_tree_crashes_nicely(ops: Vec<Op>, flusher: bool) -> bool {
    lazy_static! {
        // forces quickcheck to run one thread at a time
        static ref M: Mutex<()> = Mutex::new(());
    }

    let _lock = M.lock().unwrap();

    // clear all failpoints that may be left over from the last run
    fail::teardown();

    let config = ConfigBuilder::new()
        .temporary(true)
        .snapshot_after_ops(1)
        .flush_every_ms(if flusher { Some(1) } else {None})
        .io_buf_size(300)
        .min_items_per_segment(1)
        .blink_fanout(2) // smol pages for smol buffers
        .cache_capacity(40)
        .build();

    let mut tree = sled::Tree::start(config.clone()).unwrap();
    let mut reference = BTreeMap::new();
    let mut fail_points = HashSet::new();

    macro_rules! restart {
        () => {
            drop(tree);
            let tree_res = sled::Tree::start(config.clone());
            if let Err(ref e) = tree_res {
                if e == &Error::FailPoint {
                    return true;
                }

                println!("could not start database: {}", e);
                return false;
            }

            tree = tree_res.unwrap();

            let tree_iter = tree.iter().map(|res| {
                let (ref tk, _) = res.unwrap();
                v(tk)
            });
            let mut ref_iter = reference.iter().map(|(ref rk, ref rv)| (**rk, **rv));
            for t in tree_iter {
                // make sure the tree value is in there
                while let Some((r, (_rv, certainty))) = ref_iter.next() {
                    if certainty {
                        // tree MUST match reference if we have a certain reference
                        if t != r {
                            println!("expected to iterate over {:?} but got {:?} instead", r, t);
                            return false;
                        }
                        break;
                    } else {
                        // we have an uncertain reference, so we iterate through
                        // it and guarantee the reference is never higher than
                        // the tree value.
                        if t == r {
                            // we can move on to the next tree item
                            break;
                        }

                        if r > t {
                            // we have a bug, the reference iterator should always be <= tree
                            println!("tree verification failed: expected {:?} got {:?}", r, t);
                            return false;
                        }

                        // we are iterating through the reference until we have a certain
                        // item or an item that matches the tree's real item anyway
                    }
                }
            }
        }
    }

    macro_rules! fp_crash {
        ($e:expr) => {
            match $e {
                Ok(thing) => thing,
                Err(Error::FailPoint) => {
                    fail::teardown();
                    restart!();
                    continue;
                }
                other => {
                    println!("got non-failpoint err: {:?}", other);
                    return false;
                },
            }
        }
    }

    // we always increase set_counter because
    let mut set_counter = 0u16;

    for op in ops.into_iter() {
        match op {
            Set => {
                let hi = (set_counter >> 8) as u8;
                let lo = set_counter as u8;

                // insert false certainty until it fully completes
                reference.insert(set_counter, (set_counter, false));

                fp_crash!(tree.set(vec![hi, lo], vec![hi, lo]));

                // make sure we keep the disk and reference in-sync
                // maybe in the future put pending things in their own
                // reference and have a Flush op that syncs them.
                // just because the set above didn't hit a failpoint,
                // it doesn't mean this flush won't hit one, so we
                // also use the fp_crash macro here for handling it.
                fp_crash!(tree.flush());

                // now we should be certain the thing is in there, set certainty to true
                reference.insert(set_counter, (set_counter, true));

                set_counter += 1;
            }
            Del(k) => {
                // insert false certainty before completes
                reference.insert(k as u16, (k as u16, false));

                fp_crash!(tree.del(&*vec![0, k]));
                tree.flush().unwrap();

                reference.remove(&(k as u16));
            }
            Restart => {
                restart!();
            }
            FailPoint(fp) => {
                fail_points.insert(fp.clone());
                fail::cfg(&*fp, "return").unwrap();
            }
        }
    }

    fail::teardown();

    true
}

#[test]
fn quickcheck_tree_with_failpoints() {
    // use fewer tests for travis OSX builds that stall out all the time
    #[cfg(target_os = "macos")]
    let n_tests = 50;

    #[cfg(not(target_os = "macos"))]
    let n_tests = 100;

    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 100))
        .tests(n_tests)
        .max_tests(10000)
        .quickcheck(prop_tree_crashes_nicely as fn(Vec<Op>, bool) -> bool);
}

#[test]
fn failpoints_bug_1() {
    // postmortem 1: model did not account for proper reasons to fail to start
    assert!(prop_tree_crashes_nicely(
        vec![FailPoint("snap write"), Restart],
        false,
    ));
}

#[test]
fn failpoints_bug_2() {
    // postmortem 1: the system was assuming the happy path across failpoints
    assert!(prop_tree_crashes_nicely(
        vec![FailPoint("buffer write post"), Set, Set, Restart],
        false,
    ))
}

#[test]
fn failpoints_bug_3() {
    // postmortem 1: this was a regression that happened because we
    // chose to eat errors about advancing snapshots, which trigger
    // log flushes. We should not trigger flushes from snapshots,
    // but first we need to make sure we are better about detecting
    // tears, by not also using 0 as a failed flush signifier.
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            Set,
            Set,
            Set,
            FailPoint("trailer write"),
            Set,
            Set,
            Set,
            Set,
            Restart,
        ],
        false,
    ))
}

#[test]
fn failpoints_bug_4() {
    // postmortem 1: the test model was not properly accounting for
    // writes that may-or-may-not be present due to an error.
    assert!(prop_tree_crashes_nicely(
        vec![Set, FailPoint("snap write"), Del(0), Set, Restart],
        false,
    ))
}

#[test]
#[ignore]
fn failpoints_bug_5() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            FailPoint("snap write mv post"),
            Set,
            FailPoint("snap write"),
            Set,
            Set,
            Set,
            Restart,
            FailPoint("zero segment"),
            Set,
            Set,
            Set,
            Restart,
        ],
        false,
    ))
}

#[test]
#[ignore]
fn failpoints_bug_6() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            Del(0),
            Set,
            Set,
            Set,
            Restart,
            FailPoint("zero segment post"),
            Set,
            Set,
            Set,
            Restart,
        ],
        false,
    ))
}

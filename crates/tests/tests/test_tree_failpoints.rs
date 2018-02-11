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
    Set(u8, u8),
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
            //B"buffer write",
            //B"buffer write post",
            "write_config bytes",
            "write_config crc",
            "write_config post",
            "trailer write",
            "trailer write post",
            //B"snap write",
            //B"snap write len",
            //B"snap write crc",
            //B"snap write post",
            //B"snap write mv",
            //B"snap write mv post",
            "snap write rm old",
        ];

        if g.gen_weighted_bool(100) {
            return FailPoint((*g.choose(&fail_points).unwrap()));
        }

        if g.gen_weighted_bool(10) {
            return Restart;
        }

        let choice = g.gen_range(0, 2);

        match choice {
            0 => Set(g.gen::<u8>(), g.gen::<u8>()),
            1 => Del(g.gen::<u8>()),
            _ => panic!("impossible choice"),
        }
    }
}

#[derive(Debug, Clone)]
struct OpVec {
    ops: Vec<Op>,
}

impl Arbitrary for OpVec {
    fn arbitrary<G: Gen>(g: &mut G) -> OpVec {
        let mut ops = vec![];
        // FIXME set this to 0..500 and debug stalls
        for _ in 0..100 {
            let op = Op::arbitrary(g);
            ops.push(op);
        }
        OpVec {
            ops: ops,
        }
    }

    fn shrink(&self) -> Box<Iterator<Item = OpVec>> {
        let mut smaller = vec![];
        for i in 0..self.ops.len() {
            let mut clone = self.clone();
            clone.ops.remove(i);
            smaller.push(clone);
        }

        Box::new(smaller.into_iter())
    }
}

fn prop_tree_crashes_nicely(ops: OpVec) -> bool {
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
        .flush_every_ms(Some(1))
        .io_buf_size(10000)
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

                return false;
            }

            tree = tree_res.unwrap();

            let tree_iter = tree.iter().map(|res| {
                let (ref tk, ref tv) = res.unwrap();
                (tk[0], tv[0])
            });
            let ref_iter =
                reference.iter().map(|(ref rk, ref rv)| (**rk, **rv));
            for (t, r) in tree_iter.zip(ref_iter) {
                if t != r {
                    println!("tree value {:?} failed to match expected reference {:?}", t, r);
                    return false;
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
                _ => {
                    println!("about to unwrap an unexpected result: {:?}", $e);
                    return false;
                },
            }
        }
    }

    for op in ops.ops.into_iter() {
        match op {
            Set(k, v) => {
                fp_crash!(tree.set(vec![k], vec![v]));
                reference.insert(k, v);
            }
            Del(k) => {
                fp_crash!(tree.del(&*vec![k]));
                reference.remove(&k);
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
        .gen(StdGen::new(rand::thread_rng(), 1))
        .tests(n_tests)
        .max_tests(10000)
        .quickcheck(prop_tree_crashes_nicely as fn(OpVec) -> bool);
}

#[test]
fn failpoints_bug_1() {
    // postmortem 1: model did not account for proper reasons to fail to start
    assert_eq!(
        prop_tree_crashes_nicely(OpVec {
            ops: vec![FailPoint("snap write"), Restart],
        }),
        true
    )
}

#[test]
#[ignore]
fn failpoints_bug_2() {
    // postmortem 1:
    assert_eq!(
        prop_tree_crashes_nicely(OpVec {
            ops: vec![
                FailPoint("buffer write post"),
                Set(143, 67),
                Set(229, 66),
                Restart,
            ],
        }),
        true
    )
}

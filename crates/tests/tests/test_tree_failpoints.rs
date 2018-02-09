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
    FailPoint(String),
}

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        let fail_points = vec![
            "initial allocation",
            "initial allocation post",
            "buffer write",
            "buffer write post",
            "trailer write",
            "trailer write post",
            "zero garbage segment",
            "zero garbage segment post",
            "zero segment",
            "zero segment post",
            "snap write",
            "snap write len",
            "snap write crc",
            "snap write post",
            "snap write mv",
            "snap write mv post",
            "snap write rm old",
            "write_config bytes",
            "write_config crc",
            "write_config post",
        ];

        if g.gen_weighted_bool(10) {
            return Op::FailPoint((*g.choose(&fail_points).unwrap()).to_owned());
        }

        if g.gen_weighted_bool(10) {
            return Op::Restart;
        }

        let choice = g.gen_range(0, 2);

        match choice {
            0 => Op::Set(g.gen::<u8>(), g.gen::<u8>()),
            1 => Op::Del(g.gen::<u8>()),
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
        for _ in 0..g.gen_range(1, 100) {
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

    use self::Op::*;

    fail::setup();

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
            println!("restarting");
            drop(tree);
            let tree_res = sled::Tree::start(config.clone());
            if tree_res.is_err() {
                println!("failed to open tree: {:?}", tree_res.unwrap_err());
                clear_fps!();
                fail::teardown();
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
                    clear_fps!();
                    fail::teardown();
                    return false;
                }
            }
        }
    }

    macro_rules! clear_fps {
        () => {
            for fp in fail_points.drain() {
                fail::remove(fp);
            }
        }
    }

    macro_rules! fp_crash {
        ($e:expr) => {
            match $e {
                Ok(thing) => thing,
                Err(Error::FailPoint) => {
                    println!("got a crash");
                    clear_fps!();
                    restart!();
                    continue;
                }
                _ => {
                    println!("about to unwrap a weirdo {:?}", $e);
                    clear_fps!();
                    fail::teardown();
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
                println!("configuring fp at {}", fp);
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
    let n_tests = 100;

    #[cfg(not(target_os = "macos"))]
    let n_tests = 500;

    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 1))
        .tests(n_tests)
        .max_tests(10000)
        .quickcheck(prop_tree_crashes_nicely as fn(OpVec) -> bool);
}

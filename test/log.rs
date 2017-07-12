#![allow(unused)]
use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};
use rand::{self, Rng};

use rsdb::Log;

#[derive(Debug, Clone)]
enum Op {
    Write(Vec<u8>),
    WriteReservation(Vec<u8>),
    AbortReservation(Vec<u8>),
    Stabilize,
    Complete,
    Abort,
    Restart,
}

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        if g.gen_weighted_bool(2) {
            Op::Abort
        } else {
            Op::Complete
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
        OpVec { ops: ops }
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

fn prop_read_stable(ops: OpVec) -> bool {
    use self::Op::*;
    let log = Log::start_system("test_rsdb_quickcheck.log".to_owned());
    for op in ops.ops.iter() {
        match op {
            &Write(ref buf) => {
                log.write(&*buf);
            }
            &WriteReservation(ref buf) => {}
            &AbortReservation(ref buf) => {}
            &Stabilize => {}
            &Complete => {}
            &Abort => {}
            &Restart => {}
        }
    }
    true
}

#[test]
#[ignore]
fn qc_merge_converges() {
    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 1))
        .tests(10)
        .max_tests(10)
        .quickcheck(prop_read_stable as fn(OpVec) -> bool);
}

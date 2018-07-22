#[macro_use]
extern crate quickcheck;
extern crate art;
extern crate rand;

use self::quickcheck::{Arbitrary, Gen};

// The maximum key size. keeping it relatively
// small increases the chance of multiple
// operations being executed against the same
// key, which will tease out more bugs.
const KEY_SPACE: u8 = 20;

#[derive(Clone, Debug)]
enum Op {
    Insert(u8, u8),
    Get(u8),
}
use Op::{Get, Insert};

// Arbitrary lets you create randomized instances
// of types that you're interested in testing
// properties with. QuickCheck will look for
// this trait for things that are the arguments
// to properties that it is testing.
impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        // pick a random key to perform an operation on
        let k: u8 = g.gen_range(0, KEY_SPACE);

        if g.gen_weighted_bool(2) {
            Insert(k, g.gen())
        } else {
            Get(k)
        }
    }
}

// This macro is shorthand for creating a test
// function that calls the property functions inside.
// QuickCheck will generate a Vec of Op's of default
// length 100, which can be overridden by setting the
// QUICKCHECK_GENERATOR_SIZE env var or creating your
// own type that implements Arbitrary and using it as
// an argument to the property function.
quickcheck! {
    fn implementation_matches_model(ops: Vec<Op>) -> bool {
        let mut implementation = art::Art::default();
        let mut model = std::collections::BTreeMap::new();

        for op in ops {
            match op {
                Insert(k, v) => {
                    implementation.insert(vec![k; k as usize], v);
                    model.insert(k, v);
                }
                Get(k) => {
                    if implementation.get(&*vec![k; k as usize]) != model.get(&k) {
                        return false;
                    }
                }
            }
        }

        true
    }
}

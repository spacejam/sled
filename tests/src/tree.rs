use std::{collections::BTreeMap, fmt, panic};

use quickcheck::{Arbitrary, Gen, RngCore};
use rand::distributions::{Distribution, Gamma};
use rand::{rngs::StdRng, Rng, SeedableRng};

use sled::*;

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Key(pub Vec<u8>);

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Key(vec!{:?})", self.0)
    }
}

struct SledGen {
    r: StdRng,
    size: usize,
}

impl Gen for SledGen {
    fn size(&self) -> usize {
        self.size
    }
}

impl RngCore for SledGen {
    fn next_u32(&mut self) -> u32 {
        self.r.gen::<u32>()
    }

    fn next_u64(&mut self) -> u64 {
        self.r.gen::<u64>()
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.r.fill_bytes(dest)
    }

    fn try_fill_bytes(
        &mut self,
        dest: &mut [u8],
    ) -> std::result::Result<(), rand::Error> {
        self.r.try_fill_bytes(dest)
    }
}

pub fn fuzz_then_shrink(buf: &[u8]) {
    let ops: Vec<Op> = buf
        .chunks(2)
        .map(|chunk| {
            let mut seed = [0u8; 32];
            seed[0..chunk.len()].copy_from_slice(chunk);
            let rng: StdRng = SeedableRng::from_seed(seed);
            let mut sled_rng = SledGen { r: rng, size: 2 };

            Op::arbitrary(&mut sled_rng)
        })
        .collect();

    println!("working on ops {:?}", ops);
    match panic::catch_unwind(move || {
        prop_tree_matches_btreemap(ops, 0, 100, false)
    }) {
        Ok(_) => {}
        Err(_e) => panic!("TODO"),
    }
}

impl Arbitrary for Key {
    fn arbitrary<G: Gen>(g: &mut G) -> Key {
        if g.gen::<bool>() {
            let gs = g.size();
            let gamma = Gamma::new(0.3, gs as f64);
            let v = gamma.sample(&mut rand::thread_rng());
            let len = if v > 30000.0 {
                30000
            } else if v < 1. && v > 0.0001 {
                1
            } else {
                v as usize
            };

            let space = g.gen_range(0, gs) + 1;

            let inner = (0..len)
                .map(|_| g.gen_range(0, space) as u8)
                .collect();

            Key(inner)
        } else {
            let len = g.gen_range(0, 2);
            let mut inner = vec![];

            for _ in 0..len {
                inner.push(g.gen::<u8>());
            }

            Key(inner)
        }
    }

    fn shrink(&self) -> Box<Iterator<Item = Key>> {
        // we only want to shrink on length, not byte values
        Box::new(
            self.0
                .len()
                .shrink()
                .zip(std::iter::repeat(self.0.clone()))
                .map(|(len, underlying)| {
                    Key(underlying[..len].to_vec())
                }),
        )
    }
}

#[derive(Debug, Clone)]
pub enum Op {
    Set(Key, u8),
    Merge(Key, u8),
    Get(Key),
    GetLt(Key),
    GetGt(Key),
    Del(Key),
    Cas(Key, u8, u8),
    Scan(Key, usize),
    Restart,
}

use self::Op::*;

impl Op {
    pub fn is_restart(&self) -> bool {
        if let Restart = self {
            true
        } else {
            false
        }
    }
}

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        if g.gen_bool(1. / 10.) {
            return Restart;
        }

        let choice = g.gen_range(0, 8);

        match choice {
            0 => Set(Key::arbitrary(g), g.gen::<u8>()),
            1 => Merge(Key::arbitrary(g), g.gen::<u8>()),
            2 => Get(Key::arbitrary(g)),
            3 => GetLt(Key::arbitrary(g)),
            4 => GetGt(Key::arbitrary(g)),
            5 => Del(Key::arbitrary(g)),
            6 => Cas(Key::arbitrary(g), g.gen::<u8>(), g.gen::<u8>()),
            7 => Scan(Key::arbitrary(g), g.gen_range(0, 40)),
            _ => panic!("impossible choice"),
        }
    }

    fn shrink(&self) -> Box<Iterator<Item = Op>> {
        match *self {
            Set(ref k, v) => {
                Box::new(k.shrink().map(move |sk| Set(sk, v.clone())))
            }
            Merge(ref k, v) => {
                Box::new(k.shrink().map(move |k| Merge(k, v.clone())))
            }
            Get(ref k) => Box::new(k.shrink().map(move |k| Get(k))),
            GetLt(ref k) => {
                Box::new(k.shrink().map(move |k| GetLt(k)))
            }
            GetGt(ref k) => {
                Box::new(k.shrink().map(move |k| GetGt(k)))
            }
            Cas(ref k, old, new) => Box::new(Box::new(
                k.shrink()
                    .map(move |k| Cas(k, old.clone(), new.clone())),
            )),
            Scan(ref k, len) => {
                Box::new(k.shrink().map(move |k| Scan(k, len)))
            }
            Del(ref k) => Box::new(k.shrink().map(|k| Del(k))),
            Restart => Box::new(vec![].into_iter()),
        }
    }
}

fn bytes_to_u16(v: &[u8]) -> u16 {
    assert_eq!(v.len(), 2);
    ((v[0] as u16) << 8) + v[1] as u16
}

fn u16_to_bytes(u: u16) -> Vec<u8> {
    let ret = vec![(u >> 8) as u8, u as u8];
    ret
}

// just adds up values as if they were u16's
fn test_merge_operator(
    _k: &[u8],
    old: Option<&[u8]>,
    to_merge: &[u8],
) -> Option<Vec<u8>> {
    let base = old.unwrap_or(&[0, 0]);
    let base_n = bytes_to_u16(base);
    let new_n = base_n + to_merge[0] as u16;
    let ret = u16_to_bytes(new_n);
    Some(ret)
}

pub fn prop_tree_matches_btreemap(
    ops: Vec<Op>,
    blink_node_exponent: u8,
    snapshot_after: u8,
    flusher: bool,
) -> bool {
    use self::*;
    let config = ConfigBuilder::new()
        .temporary(true)
        .snapshot_after_ops(snapshot_after as usize + 1)
        .flush_every_ms(if flusher { Some(1) } else { None })
        .io_buf_size(10000)
        .blink_node_split_size(
            1 << std::cmp::min(blink_node_exponent, 20),
        )
        .cache_capacity(40)
        .cache_bits(0)
        .merge_operator(test_merge_operator)
        .build();

    let mut tree = sled::Tree::start(config.clone()).unwrap();
    let mut reference: BTreeMap<Key, u16> = BTreeMap::new();

    for op in ops.into_iter() {
        match op {
            Set(k, v) => {
                tree.set(&k.0, vec![0, v]).unwrap();
                reference.insert(k.clone(), v as u16);
            }
            Merge(k, v) => {
                tree.merge(&k.0, vec![v]).unwrap();
                let entry = reference.entry(k).or_insert(0u16);
                *entry += v as u16;
            }
            Get(k) => {
                let res1 = tree
                    .get(&*k.0)
                    .unwrap()
                    .map(|v| bytes_to_u16(&*v));
                let res2 = reference.get(&k).cloned();
                assert_eq!(res1, res2);
            }
            GetLt(k) => {
                let res1 = tree
                    .get_lt(&*k.0)
                    .unwrap()
                    .map(|v| bytes_to_u16(&*v.1));
                let res2 = reference
                    .iter()
                    .rev()
                    .filter(|(key, _)| *key < &k)
                    .nth(0)
                    .map(|(_, v)| *v);
                assert_eq!(res1, res2);
            }
            GetGt(k) => {
                let res1 = tree
                    .get_gt(&*k.0)
                    .unwrap()
                    .map(|v| bytes_to_u16(&*v.1));
                let res2 = reference
                    .iter()
                    .filter(|(key, _)| *key > &k)
                    .nth(0)
                    .map(|(_, v)| *v);
                assert_eq!(res1, res2);
            }
            Del(k) => {
                tree.del(&*k.0).unwrap();
                reference.remove(&k);
            }
            Cas(k, old, new) => {
                let tree_old = tree.get(&*k.0).unwrap();
                if let Some(old_tree) = tree_old {
                    if old_tree == &*vec![0, old] {
                        tree.set(&k.0, vec![0, new]).unwrap();
                    }
                }

                let ref_old = reference.get(&k).cloned();
                if ref_old == Some(old as u16) {
                    reference.insert(k, new as u16);
                }
            }
            Scan(k, len) => {
                let mut tree_iter = tree
                    .scan(&*k.0)
                    .take(len)
                    .map(|res| res.unwrap());
                let ref_iter = reference
                    .iter()
                    .filter(|&(ref rk, _rv)| **rk >= k)
                    .take(len)
                    .map(|(ref rk, ref rv)| (rk.0.clone(), **rv));

                for r in ref_iter {
                    let tree_next = tree_iter.next().unwrap();
                    let lhs = (tree_next.0, &*tree_next.1);
                    let rhs = (r.0.clone(), &*u16_to_bytes(r.1));
                    assert_eq!(
                        lhs, rhs,
                        "expected iteration over the Tree \
                         to match our BTreeMap model"
                    );
                }
            }
            Restart => {
                drop(tree);
                tree = sled::Tree::start(config.clone()).unwrap();
            }
        }
    }

    true
}

#[test]
fn test_fuzz_test() {
    let seed = [0; 32];
    fuzz_then_shrink(&seed);
    let seed = [0; 31];
    fuzz_then_shrink(&seed);
    let seed = [0; 33];
    fuzz_then_shrink(&seed);
    fuzz_then_shrink(&[]);
}

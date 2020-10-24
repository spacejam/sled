use std::{collections::BTreeMap, convert::TryInto, fmt, panic};

use quickcheck::{Arbitrary, Gen, RngCore};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rand_distr::{Distribution, Gamma};

use sled::*;

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Key(pub Vec<u8>);

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.0.is_empty() {
            write!(
                f,
                "Key(vec![{}; {}])",
                self.0.get(0).copied().unwrap_or(0),
                self.0.len()
            )
        } else {
            write!(f, "Key(vec!{:?})", self.0)
        }
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
    let use_compression = cfg!(feature = "compression")
        && !cfg!(miri)
        && buf.get(0).unwrap_or(&0) % 2 == 0;

    let ops: Vec<Op> = buf
        .chunks(2)
        .map(|chunk| {
            let mut seed = [0_u8; 32];
            seed[0..chunk.len()].copy_from_slice(chunk);
            let rng: StdRng = SeedableRng::from_seed(seed);
            let mut sled_rng = SledGen { r: rng, size: 2 };

            Op::arbitrary(&mut sled_rng)
        })
        .collect();

    let cache_bits = *buf.get(1).unwrap_or(&0);
    let segment_size_bits = *buf.get(2).unwrap_or(&0);

    match panic::catch_unwind(move || {
        prop_tree_matches_btreemap(
            ops,
            false,
            use_compression,
            cache_bits,
            segment_size_bits,
        )
    }) {
        Ok(_) => {}
        Err(_e) => panic!("TODO"),
    }
}

impl Arbitrary for Key {
    #![allow(clippy::cast_possible_truncation)]
    #![allow(clippy::cast_precision_loss)]
    #![allow(clippy::cast_sign_loss)]

    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        if g.gen::<bool>() {
            let gs = g.size();
            let gamma = Gamma::new(0.3, gs as f64).unwrap();
            let v = gamma.sample(&mut rand::thread_rng());
            let len = if v > 3000.0 { 10000 } else { (v % 300.) as usize };

            let space = g.gen_range(0, gs) + 1;

            let inner = (0..len).map(|_| g.gen_range(0, space) as u8).collect();

            Self(inner)
        } else {
            let len = g.gen_range(0, 2);
            let mut inner = vec![];

            for _ in 0..len {
                inner.push(g.gen::<u8>());
            }

            Self(inner)
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // we only want to shrink on length, not byte values
        Box::new(
            self.0
                .len()
                .shrink()
                .zip(std::iter::repeat(self.0.clone()))
                .map(|(len, underlying)| Self(underlying[..len].to_vec())),
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
    Scan(Key, isize),
    Restart,
}

use self::Op::{Cas, Del, Get, GetGt, GetLt, Merge, Restart, Scan, Set};

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
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
            7 => Scan(Key::arbitrary(g), g.gen_range(-40, 40)),
            _ => panic!("impossible choice"),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match *self {
            Set(ref k, v) => Box::new(k.shrink().map(move |sk| Set(sk, v))),
            Merge(ref k, v) => Box::new(
                k.shrink()
                    .flat_map(move |k| vec![Set(k.clone(), v), Merge(k, v)]),
            ),
            Get(ref k) => Box::new(k.shrink().map(Get)),
            GetLt(ref k) => Box::new(k.shrink().map(GetLt)),
            GetGt(ref k) => Box::new(k.shrink().map(GetGt)),
            Cas(ref k, old, new) => {
                Box::new(k.shrink().map(move |k| Cas(k, old, new)))
            }
            Scan(ref k, len) => Box::new(k.shrink().map(move |k| Scan(k, len))),
            Del(ref k) => Box::new(k.shrink().map(Del)),
            Restart => Box::new(vec![].into_iter()),
        }
    }
}

fn bytes_to_u16(v: &[u8]) -> u16 {
    assert_eq!(v.len(), 2);
    (u16::from(v[0]) << 8) + u16::from(v[1])
}

fn u16_to_bytes(u: u16) -> Vec<u8> {
    u.to_be_bytes().to_vec()
}

// just adds up values as if they were u16's
fn test_merge_operator(
    _k: &[u8],
    old: Option<&[u8]>,
    to_merge: &[u8],
) -> Option<Vec<u8>> {
    let base = old.unwrap_or(&[0, 0]);
    let base_n = bytes_to_u16(base);
    let new_n = base_n + u16::from(to_merge[0]);
    let ret = u16_to_bytes(new_n);
    Some(ret)
}

pub fn prop_tree_matches_btreemap(
    ops: Vec<Op>,
    flusher: bool,
    use_compression: bool,
    cache_bits: u8,
    segment_size_bits: u8,
) -> bool {
    if let Err(e) = prop_tree_matches_btreemap_inner(
        ops,
        flusher,
        use_compression,
        cache_bits,
        segment_size_bits,
    ) {
        eprintln!("hit error while running quickcheck on tree: {:?}", e);
        false
    } else {
        true
    }
}

fn prop_tree_matches_btreemap_inner(
    ops: Vec<Op>,
    flusher: bool,
    use_compression: bool,
    cache_bits: u8,
    segment_size_bits: u8,
) -> Result<()> {
    use self::*;

    super::common::setup_logger();

    let use_compression = cfg!(feature = "compression") && use_compression;

    let config = Config::new()
        .temporary(true)
        .use_compression(use_compression)
        .flush_every_ms(if flusher { Some(1) } else { None })
        .cache_capacity(256 * (1 << (cache_bits as usize % 16)))
        .idgen_persist_interval(1)
        .segment_size(256 * (1 << (segment_size_bits as usize % 16)));

    let mut tree = config.open().unwrap();
    tree.set_merge_operator(test_merge_operator);

    let mut reference: BTreeMap<Key, u16> = BTreeMap::new();

    for op in ops {
        match op {
            Set(k, v) => {
                let old_actual = tree.insert(&k.0, vec![0, v]).unwrap();
                let old_reference = reference.insert(k.clone(), u16::from(v));
                assert_eq!(
                    old_actual.map(|v| bytes_to_u16(&*v)),
                    old_reference,
                    "when setting key {:?}, expected old returned value to be {:?}\n{:?}",
                    k,
                    old_reference,
                    tree
                );
            }
            Merge(k, v) => {
                tree.merge(&k.0, vec![v]).unwrap();
                let entry = reference.entry(k).or_insert(0_u16);
                *entry += u16::from(v);
            }
            Get(k) => {
                let res1 = tree.get(&*k.0).unwrap().map(|v| bytes_to_u16(&*v));
                let res2 = reference.get(&k).cloned();
                assert_eq!(res1, res2);
            }
            GetLt(k) => {
                let res1 = tree.get_lt(&*k.0).unwrap().map(|v| v.0);
                let res2 = reference
                    .iter()
                    .rev()
                    .find(|(key, _)| **key < k)
                    .map(|(k, _v)| IVec::from(&*k.0));
                assert_eq!(
                    res1, res2,
                    "get_lt({:?}) should have returned {:?} \
                     but it returned {:?} instead. \
                     \n Db: {:?}",
                    k, res2, res1, tree
                );
            }
            GetGt(k) => {
                let res1 = tree.get_gt(&*k.0).unwrap().map(|v| v.0);
                let res2 = reference
                    .iter()
                    .find(|(key, _)| **key > k)
                    .map(|(k, _v)| IVec::from(&*k.0));
                assert_eq!(
                    res1, res2,
                    "get_gt({:?}) expected {:?} in tree {:?}",
                    k, res2, tree
                );
            }
            Del(k) => {
                tree.remove(&*k.0).unwrap();
                reference.remove(&k);
            }
            Cas(k, old, new) => {
                let tree_old = tree.get(&*k.0).unwrap();
                if let Some(old_tree) = tree_old {
                    if old_tree == *vec![0, old] {
                        tree.insert(&k.0, vec![0, new]).unwrap();
                    }
                }

                let ref_old = reference.get(&k).cloned();
                if ref_old == Some(u16::from(old)) {
                    reference.insert(k, u16::from(new));
                }
            }
            Scan(k, len) => {
                if len > 0 {
                    let mut tree_iter = tree
                        .range(&*k.0..)
                        .take(len.abs().try_into().unwrap())
                        .map(Result::unwrap);
                    let ref_iter = reference
                        .iter()
                        .filter(|&(ref rk, _rv)| **rk >= k)
                        .take(len.abs().try_into().unwrap())
                        .map(|(ref rk, ref rv)| (rk.0.clone(), **rv));

                    for r in ref_iter {
                        let tree_next = tree_iter.next().unwrap();
                        let lhs = (tree_next.0, &*tree_next.1);
                        let rhs = (r.0.clone(), &*u16_to_bytes(r.1));
                        assert_eq!(
                            (lhs.0.as_ref(), lhs.1),
                            (rhs.0.as_ref(), rhs.1),
                            "expected {:?} while iterating from {:?} on tree: {:?}",
                            rhs,
                            k,
                            tree
                        );
                    }
                } else {
                    let mut tree_iter = tree
                        .range(&*k.0..)
                        .rev()
                        .take(len.abs().try_into().unwrap())
                        .map(Result::unwrap);
                    let ref_iter = reference
                        .iter()
                        .rev()
                        .filter(|&(ref rk, _rv)| **rk >= k)
                        .take(len.abs().try_into().unwrap())
                        .map(|(ref rk, ref rv)| (rk.0.clone(), **rv));

                    for r in ref_iter {
                        let tree_next = tree_iter.next().unwrap();
                        let lhs = (tree_next.0, &*tree_next.1);
                        let rhs = (r.0.clone(), &*u16_to_bytes(r.1));
                        assert_eq!(
                            (lhs.0.as_ref(), lhs.1),
                            (rhs.0.as_ref(), rhs.1),
                            "expected {:?} while reverse iterating from {:?} on tree: {:?}",
                            rhs,
                            k,
                            tree
                        );
                    }
                }
            }
            Restart => {
                drop(tree);
                tree = config.open().unwrap();
                tree.set_merge_operator(test_merge_operator);
            }
        }
        if let Err(e) = config.global_error() {
            eprintln!("quickcheck test encountered error: {:?}", e);
            return Err(e);
        }
    }

    let space_amplification = tree
        .space_amplification()
        .expect("should be able to read files and pages");

    assert!(
        space_amplification < MAX_SPACE_AMPLIFICATION,
        "space amplification was measured to be {}, \
         which is higher than the maximum of {}",
        space_amplification,
        MAX_SPACE_AMPLIFICATION
    );

    drop(tree);
    config.global_error()
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

#![cfg(feature = "failpoints")]
mod common;

use std::collections::BTreeMap;
use std::convert::TryInto;
use std::sync::Mutex;

use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};
use rand::{seq::SliceRandom, Rng};

use sled::*;

const SEGMENT_SIZE: usize = 256;
const BATCH_COUNTER_KEY: &[u8] = b"batch_counter";

#[derive(Debug, Clone)]
enum Op {
    Set,
    Del(u8),
    Id,
    Batched(Vec<BatchOp>),
    Restart,
    Flush,
    FailPoint(&'static str, u64),
}

#[derive(Debug, Clone)]
enum BatchOp {
    Set,
    Del(u8),
}

impl Arbitrary for BatchOp {
    fn arbitrary<G: Gen>(g: &mut G) -> BatchOp {
        if g.gen_ratio(1, 2) {
            BatchOp::Set
        } else {
            BatchOp::Del(g.gen::<u8>())
        }
    }
}

use self::Op::*;

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        let fail_points = vec![
            "buffer write",
            "zero garbage segment",
            "zero garbage segment post",
            "zero garbage segment SA",
            "buffer write post",
            "write_config bytes",
            "write_config crc",
            "write_config post",
            "segment initial free zero",
            "snap write",
            "snap write len",
            "snap write crc",
            "snap write post",
            "snap write mv",
            "snap write mv post",
            "snap write rm old",
            "blob blob write",
            "write_blob write crc",
            "write_blob write kind_byte",
            "write_blob write buf",
            "file truncation",
        ];

        if g.gen_bool(1. / 30.) {
            return FailPoint(fail_points.choose(g).unwrap(), g.gen::<u64>());
        }

        if g.gen_bool(1. / 10.) {
            return Restart;
        }

        let choice = g.gen_range(0, 5);

        match choice {
            0 => Set,
            1 => Del(g.gen::<u8>()),
            2 => Id,
            3 => Batched(Arbitrary::arbitrary(g)),
            4 => Flush,
            _ => panic!("impossible choice"),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Op>> {
        match self {
            Del(ref lid) if *lid > 0 => {
                Box::new(vec![Del(*lid / 2), Del(*lid - 1)].into_iter())
            }
            Batched(batch_ops) => Box::new(batch_ops.shrink().map(Batched)),
            FailPoint(name, bitset) => {
                if bitset.count_ones() > 1 {
                    Box::new(
                        vec![
                            // clear last failure bit
                            FailPoint(
                                name,
                                bitset ^ (1 << (63 - bitset.leading_zeros())),
                            ),
                            // clear first failure bit
                            FailPoint(
                                name,
                                bitset ^ (1 << bitset.trailing_zeros()),
                            ),
                            // rewind all failure bits by one call
                            FailPoint(name, bitset >> 1),
                        ]
                        .into_iter(),
                    )
                } else if *bitset > 1 {
                    Box::new(vec![FailPoint(name, bitset >> 1)].into_iter())
                } else {
                    Box::new(vec![].into_iter())
                }
            }
            _ => Box::new(vec![].into_iter()),
        }
    }
}

fn v(b: &[u8]) -> u16 {
    if b[0] % 4 != 0 {
        assert_eq!(b.len(), 2);
    }
    (u16::from(b[0]) << 8) + u16::from(b[1])
}

fn value_factory(set_counter: u16) -> Vec<u8> {
    let hi = (set_counter >> 8) as u8;
    let lo = set_counter as u8;
    if hi % 4 == 0 {
        let mut val = vec![hi, lo];
        val.extend(vec![
            lo;
            hi as usize * SEGMENT_SIZE / 4 * set_counter as usize
        ]);
        val
    } else {
        vec![hi, lo]
    }
}

fn tear_down_failpoints() {
    sled::fail::reset();
}

#[derive(Debug)]
struct ReferenceVersion {
    value: Option<u16>,
    batch: Option<u32>,
}

#[derive(Debug)]
struct ReferenceEntry {
    versions: Vec<ReferenceVersion>,
    crash_epoch: u32,
}

fn prop_tree_crashes_nicely(ops: Vec<Op>, flusher: bool) -> bool {
    // forces quickcheck to run one thread at a time
    static M: Lazy<Mutex<()>, fn() -> Mutex<()>> = Lazy::new(|| Mutex::new(()));

    let _lock = M.lock().expect("our test lock should not be poisoned");

    // clear all failpoints that may be left over from the last run
    tear_down_failpoints();

    let res = std::panic::catch_unwind(|| {
        run_tree_crashes_nicely(ops.clone(), flusher)
    });

    tear_down_failpoints();

    match res {
        Err(e) => {
            println!(
                "failed with {:?} on ops {:?} flusher {}",
                e, ops, flusher
            );
            false
        }
        Ok(res) => {
            if !res {
                println!("failed with ops {:?} flusher: {}", ops, flusher);
            }
            res
        }
    }
}

fn run_tree_crashes_nicely(ops: Vec<Op>, flusher: bool) -> bool {
    common::setup_logger();

    let config = Config::new()
        .temporary(true)
        .flush_every_ms(if flusher { Some(1) } else { None })
        .cache_capacity(256)
        .idgen_persist_interval(1)
        .segment_size(SEGMENT_SIZE);

    let mut tree = config.open().expect("tree should start");
    let mut reference = BTreeMap::new();
    let mut max_id: isize = -1;
    let mut crash_counter = 0;
    let mut batch_counter: u32 = 1;

    // For each Set operation, one entry is inserted to the tree with a two-byte
    // key, and a variable-length value. The key is set to the encoded value
    // of the `set_counter`, which increments by one with each Set
    // operation. The value starts with the same two bytes as the
    // key does, but some values are extended to be many segments long.
    //
    // Del operations delete one entry from the tree. Only keys from 0 to 255
    // are eligible for deletion.

    macro_rules! restart {
        () => {
            drop(tree);
            let tree_res = config.global_error().and_then(|_| config.open());
            tree = match tree_res {
                Err(Error::FailPoint) => return true,
                Err(e) => {
                    println!("could not start database: {}", e);
                    return false;
                }
                Ok(tree) => tree,
            };

            let stable_batch = match tree.get(BATCH_COUNTER_KEY) {
                Ok(Some(value)) => u32::from_be_bytes(value.as_ref().try_into().unwrap()),
                Ok(None) => 0,
                Err(Error::FailPoint) => return true,
                Err(other) => panic!("failed to fetch batch counter after restart: {:?}", other),
            };
            for (_, ref_entry) in reference.iter_mut() {
                if ref_entry.versions.len() == 1 {
                    continue;
                }
                // find the last version from a stable batch, if there is one,
                // throw away all preceeding versions
                let committed_find_result = ref_entry.versions.iter().enumerate().rev().find(|(_, ReferenceVersion{ batch, value: _ })| match batch {
                    Some(batch) => *batch <= stable_batch,
                    None => false,
                });
                if let Some((committed_index, _)) = committed_find_result {
                    let tail_versions = ref_entry.versions.split_off(committed_index);
                    let _ = std::mem::replace(&mut ref_entry.versions, tail_versions);
                }
                // find the first version from a batch that wasn't committed,
                // throw away it and all subsequent versions
                let discarded_find_result = ref_entry.versions.iter().enumerate().find(|(_, ReferenceVersion{ batch, value: _})| match batch {
                    Some(batch) => *batch > stable_batch,
                    None => false,
                });
                if let Some((discarded_index, _)) = discarded_find_result {
                    let _ = ref_entry.versions.split_off(discarded_index);
                }
            }

            let mut ref_iter = reference.iter().map(|(ref rk, ref rv)| (**rk, *rv));
            for res in tree.iter() {
                let actual = match res {
                    Ok((ref tk, _)) => {
                        if tk == BATCH_COUNTER_KEY {
                            continue;
                        }
                        v(tk)
                    }
                    Err(Error::FailPoint) => return true,
                    Err(other) => panic!("failed to iterate over items in tree after restart: {:?}", other),
                };

                // make sure the tree value is in there
                while let Some((ref_key, ref_expected)) = ref_iter.next() {
                    if ref_expected.versions.iter().all(|version| version.value.is_none()) {
                        // this key should not be present in the tree, skip it and move on to the
                        // next entry in the reference
                        continue;
                    } else if ref_expected.versions.iter().all(|version| version.value.is_some()) {
                        // this key must be present in the tree, check if the keys from both
                        // iterators match
                        if actual != ref_key {
                            panic!(
                                "expected to iterate over key {:?} but got {:?} instead due to it being missing in \n\ntree: {:?}\n\nref: {:?}\n",
                                ref_key,
                                actual,
                                tree,
                                reference,

                            );
                        }
                        break;
                    } else {
                        // according to the reference, this key could either be present or absent,
                        // depending on whether recent writes were successful. check whether the
                        // keys from the two iterators match, if they do, the key happens to be
                        // present, which is okay, if they don't, and the tree iterator is further
                        // ahead than the reference iterator, the key happens to be absent, so we
                        // skip the entry in the reference. if the reference iterator ever gets
                        // further than the tree iterator, that means the tree has a key that it
                        // should not.
                        if actual == ref_key {
                            // tree and reference agree, we can move on to the next tree item
                            break;
                        } else if ref_key > actual {
                            // we have a bug, the reference iterator should always be <= tree
                            // (this means that the key t was in the tree, but it wasn't in
                            // the reference, so the reference iterator has advanced on past t)
                            println!(
                                "tree verification failed: expected {:?} got {:?}",
                                ref_key,
                                actual
                            );
                            return false;
                        } else {
                            // we are iterating through the reference until we have an item that
                            // must be present or an uncertain item that matches the tree's real
                            // item anyway
                            continue;
                        }
                    }
                }
            }

            // finish the rest of the reference iterator, and confirm the tree isn't missing
            // any keys it needs to have at the end
            while let Some((ref_key, ref_expected)) = ref_iter.next() {
                if ref_expected.versions.iter().all(|version| version.value.is_some()) {
                    // this key had to be present, but we got to the end of the tree without
                    // seeing it
                    println!("tree verification failed: expected {:?} got end", ref_key);
                    println!("expected: {:?}", ref_expected);
                    println!("tree: {:?}", tree);
                    return false;
                }
            }
            println!("finished verification");
        }
    }

    macro_rules! fp_crash {
        ($e:expr) => {
            match $e {
                Ok(thing) => thing,
                Err(Error::FailPoint) => {
                    tear_down_failpoints();
                    crash_counter += 1;
                    restart!();
                    continue;
                }
                other => {
                    println!("got non-failpoint err: {:?}", other);
                    return false;
                }
            }
        };
    }

    let mut set_counter = 0u16;

    println!("ops: {:?}", ops);

    for op in ops.into_iter() {
        match op {
            Set => {
                // update the reference to show that this key could be present.
                // the next Flush operation will update the
                // reference again, and require this key to be present
                // (unless there's a crash before then).
                let reference_entry = reference
                    .entry(set_counter)
                    .or_insert_with(|| ReferenceEntry {
                        versions: vec![ReferenceVersion {
                            value: None,
                            batch: None,
                        }],
                        crash_epoch: crash_counter,
                    });
                reference_entry.versions.push(ReferenceVersion {
                    value: Some(set_counter),
                    batch: None,
                });
                reference_entry.crash_epoch = crash_counter;

                fp_crash!(tree.insert(
                    &u16::to_be_bytes(set_counter),
                    value_factory(set_counter),
                ));

                set_counter += 1;
            }
            Del(k) => {
                // if this key was already set, update the reference to show
                // that this key could either be present or
                // absent. the next Flush operation will update the reference
                // again, and require this key to be absent (unless there's a
                // crash before then).
                reference.entry(u16::from(k)).and_modify(|v| {
                    v.versions
                        .push(ReferenceVersion { value: None, batch: None });
                    v.crash_epoch = crash_counter;
                });

                fp_crash!(tree.remove(&*vec![0, k]));
            }
            Id => {
                let id = fp_crash!(tree.generate_id());
                assert!(
                    id as isize > max_id,
                    "generated id of {} is not larger \
                     than previous max id of {}",
                    id,
                    max_id,
                );
                max_id = id as isize;
            }
            Batched(batch_ops) => {
                let mut batch = Batch::default();
                batch.insert(
                    BATCH_COUNTER_KEY,
                    batch_counter.to_be_bytes().to_vec(),
                );
                for batch_op in batch_ops {
                    match batch_op {
                        BatchOp::Set => {
                            let reference_entry = reference
                                .entry(set_counter)
                                .or_insert_with(|| ReferenceEntry {
                                    versions: vec![ReferenceVersion {
                                        value: None,
                                        batch: None,
                                    }],
                                    crash_epoch: crash_counter,
                                });
                            reference_entry.versions.push(ReferenceVersion {
                                value: Some(set_counter),
                                batch: Some(batch_counter),
                            });
                            reference_entry.crash_epoch = crash_counter;

                            batch.insert(
                                u16::to_be_bytes(set_counter).to_vec(),
                                value_factory(set_counter),
                            );

                            set_counter += 1;
                        }
                        BatchOp::Del(k) => {
                            reference.entry(u16::from(k)).and_modify(|v| {
                                v.versions.push(ReferenceVersion {
                                    value: None,
                                    batch: Some(batch_counter),
                                });
                                v.crash_epoch = crash_counter;
                            });

                            batch.remove(u16::to_be_bytes(k.into()).to_vec());
                        }
                    }
                }
                batch_counter += 1;
                fp_crash!(tree.apply_batch(batch));
            }
            Flush => {
                fp_crash!(tree.flush());

                // once a flush has been successfully completed, recent Set/Del
                // operations should be durable. go through the
                // reference, and if a Set/Del operation was done since
                // the last crash, keep the value for that key corresponding to
                // the most recent operation, and toss the rest.
                for (_key, reference_entry) in reference.iter_mut() {
                    if reference_entry.versions.len() > 1
                        && reference_entry.crash_epoch == crash_counter
                    {
                        let last = std::mem::replace(
                            &mut reference_entry.versions,
                            Vec::new(),
                        )
                        .pop()
                        .unwrap();
                        reference_entry.versions.push(last);
                    }
                }
            }
            Restart => {
                restart!();
            }
            FailPoint(fp, bitset) => {
                sled::fail::set(&*fp, bitset);
            }
        }
    }

    true
}

#[test]
#[cfg_attr(any(target_os = "fuchsia", miri), ignore)]
fn quickcheck_tree_with_failpoints() {
    // use fewer tests for travis OSX builds that stall out all the time
    let mut n_tests = 50;
    if let Ok(Ok(value)) = std::env::var("QUICKCHECK_TESTS").map(|s| s.parse())
    {
        n_tests = value;
    }

    let generator_sz = 100;

    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), generator_sz))
        .tests(n_tests)
        .quickcheck(prop_tree_crashes_nicely as fn(Vec<Op>, bool) -> bool);
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_01() {
    // postmortem 1: model did not account for proper reasons to fail to start
    assert!(prop_tree_crashes_nicely(
        vec![FailPoint("snap write", 0xFFFFFFFFFFFFFFFF), Restart],
        false,
    ));
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_02() {
    // postmortem 1: the system was assuming the happy path across failpoints
    assert!(prop_tree_crashes_nicely(
        vec![
            FailPoint("buffer write post", 0xFFFFFFFFFFFFFFFF),
            Set,
            Set,
            Restart
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_03() {
    // postmortem 1: this was a regression that happened because we
    // chose to eat errors about advancing snapshots, which trigger
    // log flushes. We should not trigger flushes from snapshots,
    // but first we need to make sure we are better about detecting
    // tears, by not also using 0 as a failed flush signifier.
    assert!(prop_tree_crashes_nicely(
        vec![Set, Set, Set, Set, Set, Set, Set, Set, Restart,],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_04() {
    // postmortem 1: the test model was not properly accounting for
    // writes that may-or-may-not be present due to an error.
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            FailPoint("snap write", 0xFFFFFFFFFFFFFFFF),
            Del(0),
            Set,
            Restart
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_05() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            FailPoint("snap write mv post", 0xFFFFFFFFFFFFFFFF),
            Set,
            FailPoint("snap write", 0xFFFFFFFFFFFFFFFF),
            Set,
            Set,
            Set,
            Restart,
            FailPoint("zero segment", 0xFFFFFFFFFFFFFFFF),
            Set,
            Set,
            Set,
            Restart,
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_06() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            Del(0),
            Set,
            Set,
            Set,
            Restart,
            FailPoint("zero segment post", 0xFFFFFFFFFFFFFFFF),
            Set,
            Set,
            Set,
            Restart,
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_07() {
    // postmortem 1: We were crashing because a Segment was
    // in the SegmentAccountant's to_clean Vec, but it had
    // no present pages. This can legitimately happen when
    // a Segment only contains failed log flushes.
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Del(17),
            Del(29),
            Del(246),
            Del(248),
            Set,
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_08() {
    // postmortem 1: we were assuming that deletes would fail if buffer writes
    // are disabled, but that's not true, because deletes might not cause any
    // writes if the value was not present.
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Del(0),
            FailPoint("buffer write post", 0xFFFFFFFFFFFFFFFF),
            Del(179),
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_09() {
    // postmortem 1: recovery was not properly accounting for
    // ordering issues around allocation and freeing of pages.
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            Restart,
            Del(110),
            Del(0),
            Set,
            Restart,
            Set,
            Del(255),
            Set,
            Set,
            Set,
            Set,
            Set,
            Del(38),
            Set,
            Set,
            Del(253),
            Set,
            Restart,
            Set,
            Del(19),
            Set,
            Del(118),
            Set,
            Set,
            Set,
            Set,
            Set,
            Del(151),
            Set,
            Set,
            Del(201),
            Set,
            Restart,
            Set,
            Set,
            Del(17),
            Set,
            Set,
            Set,
            Del(230),
            Set,
            Restart,
        ],
        true,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_10() {
    // expected to iterate over 50 but got 49 instead
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![
            Del(175),
            Del(19),
            Restart,
            Del(155),
            Del(111),
            Set,
            Del(4),
            Set,
            Set,
            Set,
            Set,
            Restart,
            Del(94),
            Set,
            Del(83),
            Del(181),
            Del(218),
            Set,
            Set,
            Del(60),
            Del(248),
            Set,
            Set,
            Set,
            Del(167),
            Del(180),
            Del(180),
            Set,
            Restart,
            Del(14),
            Set,
            Set,
            Del(156),
            Del(29),
            Del(190),
            Set,
            Set,
            Del(245),
            Set,
            Del(231),
            Del(95),
            Set,
            Restart,
            Set,
            Del(189),
            Set,
            Restart,
            Set,
            Del(249),
            Set,
            Set,
            Del(110),
            Del(75),
            Set,
            Restart,
            Del(156),
            Del(140),
            Del(101),
            Del(45),
            Del(115),
            Del(162),
            Set,
            Set,
            Del(192),
            Del(31),
            Del(224),
            Set,
            Del(84),
            Del(6),
            Set,
            Del(191),
            Set,
            Set,
            Set,
            Del(86),
            Del(143),
            Del(168),
            Del(175),
            Set,
            Restart,
            Set,
            Set,
            Set,
            Set,
            Set,
            Restart,
            Del(14),
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Del(60),
            Set,
            Del(115),
            Restart,
            Set,
            Del(203),
            Del(12),
            Del(134),
            Del(118),
            Del(26),
            Del(161),
            Set,
            Del(6),
            Del(23),
            Set,
            Del(122),
            Del(251),
            Set,
            Restart,
            Set,
            Set,
            Del(252),
            Del(88),
            Set,
            Del(140),
            Del(164),
            Del(203),
            Del(165),
            Set,
            Set,
            Restart,
            Del(0),
            Set,
            Del(146),
            Del(83),
            Restart,
            Del(0),
            Set,
            Del(55),
            Set,
            Set,
            Del(89),
            Set,
            Set,
            Del(105),
            Restart,
            Set,
            Restart,
            Del(145),
            Set,
            Del(17),
            Del(123),
            Set,
            Del(203),
            Set,
            Set,
            Set,
            Set,
            Del(192),
            Del(58),
            Restart,
            Set,
            Restart,
            Set,
            Restart,
            Set,
            Del(142),
            Set,
            Del(220),
            Del(185),
            Set,
            Del(86),
            Set,
            Set,
            Del(123),
            Set,
            Restart,
            Del(56),
            Del(191),
            Set,
            Set,
            Set,
            Set,
            Set,
            Del(123),
            Set,
            Set,
            Set,
            Restart,
            Del(20),
            Del(47),
            Del(207),
            Del(45),
            Set,
            Set,
            Set,
            Del(83),
            Set,
            Del(92),
            Del(117),
            Set,
            Set,
            Restart,
            Del(241),
            Set,
            Del(49),
            Set,
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_11() {
    // dupe lsn detected
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Restart,
            Del(21),
            Set,
            Set,
            FailPoint("buffer write post", 0xFFFFFFFFFFFFFFFF),
            Set,
            Set,
            Restart,
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_12() {
    // postmortem 1: we were not sorting the recovery state, which
    // led to divergent state across recoveries. TODO wut
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            Del(0),
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Restart,
            Set,
            Set,
            Set,
            Restart,
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_13() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Del(0),
            Set,
            Set,
            Set,
            Del(2),
            Set,
            Set,
            Set,
            Set,
            Del(1),
            Del(3),
            Del(18),
            Set,
            Set,
            Set,
            Restart,
            Set,
            Set,
            Set,
            Set,
            FailPoint("snap write", 0xFFFFFFFFFFFFFFFF),
            Del(4),
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_14() {
    // postmortem 1: improper bounds on splits caused a loop to happen
    assert!(prop_tree_crashes_nicely(
        vec![
            FailPoint("blob blob write", 0xFFFFFFFFFFFFFFFF),
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
            Set,
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_15() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![FailPoint("buffer write", 0xFFFFFFFFFFFFFFFF), Id, Restart, Id],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_16() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![FailPoint("zero garbage segment", 0xFFFFFFFFFFFFFFFF), Id, Id],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_17() {
    // postmortem 1: during recovery we were not properly
    // filtering replaced pages in segments by the source
    // segment still
    assert!(prop_tree_crashes_nicely(
        vec![
            Del(0),
            Set,
            Set,
            Set,
            Del(3),
            Id,
            Id,
            Set,
            Id,
            Id,
            Del(3),
            Id,
            Id,
            Del(3),
            Restart,
            Id,
            FailPoint("blob blob write", 0xFFFFFFFFFFFFFFFF),
            Id,
            Restart,
            Id,
            Set,
            Id,
            Del(3),
            Set
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_18() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![Id, Id, Set, Id, Id, Id, Set, Del(0), Restart, Del(0), Id, Set],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_19() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            Set,
            Set,
            Set,
            Del(4),
            Id,
            Del(4),
            Id,
            Id,
            Set,
            Set,
            Set,
            Set,
            Set,
            Id,
            Set,
            Set,
            Del(11),
            Del(13),
            Id,
            Del(122),
            Del(134),
            Del(101),
            Del(81),
            Set,
            Del(15),
            Del(76),
            Restart,
            Set,
            Id,
            Id,
            Set,
            Restart
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_20() {
    // postmortem 1: failed to filter out segments with
    // uninitialized segment ID's when creating a segment
    // iterator.
    assert!(prop_tree_crashes_nicely(
        vec![Restart, Set, Set, Del(0), Id, Id, Set, Del(0), Id, Set],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_21() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![
            Id,
            Del(242),
            Set,
            Del(172),
            Id,
            Del(142),
            Del(183),
            Set,
            Set,
            Set,
            Set,
            Set,
            Id,
            Id,
            Set,
            Id,
            Set,
            Id,
            Del(187),
            Set,
            Id,
            Set,
            Id,
            Del(152),
            Del(231),
            Del(45),
            Del(181),
            Restart,
            Id,
            Id,
            Id,
            Id,
            Id,
            Set,
            Del(53),
            Restart,
            Set,
            Del(202),
            Id,
            Set,
            Set,
            Set,
            Id,
            Restart,
            Del(99),
            Set,
            Set,
            Id,
            Restart,
            Del(93),
            Id,
            Set,
            Del(38),
            Id,
            Del(158),
            Del(49),
            Id,
            Del(145),
            Del(35),
            Set,
            Del(94),
            Del(115),
            Id,
            Restart,
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_22() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![Id, FailPoint("buffer write", 0xFFFFFFFFFFFFFFFF), Set, Id],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_23() {
    // postmortem 1: failed to handle allocation failures
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            FailPoint("blob blob write", 0xFFFFFFFFFFFFFFFF),
            Set,
            Set,
            Set
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_24() {
    // postmortem 1: was incorrectly setting global
    // errors, and they were being used-after-free
    assert!(prop_tree_crashes_nicely(
        vec![FailPoint("buffer write", 0xFFFFFFFFFFFFFFFF), Id,],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_25() {
    // postmortem 1: after removing segment trailers, we
    // no longer have the invariant that a write
    // must be more than one byte
    assert!(prop_tree_crashes_nicely(
        vec![
            Del(103),
            Restart,
            Del(242),
            Del(125),
            Restart,
            Set,
            Restart,
            Id,
            Del(183),
            Id,
            FailPoint("snap write crc", 0xFFFFFFFFFFFFFFFF),
            Del(141),
            Del(8),
            Del(188),
            Set,
            Set,
            Restart,
            Id,
            Id,
            Id,
            Set,
            Id,
            Id,
            Set,
            Del(65),
            Del(6),
            Del(198),
            Del(57),
            Id,
            FailPoint("snap write mv", 0xFFFFFFFFFFFFFFFF),
            Set,
            Del(164),
            Del(43),
            Del(161),
            Id,
            Restart,
            Set,
            Id,
            Id,
            Set,
            Set,
            Restart,
            Restart,
            Set,
            Set,
            Del(252),
            Set,
            Del(111),
            Id,
            Del(55)
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_26() {
    // postmortem 1: after removing segment trailers, we
    // no longer handled maxed segment recovery properly
    assert!(prop_tree_crashes_nicely(
        vec![
            Id,
            Set,
            Set,
            Del(167),
            Del(251),
            Del(24),
            Set,
            Del(111),
            Id,
            Del(133),
            Del(187),
            Restart,
            Set,
            Del(52),
            Set,
            Restart,
            Set,
            Set,
            Id,
            Set,
            Set,
            Id,
            Id,
            Set,
            Set,
            Del(95),
            Set,
            Id,
            Del(59),
            Del(133),
            Del(209),
            Id,
            Del(89),
            Id,
            Set,
            Del(46),
            Set,
            Del(246),
            Restart,
            Set,
            Restart,
            Restart,
            Del(28),
            Set,
            Del(9),
            Del(101),
            Id,
            Del(73),
            Del(192),
            Set,
            Set,
            Set,
            Id,
            Set,
            Set,
            Set,
            Id,
            Restart,
            Del(92),
            Del(212),
            Del(215)
        ],
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_27() {
    // postmortem 1: a segment is recovered as empty at recovery,
    // which prevented its lsn from being known, and when the SA
    // was recovered it erroneously calculated its lsn as being -1
    assert!(prop_tree_crashes_nicely(
        vec![
            Id,
            Id,
            Set,
            Set,
            Restart,
            Set,
            Id,
            Id,
            Set,
            Del(197),
            Del(148),
            Restart,
            Id,
            Set,
            Del(165),
            Set,
            Set,
            Set,
            Set,
            Id,
            Del(29),
            Set,
            Set,
            Del(75),
            Del(170),
            Restart,
            Restart,
            Set
        ],
        true,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_28() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![
            Del(61),
            Id,
            Del(127),
            Set,
            Restart,
            Del(219),
            Id,
            Set,
            Id,
            Del(41),
            Id,
            Id,
            Set,
            Del(227),
            Set,
            Del(191),
            Id,
            Del(78),
            Set,
            Id,
            Set,
            Del(123),
            Restart,
            Restart,
            Restart,
            Id
        ],
        true,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_29() {
    // postmortem 1: the test model was turning uncertain entries
    // into certain entries even when there was an intervening crash
    // between the Set and the Flush
    assert!(prop_tree_crashes_nicely(
        vec![
            FailPoint("buffer write", 0xFFFFFFFFFFFFFFFF),
            Set,
            Flush,
            Restart
        ],
        false,
    ));
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            Set,
            Set,
            FailPoint("snap write mv", 0xFFFFFFFFFFFFFFFF),
            Set,
            Flush,
            Restart
        ],
        false,
    ));
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_30() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![
            Set,
            FailPoint("buffer write", 0xFFFFFFFFFFFFFFFF),
            Restart,
            Flush,
            Id
        ],
        false,
    ));
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_31() {
    // postmortem 1: apply_batch_inner drops a RecoveryGuard, which in turn
    // drops a Reservation, and Reservation's drop implementation flushes
    // itself and unwraps the Result returned, which has the FailPoint error
    // in it
    for _ in 0..10 {
        assert!(prop_tree_crashes_nicely(
            vec![
                Del(0),
                FailPoint("snap write", 0xFFFFFFFFFFFFFFFF),
                Batched(vec![])
            ],
            true,
        ));
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_32() {
    // postmortem 1:
    for _ in 0..10 {
        assert!(prop_tree_crashes_nicely(
            vec![Batched(vec![BatchOp::Set, BatchOp::Set]), Restart],
            false
        ));
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_33() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![
            Batched(vec![
                BatchOp::Set,
                BatchOp::Set,
                BatchOp::Del(85),
                BatchOp::Set,
                BatchOp::Set,
                BatchOp::Set,
                BatchOp::Del(148),
                BatchOp::Set,
                BatchOp::Set
            ]),
            Restart,
            Batched(vec![
                BatchOp::Del(255),
                BatchOp::Del(42),
                BatchOp::Del(150),
                BatchOp::Del(16),
                BatchOp::Set,
                BatchOp::Set,
                BatchOp::Set,
                BatchOp::Set,
                BatchOp::Set,
                BatchOp::Del(111),
                BatchOp::Del(65),
                BatchOp::Del(102),
                BatchOp::Del(99),
                BatchOp::Del(25),
                BatchOp::Del(156),
                BatchOp::Set,
                BatchOp::Set,
                BatchOp::Set,
                BatchOp::Del(73),
                BatchOp::Set,
                BatchOp::Set,
                BatchOp::Set,
                BatchOp::Set,
                BatchOp::Del(238),
                BatchOp::Del(211),
                BatchOp::Del(14),
                BatchOp::Del(7),
                BatchOp::Del(137),
                BatchOp::Del(115),
                BatchOp::Del(91),
                BatchOp::Set,
                BatchOp::Del(172),
                BatchOp::Del(49),
                BatchOp::Del(152),
                BatchOp::Set,
                BatchOp::Del(189),
                BatchOp::Set,
                BatchOp::Del(37),
                BatchOp::Set,
                BatchOp::Set,
                BatchOp::Del(96),
                BatchOp::Set,
                BatchOp::Set,
                BatchOp::Del(159),
                BatchOp::Del(126)
            ])
        ],
        false
    ));
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_34() {
    // postmortem 1: the implementation of make_durable was not properly
    // exiting the function when local durability was detected
    use BatchOp::*;
    assert!(prop_tree_crashes_nicely(
        vec![Batched(vec![
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set, Set,
            Set, Set, Set, Set, Set, Set,
        ])],
        false
    ));
}

#[test]
#[cfg_attr(miri, ignore)]
fn failpoints_bug_35() {
    // postmortem 1:
    use BatchOp::*;
    for _ in 0..50 {
        assert!(prop_tree_crashes_nicely(
            vec![
                Batched(vec![Del(106), Set, Del(32), Del(149), Set]),
                Flush,
                Batched(vec![Del(136), Set, Set, Del(61), Set, Del(202)]),
                Flush,
                Batched(vec![Del(106), Set, Del(32), Del(149), Set]),
                Flush,
                Batched(vec![Del(136), Set, Set, Del(61), Set, Del(202)]),
                Flush,
                Batched(vec![Del(106), Set, Del(32), Del(149), Set]),
                Flush,
                Batched(vec![Del(136), Set, Set, Del(61), Set, Del(202)]),
                Flush,
            ],
            true
        ));
    }
}

#![cfg(feature = "failpoints")]
mod common;

use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;

use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};
use rand::{seq::SliceRandom, Rng};

use sled::*;

#[derive(Debug, Clone)]
enum Op {
    Set,
    Del(u8),
    Id,
    Restart,
    Flush,
    FailPoint(&'static str),
}

use self::Op::*;

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        let fail_points = vec![
            "zero garbage segment",
            "zero garbage segment post",
            "buffer write",
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
        ];

        if g.gen_bool(1. / 30.) {
            return FailPoint(fail_points.choose(g).unwrap());
        }

        if g.gen_bool(1. / 10.) {
            return Restart;
        }

        let choice = g.gen_range(0, 4);

        match choice {
            0 => Set,
            1 => Del(g.gen::<u8>()),
            2 => Id,
            3 => Flush,
            _ => panic!("impossible choice"),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Op>> {
        match *self {
            Op::Del(ref lid) if *lid > 0 => {
                Box::new(vec![Op::Del(*lid / 2), Op::Del(*lid - 1)].into_iter())
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

fn tear_down_failpoints() {
    sled::fail::reset();
}

#[derive(Debug)]
struct ReferenceEntry {
    values: Vec<Option<u16>>,
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

    let segment_size = 256;

    let config = Config::new()
        .temporary(true)
        .flush_every_ms(if flusher { Some(1) } else { None })
        .cache_capacity(256)
        .idgen_persist_interval(1)
        .segment_size(segment_size);

    let mut tree = config.open().expect("tree should start");
    let mut reference = BTreeMap::new();
    let mut fail_points = HashSet::new();
    let mut max_id: isize = -1;
    let mut crash_counter = 0;

    macro_rules! restart {
        () => {
            drop(tree);
            let tree_res = config.global_error().and_then(|_| config.open());
            if let Err(ref e) = tree_res {
                if e == &Error::FailPoint {
                    return true;
                }

                println!("could not start database: {}", e);
                return false;
            }

            tree = tree_res.expect("tree should restart");

            let mut ref_iter = reference.iter().map(|(ref rk, ref rv)| (**rk, *rv));
            for res in tree.iter() {
                let t = match res {
                    Ok((ref tk, _)) => v(tk),
                    Err(Error::FailPoint) => return true,
                    Err(other) => panic!("failed to iterate over items in tree after restart: {:?}", other),
                };

                // make sure the tree value is in there
                while let Some((ref_key, ref_expected)) = ref_iter.next() {
                    if ref_expected.values.iter().all(Option::is_none) {
                        // this key should not be present in the tree, skip it and move on to the
                        // next entry in the reference
                        continue;
                    } else if ref_expected.values.iter().all(Option::is_some) {
                        // this key must be present in the tree, check if the keys from both
                        // iterators match
                        if t != ref_key {
                            println!(
                                "expected to iterate over {:?} but got {:?} instead",
                                ref_key,
                                t
                            );
                            return false;
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
                        if t == ref_key {
                            // tree and reference agree, we can move on to the next tree item
                            break;
                        } else if ref_key > t {
                            // we have a bug, the reference iterator should always be <= tree
                            // (this means that the key t was in the tree, but it wasn't in
                            // the reference, so the reference iterator has advanced on past t)
                            println!(
                                "tree verification failed: expected {:?} got {:?}",
                                ref_key,
                                t
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
                if ref_expected.values.iter().all(Option::is_some) {
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
                let hi = (set_counter >> 8) as u8;
                let lo = set_counter as u8;
                let val = if hi % 4 == 0 {
                    let mut val = vec![hi, lo];
                    val.extend(vec![
                        lo;
                        hi as usize * segment_size / 4
                            * set_counter as usize
                    ]);
                    val
                } else {
                    vec![hi, lo]
                };

                // update the reference to show that this key could be present.
                // the next Flush operation will update the
                // reference again, and require this key to be present
                // (unless there's a crash before then).
                let reference_entry = reference
                    .entry(set_counter)
                    .or_insert_with(|| ReferenceEntry {
                        values: vec![None],
                        crash_epoch: crash_counter,
                    });
                reference_entry.values.push(Some(set_counter));
                reference_entry.crash_epoch = crash_counter;

                fp_crash!(tree.insert(&[hi, lo], val));

                set_counter += 1;
            }
            Del(k) => {
                // if this key was already set, update the reference to show
                // that this key could either be present or
                // absent. the next Flush operation will update the reference
                // again, and require this key to be absent (unless there's a
                // crash before then).
                reference.entry(u16::from(k)).and_modify(|v| {
                    v.values.push(None);
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
            Flush => {
                fp_crash!(tree.flush());

                // once a flush has been successfully completed, recent Set/Del
                // operations should be durable. go through the
                // reference, and if a Set/Del operation was done since
                // the last crash, keep the value for that key corresponding to
                // the most recent operation, and toss the rest.
                for (_key, reference_entry) in reference.iter_mut() {
                    if reference_entry.values.len() > 1 {
                        if reference_entry.crash_epoch == crash_counter {
                            let last = *reference_entry.values.last().unwrap();
                            reference_entry.values.clear();
                            reference_entry.values.push(last);
                        }
                    }
                }
            }
            Restart => {
                restart!();
            }
            FailPoint(fp) => {
                fail_points.insert(fp);
                sled::fail::set(&*fp);
            }
        }
    }

    true
}

#[test]
#[cfg(not(target_os = "fuchsia"))]
fn quickcheck_tree_with_failpoints() {
    // use fewer tests for travis OSX builds that stall out all the time
    let n_tests = 50;

    let generator_sz = 100;

    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), generator_sz))
        .tests(n_tests)
        .max_tests(10000)
        .quickcheck(prop_tree_crashes_nicely as fn(Vec<Op>, bool) -> bool);
}

#[test]
fn failpoints_bug_01() {
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
        vec![Set, Set, Set, Set, Set, Set, Set, Set, Restart,],
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

#[test]
fn failpoints_bug_7() {
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
fn failpoints_bug_8() {
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
            FailPoint("buffer write post"),
            Del(179),
        ],
        false,
    ))
}

#[test]
fn failpoints_bug_9() {
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
            FailPoint("buffer write post"),
            Set,
            Set,
            Restart,
        ],
        false,
    ))
}

#[test]
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
            FailPoint("snap write"),
            Del(4),
        ],
        false,
    ))
}

#[test]
fn failpoints_bug_14() {
    // postmortem 1: improper bounds on splits caused a loop to happen
    assert!(prop_tree_crashes_nicely(
        vec![
            FailPoint("blob blob write"),
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
fn failpoints_bug_15() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![FailPoint("buffer write"), Id, Restart, Id],
        false,
    ))
}

#[test]
fn failpoints_bug_16() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![FailPoint("zero garbage segment"), Id, Id],
        false,
    ))
}

#[test]
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
            FailPoint("blob blob write"),
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
fn failpoints_bug_18() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![Id, Id, Set, Id, Id, Id, Set, Del(0), Restart, Del(0), Id, Set],
        false,
    ))
}

#[test]
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
fn failpoints_bug_22() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![Id, FailPoint("buffer write"), Set, Id],
        false,
    ))
}

#[test]
fn failpoints_bug_23() {
    // postmortem 1: failed to handle allocation failures
    assert!(prop_tree_crashes_nicely(
        vec![Set, FailPoint("blob blob write"), Set, Set, Set],
        false,
    ))
}

#[test]
fn failpoints_bug_24() {
    // postmortem 1: was incorrectly setting global
    // errors, and they were being used-after-free
    assert!(prop_tree_crashes_nicely(
        vec![FailPoint("buffer write"), Id,],
        false,
    ))
}

#[test]
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
            FailPoint("snap write crc"),
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
            FailPoint("snap write mv"),
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
fn failpoints_bug_29() {
    // postmortem 1: the test model was turning uncertain entries
    // into certain entries even when there was an intervening crash
    // between the Set and the Flush
    assert!(prop_tree_crashes_nicely(
        vec![FailPoint("buffer write"), Set, Flush, Restart],
        false,
    ));
    assert!(prop_tree_crashes_nicely(
        vec![Set, Set, Set, FailPoint("snap write mv"), Set, Flush, Restart],
        false,
    ));
}

#[test]
fn failpoints_bug_30() {
    // postmortem 1:
    assert!(prop_tree_crashes_nicely(
        vec![Set, FailPoint("buffer write"), Restart, Flush, Id],
        false,
    ));
}

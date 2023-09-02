#![no_main]
#[macro_use]
extern crate libfuzzer_sys;
extern crate arbitrary;
extern crate sled;

use arbitrary::Arbitrary;

use sled::{Config, Db as SledDb, InlineArray};

type Db = SledDb<3>;

const KEYSPACE: u64 = 128;

#[derive(Debug)]
enum Op {
    Get { key: InlineArray },
    Insert { key: InlineArray, value: InlineArray },
    Reboot,
    Remove { key: InlineArray },
    Cas { key: InlineArray, old: Option<InlineArray>, new: Option<InlineArray> },
    Range { start: InlineArray, end: InlineArray },
}

fn keygen(
    u: &mut arbitrary::Unstructured<'_>,
) -> arbitrary::Result<InlineArray> {
    let key_i: u64 = u.int_in_range(0..=KEYSPACE)?;
    Ok(key_i.to_be_bytes().as_ref().into())
}

impl<'a> Arbitrary<'a> for Op {
    fn arbitrary(
        u: &mut arbitrary::Unstructured<'a>,
    ) -> arbitrary::Result<Self> {
        Ok(if u.ratio(1, 2)? {
            Op::Insert { key: keygen(u)?, value: keygen(u)? }
        } else if u.ratio(1, 2)? {
            Op::Get { key: keygen(u)? }
        } else if u.ratio(1, 2)? {
            Op::Reboot
        } else if u.ratio(1, 2)? {
            Op::Remove { key: keygen(u)? }
        } else if u.ratio(1, 2)? {
            Op::Cas {
                key: keygen(u)?,
                old: if u.ratio(1, 2)? { Some(keygen(u)?) } else { None },
                new: if u.ratio(1, 2)? { Some(keygen(u)?) } else { None },
            }
        } else {
            let start = u.int_in_range(0..=KEYSPACE)?;
            let end = (start + 1).max(u.int_in_range(0..=KEYSPACE)?);

            Op::Range {
                start: start.to_be_bytes().as_ref().into(),
                end: end.to_be_bytes().as_ref().into(),
            }
        })
    }
}

fuzz_target!(|ops: Vec<Op>| {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let tmp_path = tmp_dir.path().to_owned();
    let config = Config::new().path(tmp_path);

    let mut tree: Db = config.open().unwrap();
    let mut model = std::collections::BTreeMap::new();

    for (_i, op) in ops.into_iter().enumerate() {
        match op {
            Op::Insert { key, value } => {
                assert_eq!(
                    tree.insert(key.clone(), value.clone()).unwrap(),
                    model.insert(key, value)
                );
            }
            Op::Get { key } => {
                assert_eq!(tree.get(&key).unwrap(), model.get(&key).cloned());
            }
            Op::Reboot => {
                drop(tree);
                tree = config.open().unwrap();
            }
            Op::Remove { key } => {
                assert_eq!(tree.remove(&key).unwrap(), model.remove(&key));
            }
            Op::Range { start, end } => {
                let mut model_iter =
                    model.range::<InlineArray, _>(&start..&end);
                let mut tree_iter = tree.range(start..end);

                for (k1, v1) in &mut model_iter {
                    let (k2, v2) = tree_iter
                        .next()
                        .expect("None returned from iter when Some expected")
                        .expect("IO issue encountered");
                    assert_eq!((k1, v1), (&k2, &v2));
                }

                assert!(tree_iter.next().is_none());
            }
            Op::Cas { key, old, new } => {
                let succ = if old == model.get(&key).cloned() {
                    if let Some(n) = &new {
                        model.insert(key.clone(), n.clone());
                    } else {
                        model.remove(&key);
                    }
                    true
                } else {
                    false
                };

                let res = tree
                    .compare_and_swap(key, old.as_ref(), new)
                    .expect("hit IO error");

                if succ {
                    assert!(res.is_ok());
                } else {
                    assert!(res.is_err());
                }
            }
        };

        for (key, value) in &model {
            assert_eq!(tree.get(key).unwrap().unwrap(), value);
        }

        for kv_res in &tree {
            let (key, value) = kv_res.unwrap();
            assert_eq!(model.get(&key), Some(&value));
        }
    }

    let mut model_iter = model.iter();
    let mut tree_iter = tree.iter();

    for (k1, v1) in &mut model_iter {
        let (k2, v2) = tree_iter.next().unwrap().unwrap();
        assert_eq!((k1, v1), (&k2, &v2));
    }

    assert!(tree_iter.next().is_none());
});

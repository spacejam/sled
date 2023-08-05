#![no_main]
#[macro_use]
extern crate libfuzzer_sys;
extern crate arbitrary;
extern crate sled;

use arbitrary::Arbitrary;

use sled::InlineArray;

const KEYSPACE: u64 = 128;

#[derive(Debug)]
enum Op {
    Get { key: u64 },
    Insert { key: u64, value: u64 },
    Reboot,
    Remove { key: u64 },
    Cas { key: u64, old: Option<u64>, new: Option<u64> },
    Range { start: u64, end: u64 },
}

impl<'a> Arbitrary<'a> for Op {
    fn arbitrary(
        u: &mut arbitrary::Unstructured<'a>,
    ) -> arbitrary::Result<Self> {
        Ok(if u.ratio(1, 2)? {
            Op::Insert {
                key: u.int_in_range(0..=KEYSPACE as u64)?,
                value: u.int_in_range(0..=KEYSPACE as u64)?,
            }
        } else if u.ratio(1, 2)? {
            Op::Get { key: u.int_in_range(0..=KEYSPACE as u64)? }
        } else if u.ratio(1, 2)? {
            Op::Reboot
        } else if u.ratio(1, 2)? {
            Op::Remove { key: u.int_in_range(0..=KEYSPACE as u64)? }
        } else {
            let start = u.int_in_range(0..=KEYSPACE as u64)?;
            let end = (start + 1).max(u.int_in_range(0..=KEYSPACE as u64)?);
            Op::Range { start, end }
        })
    }
}

fuzz_target!(|ops: Vec<Op>| {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let tmp_path = tmp_dir.path().to_owned();

    let mut tree = sled::open_default(&tmp_path).unwrap();
    let mut model = std::collections::BTreeMap::new();

    for (_i, op) in ops.into_iter().enumerate() {
        match op {
            Op::Insert { key, value } => {
                let k = InlineArray::from(&key.to_be_bytes());
                let v = InlineArray::from(&value.to_be_bytes());
                assert_eq!(
                    tree.insert(k.clone(), v.clone()).unwrap(),
                    model.insert(k, v)
                );
            }
            Op::Get { key } => {
                let k: &[u8] = &key.to_be_bytes();
                assert_eq!(tree.get(k).unwrap(), model.get(k).cloned());
            }
            Op::Reboot => {
                drop(tree);
                tree = sled::open_default(&tmp_path).unwrap();
            }
            Op::Remove { key } => {
                let k: &[u8] = &key.to_be_bytes();
                assert_eq!(tree.remove(k).unwrap(), model.remove(k));
            }
            /*
            Op::Range { start, end } => {
                let mut model_iter = model.range(start..end);
                let mut tree_iter = tree.range(start..end);

                for (k1, v1) in &mut model_iter {
                    let (k2, v2) = tree_iter.next().unwrap();
                    assert_eq!((k1, v1), (&k2, &v2));
                }

                assert_eq!(tree_iter.next(), None);
            }
            Op::Cas { key, old, new } => {
                let succ = if old == model.get(&key).copied() {
                    if let Some(n) = new {
                        model.insert(key, n);
                    } else {
                        model.remove(&key);
                    }
                    true
                } else {
                    false
                };

                let res = tree.cas(key, old.as_ref(), new);

                if succ {
                    assert!(res.is_ok());
                } else {
                    assert!(res.is_err());
                }
            }
            */
            _ => {}
        };

        for (key, value) in &model {
            assert_eq!(tree.get(key).unwrap().unwrap(), value);
        }

        /* TODO
        for (key, value) in &tree {
            assert_eq!(model.get(key), Some(value));
        }
        */
    }

    /*
    let mut model_iter = model.iter();
    let mut tree_iter = tree.iter();

    for (k1, v1) in &mut model_iter {
        let (k2, v2) = tree_iter.next().unwrap().unwrap();
        assert_eq!((k1, v1), (&k2, &v2));
    }

    assert!(tree_iter.next().is_none());
    */
});

#[macro_use]
extern crate model;

extern crate pagetable;
extern crate sled_sync as sync;

use std::collections::HashMap;

use model::Shared;
use pagetable::PageTable;
use sync::{pin, Owned, Shared as EpochShared};

#[test]
fn test_model() {
    model! {
        Model => let mut m = HashMap::new(),
        Implementation => let i = PageTable::default(),
        Insert((usize, usize))((k, new) in (0usize..4, 0usize..4)) => {
            if !m.contains_key(&k) {
                m.insert(k, new);

                let guard = pin();
                let v = Owned::new(new).into_shared(&guard);
                i.cas(k, EpochShared::null(), v, &guard ).expect("should be able to insert a value");
            }
        },
        Get(usize)(k in 0usize..4) => {
            let guard = pin();
            let expected = m.get(&k);
            let actual = i.get(k, &guard).map(|s| unsafe { s.deref() });
            assert_eq!(expected, actual);
        },
        Cas((usize, usize, usize))((k, old, new) in (0usize..4, 0usize..4, 0usize..4)) => {
            let guard = pin();
            let expected_current = m.get(&k).cloned();
            let actual_current = i.get(k, &guard);
            assert_eq!(expected_current, actual_current.map(|s| unsafe { *s.deref() }));
            if expected_current.is_none() {
                continue;
            }

            let new_v = Owned::new(new).into_shared(&guard);

            if expected_current == Some(old) {
                m.insert(k, new);
                let cas_res = i.cas(k, actual_current.unwrap(), new_v, &guard);
                assert!(cas_res.is_ok());
            };
        }
    }
}

#[test]
#[ignore]
fn test_linearizability() {
    linearizable! {
        Implementation => let i = Shared::new(PageTable::default()),
        Get(usize)(k in any::<usize>()) -> Option<usize> {
            let guard = pin();
            unsafe {
                i.get(k, &guard).map(|s| *s.deref())
            }
        },
        Insert((usize, usize))((k, new) in (0usize..4, 0usize..4)) -> bool {
            let guard = pin();
            let v = Owned::new(new).into_shared(&guard);
            i.cas(k, EpochShared::null(), v, &guard).is_err()
        },
        Cas((usize, usize, usize))((k, old, new)
            in (0usize..4, 0usize..4, 0usize..4))
            -> Result<(), usize> {
            let guard = pin();
            i.cas(k, Owned::new(old).into_shared(&guard), Owned::new(new).into_shared(&guard), &guard)
                .map(|_| ())
                .map_err(|s| unsafe { *s.deref() })
        }
    }
}

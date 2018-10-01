#[macro_use]
extern crate model;
#[macro_use]
extern crate proptest;
extern crate crossbeam_epoch as epoch;
extern crate pagetable;

use std::collections::HashMap;

use epoch::pin;
use model::Shared;
use pagetable::PageTable;

#[test]
fn test_model() {
    model! {
        Model => let mut m = HashMap::new(),
        Implementation => let i = PageTable::default(),
        Insert((usize, usize))((k, new) in (0usize..4, 0usize..4)) => {
            if !m.contains_key(&k) {
                m.insert(k, new);
                i.insert(k, new).expect("should be able to insert a value");
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

            let new_v = epoch::Owned::new(new).into_shared(&guard);

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
        Insert((usize, usize))((k, new) in (0usize..4, 0usize..4)) -> Result<(), ()> {
            i.insert(k, new)
        },
        Cas((usize, usize, usize))((k, old, new)
            in (0usize..4, 0usize..4, 0usize..4))
            -> Result<(), usize> {
            let guard = pin();
            i.cas(k, epoch::Owned::new(old).into_shared(&guard), epoch::Owned::new(new).into_shared(&guard), &guard)
                .map(|_| ())
                .map_err(|s| unsafe { *s.deref() })
        }
    }
}

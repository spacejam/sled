#![allow(unused)]
use std::collections::HashMap;
use std::hash::Hash;
use std::io;
use std::ops::Range;
use std::sync::{Arc, RwLock, TryLockResult};

// NB use try_read / try_write to fake CAS failure semantics
// TODO in the future this will become a lockless version using CAS
#[derive(Clone)]
pub struct CASMap<K, V> {
    inner: Arc<RwLock<HashMap<K, Arc<RwLock<V>>>>>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CASFail;

impl<K: Eq + Hash, V: PartialEq> CASMap<K, V> {
    fn new() -> CASMap<K, V> {
        CASMap { inner: Arc::new(RwLock::new(HashMap::new())) }
    }

    fn cas(&self, k: K, old_v: Option<&V>, new_v: Option<V>) -> Result<(), CASFail> {
        // NB this is fake, aiming only to imitate a real CAS interface
        // TODO use real CAS
        if let Ok(mut inner) = self.inner.try_write() {
            if let Some(cur_lock) = inner.get(&k) {
                if old_v.is_none() {
                    // mismatch between old & current
                    return Err(CASFail);
                }
                let cur = cur_lock.read().unwrap();
                let old = old_v.unwrap();
                if *old != *cur {
                    // mismatch between old & current
                    return Err(CASFail);
                }
                // old & current compare to equal, good to continue to set new
            } else {
                if old_v.is_some() {
                    // mismatch between old & current
                    return Err(CASFail);
                }
                // old & current None, good to continue to set new
            }
            if let Some(new) = new_v {
                inner.insert(k, Arc::new(RwLock::new(new)));
            } else {
                inner.remove(&k);
            }
            Ok(())
        } else {
            return Err(CASFail);
        }

    }

    fn get(&self, k: &K) -> Option<Arc<RwLock<V>>> {
        let inner = self.inner.read().unwrap();
        inner.get(k).cloned()
    }
}

#[test]
fn basic_naive_map_test() {
    use std::thread;

    let nmt = CASMap::new();
    nmt.cas(1, None, Some(1));
    let nmt2 = nmt.clone();
    let nmt3 = nmt.clone();

    let t1 = thread::spawn(move || {
        let g1 = nmt2.get(&1).unwrap();
        assert_eq!(*g1.read().unwrap(), 1);
        nmt2.cas(1, Some(&1), Some(2));
        nmt2.cas(2, None, Some(2));
    });
    t1.join().unwrap();
    let t2 = thread::spawn(move || {
        let g1 = nmt3.get(&1).unwrap();
        let g2 = nmt3.get(&2).unwrap();
        assert_eq!(*g1.read().unwrap(), 2);
        assert_eq!(*g2.read().unwrap(), 2);
        nmt3.cas(3, None, Some(3));
    });
    t2.join().unwrap();
    let g3 = nmt.get(&3).unwrap();
    assert_eq!(*g3.read().unwrap(), 3);
    assert_eq!(nmt.cas(1, Some(&1), None), Err(CASFail));
    assert_eq!(nmt.cas(5, Some(&1), None), Err(CASFail));
    assert_eq!(nmt.cas(5, None, None), Ok(()));
    assert_eq!(nmt.cas(5, None, Some(1)), Ok(()));
}

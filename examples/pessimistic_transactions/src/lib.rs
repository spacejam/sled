//! An example of how to build multi-key transactions on top of
//! sled's single-key atomicity guarantees.
//!
//! The basic idea is to hash all of the keys, and acquire locks
//! for each one in a big array of RwLock's. If we fail to acquire
//! a lock in a non-blocking way, we try to acquire a global serializing
//! mutex, then take out blocking locks on the required keys. The
//! multiple locks are to avoid deadlock. Maybe it's just really buggy!
//! The global mutex does not prevent other thgets from claiming sharded
//! mutexes in a non-blocking way, so it shouldn't get hit all that often.
//! But this isn't measured, so maybe it happens all the time!
//! Computers, right?

extern crate sled;

mod crc16;

use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use sled::{Config, DbResult, Error, Tree};

macro_rules! rep_no_copy {
    ($e:expr; $n:expr) => {
        {
            let mut v = Vec::with_capacity($n);
            for _ in 0..$n {
                v.push($e);
            }
            v
        }
    };
}

type TxId = u64;
type Key = Vec<u8>;
type Value = Vec<u8>;
type PredicateFn = fn(&Key, &Option<Value>) -> bool;

#[derive(Debug, PartialEq)]
enum TxRet {
    Committed(Vec<(Key, Option<Value>)>),
    PredicateFailure,
}

unsafe impl Send for TxRet {}

struct TxDb {
    locks: Vec<RwLock<()>>,
    tree: Tree,
    blocker: Mutex<()>,
}

unsafe impl Send for TxDb {}
unsafe impl Sync for TxDb {}

struct Tx<'a> {
    predicates: Vec<Predicate>,
    sets: Vec<Write>,
    gets: Vec<Read>,
    deletes: Vec<Delete>,
    db: &'a TxDb,
}

unsafe impl<'a> Send for Tx<'a> {}

struct Predicate(Key, Box<PredicateFn>);
struct Write(Key, Value);
struct Read(Key);
struct Delete(Key);

fn split_txid_from_v(v: Value) -> DbResult<(TxId, Value), ()> {
    if v.len() < 8 {
        return Err(Error::ReportableBug("get bad txid".to_owned()));
    }
    let mut first_8 = [0; 8];
    first_8.copy_from_slice(&*v);
    let txid = unsafe { std::mem::transmute(first_8) };
    Ok((txid, v.into_iter().skip(8).collect()))
}

impl TxDb {
    fn start(config: Config) -> DbResult<TxDb, ()> {
        Ok(TxDb {
            // pluggable
            tree: Tree::start(config)?,
            blocker: Mutex::new(()),
            // 65k locks! webscale!!!
            locks: rep_no_copy!(RwLock::new(()); 1 << 16),
        })
    }

    fn tx<'a>(&'a self) -> Tx<'a> {
        Tx {
            predicates: vec![],
            gets: vec![],
            sets: vec![],
            deletes: vec![],
            db: &self,
        }
    }

    fn lock_keys(
        &self,
        get_hashes: &[u16],
        set_hashes: &[u16],
    ) -> (Vec<RwLockReadGuard<()>>, Vec<RwLockWriteGuard<()>>) {
        let mut get_locks = vec![];
        for get_idx in get_hashes {
            if let Ok(get_guard) = self.locks[*get_idx as usize].try_read() {
                get_locks.push(get_guard);
            } else {
                drop(get_locks);
                return self.pessimistic_lock_keys(get_hashes, set_hashes);
            }
        }

        let mut set_locks = vec![];
        for set_idx in set_hashes {
            if let Ok(set_guard) = self.locks[*set_idx as usize].try_write() {
                set_locks.push(set_guard);
            } else {
                drop(set_locks);
                let (_r, w) = self.pessimistic_lock_keys(&[], set_hashes);
                set_locks = w;
                break;
            }
        }

        (get_locks, set_locks)
    }

    fn pessimistic_lock_keys(
        &self,
        get_hashes: &[u16],
        set_hashes: &[u16],
    ) -> (Vec<RwLockReadGuard<()>>, Vec<RwLockWriteGuard<()>>) {
        let _global_mu = self.blocker.lock().unwrap();

        let mut get_locks = vec![];
        for get_idx in get_hashes {
            let get_guard = self.locks[*get_idx as usize].read().unwrap();
            get_locks.push(get_guard);
        }

        let mut set_locks = vec![];
        for set_idx in set_hashes {
            let set_guard = self.locks[*set_idx as usize].write().unwrap();
            set_locks.push(set_guard);
        }

        (get_locks, set_locks)
    }
}

impl<'a> Tx<'a> {
    fn predicate(&mut self, k: Key, p: PredicateFn) {
        self.predicates.push(Predicate(k, Box::new(p)));
    }

    fn get(&mut self, k: Key) {
        self.gets.push(Read(k));
    }

    fn set(&mut self, k: Key, v: Value) {
        self.sets.push(Write(k, v));
    }

    fn del(&mut self, k: Key) {
        self.deletes.push(Delete(k));
    }

    fn execute(self) -> DbResult<TxRet, ()> {
        // claim locks
        let mut get_keys = vec![];
        let mut set_keys = vec![];

        for &Predicate(ref k, _) in &self.predicates {
            get_keys.push(k);
        }

        for &Read(ref k) in &self.gets {
            get_keys.push(k);
        }

        for &Write(ref k, _) in &self.sets {
            set_keys.push(k);
        }

        for &Delete(ref k) in &self.deletes {
            set_keys.push(k);
        }

        let mut set_hashes: Vec<u16> =
            set_keys.into_iter().map(|k| crc16::crc16(k)).collect();
        set_hashes.sort();
        set_hashes.dedup();

        let mut get_hashes: Vec<u16> = get_keys
            .into_iter()
            .map(|k| crc16::crc16(k))
            .filter(|hash| !set_hashes.contains(hash))
            .collect();
        get_hashes.sort();
        get_hashes.dedup();

        let (get_locks, _set_locks) =
            self.db.lock_keys(&*get_hashes, &*set_hashes);

        // perform predicate matches
        for &Predicate(ref k, ref p) in &self.predicates {
            let current = self.db.tree.get(k)?;
            if !p(&k, &current) {
                return Ok(TxRet::PredicateFailure);
            }
        }

        // perform sets
        for &Write(ref k, ref v) in &self.sets {
            self.db.tree.set(k.clone(), v.clone())?;
        }

        // perform deletes
        for &Delete(ref k) in &self.deletes {
            self.db.tree.del(k)?;
        }

        // perform gets
        let mut ret_gets = vec![];
        for &Read(ref k) in &self.gets {
            ret_gets.push((k.clone(), self.db.tree.get(k)?));
        }

        Ok(TxRet::Committed(ret_gets))
    }
}

#[test]
fn it_works() {
    let conf = sled::ConfigBuilder::new().temporary(true).build();
    let txdb = TxDb::start(conf).unwrap();

    let mut tx = txdb.tx();
    tx.set(b"cats".to_vec(), b"meow".to_vec());
    tx.set(b"dogs".to_vec(), b"woof".to_vec());
    assert_eq!(tx.execute(), Ok(TxRet::Committed(vec![])));

    let mut tx = txdb.tx();
    tx.predicate(b"cats".to_vec(), |k, v| *v == Some(b"meow".to_vec()));
    tx.predicate(b"dogs".to_vec(), |k, v| *v == Some(b"woof".to_vec()));
    tx.set(b"cats".to_vec(), b"woof".to_vec());
    tx.set(b"dogs".to_vec(), b"meow".to_vec());
    tx.get(b"dogs".to_vec());
    assert_eq!(
        tx.execute(),
        Ok(TxRet::Committed(vec![(b"dogs".to_vec(), Some(b"meow".to_vec()))]))
    );

    let mut tx = txdb.tx();
    tx.predicate(b"cats".to_vec(), |k, v| *v == Some(b"meow".to_vec()));
    assert_eq!(tx.execute(), Ok(TxRet::PredicateFailure));
}

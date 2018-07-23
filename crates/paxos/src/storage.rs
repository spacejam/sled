use std::collections::HashMap;

use bincode::{deserialize, serialize};

use super::*;

/// Use Storage to plug this CASPaxos instance into an underlying store.
pub trait Storage: Clone {
    fn get_highest_seen(&mut self, Key) -> Ballot;
    fn get_accepted_ballot(&mut self, Key) -> Ballot;
    fn get_accepted_value(&mut self, Key) -> Option<Value>;
    fn set_highest_seen(&mut self, Key, Ballot);
    fn set_accepted_ballot(&mut self, Key, Ballot);
    fn set_accepted_value(&mut self, Key, Option<Value>);
}

const HIGHEST_SEEN_SUFFIX: u8 = 0;
const LAST_BALLOT_SUFFIX: u8 = 1;
const LAST_VALUE_SUFFIX: u8 = 2;

#[derive(Default, Clone)]
pub struct MemStorage {
    inner: HashMap<Key, Value>,
}

impl Storage for MemStorage {
    fn get_highest_seen(&mut self, mut k: Key) -> Ballot {
        k.push(HIGHEST_SEEN_SUFFIX);
        self.inner
            .get(&k)
            .cloned()
            .map(|v| deserialize(&v[..]).unwrap())
            .unwrap_or_else(|| Ballot::default())
    }
    fn get_accepted_ballot(&mut self, mut k: Key) -> Ballot {
        k.push(LAST_BALLOT_SUFFIX);
        self.inner
            .get(&k)
            .cloned()
            .map(|v| deserialize(&v[..]).unwrap())
            .unwrap_or_else(|| Ballot::default())
    }
    fn get_accepted_value(&mut self, mut k: Key) -> Option<Value> {
        k.push(LAST_VALUE_SUFFIX);
        self.inner.get(&k).cloned()
    }
    fn set_highest_seen(&mut self, mut k: Key, ballot: Ballot) {
        k.push(HIGHEST_SEEN_SUFFIX);
        let v = serialize(&ballot).unwrap();
        self.inner.insert(k, v);
    }
    fn set_accepted_ballot(&mut self, mut k: Key, ballot: Ballot) {
        k.push(LAST_BALLOT_SUFFIX);
        let v = serialize(&ballot).unwrap();
        self.inner.insert(k, v);
    }
    fn set_accepted_value(
        &mut self,
        mut k: Key,
        value: Option<Value>,
    ) {
        k.push(LAST_VALUE_SUFFIX);
        if let Some(v) = value {
            self.inner.insert(k, v);
        } else {
            self.inner.remove(&k);
        }
    }
}

#[derive(Clone)]
pub struct SledStorage {
    inner: sled::Tree,
}

impl SledStorage {
    pub fn new(tree: sled::Tree) -> SledStorage {
        SledStorage { inner: tree }
    }
}

impl Storage for SledStorage {
    fn get_highest_seen(&mut self, mut k: Key) -> Ballot {
        k.push(HIGHEST_SEEN_SUFFIX);
        self.inner
            .get(&k)
            .unwrap()
            .map(|v| deserialize(&v[..]).unwrap())
            .unwrap_or_else(|| Ballot::default())
    }
    fn get_accepted_ballot(&mut self, mut k: Key) -> Ballot {
        k.push(LAST_BALLOT_SUFFIX);
        self.inner
            .get(&k)
            .unwrap()
            .map(|v| deserialize(&v[..]).unwrap())
            .unwrap_or_else(|| Ballot::default())
    }
    fn get_accepted_value(&mut self, mut k: Key) -> Option<Value> {
        k.push(LAST_VALUE_SUFFIX);
        self.inner.get(&k).unwrap()
    }
    fn set_highest_seen(&mut self, mut k: Key, ballot: Ballot) {
        k.push(HIGHEST_SEEN_SUFFIX);
        let v = serialize(&ballot).unwrap();
        self.inner.set(k, v).unwrap();
    }
    fn set_accepted_ballot(&mut self, mut k: Key, ballot: Ballot) {
        k.push(LAST_BALLOT_SUFFIX);
        let v = serialize(&ballot).unwrap();
        self.inner.set(k, v).unwrap();
    }
    fn set_accepted_value(
        &mut self,
        mut k: Key,
        value: Option<Value>,
    ) {
        k.push(LAST_VALUE_SUFFIX);
        if let Some(v) = value {
            self.inner.set(k, v).unwrap();
        } else {
            self.inner.del(&k).unwrap();
        }
    }
}

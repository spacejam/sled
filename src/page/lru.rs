use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::rc::Rc;

use super::*;

pub struct Lru {
    shards: Vec<RwLock<Shard>>,
    cache_capacity: usize,
    cache_bits: usize,
    approx_size: AtomicIsize,
}

impl Lru {
    /// Instantiates a new `Lru` cache.
    pub fn new(cache_capacity: usize, cache_bits: usize) -> Lru {
        assert!(cache_bits <= 20,
                "way too many shards. use a smaller number of cache_bits");
        let size = 2 << cache_bits;
        Lru {
            shards: rep_no_copy![RwLock::new(Shard::default()); size],
            cache_capacity: cache_capacity,
            cache_bits: cache_bits,
            approx_size: AtomicIsize::new(1),
        }
    }

    /// Whenever data is paged-in, we set up bookkeeping for its accesses here.
    /// Returns a Vec of items that should now be paged-out to stay under the
    /// configured cache size.
    pub fn paged_in(&self, pid: PageID, lid: LogID, sz: usize) -> Vec<(PageID, LogID)> {
        let k: [u8; 8] = unsafe { std::mem::transmute(lid) };
        let h = hash::hash(&k, 0) as usize;
        let idx = h % (2 << self.cache_bits);

        let entry = Entry {
            pid: pid,
            lid: lid,
            mtime: time::get_time().sec,
            sz: sz,
            accesses: Rc::new(AtomicUsize::new(1)),
        };

        let ref shard_mu = self.shards[idx];
        let mut shard = shard_mu.write().unwrap();
        shard.insert(entry);

        let mut to_evict = vec![];
        let mut cur_sz = self.approx_size.fetch_add(sz as isize, Ordering::SeqCst) + sz as isize;
        while cur_sz > self.cache_capacity as isize {
            if shard.len() == 1 {
                // don't evict what we just added
                break;
            }
            let min = shard.min();
            shard.pop(min.lid).unwrap();
            to_evict.push((min.pid, min.lid));
            cur_sz -= min.sz as isize;
            self.approx_size.fetch_sub(min.sz as isize, Ordering::SeqCst);
        }

        to_evict
    }

    /// Bumps the LRU stats for this page.
    pub fn accessed(&self, lid: LogID) {
        let k: [u8; 8] = unsafe { std::mem::transmute(lid) };
        let h = hash::hash(&k, 0) as usize;
        let idx = h % (2 << self.cache_bits);

        let ref shard_mu = self.shards[idx];
        let mut shard = shard_mu.write().unwrap();

        shard.bump(lid);
    }
}

#[derive(Clone)]
struct Entry {
    pid: PageID,
    lid: LogID,
    mtime: i64,
    sz: usize,
    accesses: Rc<AtomicUsize>,
}

#[derive(Clone, Default)]
struct Shard {
    accesses: BTreeMap<i64, Entry>,
    entries: HashMap<LogID, Entry>,
}

impl Shard {
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn insert(&mut self, mut entry: Entry) {
        loop {
            if self.accesses.contains_key(&entry.mtime) {
                entry.mtime += 1;
                continue;
            }

            self.accesses.insert(entry.mtime, entry.clone());
            break;
        }
        self.entries.insert(entry.lid, entry);
    }

    fn bump(&mut self, lid: LogID) {
        let mtime = time::get_time().sec;
        let mut entry = self.entries.remove(&lid).unwrap();
        entry.accesses.fetch_add(1, Ordering::SeqCst);

        let old_mtime = entry.mtime;
        entry.mtime = mtime;

        self.accesses.remove(&old_mtime).unwrap();
        loop {
            if self.accesses.contains_key(&entry.mtime) {
                entry.mtime += 1;
                continue;
            }

            self.accesses.insert(entry.mtime, entry.clone());
            break;
        }
        self.entries.insert(entry.lid, entry);
    }

    fn pop(&mut self, lid: LogID) -> Result<(), ()> {
        let entry_opt = self.entries.remove(&lid);
        if entry_opt.is_none() {
            return Err(());
        }

        let mtime = entry_opt.unwrap().mtime;
        let access_opt = self.accesses.remove(&mtime);
        if access_opt.is_none() {
            return Err(());
        }

        Ok(())
    }

    fn min(&self) -> Entry {
        self.accesses.iter().nth(0).unwrap().1.clone()
    }
}

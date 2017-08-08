use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use super::*;

pub struct Lru {
    shards: Vec<Mutex<Shard>>,
    cache_bits: usize,
}

impl Lru {
    /// Instantiates a new `Lru` cache.
    pub fn new(cache_capacity: usize, cache_bits: usize) -> Lru {
        assert!(cache_bits <= 20,
                "way too many shards. use a smaller number of cache_bits");
        let size = 2 << cache_bits;
        let shard_capacity = cache_capacity / size;
        Lru {
            shards: rep_no_copy![Mutex::new(Shard::new(shard_capacity)); size],
            cache_bits: cache_bits,
        }
    }

    /// Called when a page is accessed. Returns a Vec of pages to
    /// try to page-out. For each one of these, the caller is expected
    /// to call `page_out_succeeded` if the page-out succeeded.
    pub fn accessed(&self, pid: PageID, sz: usize) -> Vec<PageID> {
        let idx = self.idx(pid);
        let ref shard_mu = self.shards[idx];
        let mut shard = shard_mu.lock().unwrap();
        shard.accessed(pid, sz)
    }

    /// Signal that a page-out was successful.
    pub fn page_out_succeeded(&self, pid: PageID) {
        let idx = self.idx(pid);
        let ref shard_mu = self.shards[idx];
        let mut shard = shard_mu.lock().unwrap();
        shard.page_out_succeeded(pid);
    }

    fn idx(&self, pid: PageID) -> usize {
        let k: [u8; 8] = unsafe { std::mem::transmute(pid) };
        let h = hash::hash(&k, 0) as usize;
        h % (2 << self.cache_bits)
    }
}

#[derive(Clone, Default)]
struct Entry {
    pid: PageID,
    mtime: i64,
    sz: usize,
    accesses: u64,
}

#[derive(Clone, Default)]
struct Shard {
    accesses: BTreeMap<i64, PageID>,
    entries: HashMap<PageID, Entry>,
    shadow: HashMap<PageID, Entry>,
    capacity: usize,
    sz: usize,
}

impl Shard {
    fn new(capacity: usize) -> Shard {
        let mut s = Shard::default();
        s.capacity = capacity;
        s
    }

    fn pop(&mut self, pid: PageID) -> Option<Entry> {
        let shadow = self.shadow.remove(&pid);
        if shadow.is_some() {
            return shadow;
        }

        if let Some(entry) = self.entries.remove(&pid) {
            self.accesses.remove(&entry.mtime);
            return Some(entry);
        }

        None
    }

    fn insert(&mut self, mut entry: Entry) {
        while self.accesses.contains_key(&entry.mtime) {
            entry.mtime += 1;
        }
        self.accesses.insert(entry.mtime, entry.pid);
        self.entries.insert(entry.pid, entry);
    }

    fn accessed(&mut self, pid: PageID, sz: usize) -> Vec<PageID> {
        let mut entry = self.pop(pid).unwrap_or_else(|| Entry::default());

        self.sz -= entry.sz;

        // TODO use much finer granularity
        let now = time::get_time().sec;

        entry.pid = pid;
        entry.accesses += 1;
        entry.sz = sz;
        entry.mtime = now;

        self.sz += entry.sz;

        self.insert(entry);

        let mut to_evict = vec![];
        while self.sz > self.capacity {
            if self.entries.len() == 1 {
                // don't evict what we just added
                break;
            }
            let min_pid = self.min();
            let min = self.pop(min_pid).unwrap();
            self.shadow.insert(min_pid, min.clone());

            to_evict.push(min.pid);
            self.sz -= min.sz;
        }

        to_evict
    }

    fn page_out_succeeded(&mut self, pid: PageID) {
        self.shadow.remove(&pid);
    }

    fn min(&self) -> PageID {
        *self.accesses.iter().nth(0).unwrap().1
    }
}

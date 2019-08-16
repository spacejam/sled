use parking_lot::Mutex;
use std::ptr;

use super::*;

/// A simple Lru cache.
pub struct Lru {
    shards: Vec<Mutex<Shard>>,
}

unsafe impl Sync for Lru {}

impl Lru {
    /// Instantiates a new `Lru` cache.
    pub fn new(cache_capacity: u64) -> Self {
        assert!(
            cache_capacity >= 256,
            "Please configure the cache \
             capacity to be at least 256 bytes"
        );
        let n_shards = 256;
        let shard_capacity = cache_capacity / n_shards as u64;

        let mut shards = Vec::with_capacity(n_shards);
        shards.resize_with(n_shards, || Mutex::new(Shard::new(shard_capacity)));

        Self { shards }
    }

    /// Called when a page is accessed. Returns a Vec of pages to
    /// try to page-out. For each one of these, the caller is expected
    /// to call `page_out_succeeded` if the page-out succeeded.
    pub fn accessed(&self, pid: PageId, sz: u64) -> Vec<PageId> {
        let shard_idx = pid % self.shards.len() as u64;
        let rel_idx = pid / self.shards.len() as u64;
        let shard_mu = &self.shards[usize::try_from(shard_idx).unwrap()];
        let mut shard = shard_mu.lock();
        let mut rel_ids = shard.accessed(rel_idx, sz);

        for rel_id in &mut rel_ids {
            let real_id = (*rel_id * self.shards.len() as u64) + shard_idx;
            *rel_id = real_id;
        }

        rel_ids
    }
}

#[derive(Clone)]
struct Entry {
    ptr: *mut dll::Node,
    sz: u64,
}

impl Default for Entry {
    fn default() -> Self {
        Self {
            ptr: ptr::null_mut(),
            sz: 0,
        }
    }
}

struct Shard {
    list: Dll,
    entries: Vec<Entry>,
    capacity: u64,
    sz: u64,
}

impl Shard {
    fn new(capacity: u64) -> Self {
        assert!(capacity > 0, "shard capacity must be non-zero");

        Self {
            list: Dll::default(),
            entries: vec![],
            capacity,
            sz: 0,
        }
    }

    fn accessed(&mut self, rel_idx: PageId, sz: u64) -> Vec<PageId> {
        if PageId::try_from(self.entries.len()).unwrap() <= rel_idx {
            self.entries.resize(
                usize::try_from(rel_idx).unwrap() + 1,
                Entry::default(),
            );
        }

        {
            let entry = &mut self.entries[usize::try_from(rel_idx).unwrap()];

            self.sz -= entry.sz;
            entry.sz = sz;
            self.sz += sz;

            if entry.ptr.is_null() {
                entry.ptr = self.list.push_head(rel_idx);
            } else {
                entry.ptr = self.list.promote(entry.ptr);
            }
        }

        let mut to_evict = vec![];
        while self.sz > self.capacity {
            if self.list.len() == 1 {
                // don't evict what we just added
                break;
            }

            let min_pid = self.list.pop_tail().unwrap();
            self.entries[usize::try_from(min_pid).unwrap()].ptr =
                ptr::null_mut();

            to_evict.push(min_pid);

            self.sz -= self.entries[usize::try_from(min_pid).unwrap()].sz;
            self.entries[usize::try_from(min_pid).unwrap()].sz = 0;
        }

        to_evict
    }
}

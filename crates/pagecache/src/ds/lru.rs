use std::ptr;
use std::sync::Mutex;

use super::*;

/// A simple Lru cache.
pub(crate) struct Lru {
    shards: Vec<Mutex<Shard>>,
}

unsafe impl Sync for Lru {}

impl Lru {
    /// Instantiates a new `Lru` cache.
    pub(crate) fn new(
        cache_capacity: usize,
        cache_bits: usize,
    ) -> Lru {
        assert!(
            cache_bits <= 20,
            "way too many shards. use a smaller number of cache_bits"
        );
        let size = 1 << cache_bits;
        let shard_capacity = cache_capacity / size;

        Lru {
            shards: rep_no_copy![Mutex::new(Shard::new(shard_capacity)); size],
        }
    }

    /// Called when a page is accessed. Returns a Vec of pages to
    /// try to page-out. For each one of these, the caller is expected
    /// to call `page_out_succeeded` if the page-out succeeded.
    pub(crate) fn accessed(
        &self,
        pid: PageId,
        sz: usize,
    ) -> Vec<PageId> {
        let shard_idx = pid % self.shards.len();
        let rel_idx = pid / self.shards.len();
        let shard_mu = &self.shards[shard_idx];
        let mut shard = shard_mu.lock().expect(
            "Lru was poisoned by a \
             thread that panicked \
             inside a critical section",
        );
        let mut rel_ids = shard.accessed(rel_idx, sz);

        for rel_id in &mut rel_ids {
            let real_id = (*rel_id * self.shards.len()) + shard_idx;
            *rel_id = real_id;
        }

        rel_ids
    }
}

#[derive(Clone)]
struct Entry {
    ptr: *mut dll::Node,
    sz: usize,
}

impl Default for Entry {
    fn default() -> Entry {
        Entry {
            ptr: ptr::null_mut(),
            sz: 0,
        }
    }
}

struct Shard {
    list: Dll,
    entries: Vec<Entry>,
    capacity: usize,
    sz: usize,
}

impl Shard {
    fn new(capacity: usize) -> Shard {
        assert!(capacity > 0, "shard capacity must be non-zero");

        Shard {
            list: Dll::default(),
            entries: vec![],
            capacity: capacity,
            sz: 0,
        }
    }

    fn accessed(
        &mut self,
        rel_idx: PageId,
        sz: usize,
    ) -> Vec<PageId> {
        if self.entries.len() <= rel_idx {
            self.entries.resize(rel_idx + 1, Entry::default());
        }

        {
            let entry = &mut self.entries[rel_idx];

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
            self.entries[min_pid].ptr = ptr::null_mut();

            to_evict.push(min_pid);

            self.sz -= self.entries[min_pid].sz;
            self.entries[min_pid].sz = 0;
        }

        to_evict
    }
}

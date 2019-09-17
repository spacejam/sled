use super::dll::{DoublyLinkedList, Item, Node};
use parking_lot::Mutex;
use std::convert::TryFrom;
use std::ptr;

/// A simple LRU cache.
pub struct Lru {
    shards: Vec<Mutex<Shard>>,
}

#[allow(unsafe_code)]
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

    /// Called when an item is accessed. Returns a Vec of items to be
    /// evicted.
    ///
    /// Items layout:
    ///   items:   1 2 3 4 5 6 7 8 9 10
    ///   shards:  1 0 1 0 1 0 1 0 1 0
    ///   shard 0:   2   4   6   8   10
    ///   shard 1: 1   3   5   7   9
    pub fn accessed(&self, id: Item, item_size: u64) -> Vec<Item> {
        let shards = self.shards.len() as u64;
        let (shard_idx, item_pos) = (id % shards, id / shards);
        let shard_mu: &Mutex<Shard> = &self.shards[safe_usize(shard_idx)];
        let mut shard = shard_mu.lock();
        let mut to_evict = shard.accessed(safe_usize(item_pos), item_size);
        // map shard internal offsets to global items ids
        to_evict
            .iter_mut()
            .for_each(|pos| *pos = (*pos * shards) + shard_idx);
        to_evict
    }
}

#[derive(Clone)]
struct Entry {
    ptr: *mut Node,
    size: u64,
}

impl Default for Entry {
    fn default() -> Self {
        Self {
            ptr: ptr::null_mut(),
            size: 0,
        }
    }
}

struct Shard {
    list: DoublyLinkedList,
    entries: Vec<Entry>,
    capacity: u64,
    size: u64,
}

impl Shard {
    fn new(capacity: u64) -> Self {
        assert!(capacity > 0, "shard capacity must be non-zero");

        Self {
            list: DoublyLinkedList::default(),
            entries: vec![],
            capacity,
            size: 0,
        }
    }

    /// Items in the shard list are indexes of the entries.
    fn accessed(&mut self, pos: usize, size: u64) -> Vec<Item> {
        if pos >= self.entries.len() {
            self.entries.resize(pos + 1, Entry::default());
        }

        {
            let entry = &mut self.entries[pos];

            self.size -= entry.size;
            entry.size = size;
            self.size += size;

            if entry.ptr.is_null() {
                entry.ptr = self.list.push_head(Item::try_from(pos).unwrap());
            } else {
                entry.ptr = self.list.promote(entry.ptr);
            }
        }

        let mut to_evict = vec![];
        while self.size > self.capacity {
            if self.list.len() == 1 {
                // don't evict what we just added
                break;
            }

            let min_pid = self.list.pop_tail().unwrap();
            let min_pid_idx = safe_usize(min_pid);

            self.entries[min_pid_idx].ptr = ptr::null_mut();

            to_evict.push(min_pid);

            self.size -= self.entries[min_pid_idx].size;
            self.entries[min_pid_idx].size = 0;
        }

        to_evict
    }
}

#[inline]
fn safe_usize(value: Item) -> usize {
    usize::try_from(value).unwrap()
}

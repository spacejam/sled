use std::sync::Mutex;

use super::*;

pub struct Cache {
    shards: Vec<Mutex<Shard>>,
    cache_capacity: usize,
    cache_bits: usize,
}

impl Cache {
    fn new(cache_capacity: usize, cache_bits: usize) -> Cache {
        assert!(cache_bits <= 20,
                "way too many shards. use a smaller number of cache_bits");
        let size = 2 << cache_bits;
        Cache {
            shards: rep_no_copy![Mutex::new(Shard); size],
            cache_capacity: cache_capacity,
            cache_bits: cache_bits,
        }
    }

    fn get(&self, k: Key) -> Value {
        let h = hash::hash(&*k, 0) as usize;
        let idx = h % (2 << self.cache_bits);
        let ref shard = self.shards[idx].lock().unwrap();

        shard.get(k)
    }
}

#[derive(Clone)]
struct Shard;

impl Shard {
    fn get(&self, _: Key) -> Value {
        vec![]
    }
}

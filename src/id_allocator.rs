use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_queue::SegQueue;
use fnv::FnvHashSet;
use parking_lot::Mutex;

#[derive(Default, Debug)]
pub(crate) struct Allocator {
    free_and_pending: Mutex<BinaryHeap<Reverse<u64>>>,
    /// Flat combining.
    ///
    /// A lock free queue of recently freed ids which uses when there is contention on `free_and_pending`.
    free_queue: SegQueue<u64>,
    next_to_allocate: AtomicU64,
}

impl Allocator {
    pub fn from_allocated(allocated: &FnvHashSet<u64>) -> Allocator {
        let mut heap = BinaryHeap::<Reverse<u64>>::default();
        let max = allocated.iter().copied().max();

        for i in 0..max.unwrap_or(0) {
            if !allocated.contains(&i) {
                heap.push(Reverse(i));
            }
        }

        Allocator {
            free_and_pending: Mutex::new(heap),
            free_queue: SegQueue::default(),
            next_to_allocate: max.map(|m| m + 1).unwrap_or(0).into(),
        }
    }

    pub fn allocate(&self) -> u64 {
        let mut free = self.free_and_pending.lock();
        while let Some(free_id) = self.free_queue.pop() {
            free.push(Reverse(free_id));
        }
        let pop_attempt = free.pop();

        if let Some(id) = pop_attempt {
            id.0
        } else {
            self.next_to_allocate.fetch_add(1, Ordering::Release)
        }
    }

    pub fn free(&self, id: u64) {
        if let Some(mut free) = self.free_and_pending.try_lock() {
            while let Some(free_id) = self.free_queue.pop() {
                free.push(Reverse(free_id));
            }
            free.push(Reverse(id));
        } else {
            self.free_queue.push(id);
        }
    }
}

pub(crate) struct DeferredFree {
    pub allocator: Arc<Allocator>,
    pub freed_slot: u64,
}

impl Drop for DeferredFree {
    fn drop(&mut self) {
        self.allocator.free(self.freed_slot)
    }
}

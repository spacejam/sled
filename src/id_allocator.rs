use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_queue::SegQueue;
use fnv::FnvHashSet;
use parking_lot::Mutex;

#[derive(Default, Debug)]
struct FreeSetAndTip {
    free_set: BTreeSet<u64>,
    next_to_allocate: u64,
}

#[derive(Default, Debug)]
pub struct Allocator {
    free_and_pending: Mutex<FreeSetAndTip>,
    /// Flat combining.
    ///
    /// A lock free queue of recently freed ids which uses when there is contention on `free_and_pending`.
    free_queue: SegQueue<u64>,
    allocation_counter: AtomicU64,
    free_counter: AtomicU64,
}

impl Allocator {
    /// Intended primarily for heap slab slot allocators when performing GC.
    ///
    /// If the slab is fragmented beyond the desired fill ratio, this returns
    /// the range of offsets (min inclusive, max exclusive) that may be copied
    /// into earlier free slots if they are currently occupied in order to
    /// achieve the desired fragmentation ratio.
    pub fn fragmentation_cutoff(
        &self,
        desired_ratio: f32,
    ) -> Option<(u64, u64)> {
        let mut free_and_tip = self.free_and_pending.lock();

        let next_to_allocate = free_and_tip.next_to_allocate;

        if next_to_allocate == 0 {
            return None;
        }

        while let Some(free_id) = self.free_queue.pop() {
            free_and_tip.free_set.insert(free_id);
        }

        let live_objects =
            next_to_allocate - free_and_tip.free_set.len() as u64;
        let actual_ratio = live_objects as f32 / next_to_allocate as f32;

        log::trace!(
            "fragmented_slots actual ratio: {actual_ratio}, free len: {}",
            free_and_tip.free_set.len()
        );

        if desired_ratio <= actual_ratio {
            return None;
        }

        // calculate theoretical cut-off point, return everything past that
        let min = (live_objects as f32 / desired_ratio) as u64;
        let max = next_to_allocate;
        assert!(min < max);
        Some((min, max))
    }

    pub fn from_allocated(allocated: &FnvHashSet<u64>) -> Allocator {
        let mut heap = BTreeSet::<u64>::default();
        let max = allocated.iter().copied().max();

        for i in 0..max.unwrap_or(0) {
            if !allocated.contains(&i) {
                heap.insert(i);
            }
        }

        let free_and_pending = Mutex::new(FreeSetAndTip {
            free_set: heap,
            next_to_allocate: max.map(|m| m + 1).unwrap_or(0),
        });

        Allocator {
            free_and_pending,
            free_queue: SegQueue::default(),
            allocation_counter: 0.into(),
            free_counter: 0.into(),
        }
    }

    pub fn max_allocated(&self) -> Option<u64> {
        let next = self.free_and_pending.lock().next_to_allocate;

        if next == 0 {
            None
        } else {
            Some(next - 1)
        }
    }

    pub fn allocate(&self) -> u64 {
        self.allocation_counter.fetch_add(1, Ordering::Relaxed);
        let mut free_and_tip = self.free_and_pending.lock();
        while let Some(free_id) = self.free_queue.pop() {
            free_and_tip.free_set.insert(free_id);
        }

        compact(&mut free_and_tip);

        let pop_attempt = free_and_tip.free_set.pop_first();

        if let Some(id) = pop_attempt {
            id
        } else {
            let ret = free_and_tip.next_to_allocate;
            free_and_tip.next_to_allocate += 1;
            ret
        }
    }

    pub fn free(&self, id: u64) {
        if cfg!(not(feature = "monotonic-behavior")) {
            self.free_counter.fetch_add(1, Ordering::Relaxed);
            if let Some(mut free) = self.free_and_pending.try_lock() {
                while let Some(free_id) = self.free_queue.pop() {
                    free.free_set.insert(free_id);
                }
                free.free_set.insert(id);

                compact(&mut free);
            } else {
                self.free_queue.push(id);
            }
        }
    }

    /// Returns the counters for allocated, free
    pub fn counters(&self) -> (u64, u64) {
        (
            self.allocation_counter.load(Ordering::Acquire),
            self.free_counter.load(Ordering::Acquire),
        )
    }
}

fn compact(free: &mut FreeSetAndTip) {
    let next = &mut free.next_to_allocate;

    while *next > 1 && free.free_set.contains(&(*next - 1)) {
        free.free_set.remove(&(*next - 1));
        *next -= 1;
    }
}

pub struct DeferredFree {
    pub allocator: Arc<Allocator>,
    pub freed_slot: u64,
}

impl Drop for DeferredFree {
    fn drop(&mut self) {
        self.allocator.free(self.freed_slot)
    }
}

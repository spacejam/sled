use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_queue::SegQueue;
use fnv::FnvHashSet;
use parking_lot::{Mutex, MutexGuard};

#[derive(Default, Debug)]
pub(crate) struct Allocator {
    free_and_pending: Mutex<BTreeSet<u64>>,
    /// Flat combining.
    ///
    /// A lock free queue of recently freed ids which uses when there is contention on `free_and_pending`.
    free_queue: SegQueue<u64>,
    next_to_allocate: AtomicU64,
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
        let next_to_allocate = self.next_to_allocate.load(Ordering::Acquire);

        if next_to_allocate == 0 {
            return None;
        }

        let mut free = self.free_and_pending.lock();
        while let Some(free_id) = self.free_queue.pop() {
            free.insert(free_id);
        }

        let live_objects = next_to_allocate - free.len() as u64;
        let actual_ratio = live_objects as f32 / next_to_allocate as f32;

        log::trace!(
            "fragmented_slots actual ratio: {actual_ratio}, free len: {}",
            free.len()
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

        Allocator {
            free_and_pending: Mutex::new(heap),
            free_queue: SegQueue::default(),
            next_to_allocate: max.map(|m| m + 1).unwrap_or(0).into(),
            allocation_counter: 0.into(),
            free_counter: 0.into(),
        }
    }

    pub fn max_allocated(&self) -> Option<u64> {
        let next = self.next_to_allocate.load(Ordering::Acquire);

        if next == 0 {
            None
        } else {
            Some(next - 1)
        }
    }

    pub fn allocate(&self) -> u64 {
        self.allocation_counter.fetch_add(1, Ordering::Relaxed);
        let mut free = self.free_and_pending.lock();
        while let Some(free_id) = self.free_queue.pop() {
            free.insert(free_id);
        }

        compact(&mut free, &self.next_to_allocate);

        let pop_attempt = free.pop_first();

        if let Some(id) = pop_attempt {
            id
        } else {
            self.next_to_allocate.fetch_add(1, Ordering::Release)
        }
    }

    pub fn free(&self, id: u64) {
        self.free_counter.fetch_add(1, Ordering::Relaxed);
        if let Some(mut free) = self.free_and_pending.try_lock() {
            while let Some(free_id) = self.free_queue.pop() {
                free.insert(free_id);
            }
            free.insert(id);

            compact(&mut free, &self.next_to_allocate);
        } else {
            self.free_queue.push(id);
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

fn compact(
    free: &mut MutexGuard<'_, BTreeSet<u64>>,
    next_to_allocate: &AtomicU64,
) {
    let mut next = next_to_allocate.load(Ordering::Acquire);

    while next > 1 && free.contains(&(next - 1)) {
        free.remove(&(next - 1));
        next = next_to_allocate.fetch_sub(1, Ordering::SeqCst) - 1;
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

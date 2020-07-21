#![allow(unsafe_code)]

use std::convert::TryFrom;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crate::{
    atomic_shim::AtomicU64,
    debug_delay,
    dll::{DoublyLinkedList, Node},
    fastlock::FastLock,
    Guard, PageId,
};

#[cfg(any(test, feature = "lock_free_delays"))]
const MAX_QUEUE_ITEMS: usize = 4;

#[cfg(not(any(test, feature = "lock_free_delays")))]
const MAX_QUEUE_ITEMS: usize = 64;

#[cfg(any(test, feature = "lock_free_delays"))]
const N_SHARDS: usize = 2;

#[cfg(not(any(test, feature = "lock_free_delays")))]
const N_SHARDS: usize = 256;

struct AccessBlock {
    len: AtomicUsize,
    block: [AtomicU64; MAX_QUEUE_ITEMS],
    next: AtomicPtr<AccessBlock>,
}

impl Default for AccessBlock {
    fn default() -> AccessBlock {
        AccessBlock {
            len: AtomicUsize::new(0),
            block: unsafe { MaybeUninit::zeroed().assume_init() },
            next: AtomicPtr::default(),
        }
    }
}

struct AccessQueue {
    writing: AtomicPtr<AccessBlock>,
    full_list: AtomicPtr<AccessBlock>,
}

impl Default for AccessQueue {
    fn default() -> AccessQueue {
        AccessQueue {
            writing: AtomicPtr::new(Box::into_raw(Box::new(
                AccessBlock::default(),
            ))),
            full_list: AtomicPtr::default(),
        }
    }
}

impl AccessQueue {
    fn push(&self, item: CacheAccess) -> bool {
        let mut filled = false;
        loop {
            debug_delay();
            let head = self.writing.load(Ordering::Acquire);
            let block = unsafe { &*head };

            debug_delay();
            let offset = block.len.fetch_add(1, Ordering::Release);

            if offset < MAX_QUEUE_ITEMS {
                debug_delay();
                unsafe {
                    block
                        .block
                        .get_unchecked(offset)
                        .store(item.0, Ordering::Release);
                }
                return filled;
            } else {
                // install new writer
                let new = Box::into_raw(Box::new(AccessBlock::default()));
                debug_delay();
                let prev =
                    self.writing.compare_and_swap(head, new, Ordering::Release);
                if prev != head {
                    // we lost the CAS, free the new item that was
                    // never published to other threads
                    unsafe {
                        drop(Box::from_raw(new));
                    }
                    continue;
                }

                // push the now-full item to the full list for future
                // consumption
                let mut ret;
                let mut full_list_ptr = self.full_list.load(Ordering::Acquire);
                while {
                    // we loop because maybe other threads are pushing stuff too
                    block.next.store(full_list_ptr, Ordering::Release);
                    debug_delay();
                    ret = self.full_list.compare_and_swap(
                        full_list_ptr,
                        head,
                        Ordering::Release,
                    );
                    ret != full_list_ptr
                } {
                    full_list_ptr = ret;
                }
                filled = true;
            }
        }
    }

    fn take<'a>(&self, guard: &'a Guard) -> CacheAccessIter<'a> {
        debug_delay();
        let ptr = self.full_list.swap(std::ptr::null_mut(), Ordering::AcqRel);

        CacheAccessIter { guard, current_offset: 0, current_block: ptr }
    }
}

impl Drop for AccessQueue {
    fn drop(&mut self) {
        debug_delay();
        let writing = self.writing.load(Ordering::Acquire);
        unsafe {
            Box::from_raw(writing);
        }
        debug_delay();
        let mut head = self.full_list.load(Ordering::Acquire);
        while !head.is_null() {
            unsafe {
                debug_delay();
                let next =
                    (*head).next.swap(std::ptr::null_mut(), Ordering::Release);
                Box::from_raw(head);
                head = next;
            }
        }
    }
}

struct CacheAccessIter<'a> {
    guard: &'a Guard,
    current_offset: usize,
    current_block: *mut AccessBlock,
}

impl<'a> Iterator for CacheAccessIter<'a> {
    type Item = CacheAccess;

    fn next(&mut self) -> Option<CacheAccess> {
        while !self.current_block.is_null() {
            let current_block = unsafe { &*self.current_block };

            debug_delay();
            if self.current_offset >= MAX_QUEUE_ITEMS {
                let to_drop_ptr = self.current_block;
                debug_delay();
                self.current_block = current_block.next.load(Ordering::Acquire);
                self.current_offset = 0;
                debug_delay();
                let to_drop = unsafe { Box::from_raw(to_drop_ptr) };
                self.guard.defer(|| to_drop);
                continue;
            }

            let mut next = 0;
            while next == 0 {
                // we spin here because there's a race between bumping
                // the offset and setting the value to something other
                // than 0 (and 0 is an invalid value)
                debug_delay();
                next = current_block.block[self.current_offset]
                    .load(Ordering::Acquire);
            }
            self.current_offset += 1;
            return Some(CacheAccess(next));
        }

        None
    }
}

#[derive(Clone, Copy)]
struct CacheAccess(u64);

impl CacheAccess {
    fn new(pid: PageId, sz: u64) -> CacheAccess {
        let rounded_up_power_of_2 =
            u64::from(sz.next_power_of_two().trailing_zeros());

        assert!(rounded_up_power_of_2 < 256);

        CacheAccess(pid | (rounded_up_power_of_2 << 56))
    }

    const fn decompose(self) -> (PageId, u64) {
        let sz = 1 << (self.0 >> 56);
        let pid = self.0 << 8 >> 8;
        (pid, sz)
    }
}

/// A simple LRU cache.
pub struct Lru {
    shards: Vec<(AccessQueue, FastLock<Shard>)>,
}

unsafe impl Sync for Lru {}

impl Lru {
    /// Instantiates a new `Lru` cache.
    pub(crate) fn new(cache_capacity: u64) -> Self {
        assert!(
            cache_capacity >= 256,
            "Please configure the cache \
             capacity to be at least 256 bytes"
        );
        let shard_capacity = cache_capacity / N_SHARDS as u64;

        let mut shards = Vec::with_capacity(N_SHARDS);
        shards.resize_with(N_SHARDS, || {
            (AccessQueue::default(), FastLock::new(Shard::new(shard_capacity)))
        });

        Self { shards }
    }

    /// Called when an item is accessed. Returns a Vec of items to be
    /// evicted. Uses flat-combining to avoid blocking on what can
    /// be an asynchronous operation.
    ///
    /// layout:
    ///   items:   1 2 3 4 5 6 7 8 9 10
    ///   shards:  1 0 1 0 1 0 1 0 1 0
    ///   shard 0:   2   4   6   8   10
    ///   shard 1: 1   3   5   7   9
    pub(crate) fn accessed(
        &self,
        id: PageId,
        item_size: u64,
        guard: &Guard,
    ) -> Vec<PageId> {
        let mut ret = vec![];
        let shards = self.shards.len() as u64;
        let (shard_idx, item_pos) = (id % shards, id / shards);
        let (stack, shard_mu) = &self.shards[safe_usize(shard_idx)];

        let filled = stack.push(CacheAccess::new(item_pos, item_size));

        if filled {
            // only try to acquire this if
            if let Some(mut shard) = shard_mu.try_lock() {
                let accesses = stack.take(guard);
                for item in accesses {
                    let (item_pos, item_size) = item.decompose();
                    let to_evict =
                        shard.accessed(safe_usize(item_pos), item_size);
                    // map shard internal offsets to global items ids
                    for pos in to_evict {
                        let item = (pos * shards) + shard_idx;
                        ret.push(item);
                    }
                }
            }
        }
        ret
    }
}

#[derive(Clone)]
struct Entry {
    ptr: *mut Node,
    size: u64,
}

impl Default for Entry {
    fn default() -> Self {
        Self { ptr: ptr::null_mut(), size: 0 }
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

    /// `PageId`s in the shard list are indexes of the entries.
    fn accessed(&mut self, pos: usize, size: u64) -> Vec<PageId> {
        if pos >= self.entries.len() {
            self.entries.resize(pos + 1, Entry::default());
        }

        {
            let entry = &mut self.entries[pos];

            self.size -= entry.size;
            entry.size = size;
            self.size += size;

            if entry.ptr.is_null() {
                entry.ptr = self.list.push_head(PageId::try_from(pos).unwrap());
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
fn safe_usize(value: PageId) -> usize {
    usize::try_from(value).unwrap()
}

#[test]
fn lru_smoke_test() {
    use crate::pin;

    let lru = Lru::new(256);
    for i in 0..1000 {
        let guard = pin();
        lru.accessed(i, 16, &guard);
    }
}

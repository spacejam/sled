#![allow(unsafe_code)]

use std::{
    borrow::{Borrow, BorrowMut},
    convert::TryFrom,
    hash::{Hash, Hasher},
    mem::MaybeUninit,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

use crate::{
    atomic_shim::AtomicU64,
    debug_delay,
    dll::{DoublyLinkedList, Node},
    fastlock::FastLock,
    FastSet8, Guard, PageId,
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

impl AccessBlock {
    fn new(item: CacheAccess) -> AccessBlock {
        let mut ret = AccessBlock {
            len: AtomicUsize::new(1),
            block: unsafe { MaybeUninit::zeroed().assume_init() },
            next: AtomicPtr::default(),
        };
        ret.block[0] = AtomicU64::from(u64::from(item));
        ret
    }
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
        loop {
            debug_delay();
            let head = self.writing.load(Ordering::Acquire);
            let block = unsafe { &*head };

            debug_delay();
            let offset = block.len.fetch_add(1, Ordering::Acquire);

            if offset < MAX_QUEUE_ITEMS {
                let item_u64: u64 = item.into();
                assert_ne!(item_u64, 0);
                debug_delay();
                unsafe {
                    block
                        .block
                        .get_unchecked(offset)
                        .store(item_u64, Ordering::Release);
                }
                return false;
            } else {
                // install new writer
                let new = Box::into_raw(Box::new(AccessBlock::new(item)));
                debug_delay();
                let res = self.writing.compare_exchange(
                    head,
                    new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                if res.is_err() {
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
                    ret = self.full_list.compare_exchange(
                        full_list_ptr,
                        head,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    ret.is_err()
                } {
                    full_list_ptr = ret.unwrap_err();
                }
                return true;
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
            return Some(CacheAccess::from(next));
        }

        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CacheAccess {
    // safe because MAX_PID_BITS / N_SHARDS < u32::MAX
    //                     2**37 /   2**8   < 2**32
    pub pid: u32,
    pub sz: u8,
}

impl From<CacheAccess> for u64 {
    fn from(ca: CacheAccess) -> u64 {
        (u64::from(ca.pid) << 8) | u64::from(ca.sz)
    }
}

#[allow(clippy::fallible_impl_from)]
impl From<u64> for CacheAccess {
    fn from(u: u64) -> CacheAccess {
        let sz = usize::try_from((u << 56) >> 56).unwrap();
        assert_ne!(sz, 0);
        let pid = u >> 8;
        assert!(pid < u64::from(u32::MAX));
        CacheAccess {
            pid: u32::try_from(pid).unwrap(),
            sz: u8::try_from(sz).unwrap(),
        }
    }
}

impl CacheAccess {
    fn size(&self) -> usize {
        1 << usize::from(self.sz)
    }

    fn new(pid: PageId, sz: usize) -> CacheAccess {
        let rounded_up_power_of_2 =
            u8::try_from(sz.next_power_of_two().trailing_zeros()).unwrap();

        CacheAccess {
            pid: u32::try_from(pid).expect("expected caller to shift pid down"),
            sz: rounded_up_power_of_2,
        }
    }
}

/// A simple LRU cache.
pub struct Lru {
    shards: Vec<(AccessQueue, FastLock<Shard>)>,
}

impl Lru {
    /// Instantiates a new `Lru` cache.
    pub(crate) fn new(cache_capacity: usize) -> Self {
        assert!(
            cache_capacity >= N_SHARDS,
            "Please configure the cache \
             capacity to be at least 256 bytes"
        );
        let shard_capacity = cache_capacity / N_SHARDS;

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
        item_size: usize,
        guard: &Guard,
    ) -> Vec<PageId> {
        const SHARD_BITS: usize = N_SHARDS.trailing_zeros() as usize;

        let mut ret = vec![];
        let shards = N_SHARDS as u64;
        let (shard_idx, shifted_pid) = (id % shards, id >> SHARD_BITS);
        let (access_queue, shard_mu) = &self.shards[safe_usize(shard_idx)];

        let cache_access = CacheAccess::new(shifted_pid, item_size);
        let filled = access_queue.push(cache_access);

        if filled {
            // only try to acquire this if the access queue has filled
            // an entire segment
            if let Some(mut shard) = shard_mu.try_lock() {
                let accesses = access_queue.take(guard);
                for item in accesses {
                    let to_evict = shard.accessed(item);
                    // map shard internal offsets to global items ids
                    for pos in to_evict {
                        let address =
                            (PageId::from(pos) << SHARD_BITS) + shard_idx;
                        ret.push(address);
                    }
                }
            }
        }
        ret
    }
}

#[derive(Eq)]
struct Entry(*mut Node);

unsafe impl Send for Entry {}

impl Ord for Entry {
    fn cmp(&self, other: &Entry) -> std::cmp::Ordering {
        let left_pid: u32 = *self.borrow();
        let right_pid: u32 = *other.borrow();
        left_pid.cmp(&right_pid)
    }
}

impl PartialOrd<Entry> for Entry {
    fn partial_cmp(&self, other: &Entry) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Entry) -> bool {
        unsafe { (*self.0).pid == (*other.0).pid }
    }
}

impl BorrowMut<CacheAccess> for Entry {
    fn borrow_mut(&mut self) -> &mut CacheAccess {
        unsafe { &mut *self.0 }
    }
}

impl Borrow<CacheAccess> for Entry {
    fn borrow(&self) -> &CacheAccess {
        unsafe { &*self.0 }
    }
}

impl Borrow<u32> for Entry {
    fn borrow(&self) -> &u32 {
        unsafe { &(*self.0).pid }
    }
}

// we only hash on pid, since we will change
// sz sometimes and we access the item by pid
impl Hash for Entry {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        unsafe { (*self.0).pid.hash(hasher) }
    }
}

struct Shard {
    dll: DoublyLinkedList,
    entries: FastSet8<Entry>,
    capacity: usize,
    size: usize,
}

impl Shard {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "shard capacity must be non-zero");

        Self {
            dll: DoublyLinkedList::default(),
            entries: FastSet8::default(),
            capacity,
            size: 0,
        }
    }

    /// `PageId`s in the shard list are indexes of the entries.
    fn accessed(&mut self, cache_access: CacheAccess) -> Vec<u32> {
        if let Some(entry) = self.entries.get(&cache_access.pid) {
            let old_sz_po2 = unsafe { (*entry.0).swap_sz(cache_access.sz) };
            let old_size = 1 << usize::from(old_sz_po2);

            self.size -= old_size;
            self.dll.promote(entry.0);
        } else {
            let ptr = self.dll.push_head(cache_access);
            self.entries.insert(Entry(ptr));
        };

        self.size += cache_access.size();

        let mut to_evict = vec![];

        while self.size > self.capacity {
            if self.dll.len() == 1 {
                // don't evict what we just added
                break;
            }

            let node = self.dll.pop_tail().unwrap();

            assert!(self.entries.remove(&node.pid));

            to_evict.push(node.pid);

            self.size -= node.size();

            // NB: node is stored in our entries map
            // via a raw pointer, which points to
            // the same allocation used in the DLL.
            // We have to be careful to free node
            // only after removing it from both
            // the DLL and our entries map.
            drop(node);
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

    let lru = Lru::new(2);
    for i in 0..1000 {
        let guard = pin();
        lru.accessed(i, 16, &guard);
    }
}

#[test]
fn lru_access_test() {
    use crate::pin;

    let ci = CacheAccess::new(6, 20667);
    assert_eq!(ci.size(), 32 * 1024);

    let lru = Lru::new(4096);

    let guard = pin();

    assert_eq!(lru.accessed(0, 20667, &guard), vec![]);
    assert_eq!(lru.accessed(2, 20667, &guard), vec![]);
    assert_eq!(lru.accessed(4, 20667, &guard), vec![]);
    assert_eq!(lru.accessed(6, 20667, &guard), vec![]);
    assert_eq!(lru.accessed(8, 20667, &guard), vec![0, 2, 4]);
    assert_eq!(lru.accessed(10, 20667, &guard), vec![]);
    assert_eq!(lru.accessed(12, 20667, &guard), vec![]);
    assert_eq!(lru.accessed(14, 20667, &guard), vec![]);
    assert_eq!(lru.accessed(16, 20667, &guard), vec![6, 8, 10, 12]);
    assert_eq!(lru.accessed(18, 20667, &guard), vec![]);
    assert_eq!(lru.accessed(20, 20667, &guard), vec![]);
    assert_eq!(lru.accessed(22, 20667, &guard), vec![]);
    assert_eq!(lru.accessed(24, 20667, &guard), vec![14, 16, 18, 20]);
}

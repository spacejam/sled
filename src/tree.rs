use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::hint;
use std::io;
use std::mem::{self, ManuallyDrop};
use std::ops;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use concurrent_map::Minimum;
use inline_array::InlineArray;
use parking_lot::{
    lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard},
    RawRwLock,
};

use crate::*;

#[derive(Clone)]
pub struct Tree<const LEAF_FANOUT: usize = 1024> {
    collection_id: CollectionId,
    cache: ObjectCache<LEAF_FANOUT>,
    index: Index<LEAF_FANOUT>,
    _shutdown_dropper: Arc<ShutdownDropper<LEAF_FANOUT>>,
}

impl<const LEAF_FANOUT: usize> Drop for Tree<LEAF_FANOUT> {
    fn drop(&mut self) {
        if self.cache.config.flush_every_ms.is_none() {
            if let Err(e) = self.flush() {
                log::error!("failed to flush Db on Drop: {e:?}");
            }
        } else {
            // otherwise, it is expected that the flusher thread will
            // flush while shutting down the final Db/Tree instance
        }
    }
}

impl<const LEAF_FANOUT: usize> fmt::Debug for Tree<LEAF_FANOUT> {
    fn fmt(&self, w: &mut fmt::Formatter<'_>) -> fmt::Result {
        let alternate = w.alternate();

        let mut debug_struct = w.debug_struct(&format!("Db<{}>", LEAF_FANOUT));

        if alternate {
            debug_struct
                .field("global_error", &self.check_error())
                .field(
                    "data",
                    &format!("{:?}", self.iter().collect::<Vec<_>>()),
                )
                .finish()
        } else {
            debug_struct.field("global_error", &self.check_error()).finish()
        }
    }
}

#[must_use]
struct LeafReadGuard<'a, const LEAF_FANOUT: usize = 1024> {
    leaf_read:
        ManuallyDrop<ArcRwLockReadGuard<RawRwLock, CacheBox<LEAF_FANOUT>>>,
    low_key: InlineArray,
    inner: &'a Tree<LEAF_FANOUT>,
    object_id: ObjectId,
    external_cache_access_and_eviction: bool,
}

impl<'a, const LEAF_FANOUT: usize> Drop for LeafReadGuard<'a, LEAF_FANOUT> {
    fn drop(&mut self) {
        let size = self.leaf_read.leaf.as_ref().unwrap().in_memory_size;
        // we must drop our mutex before calling mark_access_and_evict
        unsafe {
            ManuallyDrop::drop(&mut self.leaf_read);
        }
        if self.external_cache_access_and_eviction {
            return;
        }

        let current_epoch = self.inner.cache.current_flush_epoch();

        if let Err(e) = self.inner.cache.mark_access_and_evict(
            self.object_id,
            size,
            current_epoch,
        ) {
            self.inner.set_error(&e);
            log::error!(
                "io error while paging out dirty data: {:?} \
                for guard of leaf with low key {:?}",
                e,
                self.low_key
            );
        }
    }
}

struct LeafWriteGuard<'a, const LEAF_FANOUT: usize = 1024> {
    leaf_write:
        ManuallyDrop<ArcRwLockWriteGuard<RawRwLock, CacheBox<LEAF_FANOUT>>>,
    flush_epoch_guard: FlushEpochGuard<'a>,
    low_key: InlineArray,
    inner: &'a Tree<LEAF_FANOUT>,
    node: Object<LEAF_FANOUT>,
    external_cache_access_and_eviction: bool,
}

impl<'a, const LEAF_FANOUT: usize> LeafWriteGuard<'a, LEAF_FANOUT> {
    fn epoch(&self) -> FlushEpoch {
        self.flush_epoch_guard.epoch()
    }

    fn handle_cache_access_and_eviction_externally(
        mut self,
    ) -> (ObjectId, usize) {
        self.external_cache_access_and_eviction = true;
        (
            self.node.object_id,
            self.leaf_write.leaf.as_ref().unwrap().in_memory_size,
        )
    }
}

impl<'a, const LEAF_FANOUT: usize> Drop for LeafWriteGuard<'a, LEAF_FANOUT> {
    fn drop(&mut self) {
        let size = self.leaf_write.leaf.as_ref().unwrap().in_memory_size;

        // we must drop our mutex before calling mark_access_and_evict
        unsafe {
            ManuallyDrop::drop(&mut self.leaf_write);
        }
        if self.external_cache_access_and_eviction {
            return;
        }

        if let Err(e) = self.inner.cache.mark_access_and_evict(
            self.node.object_id,
            size,
            self.epoch(),
        ) {
            self.inner.set_error(&e);
            log::error!("io error while paging out dirty data: {:?}", e);
        }
    }
}

impl<const LEAF_FANOUT: usize> Tree<LEAF_FANOUT> {
    pub(crate) fn new(
        collection_id: CollectionId,
        cache: ObjectCache<LEAF_FANOUT>,
        index: Index<LEAF_FANOUT>,
        _shutdown_dropper: Arc<ShutdownDropper<LEAF_FANOUT>>,
    ) -> Tree<LEAF_FANOUT> {
        Tree { collection_id, cache, index, _shutdown_dropper }
    }

    // This is only pub for an extra assertion during testing.
    #[doc(hidden)]
    pub fn check_error(&self) -> io::Result<()> {
        self.cache.check_error()
    }

    fn set_error(&self, error: &io::Error) {
        self.cache.set_error(error)
    }

    pub fn storage_stats(&self) -> Stats {
        Stats { cache: self.cache.stats() }
    }

    /// Synchronously flushes all dirty IO buffers and calls
    /// fsync. If this succeeds, it is guaranteed that all
    /// previous writes will be recovered if the system
    /// crashes. Returns the number of bytes flushed during
    /// this call.
    ///
    /// Flushing can take quite a lot of time, and you should
    /// measure the performance impact of using it on
    /// realistic sustained workloads running on realistic
    /// hardware.
    ///
    /// This is called automatically on drop of the last open Db
    /// instance.
    pub fn flush(&self) -> io::Result<FlushStats> {
        self.cache.flush()
    }

    fn page_in(
        &self,
        key: &[u8],
        flush_epoch: FlushEpoch,
    ) -> io::Result<(
        InlineArray,
        ArcRwLockWriteGuard<RawRwLock, CacheBox<LEAF_FANOUT>>,
        Object<LEAF_FANOUT>,
    )> {
        let before_read_io = Instant::now();

        let mut read_loops: u64 = 0;
        loop {
            let _heap_pin = self.cache.heap_object_id_pin();

            let (low_key, node) = self.index.get_lte(key).unwrap();
            if node.collection_id != self.collection_id {
                log::trace!("retry due to mismatched collection id in page_in");

                hint::spin_loop();

                continue;
            }

            let mut write = node.inner.write_arc();
            if write.leaf.is_none() {
                self.cache
                    .read_stats
                    .cache_misses
                    .fetch_add(1, Ordering::Relaxed);

                let leaf_bytes =
                    if let Some(read_res) = self.cache.read(node.object_id) {
                        read_res?
                    } else {
                        // this particular object ID is not present
                        read_loops += 1;
                        // TODO change this assertion
                        debug_assert!(
                            read_loops < 10_000_000_000,
                            "search key: {:?} node key: {:?} object id: {:?}",
                            key,
                            low_key,
                            node.object_id
                        );

                        hint::spin_loop();

                        continue;
                    };

                let read_io_latency_us =
                    u64::try_from(before_read_io.elapsed().as_micros())
                        .unwrap();
                self.cache
                    .read_stats
                    .max_read_io_latency_us
                    .fetch_max(read_io_latency_us, Ordering::Relaxed);
                self.cache
                    .read_stats
                    .sum_read_io_latency_us
                    .fetch_add(read_io_latency_us, Ordering::Relaxed);

                let before_deserialization = Instant::now();

                let leaf: Box<Leaf<LEAF_FANOUT>> =
                    Leaf::deserialize(&leaf_bytes).unwrap();

                let deserialization_latency_us =
                    u64::try_from(before_deserialization.elapsed().as_micros())
                        .unwrap();
                self.cache
                    .read_stats
                    .max_deserialization_latency_us
                    .fetch_max(deserialization_latency_us, Ordering::Relaxed);
                self.cache
                    .read_stats
                    .sum_deserialization_latency_us
                    .fetch_add(deserialization_latency_us, Ordering::Relaxed);

                #[cfg(feature = "for-internal-testing-only")]
                {
                    self.cache.event_verifier.mark(
                        node.object_id,
                        FlushEpoch::MIN,
                        event_verifier::State::CleanPagedIn,
                        concat!(file!(), ':', line!(), ":page-in"),
                    );
                }

                write.leaf = Some(leaf);
            } else {
                self.cache
                    .read_stats
                    .cache_hits
                    .fetch_add(1, Ordering::Relaxed);
            }
            let leaf = write.leaf.as_mut().unwrap();

            if leaf.deleted.is_some() {
                log::trace!("retry due to deleted node in page_in");
                drop(write);

                hint::spin_loop();

                continue;
            }

            if &*leaf.lo > key {
                let size = leaf.in_memory_size;
                drop(write);
                log::trace!("key undershoot in page_in");
                self.cache.mark_access_and_evict(
                    node.object_id,
                    size,
                    flush_epoch,
                )?;

                hint::spin_loop();

                continue;
            }

            if let Some(ref hi) = leaf.hi {
                if &**hi <= key {
                    let size = leaf.in_memory_size;
                    drop(write);
                    log::trace!("key overshoot in page_in");
                    self.cache.mark_access_and_evict(
                        node.object_id,
                        size,
                        flush_epoch,
                    )?;

                    hint::spin_loop();

                    continue;
                }
            }
            return Ok((low_key, write, node));
        }
    }

    // NB: must never be called without having already added the empty leaf
    // operations to a normal flush epoch. This function acquires the lock
    // for the left sibling so that the empty leaf's hi key can be given
    // to the left sibling, but for this to happen atomically, the act of
    // moving left must "happen" in the same flush epoch. By "pushing" the
    // merge left potentially into a future flush epoch, any deletions that the
    // leaf had applied that may have been a part of a previous batch would also
    // be pushed into the future flush epoch, which would break the crash
    // atomicity of the batch if the updates were not flushed in the same epoch
    // as the rest of the batch. So, this is why we potentially separate the
    // flush of the left merge from the flush of the operations that caused
    // the leaf to empty in the first place.
    fn merge_leaf_into_left_sibling<'a>(
        &'a self,
        mut leaf_guard: LeafWriteGuard<'a, LEAF_FANOUT>,
    ) -> io::Result<()> {
        let mut predecessor_guard = self.predecessor_leaf_mut(&leaf_guard)?;

        assert!(predecessor_guard.epoch() >= leaf_guard.epoch());

        let merge_epoch = leaf_guard.epoch().max(predecessor_guard.epoch());

        let leaf = leaf_guard.leaf_write.leaf.as_mut().unwrap();
        let predecessor = predecessor_guard.leaf_write.leaf.as_mut().unwrap();

        assert!(leaf.deleted.is_none());
        assert!(leaf.data.is_empty());
        assert!(predecessor.deleted.is_none());
        assert_eq!(predecessor.hi.as_ref(), Some(&leaf.lo));

        log::trace!(
            "deleting empty node id {} with low key {:?} and high key {:?}",
            leaf_guard.node.object_id.0,
            leaf.lo,
            leaf.hi
        );

        predecessor.hi = leaf.hi.clone();
        predecessor.set_dirty_epoch(merge_epoch);

        leaf.deleted = Some(merge_epoch);

        leaf_guard
            .inner
            .cache
            .tree_leaves_merged
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.index.remove(&leaf_guard.low_key).unwrap();
        self.cache.object_id_index.remove(&leaf_guard.node.object_id).unwrap();

        // NB: these updates must "happen" atomically in the same flush epoch
        self.cache.install_dirty(
            merge_epoch,
            leaf_guard.node.object_id,
            Dirty::MergedAndDeleted {
                object_id: leaf_guard.node.object_id,
                collection_id: self.collection_id,
            },
        );

        #[cfg(feature = "for-internal-testing-only")]
        {
            self.cache.event_verifier.mark(
                leaf_guard.node.object_id,
                merge_epoch,
                event_verifier::State::Unallocated,
                concat!(file!(), ':', line!(), ":merged"),
            );
        }

        self.cache.install_dirty(
            merge_epoch,
            predecessor_guard.node.object_id,
            Dirty::NotYetSerialized {
                low_key: predecessor.lo.clone(),
                node: predecessor_guard.node.clone(),
                collection_id: self.collection_id,
            },
        );

        #[cfg(feature = "for-internal-testing-only")]
        {
            self.cache.event_verifier.mark(
                predecessor_guard.node.object_id,
                merge_epoch,
                event_verifier::State::Dirty,
                concat!(file!(), ':', line!(), ":merged-into"),
            );
        }

        let (p_object_id, p_sz) =
            predecessor_guard.handle_cache_access_and_eviction_externally();
        let (s_object_id, s_sz) =
            leaf_guard.handle_cache_access_and_eviction_externally();

        self.cache.mark_access_and_evict(p_object_id, p_sz, merge_epoch)?;

        // TODO maybe we don't need to do this, since we're removing it
        self.cache.mark_access_and_evict(s_object_id, s_sz, merge_epoch)?;

        Ok(())
    }

    fn predecessor_leaf_mut<'a>(
        &'a self,
        successor: &LeafWriteGuard<'a, LEAF_FANOUT>,
    ) -> io::Result<LeafWriteGuard<'a, LEAF_FANOUT>> {
        assert_ne!(successor.low_key, &*InlineArray::MIN);

        loop {
            let search_key = self
                .index
                .range::<InlineArray, _>(..&successor.low_key)
                .next_back()
                .unwrap()
                .0;

            assert!(search_key < successor.low_key);

            // TODO this can probably deadlock
            let node = self.leaf_for_key_mut(&search_key)?;

            let leaf = node.leaf_write.leaf.as_ref().unwrap();

            assert!(leaf.lo < successor.low_key);

            let leaf_hi = leaf.hi.as_ref().expect("we hold the successor mutex, so the predecessor should have a hi key");

            if leaf_hi > &successor.low_key {
                let still_in_index = self.index.get(&successor.low_key);
                panic!(
                    "somehow, predecessor high key of {:?} \
                    is greater than successor low key of {:?}. current index presence: {:?} \n \
                    predecessor: {:?} \n successor: {:?}",
                    leaf_hi, successor.low_key, still_in_index, leaf, successor.leaf_write.leaf.as_ref().unwrap(),
                );
            }
            if leaf_hi != &successor.low_key {
                continue;
            }
            return Ok(node);
        }
    }

    fn leaf_for_key<'a>(
        &'a self,
        key: &[u8],
    ) -> io::Result<LeafReadGuard<'a, LEAF_FANOUT>> {
        loop {
            let (low_key, node) = self.index.get_lte(key).unwrap();
            let mut read = node.inner.read_arc();

            if read.leaf.is_none() {
                drop(read);
                let (read_low_key, write, _node) =
                    self.page_in(key, self.cache.current_flush_epoch())?;
                assert!(&*read_low_key <= key);
                read = ArcRwLockWriteGuard::downgrade(write);
            } else {
                self.cache
                    .read_stats
                    .cache_hits
                    .fetch_add(1, Ordering::Relaxed);
            }

            let leaf_guard = LeafReadGuard {
                leaf_read: ManuallyDrop::new(read),
                inner: self,
                low_key,
                object_id: node.object_id,
                external_cache_access_and_eviction: false,
            };

            let leaf = leaf_guard.leaf_read.leaf.as_ref().unwrap();

            if leaf.deleted.is_some() {
                log::trace!("retry due to deleted node in leaf_for_key");
                drop(leaf_guard);
                hint::spin_loop();
                continue;
            }
            if &*leaf.lo > key {
                log::trace!("key undershoot in leaf_for_key");
                drop(leaf_guard);
                hint::spin_loop();
                continue;
            }
            if let Some(ref hi) = leaf.hi {
                if &**hi <= key {
                    log::trace!("key overshoot on leaf_for_key");
                    // cache maintenance occurs in Drop for LeafReadGuard
                    drop(leaf_guard);
                    hint::spin_loop();
                    continue;
                }
            }

            return Ok(leaf_guard);
        }
    }

    fn leaf_for_key_mut<'a>(
        &'a self,
        key: &[u8],
    ) -> io::Result<LeafWriteGuard<'a, LEAF_FANOUT>> {
        let reader_epoch = self.cache.current_flush_epoch();

        let (low_key, mut write, node) = self.page_in(key, reader_epoch)?;

        // by checking into an epoch after acquiring the node mutex, we
        // avoid inversions where progress may be observed to go backwards.
        let flush_epoch_guard = self.cache.check_into_flush_epoch();

        let leaf = write.leaf.as_mut().unwrap();

        // NB: these invariants should be enforced in page_in
        assert!(leaf.deleted.is_none());
        assert!(&*leaf.lo <= key);
        if let Some(ref hi) = leaf.hi {
            assert!(
                &**hi > key,
                "while retrieving the leaf for key {:?} \
                we pulled a leaf with hi key of {:?}",
                key,
                hi
            );
        }

        if let Some(old_dirty_epoch) = leaf.dirty_flush_epoch {
            if old_dirty_epoch != flush_epoch_guard.epoch() {
                log::trace!(
                    "cooperatively flushing {:?} with dirty {:?} after checking into {:?}",
                    node.object_id,
                    old_dirty_epoch,
                    flush_epoch_guard.epoch()
                );

                assert!(old_dirty_epoch < flush_epoch_guard.epoch());

                // cooperatively serialize and put into dirty
                leaf.max_unflushed_epoch = leaf.dirty_flush_epoch.take();
                leaf.page_out_on_flush.take();
                log::trace!(
                    "starting cooperatively serializing {:?} for {:?} because we want to use it in {:?}",
                    node.object_id, old_dirty_epoch, flush_epoch_guard.epoch(),
                );
                #[cfg(feature = "for-internal-testing-only")]
                {
                    self.cache.event_verifier.mark(
                        node.object_id,
                        old_dirty_epoch,
                        event_verifier::State::CooperativelySerialized,
                        concat!(
                            file!(),
                            ':',
                            line!(),
                            ":cooperative-serialize"
                        ),
                    );
                }

                // be extra-explicit about serialized bytes
                let leaf_ref: &Leaf<LEAF_FANOUT> = &*leaf;

                let serialized = leaf_ref
                    .serialize(self.cache.config.zstd_compression_level);

                log::trace!(
                    "D adding node {} to dirty {:?}",
                    node.object_id.0,
                    old_dirty_epoch
                );

                self.cache.install_dirty(
                    old_dirty_epoch,
                    node.object_id,
                    Dirty::CooperativelySerialized {
                        object_id: node.object_id,
                        collection_id: self.collection_id,
                        low_key: leaf.lo.clone(),
                        mutation_count: leaf.mutation_count,
                        data: Arc::new(serialized),
                    },
                );
                log::trace!(
                    "finished cooperatively serializing {:?} for {:?} because we want to use it in {:?}",
                    node.object_id, old_dirty_epoch, flush_epoch_guard.epoch(),
                );

                assert!(
                    old_dirty_epoch < flush_epoch_guard.epoch(),
                    "flush epochs somehow became unlinked"
                );
            }
        }

        Ok(LeafWriteGuard {
            flush_epoch_guard,
            leaf_write: ManuallyDrop::new(write),
            inner: self,
            low_key,
            node,
            external_cache_access_and_eviction: false,
        })
    }

    /// Retrieve a value from the `Tree` if it exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// db.insert(&[0], vec![0])?;
    /// assert_eq!(db.get(&[0]).unwrap(), Some(sled::InlineArray::from(vec![0])));
    /// assert!(db.get(&[1]).unwrap().is_none());
    /// # Ok(()) }
    /// ```
    pub fn get<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> io::Result<Option<InlineArray>> {
        self.check_error()?;

        let key_ref = key.as_ref();

        let leaf_guard = self.leaf_for_key(key_ref)?;

        let leaf = leaf_guard.leaf_read.leaf.as_ref().unwrap();

        if let Some(ref hi) = leaf.hi {
            assert!(&**hi > key_ref);
        }

        Ok(leaf.get(key_ref).cloned())
    }

    /// Insert a key to a new value, returning the last value if it
    /// was set.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// assert_eq!(db.insert(&[1, 2, 3], vec![0]).unwrap(), None);
    /// assert_eq!(db.insert(&[1, 2, 3], vec![1]).unwrap(), Some(sled::InlineArray::from(&[0])));
    /// # Ok(()) }
    /// ```
    #[doc(alias = "set")]
    #[doc(alias = "put")]
    pub fn insert<K, V>(
        &self,
        key: K,
        value: V,
    ) -> io::Result<Option<InlineArray>>
    where
        K: AsRef<[u8]>,
        V: Into<InlineArray>,
    {
        self.check_error()?;

        let key_ref = key.as_ref();

        let value_ivec = value.into();
        let mut leaf_guard = self.leaf_for_key_mut(key_ref)?;
        let new_epoch = leaf_guard.epoch();

        let leaf = leaf_guard.leaf_write.leaf.as_mut().unwrap();

        let ret = leaf.insert(key_ref.into(), value_ivec.clone());

        let old_size =
            ret.as_ref().map(|v| key_ref.len() + v.len()).unwrap_or(0);
        let new_size = key_ref.len() + value_ivec.len();

        if new_size > old_size {
            leaf.in_memory_size += new_size - old_size;
        } else {
            leaf.in_memory_size =
                leaf.in_memory_size.saturating_sub(old_size - new_size);
        }

        let split =
            leaf.split_if_full(new_epoch, &self.cache, self.collection_id);
        if split.is_some() || Some(value_ivec) != ret {
            leaf.mutation_count += 1;
            leaf.set_dirty_epoch(new_epoch);
            log::trace!(
                "F adding node {} to dirty {:?}",
                leaf_guard.node.object_id.0,
                new_epoch
            );

            self.cache.install_dirty(
                new_epoch,
                leaf_guard.node.object_id,
                Dirty::NotYetSerialized {
                    collection_id: self.collection_id,
                    node: leaf_guard.node.clone(),
                    low_key: leaf_guard.low_key.clone(),
                },
            );

            #[cfg(feature = "for-internal-testing-only")]
            {
                self.cache.event_verifier.mark(
                    leaf_guard.node.object_id,
                    new_epoch,
                    event_verifier::State::Dirty,
                    concat!(file!(), ':', line!(), ":insert"),
                );
            }
        }
        if let Some((split_key, rhs_node)) = split {
            assert_eq!(leaf.hi.as_ref().unwrap(), &split_key);
            log::trace!(
                "G adding new from split {:?} to dirty {:?}",
                rhs_node.object_id,
                new_epoch
            );

            assert_ne!(rhs_node.object_id, leaf_guard.node.object_id);
            assert!(!split_key.is_empty());

            let rhs_object_id = rhs_node.object_id;

            self.cache.install_dirty(
                new_epoch,
                rhs_object_id,
                Dirty::NotYetSerialized {
                    collection_id: self.collection_id,
                    node: rhs_node.clone(),
                    low_key: split_key.clone(),
                },
            );

            #[cfg(feature = "for-internal-testing-only")]
            {
                self.cache.event_verifier.mark(
                    rhs_object_id,
                    new_epoch,
                    event_verifier::State::Dirty,
                    concat!(file!(), ':', line!(), ":insert-split"),
                );
            }

            // NB only make the new node reachable via the index after
            // we marked it as dirty, as from this point on, any other
            // thread may cooperatively deserialize it and maybe conflict
            // with that previous NotYetSerialized marker.
            self.cache
                .object_id_index
                .insert(rhs_node.object_id, rhs_node.clone());
            let prev = self.index.insert(split_key, rhs_node);
            assert!(prev.is_none());
        }

        // this is for clarity, that leaf_guard is held while
        // inserting into dirty with its guarded epoch
        drop(leaf_guard);

        Ok(ret)
    }

    /// Delete a value, returning the old value if it existed.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// db.insert(&[1], vec![1]);
    /// assert_eq!(db.remove(&[1]).unwrap(), Some(sled::InlineArray::from(vec![1])));
    /// assert!(db.remove(&[1]).unwrap().is_none());
    /// # Ok(()) }
    /// ```
    #[doc(alias = "delete")]
    #[doc(alias = "del")]
    pub fn remove<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> io::Result<Option<InlineArray>> {
        self.check_error()?;

        let key_ref = key.as_ref();

        let mut leaf_guard = self.leaf_for_key_mut(key_ref)?;

        let new_epoch = leaf_guard.epoch();

        let leaf = leaf_guard.leaf_write.leaf.as_mut().unwrap();

        assert!(leaf.deleted.is_none());

        let ret = leaf.remove(key_ref);

        if ret.is_some() {
            leaf.mutation_count += 1;

            leaf.set_dirty_epoch(new_epoch);

            log::trace!(
                "H adding node {} to dirty {:?}",
                leaf_guard.node.object_id.0,
                new_epoch
            );

            self.cache.install_dirty(
                new_epoch,
                leaf_guard.node.object_id,
                Dirty::NotYetSerialized {
                    collection_id: self.collection_id,
                    low_key: leaf_guard.low_key.clone(),
                    node: leaf_guard.node.clone(),
                },
            );

            #[cfg(feature = "for-internal-testing-only")]
            {
                self.cache.event_verifier.mark(
                    leaf_guard.node.object_id,
                    new_epoch,
                    event_verifier::State::Dirty,
                    concat!(file!(), ':', line!(), ":remove"),
                );
            }

            if leaf.data.is_empty() && leaf_guard.low_key != InlineArray::MIN {
                self.merge_leaf_into_left_sibling(leaf_guard)?;
            }
        }

        Ok(ret)
    }
    /// Compare and swap. Capable of unique creation, conditional modification,
    /// or deletion. If old is `None`, this will only set the value if it
    /// doesn't exist yet. If new is `None`, will delete the value if old is
    /// correct. If both old and new are `Some`, will modify the value if
    /// old is correct.
    ///
    /// It returns `Ok(Ok(CompareAndSwapSuccess { new_value, previous_value }))` if operation finishes successfully.
    ///
    /// If it fails it returns:
    ///     - `Ok(Err(CompareAndSwapError{ current, proposed }))` if no IO
    ///       error was encountered but the operation
    ///       failed to specify the correct current value. `CompareAndSwapError` contains
    ///       current and proposed values.
    ///     - `Err(io::Error)` if there was a high-level IO problem that prevented
    ///       the operation from logically progressing. This is usually fatal and
    ///       will prevent future requests from functioning, and requires the
    ///       administrator to fix the system issue before restarting.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// // unique creation
    /// assert!(
    ///     db.compare_and_swap(&[1], None as Option<&[u8]>, Some(&[10])).unwrap().is_ok(),
    /// );
    ///
    /// // conditional modification
    /// assert!(
    ///     db.compare_and_swap(&[1], Some(&[10]), Some(&[20])).unwrap().is_ok(),
    /// );
    ///
    /// // failed conditional modification -- the current value is returned in
    /// // the error variant
    /// let operation = db.compare_and_swap(&[1], Some(&[30]), Some(&[40]));
    /// assert!(operation.is_ok()); // the operation succeeded
    /// let modification = operation.unwrap();
    /// assert!(modification.is_err());
    /// let actual_value = modification.unwrap_err();
    /// assert_eq!(actual_value.current.map(|ivec| ivec.to_vec()), Some(vec![20]));
    ///
    /// // conditional deletion
    /// assert!(
    ///     db.compare_and_swap(&[1], Some(&[20]), None as Option<&[u8]>).unwrap().is_ok(),
    /// );
    /// assert!(db.get(&[1]).unwrap().is_none());
    /// # Ok(()) }
    /// ```
    #[doc(alias = "cas")]
    #[doc(alias = "tas")]
    #[doc(alias = "test_and_swap")]
    #[doc(alias = "compare_and_set")]
    pub fn compare_and_swap<K, OV, NV>(
        &self,
        key: K,
        old: Option<OV>,
        new: Option<NV>,
    ) -> CompareAndSwapResult
    where
        K: AsRef<[u8]>,
        OV: AsRef<[u8]>,
        NV: Into<InlineArray>,
    {
        self.check_error()?;

        let key_ref = key.as_ref();

        let mut leaf_guard = self.leaf_for_key_mut(key_ref)?;
        let new_epoch = leaf_guard.epoch();

        let proposed: Option<InlineArray> = new.map(Into::into);

        let leaf = leaf_guard.leaf_write.leaf.as_mut().unwrap();

        let current = leaf.get(key_ref).cloned();

        let previous_matches = match (old, &current) {
            (None, None) => true,
            (Some(conditional), Some(current))
                if conditional.as_ref() == current.as_ref() =>
            {
                true
            }
            _ => false,
        };

        let ret = if previous_matches {
            if let Some(ref new_value) = proposed {
                leaf.insert(key_ref.into(), new_value.clone())
            } else {
                leaf.remove(key_ref)
            };

            Ok(CompareAndSwapSuccess {
                new_value: proposed,
                previous_value: current,
            })
        } else {
            Err(CompareAndSwapError { current, proposed })
        };

        let split =
            leaf.split_if_full(new_epoch, &self.cache, self.collection_id);
        let split_happened = split.is_some();
        if split_happened || ret.is_ok() {
            leaf.mutation_count += 1;

            leaf.set_dirty_epoch(new_epoch);
            log::trace!(
                "A adding node {} to dirty {:?}",
                leaf_guard.node.object_id.0,
                new_epoch
            );

            self.cache.install_dirty(
                new_epoch,
                leaf_guard.node.object_id,
                Dirty::NotYetSerialized {
                    collection_id: self.collection_id,
                    node: leaf_guard.node.clone(),
                    low_key: leaf_guard.low_key.clone(),
                },
            );

            #[cfg(feature = "for-internal-testing-only")]
            {
                self.cache.event_verifier.mark(
                    leaf_guard.node.object_id,
                    new_epoch,
                    event_verifier::State::Dirty,
                    concat!(file!(), ':', line!(), ":cas"),
                );
            }
        }
        if let Some((split_key, rhs_node)) = split {
            log::trace!(
                "B adding new from split {:?} to dirty {:?}",
                rhs_node.object_id,
                new_epoch
            );
            self.cache.install_dirty(
                new_epoch,
                rhs_node.object_id,
                Dirty::NotYetSerialized {
                    collection_id: self.collection_id,
                    node: rhs_node.clone(),
                    low_key: split_key.clone(),
                },
            );

            #[cfg(feature = "for-internal-testing-only")]
            {
                self.cache.event_verifier.mark(
                    rhs_node.object_id,
                    new_epoch,
                    event_verifier::State::Dirty,
                    concat!(file!(), ':', line!(), "cas-split"),
                );
            }

            // NB only make the new node reachable via the index after
            // we marked it as dirty, as from this point on, any other
            // thread may cooperatively deserialize it and maybe conflict
            // with that previous NotYetSerialized marker.
            self.cache
                .object_id_index
                .insert(rhs_node.object_id, rhs_node.clone());
            let prev = self.index.insert(split_key, rhs_node);
            assert!(prev.is_none());
        }

        if leaf.data.is_empty() && leaf_guard.low_key != InlineArray::MIN {
            assert!(!split_happened);
            self.merge_leaf_into_left_sibling(leaf_guard)?;
        }

        Ok(ret)
    }

    /// Fetch the value, apply a function to it and return the result.
    ///
    /// # Note
    ///
    /// This may call the function multiple times if the value has been
    /// changed from other threads in the meantime.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sled::{Config, InlineArray};
    ///
    /// let config = Config::tmp().unwrap();
    /// let db: sled::Db<1024> = config.open()?;
    ///
    /// fn u64_to_ivec(number: u64) -> InlineArray {
    ///     InlineArray::from(number.to_be_bytes().to_vec())
    /// }
    ///
    /// let zero = u64_to_ivec(0);
    /// let one = u64_to_ivec(1);
    /// let two = u64_to_ivec(2);
    /// let three = u64_to_ivec(3);
    ///
    /// fn increment(old: Option<&[u8]>) -> Option<Vec<u8>> {
    ///     let number = match old {
    ///         Some(bytes) => {
    ///             let array: [u8; 8] = bytes.try_into().unwrap();
    ///             let number = u64::from_be_bytes(array);
    ///             number + 1
    ///         }
    ///         None => 0,
    ///     };
    ///
    ///     Some(number.to_be_bytes().to_vec())
    /// }
    ///
    /// assert_eq!(db.update_and_fetch("counter", increment).unwrap(), Some(zero));
    /// assert_eq!(db.update_and_fetch("counter", increment).unwrap(), Some(one));
    /// assert_eq!(db.update_and_fetch("counter", increment).unwrap(), Some(two));
    /// assert_eq!(db.update_and_fetch("counter", increment).unwrap(), Some(three));
    /// # Ok(()) }
    /// ```
    pub fn update_and_fetch<K, V, F>(
        &self,
        key: K,
        mut f: F,
    ) -> io::Result<Option<InlineArray>>
    where
        K: AsRef<[u8]>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
        V: Into<InlineArray>,
    {
        let key_ref = key.as_ref();
        let mut current = self.get(key_ref)?;

        loop {
            let tmp = current.as_ref().map(AsRef::as_ref);
            let next = f(tmp).map(Into::into);
            match self.compare_and_swap::<_, _, InlineArray>(
                key_ref,
                tmp,
                next.clone(),
            )? {
                Ok(_) => return Ok(next),
                Err(CompareAndSwapError { current: cur, .. }) => {
                    current = cur;
                }
            }
        }
    }

    /// Fetch the value, apply a function to it and return the previous value.
    ///
    /// # Note
    ///
    /// This may call the function multiple times if the value has been
    /// changed from other threads in the meantime.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sled::{Config, InlineArray};
    ///
    /// let config = Config::tmp().unwrap();
    /// let db: sled::Db<1024> = config.open()?;
    ///
    /// fn u64_to_ivec(number: u64) -> InlineArray {
    ///     InlineArray::from(number.to_be_bytes().to_vec())
    /// }
    ///
    /// let zero = u64_to_ivec(0);
    /// let one = u64_to_ivec(1);
    /// let two = u64_to_ivec(2);
    ///
    /// fn increment(old: Option<&[u8]>) -> Option<Vec<u8>> {
    ///     let number = match old {
    ///         Some(bytes) => {
    ///             let array: [u8; 8] = bytes.try_into().unwrap();
    ///             let number = u64::from_be_bytes(array);
    ///             number + 1
    ///         }
    ///         None => 0,
    ///     };
    ///
    ///     Some(number.to_be_bytes().to_vec())
    /// }
    ///
    /// assert_eq!(db.fetch_and_update("counter", increment).unwrap(), None);
    /// assert_eq!(db.fetch_and_update("counter", increment).unwrap(), Some(zero));
    /// assert_eq!(db.fetch_and_update("counter", increment).unwrap(), Some(one));
    /// assert_eq!(db.fetch_and_update("counter", increment).unwrap(), Some(two));
    /// # Ok(()) }
    /// ```
    pub fn fetch_and_update<K, V, F>(
        &self,
        key: K,
        mut f: F,
    ) -> io::Result<Option<InlineArray>>
    where
        K: AsRef<[u8]>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
        V: Into<InlineArray>,
    {
        let key_ref = key.as_ref();
        let mut current = self.get(key_ref)?;

        loop {
            let tmp = current.as_ref().map(AsRef::as_ref);
            let next = f(tmp);
            match self.compare_and_swap(key_ref, tmp, next)? {
                Ok(_) => return Ok(current),
                Err(CompareAndSwapError { current: cur, .. }) => {
                    current = cur;
                }
            }
        }
    }

    pub fn iter(&self) -> Iter<LEAF_FANOUT> {
        Iter {
            prefetched: VecDeque::new(),
            prefetched_back: VecDeque::new(),
            next_fetch: Some(InlineArray::MIN),
            next_back_last_lo: None,
            next_calls: 0,
            next_back_calls: 0,
            inner: self.clone(),
            bounds: (Bound::Unbounded, Bound::Unbounded),
        }
    }

    pub fn range<K, R>(&self, range: R) -> Iter<LEAF_FANOUT>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        let start: Bound<InlineArray> =
            map_bound(range.start_bound(), |b| InlineArray::from(b.as_ref()));
        let end: Bound<InlineArray> =
            map_bound(range.end_bound(), |b| InlineArray::from(b.as_ref()));

        let next_fetch = Some(match &start {
            Bound::Included(b) | Bound::Excluded(b) => b.clone(),
            Bound::Unbounded => InlineArray::MIN,
        });

        Iter {
            prefetched: VecDeque::new(),
            prefetched_back: VecDeque::new(),
            next_fetch,
            next_back_last_lo: None,
            next_calls: 0,
            next_back_calls: 0,
            inner: self.clone(),
            bounds: (start, end),
        }
    }

    /// Create a new batched update that is applied
    /// atomically. Readers will atomically see all updates
    /// at an atomic instant, and if the database crashes,
    /// either 0% or 100% of the full batch will be recovered,
    /// but never a partial batch. If a `flush` operation succeeds
    /// after this, it is guaranteed that 100% of the batch will be
    /// visible, unless later concurrent updates changed the values
    /// before the flush.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let _ = std::fs::remove_dir_all("batch_doctest");
    /// # let db: sled::Db<1024> = sled::open("batch_doctest")?;
    /// db.insert("key_0", "val_0")?;
    ///
    /// let mut batch = sled::Batch::default();
    /// batch.insert("key_a", "val_a");
    /// batch.insert("key_b", "val_b");
    /// batch.insert("key_c", "val_c");
    /// batch.remove("key_0");
    ///
    /// db.apply_batch(batch)?;
    /// // key_0 no longer exists, and key_a, key_b, and key_c
    /// // now do exist.
    /// # let _ = std::fs::remove_dir_all("batch_doctest");
    /// # Ok(()) }
    /// ```
    pub fn apply_batch(&self, batch: Batch) -> io::Result<()> {
        // NB: we rely on lexicographic lock acquisition
        // by iterating over the batch's BTreeMap to avoid
        // deadlocks during 2PL
        let mut acquired_locks: BTreeMap<
            InlineArray,
            (
                ArcRwLockWriteGuard<RawRwLock, CacheBox<LEAF_FANOUT>>,
                Object<LEAF_FANOUT>,
            ),
        > = BTreeMap::new();

        // Phase 1: lock acquisition
        let mut last: Option<(
            InlineArray,
            ArcRwLockWriteGuard<RawRwLock, CacheBox<LEAF_FANOUT>>,
            Object<LEAF_FANOUT>,
        )> = None;

        for key in batch.writes.keys() {
            if let Some((_lo, w, _id)) = &last {
                let leaf = w.leaf.as_ref().unwrap();
                assert!(&leaf.lo <= key);
                if let Some(hi) = &leaf.hi {
                    if hi <= key {
                        let (lo, w, n) = last.take().unwrap();
                        acquired_locks.insert(lo, (w, n));
                    }
                }
            }
            if last.is_none() {
                // TODO evaluate whether this is correct, as page_in performs
                // cache maintenance internally if it over/undershoots due to
                // concurrent modifications.
                last =
                    Some(self.page_in(key, self.cache.current_flush_epoch())?);
            }
        }

        if let Some((lo, w, id)) = last.take() {
            acquired_locks.insert(lo, (w, id));
        }

        // NB: add the flush epoch at the end of the lock acquisition
        // process when all locks have been acquired, to avoid situations
        // where a leaf is already dirty with an epoch "from the future".
        let flush_epoch_guard = self.cache.check_into_flush_epoch();
        let new_epoch = flush_epoch_guard.epoch();

        // Flush any leaves that are dirty from a previous flush epoch
        // before performing operations.
        for (write, node) in acquired_locks.values_mut() {
            let leaf = write.leaf.as_mut().unwrap();
            if let Some(old_flush_epoch) = leaf.dirty_flush_epoch {
                if old_flush_epoch == new_epoch {
                    // no need to cooperatively flush
                    continue;
                }

                assert!(old_flush_epoch < new_epoch);

                log::trace!(
                    "cooperatively flushing {:?} with dirty {:?} after checking into {:?}",
                    node.object_id,
                    old_flush_epoch,
                    new_epoch
                );

                // cooperatively serialize and put into dirty
                let old_dirty_epoch = leaf.dirty_flush_epoch.take().unwrap();
                leaf.max_unflushed_epoch = Some(old_dirty_epoch);

                #[cfg(feature = "for-internal-testing-only")]
                {
                    self.cache.event_verifier.mark(
                        node.object_id,
                        old_dirty_epoch,
                        event_verifier::State::CleanPagedIn,
                        concat!(
                            file!(),
                            ':',
                            line!(),
                            "batch-cooperative-serialization"
                        ),
                    );
                }

                // be extra-explicit about serialized bytes
                let leaf_ref: &Leaf<LEAF_FANOUT> = &*leaf;

                let serialized = leaf_ref
                    .serialize(self.cache.config.zstd_compression_level);

                log::trace!(
                    "C adding node {} to dirty epoch {:?}",
                    node.object_id.0,
                    old_dirty_epoch
                );

                self.cache.install_dirty(
                    old_dirty_epoch,
                    node.object_id,
                    Dirty::CooperativelySerialized {
                        object_id: node.object_id,
                        collection_id: self.collection_id,
                        mutation_count: leaf_ref.mutation_count,
                        low_key: leaf.lo.clone(),
                        data: Arc::new(serialized),
                    },
                );

                #[cfg(feature = "for-internal-testing-only")]
                {
                    self.cache.event_verifier.mark(
                        node.object_id,
                        old_dirty_epoch,
                        event_verifier::State::Dirty,
                        concat!(file!(), ':', line!(), ":apply-batch"),
                    );
                }

                assert!(
                    old_flush_epoch < flush_epoch_guard.epoch(),
                    "flush epochs somehow became unlinked"
                );
            }
        }

        let mut splits: Vec<(InlineArray, Object<LEAF_FANOUT>)> = vec![];

        // Insert and split when full
        for (key, value_opt) in batch.writes {
            let range = ..=&key;
            let (_lo, (ref mut w, _id)) = acquired_locks
                .range_mut::<InlineArray, _>(range)
                .next_back()
                .unwrap();
            let leaf = w.leaf.as_mut().unwrap();

            assert!(leaf.lo <= key);
            if let Some(hi) = &leaf.hi {
                assert!(hi > &key);
            }

            if let Some(value) = value_opt {
                leaf.insert(key, value);

                if let Some((split_key, rhs_node)) = leaf.split_if_full(
                    new_epoch,
                    &self.cache,
                    self.collection_id,
                ) {
                    let write = rhs_node.inner.write_arc();
                    assert!(write.leaf.is_some());

                    splits.push((split_key.clone(), rhs_node.clone()));
                    acquired_locks.insert(split_key, (write, rhs_node));
                }
            } else {
                leaf.remove(&key);
            }
        }

        // Make splits globally visible
        for (split_key, rhs_node) in splits {
            self.cache
                .object_id_index
                .insert(rhs_node.object_id, rhs_node.clone());
            self.index.insert(split_key, rhs_node);
        }

        // Add all written leaves to dirty and prepare to mark cache accesses
        let mut cache_accesses = Vec::with_capacity(acquired_locks.len());
        for (low_key, (write, node)) in &mut acquired_locks {
            let leaf = write.leaf.as_mut().unwrap();
            leaf.set_dirty_epoch(new_epoch);
            leaf.mutation_count += 1;
            cache_accesses.push((node.object_id, leaf.in_memory_size));
            self.cache.install_dirty(
                new_epoch,
                node.object_id,
                Dirty::NotYetSerialized {
                    collection_id: self.collection_id,
                    node: node.clone(),
                    low_key: low_key.clone(),
                },
            );

            #[cfg(feature = "for-internal-testing-only")]
            {
                self.cache.event_verifier.mark(
                    node.object_id,
                    new_epoch,
                    event_verifier::State::Dirty,
                    concat!(file!(), ':', line!(), ":apply-batch"),
                );
            }
        }

        // Drop locks
        drop(acquired_locks);

        // Perform cache maintenance
        for (object_id, size) in cache_accesses {
            self.cache.mark_access_and_evict(object_id, size, new_epoch)?;
        }

        Ok(())
    }

    /// Returns `true` if the `Tree` contains a value for
    /// the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// db.insert(&[0], vec![0])?;
    /// assert!(db.contains_key(&[0])?);
    /// assert!(!db.contains_key(&[1])?);
    /// # Ok(()) }
    /// ```
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> io::Result<bool> {
        self.get(key).map(|v| v.is_some())
    }

    /// Retrieve the key and value before the provided key,
    /// if one exists.
    ///
    /// # Note
    /// The order follows the Ord implementation for `Vec<u8>`:
    ///
    /// `[] < [0] < [255] < [255, 0] < [255, 255] ...`
    ///
    /// To retain the ordering of numerical types use big endian reprensentation
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sled::InlineArray;
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// for i in 0..10 {
    ///     db.insert(&[i], vec![i])
    ///         .expect("should write successfully");
    /// }
    ///
    /// assert!(db.get_lt(&[]).unwrap().is_none());
    /// assert!(db.get_lt(&[0]).unwrap().is_none());
    /// assert_eq!(
    ///     db.get_lt(&[1]).unwrap(),
    ///     Some((InlineArray::from(&[0]), InlineArray::from(&[0])))
    /// );
    /// assert_eq!(
    ///     db.get_lt(&[9]).unwrap(),
    ///     Some((InlineArray::from(&[8]), InlineArray::from(&[8])))
    /// );
    /// assert_eq!(
    ///     db.get_lt(&[10]).unwrap(),
    ///     Some((InlineArray::from(&[9]), InlineArray::from(&[9])))
    /// );
    /// assert_eq!(
    ///     db.get_lt(&[255]).unwrap(),
    ///     Some((InlineArray::from(&[9]), InlineArray::from(&[9])))
    /// );
    /// # Ok(()) }
    /// ```
    pub fn get_lt<K>(
        &self,
        key: K,
    ) -> io::Result<Option<(InlineArray, InlineArray)>>
    where
        K: AsRef<[u8]>,
    {
        self.range(..key).next_back().transpose()
    }

    /// Retrieve the next key and value from the `Tree` after the
    /// provided key.
    ///
    /// # Note
    /// The order follows the Ord implementation for `Vec<u8>`:
    ///
    /// `[] < [0] < [255] < [255, 0] < [255, 255] ...`
    ///
    /// To retain the ordering of numerical types use big endian reprensentation
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sled::InlineArray;
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// for i in 0..10 {
    ///     db.insert(&[i], vec![i])?;
    /// }
    ///
    /// assert_eq!(
    ///     db.get_gt(&[]).unwrap(),
    ///     Some((InlineArray::from(&[0]), InlineArray::from(&[0])))
    /// );
    /// assert_eq!(
    ///     db.get_gt(&[0]).unwrap(),
    ///     Some((InlineArray::from(&[1]), InlineArray::from(&[1])))
    /// );
    /// assert_eq!(
    ///     db.get_gt(&[1]).unwrap(),
    ///     Some((InlineArray::from(&[2]), InlineArray::from(&[2])))
    /// );
    /// assert_eq!(
    ///     db.get_gt(&[8]).unwrap(),
    ///     Some((InlineArray::from(&[9]), InlineArray::from(&[9])))
    /// );
    /// assert!(db.get_gt(&[9]).unwrap().is_none());
    ///
    /// db.insert(500u16.to_be_bytes(), vec![10]);
    /// assert_eq!(
    ///     db.get_gt(&499u16.to_be_bytes()).unwrap(),
    ///     Some((InlineArray::from(&500u16.to_be_bytes()), InlineArray::from(&[10])))
    /// );
    /// # Ok(()) }
    /// ```
    pub fn get_gt<K>(
        &self,
        key: K,
    ) -> io::Result<Option<(InlineArray, InlineArray)>>
    where
        K: AsRef<[u8]>,
    {
        self.range((ops::Bound::Excluded(key), ops::Bound::Unbounded))
            .next()
            .transpose()
    }

    /// Create an iterator over tuples of keys and values
    /// where all keys start with the given prefix.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// use sled::InlineArray;
    /// db.insert(&[0, 0, 0], vec![0, 0, 0])?;
    /// db.insert(&[0, 0, 1], vec![0, 0, 1])?;
    /// db.insert(&[0, 0, 2], vec![0, 0, 2])?;
    /// db.insert(&[0, 0, 3], vec![0, 0, 3])?;
    /// db.insert(&[0, 1, 0], vec![0, 1, 0])?;
    /// db.insert(&[0, 1, 1], vec![0, 1, 1])?;
    ///
    /// let prefix: &[u8] = &[0, 0];
    /// let mut r = db.scan_prefix(prefix);
    /// assert_eq!(
    ///     r.next().unwrap().unwrap(),
    ///     (InlineArray::from(&[0, 0, 0]), InlineArray::from(&[0, 0, 0]))
    /// );
    /// assert_eq!(
    ///     r.next().unwrap().unwrap(),
    ///     (InlineArray::from(&[0, 0, 1]), InlineArray::from(&[0, 0, 1]))
    /// );
    /// assert_eq!(
    ///     r.next().unwrap().unwrap(),
    ///     (InlineArray::from(&[0, 0, 2]), InlineArray::from(&[0, 0, 2]))
    /// );
    /// assert_eq!(
    ///     r.next().unwrap().unwrap(),
    ///     (InlineArray::from(&[0, 0, 3]), InlineArray::from(&[0, 0, 3]))
    /// );
    /// assert!(r.next().is_none());
    /// # Ok(()) }
    /// ```
    pub fn scan_prefix<'a, P>(&'a self, prefix: P) -> Iter<LEAF_FANOUT>
    where
        P: AsRef<[u8]>,
    {
        let prefix_ref = prefix.as_ref();
        let mut upper = prefix_ref.to_vec();

        while let Some(last) = upper.pop() {
            if last < u8::MAX {
                upper.push(last + 1);
                return self.range(prefix_ref..&upper);
            }
        }

        self.range(prefix..)
    }

    /// Returns the first key and value in the `Tree`, or
    /// `None` if the `Tree` is empty.
    pub fn first(&self) -> io::Result<Option<(InlineArray, InlineArray)>> {
        self.iter().next().transpose()
    }

    /// Returns the last key and value in the `Tree`, or
    /// `None` if the `Tree` is empty.
    pub fn last(&self) -> io::Result<Option<(InlineArray, InlineArray)>> {
        self.iter().next_back().transpose()
    }

    /// Atomically removes the maximum item in the `Tree` instance.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// db.insert(&[0], vec![0])?;
    /// db.insert(&[1], vec![10])?;
    /// db.insert(&[2], vec![20])?;
    /// db.insert(&[3], vec![30])?;
    /// db.insert(&[4], vec![40])?;
    /// db.insert(&[5], vec![50])?;
    ///
    /// assert_eq!(&db.pop_last()?.unwrap().0, &[5]);
    /// assert_eq!(&db.pop_last()?.unwrap().0, &[4]);
    /// assert_eq!(&db.pop_last()?.unwrap().0, &[3]);
    /// assert_eq!(&db.pop_last()?.unwrap().0, &[2]);
    /// assert_eq!(&db.pop_last()?.unwrap().0, &[1]);
    /// assert_eq!(&db.pop_last()?.unwrap().0, &[0]);
    /// assert_eq!(db.pop_last()?, None);
    /// # Ok(()) }
    /// ```
    pub fn pop_last(&self) -> io::Result<Option<(InlineArray, InlineArray)>> {
        loop {
            if let Some(first_res) = self.iter().next_back() {
                let first = first_res?;
                if self
                    .compare_and_swap::<_, _, &[u8]>(
                        &first.0,
                        Some(&first.1),
                        None,
                    )?
                    .is_ok()
                {
                    log::trace!("pop_last removed item {:?}", first);
                    return Ok(Some(first));
                }
            // try again
            } else {
                log::trace!("pop_last removed nothing from empty tree");
                return Ok(None);
            }
        }
    }

    /// Pops the last kv pair in the provided range, or returns `Ok(None)` if nothing
    /// exists within that range.
    ///
    /// # Panics
    ///
    /// This will panic if the provided range's end_bound() == Bound::Excluded(K::MIN).
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    ///
    /// let data = vec![
    ///     (b"key 1", b"value 1"),
    ///     (b"key 2", b"value 2"),
    ///     (b"key 3", b"value 3")
    /// ];
    ///
    /// for (k, v) in data {
    ///     db.insert(k, v).unwrap();
    /// }
    ///
    /// let r1 = db.pop_last_in_range(b"key 1".as_ref()..=b"key 3").unwrap();
    /// assert_eq!(Some((b"key 3".into(), b"value 3".into())), r1);
    ///
    /// let r2 = db.pop_last_in_range(b"key 1".as_ref()..b"key 3").unwrap();
    /// assert_eq!(Some((b"key 2".into(), b"value 2".into())), r2);
    ///
    /// let r3 = db.pop_last_in_range(b"key 4".as_ref()..).unwrap();
    /// assert!(r3.is_none());
    ///
    /// let r4 = db.pop_last_in_range(b"key 2".as_ref()..=b"key 3").unwrap();
    /// assert!(r4.is_none());
    ///
    /// let r5 = db.pop_last_in_range(b"key 0".as_ref()..=b"key 3").unwrap();
    /// assert_eq!(Some((b"key 1".into(), b"value 1".into())), r5);
    ///
    /// let r6 = db.pop_last_in_range(b"key 0".as_ref()..=b"key 3").unwrap();
    /// assert!(r6.is_none());
    /// # Ok (()) }
    /// ```
    pub fn pop_last_in_range<K, R>(
        &self,
        range: R,
    ) -> io::Result<Option<(InlineArray, InlineArray)>>
    where
        K: AsRef<[u8]>,
        R: Clone + RangeBounds<K>,
    {
        loop {
            let mut r = self.range(range.clone());
            let (k, v) = if let Some(kv_res) = r.next_back() {
                kv_res?
            } else {
                return Ok(None);
            };
            if self
                .compare_and_swap(&k, Some(&v), None as Option<InlineArray>)?
                .is_ok()
            {
                return Ok(Some((k, v)));
            }
        }
    }

    /// Atomically removes the minimum item in the `Tree` instance.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// db.insert(&[0], vec![0])?;
    /// db.insert(&[1], vec![10])?;
    /// db.insert(&[2], vec![20])?;
    /// db.insert(&[3], vec![30])?;
    /// db.insert(&[4], vec![40])?;
    /// db.insert(&[5], vec![50])?;
    ///
    /// assert_eq!(&db.pop_first()?.unwrap().0, &[0]);
    /// assert_eq!(&db.pop_first()?.unwrap().0, &[1]);
    /// assert_eq!(&db.pop_first()?.unwrap().0, &[2]);
    /// assert_eq!(&db.pop_first()?.unwrap().0, &[3]);
    /// assert_eq!(&db.pop_first()?.unwrap().0, &[4]);
    /// assert_eq!(&db.pop_first()?.unwrap().0, &[5]);
    /// assert_eq!(db.pop_first()?, None);
    /// # Ok(()) }
    /// ```
    pub fn pop_first(&self) -> io::Result<Option<(InlineArray, InlineArray)>> {
        loop {
            if let Some(first_res) = self.iter().next() {
                let first = first_res?;
                if self
                    .compare_and_swap::<_, _, &[u8]>(
                        &first.0,
                        Some(&first.1),
                        None,
                    )?
                    .is_ok()
                {
                    log::trace!("pop_first removed item {:?}", first);
                    return Ok(Some(first));
                }
            // try again
            } else {
                log::trace!("pop_first removed nothing from empty tree");
                return Ok(None);
            }
        }
    }

    /// Pops the first kv pair in the provided range, or returns `Ok(None)` if nothing
    /// exists within that range.
    ///
    /// # Panics
    ///
    /// This will panic if the provided range's end_bound() == Bound::Excluded(K::MIN).
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    ///
    /// let data = vec![
    ///     (b"key 1", b"value 1"),
    ///     (b"key 2", b"value 2"),
    ///     (b"key 3", b"value 3")
    /// ];
    ///
    /// for (k, v) in data {
    ///     db.insert(k, v).unwrap();
    /// }
    ///
    /// let r1 = db.pop_first_in_range("key 1".as_ref()..="key 3").unwrap();
    /// assert_eq!(Some((b"key 1".into(), b"value 1".into())), r1);
    ///
    /// let r2 = db.pop_first_in_range("key 1".as_ref().."key 3").unwrap();
    /// assert_eq!(Some((b"key 2".into(), b"value 2".into())), r2);
    ///
    /// let r3_res: std::io::Result<Vec<_>> = db.range(b"key 4".as_ref()..).collect();
    /// let r3: Vec<_> = r3_res.unwrap();
    /// assert!(r3.is_empty());
    ///
    /// let r4 = db.pop_first_in_range("key 2".as_ref()..="key 3").unwrap();
    /// assert_eq!(Some((b"key 3".into(), b"value 3".into())), r4);
    /// # Ok (()) }
    /// ```
    pub fn pop_first_in_range<K, R>(
        &self,
        range: R,
    ) -> io::Result<Option<(InlineArray, InlineArray)>>
    where
        K: AsRef<[u8]>,
        R: Clone + RangeBounds<K>,
    {
        loop {
            let mut r = self.range(range.clone());
            let (k, v) = if let Some(kv_res) = r.next() {
                kv_res?
            } else {
                return Ok(None);
            };
            if self
                .compare_and_swap(&k, Some(&v), None as Option<InlineArray>)?
                .is_ok()
            {
                return Ok(Some((k, v)));
            }
        }
    }

    /// Returns the number of elements in this tree.
    ///
    /// Beware: performs a full O(n) scan under the hood.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// db.insert(b"a", vec![0]);
    /// db.insert(b"b", vec![1]);
    /// assert_eq!(db.len(), 2);
    /// # Ok(()) }
    /// ```
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Returns `true` if the `Tree` contains no elements.
    ///
    /// This is O(1), as we only need to see if an iterator
    /// returns anything for the first call to `next()`.
    pub fn is_empty(&self) -> io::Result<bool> {
        if let Some(res) = self.iter().next() {
            res?;
            Ok(false)
        } else {
            Ok(true)
        }
    }

    /// Clears the `Tree`, removing all values.
    ///
    /// Note that this is not atomic.
    ///
    /// Beware: performs a full O(n) scan under the hood.
    pub fn clear(&self) -> io::Result<()> {
        for k in self.iter().keys() {
            let key = k?;
            let _old = self.remove(key)?;
        }
        Ok(())
    }

    /// Returns the CRC32 of all keys and values
    /// in this Tree.
    ///
    /// This is O(N) and locks the underlying tree
    /// for the duration of the entire scan.
    pub fn checksum(&self) -> io::Result<u32> {
        let mut hasher = crc32fast::Hasher::new();
        for kv_res in self.iter() {
            let (k, v) = kv_res?;
            hasher.update(&k);
            hasher.update(&v);
        }
        Ok(hasher.finalize())
    }
}

#[allow(unused)]
pub struct Iter<const LEAF_FANOUT: usize> {
    inner: Tree<LEAF_FANOUT>,
    bounds: (Bound<InlineArray>, Bound<InlineArray>),
    next_calls: usize,
    next_back_calls: usize,
    next_fetch: Option<InlineArray>,
    next_back_last_lo: Option<InlineArray>,
    prefetched: VecDeque<(InlineArray, InlineArray)>,
    prefetched_back: VecDeque<(InlineArray, InlineArray)>,
}

impl<const LEAF_FANOUT: usize> Iterator for Iter<LEAF_FANOUT> {
    type Item = io::Result<(InlineArray, InlineArray)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_calls += 1;
        while self.prefetched.is_empty() {
            let search_key = if let Some(last) = &self.next_fetch {
                last.clone()
            } else {
                return None;
            };

            let node = match self.inner.leaf_for_key(&search_key) {
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };

            let leaf = node.leaf_read.leaf.as_ref().unwrap();

            if let Some(leaf_hi) = &leaf.hi {
                if leaf_hi <= &search_key {
                    // concurrent merge, retry
                    log::trace!("undershot in interator, retrying search");
                    continue;
                }
            }

            if leaf.lo > search_key {
                // concurrent successor split, retry
                log::trace!("overshot in interator, retrying search");
                continue;
            }

            for (k, v) in leaf.data.iter() {
                if self.bounds.contains(k) && &search_key <= k {
                    self.prefetched.push_back((k.clone(), v.clone()));
                }
            }

            self.next_fetch = leaf.hi.clone();
        }

        self.prefetched.pop_front().map(Ok)
    }
}

impl<const LEAF_FANOUT: usize> DoubleEndedIterator for Iter<LEAF_FANOUT> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.next_back_calls += 1;
        while self.prefetched_back.is_empty() {
            let search_key: InlineArray = if let Some(last) =
                &self.next_back_last_lo
            {
                if !self.bounds.contains(last) || last == &InlineArray::MIN {
                    return None;
                }
                self.inner
                    .index
                    .range::<InlineArray, _>(..last)
                    .next_back()
                    .unwrap()
                    .0
            } else {
                match &self.bounds.1 {
                    Bound::Included(k) => k.clone(),
                    Bound::Excluded(k) if k == &InlineArray::MIN => {
                        InlineArray::MIN
                    }
                    Bound::Excluded(k) => self.inner.index.get_lt(k).unwrap().0,
                    Bound::Unbounded => self.inner.index.last().unwrap().0,
                }
            };

            let node = match self.inner.leaf_for_key(&search_key) {
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };

            let leaf = node.leaf_read.leaf.as_ref().unwrap();

            if leaf.lo > search_key {
                // concurrent successor split, retry
                log::trace!("overshot in reverse interator, retrying search");
                continue;
            }

            // determine if we undershot our target due to concurrent modifications
            let undershot =
                match (&leaf.hi, &self.next_back_last_lo, &self.bounds.1) {
                    (Some(leaf_hi), Some(last_lo), _) => leaf_hi < last_lo,
                    (Some(_leaf_hi), None, Bound::Unbounded) => true,
                    (Some(leaf_hi), None, Bound::Included(bound_key)) => {
                        leaf_hi <= bound_key
                    }
                    (Some(leaf_hi), None, Bound::Excluded(bound_key)) => {
                        leaf_hi < bound_key
                    }
                    (None, _, _) => false,
                };

            if undershot {
                log::trace!(
                    "undershoot detected in reverse iterator with \
                    (leaf_hi, next_back_last_lo, self.bounds.1) being {:?}",
                    (&leaf.hi, &self.next_back_last_lo, &self.bounds.1)
                );
                continue;
            }

            for (k, v) in leaf.data.iter() {
                if self.bounds.contains(k) {
                    let beneath_last_lo =
                        if let Some(last_lo) = &self.next_back_last_lo {
                            k < last_lo
                        } else {
                            true
                        };
                    if beneath_last_lo {
                        self.prefetched_back.push_back((k.clone(), v.clone()));
                    }
                }
            }
            self.next_back_last_lo = Some(leaf.lo.clone());
        }

        self.prefetched_back.pop_back().map(Ok)
    }
}

impl<const LEAF_FANOUT: usize> Iter<LEAF_FANOUT> {
    pub fn keys(
        self,
    ) -> impl DoubleEndedIterator<Item = io::Result<InlineArray>> {
        self.into_iter().map(|kv_res| kv_res.map(|(k, _v)| k))
    }

    pub fn values(
        self,
    ) -> impl DoubleEndedIterator<Item = io::Result<InlineArray>> {
        self.into_iter().map(|kv_res| kv_res.map(|(_k, v)| v))
    }
}

impl<const LEAF_FANOUT: usize> IntoIterator for &Tree<LEAF_FANOUT> {
    type Item = io::Result<(InlineArray, InlineArray)>;
    type IntoIter = Iter<LEAF_FANOUT>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A batch of updates that will
/// be applied atomically to the
/// Tree.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use sled::{Batch, open};
///
/// # let _ = std::fs::remove_dir_all("batch_db_2");
/// let db: sled::Db<1024> = open("batch_db_2")?;
/// db.insert("key_0", "val_0")?;
///
/// let mut batch = Batch::default();
/// batch.insert("key_a", "val_a");
/// batch.insert("key_b", "val_b");
/// batch.insert("key_c", "val_c");
/// batch.remove("key_0");
///
/// db.apply_batch(batch)?;
/// // key_0 no longer exists, and key_a, key_b, and key_c
/// // now do exist.
/// # let _ = std::fs::remove_dir_all("batch_db_2");
/// # Ok(()) }
/// ```
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Batch {
    pub(crate) writes:
        std::collections::BTreeMap<InlineArray, Option<InlineArray>>,
}

impl Batch {
    /// Set a key to a new value
    pub fn insert<K, V>(&mut self, key: K, value: V)
    where
        K: Into<InlineArray>,
        V: Into<InlineArray>,
    {
        self.writes.insert(key.into(), Some(value.into()));
    }

    /// Remove a key
    pub fn remove<K>(&mut self, key: K)
    where
        K: Into<InlineArray>,
    {
        self.writes.insert(key.into(), None);
    }

    /// Get a value if it is present in the `Batch`.
    /// `Some(None)` means it's present as a deletion.
    pub fn get<K: AsRef<[u8]>>(&self, k: K) -> Option<Option<&InlineArray>> {
        let inner = self.writes.get(k.as_ref())?;
        Some(inner.as_ref())
    }
}

impl<const LEAF_FANOUT: usize> Leaf<LEAF_FANOUT> {
    pub fn serialize(&self, zstd_compression_level: i32) -> Vec<u8> {
        let mut ret = vec![];

        let mut zstd_enc =
            zstd::stream::Encoder::new(&mut ret, zstd_compression_level)
                .unwrap();

        bincode::serialize_into(&mut zstd_enc, self).unwrap();

        zstd_enc.finish().unwrap();

        ret
    }

    fn deserialize(buf: &[u8]) -> io::Result<Box<Leaf<LEAF_FANOUT>>> {
        let zstd_decoded = zstd::stream::decode_all(buf).unwrap();
        let mut leaf: Box<Leaf<LEAF_FANOUT>> =
            bincode::deserialize(&zstd_decoded).unwrap();

        // use decompressed buffer length as a cheap proxy for in-memory size for now
        leaf.in_memory_size = zstd_decoded.len();

        Ok(leaf)
    }

    fn set_in_memory_size(&mut self) {
        self.in_memory_size = mem::size_of::<Leaf<LEAF_FANOUT>>()
            + self.hi.as_ref().map(|h| h.len()).unwrap_or(0)
            + self.lo.len()
            + self.data.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>();
    }

    fn split_if_full(
        &mut self,
        new_epoch: FlushEpoch,
        allocator: &ObjectCache<LEAF_FANOUT>,
        collection_id: CollectionId,
    ) -> Option<(InlineArray, Object<LEAF_FANOUT>)> {
        if self.data.is_full() {
            // split
            let split_offset = if self.lo.is_empty() {
                // split left-most shard almost at the beginning for
                // optimizing downward-growing workloads
                1
            } else if self.hi.is_none() {
                // split right-most shard almost at the end for
                // optimizing upward-growing workloads
                self.data.len() - 2
            } else {
                self.data.len() / 2
            };

            let data = self.data.split_off(split_offset);

            let left_max = &self.data.last().unwrap().0;
            let right_min = &data.first().unwrap().0;

            // suffix truncation attempts to shrink the split key
            // so that shorter keys bubble up into the index
            let splitpoint_length = right_min
                .iter()
                .zip(left_max.iter())
                .take_while(|(a, b)| a == b)
                .count()
                + 1;

            let split_key = InlineArray::from(&right_min[..splitpoint_length]);

            let rhs_id = allocator.allocate_object_id(new_epoch);

            log::trace!(
                "split leaf {:?} at split key: {:?} into new {:?} at {:?}",
                self.lo,
                split_key,
                rhs_id,
                new_epoch,
            );

            let mut rhs = Leaf {
                dirty_flush_epoch: Some(new_epoch),
                hi: self.hi.clone(),
                lo: split_key.clone(),
                prefix_length: 0,
                in_memory_size: 0,
                data,
                mutation_count: 0,
                page_out_on_flush: None,
                deleted: None,
                max_unflushed_epoch: None,
            };
            rhs.set_in_memory_size();

            self.hi = Some(split_key.clone());
            self.set_in_memory_size();

            assert_eq!(self.hi.as_ref().unwrap(), &split_key);
            assert_eq!(rhs.lo, &split_key);

            let rhs_node = Object {
                object_id: rhs_id,
                collection_id,
                low_key: split_key.clone(),
                inner: Arc::new(RwLock::new(CacheBox {
                    leaf: Some(Box::new(rhs)).into(),
                    logged_index: BTreeMap::default(),
                })),
            };

            return Some((split_key, rhs_node));
        }

        None
    }
}

use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cache_advisor::CacheAdvisor;
use concurrent_map::{ConcurrentMap, Minimum};
use inline_array::InlineArray;
use parking_lot::RwLock;

use crate::*;

#[derive(Debug, Copy, Clone)]
pub struct CacheStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_hit_ratio: f32,
    pub max_read_io_latency_us: u64,
    pub sum_read_io_latency_us: u64,
    pub deserialization_latency_max_us: u64,
    pub deserialization_latency_sum_us: u64,
    pub heap: HeapStats,
    pub flush_max: FlushStats,
    pub flush_sum: FlushStats,
    pub compacted_heap_slots: u64,
    pub tree_leaves_merged: u64,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct FlushStats {
    pub pre_block_on_previous_flush: Duration,
    pub pre_block_on_current_quiescence: Duration,
    pub serialization_latency: Duration,
    pub compute_defrag_latency: Duration,
    pub storage_latency: Duration,
    pub post_write_eviction_latency: Duration,
    pub objects_flushed: u64,
    pub write_batch: WriteBatchStats,
}

impl FlushStats {
    pub fn sum(&self, other: &FlushStats) -> FlushStats {
        use std::ops::Add;

        FlushStats {
            pre_block_on_previous_flush: self
                .pre_block_on_previous_flush
                .add(other.pre_block_on_previous_flush),
            pre_block_on_current_quiescence: self
                .pre_block_on_current_quiescence
                .add(other.pre_block_on_current_quiescence),
            compute_defrag_latency: self
                .compute_defrag_latency
                .add(other.compute_defrag_latency),
            serialization_latency: self
                .serialization_latency
                .add(other.serialization_latency),
            storage_latency: self.storage_latency.add(other.storage_latency),
            post_write_eviction_latency: self
                .post_write_eviction_latency
                .add(other.post_write_eviction_latency),
            objects_flushed: self.objects_flushed.add(other.objects_flushed),
            write_batch: self.write_batch.sum(&other.write_batch),
        }
    }
    pub fn max(&self, other: &FlushStats) -> FlushStats {
        FlushStats {
            pre_block_on_previous_flush: self
                .pre_block_on_previous_flush
                .max(other.pre_block_on_previous_flush),
            pre_block_on_current_quiescence: self
                .pre_block_on_current_quiescence
                .max(other.pre_block_on_current_quiescence),
            compute_defrag_latency: self
                .compute_defrag_latency
                .max(other.compute_defrag_latency),
            serialization_latency: self
                .serialization_latency
                .max(other.serialization_latency),
            storage_latency: self.storage_latency.max(other.storage_latency),
            post_write_eviction_latency: self
                .post_write_eviction_latency
                .max(other.post_write_eviction_latency),
            objects_flushed: self.objects_flushed.max(other.objects_flushed),
            write_batch: self.write_batch.max(&other.write_batch),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Dirty<const LEAF_FANOUT: usize> {
    NotYetSerialized {
        low_key: InlineArray,
        node: Object<LEAF_FANOUT>,
        collection_id: CollectionId,
    },
    CooperativelySerialized {
        object_id: ObjectId,
        collection_id: CollectionId,
        low_key: InlineArray,
        data: Arc<Vec<u8>>,
        mutation_count: u64,
    },
    MergedAndDeleted {
        object_id: ObjectId,
        collection_id: CollectionId,
    },
}

impl<const LEAF_FANOUT: usize> Dirty<LEAF_FANOUT> {
    pub fn is_final_state(&self) -> bool {
        match self {
            Dirty::NotYetSerialized { .. } => false,
            Dirty::CooperativelySerialized { .. } => true,
            Dirty::MergedAndDeleted { .. } => true,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct FlushStatTracker {
    count: u64,
    sum: FlushStats,
    max: FlushStats,
}

#[derive(Debug, Default)]
pub(crate) struct ReadStatTracker {
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub max_read_io_latency_us: AtomicU64,
    pub sum_read_io_latency_us: AtomicU64,
    pub max_deserialization_latency_us: AtomicU64,
    pub sum_deserialization_latency_us: AtomicU64,
}

#[derive(Clone)]
pub(crate) struct ObjectCache<const LEAF_FANOUT: usize> {
    pub config: Config,
    global_error: Arc<AtomicPtr<(io::ErrorKind, String)>>,
    pub object_id_index: ConcurrentMap<
        ObjectId,
        Object<LEAF_FANOUT>,
        INDEX_FANOUT,
        EBR_LOCAL_GC_BUFFER_SIZE,
    >,
    heap: Heap,
    cache_advisor: RefCell<CacheAdvisor>,
    flush_epoch: FlushEpochTracker,
    dirty: ConcurrentMap<(FlushEpoch, ObjectId), Dirty<LEAF_FANOUT>, 4>,
    compacted_heap_slots: Arc<AtomicU64>,
    pub(super) tree_leaves_merged: Arc<AtomicU64>,
    #[cfg(feature = "for-internal-testing-only")]
    pub(super) event_verifier: Arc<crate::event_verifier::EventVerifier>,
    invariants: Arc<FlushInvariants>,
    flush_stats: Arc<RwLock<FlushStatTracker>>,
    pub(super) read_stats: Arc<ReadStatTracker>,
}

impl<const LEAF_FANOUT: usize> std::panic::RefUnwindSafe
    for ObjectCache<LEAF_FANOUT>
{
}

impl<const LEAF_FANOUT: usize> ObjectCache<LEAF_FANOUT> {
    /// Returns the recovered ObjectCache, the tree indexes, and a bool signifying whether the system
    /// was recovered or not
    pub(crate) fn recover(
        config: &Config,
    ) -> io::Result<(
        ObjectCache<LEAF_FANOUT>,
        HashMap<CollectionId, Index<LEAF_FANOUT>>,
        bool,
    )> {
        let HeapRecovery { heap, recovered_nodes, was_recovered } =
            recover(&config.path, LEAF_FANOUT, config)?;

        let (object_id_index, indices) = initialize(&recovered_nodes, &heap);

        // validate recovery
        for ObjectRecovery { object_id, collection_id, metadata } in
            recovered_nodes
        {
            let index = indices.get(&collection_id).unwrap();
            let node = index.get(&metadata).unwrap();
            assert_eq!(node.object_id, object_id);
        }

        if config.cache_capacity_bytes < 256 {
            log::debug!(
                "Db configured to have Config.cache_capacity_bytes \
                of under 256, so we will use the minimum of 256 bytes instead"
            );
        }

        if config.entry_cache_percent > 80 {
            log::debug!(
                "Db configured to have Config.entry_cache_percent\
                of over 80%, so we will clamp it to the maximum of 80% instead"
            );
        }

        let pc = ObjectCache {
            config: config.clone(),
            object_id_index,
            cache_advisor: RefCell::new(CacheAdvisor::new(
                config.cache_capacity_bytes.max(256),
                config.entry_cache_percent.min(80),
            )),
            global_error: heap.get_global_error_arc(),
            heap,
            dirty: Default::default(),
            flush_epoch: Default::default(),
            #[cfg(feature = "for-internal-testing-only")]
            event_verifier: Arc::default(),
            compacted_heap_slots: Arc::default(),
            tree_leaves_merged: Arc::default(),
            invariants: Arc::default(),
            flush_stats: Arc::default(),
            read_stats: Arc::default(),
        };

        Ok((pc, indices, was_recovered))
    }

    pub fn is_clean(&self) -> bool {
        self.dirty.is_empty()
    }

    pub fn read(&self, object_id: ObjectId) -> Option<io::Result<Vec<u8>>> {
        self.heap.read(object_id)
    }

    pub fn stats(&self) -> CacheStats {
        let flush_stats = { *self.flush_stats.read() };
        let cache_hits = self.read_stats.cache_hits.load(Ordering::Acquire);
        let cache_misses = self.read_stats.cache_misses.load(Ordering::Acquire);
        let cache_hit_ratio =
            cache_hits as f32 / (cache_hits + cache_misses).max(1) as f32;

        CacheStats {
            cache_hits,
            cache_misses,
            cache_hit_ratio,
            compacted_heap_slots: self
                .compacted_heap_slots
                .load(Ordering::Acquire),
            tree_leaves_merged: self.tree_leaves_merged.load(Ordering::Acquire),
            heap: self.heap.stats(),
            flush_max: flush_stats.max,
            flush_sum: flush_stats.sum,
            deserialization_latency_max_us: self
                .read_stats
                .max_deserialization_latency_us
                .load(Ordering::Acquire),
            deserialization_latency_sum_us: self
                .read_stats
                .sum_deserialization_latency_us
                .load(Ordering::Acquire),
            max_read_io_latency_us: self
                .read_stats
                .max_read_io_latency_us
                .load(Ordering::Acquire),
            sum_read_io_latency_us: self
                .read_stats
                .sum_read_io_latency_us
                .load(Ordering::Acquire),
        }
    }

    pub fn check_error(&self) -> io::Result<()> {
        let err_ptr: *const (io::ErrorKind, String) =
            self.global_error.load(Ordering::Acquire);

        if err_ptr.is_null() {
            Ok(())
        } else {
            let deref: &(io::ErrorKind, String) = unsafe { &*err_ptr };
            Err(io::Error::new(deref.0, deref.1.clone()))
        }
    }

    pub fn set_error(&self, error: &io::Error) {
        let kind = error.kind();
        let reason = error.to_string();

        let boxed = Box::new((kind, reason));
        let ptr = Box::into_raw(boxed);

        if self
            .global_error
            .compare_exchange(
                std::ptr::null_mut(),
                ptr,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            // global fatal error already installed, drop this one
            unsafe {
                drop(Box::from_raw(ptr));
            }
        }
    }

    pub fn allocate_node(
        &self,
        collection_id: CollectionId,
        low_key: InlineArray,
        flush_epoch: FlushEpoch,
    ) -> Object<LEAF_FANOUT> {
        let object_id = self.allocate_object_id(flush_epoch);

        let node = Object {
            object_id,
            collection_id,
            low_key,
            inner: Arc::new(RwLock::new(CacheBox {
                leaf: Some(Box::default()).into(),
                logged_index: BTreeMap::default(),
            })),
        };

        self.object_id_index.insert(object_id, node.clone());

        node
    }

    pub fn allocate_object_id(&self, flush_epoch: FlushEpoch) -> ObjectId {
        let object_id = self.heap.allocate_object_id();

        #[cfg(feature = "for-internal-testing-only")]
        {
            self.event_verifier.mark(
                object_id,
                flush_epoch,
                event_verifier::State::CleanPagedIn,
                concat!(file!(), ':', line!(), ":allocated"),
            );
        }

        object_id
    }

    pub fn current_flush_epoch(&self) -> FlushEpoch {
        self.flush_epoch.current_flush_epoch()
    }
    pub fn check_into_flush_epoch(&self) -> FlushEpochGuard {
        self.flush_epoch.check_in()
    }

    pub fn install_dirty(
        &self,
        flush_epoch: FlushEpoch,
        object_id: ObjectId,
        dirty: Dirty<LEAF_FANOUT>,
    ) {
        // dirty can transition from:
        // None -> NotYetSerialized
        // None -> MergedAndDeleted
        // None -> CooperativelySerialized
        //
        // NotYetSerialized -> MergedAndDeleted
        // NotYetSerialized -> CooperativelySerialized
        //
        // if the new Dirty is final, we must assert that
        // we are transitioning from None or NotYetSerialized.
        //
        // if the new Dirty is not final, we must assert
        // that the old value is also not final.

        let last_dirty_opt = self.dirty.insert((flush_epoch, object_id), dirty);

        if let Some(last_dirty) = last_dirty_opt {
            assert!(
                !last_dirty.is_final_state(),
                "tried to install another Dirty marker for a node that is already
                finalized for this flush epoch. \nflush_epoch: {:?}\nlast: {:?}",
                flush_epoch, last_dirty,
            );
        }
    }

    // NB: must not be called while holding a leaf lock - which also means
    // that no two LeafGuards can be held concurrently in the same scope due to
    // this being called in the destructor.
    pub fn mark_access_and_evict(
        &self,
        object_id: ObjectId,
        size: usize,
        flush_epoch: FlushEpoch,
    ) -> io::Result<()> {
        let mut ca = self.cache_advisor.borrow_mut();
        let to_evict = ca.accessed_reuse_buffer(*object_id, size);
        for (node_to_evict, _rough_size) in to_evict {
            let object_id =
                if let Some(object_id) = ObjectId::new(*node_to_evict) {
                    object_id
                } else {
                    unreachable!("object ID must never have been 0");
                };

            let node = if let Some(n) = self.object_id_index.get(&object_id) {
                if *n.object_id != *node_to_evict {
                    log::debug!("during cache eviction, node to evict did not match current occupant for {:?}", node_to_evict);
                    continue;
                }
                n
            } else {
                log::debug!("during cache eviction, unable to find node to evict for {:?}", node_to_evict);
                continue;
            };

            let mut write = node.inner.write();
            if write.leaf.is_none() {
                // already paged out
                continue;
            }
            let leaf: &mut Leaf<LEAF_FANOUT> = write.leaf.as_mut().unwrap();

            if let Some(dirty_epoch) = leaf.dirty_flush_epoch {
                // We can't page out this leaf until it has been
                // flushed, because its changes are not yet durable.
                leaf.page_out_on_flush =
                    leaf.page_out_on_flush.max(Some(dirty_epoch));
            } else if let Some(max_unflushed_epoch) = leaf.max_unflushed_epoch {
                leaf.page_out_on_flush =
                    leaf.page_out_on_flush.max(Some(max_unflushed_epoch));
            } else {
                #[cfg(feature = "for-internal-testing-only")]
                {
                    self.event_verifier.mark(
                        node.object_id,
                        flush_epoch,
                        event_verifier::State::PagedOut,
                        concat!(file!(), ':', line!(), ":page-out"),
                    );
                }
                write.leaf = None;
            }
        }

        Ok(())
    }

    pub fn heap_object_id_pin(&self) -> ebr::Guard<'_, DeferredFree, 16, 16> {
        self.heap.heap_object_id_pin()
    }

    pub fn flush(&self) -> io::Result<FlushStats> {
        let mut write_batch = vec![];

        log::trace!("advancing epoch");
        let (
            previous_flush_complete_notifier,
            this_vacant_notifier,
            forward_flush_notifier,
        ) = self.flush_epoch.roll_epoch_forward();

        let before_previous_block = Instant::now();

        log::trace!(
            "waiting for previous flush of {:?} to complete",
            previous_flush_complete_notifier.epoch()
        );
        let previous_epoch =
            previous_flush_complete_notifier.wait_for_complete();

        let pre_block_on_previous_flush = before_previous_block.elapsed();

        let before_current_quiescence = Instant::now();

        log::trace!(
            "waiting for our epoch {:?} to become vacant",
            this_vacant_notifier.epoch()
        );

        assert_eq!(previous_epoch.increment(), this_vacant_notifier.epoch());

        let flush_through_epoch: FlushEpoch =
            this_vacant_notifier.wait_for_complete();

        let pre_block_on_current_quiescence =
            before_current_quiescence.elapsed();

        self.invariants.mark_flushing_epoch(flush_through_epoch);

        let mut objects_to_defrag = self.heap.objects_to_defrag();

        let flush_boundary = (flush_through_epoch.increment(), ObjectId::MIN);

        let mut evict_after_flush = vec![];

        let before_serialization = Instant::now();

        for ((dirty_epoch, dirty_object_id), dirty_value_initial_read) in
            self.dirty.range(..flush_boundary)
        {
            objects_to_defrag.remove(&dirty_object_id);

            let dirty_value = self
                .dirty
                .remove(&(dirty_epoch, dirty_object_id))
                .expect("violation of flush responsibility");

            if let Dirty::NotYetSerialized { .. } = &dirty_value {
                assert_eq!(dirty_value_initial_read, dirty_value);
            }

            // drop is necessary to increase chance of Arc strong count reaching 1
            // while taking ownership of the value
            drop(dirty_value_initial_read);

            assert_eq!(dirty_epoch, flush_through_epoch);

            match dirty_value {
                Dirty::MergedAndDeleted { object_id, collection_id } => {
                    assert_eq!(object_id, dirty_object_id);

                    log::trace!(
                        "MergedAndDeleted for {:?}, adding None to write_batch",
                        object_id
                    );
                    write_batch.push(Update::Free { object_id, collection_id });

                    #[cfg(feature = "for-internal-testing-only")]
                    {
                        self.event_verifier.mark(
                            object_id,
                            dirty_epoch,
                            event_verifier::State::AddedToWriteBatch,
                            concat!(
                                file!(),
                                ':',
                                line!(),
                                ":flush-merged-and-deleted"
                            ),
                        );
                    }
                }
                Dirty::CooperativelySerialized {
                    object_id: _,
                    collection_id,
                    low_key,
                    mutation_count: _,
                    mut data,
                } => {
                    Arc::make_mut(&mut data);
                    let data = Arc::into_inner(data).unwrap();
                    write_batch.push(Update::Store {
                        object_id: dirty_object_id,
                        collection_id,
                        metadata: low_key,
                        data,
                    });

                    #[cfg(feature = "for-internal-testing-only")]
                    {
                        self.event_verifier.mark(
                            dirty_object_id,
                            dirty_epoch,
                            event_verifier::State::AddedToWriteBatch,
                            concat!(
                                file!(),
                                ':',
                                line!(),
                                ":flush-cooperative"
                            ),
                        );
                    }
                }
                Dirty::NotYetSerialized { low_key, collection_id, node } => {
                    assert_eq!(dirty_object_id, node.object_id, "mismatched node ID for NotYetSerialized with low key {:?}", low_key);
                    let mut lock = node.inner.write();

                    let leaf_ref: &mut Leaf<LEAF_FANOUT> = if let Some(
                        lock_ref,
                    ) =
                        lock.leaf.as_mut()
                    {
                        lock_ref
                    } else {
                        #[cfg(feature = "for-internal-testing-only")]
                        self.event_verifier
                            .print_debug_history_for_object(dirty_object_id);

                        panic!("failed to get lock for node that was NotYetSerialized, low key {:?} id {:?}", low_key, node.object_id);
                    };

                    let data = if leaf_ref.dirty_flush_epoch
                        == Some(flush_through_epoch)
                    {
                        if let Some(deleted_at) = leaf_ref.deleted {
                            #[cfg(feature = "for-internal-testing-only")]
                            if deleted_at <= flush_through_epoch {
                                println!(
                                    "{dirty_object_id:?} deleted at {deleted_at:?} \
                                    but we are flushing at {flush_through_epoch:?}"
                                );
                                self.event_verifier
                                    .print_debug_history_for_object(
                                        dirty_object_id,
                                    );
                            }
                            assert!(deleted_at > flush_through_epoch);
                        }

                        leaf_ref.max_unflushed_epoch =
                            leaf_ref.dirty_flush_epoch.take();

                        #[cfg(feature = "for-internal-testing-only")]
                        {
                            self.event_verifier.mark(
                                dirty_object_id,
                                dirty_epoch,
                                event_verifier::State::AddedToWriteBatch,
                                concat!(
                                    file!(),
                                    ':',
                                    line!(),
                                    ":flush-serialize"
                                ),
                            );
                        }

                        leaf_ref.serialize(self.config.zstd_compression_level)
                    } else {
                        // Here we expect that there was a benign data race and that another thread
                        // mutated the leaf after encountering it being dirty for our epoch, after
                        // storing a CooperativelySerialized in the dirty map.
                        let dirty_value_2_opt =
                            self.dirty.remove(&(dirty_epoch, dirty_object_id));

                        if let Some(Dirty::CooperativelySerialized {
                            low_key: low_key_2,
                            mutation_count: _,
                            mut data,
                            collection_id: ci2,
                            object_id: ni2,
                        }) = dirty_value_2_opt
                        {
                            assert_eq!(node.object_id, ni2);
                            assert_eq!(node.object_id, dirty_object_id);
                            assert_eq!(low_key, low_key_2);
                            assert_eq!(collection_id, ci2);
                            Arc::make_mut(&mut data);

                            #[cfg(feature = "for-internal-testing-only")]
                            {
                                self.event_verifier.mark(
                                    dirty_object_id,
                                    dirty_epoch,
                                    event_verifier::State::AddedToWriteBatch,
                                    concat!(
                                        file!(),
                                        ':',
                                        line!(),
                                        ":flush-laggy-cooperative"
                                    ),
                                );
                            }

                            Arc::into_inner(data).unwrap()
                        } else {
                            log::error!(
                                "violation of flush responsibility for second read \
                                of expected cooperative serialization. leaf in question's \
                                dirty_flush_epoch is {:?}, our expected key was {:?}. node.deleted: {:?}",
                                leaf_ref.dirty_flush_epoch,
                                (dirty_epoch, dirty_object_id),
                                leaf_ref.deleted,
                            );
                            #[cfg(feature = "for-internal-testing-only")]
                            self.event_verifier.print_debug_history_for_object(
                                dirty_object_id,
                            );

                            unreachable!("a leaf was expected to be cooperatively serialized but it was not available");
                        }
                    };

                    write_batch.push(Update::Store {
                        object_id: dirty_object_id,
                        collection_id: collection_id,
                        metadata: low_key,
                        data,
                    });

                    if leaf_ref.page_out_on_flush == Some(flush_through_epoch) {
                        // page_out_on_flush is set to false
                        // on page-in due to serde(skip)
                        evict_after_flush.push(node.clone());
                    }
                }
            }
        }

        if !objects_to_defrag.is_empty() {
            log::debug!(
                "objects to defrag (after flush loop): {}",
                objects_to_defrag.len()
            );
            self.compacted_heap_slots
                .fetch_add(objects_to_defrag.len() as u64, Ordering::Relaxed);
        }

        let before_compute_defrag = Instant::now();

        for fragmented_object_id in objects_to_defrag {
            let object_opt = self.object_id_index.get(&fragmented_object_id);

            let object = if let Some(object) = object_opt {
                object
            } else {
                log::debug!("defragmenting object not found in object_id_index: {fragmented_object_id:?}");
                continue;
            };

            if let Some(ref inner) = object.inner.read().leaf {
                if let Some(dirty) = inner.dirty_flush_epoch {
                    assert!(dirty > flush_through_epoch);
                    // This object will be rewritten anyway when its dirty epoch gets flushed
                    continue;
                }
            }

            let data = match self.read(fragmented_object_id) {
                Some(Ok(data)) => data,
                Some(Err(e)) => {
                    log::error!("failed to read object during GC: {e:?}");
                    continue;
                }
                None => {
                    log::error!(
                        "failed to read object during GC: object not found"
                    );
                    continue;
                }
            };

            write_batch.push(Update::Store {
                object_id: fragmented_object_id,
                collection_id: object.collection_id,
                metadata: object.low_key,
                data,
            });
        }

        let compute_defrag_latency = before_compute_defrag.elapsed();

        let serialization_latency = before_serialization.elapsed();

        let before_storage = Instant::now();

        let objects_flushed = write_batch.len() as u64;

        #[cfg(feature = "for-internal-testing-only")]
        let write_batch_object_ids: Vec<ObjectId> =
            write_batch.iter().map(Update::object_id).collect();

        let write_batch_stats = if objects_flushed > 0 {
            let write_batch_stats = self.heap.write_batch(write_batch)?;
            log::trace!(
                "marking {flush_through_epoch:?} as flushed - \
                {objects_flushed} objects written, {write_batch_stats:?}",
            );
            write_batch_stats
        } else {
            WriteBatchStats::default()
        };

        let storage_latency = before_storage.elapsed();

        #[cfg(feature = "for-internal-testing-only")]
        {
            for update_object_id in write_batch_object_ids {
                self.event_verifier.mark(
                    update_object_id,
                    flush_through_epoch,
                    event_verifier::State::Flushed,
                    concat!(file!(), ':', line!(), ":flush-finished"),
                );
            }
        }

        log::trace!(
            "marking the forward flush notifier that {:?} is flushed",
            flush_through_epoch
        );

        self.invariants.mark_flushed_epoch(flush_through_epoch);

        forward_flush_notifier.mark_complete();

        let before_eviction = Instant::now();

        for node_to_evict in evict_after_flush {
            // NB: since we dropped this leaf and lock after we marked its
            // node in evict_after_flush, it's possible that it may have
            // been written to afterwards.
            let mut lock = node_to_evict.inner.write();
            let leaf = lock.leaf.as_mut().unwrap();

            if let Some(dirty_epoch) = leaf.dirty_flush_epoch {
                if dirty_epoch != flush_through_epoch {
                    continue;
                }
            } else {
                continue;
            }

            #[cfg(feature = "for-internal-testing-only")]
            {
                self.event_verifier.mark(
                    node_to_evict.object_id,
                    flush_through_epoch,
                    event_verifier::State::PagedOut,
                    concat!(file!(), ':', line!(), ":page-out-after-flush"),
                );
            }

            lock.leaf = None;
        }

        let post_write_eviction_latency = before_eviction.elapsed();

        // kick forward the low level epoch-based reclamation systems
        // because this operation can cause a lot of garbage to build
        // up, and this speeds up its reclamation.
        self.flush_epoch.manually_advance_epoch();
        self.heap.manually_advance_epoch();

        let ret = FlushStats {
            pre_block_on_current_quiescence,
            pre_block_on_previous_flush,
            serialization_latency,
            storage_latency,
            post_write_eviction_latency,
            objects_flushed,
            write_batch: write_batch_stats,
            compute_defrag_latency,
        };

        let mut flush_stats = self.flush_stats.write();
        flush_stats.count += 1;
        flush_stats.max = flush_stats.max.max(&ret);
        flush_stats.sum = flush_stats.sum.sum(&ret);

        Ok(ret)
    }
}

fn initialize<const LEAF_FANOUT: usize>(
    recovered_nodes: &[ObjectRecovery],
    heap: &Heap,
) -> (
    ConcurrentMap<
        ObjectId,
        Object<LEAF_FANOUT>,
        INDEX_FANOUT,
        EBR_LOCAL_GC_BUFFER_SIZE,
    >,
    HashMap<CollectionId, Index<LEAF_FANOUT>>,
) {
    let mut trees: HashMap<CollectionId, Index<LEAF_FANOUT>> = HashMap::new();

    let object_id_index: ConcurrentMap<
        ObjectId,
        Object<LEAF_FANOUT>,
        INDEX_FANOUT,
        EBR_LOCAL_GC_BUFFER_SIZE,
    > = ConcurrentMap::default();

    for ObjectRecovery { object_id, collection_id, metadata } in recovered_nodes
    {
        let node = Object {
            object_id: *object_id,
            collection_id: *collection_id,
            low_key: metadata.clone(),
            inner: Arc::new(RwLock::new(CacheBox {
                leaf: None.into(),
                logged_index: BTreeMap::default(),
            })),
        };

        assert!(object_id_index.insert(*object_id, node.clone()).is_none());

        let tree = trees.entry(*collection_id).or_default();

        assert!(tree.insert(metadata.clone(), node).is_none());
    }

    // initialize default collections if not recovered
    for collection_id in [NAME_MAPPING_COLLECTION_ID, DEFAULT_COLLECTION_ID] {
        let tree = trees.entry(collection_id).or_default();

        if tree.is_empty() {
            let object_id = heap.allocate_object_id();

            let initial_low_key = InlineArray::default();

            let empty_node = Object {
                object_id,
                collection_id,
                low_key: initial_low_key.clone(),
                inner: Arc::new(RwLock::new(CacheBox {
                    leaf: Some(Box::default()).into(),
                    logged_index: BTreeMap::default(),
                })),
            };

            assert!(object_id_index
                .insert(object_id, empty_node.clone())
                .is_none());

            assert!(tree.insert(initial_low_key, empty_node).is_none());
        }
    }

    for (_cid, tree) in &trees {
        assert!(tree.contains_key(&InlineArray::MIN));
    }

    (object_id_index, trees)
}

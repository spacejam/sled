use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

use cache_advisor::CacheAdvisor;
use concurrent_map::{ConcurrentMap, Minimum};
use inline_array::InlineArray;

use crate::*;

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
            recover(&config.path, LEAF_FANOUT)?;

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
        };

        Ok((pc, indices, was_recovered))
    }

    pub fn is_clean(&self) -> bool {
        self.dirty.is_empty()
    }

    pub fn read(&self, object_id: u64) -> io::Result<Vec<u8>> {
        self.heap.read(ObjectId(object_id))
    }

    pub fn stats(&self) -> Stats {
        self.heap.stats()
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
    ) -> Object<LEAF_FANOUT> {
        let object_id = ObjectId(self.heap.allocate_object_id());

        let node = Object {
            object_id,
            collection_id,
            low_key,
            inner: Arc::new(Some(Box::default()).into()),
        };

        self.object_id_index.insert(object_id, node.clone());

        node
    }

    pub fn allocate_object_id(&self) -> u64 {
        self.heap.allocate_object_id()
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

        let old_dirty = self.dirty.insert((flush_epoch, object_id), dirty);

        if let Some(old) = old_dirty {
            assert!(!old.is_final_state(),
                "tried to install another Dirty marker for a node that is already
                finalized for this flush epoch. {:?} old: {:?}",
                flush_epoch, old
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
    ) -> io::Result<()> {
        let mut ca = self.cache_advisor.borrow_mut();
        let to_evict = ca.accessed_reuse_buffer(object_id.0, size);
        for (node_to_evict, _rough_size) in to_evict {
            let node = if let Some(n) =
                self.object_id_index.get(&ObjectId(*node_to_evict))
            {
                if n.object_id.0 != *node_to_evict {
                    log::warn!("during cache eviction, node to evict did not match current occupant for {:?}", node_to_evict);
                    continue;
                }
                n
            } else {
                log::warn!("during cache eviction, unable to find node to evict for {:?}", node_to_evict);
                continue;
            };

            let mut write = node.inner.write();
            if write.is_none() {
                // already paged out
                continue;
            }
            let leaf: &mut Leaf<LEAF_FANOUT> = write.as_mut().unwrap();

            if let Some(dirty_epoch) = leaf.dirty_flush_epoch {
                // We can't page out this leaf until it has been
                // flushed, because its changes are not yet durable.
                leaf.page_out_on_flush = Some(dirty_epoch);
            } else {
                *write = None;
            }
        }

        Ok(())
    }

    pub fn flush(&self) -> io::Result<()> {
        let mut write_batch = vec![];

        log::trace!("advancing epoch");
        let (
            previous_flush_complete_notifier,
            this_vacant_notifier,
            forward_flush_notifier,
        ) = self.flush_epoch.roll_epoch_forward();

        log::trace!("waiting for previous flush to complete");
        previous_flush_complete_notifier.wait_for_complete();

        log::trace!("waiting for our epoch to become vacant");
        let flush_through_epoch: FlushEpoch =
            this_vacant_notifier.wait_for_complete();

        log::trace!("performing flush");

        let flush_boundary = (flush_through_epoch.increment(), ObjectId::MIN);

        let mut evict_after_flush = vec![];

        for ((dirty_epoch, dirty_object_id), dirty_value_initial_read) in
            self.dirty.range(..flush_boundary)
        {
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

            #[cfg(feature = "for-internal-testing-only")]
            {
                let mutation_count = lock.as_ref().unwrap().mutation_count;
                self.event_verifier.mark_flush(
                    node.id,
                    dirty_epoch,
                    mutation_count,
                );
            }

            match dirty_value {
                Dirty::MergedAndDeleted { object_id, collection_id } => {
                    log::trace!(
                        "MergedAndDeleted for {:?}, adding None to write_batch",
                        object_id
                    );
                    write_batch.push(Update::Free {
                        object_id: dirty_object_id,
                        collection_id,
                    });
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
                }
                Dirty::NotYetSerialized { low_key, collection_id, node } => {
                    assert_eq!(dirty_object_id, node.object_id, "mismatched node ID for NotYetSerialized with low key {:?}", low_key);
                    let mut lock = node.inner.write();

                    let leaf_ref: &mut Leaf<LEAF_FANOUT> =
                        lock.as_mut().unwrap();

                    let data = if leaf_ref.dirty_flush_epoch
                        == Some(flush_through_epoch)
                    {
                        if let Some(deleted_at) = leaf_ref.deleted {
                            assert!(deleted_at > flush_through_epoch);
                        }
                        leaf_ref.dirty_flush_epoch.take();
                        leaf_ref.serialize(self.config.zstd_compression_level)
                    } else {
                        // Here we expect that there was a benign data race and that another thread
                        // mutated the leaf after encountering it being dirty for our epoch, after
                        // storing a CooperativelySerialized in the dirty map.
                        let dirty_value_2_opt =
                            self.dirty.remove(&(dirty_epoch, dirty_object_id));

                        let dirty_value_2 = if let Some(dv2) = dirty_value_2_opt
                        {
                            dv2
                        } else {
                            log::error!(
                                "violation of flush responsibility for second read \
                                of expected cooperative serialization. leaf in question's \
                                dirty_flush_epoch is {:?}, our expected key was {:?}. node.deleted: {:?}",
                                leaf_ref.dirty_flush_epoch,
                                (dirty_epoch, dirty_object_id),
                                leaf_ref.deleted,
                            );

                            std::process::abort();
                        };

                        if let Dirty::CooperativelySerialized {
                            low_key: low_key_2,
                            mutation_count: _,
                            mut data,
                            collection_id: ci2,
                            object_id: ni2,
                        } = dirty_value_2
                        {
                            assert_eq!(node.object_id, ni2);
                            assert_eq!(node.object_id, dirty_object_id);
                            assert_eq!(low_key, low_key_2);
                            assert_eq!(collection_id, ci2);
                            Arc::make_mut(&mut data);
                            Arc::into_inner(data).unwrap()
                        } else {
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

        let written_count = write_batch.len();
        if written_count > 0 {
            self.heap.write_batch(write_batch)?;
            log::trace!(
                "marking {:?} as flushed - {} objects written",
                flush_through_epoch,
                written_count
            );
        }
        log::trace!(
            "marking the forward flush notifier that {:?} is flushed",
            flush_through_epoch
        );
        forward_flush_notifier.mark_complete();

        for node_to_evict in evict_after_flush {
            let mut lock = node_to_evict.inner.write();
            if let Some(ref mut leaf) = *lock {
                if leaf.page_out_on_flush == Some(flush_through_epoch) {
                    *lock = None;
                }
            }
        }

        self.maintenance();

        Ok(())
    }

    // maintenance is infallible because if it fails, it doesn't invalidate
    // the durability already achieved earlier.
    fn maintenance(&self) {}
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
            inner: Arc::new(None.into()),
        };

        assert!(object_id_index.insert(*object_id, node.clone()).is_none());

        let tree = trees.entry(*collection_id).or_default();

        assert!(tree.insert(metadata.clone(), node).is_none());
    }

    // initialize default collections if not recovered
    for collection_id in [NAME_MAPPING_COLLECTION_ID, DEFAULT_COLLECTION_ID] {
        let tree = trees.entry(collection_id).or_default();

        if tree.is_empty() {
            let object_id = ObjectId(heap.allocate_object_id());

            let initial_low_key = InlineArray::default();

            let empty_node = Object {
                object_id,
                collection_id,
                low_key: initial_low_key.clone(),
                inner: Arc::new(Some(Box::default()).into()),
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

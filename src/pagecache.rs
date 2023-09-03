use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::io;
use std::mem::{self, ManuallyDrop};
use std::ops;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use cache_advisor::CacheAdvisor;
use concurrent_map::{ConcurrentMap, Minimum};
use inline_array::InlineArray;
use parking_lot::{
    lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard},
    RawRwLock, RwLock,
};
use stack_map::StackMap;

use crate::*;

#[derive(Clone)]
struct PageCache<const LEAF_FANOUT: usize> {
    config: Config,
    global_error: Arc<AtomicPtr<(io::ErrorKind, String)>>,
    node_id_index: ConcurrentMap<
        u64,
        Node<LEAF_FANOUT>,
        INDEX_FANOUT,
        EBR_LOCAL_GC_BUFFER_SIZE,
    >,
    store: heap::Heap,
    cache_advisor: RefCell<CacheAdvisor>,
    flush_epoch: FlushEpochTracker,
    dirty: ConcurrentMap<(FlushEpoch, NodeId), Dirty<LEAF_FANOUT>>,
}

impl<const LEAF_FANOUT: usize> PageCache<LEAF_FANOUT> {
    fn check_error(&self) -> io::Result<()> {
        let err_ptr: *const (io::ErrorKind, String) =
            self.global_error.load(Ordering::Acquire);

        if err_ptr.is_null() {
            Ok(())
        } else {
            let deref: &(io::ErrorKind, String) = unsafe { &*err_ptr };
            Err(io::Error::new(deref.0, deref.1.clone()))
        }
    }

    fn set_error(&self, error: &io::Error) {
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

    // NB: must not be called while holding a leaf lock - which also means
    // that no two LeafGuards can be held concurrently in the same scope due to
    // this being called in the destructor.
    fn mark_access_and_evict(
        &self,
        node_id: NodeId,
        size: usize,
    ) -> io::Result<()> {
        let mut ca = self.cache_advisor.borrow_mut();
        let to_evict = ca.accessed_reuse_buffer(node_id.0, size);
        for (node_to_evict, _rough_size) in to_evict {
            let node = if let Some(n) = self.node_id_index.get(&node_to_evict) {
                if n.id.0 != *node_to_evict {
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

    fn flush(&self) -> io::Result<()> {
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

        let flush_boundary = (flush_through_epoch.increment(), NodeId::MIN);

        let mut evict_after_flush = vec![];

        for ((dirty_epoch, dirty_node_id), dirty_value_initial_read) in
            self.dirty.range(..flush_boundary)
        {
            let dirty_value = self
                .dirty
                .remove(&(dirty_epoch, dirty_node_id))
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
                Dirty::MergedAndDeleted { node_id } => {
                    log::trace!(
                        "MergedAndDeleted for {:?}, adding None to write_batch",
                        node_id
                    );
                    write_batch.push(heap::Update::Free {
                        node_id: dirty_node_id,
                        collection_id: CollectionId::MIN,
                    });
                }
                Dirty::CooperativelySerialized {
                    low_key,
                    mutation_count: _,
                    mut data,
                } => {
                    Arc::make_mut(&mut data);
                    let data = Arc::into_inner(data).unwrap();
                    write_batch.push(heap::Update::Store {
                        node_id: dirty_node_id,
                        collection_id: CollectionId::MIN,
                        metadata: low_key,
                        data,
                    });
                }
                Dirty::NotYetSerialized { low_key, node } => {
                    assert_eq!(dirty_node_id, node.id, "mismatched node ID for NotYetSerialized with low key {:?}", low_key);
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
                            self.dirty.remove(&(dirty_epoch, dirty_node_id));

                        let dirty_value_2 = if let Some(dv2) = dirty_value_2_opt
                        {
                            dv2
                        } else {
                            eprintln!(
                                "violation of flush responsibility for second read \
                                of expected cooperative serialization. leaf in question's \
                                dirty_flush_epoch is {:?}, our expected key was {:?}. node.deleted: {:?}",
                                leaf_ref.dirty_flush_epoch,
                                (dirty_epoch, dirty_node_id),
                                leaf_ref.deleted,
                            );

                            std::process::abort();
                        };

                        if let Dirty::CooperativelySerialized {
                            low_key: low_key_2,
                            mutation_count: _,
                            mut data,
                        } = dirty_value_2
                        {
                            assert_eq!(low_key, low_key_2);
                            Arc::make_mut(&mut data);
                            Arc::into_inner(data).unwrap()
                        } else {
                            unreachable!("a leaf was expected to be cooperatively serialized but it was not available");
                        }
                    };

                    write_batch.push(heap::Update::Store {
                        node_id: dirty_node_id,
                        collection_id: CollectionId::MIN,
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
            self.store.write_batch(write_batch)?;
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

        self.store.maintenance()?;
        Ok(())
    }
}

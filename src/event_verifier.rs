use std::collections::BTreeMap;
use std::num::NonZeroU64;

use crossbeam_queue::SegQueue;

use crate::NodeId;

pub(crate) enum Event {
    /// Records a set of pages and their mutation_count that
    /// was flushed.
    Flush { flush_epoch: NonZeroU64, write_batch: BTreeMap<NodeId, u64> },
    /// Records a single mutation to a page and the epoch that it
    /// was marked dirty at.
    MarkDirty {
        node_id: NodeId,
        flush_epoch: NonZeroU64,
        mutation_count: u64,
        cooperative: bool,
    },
    /// Records when a Flush encounters an earlier Dirty item than expected
    FlushUnexpectedEpoch {
        flush_epoch: NonZeroU64,
        node_id: NodeId,
        mutation_count: u64,
    },
}

#[derive(Debug, Default)]
pub(crate) struct EventVerifier {
    event_q: SegQueue<Event>,
}

impl EventVerifier {
    pub(crate) fn mark_flush(
        &self,
        flush_epoch: NonZeroU64,
        flushed_objects_to_mutation_count: impl IntoIterator<Item = (NodeId, u64)>,
    ) {
        let mut write_batch =
            flushed_objects_to_mutation_count.into_iter().collect();
        self.event_q.push(Event::Flush { flush_epoch, write_batch });
    }

    pub(crate) fn mark_unexpected_flush_epoch(
        &self,
        node_id: NodeId,
        flush_epoch: NonZeroU64,
        mutation_count: u64,
    ) {
        self.event_q.push(Event::FlushUnexpectedEpoch {
            node_id,
            flush_epoch,
            mutation_count,
        });
    }

    pub(crate) fn mark_dirty(
        &self,
        node_id: NodeId,
        flush_epoch: NonZeroU64,
        mutation_count: u64,
        cooperative: bool,
    ) {
        self.event_q.push(Event::MarkDirty {
            node_id,
            flush_epoch,
            mutation_count,
            cooperative,
        });
    }

    pub(crate) fn verify(&self) {
        println!("verifying!");
        // flush_epoch -> node_id -> flushed version
        let mut flush_model: BTreeMap<NonZeroU64, BTreeMap<NodeId, u64>> =
            BTreeMap::new();
        let mut dirty_model: BTreeMap<NonZeroU64, BTreeMap<NodeId, u64>> =
            BTreeMap::new();

        while let Some(event) = self.event_q.pop() {
            match event {
                Event::Flush { write_batch, flush_epoch } => {
                    let epoch_entry =
                        dirty_model.entry(flush_epoch).or_default();

                    for (node_id, mutation_count) in write_batch {
                        let dirty_object_entry =
                            epoch_entry.entry(node_id).or_default();
                        *dirty_object_entry =
                            (*dirty_object_entry).max(mutation_count);
                    }
                }
                Event::FlushUnexpectedEpoch {
                    flush_epoch,
                    node_id,
                    mutation_count,
                } => {
                    // assert that this object+mutation count was
                    // already flushed
                    let epoch_entry =
                        dirty_model.entry(flush_epoch).or_default();
                    let flushed_mutation_count = epoch_entry.get(&node_id);
                    println!("checking!");
                    assert_eq!(Some(&mutation_count), flushed_mutation_count);
                    println!("good!");
                }
                Event::MarkDirty {
                    node_id,
                    flush_epoch,
                    mutation_count,
                    cooperative,
                } => {
                    let epoch_entry =
                        dirty_model.entry(flush_epoch).or_default();
                    let dirty_object_entry =
                        epoch_entry.entry(node_id).or_default();
                    *dirty_object_entry =
                        (*dirty_object_entry).max(mutation_count);
                }
            }
        }

        // Basically, if a MarkDirty ever happens after its
        // flush_epoch is flushed, we blow up.
    }
}

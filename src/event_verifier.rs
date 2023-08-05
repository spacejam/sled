use std::collections::BTreeMap;

use crossbeam_queue::SegQueue;

pub(crate) enum Event {
    /// Records a set of pages and their mutation_count that
    /// was flushed.
    Flush { flush_epoch: u64, write_batch: BTreeMap<u64, u64> },
    /// Records a single mutation to a page and the epoch that it
    /// was marked dirty at.
    MarkDirty { object_id: u64, flush_epoch: u64, mutation_count: u64 },
}

#[derive(Debug, Default)]
pub(crate) struct EventVerifier {
    event_rx: SegQueue<Event>,
}

impl EventVerifier {
    pub(crate) fn mark_flush(&self, write_set: ()) {
        todo!()
    }

    pub(crate) fn mark_dirty(
        &self,
        object_id: u64,
        flush_epoch: u64,
        mutation_count: u64,
    ) {
        todo!()
    }

    pub(crate) fn verify(&self) {
        // flush_epoch -> object_id -> flushed version
        let mut flush_model: BTreeMap<u64, BTreeMap<u64, u64>> =
            BTreeMap::new();
        let mut dirty_model: BTreeMap<u64, BTreeMap<u64, u64>> =
            BTreeMap::new();

        while let Some(event) = self.event_rx.pop() {
            match event {
                Event::Flush { write_batch, flush_epoch } => {
                    let epoch_entry =
                        dirty_model.entry(flush_epoch).or_default();

                    for (object_id, mutation_count) in write_batch {
                        let dirty_object_entry =
                            epoch_entry.entry(object_id).or_default();
                        *dirty_object_entry =
                            (*dirty_object_entry).max(mutation_count);
                    }
                }
                Event::MarkDirty { object_id, flush_epoch, mutation_count } => {
                    let epoch_entry =
                        dirty_model.entry(flush_epoch).or_default();
                    let dirty_object_entry =
                        epoch_entry.entry(object_id).or_default();
                    *dirty_object_entry =
                        (*dirty_object_entry).max(mutation_count);
                }
            }
        }

        // basically, if a MarkDirty ever happens after its flush_epoch is flushed, we blow up
    }
}

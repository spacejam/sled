use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::Mutex;

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
    flush_model: Mutex<BTreeMap<NonZeroU64, BTreeMap<NodeId, u64>>>,
}

impl EventVerifier {
    pub(crate) fn mark_flush(
        &self,
        node_id: NodeId,
        flush_epoch: NonZeroU64,
        mutation_count: u64,
    ) {
        let mut flush_model = self.flush_model.lock().unwrap();

        let epoch_entry = flush_model.entry(flush_epoch).or_default();

        let last = epoch_entry.insert(node_id, mutation_count);
        assert_eq!(last, None);
    }

    pub(crate) fn mark_unexpected_flush_epoch(
        &self,
        node_id: NodeId,
        flush_epoch: NonZeroU64,
        mutation_count: u64,
    ) {
        let mut flush_model = self.flush_model.lock().unwrap();
        // assert that this object+mutation count was
        // already flushed
        let epoch_entry = flush_model.entry(flush_epoch).or_default();
        let flushed_mutation_count = epoch_entry.get(&node_id);
        println!("checking!");
        assert_eq!(Some(&mutation_count), flushed_mutation_count);
        println!("good!");
    }

    pub(crate) fn mark_dirty(
        &self,
        node_id: NodeId,
        flush_epoch: NonZeroU64,
        mutation_count: u64,
        cooperative: bool,
    ) {
        let flush_model = self.flush_model.lock().unwrap();
        assert!(!flush_model.contains_key(&flush_epoch));
    }
}

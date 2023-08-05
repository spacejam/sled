use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::Mutex;

use crate::NodeId;

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
}

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::{FlushEpoch, ObjectId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    Unallocated,
    Dirty,
    CooperativelySerialized,
    AddedToWriteBatch,
    Flushed,
    CleanPagedIn,
    PagedOut,
}

impl State {
    fn can_transition_within_epoch_to(&self, next: State) -> bool {
        match (self, next) {
            (State::Flushed, State::PagedOut) => true,
            (State::Flushed, _) => false,
            (State::AddedToWriteBatch, State::Flushed) => true,
            (State::AddedToWriteBatch, _) => false,
            (State::CleanPagedIn, State::AddedToWriteBatch) => false,
            (State::CleanPagedIn, State::Flushed) => false,
            (State::Dirty, State::AddedToWriteBatch) => true,
            (State::CooperativelySerialized, State::AddedToWriteBatch) => true,
            (State::CooperativelySerialized, _) => false,
            (State::Unallocated, State::AddedToWriteBatch) => true,
            (State::Unallocated, _) => false,
            (State::Dirty, State::Dirty) => true,
            (State::Dirty, State::CooperativelySerialized) => true,
            (State::Dirty, State::Unallocated) => true,
            (State::Dirty, _) => false,
            (State::CleanPagedIn, State::Dirty) => true,
            (State::CleanPagedIn, State::PagedOut) => true,
            (State::CleanPagedIn, State::CleanPagedIn) => true,
            (State::CleanPagedIn, State::Unallocated) => true,
            (State::CleanPagedIn, State::CooperativelySerialized) => true,
            (State::PagedOut, State::CleanPagedIn) => true,
            (State::PagedOut, _) => false,
        }
    }

    fn needs_flush(&self) -> bool {
        match self {
            State::CleanPagedIn => false,
            State::Flushed => false,
            State::PagedOut => false,
            _ => true,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct EventVerifier {
    flush_model:
        Mutex<BTreeMap<(ObjectId, FlushEpoch), Vec<(State, &'static str)>>>,
}

impl Drop for EventVerifier {
    fn drop(&mut self) {
        // assert that nothing is currently Dirty
        let flush_model = self.flush_model.lock().unwrap();
        for ((oid, _epoch), history) in flush_model.iter() {
            if let Some((last_state, _at)) = history.last() {
                assert_ne!(
                    *last_state,
                    State::Dirty,
                    "{oid:?} is Dirty when system shutting down"
                );
            }
        }
    }
}

impl EventVerifier {
    pub(crate) fn mark(
        &self,
        object_id: ObjectId,
        epoch: FlushEpoch,
        state: State,
        at: &'static str,
    ) {
        if matches!(state, State::PagedOut) {
            let dirty_epochs = self.dirty_epochs(object_id);
            if !dirty_epochs.is_empty() {
                println!("{object_id:?} was paged out while having dirty epochs {dirty_epochs:?}");
                self.print_debug_history_for_object(object_id);
                println!("{state:?} {epoch:?} {at}");
                println!("invalid object state transition");
                std::process::abort();
            }
        }

        let mut flush_model = self.flush_model.lock().unwrap();
        let history = flush_model.entry((object_id, epoch)).or_default();

        if let Some((last_state, _at)) = history.last() {
            if !last_state.can_transition_within_epoch_to(state) {
                println!(
                    "object_id {object_id:?} performed \
                    illegal state transition from {last_state:?} \
                    to {state:?} at {at} in epoch {epoch:?}."
                );

                println!("history:");
                history.push((state, at));

                let active_epochs = flush_model.range(
                    (object_id, FlushEpoch::MIN)..=(object_id, FlushEpoch::MAX),
                );
                for ((_oid, epoch), history) in active_epochs {
                    for (last_state, at) in history {
                        println!("{last_state:?} {epoch:?} {at}");
                    }
                }

                println!("invalid object state transition");

                std::process::abort();
            }
        }
        history.push((state, at));
    }

    /// Returns the FlushEpochs for which this ObjectId has unflushed
    /// dirty data for.
    fn dirty_epochs(&self, object_id: ObjectId) -> Vec<FlushEpoch> {
        let mut dirty_epochs = vec![];
        let flush_model = self.flush_model.lock().unwrap();

        let active_epochs = flush_model
            .range((object_id, FlushEpoch::MIN)..=(object_id, FlushEpoch::MAX));

        for ((_oid, epoch), history) in active_epochs {
            let (last_state, _at) = history.last().unwrap();
            if last_state.needs_flush() {
                dirty_epochs.push(*epoch);
            }
        }

        dirty_epochs
    }

    pub(crate) fn print_debug_history_for_object(&self, object_id: ObjectId) {
        let flush_model = self.flush_model.lock().unwrap();
        println!("history for object {:?}:", object_id);
        let active_epochs = flush_model
            .range((object_id, FlushEpoch::MIN)..=(object_id, FlushEpoch::MAX));
        for ((_oid, epoch), history) in active_epochs {
            for (last_state, at) in history {
                println!("{last_state:?} {epoch:?} {at}");
            }
        }
    }
}

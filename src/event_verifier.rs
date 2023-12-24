use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::{FlushEpoch, ObjectId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    Unallocated,
    Dirty,
    CleanPagedIn,
    PagedOut,
}

impl State {
    fn can_transition_to(&self, next: State) -> bool {
        match (self, next) {
            (State::Unallocated, State::CleanPagedIn) => true,
            (State::Unallocated, _) => false,
            (State::Dirty, State::Dirty) => true,
            (State::Dirty, State::CleanPagedIn) => true,
            (State::Dirty, _) => false,
            (State::CleanPagedIn, State::Dirty) => true,
            (State::CleanPagedIn, State::PagedOut) => true,
            (State::CleanPagedIn, _) => false,
            (State::PagedOut, State::CleanPagedIn) => true,
            (State::PagedOut, _) => false,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct EventVerifier {
    flush_model: Mutex<
        BTreeMap<ObjectId, Vec<(State, Option<FlushEpoch>, &'static str)>>,
    >,
}

impl Drop for EventVerifier {
    fn drop(&mut self) {
        // assert that nothing is currently Dirty
        let flush_model = self.flush_model.lock().unwrap();
        for (oid, history) in flush_model.iter() {
            if let Some((last_state, _epoch, _at)) = history.last() {
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
        epoch: Option<FlushEpoch>,
        state: State,
        at: &'static str,
    ) {
        let mut flush_model = self.flush_model.lock().unwrap();
        let history = flush_model.entry(object_id).or_default();

        if let Some((last_state, _epoch, _at)) = history.last() {
            assert!(
                last_state.can_transition_to(state),
                "object_id {object_id:?} performed \
                illegal state transition from {last_state:?} to {state:?} at {at} in epoch {epoch:?}.\nhistory: {:#?}",
                *history
            );
        }
        history.push((state, epoch, at));
    }

    pub(crate) fn print_debug_history_for_object(&self, object_id: ObjectId) {
        let flush_model = self.flush_model.lock().unwrap();
        let history = flush_model.get(&object_id).unwrap();
        println!("history for object {:?}: {:#?}", object_id, history);
    }
}

use crate::epoch;

/// A guard used with epoch-based reclamation (EBR)
/// to track threads accessing shared lock-free
/// data structures, and safely drop data
/// after any thread may have accessed it.
pub type Guard = epoch::Guard;

/// Enter an epoch which allows us to safely
/// access shared data structures without
/// using mutexes.
pub fn pin() -> Guard {
    epoch::pin()
}

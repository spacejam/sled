use crate::epoch::{self, Collector, LocalHandle};

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

lazy_static! {
    /// This collector is for controlling freeing log segments,
    /// and NOTHING ELSE! All reservations get a guard, and
    /// segments are only added to the free list after all
    /// reservations before or equal have been completed.
    static ref LOG_COLLECTOR: Collector = Collector::new();
}

thread_local! {
    static LOG_HANDLE: LocalHandle = LOG_COLLECTOR.register();
}

pub(crate) unsafe fn pin_log() -> Guard {
    LOG_HANDLE
        .try_with(|h| h.pin())
        .unwrap_or_else(|_| LOG_COLLECTOR.register().pin())
}

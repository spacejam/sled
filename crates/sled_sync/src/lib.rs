extern crate crossbeam_epoch as epoch;
#[cfg_attr(any(test, feature = "lock_free_delays"), macro_use)]
extern crate log;

#[cfg(any(test, feature = "lock_free_delays"))]
extern crate rand;

#[cfg(any(test, feature = "lock_free_delays"))]
mod debug_delay;

#[cfg(any(test, feature = "lock_free_delays"))]
pub use debug_delay::debug_delay;

/// This function is useful for inducing random jitter into our atomic
/// operations, shaking out more possible interleavings quickly. It gets
/// fully elliminated by the compiler in non-test code.
#[cfg(not(any(test, feature = "lock_free_delays")))]
pub fn debug_delay() {}

pub use epoch::{
    unprotected, Atomic, Collector, CompareAndSetError, Guard,
    LocalHandle, Owned, Shared,
};

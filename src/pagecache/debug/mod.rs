/// This module helps test concurrent issues with random jittering and another instruments.

#[cfg(any(test, feature = "lock_free_delays"))]
mod delay;

#[cfg(any(test, feature = "lock_free_delays"))]
pub use delay::debug_delay;

/// This function is useful for inducing random jitter into our atomic
/// operations, shaking out more possible interleavings quickly. It gets
/// fully eliminated by the compiler in non-test code.
#[cfg(not(any(test, feature = "lock_free_delays")))]
pub fn debug_delay() {}

/// Vendored and simplified from crossbeam-utils
use core::cell::Cell;
use core::sync::atomic;

const SPIN_LIMIT: u32 = 6;

/// Performs exponential backoff in spin loops.
///
/// Backing off in spin loops reduces contention and improves overall performance.
///
/// This primitive can execute *YIELD* and *PAUSE* instructions, yield the current thread to the OS
/// scheduler, and tell when is a good time to block the thread using a different synchronization
/// mechanism. Each step of the back off procedure takes roughly twice as long as the previous
/// step.
pub struct Backoff {
    step: Cell<u32>,
}

impl Backoff {
    /// Creates a new `Backoff`.
    pub const fn new() -> Self {
        Backoff { step: Cell::new(0) }
    }

    /// Backs off in a lock-free loop.
    ///
    /// This method should be used when we need to retry an operation because another thread made
    /// progress.
    ///
    /// The processor may yield using the *YIELD* or *PAUSE* instruction.
    #[inline]
    pub fn spin(&self) {
        for _ in 0..1 << self.step.get().min(SPIN_LIMIT) {
            // `hint::spin_loop` requires Rust 1.49.
            #[allow(deprecated)]
            atomic::spin_loop_hint();
        }

        if self.step.get() <= SPIN_LIMIT {
            self.step.set(self.step.get() + 1);
        }
    }
}

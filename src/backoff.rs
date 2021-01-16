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
///
/// # Examples
///
/// Backing off in a lock-free loop:
///
/// ```
/// use crossbeam_utils::Backoff;
/// use std::sync::atomic::AtomicUsize;
/// use std::sync::atomic::Ordering::SeqCst;
///
/// fn fetch_mul(a: &AtomicUsize, b: usize) -> usize {
///     let backoff = Backoff::new();
///     loop {
///         let val = a.load(SeqCst);
///         if a.compare_and_swap(val, val.wrapping_mul(b), SeqCst) == val {
///             return val;
///         }
///         backoff.spin();
///     }
/// }
/// ```
pub struct Backoff {
    step: Cell<u32>,
}

impl Backoff {
    /// Creates a new `Backoff`.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_utils::Backoff;
    ///
    /// let backoff = Backoff::new();
    /// ```
    #[inline]
    pub fn new() -> Self {
        Backoff { step: Cell::new(0) }
    }

    /// Backs off in a lock-free loop.
    ///
    /// This method should be used when we need to retry an operation because another thread made
    /// progress.
    ///
    /// The processor may yield using the *YIELD* or *PAUSE* instruction.
    ///
    /// # Examples
    ///
    /// Backing off in a lock-free loop:
    ///
    /// ```
    /// use crossbeam_utils::Backoff;
    /// use std::sync::atomic::AtomicUsize;
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// fn fetch_mul(a: &AtomicUsize, b: usize) -> usize {
    ///     let backoff = Backoff::new();
    ///     loop {
    ///         let val = a.load(SeqCst);
    ///         if a.compare_and_swap(val, val.wrapping_mul(b), SeqCst) == val {
    ///             return val;
    ///         }
    ///         backoff.spin();
    ///     }
    /// }
    ///
    /// let a = AtomicUsize::new(7);
    /// assert_eq!(fetch_mul(&a, 8), 7);
    /// assert_eq!(a.load(SeqCst), 56);
    /// ```
    #[inline]
    pub fn spin(&self) {
        for _ in 0..1 << self.step.get().min(SPIN_LIMIT) {
            atomic::spin_loop_hint();
        }

        if self.step.get() <= SPIN_LIMIT {
            self.step.set(self.step.get() + 1);
        }
    }
}

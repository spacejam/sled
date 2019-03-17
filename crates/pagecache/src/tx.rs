use super::*;

/// A handle to an ongoing pagecache transaction. Ensures
/// that any state which is removed from a shared in-memory
/// data structure is not destroyed until all possible
/// readers have concluded.
pub struct Tx {
    #[doc(hidden)]
    pub guard: Guard,
    #[doc(hidden)]
    pub ts: u64,
}

impl Tx {
    /// Creates a new Tx with a given timestamp.
    pub fn new(ts: u64) -> Self {
        Self { guard: pin(), ts }
    }
}

impl std::ops::Deref for Tx {
    type Target = Guard;

    fn deref(&self) -> &Guard {
        &self.guard
    }
}

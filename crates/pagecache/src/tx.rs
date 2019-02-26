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

impl std::ops::Deref for Tx {
    type Target = Guard;

    fn deref(&self) -> &Guard {
        &self.guard
    }
}

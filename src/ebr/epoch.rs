//! The global epoch
//!
//! The last bit in this number is unused and is always zero. Every so often the global epoch is
//! incremented, i.e. we say it "advances". A pinned participant may advance the global epoch only
//! if all currently pinned participants have been pinned in the current epoch.
//!
//! If an object became garbage in some epoch, then we can be sure that after two advancements no
//! participant will hold a reference to it. That is the crux of safe memory reclamation.

use core::sync::atomic::{AtomicUsize, Ordering};

/// An epoch that can be marked as pinned or unpinned.
///
/// Internally, the epoch is represented as an integer that wraps around at some unspecified point
/// and a flag that represents whether it is pinned or unpinned.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct Epoch {
    /// The least significant bit is set if pinned. The rest of the bits hold the epoch.
    data: usize,
}

impl Epoch {
    /// Returns the starting epoch in unpinned state.
    pub(super) const fn starting() -> Self {
        Epoch { data: 0 }
    }

    /// Returns the number of epochs `self` is ahead of `rhs`.
    ///
    /// Internally, epochs are represented as numbers in the range `(isize::MIN / 2) .. (isize::MAX
    /// / 2)`, so the returned distance will be in the same interval.
    pub(super) const fn wrapping_sub(self, rhs: Self) -> usize {
        // The result is the same with `(self.data & !1).wrapping_sub(rhs.data & !1) as isize >> 1`,
        // because the possible difference of LSB in `(self.data & !1).wrapping_sub(rhs.data & !1)`
        // will be ignored in the shift operation.
        self.data.wrapping_sub(rhs.data & !1) >> 1
    }

    /// Returns `true` if the epoch is marked as pinned.
    pub(super) const fn is_pinned(self) -> bool {
        (self.data & 1) == 1
    }

    /// Returns the same epoch, but marked as pinned.
    pub(super) const fn pinned(self) -> Epoch {
        Epoch { data: self.data | 1 }
    }

    /// Returns the same epoch, but marked as unpinned.
    pub(super) const fn unpinned(self) -> Epoch {
        Epoch { data: self.data & !1 }
    }

    /// Returns the successor epoch.
    ///
    /// The returned epoch will be marked as pinned only if the previous one was as well.
    pub(super) const fn successor(self) -> Epoch {
        Epoch { data: self.data.wrapping_add(2) }
    }
}

/// An atomic value that holds an `Epoch`.
#[derive(Default, Debug)]
pub(super) struct AtomicEpoch {
    /// Since `Epoch` is just a wrapper around `usize`, an `AtomicEpoch` is similarly represented
    /// using an `AtomicUsize`.
    data: AtomicUsize,
}

impl AtomicEpoch {
    /// Creates a new atomic epoch.
    pub(super) const fn new(epoch: Epoch) -> Self {
        let data = AtomicUsize::new(epoch.data);
        AtomicEpoch { data }
    }

    /// Loads a value from the atomic epoch.
    #[inline]
    pub(super) fn load(&self, ord: Ordering) -> Epoch {
        Epoch { data: self.data.load(ord) }
    }

    /// Stores a value into the atomic epoch.
    #[inline]
    pub(super) fn store(&self, epoch: Epoch, ord: Ordering) {
        self.data.store(epoch.data, ord);
    }

    /// Stores a value into the atomic epoch if the current value is the same as `current`.
    ///
    /// The return value is always the previous value. If it is equal to `current`, then the value
    /// is updated.
    ///
    /// The `Ordering` argument describes the memory ordering of this operation.
    #[inline]
    pub(super) fn compare_and_swap(
        &self,
        current: Epoch,
        new: Epoch,
        ord: Ordering,
    ) -> Epoch {
        use super::atomic::CompareAndSetOrdering;

        match self.data.compare_exchange(
            current.data,
            new.data,
            ord.success(),
            ord.failure(),
        ) {
            Ok(data) | Err(data) => Epoch { data },
        }
    }
}

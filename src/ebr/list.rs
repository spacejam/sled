//! Lock-free intrusive linked list.
//!
//! Ideas from Michael.  High Performance Dynamic Lock-Free Hash Tables and List-Based Sets.  SPAA
//! 2002.  `http://dl.acm.org/citation.cfm?id=564870.564881`

use core::marker::PhantomData;
use core::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use super::{pin, Atomic, Guard, Shared};

/// An entry in a linked list.
///
/// An Entry is accessed from multiple threads, so it would be beneficial to put it in a different
/// cache-line than thread-local data in terms of performance.
#[derive(Debug)]
pub struct Entry {
    /// The next entry in the linked list.
    /// If the tag is 1, this entry is marked as deleted.
    next: Atomic<Entry>,
}

/// Implementing this trait asserts that the type `T` can be used as an element in the intrusive
/// linked list defined in this module. `T` has to contain (or otherwise be linked to) an instance
/// of `Entry`.
///
/// # Example
///
/// This trait is implemented on a type separate from `T` (although it can be just `T`), because
/// one type might be placeable into multiple lists, in which case it would require multiple
/// implementations of `IsElement`. In such cases, each struct implementing `IsElement<T>`
/// represents a distinct `Entry` in `T`.
///
/// For example, we can insert the following struct into two lists using `entry1` for one
/// and `entry2` for the other:
pub trait IsElement<T> {
    /// Returns a reference to this element's `Entry`.
    fn entry_of(_: &T) -> &Entry;

    /// Given a reference to an element's entry, returns that element.
    ///
    /// # Safety
    ///
    /// The caller has to guarantee that the `Entry` is called with was retrieved from an instance
    /// of the element type (`T`).
    unsafe fn element_of(_: &Entry) -> &T;

    /// The function that is called when an entry is unlinked from list.
    ///
    /// # Safety
    ///
    /// The caller has to guarantee that the `Entry` is called with was retrieved from an instance
    /// of the element type (`T`).
    unsafe fn finalize(_: &Entry, _: &Guard);
}

/// A lock-free, intrusive linked list of type `T`.
#[derive(Debug)]
pub struct List<T, C: IsElement<T> = T> {
    /// The head of the linked list.
    head: Atomic<Entry>,

    /// The phantom data for using `T` and `C`.
    _marker: PhantomData<(T, C)>,
}

/// An iterator used for retrieving values from the list.
pub struct Iter<'g, T, C: IsElement<T>> {
    /// The guard that protects the iteration.
    guard: &'g Guard,

    /// Pointer from the predecessor to the current entry.
    pred: &'g Atomic<Entry>,

    /// The current entry.
    curr: Shared<'g, Entry>,

    /// The list head, needed for restarting iteration.
    head: &'g Atomic<Entry>,

    /// Logically, we store a borrow of an instance of `T` and
    /// use the type information from `C`.
    _marker: PhantomData<(&'g T, C)>,
}

/// An error that occurs during iteration over the list.
#[derive(PartialEq, Debug)]
pub enum IterError {
    /// A concurrent thread modified the state of the list at the same place that this iterator
    /// was inspecting. Subsequent iteration will restart from the beginning of the list.
    Stalled,
}

impl Default for Entry {
    /// Returns the empty entry.
    fn default() -> Self {
        Self { next: Atomic::null() }
    }
}

impl Entry {
    /// Marks this entry as deleted, deferring the actual deallocation to a later iteration.
    ///
    /// # Safety
    ///
    /// The entry should be a member of a linked list, and it should not have been deleted.
    /// It should be safe to call `C::finalize` on the entry after the `guard` is dropped, where `C`
    /// is the associated helper for the linked list.
    pub unsafe fn delete(&self, guard: &Guard) {
        self.next.fetch_or(1, Release, guard);
    }
}

impl<T, C: IsElement<T>> List<T, C> {
    /// Returns a new, empty linked list.
    pub fn new() -> Self {
        Self { head: Atomic::null(), _marker: PhantomData }
    }

    /// Inserts `entry` into the head of the list.
    ///
    /// # Safety
    ///
    /// You should guarantee that:
    ///
    /// - `container` is not null
    /// - `container` is immovable, e.g. inside an `Owned`
    /// - the same `Entry` is not inserted more than once
    /// - the inserted object will be removed before the list is dropped
    pub(crate) unsafe fn insert<'g>(
        &'g self,
        container: Shared<'g, T>,
        guard: &'g Guard,
    ) {
        // Insert right after head, i.e. at the beginning of the list.
        let to = &self.head;
        // Get the intrusively stored Entry of the new element to insert.
        let entry: &Entry = C::entry_of(container.deref());
        // Make a Shared ptr to that Entry.
        #[allow(trivial_casts)]
        let entry_ptr = Shared::from(entry as *const _);
        // Read the current successor of where we want to insert.
        let mut next = to.load(Relaxed, guard);

        loop {
            // Set the Entry of the to-be-inserted element to point to the previous successor of
            // `to`.
            entry.next.store(next, Relaxed);
            match to.compare_and_set_weak(next, entry_ptr, Release, guard) {
                Ok(_) => break,
                // We lost the race or weak CAS failed spuriously. Update the successor and try
                // again.
                Err(err) => next = err.current,
            }
        }
    }

    /// Returns an iterator over all objects.
    ///
    /// # Caveat
    ///
    /// Every object that is inserted at the moment this function is called and persists at least
    /// until the end of iteration will be returned. Since this iterator traverses a lock-free
    /// linked list that may be concurrently modified, some additional caveats apply:
    ///
    /// 1. If a new object is inserted during iteration, it may or may not be returned.
    /// 2. If an object is deleted during iteration, it may or may not be returned.
    /// 3. The iteration may be aborted when it lost in a race condition. In this case, the winning
    ///    thread will continue to iterate over the same list.
    pub fn iter<'g>(&'g self, guard: &'g Guard) -> Iter<'g, T, C> {
        Iter {
            guard,
            pred: &self.head,
            curr: self.head.load(Acquire, guard),
            head: &self.head,
            _marker: PhantomData,
        }
    }
}

impl<T, C: IsElement<T>> Drop for List<T, C> {
    fn drop(&mut self) {
        unsafe {
            let guard = pin();
            let mut curr = self.head.load(Relaxed, &guard);
            while let Some(c) = curr.as_ref() {
                let succ = c.next.load(Relaxed, &guard);
                // Verify that all elements have been removed from the list.
                assert_eq!(succ.tag(), 1);

                C::finalize(curr.deref(), &guard);
                curr = succ;
            }
        }
    }
}

impl<'g, T: 'g, C: IsElement<T>> Iterator for Iter<'g, T, C> {
    type Item = Result<&'g T, IterError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(c) = unsafe { self.curr.as_ref() } {
            let succ = c.next.load(Acquire, self.guard);

            if succ.tag() == 1 {
                // This entry was removed. Try unlinking it from the list.
                let succ = succ.with_tag(0);

                // The tag should always be zero, because removing a node after a logically deleted
                // node leaves the list in an invalid state.
                debug_assert!(self.curr.tag() == 0);

                // Try to unlink `curr` from the list, and get the new value of `self.pred`.
                let succ = match self
                    .pred
                    .compare_and_set(self.curr, succ, Acquire, self.guard)
                {
                    Ok(_) => {
                        // We succeeded in unlinking `curr`, so we have to schedule
                        // deallocation. Deferred drop is okay, because `list.delete()` can only be
                        // called if `T: 'static`.
                        unsafe {
                            C::finalize(self.curr.deref(), self.guard);
                        }

                        // `succ` is the new value of `self.pred`.
                        succ
                    }
                    Err(e) => {
                        // `e.current` is the current value of `self.pred`.
                        e.current
                    }
                };

                // If the predecessor node is already marked as deleted, we need to restart from
                // `head`.
                if succ.tag() != 0 {
                    self.pred = self.head;
                    self.curr = self.head.load(Acquire, self.guard);

                    return Some(Err(IterError::Stalled));
                }

                // Move over the removed by only advancing `curr`, not `pred`.
                self.curr = succ;
                continue;
            }

            // Move one step forward.
            self.pred = &c.next;
            self.curr = succ;

            return Some(Ok(unsafe { C::element_of(c) }));
        }

        // We reached the end of the list.
        None
    }
}

#[cfg(test)]
mod tests {
    #![allow(trivial_casts)]
    use super::*;
    use crate::ebr::collector::Collector;
    use crate::ebr::Owned;

    impl IsElement<Entry> for Entry {
        fn entry_of(entry: &Entry) -> &Entry {
            entry
        }

        unsafe fn element_of(entry: &Entry) -> &Entry {
            entry
        }

        unsafe fn finalize(entry: &Entry, guard: &Guard) {
            guard.defer_destroy(Shared::from(
                Self::element_of(entry) as *const _
            ));
        }
    }

    /// Checks whether the list retains inserted elements
    /// and returns them in the correct order.
    #[test]
    fn insert() {
        let collector = Collector::new();
        let handle = collector.register();
        let guard = handle.pin();

        let l: List<Entry> = List::new();

        let e1 = Owned::new(Entry::default()).into_shared(&guard);
        let e2 = Owned::new(Entry::default()).into_shared(&guard);
        let e3 = Owned::new(Entry::default()).into_shared(&guard);

        unsafe {
            l.insert(e1, &guard);
            l.insert(e2, &guard);
            l.insert(e3, &guard);
        }

        let mut iter = l.iter(&guard);
        let maybe_e3 = iter.next();
        assert!(maybe_e3.is_some());
        assert!(maybe_e3.unwrap().unwrap() as *const Entry == e3.as_raw());
        let maybe_e2 = iter.next();
        assert!(maybe_e2.is_some());
        assert!(maybe_e2.unwrap().unwrap() as *const Entry == e2.as_raw());
        let maybe_e1 = iter.next();
        assert!(maybe_e1.is_some());
        assert!(maybe_e1.unwrap().unwrap() as *const Entry == e1.as_raw());
        assert!(iter.next().is_none());

        unsafe {
            e1.as_ref().unwrap().delete(&guard);
            e2.as_ref().unwrap().delete(&guard);
            e3.as_ref().unwrap().delete(&guard);
        }
    }

    /// Checks whether elements can be removed from the list and whether
    /// the correct elements are removed.
    #[test]
    fn delete() {
        let collector = Collector::new();
        let handle = collector.register();
        let guard = handle.pin();

        let l: List<Entry> = List::new();

        let e1 = Owned::new(Entry::default()).into_shared(&guard);
        let e2 = Owned::new(Entry::default()).into_shared(&guard);
        let e3 = Owned::new(Entry::default()).into_shared(&guard);
        unsafe {
            l.insert(e1, &guard);
            l.insert(e2, &guard);
            l.insert(e3, &guard);
            e2.as_ref().unwrap().delete(&guard);
        }

        let mut iter = l.iter(&guard);
        let maybe_e3 = iter.next();
        assert!(maybe_e3.is_some());
        assert!(maybe_e3.unwrap().unwrap() as *const Entry == e3.as_raw());
        let maybe_e1 = iter.next();
        assert!(maybe_e1.is_some());
        assert!(maybe_e1.unwrap().unwrap() as *const Entry == e1.as_raw());
        assert!(iter.next().is_none());

        unsafe {
            e1.as_ref().unwrap().delete(&guard);
            e3.as_ref().unwrap().delete(&guard);
        }

        let mut iter = l.iter(&guard);
        assert!(iter.next().is_none());
    }
}

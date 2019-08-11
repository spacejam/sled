//! This module exists because lazy_static causes TSAN to
//! be very unhappy. We rely heavily on TSAN for finding
//! races, so we don't use lazy_static.

use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering::SeqCst};

/// A lazily initialized value
pub struct Lazy<T, F> {
    value: AtomicPtr<T>,
    init_mu: AtomicBool,
    init: F,
}

impl<T, F> Lazy<T, F> {
    /// Create a new Lazy
    pub const fn new(init: F) -> Lazy<T, F>
    where
        F: Sized,
    {
        Lazy {
            value: AtomicPtr::new(std::ptr::null_mut()),
            init_mu: AtomicBool::new(false),
            init,
        }
    }
}

impl<T, F> Drop for Lazy<T, F> {
    fn drop(&mut self) {
        let value_ptr = self.value.load(SeqCst);
        if !value_ptr.is_null() {
            unsafe { drop(Box::from_raw(value_ptr)) }
        }
    }
}

impl<T, F> std::ops::Deref for Lazy<T, F>
where
    F: Fn() -> T,
{
    type Target = T;

    fn deref(&self) -> &T {
        let value_ptr = self.value.load(SeqCst);

        if !value_ptr.is_null() {
            unsafe {
                return &*value_ptr;
            }
        }

        while self.init_mu.compare_and_swap(false, true, SeqCst) != false {}

        let value_ptr = self.value.load(SeqCst);

        // we need to check this again because
        // maybe some other thread completed
        // the initialization already.
        if !value_ptr.is_null() {
            let unlock = self.init_mu.swap(false, SeqCst);
            assert!(unlock);
            unsafe {
                return &*value_ptr;
            }
        }

        let value = (self.init)();

        let value_ptr = Box::into_raw(Box::new(value));

        self.value.store(value_ptr, SeqCst);

        let unlock = self.init_mu.swap(false, SeqCst);
        assert!(unlock);

        unsafe { &*value_ptr }
    }
}

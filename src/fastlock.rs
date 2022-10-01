use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{
        AtomicBool,
        Ordering::{AcqRel, Acquire, Release},
    },
};

pub struct FastLockGuard<'a, T> {
    mu: &'a FastLock<T>,
}

impl<'a, T> Drop for FastLockGuard<'a, T> {
    fn drop(&mut self) {
        assert!(self.mu.lock.swap(false, Release));
    }
}

impl<'a, T> Deref for FastLockGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        #[allow(unsafe_code)]
        unsafe {
            &*self.mu.inner.get()
        }
    }
}

impl<'a, T> DerefMut for FastLockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        #[allow(unsafe_code)]
        unsafe {
            &mut *self.mu.inner.get()
        }
    }
}

#[repr(C)]
pub struct FastLock<T> {
    inner: UnsafeCell<T>,
    lock: AtomicBool,
}

#[allow(unsafe_code)]
unsafe impl<T: Send> Sync for FastLock<T> {}

#[allow(unsafe_code)]
unsafe impl<T: Send> Send for FastLock<T> {}

impl<T> FastLock<T> {
    pub const fn new(inner: T) -> FastLock<T> {
        FastLock { lock: AtomicBool::new(false), inner: UnsafeCell::new(inner) }
    }

    pub fn try_lock(&self) -> Option<FastLockGuard<'_, T>> {
        let lock_result =
            self.lock.compare_exchange_weak(false, true, AcqRel, Acquire);

        let success = lock_result.is_ok();

        if success {
            Some(FastLockGuard { mu: self })
        } else {
            None
        }
    }
}

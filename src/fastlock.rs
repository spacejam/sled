use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{
        AtomicBool,
        Ordering::{Acquire, Release},
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

pub struct FastLock<T> {
    lock: AtomicBool,
    inner: UnsafeCell<T>,
}

impl<T> FastLock<T> {
    pub fn new(inner: T) -> FastLock<T> {
        FastLock { lock: AtomicBool::new(false), inner: UnsafeCell::new(inner) }
    }

    pub fn try_lock(&self) -> Option<FastLockGuard<'_, T>> {
        let lock_result = self.lock.compare_and_swap(false, true, Acquire);

        // `compare_and_swap` returns the last value if successful,
        // otherwise the current value. If we succeed, it should return false.
        let success = !lock_result;

        if success { Some(FastLockGuard { mu: self }) } else { None }
    }
}

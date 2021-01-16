/// Epoch-based garbage collector.
use core::fmt;
use std::sync::Arc;

use super::internal::{Global, Local};
use super::Guard;
use crate::Lazy;

/// The global data for the default garbage collector.
static COLLECTOR: Lazy<Collector, fn() -> Collector> =
    Lazy::new(Collector::new);

thread_local! {
    /// The per-thread participant for the default garbage collector.
    static HANDLE: LocalHandle = COLLECTOR.register();
}

/// Pins the current thread.
#[inline]
pub(crate) fn pin() -> Guard {
    with_handle(LocalHandle::pin)
}

#[inline]
fn with_handle<F, R>(mut f: F) -> R
where
    F: FnMut(&LocalHandle) -> R,
{
    HANDLE.try_with(|h| f(h)).unwrap_or_else(|_| f(&COLLECTOR.register()))
}

/// An epoch-based garbage collector.
pub(super) struct Collector {
    pub(super) global: Arc<Global>,
}

unsafe impl Send for Collector {}
unsafe impl Sync for Collector {}

impl Default for Collector {
    fn default() -> Self {
        Self { global: Arc::new(Global::new()) }
    }
}

impl Collector {
    /// Creates a new collector.
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Registers a new handle for the collector.
    pub(super) fn register(&self) -> LocalHandle {
        Local::register(self)
    }
}

impl Clone for Collector {
    /// Creates another reference to the same garbage collector.
    fn clone(&self) -> Self {
        Collector { global: self.global.clone() }
    }
}

/// A handle to a garbage collector.
pub(super) struct LocalHandle {
    pub(super) local: *const Local,
}

impl LocalHandle {
    /// Pins the handle.
    #[inline]
    pub(super) fn pin(&self) -> Guard {
        unsafe { (*self.local).pin() }
    }
}

impl Drop for LocalHandle {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            Local::release_handle(&*self.local);
        }
    }
}

impl fmt::Debug for LocalHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("LocalHandle { .. }")
    }
}

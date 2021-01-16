/// Epoch-based garbage collector.
///
/// # Examples
///
/// ```
/// use crossbeam_epoch::Collector;
///
/// let collector = Collector::new();
///
/// let handle = collector.register();
/// drop(collector); // `handle` still works after dropping `collector`
///
/// handle.pin().flush();
/// ```
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

#[cfg(test)]
mod tests {
    use std::mem;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crossbeam_utils::thread;

    use super::super::Owned;
    use super::Collector;

    const NUM_THREADS: usize = 8;

    #[test]
    fn pin_reentrant() {
        let collector = Collector::new();
        let handle = collector.register();
        drop(collector);

        assert!(!handle.is_pinned());
        {
            let _guard = &handle.pin();
            assert!(handle.is_pinned());
            {
                let _guard = &handle.pin();
                assert!(handle.is_pinned());
            }
            assert!(handle.is_pinned());
        }
        assert!(!handle.is_pinned());
    }

    #[test]
    fn flush_local_bag() {
        let collector = Collector::new();
        let handle = collector.register();
        drop(collector);

        for _ in 0..100 {
            let guard = &handle.pin();
            unsafe {
                let a = Owned::new(7).into_shared(guard);
                guard.defer_destroy(a);

                assert!(!(*(*guard.local).bag.get()).is_empty());

                while !(*(*guard.local).bag.get()).is_empty() {
                    guard.flush();
                }
            }
        }
    }

    #[test]
    fn garbage_buffering() {
        let collector = Collector::new();
        let handle = collector.register();
        drop(collector);

        let guard = &handle.pin();
        unsafe {
            for _ in 0..10 {
                let a = Owned::new(7).into_shared(guard);
                guard.defer_destroy(a);
            }
            assert!(!(*(*guard.local).bag.get()).is_empty());
        }
    }

    #[test]
    fn pin_holds_advance() {
        let collector = Collector::new();

        thread::scope(|scope| {
            for _ in 0..NUM_THREADS {
                scope.spawn(|_| {
                    let handle = collector.register();
                    for _ in 0..500_000 {
                        let guard = &handle.pin();

                        let before =
                            collector.global.epoch.load(Ordering::Relaxed);
                        collector.global.collect(guard);
                        let after =
                            collector.global.epoch.load(Ordering::Relaxed);

                        assert!(after.wrapping_sub(before) <= 2);
                    }
                });
            }
        })
        .unwrap();
    }

    #[test]
    fn incremental() {
        const COUNT: usize = 100_000;
        static DESTROYS: AtomicUsize = AtomicUsize::new(0);

        let collector = Collector::new();
        let handle = collector.register();

        unsafe {
            let guard = &handle.pin();
            for _ in 0..COUNT {
                let a = Owned::new(7i32).into_shared(guard);
                guard.defer_unchecked(move || {
                    drop(a.into_owned());
                    DESTROYS.fetch_add(1, Ordering::Relaxed);
                });
            }
            guard.flush();
        }

        let mut last = 0;

        while last < COUNT {
            let curr = DESTROYS.load(Ordering::Relaxed);
            assert!(curr - last <= 1024);
            last = curr;

            let guard = &handle.pin();
            collector.global.collect(guard);
        }
        assert!(DESTROYS.load(Ordering::Relaxed) == 100_000);
    }

    #[test]
    fn buffering() {
        const COUNT: usize = 10;
        static DESTROYS: AtomicUsize = AtomicUsize::new(0);

        let collector = Collector::new();
        let handle = collector.register();

        unsafe {
            let guard = &handle.pin();
            for _ in 0..COUNT {
                let a = Owned::new(7i32).into_shared(guard);
                guard.defer_unchecked(move || {
                    drop(a.into_owned());
                    DESTROYS.fetch_add(1, Ordering::Relaxed);
                });
            }
        }

        for _ in 0..100_000 {
            collector.global.collect(&handle.pin());
        }
        assert!(DESTROYS.load(Ordering::Relaxed) < COUNT);

        handle.pin().flush();

        while DESTROYS.load(Ordering::Relaxed) < COUNT {
            let guard = &handle.pin();
            collector.global.collect(guard);
        }
        assert_eq!(DESTROYS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn count_drops() {
        const COUNT: usize = 100_000;
        static DROPS: AtomicUsize = AtomicUsize::new(0);

        struct Elem(i32);

        impl Drop for Elem {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        let collector = Collector::new();
        let handle = collector.register();

        unsafe {
            let guard = &handle.pin();

            for _ in 0..COUNT {
                let a = Owned::new(Elem(7i32)).into_shared(guard);
                guard.defer_destroy(a);
            }
            guard.flush();
        }

        while DROPS.load(Ordering::Relaxed) < COUNT {
            let guard = &handle.pin();
            collector.global.collect(guard);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn count_destroy() {
        const COUNT: usize = 100_000;
        static DESTROYS: AtomicUsize = AtomicUsize::new(0);

        let collector = Collector::new();
        let handle = collector.register();

        unsafe {
            let guard = &handle.pin();

            for _ in 0..COUNT {
                let a = Owned::new(7i32).into_shared(guard);
                guard.defer_unchecked(move || {
                    drop(a.into_owned());
                    DESTROYS.fetch_add(1, Ordering::Relaxed);
                });
            }
            guard.flush();
        }

        while DESTROYS.load(Ordering::Relaxed) < COUNT {
            let guard = &handle.pin();
            collector.global.collect(guard);
        }
        assert_eq!(DESTROYS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn drop_array() {
        const COUNT: usize = 700;
        static DROPS: AtomicUsize = AtomicUsize::new(0);

        struct Elem(i32);

        impl Drop for Elem {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        let collector = Collector::new();
        let handle = collector.register();

        let mut guard = handle.pin();

        let mut v = Vec::with_capacity(COUNT);
        for i in 0..COUNT {
            v.push(Elem(i as i32));
        }

        {
            let a = Owned::new(v).into_shared(&guard);
            unsafe {
                guard.defer_destroy(a);
            }
            guard.flush();
        }

        while DROPS.load(Ordering::Relaxed) < COUNT {
            guard = handle.pin();
            collector.global.collect(&guard);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn destroy_array() {
        const COUNT: usize = 100_000;
        static DESTROYS: AtomicUsize = AtomicUsize::new(0);

        let collector = Collector::new();
        let handle = collector.register();

        unsafe {
            let guard = &handle.pin();

            let mut v = Vec::with_capacity(COUNT);
            for i in 0..COUNT {
                v.push(i as i32);
            }

            let ptr = v.as_mut_ptr() as usize;
            let len = v.len();
            guard.defer_unchecked(move || {
                drop(Vec::from_raw_parts(
                    ptr as *const i32 as *mut i32,
                    len,
                    len,
                ));
                DESTROYS.fetch_add(len, Ordering::Relaxed);
            });
            guard.flush();

            mem::forget(v);
        }

        while DESTROYS.load(Ordering::Relaxed) < COUNT {
            let guard = &handle.pin();
            collector.global.collect(guard);
        }
        assert_eq!(DESTROYS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn stress() {
        const THREADS: usize = 8;
        const COUNT: usize = 100_000;
        static DROPS: AtomicUsize = AtomicUsize::new(0);

        struct Elem(i32);

        impl Drop for Elem {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        let collector = Collector::new();

        thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|_| {
                    let handle = collector.register();
                    for _ in 0..COUNT {
                        let guard = &handle.pin();
                        unsafe {
                            let a = Owned::new(Elem(7i32)).into_shared(guard);
                            guard.defer_destroy(a);
                        }
                    }
                });
            }
        })
        .unwrap();

        let handle = collector.register();
        while DROPS.load(Ordering::Relaxed) < COUNT * THREADS {
            let guard = &handle.pin();
            collector.global.collect(guard);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), COUNT * THREADS);
    }
}

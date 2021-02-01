#![allow(unsafe_code)]
#![allow(clippy::match_like_matches_macro)]

/// This module started its life as crossbeam-epoch,
/// and was imported into the sled codebase to perform
/// a number of use-case specific runtime tests and
/// dynamic collection for when garbage spikes happen.
/// Unused functionality was stripped which further
/// improved compile times.
mod atomic;
mod collector;
mod deferred;
mod epoch;
mod internal;
mod list;
mod queue;

pub(crate) use self::{
    atomic::{Atomic, Owned, Shared},
    collector::pin,
};

use deferred::Deferred;
use internal::Local;

pub struct Guard {
    pub(super) local: *const Local,
    #[cfg(feature = "testing")]
    pub(super) began: std::time::Instant,
}

impl Guard {
    pub(crate) fn defer<F, R>(&self, f: F)
    where
        F: FnOnce() -> R + Send + 'static,
    {
        unsafe {
            self.defer_unchecked(f);
        }
    }

    pub(super) unsafe fn defer_unchecked<F, R>(&self, f: F)
    where
        F: FnOnce() -> R,
    {
        if let Some(local) = self.local.as_ref() {
            local.defer(Deferred::new(move || drop(f())), self);
        } else {
            drop(f());
        }
    }

    pub(crate) unsafe fn defer_destroy<T>(&self, ptr: Shared<'_, T>) {
        self.defer_unchecked(move || ptr.into_owned());
    }

    pub fn flush(&self) {
        if let Some(local) = unsafe { self.local.as_ref() } {
            local.flush(self);
        }
    }
}

impl Drop for Guard {
    #[inline]
    fn drop(&mut self) {
        if let Some(local) = unsafe { self.local.as_ref() } {
            local.unpin();
        }

        #[cfg(feature = "testing")]
        {
            if self.began.elapsed() > std::time::Duration::from_secs(1) {
                log::warn!("guard lived longer than allowed");
            }
        }
    }
}

#[inline]
unsafe fn unprotected() -> &'static Guard {
    // HACK(stjepang): An unprotected guard is just a `Guard` with its field `local` set to null.
    // Since this function returns a `'static` reference to a `Guard`, we must return a reference
    // to a global guard. However, it's not possible to create a `static` `Guard` because it does
    // not implement `Sync`. To get around the problem, we create a static `usize` initialized to
    // zero and then transmute it into a `Guard`. This is safe because `usize` and `Guard`
    // (consisting of a single pointer) have the same representation in memory.
    static UNPROTECTED: &[u8] = &[0; std::mem::size_of::<Guard>()];
    #[allow(trivial_casts)]
    &*(UNPROTECTED as *const _ as *const Guard)
}

use std::num::NonZeroU64;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

const SEAL_BIT: u64 = 1 << 63;
const SEAL_MASK: u64 = u64::MAX - SEAL_BIT;

#[derive(Clone, Debug)]
pub(crate) struct Completion {
    mu: Arc<Mutex<bool>>,
    cv: Arc<Condvar>,
    epoch: NonZeroU64,
}

impl Completion {
    pub fn new(epoch: NonZeroU64) -> Completion {
        Completion { mu: Default::default(), cv: Default::default(), epoch }
    }

    pub fn wait_for_complete(self) -> NonZeroU64 {
        let mut mu = self.mu.lock().unwrap();
        while !*mu {
            mu = self.cv.wait(mu).unwrap();
        }

        self.epoch
    }

    pub fn mark_complete(self) {
        self.mark_complete_inner(false);
    }

    fn mark_complete_inner(&self, previously_sealed: bool) {
        let mut mu = self.mu.lock().unwrap();
        if !previously_sealed {
            assert!(!*mu);
        }
        log::trace!("marking epoch {:?} as complete", self.epoch);
        // it's possible for *mu to already be true due to this being
        // immediately dropped in the check_in method when we see that
        // the checked-in epoch has already been marked as sealed.
        *mu = true;
        drop(mu);
        self.cv.notify_all();
    }

    #[cfg(test)]
    pub fn is_complete(&self) -> bool {
        *self.mu.lock().unwrap()
    }
}

pub(crate) struct FlushEpochGuard<'a> {
    tracker: &'a EpochTracker,
    previously_sealed: bool,
}

impl<'a> Drop for FlushEpochGuard<'a> {
    fn drop(&mut self) {
        let rc = self.tracker.rc.fetch_sub(1, Ordering::Release) - 1;
        if rc & SEAL_MASK == 0 && (rc & SEAL_BIT) == SEAL_BIT {
            self.tracker
                .vacancy_notifier
                .mark_complete_inner(self.previously_sealed);
        }
    }
}

impl<'a> FlushEpochGuard<'a> {
    pub fn epoch(&self) -> NonZeroU64 {
        self.tracker.epoch
    }
}

#[derive(Debug)]
pub(crate) struct EpochTracker {
    epoch: NonZeroU64,
    rc: AtomicU64,
    vacancy_notifier: Completion,
    previous_flush_complete: Completion,
}

#[derive(Clone, Debug)]
pub(crate) struct FlushEpoch {
    active_ebr: ebr::Ebr<Box<EpochTracker>>,
    inner: Arc<FlushEpochInner>,
}

#[derive(Debug)]
pub(crate) struct FlushEpochInner {
    counter: AtomicU64,
    roll_mu: Mutex<()>,
    current_active: AtomicPtr<EpochTracker>,
}

impl Drop for FlushEpochInner {
    fn drop(&mut self) {
        let vacancy_mu = self.roll_mu.lock().unwrap();
        let old_ptr =
            self.current_active.swap(std::ptr::null_mut(), Ordering::Release);
        if !old_ptr.is_null() {
            //let old: &EpochTracker = &*old_ptr;
            unsafe { drop(Box::from_raw(old_ptr)) }
        }
        drop(vacancy_mu);
    }
}

impl Default for FlushEpoch {
    fn default() -> FlushEpoch {
        let last = Completion::new(NonZeroU64::new(1).unwrap());
        let current_active_ptr = Box::into_raw(Box::new(EpochTracker {
            epoch: NonZeroU64::new(2).unwrap(),
            rc: AtomicU64::new(0),
            vacancy_notifier: Completion::new(NonZeroU64::new(2).unwrap()),
            previous_flush_complete: last.clone(),
        }));

        last.mark_complete();

        let current_active = AtomicPtr::new(current_active_ptr);

        FlushEpoch {
            inner: Arc::new(FlushEpochInner {
                counter: AtomicU64::new(2),
                roll_mu: Mutex::new(()),
                current_active,
            }),
            active_ebr: ebr::Ebr::default(),
        }
    }
}

impl FlushEpoch {
    /// Returns the epoch notifier for the previous epoch.
    /// Intended to be passed to a flusher that can eventually
    /// notify the flush-requesting thread.
    pub fn roll_epoch_forward(&self) -> (Completion, Completion, Completion) {
        let mut guard = self.active_ebr.pin();
        let vacancy_mu = self.inner.roll_mu.lock().unwrap();
        let flush_through = self.inner.counter.fetch_add(1, Ordering::Release);
        let new_epoch = NonZeroU64::new(flush_through + 1).unwrap();
        let forward_flush_notifier =
            Completion::new(NonZeroU64::new(u64::MAX).unwrap());
        let new_active = Box::into_raw(Box::new(EpochTracker {
            epoch: new_epoch,
            rc: AtomicU64::new(0),
            vacancy_notifier: Completion::new(new_epoch),
            previous_flush_complete: forward_flush_notifier.clone(),
        }));
        let old_ptr =
            self.inner.current_active.swap(new_active, Ordering::Release);
        assert!(!old_ptr.is_null());

        let (last_flush_complete_notifier, vacancy_notifier) = unsafe {
            let old: &EpochTracker = &*old_ptr;
            let last = old.rc.fetch_add(SEAL_BIT + 1, Ordering::SeqCst);

            assert_eq!(
                last & SEAL_BIT,
                0,
                "epoch {} double-sealed",
                flush_through
            );

            // mark_complete_inner called via drop in a uniform way
            drop(FlushEpochGuard { tracker: old, previously_sealed: true });

            (old.previous_flush_complete.clone(), old.vacancy_notifier.clone())
        };
        guard.defer_drop(unsafe { Box::from_raw(old_ptr) });
        drop(vacancy_mu);
        (last_flush_complete_notifier, vacancy_notifier, forward_flush_notifier)
    }

    pub fn check_in<'a>(&self) -> FlushEpochGuard<'a> {
        loop {
            let tracker: &'a EpochTracker =
                unsafe { &*self.inner.current_active.load(Ordering::Acquire) };
            let rc = tracker.rc.fetch_add(1, Ordering::Release);

            let previously_sealed = rc & SEAL_BIT == SEAL_BIT;

            let guard = FlushEpochGuard { tracker, previously_sealed };

            if previously_sealed {
                // the epoch is already closed, so we must drop the rc
                // and possibly notify, which is handled in the guard's
                // Drop impl.
                drop(guard);
            } else {
                return guard;
            }
        }
    }
}

#[test]
fn flush_epoch_basic_functionality() {
    let fa = FlushEpoch::default();

    let g1 = fa.check_in();
    let g2 = fa.check_in();

    assert_eq!(g1.tracker.epoch.get(), 2);
    assert_eq!(g2.tracker.epoch.get(), 2);

    let notifier = fa.roll_epoch_forward().1;
    assert!(!notifier.is_complete());

    drop(g1);
    assert!(!notifier.is_complete());
    drop(g2);
    assert_eq!(notifier.wait_for_complete().get(), 2);

    let g3 = fa.check_in();
    assert_eq!(g3.tracker.epoch.get(), 3);

    let notifier_2 = fa.roll_epoch_forward().1;

    drop(g3);

    assert_eq!(notifier_2.wait_for_complete().get(), 3);
}

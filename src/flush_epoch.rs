use std::num::NonZeroU64;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

const SEAL_BIT: u64 = 1 << 63;
const SEAL_MASK: u64 = u64::MAX - SEAL_BIT;

#[derive(
    Debug,
    Clone,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Hash,
)]
pub(crate) struct FlushEpoch(NonZeroU64);

impl FlushEpoch {
    pub fn increment(&self) -> FlushEpoch {
        FlushEpoch(NonZeroU64::new(self.0.get() + 1).unwrap())
    }
}

impl concurrent_map::Minimum for FlushEpoch {
    const MIN: FlushEpoch = FlushEpoch(NonZeroU64::MIN);
}

#[derive(Clone, Debug)]
pub(crate) struct Completion {
    mu: Arc<Mutex<bool>>,
    cv: Arc<Condvar>,
    epoch: FlushEpoch,
}

impl Completion {
    pub fn new(epoch: FlushEpoch) -> Completion {
        Completion { mu: Default::default(), cv: Default::default(), epoch }
    }

    pub fn wait_for_complete(self) -> FlushEpoch {
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
            // TODO reevaluate - assert!(!*mu);
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
        let rc = self.tracker.rc.fetch_sub(1, Ordering::SeqCst) - 1;
        if rc & SEAL_MASK == 0 && (rc & SEAL_BIT) == SEAL_BIT {
            crate::debug_delay();
            self.tracker
                .vacancy_notifier
                .mark_complete_inner(self.previously_sealed);
        }
    }
}

impl<'a> FlushEpochGuard<'a> {
    pub fn epoch(&self) -> FlushEpoch {
        self.tracker.epoch
    }
}

#[derive(Debug)]
pub(crate) struct EpochTracker {
    epoch: FlushEpoch,
    rc: AtomicU64,
    vacancy_notifier: Completion,
    previous_flush_complete: Completion,
}

#[derive(Clone, Debug)]
pub(crate) struct FlushEpochTracker {
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
            self.current_active.swap(std::ptr::null_mut(), Ordering::SeqCst);
        if !old_ptr.is_null() {
            //let old: &EpochTracker = &*old_ptr;
            unsafe { drop(Box::from_raw(old_ptr)) }
        }
        drop(vacancy_mu);
    }
}

impl Default for FlushEpochTracker {
    fn default() -> FlushEpochTracker {
        let last = Completion::new(FlushEpoch(NonZeroU64::new(1).unwrap()));
        let current_active_ptr = Box::into_raw(Box::new(EpochTracker {
            epoch: FlushEpoch(NonZeroU64::new(2).unwrap()),
            rc: AtomicU64::new(0),
            vacancy_notifier: Completion::new(FlushEpoch(
                NonZeroU64::new(2).unwrap(),
            )),
            previous_flush_complete: last.clone(),
        }));

        last.mark_complete();

        let current_active = AtomicPtr::new(current_active_ptr);

        FlushEpochTracker {
            inner: Arc::new(FlushEpochInner {
                counter: AtomicU64::new(2),
                roll_mu: Mutex::new(()),
                current_active,
            }),
            active_ebr: ebr::Ebr::default(),
        }
    }
}

impl FlushEpochTracker {
    /// Returns the epoch notifier for the previous epoch.
    /// Intended to be passed to a flusher that can eventually
    /// notify the flush-requesting thread.
    pub fn roll_epoch_forward(&self) -> (Completion, Completion, Completion) {
        let mut tracker_guard = self.active_ebr.pin();
        let vacancy_mu = self.inner.roll_mu.lock().unwrap();
        let flush_through = self.inner.counter.fetch_add(1, Ordering::SeqCst);
        let new_epoch = FlushEpoch(NonZeroU64::new(flush_through + 1).unwrap());
        let forward_flush_notifier =
            Completion::new(FlushEpoch(NonZeroU64::new(u64::MAX).unwrap()));
        let new_active = Box::into_raw(Box::new(EpochTracker {
            epoch: new_epoch,
            rc: AtomicU64::new(0),
            vacancy_notifier: Completion::new(new_epoch),
            previous_flush_complete: forward_flush_notifier.clone(),
        }));
        let old_ptr =
            self.inner.current_active.swap(new_active, Ordering::SeqCst);
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
            //println!("dropping flush epoch guard for epoch {flush_through}");
            drop(FlushEpochGuard { tracker: old, previously_sealed: true });

            (old.previous_flush_complete.clone(), old.vacancy_notifier.clone())
        };
        tracker_guard.defer_drop(unsafe { Box::from_raw(old_ptr) });
        drop(vacancy_mu);
        (last_flush_complete_notifier, vacancy_notifier, forward_flush_notifier)
    }

    pub fn check_in<'a>(&self) -> FlushEpochGuard<'a> {
        let mut tracker_guard = self.active_ebr.pin();
        loop {
            let tracker: &'a EpochTracker =
                unsafe { &*self.inner.current_active.load(Ordering::SeqCst) };

            let rc = tracker.rc.fetch_add(1, Ordering::SeqCst);

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
    let fa = FlushEpochTracker::default();

    let g1 = fa.check_in();
    let g2 = fa.check_in();

    assert_eq!(g1.tracker.epoch.0.get(), 2);
    assert_eq!(g2.tracker.epoch.0.get(), 2);

    let notifier = fa.roll_epoch_forward().1;
    assert!(!notifier.is_complete());

    drop(g1);
    assert!(!notifier.is_complete());
    drop(g2);
    assert_eq!(notifier.wait_for_complete().0.get(), 2);

    let g3 = fa.check_in();
    assert_eq!(g3.tracker.epoch.0.get(), 3);

    let notifier_2 = fa.roll_epoch_forward().1;

    drop(g3);

    assert_eq!(notifier_2.wait_for_complete().0.get(), 3);
}

#[cfg(test)]
fn concurrent_flush_epoch_burn_in_inner() {
    let fa = FlushEpochTracker::default();

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(21));

    let pt = pagetable::PageTable::<AtomicU64>::default();

    let rolls = || {
        let fa = fa.clone();
        let barrier = barrier.clone();
        let pt = &pt;
        move || {
            barrier.wait();
            for _ in 0..3000 {
                let (previous, this, next) = fa.roll_epoch_forward();
                let last_epoch = previous.wait_for_complete().0.get();
                assert_eq!(0, pt.get(last_epoch).load(Ordering::Acquire));
                let flush_through_epoch = this.wait_for_complete().0.get();
                assert_eq!(
                    0,
                    pt.get(flush_through_epoch).load(Ordering::Acquire)
                );

                next.mark_complete();
            }
        }
    };

    let check_ins = || {
        let fa = fa.clone();
        let barrier = barrier.clone();
        let pt = &pt;
        move || {
            barrier.wait();
            for _ in 0..3000 {
                let guard = fa.check_in();
                let epoch = guard.epoch().0.get();
                pt.get(epoch).fetch_add(1, Ordering::SeqCst);
                std::thread::yield_now();
                pt.get(epoch).fetch_sub(1, Ordering::SeqCst);
                drop(guard);
            }
        }
    };

    std::thread::scope(|s| {
        let mut threads = vec![];

        for _ in 0..10 {
            threads.push(s.spawn(rolls()));
            threads.push(s.spawn(check_ins()));
        }

        barrier.wait();

        for thread in threads.into_iter() {
            thread.join().expect("a test thread crashed unexpectedly");
        }
    });
}

#[test]
fn concurrent_flush_epoch_burn_in() {
    for _ in 0..128 {
        concurrent_flush_epoch_burn_in_inner();
    }
}

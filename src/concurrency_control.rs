#[cfg(feature = "testing")]
use std::cell::RefCell;
use std::sync::atomic::AtomicBool;

use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

use super::*;

#[cfg(feature = "testing")]
thread_local! {
    pub static COUNT: RefCell<u32> = RefCell::new(0);
}

const RW_REQUIRED_BIT: usize = 1 << 31;

#[derive(Default)]
pub(crate) struct ConcurrencyControl {
    active: AtomicUsize,
    upgrade_complete: AtomicBool,
    rw: RwLock<()>,
}

static CONCURRENCY_CONTROL: Lazy<
    ConcurrencyControl,
    fn() -> ConcurrencyControl,
> = Lazy::new(init_cc);

fn init_cc() -> ConcurrencyControl {
    ConcurrencyControl::default()
}

#[derive(Debug)]
#[must_use]
pub(crate) enum Protector<'a> {
    Write(RwLockWriteGuard<'a, ()>),
    Read(RwLockReadGuard<'a, ()>),
    None(&'a AtomicUsize),
}

impl<'a> Drop for Protector<'a> {
    fn drop(&mut self) {
        if let Protector::None(active) = self {
            active.fetch_sub(1, Release);
        }
        #[cfg(feature = "testing")]
        COUNT.with(|c| {
            let mut c = c.borrow_mut();
            *c -= 1;
            assert_eq!(*c, 0);
        });
    }
}

pub(crate) fn read<'a>() -> Protector<'a> {
    CONCURRENCY_CONTROL.read()
}

pub(crate) fn write<'a>() -> Protector<'a> {
    CONCURRENCY_CONTROL.write()
}

impl ConcurrencyControl {
    fn enable(&self) {
        if self.active.fetch_or(RW_REQUIRED_BIT, SeqCst) < RW_REQUIRED_BIT {
            // we are the first to set this bit
            while self.active.load(Acquire) != RW_REQUIRED_BIT {
                std::sync::atomic::spin_loop_hint()
            }
            self.upgrade_complete.store(true, Release);
        }
    }

    fn read(&self) -> Protector<'_> {
        #[cfg(feature = "testing")]
        COUNT.with(|c| {
            let mut c = c.borrow_mut();
            *c += 1;
            assert_eq!(*c, 1);
        });

        let active = self.active.fetch_add(1, Release);

        if active >= RW_REQUIRED_BIT {
            self.active.fetch_sub(1, Release);
            Protector::Read(self.rw.read())
        } else {
            Protector::None(&self.active)
        }
    }

    fn write(&self) -> Protector<'_> {
        #[cfg(feature = "testing")]
        COUNT.with(|c| {
            let mut c = c.borrow_mut();
            *c += 1;
            assert_eq!(*c, 1);
        });
        self.enable();
        while !self.upgrade_complete.load(Acquire) {
            std::sync::atomic::spin_loop_hint()
        }
        Protector::Write(self.rw.write())
    }
}

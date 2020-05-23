use std::sync::atomic::AtomicBool;

use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

use super::*;

#[derive(Default)]
pub(crate) struct ConcurrencyControl {
    necessary: AtomicBool,
    active_non_lockers: AtomicUsize,
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

pub(crate) enum Protector<'a> {
    Write(RwLockWriteGuard<'a, ()>),
    Read(RwLockReadGuard<'a, ()>),
    None(&'a AtomicUsize),
}

impl<'a> Drop for Protector<'a> {
    fn drop(&mut self) {
        if let Protector::None(active_non_lockers) = self {
            active_non_lockers.fetch_sub(1, Release);
        }
    }
}

pub(crate) fn read<'a>(guard: &'a Guard) -> Protector<'a> {
    CONCURRENCY_CONTROL.read(guard)
}

pub(crate) fn write<'a>() -> Protector<'a> {
    CONCURRENCY_CONTROL.write()
}

impl ConcurrencyControl {
    fn enable(&self) {
        if !self.necessary.load(Acquire) && !self.necessary.swap(true, SeqCst) {
            while self.active_non_lockers.load(Acquire) != 0 {
                std::sync::atomic::spin_loop_hint()
            }
            self.upgrade_complete.store(true, Release);
        }
    }

    fn read<'a>(&'a self, _: &'a Guard) -> Protector<'a> {
        if self.necessary.load(Acquire) {
            Protector::Read(self.rw.read())
        } else {
            self.active_non_lockers.fetch_add(1, Release);
            Protector::None(&self.active_non_lockers)
        }
    }

    fn write(&self) -> Protector<'_> {
        self.enable();
        while !self.upgrade_complete.load(Acquire) {
            std::sync::atomic::spin_loop_hint()
        }
        Protector::Write(self.rw.write())
    }
}

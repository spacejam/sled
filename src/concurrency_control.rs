use std::sync::atomic::AtomicBool;

use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

use super::*;

#[derive(Default)]
pub(crate) struct ConcurrencyControl {
    necessary: AtomicBool,
    upgrade_complete: AtomicBool,
    rw: RwLock<()>,
}

pub(crate) enum Protector<'a> {
    Write(RwLockWriteGuard<'a, ()>),
    Read(RwLockReadGuard<'a, ()>),
    None,
}

impl ConcurrencyControl {
    fn enable(&self) {
        if !self.necessary.load(Acquire) && !self.necessary.swap(true, SeqCst) {
            // upgrade the system to using transactional
            // concurrency control, which is a little
            // more expensive for every operation.
            let (tx, rx) = std::sync::mpsc::channel();

            let guard = pin();
            guard.defer(move || tx.send(()).unwrap());
            guard.flush();
            drop(guard);

            rx.recv().unwrap();
            self.upgrade_complete.store(true, Release);
        }
    }

    pub(crate) fn read<'a>(&'a self, _: &'a Guard) -> Protector<'a> {
        if self.necessary.load(Acquire) {
            Protector::Read(self.rw.read())
        } else {
            Protector::None
        }
    }

    pub(crate) fn write(&self) -> Protector<'_> {
        self.enable();
        while !self.upgrade_complete.load(Acquire) {
            std::sync::atomic::spin_loop_hint()
        }
        Protector::Write(self.rw.write())
    }
}

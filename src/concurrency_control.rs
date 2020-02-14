use std::sync::atomic::AtomicBool;

use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

use super::*;

#[derive(Default)]
pub(crate) struct ConcurrencyControl {
    necessary: AtomicBool,
    rw: RwLock<()>,
}

pub(crate) enum Protector<'a> {
    Write(RwLockWriteGuard<'a, ()>),
    Read(RwLockReadGuard<'a, ()>),
    None,
}

impl ConcurrencyControl {
    fn enable(&self) {
        if !self.necessary.swap(true, SeqCst) {
            // upgrade the system to using transactional
            // concurrency control, which is a little
            // more expensive for every operation.
            let (tx, rx) = std::sync::mpsc::channel();

            let guard = pin();
            guard.defer(move || tx.send(()).unwrap());
            guard.flush();
            drop(guard);

            rx.recv().unwrap();
        }
    }

    pub(crate) fn read<'a>(&'a self, _: &'a Guard) -> Protector<'a> {
        if self.necessary.load(SeqCst) {
            Protector::Read(self.rw.read())
        } else {
            Protector::None
        }
    }

    pub(crate) fn write(&self) -> Protector<'_> {
        self.enable();
        Protector::Write(self.rw.write())
    }
}

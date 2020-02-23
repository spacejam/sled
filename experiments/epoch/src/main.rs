/// A simple implementation of epoch-based reclamation.
///
/// Using the `pin` method, a thread checks into an epoch
/// before operating on a shared resource. If that thread
/// makes a shared resource inaccessible, it can defer its
/// destruction until all threads that may have already
/// checked in have moved on.
use std::{
    cell::RefCell,
    sync::{
        atomic::{AtomicPtr, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

const EPOCH_SZ: usize = 16;

#[derive(Default)]
struct Epoch {
    garbage: [AtomicPtr<Box<dyn FnOnce()>>; EPOCH_SZ],
    offset: AtomicUsize,
    next: AtomicPtr<Epoch>,
    id: u64,
}

impl Drop for Epoch {
    fn drop(&mut self) {
        let count = std::cmp::min(EPOCH_SZ, self.offset.load(SeqCst));
        for offset in 0..count {
            let mut garbage_ptr: *mut Box<dyn FnOnce()> =
                self.garbage[offset].load(SeqCst);
            while garbage_ptr.is_null() {
                // maybe this is impossible, but this is to
                // be defensive against race conditions.
                garbage_ptr = self.garbage[offset].load(SeqCst);
            }

            let garbage: Box<Box<dyn FnOnce()>> =
                unsafe { Box::from_raw(garbage_ptr) };

            drop(garbage);
        }

        let next = self.next.swap(std::ptr::null_mut(), SeqCst);
        if !next.is_null() {
            let arc = unsafe { Arc::from_raw(next) };
            drop(arc);
        }
    }
}

struct Collector {
    head: AtomicPtr<Epoch>,
}

unsafe impl Send for Collector {}
unsafe impl Sync for Collector {}

impl Default for Collector {
    fn default() -> Collector {
        let ptr = Arc::into_raw(Arc::new(Epoch::default())) as *mut Epoch;
        Collector { head: AtomicPtr::new(ptr) }
    }
}

impl Collector {
    fn pin(&self) -> Guard {
        let head_ptr = self.head.load(SeqCst);
        assert!(!head_ptr.is_null());
        let mut head = unsafe { Arc::from_raw(head_ptr) };
        let mut next = head.next.load(SeqCst);
        let mut last_head = head_ptr;

        // forward head to current tip
        while !next.is_null() {
            std::mem::forget(head);

            let res = self.head.compare_and_swap(last_head, next, SeqCst);
            if res == last_head {
                head = unsafe { Arc::from_raw(next) };
                last_head = next;
            } else {
                head = unsafe { Arc::from_raw(res) };
                last_head = res;
            }

            next = head.next.load(SeqCst);
        }

        let (a1, a2) = (head.clone(), head.clone());
        std::mem::forget(head);

        Guard {
            _entry_epoch: a1,
            current_epoch: a2,
            trash_sack: RefCell::new(vec![]),
        }
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        let head_ptr = self.head.load(SeqCst);
        assert!(!head_ptr.is_null());
        unsafe {
            let head = Arc::from_raw(head_ptr);
            drop(head);
        }
    }
}

pub(crate) struct Guard {
    _entry_epoch: Arc<Epoch>,
    current_epoch: Arc<Epoch>,
    trash_sack: RefCell<Vec<*mut Box<dyn FnOnce()>>>,
}

impl Guard {
    pub fn defer<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let garbage_ptr =
            Box::into_raw(Box::new(Box::new(f) as Box<dyn FnOnce()>));
        let mut trash_sack = self.trash_sack.borrow_mut();
        trash_sack.push(garbage_ptr);
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let trash_sack = self.trash_sack.replace(vec![]);

        for garbage_ptr in trash_sack.into_iter() {
            // try to reserve
            let mut offset = self.current_epoch.offset.fetch_add(1, SeqCst);
            while offset >= EPOCH_SZ {
                let next = self.current_epoch.next.load(SeqCst);
                if !next.is_null() {
                    unsafe {
                        let raced_arc = Arc::from_raw(next);
                        self.current_epoch = raced_arc.clone();
                        std::mem::forget(raced_arc);
                    }
                    offset = self.current_epoch.offset.fetch_add(1, SeqCst);
                    continue;
                }

                // push epoch forward if we're full
                let mut next_epoch = Epoch::default();
                next_epoch.id = self.current_epoch.id + 1;

                let next_epoch_arc = Arc::new(next_epoch);
                let next_ptr =
                    Arc::into_raw(next_epoch_arc.clone()) as *mut Epoch;
                let old = self.current_epoch.next.compare_and_swap(
                    std::ptr::null_mut(),
                    next_ptr,
                    SeqCst,
                );
                if old != std::ptr::null_mut() {
                    // somebody else already installed a new segment
                    unsafe {
                        let unneeded = Arc::from_raw(next_ptr);
                        drop(unneeded);

                        let raced_arc = Arc::from_raw(old);
                        self.current_epoch = raced_arc.clone();
                        std::mem::forget(raced_arc);
                    }
                    offset = self.current_epoch.offset.fetch_add(1, SeqCst);
                    continue;
                }

                self.current_epoch = next_epoch_arc;
                offset = self.current_epoch.offset.fetch_add(1, SeqCst);
            }

            let old =
                self.current_epoch.garbage[offset].swap(garbage_ptr, SeqCst);
            assert!(old.is_null());
        }
    }
}

#[derive(Debug)]
struct S(usize);

fn main() {
    let collector = Arc::new(Collector::default());

    let mut threads = vec![];

    for t in 0..100 {
        use std::thread::spawn;

        let collector = collector.clone();
        let thread = spawn(move || {
            for _ in 0..1000000 {
                let guard = collector.pin();
                guard.defer(move || {
                    S(t as usize);
                });

                let guard = crossbeam_epoch::pin();
                guard.defer(move || {
                    S(t as usize);
                });
            }
        });

        threads.push(thread);
    }

    for thread in threads.into_iter() {
        thread.join().unwrap();
    }
}

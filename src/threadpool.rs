//! A simple adaptive threadpool that returns a oneshot future.

use std::sync::Arc;

use crate::{OneShot, Result};

#[cfg(not(miri))]
mod queue {
    use std::{
        cell::RefCell,
        collections::VecDeque,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Once,
        },
        time::{Duration, Instant},
    };

    use parking_lot::{Condvar, Mutex};

    use crate::{debug_delay, Lazy, OneShot};

    thread_local! {
        static WORKER: RefCell<bool> = RefCell::new(false);
    }

    fn is_worker() -> bool {
        WORKER.with(|w| *w.borrow())
    }

    pub(super) static BLOCKING_QUEUE: Lazy<Queue, fn() -> Queue> =
        Lazy::new(Default::default);
    pub(super) static IO_QUEUE: Lazy<Queue, fn() -> Queue> =
        Lazy::new(Default::default);
    pub(super) static SNAPSHOT_QUEUE: Lazy<Queue, fn() -> Queue> =
        Lazy::new(Default::default);
    pub(super) static TRUNCATE_QUEUE: Lazy<Queue, fn() -> Queue> =
        Lazy::new(Default::default);

    type Work = Box<dyn FnOnce() + Send + 'static>;

    pub(super) fn spawn_to<F, R>(work: F, queue: &'static Queue) -> OneShot<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static + Sized,
    {
        static START_THREADS: Once = Once::new();

        START_THREADS.call_once(|| {
            std::thread::Builder::new()
                .name("sled-io-thread".into())
                .spawn(|| IO_QUEUE.perform_work(true, false))
                .expect("failed to spawn critical IO thread");

            std::thread::Builder::new()
                .name("sled-blocking-thread".into())
                .spawn(|| BLOCKING_QUEUE.perform_work(true, false))
                .expect("failed to spawn critical blocking thread");

            std::thread::Builder::new()
                .name("sled-snapshot-thread".into())
                .spawn(|| SNAPSHOT_QUEUE.perform_work(true, false))
                .expect("failed to spawn critical snapshot thread");

            std::thread::Builder::new()
                .name("sled-truncate-thread".into())
                .spawn(|| TRUNCATE_QUEUE.perform_work(false, false))
                .expect("failed to spawn critical truncation thread");
        });

        let (promise_filler, promise) = OneShot::pair();
        let task = move || {
            promise_filler.fill((work)());
        };

        if is_worker() {
            // NB this could prevent deadlocks because
            // if a threadpool thread spawns work into
            // the threadpool's queue, which it later
            // blocks on the completion of, it would be
            // possible for threadpool threads to block
            // forever on the completion of work that
            // exists in the queue but will never be
            // scheduled.
            task();
        } else {
            queue.send(Box::new(task));
        }

        promise
    }

    #[derive(Default)]
    pub(super) struct Queue {
        cv: Condvar,
        mu: Mutex<VecDeque<Work>>,
        temporary_threads: AtomicUsize,
        spawning: AtomicBool,
    }

    #[allow(unsafe_code)]
    unsafe impl Send for Queue {}

    impl Queue {
        fn recv_timeout(&self, duration: Duration) -> Option<(Work, usize)> {
            let mut queue = self.mu.lock();

            let cutoff = Instant::now() + duration;

            while queue.is_empty() {
                let res = self.cv.wait_until(&mut queue, cutoff);
                if res.timed_out() {
                    break;
                }
            }

            queue.pop_front().map(|w| (w, queue.len()))
        }

        fn send(&self, work: Work) -> usize {
            let mut queue = self.mu.lock();
            queue.push_back(work);

            let len = queue.len();

            // having held the mutex makes this linearized
            // with the notify below.
            drop(queue);

            self.cv.notify_all();

            len
        }

        fn perform_work(&'static self, elastic: bool, temporary: bool) {
            const MAX_TEMPORARY_THREADS: usize = 16;

            WORKER.with(|w| *w.borrow_mut() = true);

            self.spawning.store(false, Ordering::SeqCst);

            let wait_limit = Duration::from_millis(100);

            let mut unemployed_loops = 0;
            while !temporary || unemployed_loops < 50 {
                // take on a bit of GC labor
                let guard = crate::pin();
                guard.flush();
                drop(guard);

                debug_delay();
                let task_opt = self.recv_timeout(wait_limit);

                if let Some((task, outstanding_work)) = task_opt {
                    // execute the work sent to this thread
                    (task)();

                    // spin up some help if we're falling behind

                    let temporary_threads =
                        self.temporary_threads.load(Ordering::Acquire);

                    if elastic
                        && outstanding_work > 5
                        && temporary_threads < MAX_TEMPORARY_THREADS
                        && self
                            .spawning
                            .compare_exchange(
                                false,
                                true,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .is_ok()
                    {
                        self.temporary_threads.fetch_add(1, Ordering::SeqCst);
                        let spawn_res = std::thread::Builder::new()
                            .name("sled-temporary-thread".into())
                            .spawn(move || self.perform_work(false, true));
                        if let Err(e) = spawn_res {
                            log::error!(
                                "failed to spin-up temporary work thread: {:?}",
                                e
                            );
                            self.temporary_threads
                                .fetch_sub(1, Ordering::SeqCst);
                        }
                    }

                    unemployed_loops = 0;
                } else {
                    unemployed_loops += 1;
                }
            }

            assert!(temporary);
            self.temporary_threads.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// Spawn a function on the threadpool.
pub fn spawn<F, R>(work: F) -> OneShot<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static + Sized,
{
    spawn_to(work, &queue::BLOCKING_QUEUE)
}

#[cfg(miri)]
mod queue {
    /// This is the polyfill that just executes things synchronously.
    use crate::{OneShot, Result};

    pub(super) fn spawn_to<F, R>(work: F, _: &()) -> OneShot<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        // Polyfill for platforms other than those we explicitly trust to
        // perform threaded work on. Just execute a task without involving threads.
        let (promise_filler, promise) = OneShot::pair();
        promise_filler.fill((work)());
        promise
    }

    pub(super) const IO_QUEUE: () = ();
    pub(super) const BLOCKING_QUEUE: () = ();
    pub(super) const SNAPSHOT_QUEUE: () = ();
    pub(super) const TRUNCATE_QUEUE: () = ();
}

use queue::spawn_to;

pub fn truncate(config: crate::RunningConfig, at: u64) -> OneShot<Result<()>> {
    spawn_to(
        move || {
            log::debug!("truncating file to length {}", at);
            let ret: Result<()> = config
                .file
                .set_len(at)
                .and_then(|_| config.file.sync_all())
                .map_err(Into::into);

            if let Err(e) = &ret {
                config.set_global_error(*e);
            }

            ret
        },
        &queue::TRUNCATE_QUEUE,
    )
}

pub fn take_fuzzy_snapshot(pc: crate::pagecache::PageCache) -> OneShot<()> {
    spawn_to(
        move || {
            if let Err(e) = pc.take_fuzzy_snapshot() {
                log::error!("failed to write snapshot: {:?}", e);
                pc.log.iobufs.set_global_error(e);
            }
        },
        &queue::SNAPSHOT_QUEUE,
    )
}

pub(crate) fn write_to_log(
    iobuf: Arc<crate::pagecache::iobuf::IoBuf>,
    iobufs: Arc<crate::pagecache::iobuf::IoBufs>,
) -> OneShot<()> {
    spawn_to(
        move || {
            let lsn = iobuf.lsn;
            if let Err(e) = iobufs.write_to_log(iobuf) {
                log::error!(
                    "hit error while writing iobuf with lsn {}: {:?}",
                    lsn,
                    e
                );

                // store error before notifying so that waiting threads will see
                // it
                iobufs.set_global_error(e);
            }
        },
        &queue::IO_QUEUE,
    )
}

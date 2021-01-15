//! A simple adaptive threadpool that returns a oneshot future.

use std::{
    collections::VecDeque,
    sync::{Arc, Once},
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};

use crate::{debug_delay, Lazy, OneShot, Result};

static BLOCKING_QUEUE: Lazy<Queue, fn() -> Queue> = Lazy::new(Default::default);
static IO_QUEUE: Lazy<Queue, fn() -> Queue> = Lazy::new(Default::default);
static SNAPSHOT_QUEUE: Lazy<Queue, fn() -> Queue> = Lazy::new(Default::default);

type Work = Box<dyn FnOnce() + Send + 'static>;

#[derive(Default)]
struct Queue {
    cv: Condvar,
    mu: Mutex<VecDeque<Work>>,
}

#[allow(unsafe_code)]
unsafe impl Send for Queue {}

impl Queue {
    fn recv_timeout(&self, duration: Duration) -> Option<Work> {
        let mut queue = self.mu.lock();

        let cutoff = Instant::now() + duration;

        while queue.is_empty() {
            let res = self.cv.wait_until(&mut queue, cutoff);
            if res.timed_out() {
                break;
            }
        }

        queue.pop_front()
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

    fn perform_work(&self) {
        let wait_limit = Duration::from_millis(100);

        loop {
            let guard = crate::pin();
            guard.flush();
            drop(guard);
            debug_delay();
            let task_res = self.recv_timeout(wait_limit);

            if let Some(task) = task_res {
                //println!("starting work");
                (task)();
                //println!("done work");
            }
        }
    }
}

/// Spawn a function on the threadpool.
pub fn spawn<F, R>(work: F) -> Result<OneShot<R>>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static + Sized,
{
    spawn_to(work, &BLOCKING_QUEUE)
}

#[cfg(all(
    not(miri),
    any(
        windows,
        target_os = "linux",
        target_os = "macos",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
    )
))]
fn spawn_to<F, R>(work: F, queue: &'static Queue) -> Result<OneShot<R>>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static + Sized,
{
    static START_THREADS: Once = Once::new();

    START_THREADS.call_once(|| {
        std::thread::Builder::new()
            .name("sled-io-thread".into())
            .spawn(|| IO_QUEUE.perform_work())
            .expect("failed to spawn critical IO thread");

        std::thread::Builder::new()
            .name("sled-blocking-thread".into())
            .spawn(|| BLOCKING_QUEUE.perform_work())
            .expect("failed to spawn critical blocking thread");

        std::thread::Builder::new()
            .name("sled-snapshot-thread".into())
            .spawn(|| SNAPSHOT_QUEUE.perform_work())
            .expect("failed to spawn critical snapshot thread");
    });

    let (promise_filler, promise) = OneShot::pair();
    let task = move || {
        promise_filler.fill((work)());
    };

    queue.send(Box::new(task));

    Ok(promise)
}

#[cfg(any(
    miri,
    not(any(
        windows,
        target_os = "linux",
        target_os = "macos",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
    ))
))]
pub fn spawn_to<F, R>(work: F, _: &'static Queue) -> Result<OneShot<R>>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    // Polyfill for platforms other than those we explicitly trust to
    // perform threaded work on. Just execute a task without involving threads.
    let (promise_filler, promise) = OneShot::pair();
    promise_filler.fill((work)());
    Ok(promise)
}

pub fn truncate(
    config: crate::RunningConfig,
    at: u64,
) -> Result<OneShot<Result<()>>> {
    spawn_to(
        move || {
            log::debug!("truncating file to length {}", at);
            config
                .file
                .set_len(at)
                .and_then(|_| config.file.sync_all())
                .map_err(|e| e.into())
        },
        &IO_QUEUE,
    )
}

pub fn take_fuzzy_snapshot(
    pc: crate::pagecache::PageCache,
) -> Result<OneShot<()>> {
    spawn_to(
        move || {
            if let Err(e) = pc.take_fuzzy_snapshot() {
                log::error!("failed to write snapshot: {:?}", e);
            }
        },
        &SNAPSHOT_QUEUE,
    )
}

pub(crate) fn write_to_log(
    iobuf: Arc<crate::pagecache::iobuf::IoBuf>,
    iobufs: Arc<crate::pagecache::iobuf::IoBufs>,
) -> Result<OneShot<()>> {
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
                iobufs.config.set_global_error(e);

                let intervals = iobufs.intervals.lock();

                // having held the mutex makes this linearized
                // with the notify below.
                drop(intervals);

                let _notified = iobufs.interval_updated.notify_all();
            }
        },
        &IO_QUEUE,
    )
}

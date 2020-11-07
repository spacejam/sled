//! A simple adaptive threadpool that returns a oneshot future.

use std::{
    collections::VecDeque,
    sync::atomic::{AtomicBool, Ordering::Relaxed},
    thread,
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};

use crate::{
    debug_delay, warn, Acquire, AtomicUsize, Error, Lazy, OneShot, Result,
    SeqCst,
};

// This is lower for CI reasons.
#[cfg(windows)]
const MAX_THREADS: usize = 16;

#[cfg(not(windows))]
const MAX_THREADS: usize = 128;

const DESIRED_WAITING_THREADS: usize = 7;

static WAITING_THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);
static TOTAL_THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);
static SPAWNS: AtomicUsize = AtomicUsize::new(0);
static SPAWNING: AtomicBool = AtomicBool::new(false);

type Work = Box<dyn FnOnce() + Send + 'static>;

struct Queue {
    cv: Condvar,
    mu: Mutex<VecDeque<Work>>,
}

impl Queue {
    fn recv_timeout(&self, duration: Duration) -> Option<Work> {
        let mut queue = self.mu.lock();

        let cutoff = Instant::now() + duration;

        while queue.is_empty() {
            WAITING_THREAD_COUNT.fetch_add(1, SeqCst);
            let res = self.cv.wait_until(&mut queue, cutoff);
            WAITING_THREAD_COUNT.fetch_sub(1, SeqCst);
            if res.timed_out() {
                break;
            }
        }

        queue.pop_front()
    }

    fn try_recv(&self) -> Option<Work> {
        let mut queue = self.mu.lock();
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
}

static QUEUE: Lazy<Queue, fn() -> Queue> = Lazy::new(init_queue);
static BROKEN: AtomicBool = AtomicBool::new(false);

fn init_queue() -> Queue {
    debug_delay();
    for _ in 0..DESIRED_WAITING_THREADS {
        debug_delay();
        if let Err(e) = spawn_new_thread(true) {
            log::error!("failed to initialize threadpool: {:?}", e);
        }
    }
    Queue { cv: Condvar::new(), mu: Mutex::new(VecDeque::new()) }
}

fn perform_work(is_immortal: bool) {
    let wait_limit = Duration::from_secs(1);

    let mut performed = 0;
    let mut contiguous_overshoots = 0;

    while is_immortal || performed < 5 || contiguous_overshoots < 3 {
        debug_delay();
        let task_res = QUEUE.recv_timeout(wait_limit);

        if let Some(task) = task_res {
            WAITING_THREAD_COUNT.fetch_sub(1, SeqCst);
            (task)();
            WAITING_THREAD_COUNT.fetch_add(1, SeqCst);
            performed += 1;
        }

        while let Some(task) = QUEUE.try_recv() {
            debug_delay();
            WAITING_THREAD_COUNT.fetch_sub(1, SeqCst);
            (task)();
            WAITING_THREAD_COUNT.fetch_add(1, SeqCst);
            performed += 1;
        }

        debug_delay();

        let waiting = WAITING_THREAD_COUNT.load(Acquire);

        if waiting > DESIRED_WAITING_THREADS {
            contiguous_overshoots += 1;
        } else {
            contiguous_overshoots = 0;
        }
    }
}

// Create up to MAX_THREADS dynamic blocking task worker threads.
// Dynamic threads will terminate themselves if they don't
// receive any work after one second.
fn maybe_spawn_new_thread() -> Result<()> {
    debug_delay();
    let total_workers = TOTAL_THREAD_COUNT.load(Acquire);
    debug_delay();
    let waiting_threads = WAITING_THREAD_COUNT.load(Acquire);

    if waiting_threads >= DESIRED_WAITING_THREADS
        || total_workers >= MAX_THREADS
    {
        return Ok(());
    }

    if !SPAWNING.compare_and_swap(false, true, SeqCst) {
        spawn_new_thread(false)?;
    }

    Ok(())
}

fn spawn_new_thread(is_immortal: bool) -> Result<()> {
    if BROKEN.load(Relaxed) {
        return Err(Error::ReportableBug(
            "IO thread unexpectedly panicked. please report \
            this bug on the sled github repo."
                .to_string(),
        ));
    }

    let spawn_id = SPAWNS.fetch_add(1, SeqCst);

    TOTAL_THREAD_COUNT.fetch_add(1, SeqCst);
    let spawn_res = thread::Builder::new()
        .name(format!("sled-io-{}", spawn_id))
        .spawn(move || {
            SPAWNING.store(false, SeqCst);
            debug_delay();
            let res = std::panic::catch_unwind(|| perform_work(is_immortal));
            TOTAL_THREAD_COUNT.fetch_sub(1, SeqCst);
            if is_immortal || res.is_err() {
                // IO thread panicked, shut down the system
                log::error!(
                    "IO thread unexpectedly terminated.
                    please report this error at the sled github repo. {:?}",
                    res
                );
                BROKEN.store(true, SeqCst);
            }
        });

    if let Err(e) = spawn_res {
        static E: AtomicBool = AtomicBool::new(false);

        SPAWNING.store(false, SeqCst);

        if !E.compare_and_swap(false, true, Relaxed) {
            // only execute this once
            warn!(
                "Failed to dynamically increase the threadpool size: {:?}.",
                e,
            )
        }
    }

    Ok(())
}

/// Spawn a function on the threadpool.
pub fn spawn<F, R>(work: F) -> Result<OneShot<R>>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static + Sized,
{
    let (promise_filler, promise) = OneShot::pair();
    let task = move || {
        let result = (work)();
        promise_filler.fill(result);
    };

    let depth = QUEUE.send(Box::new(task));

    if depth > DESIRED_WAITING_THREADS {
        maybe_spawn_new_thread()?;
    }

    Ok(promise)
}

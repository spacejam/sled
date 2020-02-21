//! A simple adaptive threadpool that returns a oneshot future.

use std::{
    collections::VecDeque,
    sync::atomic::AtomicBool,
    thread,
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};

use crate::{debug_delay, warn, AtomicUsize, Lazy, OneShot, Relaxed, SeqCst};

// This is lower for CI reasons.
#[cfg(windows)]
const MAX_THREADS: usize = 16;

#[cfg(not(windows))]
const MAX_THREADS: usize = 128;

const MIN_THREADS: usize = 2;

static STANDBY_THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);
static TOTAL_THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);

macro_rules! once {
    ($args:block) => {
        static E: AtomicBool = AtomicBool::new(false);
        if !E.compare_and_swap(false, true, Relaxed) {
            // only execute this once
            $args;
        }
    };
}

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
            let res = self.cv.wait_until(&mut queue, cutoff);
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

    fn send(&self, work: Work) {
        let mut queue = self.mu.lock();
        queue.push_back(work);

        // having held the mutex makes this linearized
        // with the notify below.
        drop(queue);

        self.cv.notify_one();
    }
}

static QUEUE: Lazy<Queue, fn() -> Queue> = Lazy::new(init_queue);

fn init_queue() -> Queue {
    maybe_spawn_new_thread();
    Queue { cv: Condvar::new(), mu: Mutex::new(VecDeque::new()) }
}

fn perform_work() {
    let wait_limit = Duration::from_secs(1);

    while STANDBY_THREAD_COUNT.load(SeqCst) < MIN_THREADS {
        debug_delay();
        STANDBY_THREAD_COUNT.fetch_add(1, SeqCst);

        debug_delay();
        let task_res = QUEUE.recv_timeout(wait_limit);

        debug_delay();
        if STANDBY_THREAD_COUNT.fetch_sub(1, SeqCst) <= MIN_THREADS {
            maybe_spawn_new_thread();
        }

        if let Some(task) = task_res {
            (task)();
        }

        debug_delay();
        while let Some(task) = QUEUE.try_recv() {
            (task)();
            debug_delay();
        }

        debug_delay();
    }
}

// Create up to MAX_THREADS dynamic blocking task worker threads.
// Dynamic threads will terminate themselves if they don't
// receive any work after one second.
fn maybe_spawn_new_thread() {
    debug_delay();
    let total_workers = TOTAL_THREAD_COUNT.load(SeqCst);
    debug_delay();
    let standby_workers = STANDBY_THREAD_COUNT.load(SeqCst);
    if standby_workers >= MIN_THREADS || total_workers >= MAX_THREADS {
        return;
    }

    let spawn_res =
        thread::Builder::new().name("sled-io".to_string()).spawn(|| {
            debug_delay();
            TOTAL_THREAD_COUNT.fetch_add(1, SeqCst);
            perform_work();
            TOTAL_THREAD_COUNT.fetch_sub(1, SeqCst);
        });

    if let Err(e) = spawn_res {
        once!({
            warn!(
                "Failed to dynamically increase the threadpool size: {:?}. \
                 Currently have {} running IO threads",
                e, total_workers
            )
        });
    }
}

/// Spawn a function on the threadpool.
pub fn spawn<F, R>(work: F) -> OneShot<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (promise_filler, promise) = OneShot::pair();
    let task = move || {
        let result = (work)();
        promise_filler.fill(result);
    };

    QUEUE.send(Box::new(task));

    maybe_spawn_new_thread();

    promise
}

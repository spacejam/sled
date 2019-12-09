//! A simple adaptive threadpool that returns a oneshot future.

use std::thread;
use std::time::Duration;

use crossbeam_channel::{unbounded, Receiver, Sender};

use crate::{
    debug_delay, warn, AtomicBool, AtomicUsize, Lazy, OneShot, Relaxed, SeqCst,
};

const MAX_THREADS: usize = 128;

static STANDBY_THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);
static TOTAL_THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);

struct Pool {
    sender: Sender<Box<dyn FnOnce() + Send + 'static>>,
    receiver: Receiver<Box<dyn FnOnce() + Send + 'static>>,
}

static POOL: Lazy<Pool, fn() -> Pool> = Lazy::new(init_pool);

fn init_pool() -> Pool {
    let (sender, receiver) = unbounded();
    Pool { sender, receiver }
}

// Create up to MAX_THREADS dynamic blocking task worker threads.
// Dynamic threads will terminate themselves if they don't
// receive any work after one second.
fn maybe_spawn_new_thread() {
    debug_delay();
    let total_workers = TOTAL_THREAD_COUNT.load(SeqCst);
    debug_delay();
    let standby_workers = STANDBY_THREAD_COUNT.load(SeqCst);
    if standby_workers >= 1 || total_workers >= MAX_THREADS {
        return;
    }

    let spawn_res =
        thread::Builder::new().name("sled-io".to_string()).spawn(|| {
            let wait_limit = Duration::from_secs(1);

            debug_delay();
            TOTAL_THREAD_COUNT.fetch_add(1, SeqCst);
            loop {
                debug_delay();
                STANDBY_THREAD_COUNT.fetch_add(1, SeqCst);

                debug_delay();
                let task = POOL.receiver.recv_timeout(wait_limit);

                debug_delay();
                if STANDBY_THREAD_COUNT.fetch_sub(1, SeqCst) == 1 {
                    maybe_spawn_new_thread();
                }

                debug_delay();
                if let Ok(task) = task {
                    (task)();
                } else if STANDBY_THREAD_COUNT.compare_and_swap(0, 1, SeqCst)
                    == 0
                {
                    // avoid bottoming out the standby pool
                    continue;
                } else {
                    TOTAL_THREAD_COUNT.fetch_sub(1, SeqCst);
                    return;
                }
            }
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

    POOL.sender.send(Box::new(task)).unwrap();

    maybe_spawn_new_thread();

    promise
}

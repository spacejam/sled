use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use crossbeam::channel::{bounded, Receiver, Sender};
use lazy_static::lazy_static;

use super::Promise;

const MAX_THREADS: u64 = 10_000;

static DYNAMIC_THREAD_COUNT: AtomicU64 = AtomicU64::new(0);

struct Pool {
    sender: Sender<Box<dyn FnOnce() + Send + 'static>>,
    receiver: Receiver<Box<dyn FnOnce() + Send + 'static>>,
}

lazy_static! {
    static ref POOL: Pool = {
        for _ in 0..2 {
            thread::Builder::new()
                .name("sled-io".to_string())
                .spawn(|| {
                    for task in &POOL.receiver {
                        (task)()
                    }
                })
                .expect("cannot start a thread driving blocking tasks");
        }

        // We want to use an unbuffered channel here to help
        // us drive our dynamic control. In effect, the
        // kernel's scheduler becomes the queue, reducing
        // the number of buffers that work must flow through
        // before being acted on by a core. This helps keep
        // latency snappy in the overall async system by
        // reducing bufferbloat.
        let (sender, receiver) = bounded(0);
        Pool { sender, receiver }
    };
}

// Create up to MAX_THREADS dynamic blocking task worker threads.
// Dynamic threads will terminate themselves if they don't
// receive any work after one second.
fn maybe_create_another_blocking_thread() {
    // We use a `Relaxed` atomic operation because
    // it's just a heuristic, and would not lose correctness
    // even if it's random.
    let workers = DYNAMIC_THREAD_COUNT.load(Ordering::Relaxed);
    if workers >= MAX_THREADS {
        return;
    }

    thread::Builder::new()
        .name("sled-io".to_string())
        .spawn(|| {
            let wait_limit = Duration::from_secs(1);

            DYNAMIC_THREAD_COUNT.fetch_add(1, Ordering::Relaxed);
            while let Ok(task) = POOL.receiver.recv_timeout(wait_limit) {
                (task)();
            }
            DYNAMIC_THREAD_COUNT.fetch_sub(1, Ordering::Relaxed);
        })
        .expect("cannot start a dynamic thread driving blocking tasks");
}

/// Spawn a function on the threadpool.
pub fn spawn<F, R>(work: F) -> Promise<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (promise_filler, promise) = Promise::pair();
    let task = move || {
        let result = (work)();
        promise_filler.fill(result);
    };
    let first_try_result = POOL.sender.try_send(Box::new(task));
    match first_try_result {
        Ok(()) => {
            // NICEEEE
        }
        Err(crossbeam::channel::TrySendError::Full(task)) => {
            // We were not able to send to the channel without
            // blocking. Try to spin up another thread and then
            // retry sending while blocking.
            maybe_create_another_blocking_thread();
            POOL.sender.send(task).unwrap()
        }
        Err(crossbeam::channel::TrySendError::Disconnected(_)) => {
            panic!(
                "unable to send to blocking threadpool \
                 due to receiver disconnection"
            );
        }
    }
    promise
}

use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread;
use std::time::Duration;

use super::*;

#[derive(Debug)]
pub(crate) enum ShutdownState {
    Running,
    ShuttingDown,
    ShutDown,
}

impl ShutdownState {
    fn is_running(&self) -> bool {
        if let ShutdownState::Running = self {
            true
        } else {
            false
        }
    }

    fn is_shutdown(&self) -> bool {
        if let ShutdownState::ShutDown = self {
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub(crate) struct Flusher {
    shutdown: Arc<Mutex<ShutdownState>>,
    sc: Arc<Condvar>,
    join_handle: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
}

impl Flusher {
    /// Spawns a thread that periodically calls `callback` until dropped.
    pub(crate) fn new<PM, P>(
        name: String,
        pagecache: Weak<PageCache<PM, P>>,
        flush_every_ms: u64,
    ) -> Flusher
    where
        PM: 'static + Send + Sync + pagecache::Materializer<PageFrag = P>,
        P: 'static
            + Clone
            + std::fmt::Debug
            + Serialize
            + Send
            + Sync
            + serde::de::DeserializeOwned,
    {
        #[allow(clippy::mutex_atomic)] // mutex used in CondVar below
        let shutdown = Arc::new(Mutex::new(ShutdownState::Running));
        let sc = Arc::new(Condvar::new());

        let join_handle = thread::Builder::new()
            .name(name)
            .spawn({
                let shutdown = shutdown.clone();
                let sc = sc.clone();
                move || run(shutdown, sc, pagecache, flush_every_ms)
            })
            .unwrap();

        Flusher {
            shutdown,
            sc,
            join_handle: Arc::new(Mutex::new(Some(join_handle))),
        }
    }
}

fn run<PM, P>(
    shutdown: Arc<Mutex<ShutdownState>>,
    sc: Arc<Condvar>,
    pagecache: Weak<PageCache<PM, P>>,
    flush_every_ms: u64,
) where
    PM: 'static + Send + Sync + pagecache::Materializer<PageFrag = P>,
    P: 'static
        + Clone
        + std::fmt::Debug
        + Serialize
        + Send
        + Sync
        + serde::de::DeserializeOwned,
{
    let flush_every = Duration::from_millis(flush_every_ms);
    let mut shutdown = shutdown.lock().unwrap();
    let mut wrote_data = false;
    while shutdown.is_running() || wrote_data {
        // write data until the shutdown flag
        // is triggered, and we have nothing left
        // to flush.
        let pagecache = match pagecache.upgrade() {
            Some(c) => c,
            None => {
                *shutdown = ShutdownState::ShutDown;
                sc.notify_all();
                return;
            }
        };

        let before = std::time::Instant::now();
        match pagecache.flush() {
            Ok(0) => {
                wrote_data = false;
                if !shutdown.is_running() {
                    break;
                }
                // we had no dirty data to flush,
                // so we can spend a little effort
                // cleaning up the file. try not to
                // spend more than half of our sleep
                // time rewriting pages though.
                while before.elapsed() < flush_every / 2 {
                    match pagecache.attempt_gc() {
                        Err(e) => {
                            error!(
                                "failed to clean file from async flush thread: {}",
                                e
                            );

                            #[cfg(feature = "failpoints")]
                            pagecache.set_failpoint(e);

                            *shutdown = ShutdownState::ShutDown;
                            sc.notify_all();
                            return;
                        }
                        Ok(false) => break,
                        Ok(true) => {}
                    }
                }
            }
            Ok(_) => {
                wrote_data = true;
                // at some point, we may want to
                // put adaptive logic here to tune
                // sleeps based on how much work
                // we accomplished
            }
            Err(e) => {
                error!("failed to flush from periodic flush thread: {}", e);

                #[cfg(feature = "failpoints")]
                pagecache.set_failpoint(e);

                *shutdown = ShutdownState::ShutDown;
                sc.notify_all();
                return;
            }
        }

        drop(pagecache);

        let sleep_duration = flush_every
            .checked_sub(before.elapsed())
            .unwrap_or(Duration::from_millis(1));

        shutdown = sc.wait_timeout(shutdown, sleep_duration).unwrap().0;
    }
    *shutdown = ShutdownState::ShutDown;
    sc.notify_all();
}

impl Drop for Flusher {
    fn drop(&mut self) {
        let mut shutdown = self.shutdown.lock().unwrap();
        *shutdown = ShutdownState::ShuttingDown;
        self.sc.notify_all();

        while !shutdown.is_shutdown() {
            shutdown = self.sc.wait(shutdown).unwrap();
        }

        let mut join_handle_opt = self.join_handle.lock().unwrap();
        if let Some(join_handle) = join_handle_opt.take() {
            if let Err(e) = join_handle.join() {
                error!("error joining Periodic thread: {:?}", e);
            }
        }
    }
}

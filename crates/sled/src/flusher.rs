use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread;
use std::time::Duration;

use super::*;

#[derive(Debug)]
pub(crate) struct Flusher {
    shutdown: Arc<Mutex<bool>>,
    sc: Arc<Condvar>,
    join_handle: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
}

impl Flusher {
    /// Spawns a thread that periodically calls `callback` until dropped.
    pub(crate) fn new(
        name: String,
        context: Weak<Context>,
        flush_every_ms: u64,
    ) -> Flusher {
        #[allow(clippy::mutex_atomic)] // mutex used in CondVar below
        let shutdown = Arc::new(Mutex::new(false));
        let sc = Arc::new(Condvar::new());

        let join_handle = thread::Builder::new()
            .name(name)
            .spawn({
                let shutdown = shutdown.clone();
                let sc = sc.clone();
                move || run(shutdown, sc, context, flush_every_ms)
            })
            .unwrap();

        Flusher {
            shutdown,
            sc,
            join_handle: Arc::new(Mutex::new(Some(join_handle))),
        }
    }
}

fn run(
    shutdown: Arc<Mutex<bool>>,
    sc: Arc<Condvar>,
    context: Weak<Context>,
    flush_every_ms: u64,
) {
    let flush_every = Duration::from_millis(flush_every_ms);
    let mut shutdown = shutdown.lock().unwrap();
    while !*shutdown {
        let context = match context.upgrade() {
            Some(c) => c,
            None => return,
        };

        let before = std::time::Instant::now();
        match context.pagecache.flush() {
            Ok(0) => {
                // we had no dirty data to flush,
                // so we can spend a little effort
                // cleaning up the file. try not to
                // spend more than half of our sleep
                // time rewriting pages though.
                while before.elapsed() < flush_every / 2 {
                    match context.pagecache.attempt_gc() {
                        Err(e) => {
                            error!(
                                "failed to clean file from async flush thread: {}",
                                e
                            );

                            #[cfg(feature = "failpoints")]
                            context.pagecache.set_failpoint(e);

                            return;
                        }
                        Ok(false) => break,
                        Ok(true) => {}
                    }
                }
            }
            Ok(_) => {
                // at some point, we may want to
                // put adaptive logic here to tune
                // sleeps based on how much work
                // we accomplished
            }
            Err(e) => {
                error!("failed to flush from periodic flush thread: {}", e);

                #[cfg(feature = "failpoints")]
                context.pagecache.set_failpoint(e);

                return;
            }
        }

        let sleep_duration = flush_every
            .checked_sub(before.elapsed())
            .unwrap_or(Duration::from_millis(1));

        shutdown = sc.wait_timeout(shutdown, sleep_duration).unwrap().0;
    }
}

impl Drop for Flusher {
    fn drop(&mut self) {
        let mut shutdown = self.shutdown.lock().unwrap();
        *shutdown = true;
        self.sc.notify_all();
        drop(shutdown);

        let mut join_handle_opt = self.join_handle.lock().unwrap();
        if let Some(join_handle) = join_handle_opt.take() {
            if let Err(e) = join_handle.join() {
                error!("error joining Periodic thread: {:?}", e);
            }
        }
    }
}

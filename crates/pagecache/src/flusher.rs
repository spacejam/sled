use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use super::*;

pub(crate) struct Flusher {
    shutdown: Arc<Mutex<bool>>,
    sc: Arc<Condvar>,
    join_handle: Option<std::thread::JoinHandle<()>>,
}

impl Flusher {
    /// Spawns a thread that periodically calls `callback` until dropped.
    pub(crate) fn new(
        name: String,
        iobufs: IoBufs,
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
                move || run(shutdown, sc, iobufs, flush_every_ms)
            })
            .unwrap();

        Flusher {
            shutdown,
            sc,
            join_handle: Some(join_handle),
        }
    }
}

fn run(
    shutdown: Arc<Mutex<bool>>,
    sc: Arc<Condvar>,
    iobufs: IoBufs,
    flush_every_ms: u64,
) {
    let sleep_duration = Duration::from_millis(flush_every_ms);
    let mut shutdown = shutdown.lock().unwrap();
    while !*shutdown {
        match iobufs.flush() {
            Ok(0) => {}
            Ok(_) => {
                // at some point, we may want to
                // put adaptive logic here to tune
                // sleeps based on how much work
                // we accomplished
            }
            Err(e) => {
                #[cfg(feature = "failpoints")]
                {
                    if let Error::FailPoint = e {
                        iobufs
                            .0
                            ._failpoint_crashing
                            .store(true, SeqCst);

                        // wake up any waiting threads
                        // so they don't stall forever
                        iobufs.0.interval_updated.notify_all();
                    }
                }

                error!(
                    "failed to flush from periodic flush thread: {}",
                    e
                );

                return;
            }
        }

        shutdown =
            sc.wait_timeout(shutdown, sleep_duration).unwrap().0;
    }
}

impl Drop for Flusher {
    fn drop(&mut self) {
        let mut shutdown = self.shutdown.lock().unwrap();
        *shutdown = true;
        self.sc.notify_all();
        drop(shutdown);

        let join_handle = self.join_handle.take().unwrap();
        if let Err(e) = join_handle.join() {
            error!("error joining Periodic thread: {:?}", e);
        }
    }
}

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
        let shutdown = Arc::new(Mutex::new(false));
        let sc = Arc::new(Condvar::new());

        let mu = shutdown.clone();
        let sc2 = sc.clone();
        let join_handle = thread::Builder::new()
            .name(name)
            .spawn(move || {
                let sleep_duration =
                    Duration::from_millis(flush_every_ms);
                let mut shutdown = mu.lock().unwrap();
                while !*shutdown {
                    if let Err(e) = iobufs.flush() {
                        #[cfg(feature = "failpoints")]
                        {
                            if let Error::FailPoint = e {
                                iobufs.0._failpoint_crashing
                                    .store(true, SeqCst);
                                // wake up any waiting threads so they don't stall forever
                                iobufs.0.interval_updated.notify_all();
                            }
                        }

                        error!(
                            "failed to flush from periodic flush thread: {}",
                            e
                        );

                        return;
                    }

                    shutdown = sc2
                        .wait_timeout(
                            shutdown,
                            sleep_duration,
                        ).unwrap()
                        .0;
                }
            }).unwrap();

        Flusher {
            shutdown,
            sc,
            join_handle: Some(join_handle),
        }
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

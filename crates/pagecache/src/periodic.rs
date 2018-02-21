use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::thread;
use std::time::Duration;

use super::*;

pub trait Callback: Send + 'static {
    fn call(&self);
}

pub struct Periodic<C: Callback> {
    shutdown: Arc<AtomicBool>,
    join_handle: Option<std::thread::JoinHandle<()>>,
    _marker: PhantomData<C>,
}

impl<C: Callback> Periodic<C> {
    /// Spawns a thread that periodically calls `callback` until dropped.
    pub fn new(
        name: String,
        callback: C,
        flush_every_ms: Option<u64>,
    ) -> Periodic<C> {
        let shutdown = Arc::new(AtomicBool::new(false));

        let join_handle = flush_every_ms.map(|flush_every_ms| {
            let shutdown = shutdown.clone();
            thread::Builder::new()
                .name(name)
                .spawn(move || while !shutdown.load(Acquire) {
                    callback.call();

                    thread::sleep(Duration::from_millis(flush_every_ms));
                })
                .unwrap()
        });

        Periodic {
            shutdown,
            join_handle,
            _marker: PhantomData,
        }
    }
}

impl<C: Callback> Drop for Periodic<C> {
    fn drop(&mut self) {
        if let Some(join_handle) = self.join_handle.take() {
            self.shutdown.store(true, Release);
            if let Err(e) = join_handle.join() {
                error!("error joining Periodic thread: {:?}", e);
            }
        }
    }
}

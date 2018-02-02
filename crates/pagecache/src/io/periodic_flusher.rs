use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::thread;
use std::time::Duration;

use super::*;

/// Spawns a thread that periodically calls `flush` on
/// an `IoBufs` structure until its shutdown atomic bool
/// is set to true.
pub(super) fn flusher(
    name: String,
    iob: Arc<IoBufs>,
    shutdown: Arc<AtomicBool>,
    flush_every_ms: u64,
) -> std::io::Result<thread::JoinHandle<()>> {
    thread::Builder::new().name(name).spawn(move || while
        !shutdown.load(SeqCst)
    {
        iob.flush().unwrap();

        thread::sleep(Duration::from_millis(flush_every_ms));
    })
}

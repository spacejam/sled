use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::thread;
use std::time::Duration;

use crossbeam::sync::SegQueue;

use super::*;

pub fn flusher(
    name: String,
    config: Config,
    iob: Arc<IoBufs>,
    shutdown: Arc<AtomicBool>,
    flush_every_ms: u64,
    deferred_hole_punches: Arc<SegQueue<LogID>>,
) -> std::io::Result<thread::JoinHandle<()>> {
    thread::Builder::new().name(name).spawn(move || while
        !shutdown.load(SeqCst)
    {
        iob.flush();
        let stable = iob.stable();

        let cached_f = config.cached_file();
        let mut f = cached_f.borrow_mut();
        loop {
            if let Some(lid) = deferred_hole_punches.try_pop() {
                if lid < stable {
                    punch_hole(&mut f, lid).unwrap();
                } else {
                    deferred_hole_punches.push(lid);
                    break;
                }
            } else {
                break;
            }
        }

        thread::sleep(Duration::from_millis(flush_every_ms));
    })
}

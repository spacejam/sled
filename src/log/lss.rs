use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use super::*;

/// A sequential store which allows users to create
/// reservations placed at known log offsets, used
/// for writing persistent data structures that need
/// to know where to find persisted bits in the future.
///
/// # Working with `LockFreeLog`
///
/// ```
/// use sled::Log;
///
/// let log = sled::Config::default().log();
/// let first_offset = log.write(b"1".to_vec());
/// log.write(b"22".to_vec());
/// log.write(b"333".to_vec());
///
/// // stick an abort in the middle, which should not be returned
/// let res = log.reserve(b"never_gonna_hit_disk".to_vec());
/// res.abort();
///
/// log.write(b"4444".to_vec());
/// let last_offset = log.write(b"55555".to_vec());
/// log.make_stable(last_offset);
/// let mut iter = log.iter_from(first_offset);
/// assert_eq!(iter.next().unwrap().2, b"1".to_vec());
/// assert_eq!(iter.next().unwrap().2, b"22".to_vec());
/// assert_eq!(iter.next().unwrap().2, b"333".to_vec());
/// assert_eq!(iter.next().unwrap().2, b"4444".to_vec());
/// assert_eq!(iter.next().unwrap().2, b"55555".to_vec());
/// assert_eq!(iter.next(), None);
/// ```
pub struct LockFreeLog {
    /// iobufs is the underlying IO writer
    pub iobufs: Arc<IoBufs>,
    flusher_shutdown: Arc<AtomicBool>,
    flusher_handle: Option<std::thread::JoinHandle<()>>,
}

unsafe impl Send for LockFreeLog {}
unsafe impl Sync for LockFreeLog {}

impl Drop for LockFreeLog {
    fn drop(&mut self) {
        self.flusher_shutdown.store(
            true,
            std::sync::atomic::Ordering::SeqCst,
        );
        if let Some(join_handle) = self.flusher_handle.take() {
            join_handle.join().unwrap();
        }
    }
}

impl LockFreeLog {
    /// create new lock-free log
    pub fn start_system(config: Config) -> LockFreeLog {
        #[cfg(feature = "env_logger")]
        let _r = env_logger::init();

        let iobufs = Arc::new(IoBufs::new(config.clone()));

        let flusher_shutdown = Arc::new(AtomicBool::new(false));

        let mut log = LockFreeLog {
            iobufs: iobufs.clone(),
            flusher_shutdown: flusher_shutdown.clone(),
            flusher_handle: None,
        };

        let flusher_handle = config.get_flush_every_ms().map(|flush_every_ms| {
            periodic_flusher::flusher(
                "log flusher".to_owned(),
                iobufs,
                flusher_shutdown,
                flush_every_ms,
            ).unwrap()
        });

        log.flusher_handle = flusher_handle;

        log
    }

    /// Flush the next io buffer.
    pub fn flush(&self) {
        self.iobufs.flush();
    }
}

impl Log for LockFreeLog {
    fn reserve(&self, buf: Vec<u8>) -> Reservation {
        self.iobufs.reserve(buf)
    }

    /// return the config in use for this log
    fn config(&self) -> &Config {
        self.iobufs.config()
    }

    fn write(&self, buf: Vec<u8>) -> LogID {
        self.iobufs.reserve(buf).complete()
    }

    /// Return an iterator over the log, starting with
    /// a specified offset.
    fn iter_from(&self, lsn: Lsn) -> LogIter<Self> {
        let start = clock();
        let sa = self.iobufs.segment_accountant.lock().unwrap();
        let locked = clock();
        M.accountant_lock.measure(locked - start);
        let segment_iter = sa.segment_snapshot_iter_from(lsn);
        M.accountant_hold.measure(clock() - locked);

        LogIter {
            max_encountered_lsn: 0,
            min_lsn: lsn,
            log: self,
            segment: None,
            segment_iter: segment_iter,
        }
    }

    /// read a buffer from the disk
    fn read(&self, id: LogID) -> io::Result<LogRead> {
        self.make_stable(id);
        self.config().read_entry(id)
    }

    /// returns the current stable offset written to disk
    fn stable_offset(&self) -> LogID {
        self.iobufs.stable()
    }

    /// blocks until the specified log sequence number has
    /// been made stable on disk
    fn make_stable(&self, lsn: Lsn) {
        let start = clock();

        // we make sure stable > lsn because stable starts at 0,
        // before we write the 0th byte of the file.
        // println!("before loop, waiting on lsn {}", lsn);
        while self.iobufs.stable() <= lsn {
            // println!("top of loop");
            self.iobufs.flush();

            // block until another thread updates the stable lsn
            let waiter = self.iobufs.intervals.lock().unwrap();

            if self.iobufs.stable() <= lsn {
                // println!("waiting on cond var");
                let _waiter = self.iobufs.interval_updated.wait(waiter).unwrap();
            // println!("back from cond var");
            } else {
                break;
            }
        }

        // println!("make_stable({}) returning", lsn);

        M.make_stable.measure(clock() - start);
    }
}

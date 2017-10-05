use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use super::*;

/// A sequential store which allows users to create
/// reservations placed at known log offsets, used
/// for writing persistent data structures that need
/// to know where to find persisted bits in the future.
///
/// # Working with `Log`
///
/// ```
/// let log = sled::Config::default().log();
/// let (first_lsn, _first_offset) = log.write(b"1".to_vec());
/// log.write(b"22".to_vec());
/// log.write(b"333".to_vec());
///
/// // stick an abort in the middle, which should not be returned
/// let res = log.reserve(b"never_gonna_hit_disk".to_vec());
/// res.abort();
///
/// log.write(b"4444".to_vec());
/// let (last_lsn, _last_offset) = log.write(b"55555".to_vec());
/// log.make_stable(last_lsn);
/// let mut iter = log.iter_from(first_lsn);
/// assert_eq!(iter.next().unwrap().2, b"1".to_vec());
/// assert_eq!(iter.next().unwrap().2, b"22".to_vec());
/// assert_eq!(iter.next().unwrap().2, b"333".to_vec());
/// assert_eq!(iter.next().unwrap().2, b"4444".to_vec());
/// assert_eq!(iter.next().unwrap().2, b"55555".to_vec());
/// assert_eq!(iter.next(), None);
/// ```
pub struct Log {
    /// iobufs is the underlying lock-free IO write buffer.
    pub iobufs: Arc<IoBufs>,
    flusher_shutdown: Arc<AtomicBool>,
    flusher_handle: Option<std::thread::JoinHandle<()>>,
}

unsafe impl Send for Log {}
unsafe impl Sync for Log {}

impl Drop for Log {
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

impl Log {
    /// create new lock-free log
    pub fn start_system(config: Config) -> Log {
        #[cfg(feature = "env_logger")]
        let _r = env_logger::init();

        let iobufs = Arc::new(IoBufs::new(config.clone()));

        let flusher_shutdown = Arc::new(AtomicBool::new(false));

        let mut log = Log {
            iobufs: iobufs.clone(),
            flusher_shutdown: flusher_shutdown.clone(),
            flusher_handle: None,
        };

        let flusher_handle =
            config.get_flush_every_ms().map(|flush_every_ms| {
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

    /// Reserve space in the log for a pending linearized operation.
    pub fn reserve(&self, buf: Vec<u8>) -> Reservation {
        self.iobufs.reserve(buf)
    }

    /// Return the config in use for this log.
    // TODO no need, fix up config
    pub fn config(&self) -> &Config {
        self.iobufs.config()
    }

    /// Write a buffer into the log. Returns the log sequence
    /// number and the file offset of the write.
    pub fn write(&self, buf: Vec<u8>) -> (Lsn, LogID) {
        self.iobufs.reserve(buf).complete()
    }

    /// Return an iterator over the log, starting with
    /// a specified offset.
    pub fn iter_from(&self, lsn: Lsn) -> Iter {
        let io_buf_size = self.config().get_io_buf_size();
        let segment_base_lsn = lsn / io_buf_size as Lsn * io_buf_size as Lsn;
        let min_lsn = segment_base_lsn + SEG_HEADER_LEN as Lsn;
        let corrected_lsn = std::cmp::max(lsn, min_lsn);

        // println!("iter_from {}", lsn);
        let start = clock();
        let sa = self.iobufs.segment_accountant.lock().unwrap();
        let locked = clock();
        M.accountant_lock.measure(locked - start);
        let segment_iter = sa.segment_snapshot_iter_from(segment_base_lsn);
        M.accountant_hold.measure(clock() - locked);

        Iter {
            config: self.config(),
            max_lsn: self.stable_offset(),
            cur_lsn: corrected_lsn,
            segment_base: None,
            segment_iter: segment_iter,
            segment_len: io_buf_size,
            use_compression: self.config().get_use_compression(),
            trailer: None,
        }
    }

    /// read a buffer from the disk
    pub fn read(&self, lsn: Lsn, lid: LogID) -> io::Result<LogRead> {
        // println!("read lsn {} lid {}", lsn, lid);
        self.make_stable(lsn);
        let cached_f = self.config().cached_file();
        let mut f = cached_f.borrow_mut();
        // TODO check the lsn of the read log entry below

        let read = f.read_entry(
            lid,
            self.config().get_io_buf_size(),
            self.config().get_use_compression(),
        );
        read.and_then(|log_read| match log_read {
            LogRead::Flush(read_lsn, _, _) => {
                assert_eq!(lsn, read_lsn);
                Ok(log_read)
            }
            _ => Ok(log_read),
        })
    }

    /// returns the current stable offset written to disk
    pub fn stable_offset(&self) -> LogID {
        self.iobufs.stable()
    }

    /// blocks until the specified log sequence number has
    /// been made stable on disk
    pub fn make_stable(&self, lsn: Lsn) {
        let start = clock();

        // NB we make sure stable > lsn because stable starts at 0,
        // before we write the 0th byte of the file.
        // println!("before loop, waiting on lsn {}", lsn);
        while self.iobufs.stable() <= lsn {
            // println!("stable is {}", self.iobufs.stable());
            // println!("top of loop");
            self.iobufs.flush();

            // block until another thread updates the stable lsn
            let waiter = self.iobufs.intervals.lock().unwrap();

            if self.iobufs.stable() <= lsn {
                // println!("waiting on cond var");
                let _waiter =
                    self.iobufs.interval_updated.wait(waiter).unwrap();
            //println!("back from cond var");
            } else {
                break;
            }
        }

        // println!("make_stable({}) returning", lsn);

        M.make_stable.measure(clock() - start);
    }
}

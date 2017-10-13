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
    iobufs: Arc<IoBufs>,
    config: Config,
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

        #[cfg(feature = "cpuprofiler")]
        {
            cpuprofiler::PROFILER.lock().unwrap().stop().unwrap();
        }
    }
}

impl Log {
    /// Start the log, open or create the configured file,
    /// and optionally start the periodic buffer flush thread.
    pub fn start_system(config: Config) -> Log {
        #[cfg(feature = "env_logger")]
        let _r = env_logger::init();

        #[cfg(feature = "cpuprofiler")]
        {
            use std::env;

            let key = "CPUPROFILE";
            let path = match env::var(key) {
                Ok(val) => val,
                Err(_) => "sled.profile".to_owned(),
            };
            cpuprofiler::PROFILER.lock().unwrap().start(path).unwrap();
        }


        let iobufs = Arc::new(IoBufs::new(config.clone()));

        let flusher_shutdown = Arc::new(AtomicBool::new(false));

        let mut log = Log {
            iobufs: iobufs.clone(),
            config: config.clone(),
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

    /// Write a buffer into the log. Returns the log sequence
    /// number and the file offset of the write.
    pub fn write(&self, buf: Vec<u8>) -> (Lsn, LogID) {
        self.iobufs.reserve(buf).complete()
    }

    /// Return an iterator over the log, starting with
    /// a specified offset.
    pub fn iter_from(&self, lsn: Lsn) -> Iter {
        trace!("iterating from lsn {}", lsn);
        let io_buf_size = self.config.get_io_buf_size();
        let segment_base_lsn = lsn / io_buf_size as Lsn * io_buf_size as Lsn;
        let min_lsn = segment_base_lsn + SEG_HEADER_LEN as Lsn;
        let corrected_lsn = std::cmp::max(lsn, min_lsn);

        let segment_iter =
            self.with_sa(|sa| sa.segment_snapshot_iter_from(lsn));

        Iter {
            config: &self.config,
            max_lsn: self.stable_offset(),
            cur_lsn: corrected_lsn,
            segment_base: None,
            segment_iter: segment_iter,
            segment_len: io_buf_size,
            use_compression: self.config.get_use_compression(),
            trailer: None,
        }
    }

    /// read a buffer from the disk
    pub fn read(&self, lsn: Lsn, lid: LogID) -> io::Result<LogRead> {
        trace!("reading log lsn {} lid {}", lsn, lid);
        // TODO don't skip segments in SA, unify reuse_segment logic, remove from ordering consistently assert!(lsn >= lid, "lsn should never be less than the log offset");
        self.make_stable(lsn);
        let cached_f = self.config.cached_file();
        let mut f = cached_f.borrow_mut();

        let read = f.read_message(
            lid,
            self.config.get_io_buf_size(),
            self.config.get_use_compression(),
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
        while self.iobufs.stable() <= lsn {
            self.iobufs.flush();

            // block until another thread updates the stable lsn
            let waiter = self.iobufs.intervals.lock().unwrap();

            if self.iobufs.stable() <= lsn {
                trace!("waiting on cond var for make_stable({})", lsn);
                let _waiter =
                    self.iobufs.interval_updated.wait(waiter).unwrap();
            } else {
                trace!("make_stable({}) returning", lsn);
                break;
            }
        }

        M.make_stable.measure(clock() - start);
    }

    /// SegmentAccountant access for coordination with the `PageCache`
    pub fn with_sa<B, F>(&self, f: F) -> B
        where F: FnOnce(&mut SegmentAccountant) -> B
    {
        let start = clock();

        let mut sa = self.iobufs.segment_accountant.lock().unwrap();

        let locked_at = clock();

        M.accountant_lock.measure(locked_at - start);

        let ret = f(&mut sa);

        M.accountant_hold.measure(clock() - locked_at);

        ret
    }
}

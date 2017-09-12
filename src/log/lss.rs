use std::io::ErrorKind::UnexpectedEof;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

#[cfg(feature = "zstd")]
use zstd::block::decompress;

use crossbeam::sync::SegQueue;

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
/// assert_eq!(iter.next().unwrap().1, b"1".to_vec());
/// assert_eq!(iter.next().unwrap().1, b"22".to_vec());
/// assert_eq!(iter.next().unwrap().1, b"333".to_vec());
/// assert_eq!(iter.next().unwrap().1, b"4444".to_vec());
/// assert_eq!(iter.next().unwrap().1, b"55555".to_vec());
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
        #[cfg(feature = "log")]
        let _r = env_logger::init();

        let deferred_hole_punches = Arc::new(SegQueue::new());
        let iobufs = Arc::new(IoBufs::new(config.clone(), deferred_hole_punches.clone()));

        let flusher_shutdown = Arc::new(AtomicBool::new(false));
        let flusher_handle = config.get_flush_every_ms().map(|flush_every_ms| {
            periodic_flusher::flusher(
                "log flusher".to_owned(),
                config.clone(),
                iobufs.clone(),
                flusher_shutdown.clone(),
                flush_every_ms,
                deferred_hole_punches,
            ).unwrap()
        });

        LockFreeLog {
            iobufs: iobufs,
            flusher_shutdown: flusher_shutdown,
            flusher_handle: flusher_handle,
        }
    }

    /// Flush the next io buffer.
    pub fn flush(&self) {
        self.iobufs.flush();
    }

    /// Clean up log entries for data that may not
    /// yet be on the disk yet.
    pub fn defer_hole_punch(&self, lids: Vec<LogID>) {
        self.iobufs.defer_hole_punch(lids);
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

    /// read a buffer from the disk
    fn read(&self, id: LogID) -> io::Result<LogRead> {
        let start = clock();
        let cached_f = self.config().cached_file();
        let mut f = cached_f.borrow_mut();
        f.seek(SeekFrom::Start(id))?;

        let mut valid_buf = [0u8; 1];
        f.read_exact(&mut valid_buf)?;
        let valid = valid_buf[0] == 1;

        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf)?;

        let len32: u32 = unsafe { std::mem::transmute(len_buf) };
        let mut len = len32 as usize;
        let max = self.config().get_io_buf_size() - HEADER_LEN;
        if len > max {
            #[cfg(feature = "log")]
            error!("log read invalid message length, {} should be <= {}", len, max);
            M.read.measure(clock() - start);
            return Ok(LogRead::Corrupted(len));
        } else if len == 0 && !valid {
            // skip to next record, which starts with 1
            loop {
                let mut byte = [0u8; 1];
                if let Err(e) = f.read_exact(&mut byte) {
                    if e.kind() == UnexpectedEof {
                        // we've hit the end of the file
                        break;
                    }
                    panic!("{:?}", e);
                }
                if byte[0] != 1 {
                    debug_assert_eq!(byte[0], 0);
                    len += 1;
                } else {
                    break;
                }
            }
        }

        if !valid {
            M.read.measure(clock() - start);
            return Ok(LogRead::Zeroed(len + 5));
        }

        let mut crc16_buf = [0u8; 2];
        f.read_exact(&mut crc16_buf)?;

        let mut buf = Vec::with_capacity(len);
        unsafe {
            buf.set_len(len);
        }
        f.read_exact(&mut buf)?;

        let checksum = crc16_arr(&buf);
        if checksum != crc16_buf {
            M.read.measure(clock() - start);
            return Ok(LogRead::Corrupted(len));
        }

        #[cfg(feature = "zstd")]
        let res = {
            if self.config().get_use_compression() {
                let start = clock();
                let res = Ok(LogRead::Flush(decompress(&*buf, max).unwrap(), len));
                M.decompress.measure(clock() - start);
                res
            } else {
                Ok(LogRead::Flush(buf, len))
            }
        };

        #[cfg(not(feature = "zstd"))]
        let res = Ok(LogRead::Flush(buf, len));

        M.read.measure(clock() - start);
        res
    }

    /// returns the current stable offset written to disk
    fn stable_offset(&self) -> LogID {
        self.iobufs.stable()
    }

    /// blocks until the specified id has been made stable on disk
    fn make_stable(&self, id: LogID) {
        let start = clock();
        let mut spins = 0;
        while self.iobufs.stable() <= id {
            self.iobufs.flush();
            spins += 1;
            if spins > 2_000_000 {
                #[cfg(feature = "log")]
                debug!("have spun >2000000x in make_stable");
                spins = 0;
            }
        }
        M.make_stable.measure(clock() - start);
    }

    /// deallocates the data part of a log id
    fn punch_hole(&self, id: LogID) -> std::io::Result<()> {
        // we zero out the valid byte, and use fallocate to punch a hole
        // in the actual data, but keep the len for recovery.
        let cached_f = self.config().cached_file();
        let mut f = cached_f.borrow_mut();
        punch_hole(&mut f, id)
    }
}

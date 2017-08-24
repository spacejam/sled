use std::io::{Read, Seek, Write};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

#[cfg(feature = "libc")]
#[cfg(target_os = "linux")]
use libc::{FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE, fallocate};

#[cfg(feature = "zstd")]
use zstd::block::decompress;

use super::*;

/// A sequential store which allows users to create
/// reservations placed at known log offsets, used
/// for writing persistent data structures that need
/// to know where to find persisted bits in the future.
///
/// # Working with `LockFreeLog`
///
/// ```
/// use rsdb::Log;
///
/// let log = rsdb::Config::default().log();
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
    pub(super) iobufs: Arc<IoBufs>,
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
        let iobufs = Arc::new(IoBufs::new(config.clone()));

        let flusher_shutdown = Arc::new(AtomicBool::new(false));
        let flusher_handle = config.get_flush_every_ms().map(|flush_every_ms| {
            periodic_flusher::flusher(
                "log flusher".to_owned(),
                iobufs.clone(),
                flusher_shutdown.clone(),
                flush_every_ms,
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
        let cached_f = self.config().cached_file();
        let mut f = cached_f.borrow_mut();
        f.seek(SeekFrom::Start(id))?;

        let mut valid = [0u8; 1];
        f.read_exact(&mut valid)?;

        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf)?;

        let len32: u32 = unsafe { std::mem::transmute(len_buf) };
        let len = len32 as usize;
        let max = self.config().get_io_buf_size() - HEADER_LEN;
        if len > max {
            #[cfg(feature = "log")]
            error!("log read invalid message length, {} should be <= {}", len, max);
            return Ok(LogRead::Corrupted(len));
        }

        if valid[0] == 0 {
            return Ok(LogRead::Aborted(len));
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
            return Ok(LogRead::Corrupted(len));
        }

        #[cfg(feature = "zstd")]
        {
            if self.config().get_use_compression() {
                Ok(LogRead::Flush(decompress(&*buf, max).unwrap(), len))
            } else {
                Ok(LogRead::Flush(buf, len))
            }
        }

        #[cfg(not(feature = "zstd"))] Ok(LogRead::Flush(buf, len))
    }

    /// returns the current stable offset written to disk
    fn stable_offset(&self) -> LogID {
        self.iobufs.stable()
    }

    /// blocks until the specified id has been made stable on disk
    fn make_stable(&self, id: LogID) {
        let mut spins = 0;
        loop {
            self.iobufs.flush();
            spins += 1;
            if spins > 2_000_000 {
                #[cfg(feature = "log")]
                debug!("have spun >2000000x in make_stable");
                spins = 0;
            }
            let cur = self.iobufs.stable();
            if cur > id {
                return;
            }
        }
    }

    /// deallocates the data part of a log id
    fn punch_hole(&self, id: LogID) {
        // we zero out the valid byte, and use fallocate to punch a hole
        // in the actual data, but keep the len for recovery.
        let cached_f = self.config().cached_file();
        let mut f = cached_f.borrow_mut();
        // zero out valid bit
        f.seek(SeekFrom::Start(id)).unwrap();
        let zeros = vec![0];
        f.write_all(&*zeros).unwrap();
        f.seek(SeekFrom::Start(id + 1)).unwrap();
        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf).unwrap();

        #[cfg(feature = "libc")]
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let len32: u32 = unsafe { std::mem::transmute(len_buf) };
            let len = len32 as usize;
            let mode = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE;
            let fd = f.as_raw_fd();
            unsafe {
                // 5 is valid (1) + len (4), 2 is crc16
                fallocate(fd, mode, id as i64 + 5, len as i64 + 2);
            }
        }
    }
}

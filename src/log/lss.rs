use std::io::{Error, Read, Seek};
use std::io::ErrorKind::{Other, UnexpectedEof};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

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
        #[cfg(feature = "env_logger")]
        let _r = env_logger::init();

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

    /// Scan the log file if we don't know of any
    /// Lsn offsets yet.
    pub fn scan_segment_lsns(&self) {
        let mut sa = self.iobufs.segment_accountant.lock().unwrap();
        if sa.is_recovered() {
            return;
        }

        let mut cursor = 0;
        loop {
            // in the future this can be optimized to just read
            // the initial header at that position... but we need to
            // make sure the segment is not torn
            if let Ok(segment) = self.read_segment(cursor) {
                sa.recover(segment.lsn, segment.position);
                cursor += self.config().get_io_buf_size() as LogID;
            } else {
                break;
            }
        }
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

    /// read a segment of log messages. Only call after
    /// pausing segment rewriting on the segment accountant!
    fn read_segment(&self, id: LogID) -> io::Result<Segment> {
        let segment_header_read = self.read(id)?;
        println!("shr: {:?}", segment_header_read);
        if segment_header_read.is_corrupt() {
            return Err(Error::new(Other, "corrupt segment"));
        }
        if segment_header_read.is_zeroed() {
            return Err(Error::new(Other, "empty segment"));
        }
        let lsn_vec = segment_header_read.flush().unwrap();
        let mut lsn_buf = [0u8; 8];
        lsn_buf.copy_from_slice(&*lsn_vec);
        let lsn: Lsn = unsafe { std::mem::transmute(lsn_buf) };

        let mut buf = Vec::with_capacity(self.config().get_io_buf_size());
        unsafe {
            buf.set_len(self.config().get_io_buf_size());
        }
        {
            let cached_f = self.config().cached_file();
            let mut f = cached_f.borrow_mut();
            f.seek(SeekFrom::Start(id))?;

            f.read_exact(&mut *buf)?;
        }

        Ok(Segment {
            buf: buf,
            lsn: lsn,
            read_offset: 8 + HEADER_LEN,
            position: id,
        })
    }

    /// Return an iterator over the log, starting with
    /// a specified offset.
    fn iter_from(&self, lsn: Lsn) -> LogIter<Self> {
        let sa = self.iobufs.segment_accountant.lock().unwrap();
        let segment_iter = sa.segment_snapshot_iter_from(lsn);

        LogIter {
            log: self,
            segment: None,
            segment_iter: segment_iter,
        }
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
            // this is + 5 not + HEADER_LEN because we're 2 short when
            // we started seeking for a non-zero byte (crc16)
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

    /// blocks until the specified log sequence number has
    /// been made stable on disk
    fn make_stable(&self, lsn: Lsn) {
        let start = clock();
        let mut spins = 0;
        while self.iobufs.stable() <= lsn {
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
}

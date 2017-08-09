use std::io::{Read, Seek, Write};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

#[cfg(target_os="linux")]
use libc::{FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE, fallocate};

use zstd::block::decompress;

use super::*;

/// `LockFreeLog` is responsible for putting data on disk, and retrieving
/// it later on.
pub struct LockFreeLog {
    pub(super) iobufs: Arc<IOBufs>,
    flusher_shutdown: Arc<AtomicBool>,
    flusher_handle: Option<std::thread::JoinHandle<()>>,
}

unsafe impl Send for LockFreeLog {}
unsafe impl Sync for LockFreeLog {}

impl Drop for LockFreeLog {
    fn drop(&mut self) {
        self.flusher_shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(join_handle) = self.flusher_handle.take() {
            join_handle.join().unwrap();
        }
    }
}

impl LockFreeLog {
    /// create new lock-free log
    pub fn start_system(config: Config) -> LockFreeLog {
        let iobufs = Arc::new(IOBufs::new(config.clone()));

        let flusher_shutdown = Arc::new(AtomicBool::new(false));
        let flusher_handle = config.get_flush_every_ms().map(|flush_every_ms| {
            periodic_flusher::flusher("log flusher".to_owned(),
                                      iobufs.clone(),
                                      flusher_shutdown.clone(),
                                      flush_every_ms)
                .unwrap()
        });

        LockFreeLog {
            iobufs: iobufs,
            flusher_shutdown: flusher_shutdown,
            flusher_handle: flusher_handle,
        }
    }

    fn read_with_raw_sz(&self, id: LogID) -> (io::Result<Result<Vec<u8>, usize>>, usize) {
        let cached_f = self.config().cached_file();
        let mut f = cached_f.borrow_mut();
        if let Err(res) = f.seek(SeekFrom::Start(id)) {
            return (Err(res), 0);
        }

        let mut valid = [0u8; 1];
        if let Err(res) = f.read_exact(&mut valid) {
            return (Err(res), 0);
        }

        let mut len_buf = [0u8; 4];
        if let Err(res) = f.read_exact(&mut len_buf) {
            return (Err(res), 0);
        }
        let len = ops::array_to_usize(len_buf);
        let max = self.config().get_io_buf_size() - HEADER_LEN;
        if len > max {
            let msg = format!("read invalid message length, {} should be <= {}", len, max);
            return (Err(Error::new(ErrorKind::Other, msg)), len);
        }

        if valid[0] == 0 {
            return (Ok(Err(len)), len);
        }

        let mut crc16_buf = [0u8; 2];
        if let Err(res) = f.read_exact(&mut crc16_buf) {
            return (Err(res), len);
        }

        let mut buf = Vec::with_capacity(len);
        unsafe {
            buf.set_len(len);
        }
        if let Err(res) = f.read_exact(&mut buf) {
            return (Err(res), len);
        }

        let checksum = crc16_arr(&buf);
        if checksum != crc16_buf {
            let msg = format!("read data failed crc16 checksum, {:?} should be {:?}",
                              checksum,
                              crc16_buf);
            return (Err(Error::new(ErrorKind::Other, msg)), len);
        }

        if self.config().get_use_compression() {
            (Ok(Ok(decompress(&*buf, max).unwrap())), len)
        } else {
            (Ok(Ok(buf)), len)
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

    /// read a buffer from the disk
    fn read(&self, id: LogID) -> io::Result<Result<Vec<u8>, usize>> {
        let (res, _sz) = self.read_with_raw_sz(id);
        res
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
            if spins > 2000000 {
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

        #[cfg(target_os="linux")]
        {
            use std::os::unix::io::AsRawFd;
            let len = ops::array_to_usize(len_buf);
            let mode = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE;
            let fd = f.as_raw_fd();
            unsafe {
                // 5 is valid (1) + len (4), 2 is crc16
                fallocate(fd, mode, id as i64 + 5, len as i64 + 2);
            }
        }
    }
}

impl<'a> Iterator for LogIter<'a, LockFreeLog> {
    type Item = (LogID, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let offset = self.next_offset;
            let (log_read, len) = self.log.read_with_raw_sz(self.next_offset);
            if let Ok(buf_opt) = log_read {
                match buf_opt {
                    Ok(buf) => {
                        self.next_offset += len as LogID + HEADER_LEN as LogID;
                        return Some((offset, buf));
                    }
                    Err(len) => self.next_offset += len as LogID + HEADER_LEN as LogID,
                }
            } else {
                return None;
            }
        }
    }
}

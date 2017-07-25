use std::io::{Read, Seek, Write};
use std::os::unix::io::AsRawFd;

use super::*;

/// `LockFreeLog` is responsible for putting data on disk, and retrieving
/// it later on.
pub struct LockFreeLog {
    iobufs: IOBufs,
}

unsafe impl Send for LockFreeLog {}
unsafe impl Sync for LockFreeLog {}

impl LockFreeLog {
    /// create new lock-free log
    pub fn start_system(path: String) -> LockFreeLog {
        let cur_id = fs::metadata(path.clone()).map(|m| m.len()).unwrap_or(0);

        let mut options = fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);
        let file = options.open(path).unwrap();

        let iobufs = IOBufs::new(file, cur_id);

        LockFreeLog { iobufs: iobufs }
    }
}

impl Log for LockFreeLog {
    /// claim a spot on disk, which can later be filled or aborted
    fn reserve(&self, buf: Vec<u8>) -> Reservation {
        self.iobufs.reserve(buf)
    }

    /// write a buffer to disk
    fn write(&self, buf: Vec<u8>) -> LogID {
        self.iobufs.reserve(buf).complete()
    }

    /// read a buffer from the disk
    fn read(&self, id: LogID) -> io::Result<Option<Vec<u8>>> {
        let mut f = self.iobufs.file.lock().unwrap();
        f.seek(SeekFrom::Start(id))?;

        let mut valid = [0u8; 1];
        f.read_exact(&mut valid)?;
        if valid[0] == 0 {
            return Ok(None);
        }

        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf)?;
        let len = ops::array_to_usize(len_buf);
        if len > MAX_BUF_SZ {
            let msg = format!("read invalid message length, {} should be <= {}",
                              len,
                              MAX_BUF_SZ);
            return Err(Error::new(ErrorKind::Other, msg));
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
            let msg = format!("read data failed crc16 checksum, {:?} should be {:?}",
                              checksum,
                              crc16_buf);
            return Err(Error::new(ErrorKind::Other, msg));
        }

        Ok(Some(buf))
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
                println!("{:?} have spun >2000000x in make_stable",
                         thread::current().name());
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
        let mut f = self.iobufs.file.lock().unwrap();
        // zero out valid bit
        f.seek(SeekFrom::Start(id)).unwrap();
        let zeros = vec![0];
        f.write_all(&*zeros).unwrap();
        f.seek(SeekFrom::Start(id + 1)).unwrap();
        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf).unwrap();

        let len = ops::array_to_usize(len_buf);
        #[cfg(target_os="linux")]
        let mode = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE;
        let fd = f.as_raw_fd();

        unsafe {
            // 5 is valid (1) + len (4), 2 is crc16
            #[cfg(target_os="linux")]
            fallocate(fd, mode, id as i64 + 5, len as i64 + 2);
        }
    }
}

#[cfg(windows)]
use std::sync::Mutex;

use super::*;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

/// Multithreaded IO support for Files
pub trait Pio {
    /// Read from a specific offset without changing
    /// the underlying file offset.
    fn pread_exact(
        &self,
        mut buf: &mut [u8],
        mut offset: LogID,
    ) -> io::Result<()>;

    /// Write to a specific offset without changing
    /// the underlying file offset.
    fn pwrite_all(&self, mut buf: &[u8], mut offset: LogID) -> io::Result<()>;
}

// On systems that support pread/pwrite, use them underneath.
#[cfg(unix)]
impl Pio for std::fs::File {
    fn pread_exact(
        &self,
        mut buf: &mut [u8],
        mut offset: LogID,
    ) -> io::Result<()> {
        while !buf.is_empty() {
            match self.read_at(buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    offset += n as LogID;
                    let tmp = buf;
                    buf = &mut tmp[n..];
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ))
        } else {
            Ok(())
        }
    }

    fn pwrite_all(&self, mut buf: &[u8], mut offset: LogID) -> io::Result<()> {
        while !buf.is_empty() {
            match self.write_at(buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ))
                }
                Ok(n) => {
                    offset += n as LogID;
                    buf = &buf[n..]
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

// HACK HACK HACK get this working with real parallel IO
#[cfg(windows)]
lazy_static! {
    pub static ref GLOBAL_FILE_LOCK: Mutex<()> = Mutex::new(());
}

#[cfg(windows)]
impl Pio for std::fs::File {
    fn pread_exact(
        &self,
        mut buf: &mut [u8],
        mut offset: LogID,
    ) -> io::Result<()> {
        // HACK HACK HACK get this working with real parallel IO
        let _lock = GLOBAL_FILE_LOCK.lock().unwrap();

        while !buf.is_empty() {
            match self.seek_read(buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    offset += n as LogID;
                    let tmp = buf;
                    buf = &mut tmp[n..];
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ))
        } else {
            Ok(())
        }
    }

    fn pwrite_all(&self, mut buf: &[u8], mut offset: LogID) -> io::Result<()> {
        // HACK HACK HACK get this working with real parallel IO
        let _lock = GLOBAL_FILE_LOCK.lock().unwrap();

        while !buf.is_empty() {
            match self.seek_write(buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ))
                }
                Ok(n) => {
                    offset += n as LogID;
                    buf = &buf[n..]
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

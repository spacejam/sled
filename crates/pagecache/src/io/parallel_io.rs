use super::*;

/// Multithreaded IO support for Files
#[cfg(unix)]
pub trait Pio: std::os::unix::fs::FileExt {
    /// Read from a specific offset without changing
    /// the underlying file offset.
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

    /// Write to a specific offset without changing
    /// the underlying file offset.
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

impl Pio for std::fs::File {}

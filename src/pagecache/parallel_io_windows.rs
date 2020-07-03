use std::convert::TryFrom;
use std::fs::File;
use std::io;
use std::os::windows::fs::FileExt;

use super::LogOffset;

fn seek_read_exact<F: FileExt>(
    file: &mut F,
    mut buf: &mut [u8],
    mut offset: u64,
) -> io::Result<()> {
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => break,
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
                offset += n as u64;
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

fn seek_write_all<F: FileExt>(
    file: &mut F,
    mut buf: &[u8],
    mut offset: u64,
) -> io::Result<()> {
    while !buf.is_empty() {
        match file.seek_write(buf, offset) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

pub(crate) fn pread_exact_or_eof(
    file: &File,
    mut buf: &mut [u8],
    offset: LogOffset,
) -> io::Result<usize> {
    let mut total = 0_usize;
    while !buf.is_empty() {
        match file.seek_read(buf, offset + u64::try_from(total).unwrap()) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let tmp = buf;
                buf = &mut tmp[n..];
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

pub(crate) fn pread_exact(
    file: &File,
    buf: &mut [u8],
    offset: LogOffset,
) -> io::Result<()> {
    let mut f = file.try_clone()?;
    seek_read_exact(&mut f, buf, offset)
}

pub(crate) fn pwrite_all(
    file: &File,
    buf: &[u8],
    offset: LogOffset,
) -> io::Result<()> {
    let mut f = file.try_clone()?;
    seek_write_all(&mut f, buf, offset)
}

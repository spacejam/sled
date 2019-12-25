use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;

use super::LogOffset;

pub(crate) fn pread_exact(
    file: &File,
    buf: &mut [u8],
    offset: LogOffset,
) -> io::Result<()> {
    file.read_exact_at(buf, offset)
}

pub(crate) fn pwrite_all(
    file: &File,
    buf: &[u8],
    offset: LogOffset,
) -> io::Result<()> {
    file.write_all_at(buf, offset)
}

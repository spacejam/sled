use std::convert::TryFrom;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;

use super::{LogOffset, Result};

pub(crate) fn pread_exact_or_eof(
    file: &File,
    mut buf: &mut [u8],
    offset: LogOffset,
) -> Result<usize> {
    let mut total = 0_usize;
    while !buf.is_empty() {
        match file.read_at(buf, offset + u64::try_from(total).unwrap()) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let tmp = buf;
                buf = &mut tmp[n..];
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(total)
}

pub(crate) fn pread_exact(
    file: &File,
    buf: &mut [u8],
    offset: LogOffset,
) -> Result<()> {
    file.read_exact_at(buf, offset).map_err(From::from)
}

pub(crate) fn pwrite_all(
    file: &File,
    buf: &[u8],
    offset: LogOffset,
) -> Result<()> {
    #[cfg(feature = "failpoints")]
    {
        crate::debug_delay();
        if crate::fail::is_active("pwrite") {
            return Err(crate::Error::FailPoint);
        }

        if crate::fail::is_active("pwrite partial") && !buf.is_empty() {
            // TODO perturb the length more
            let len = buf.len() / 2;

            file.write_all_at(&buf[..len], offset)?;
            return Err(crate::Error::FailPoint);
        }
    };
    file.write_all_at(buf, offset).map_err(From::from)
}

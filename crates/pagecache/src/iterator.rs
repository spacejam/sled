use std::io;

use self::reader::LogReader;
use super::*;

pub struct LogIter {
    pub config: Config,
    pub segment_iter: Box<dyn Iterator<Item = (Lsn, LogId)>>,
    pub segment_base: Option<LogId>,
    pub max_lsn: Lsn,
    pub cur_lsn: Lsn,
}

impl Iterator for LogIter {
    type Item = (LogKind, PageId, Lsn, DiskPtr, usize);

    fn next(&mut self) -> Option<Self::Item> {
        // If segment is None, get next on segment_iter, panic
        // if we can't read something we expect to be able to,
        // return None if there are no more remaining segments.
        loop {
            let remaining_seg_too_small_for_msg = !valid_entry_offset(
                self.cur_lsn as LogId,
                self.config.io_buf_size,
            );

            if self.segment_base.is_none() || remaining_seg_too_small_for_msg {
                if let Some((next_lsn, next_lid)) = self.segment_iter.next() {
                    assert!(
                        next_lsn + (self.config.io_buf_size as Lsn)
                            >= self.cur_lsn,
                        "caller is responsible for providing segments \
                         that contain the initial cur_lsn value or higher"
                    );

                    #[cfg(target_os = "linux")]
                    self.fadvise_willneed(next_lid);

                    if let Err(e) = self.read_segment(next_lsn, next_lid) {
                        debug!(
                            "hit snap while reading segments in \
                             iterator: {:?}",
                            e
                        );
                        return None;
                    }
                } else {
                    trace!("no segments remaining to iterate over");
                    return None;
                }
            }

            if self.cur_lsn > self.max_lsn {
                // all done
                trace!("hit max_lsn in iterator, stopping");
                return None;
            }

            let lid = self.segment_base.unwrap()
                + (self.cur_lsn % self.config.io_buf_size as Lsn) as LogId;

            let f = &self.config.file;

            match f.read_message(lid, self.cur_lsn, &self.config) {
                Ok(LogRead::Blob(header, _buf, blob_ptr)) => {
                    trace!("read blob flush in LogIter::next");
                    let sz = MSG_HEADER_LEN + BLOB_INLINE_LEN;
                    self.cur_lsn += sz as Lsn;

                    return Some((
                        LogKind::from(header.kind),
                        header.pid,
                        header.lsn,
                        DiskPtr::Blob(lid, blob_ptr),
                        sz,
                    ));
                }
                Ok(LogRead::Inline(header, _buf, on_disk_len)) => {
                    trace!("read inline flush in LogIter::next");
                    let sz = MSG_HEADER_LEN + on_disk_len as usize;
                    self.cur_lsn += sz as Lsn;

                    return Some((
                        LogKind::from(header.kind),
                        header.pid,
                        header.lsn,
                        DiskPtr::Inline(lid),
                        sz,
                    ));
                }
                Ok(LogRead::BatchManifest(last_lsn_in_batch)) => {
                    if last_lsn_in_batch > self.max_lsn {
                        return None;
                    } else {
                        self.cur_lsn +=
                            (MSG_HEADER_LEN + BATCH_MANIFEST_INLINE_LEN) as Lsn;
                        continue;
                    }
                }
                Ok(LogRead::Failed(_, on_disk_len)) => {
                    trace!("read zeroed in LogIter::next");
                    self.cur_lsn +=
                        Lsn::from(MSG_HEADER_LEN as u32 + on_disk_len);
                }
                Ok(LogRead::Corrupted(_len)) => {
                    trace!(
                        "read corrupted msg in LogIter::next as lid {} lsn {}",
                        lid,
                        self.cur_lsn
                    );
                    return None;
                }
                Ok(LogRead::Pad(_lsn)) => {
                    self.segment_base.take();

                    continue;
                }
                Ok(LogRead::DanglingBlob(header, blob_ptr)) => {
                    debug!(
                        "encountered dangling blob \
                         pointer at lsn {} ptr {}",
                        header.lsn, blob_ptr
                    );
                    self.cur_lsn += (MSG_HEADER_LEN + BLOB_INLINE_LEN) as Lsn;
                    continue;
                }
                Err(e) => {
                    debug!(
                        "failed to read log message at lid {} \
                         with expected lsn {} during iteration: {}",
                        lid, self.cur_lsn, e
                    );
                    return None;
                }
            }
        }
    }
}

impl LogIter {
    /// read a segment of log messages. Only call after
    /// pausing segment rewriting on the segment accountant!
    fn read_segment(&mut self, lsn: Lsn, offset: LogId) -> Result<()> {
        trace!(
            "LogIter::read_segment lsn: {:?} cur_lsn: {:?}",
            lsn,
            self.cur_lsn
        );
        // we add segment_len to this check because we may be getting the
        // initial segment that is a bit behind where we left off before.
        assert!(lsn + self.config.io_buf_size as Lsn >= self.cur_lsn);
        let f = &self.config.file;
        let segment_header = f.read_segment_header(offset)?;
        if offset % self.config.io_buf_size as LogId != 0 {
            debug!("segment offset not divisible by segment length");
            return Err(Error::Corruption {
                at: DiskPtr::Inline(offset),
            });
        }
        if segment_header.lsn % self.config.io_buf_size as Lsn != 0 {
            debug!(
                "expected a segment header lsn that is divisible \
                 by the io_buf_size ({}) instead it was {}",
                self.config.io_buf_size, segment_header.lsn
            );
            return Err(Error::Corruption {
                at: DiskPtr::Inline(offset),
            });
        }

        if segment_header.lsn != lsn {
            // this page was torn, nothing to read
            debug!(
                "segment header lsn ({}) != expected lsn ({})",
                segment_header.lsn, lsn
            );
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "encountered torn segment",
            )
            .into());
        }

        trace!("read segment header {:?}", segment_header);

        self.cur_lsn = segment_header.lsn + SEG_HEADER_LEN as Lsn;
        self.segment_base = Some(offset);

        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn fadvise_willneed(&self, lid: LogId) {
        use std::os::unix::io::AsRawFd;

        let f = &self.config.file;
        let ret = unsafe {
            libc::posix_fadvise(
                f.as_raw_fd(),
                lid as libc::off_t,
                self.config.io_buf_size as libc::off_t,
                libc::POSIX_FADV_WILLNEED,
            )
        };
        if ret != 0 {
            panic!(
                "failed to call fadvise: {}",
                std::io::Error::from_raw_os_error(ret)
            );
        }
    }
}

fn valid_entry_offset(lid: LogId, segment_len: usize) -> bool {
    let seg_start = lid / segment_len as LogId * segment_len as LogId;

    let max_lid = seg_start + segment_len as LogId - MSG_HEADER_LEN as LogId;

    let min_lid = seg_start + SEG_HEADER_LEN as LogId;

    lid >= min_lid && lid <= max_lid
}

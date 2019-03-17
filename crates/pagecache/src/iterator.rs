use std::io;

use self::reader::LogReader;
use super::*;

pub struct LogIter {
    pub config: Config,
    pub segment_iter: Box<dyn Iterator<Item = (Lsn, LogId)>>,
    pub segment_base: Option<LogId>,
    pub max_lsn: Lsn,
    pub cur_lsn: Lsn,
    pub trailer: Option<Lsn>,
}

impl Iterator for LogIter {
    type Item = (Lsn, DiskPtr, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        // If segment is None, get next on segment_iter, panic
        // if we can't read something we expect to be able to,
        // return None if there are no more remaining segments.
        loop {
            let remaining_seg_too_small_for_msg = !valid_entry_offset(
                self.cur_lsn as LogId,
                self.config.io_buf_size,
            );

            if self.trailer.is_none()
                && remaining_seg_too_small_for_msg
            {
                // We've read to the end of a torn
                // segment and should stop now.
                trace!(
                    "trailer is none, ending iteration at {}",
                    self.max_lsn
                );
                return None;
            } else if self.segment_base.is_none()
                || remaining_seg_too_small_for_msg
            {
                if let Some((next_lsn, next_lid)) =
                    self.segment_iter.next()
                {
                    assert!(
                        next_lsn + (self.config.io_buf_size as Lsn) >= self.cur_lsn,
                        "caller is responsible for providing segments \
                            that contain the initial cur_lsn value or higher"
                    );

                    #[cfg(target_os = "linux")]
                    self.fadvise_willneed(next_lid);

                    if let Err(e) =
                        self.read_segment(next_lsn, next_lid)
                    {
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
                + (self.cur_lsn % self.config.io_buf_size as Lsn)
                    as LogId;

            let f = &self.config.file;
            match f.read_message(lid, &self.config) {
                Ok(LogRead::Blob(lsn, buf, blob_ptr)) => {
                    if lsn != self.cur_lsn {
                        debug!("read Flush with bad lsn");
                        return None;
                    }
                    trace!("read blob flush in LogIter::next");
                    self.cur_lsn +=
                        (MSG_HEADER_LEN + BLOB_INLINE_LEN) as Lsn;
                    return Some((
                        lsn,
                        DiskPtr::Blob(lid, blob_ptr),
                        buf,
                    ));
                }
                Ok(LogRead::Inline(lsn, buf, on_disk_len)) => {
                    if lsn != self.cur_lsn {
                        debug!("read Flush with bad lsn");
                        return None;
                    }
                    trace!("read inline flush in LogIter::next");
                    self.cur_lsn +=
                        (MSG_HEADER_LEN + on_disk_len) as Lsn;
                    return Some((lsn, DiskPtr::Inline(lid), buf));
                }
                Ok(LogRead::Failed(lsn, on_disk_len)) => {
                    if lsn != self.cur_lsn {
                        debug!("read Failed with bad lsn");
                        return None;
                    }
                    trace!("read zeroed in LogIter::next");
                    self.cur_lsn +=
                        (MSG_HEADER_LEN + on_disk_len) as Lsn;
                }
                Ok(LogRead::Corrupted(_len)) => {
                    trace!("read corrupted msg in LogIter::next as lid {} lsn {}",
                               lid, self.cur_lsn);
                    return None;
                }
                Ok(LogRead::Pad(lsn)) => {
                    if lsn != self.cur_lsn {
                        debug!("read Pad with bad lsn");
                        return None;
                    }

                    if self.trailer.is_none() {
                        // This segment was torn, nothing left to read.
                        trace!("no segment trailer found, ending iteration");
                        return None;
                    }

                    self.segment_base.take();

                    self.trailer.take();
                    continue;
                }
                Ok(LogRead::DanglingBlob(lsn, blob_ptr)) => {
                    debug!(
                        "encountered dangling blob \
                         pointer at lsn {} ptr {}",
                        lsn, blob_ptr
                    );
                    self.cur_lsn +=
                        (MSG_HEADER_LEN + BLOB_INLINE_LEN) as Lsn;
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
    fn read_segment(
        &mut self,
        lsn: Lsn,
        offset: LogId,
    ) -> Result<(), ()> {
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

        let trailer_offset = offset
            + self.config.io_buf_size as LogId
            - SEG_TRAILER_LEN as LogId;
        let trailer_lsn = segment_header.lsn
            + self.config.io_buf_size as Lsn
            - SEG_TRAILER_LEN as Lsn;

        trace!("trying to read trailer from {}", trailer_offset);
        let segment_trailer = f.read_segment_trailer(trailer_offset);

        trace!("read segment header {:?}", segment_header);
        trace!("read segment trailer {:?}", segment_trailer);

        let trailer_lsn = segment_trailer.ok().and_then(|st| {
            if st.ok && st.lsn == trailer_lsn {
                Some(st.lsn)
            } else {
                trace!("segment trailer corrupted, not reading next segment");
                None
            }
        });

        self.trailer = trailer_lsn;
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

    let max_lid = seg_start + segment_len as LogId
        - SEG_TRAILER_LEN as LogId
        - MSG_HEADER_LEN as LogId;

    let min_lid = seg_start + SEG_HEADER_LEN as LogId;

    lid >= min_lid && lid <= max_lid
}

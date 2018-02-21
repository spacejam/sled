use std::io;

use self::reader::LogReader;
use super::*;

pub struct LogIter {
    pub config: Config,
    pub segment_iter: Box<Iterator<Item = (Lsn, LogID)>>,
    pub segment_base: Option<LogID>,
    pub segment_len: usize,
    pub use_compression: bool,
    pub max_lsn: Lsn,
    pub cur_lsn: Lsn,
    pub trailer: Option<Lsn>,
}

impl Iterator for LogIter {
    type Item = (Lsn, LogID, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        // If segment is None, get next on segment_iter, panic
        // if we can't read something we expect to be able to,
        // return None if there are no more remaining segments.
        loop {
            let invalid =
                !valid_entry_offset(self.cur_lsn as LogID, self.segment_len);
            if self.trailer.is_none() && invalid {
                // We've read to the end of a torn
                // segment and should stop now.
                return None;
            } else if self.segment_base.is_none() || invalid {
                if let Some((next_lsn, next_lid)) = self.segment_iter.next() {
                    assert!(
                        next_lsn + (self.segment_len as Lsn) >= self.cur_lsn,
                        "caller is responsible for providing segments \
                            that contain the initial cur_lsn value or higher"
                    );
                    if let Err(e) = self.read_segment(next_lsn, next_lid) {
                        debug!(
                            "hit snap while reading segments in \
                            iterator: {:?}",
                            e
                        );
                        return None;
                    }
                } else {
                    return None;
                }
            }

            if self.cur_lsn > self.max_lsn {
                // all done
                return None;
            }

            let lid = self.segment_base.unwrap() +
                (self.cur_lsn % self.segment_len as Lsn) as LogID;

            if self.max_lsn < lid as Lsn {
                // we've hit the end of the log.
                trace!(
                    "in LogIter::next self.max_lsn {} < lid {}",
                    self.max_lsn,
                    lid
                );
                return None;
            }

            if let Ok(f) = self.config.file() {
                match f.read_message(
                    lid,
                    self.segment_len,
                    self.use_compression,
                ) {
                    Ok(LogRead::Flush(lsn, buf, on_disk_len)) => {
                        trace!("read flush in LogIter::next");
                        self.cur_lsn += (MSG_HEADER_LEN + on_disk_len) as Lsn;
                        return Some((lsn, lid, buf));
                    }
                    Ok(LogRead::Failed(on_disk_len)) => {
                        trace!("read zeroed in LogIter::next");
                        self.cur_lsn += (MSG_HEADER_LEN + on_disk_len) as Lsn;
                    }
                    _ => {
                        trace!("read failed in LogIter::next");
                        if self.trailer.is_none() {
                            // This segment was torn, nothing left to read.
                            return None;
                        }
                        self.segment_base.take();
                        self.trailer.take();
                        continue;
                    }
                }
            } else {
                return None;
            }
        }
    }
}

impl LogIter {
    /// read a segment of log messages. Only call after
    /// pausing segment rewriting on the segment accountant!
    fn read_segment(&mut self, lsn: Lsn, offset: LogID) -> CacheResult<(), ()> {
        trace!(
            "LogIter::read_segment lsn: {:?} cur_lsn: {:?}",
            lsn,
            self.cur_lsn
        );
        // we add segment_len to this check because we may be getting the
        // initial segment that is a bit behind where we left off before.
        assert!(lsn + self.segment_len as Lsn >= self.cur_lsn);
        let f = self.config.file()?;
        let segment_header = f.read_segment_header(offset)?;
        if offset % self.segment_len as LogID != 0 {
            debug!("segment offset not divisible by segment length");
            return Err(Error::Corruption {
                at: offset,
            });
        }
        if segment_header.lsn % self.segment_len as Lsn != 0 {
            debug!(
                "expected a segment header lsn that is divisible \
            by the io_buf_size ({}) instead it was {}",
                self.segment_len,
                segment_header.lsn
            );
            return Err(Error::Corruption {
                at: offset,
            });
        }

        if segment_header.lsn != lsn {
            // this page was torn, nothing to read
            error!(
                "segment header lsn ({}) != expected lsn ({})",
                segment_header.lsn,
                lsn
            );
            return Err(
                io::Error::new(
                    io::ErrorKind::Other,
                    "encountered torn segment",
                ).into(),
            );
        }

        let trailer_offset = offset + self.segment_len as LogID -
            SEG_TRAILER_LEN as LogID;
        let trailer_lsn = segment_header.lsn + self.segment_len as Lsn -
            SEG_TRAILER_LEN as Lsn;

        trace!("trying to read trailer from {}", trailer_offset);
        let segment_trailer = f.read_segment_trailer(trailer_offset);

        trace!("read segment header {:?}", segment_header);
        trace!("read segment trailer {:?}", segment_trailer);

        let trailer_lsn = segment_trailer.ok().and_then(|st| if st.ok &&
            st.lsn == trailer_lsn
        {
            Some(st.lsn)
        } else {
            None
        });

        self.trailer = trailer_lsn;
        self.cur_lsn = segment_header.lsn + SEG_HEADER_LEN as Lsn;
        self.segment_base = Some(offset);

        Ok(())
    }
}

fn valid_entry_offset(lid: LogID, segment_len: usize) -> bool {
    let seg_start = lid / segment_len as LogID * segment_len as LogID;

    let max_lid = seg_start + segment_len as LogID -
        SEG_TRAILER_LEN as LogID - MSG_HEADER_LEN as LogID;

    let min_lid = seg_start + SEG_HEADER_LEN as LogID;

    lid >= min_lid && lid <= max_lid
}

use super::*;

pub struct Iter<'a> {
    pub(super) log: &'a Log,
    pub(super) segment_iter: Box<Iterator<Item = (Lsn, LogID)>>,
    pub(super) segment_base: Option<LogID>,
    pub(super) max_lsn: Lsn,
    pub(super) cur_lsn: Lsn,
    pub(super) trailer: Option<Lsn>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (Lsn, LogID, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        // If segment is None, get next on segment_iter, panic
        // if we can't read something we expect to be able to,
        // return None if there are no more remaining segments.
        let segment_len = self.log.config().get_io_buf_size();
        loop {
            if self.cur_lsn >= self.max_lsn {
                // all done
                return None;
            }
            if self.segment_base.is_none() ||
                self.segment_base.unwrap() + segment_len as LogID <=
                    self.cur_lsn + SEG_TRAILER_LEN as LogID
            {
                if let Some((next_lsn, next_lid)) = self.segment_iter.next() {
                    self.read_segment(next_lsn, next_lid).unwrap();
                } else {
                    return None;
                }
            }

            let lid = self.segment_base.unwrap() +
                (self.cur_lsn % self.log.config().get_io_buf_size() as LogID);

            println!("max is {}", self.max_lsn);
            println!("lid is {}", lid);
            match self.log.read(lid) {
                Ok(LogRead::Flush(lsn, buf, on_disk_len)) => {
                    println!("{}", on_disk_len);
                    println!("cur_lsn before: {}", self.cur_lsn);
                    self.cur_lsn += (MSG_HEADER_LEN + on_disk_len) as LogID;
                    println!("cur_lsn after: {}", self.cur_lsn);
                    return Some((lsn, lid, buf));
                }
                Ok(LogRead::Zeroed(on_disk_len)) => {
                    self.cur_lsn += on_disk_len as LogID;
                }
                _ => {
                    if self.trailer.is_none() {
                        // This segment was torn, nothing left to read.
                        return None;
                    }
                    self.segment_base.take();
                    self.trailer.take();
                    continue;
                }
            }
        }
    }
}

impl<'a> Iter<'a> {
    /// read a segment of log messages. Only call after
    /// pausing segment rewriting on the segment accountant!
    fn read_segment(&mut self, lsn: Lsn, offset: LogID) -> std::io::Result<()> {
        let segment_len = self.log.config().get_io_buf_size();
        // println!("reading segment {}", offset);

        let cached_f = self.log.config().cached_file();
        let mut f = cached_f.borrow_mut();

        let segment_header = f.read_segment_header(offset)?;
        // println!("read sh {:?} at {}", sh, offset);
        assert_eq!(offset % segment_len as Lsn, 0);
        assert_eq!(segment_header.lsn % segment_len as Lsn, 0);
        assert_eq!(segment_header.lsn, lsn);
        assert!(segment_header.lsn + self.log.config().get_io_buf_size() as LogID >= self.cur_lsn);
        // TODO turn those asserts into returned Errors? Torn pages or invariant?

        let segment_trailer = f.read_segment_trailer(offset);

        let trailer_lsn = segment_trailer.ok().and_then(|st| if st.ok &&
            st.lsn == segment_header.lsn
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

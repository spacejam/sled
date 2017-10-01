use super::*;

pub struct Iter<'a> {
    pub(super) config: &'a Config,
    pub(super) segment_iter: Box<Iterator<Item = (Lsn, LogID)>>,
    pub(super) segment_base: Option<LogID>,
    pub(super) segment_len: usize,
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
        loop {
            println!("cur is {}", self.cur_lsn);
            println!("max is {}", self.max_lsn);
            println!("base is {:?}", self.segment_base);
            println!("segment_len {}", self.segment_len);
            if self.segment_base.is_none() ||
                !valid_entry_offset(self.cur_lsn, self.segment_len)
            {
                if let Some((next_lsn, next_lid)) = self.segment_iter.next() {
                    self.read_segment(next_lsn, next_lid).unwrap();
                } else {
                    return None;
                }
            }

            if self.cur_lsn > self.max_lsn {
                // all done
                return None;
            }

            let lid = self.segment_base.unwrap() +
                (self.cur_lsn % self.segment_len as LogID);

            if self.max_lsn <= lid {
                // we've hit the end of the log.
                return None;
            }

            println!("lid is {}", lid);
            let cached_f = self.config.cached_file();
            let mut f = cached_f.borrow_mut();
            match f.read_entry(lid, self.segment_len) {
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
        // println!("reading segment {}", offset);

        let cached_f = self.config.cached_file();
        let mut f = cached_f.borrow_mut();
        let segment_header = f.read_segment_header(offset)?;
        // println!("read sh {:?} at {}", sh, offset);
        assert_eq!(offset % self.segment_len as Lsn, 0);
        assert_eq!(segment_header.lsn % self.segment_len as Lsn, 0);
        assert_eq!(segment_header.lsn, lsn);
        assert!(segment_header.lsn + self.segment_len as LogID >= self.cur_lsn);
        // TODO turn those asserts into returned Errors? Torn pages or invariant?

        let segment_trailer = f.read_segment_trailer(offset);

        let trailer_lsn = segment_trailer.ok().and_then(|st| if st.ok &&
            st.lsn ==
                segment_header.lsn
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

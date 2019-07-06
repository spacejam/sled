use std::{collections::BTreeMap, io};

use rayon::prelude::*;

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
                trace!("hit max_lsn {} in iterator, stopping", self.max_lsn);
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
                libc::off_t::try_from(lid).unwrap(),
                libc::off_t::try_from(self.config.io_buf_size).unwrap(),
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

// Scan the log file if we don't know of any Lsn offsets yet,
// and recover the order of segments, and the highest Lsn.
fn scan_segment_lsns(
    min: Lsn,
    config: &Config,
) -> Result<(BTreeMap<Lsn, LogId>, Lsn)> {
    let segment_len = LogId::try_from(config.io_buf_size).unwrap();

    let f = &config.file;
    let file_len = f.metadata()?.len();
    let segments = (file_len / segment_len)
        + if file_len % segment_len < LogId::try_from(SEG_HEADER_LEN).unwrap() {
            0
        } else {
            1
        };
    trace!(
        "file len: {} segment len {} segments: {}",
        file_len,
        segment_len,
        segments
    );
    let headers: Vec<(LogId, SegmentHeader)> = (0..segments)
        .into_par_iter()
        .filter_map(|idx| {
            let base_lid = idx * segment_len;
            let segment = f.read_segment_header(base_lid).ok()?;
            trace!(
                "SA scanned header at lid {} during startup: {:?}",
                base_lid,
                segment
            );
            if segment.ok && segment.lsn >= min {
                assert_ne!(segment.lsn, Lsn::max_value());
                Some((base_lid, segment))
            } else {
                trace!(
                    "not using segment at lid {}, ok: {} lsn: {} min lsn: {}",
                    base_lid,
                    segment.ok,
                    segment.lsn,
                    min
                );
                None
            }
        })
        .collect();

    let mut ordering = BTreeMap::new();
    let mut max_header_stable_lsn = 0;

    for (lid, header) in headers {
        max_header_stable_lsn =
            std::cmp::max(header.max_stable_lsn, max_header_stable_lsn);

        let old = ordering.insert(header.lsn, lid);
        assert_eq!(
            old, None,
            "duplicate segment LSN {} detected at both {} and {}, \
             one should have been zeroed out during recovery",
            header.lsn, ordering[&header.lsn], lid
        );
    }

    debug!("ordering before clearing tears: {:?}", ordering);

    // Check that the segments above max_header_stable_lsn
    // properly link their previous segment pointers.
    let ordering =
        clean_tail_tears(max_header_stable_lsn, ordering, config, &f)?;

    Ok((ordering, max_header_stable_lsn))
}

// This ensures that the last <# io buffers> segments on
// disk connect via their previous segment pointers in
// the header. This is important because we expect that
// the last <# io buffers> segments will join up, and we
// never reuse buffers within this safety range.
fn clean_tail_tears(
    max_header_stable_lsn: Lsn,
    mut ordering: BTreeMap<Lsn, LogId>,
    config: &Config,
    f: &std::fs::File,
) -> Result<BTreeMap<Lsn, LogId>> {
    let io_buf_size = config.io_buf_size as Lsn;
    let lowest_lsn_in_tail: Lsn =
        ((max_header_stable_lsn / io_buf_size) - 1) * io_buf_size;

    let logical_tail = ordering
        .range(lowest_lsn_in_tail..)
        .map(|(lsn, lid)| (*lsn, *lid))
        .collect::<Vec<_>>();

    let iter = LogIter {
        config: config.clone(),
        segment_iter: Box::new(logical_tail.into_iter()),
        segment_base: None,
        max_lsn: Lsn::max_value(),
        cur_lsn: 0,
    };

    let tip: (Lsn, LogId) = iter
        .max_by_key(|(_kind, _pid, lsn, _ptr, _sz)| *lsn)
        .map(|(_, _, lsn, ptr, _)| (lsn, ptr.lid()))
        .unwrap_or_else(|| {
            if max_header_stable_lsn > 0 {
                (max_header_stable_lsn, ordering[&lowest_lsn_in_tail])
            } else {
                (0, 0)
            }
        });

    debug!(
        "filtering out segments after detected tear at lsn {} lid {}",
        tip.0, tip.1
    );
    for (lsn, lid) in ordering
        .range((std::ops::Bound::Excluded(tip.0), std::ops::Bound::Unbounded))
    {
        debug!("zeroing torn segment with lsn {} at lid {}", lsn, lid);

        f.pwrite_all(
            &*vec![MessageKind::Corrupted.into(); SEG_HEADER_LEN],
            *lid,
        )?;
        f.sync_all()?;
    }

    ordering = ordering
        .into_iter()
        .filter(|&(lsn, _lid)| lsn <= tip.0)
        .collect();

    Ok(ordering)
}

pub(super) fn raw_segment_iter_from(
    lsn: Lsn,
    config: &Config,
) -> Result<(LogIter, Lsn)> {
    let segment_len = config.io_buf_size as Lsn;
    let normalized_lsn = lsn / segment_len * segment_len;

    let (ordering, max_header_stable_lsn) = scan_segment_lsns(0, &config)?;

    trace!(
        "generated iterator over segments {:?} with lsn >= {}",
        ordering,
        normalized_lsn
    );

    let segment_iter = Box::new(
        ordering
            .into_iter()
            .filter(move |&(l, _)| l >= normalized_lsn),
    );

    Ok((
        LogIter {
            config: config.clone(),
            max_lsn: Lsn::max_value(),
            cur_lsn: SEG_HEADER_LEN as Lsn,
            segment_base: None,
            segment_iter,
        },
        max_header_stable_lsn,
    ))
}

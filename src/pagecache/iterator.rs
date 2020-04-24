use std::{collections::BTreeMap, io};

use super::{
    pread_exact_or_eof, read_message, read_segment_header, BasedBuf, DiskPtr,
    LogKind, LogOffset, LogRead, Lsn, SegmentHeader, SegmentNumber,
    MAX_MSG_HEADER_LEN, SEG_HEADER_LEN,
};
use crate::*;

pub struct LogIter {
    pub config: RunningConfig,
    pub segment_iter: Box<dyn Iterator<Item = (Lsn, LogOffset)>>,
    pub segment_base: Option<BasedBuf>,
    pub max_lsn: Lsn,
    pub cur_lsn: Lsn,
}

impl Iterator for LogIter {
    type Item = (LogKind, PageId, Lsn, DiskPtr, u64);

    fn next(&mut self) -> Option<Self::Item> {
        // If segment is None, get next on segment_iter, panic
        // if we can't read something we expect to be able to,
        // return None if there are no more remaining segments.
        loop {
            let remaining_seg_too_small_for_msg = !valid_entry_offset(
                LogOffset::try_from(self.cur_lsn).unwrap(),
                self.config.segment_size,
            );

            if self.segment_base.is_none() || remaining_seg_too_small_for_msg {
                if let Some((next_lsn, next_lid)) = self.segment_iter.next() {
                    assert!(
                        next_lsn + (self.config.segment_size as Lsn)
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

            // self.segment_base is `Some` now.
            let _measure = Measure::new(&M.read_segment_message);

            if self.cur_lsn > self.max_lsn {
                // all done
                trace!("hit max_lsn {} in iterator, stopping", self.max_lsn);
                return None;
            }

            let segment_base = &self.segment_base.as_ref().unwrap();

            let lid = segment_base.1
                + LogOffset::try_from(
                    self.cur_lsn % self.config.segment_size as Lsn,
                )
                .unwrap();

            let expected_segment_number = SegmentNumber(
                u64::try_from(self.cur_lsn).unwrap()
                    / u64::try_from(self.config.segment_size).unwrap(),
            );

            match read_message(
                &**segment_base,
                lid,
                expected_segment_number,
                &self.config,
            ) {
                Ok(LogRead::Blob(header, _buf, blob_ptr, inline_len)) => {
                    trace!("read blob flush in LogIter::next");
                    let lsn = self.cur_lsn;
                    self.cur_lsn += Lsn::from(inline_len);

                    return Some((
                        LogKind::from(header.kind),
                        header.pid,
                        lsn,
                        DiskPtr::Blob(lid, blob_ptr),
                        u64::from(inline_len),
                    ));
                }
                Ok(LogRead::Inline(header, _buf, inline_len)) => {
                    trace!(
                        "read inline flush with header {:?} in LogIter::next",
                        header,
                    );
                    let lsn = self.cur_lsn;
                    self.cur_lsn += Lsn::from(inline_len);

                    return Some((
                        LogKind::from(header.kind),
                        header.pid,
                        lsn,
                        DiskPtr::Inline(lid),
                        u64::from(inline_len),
                    ));
                }
                Ok(LogRead::BatchManifest(last_lsn_in_batch, inline_len)) => {
                    if last_lsn_in_batch > self.max_lsn {
                        return None;
                    } else {
                        self.cur_lsn += Lsn::from(inline_len);
                        continue;
                    }
                }
                Ok(LogRead::Canceled(inline_len)) => {
                    trace!("read zeroed in LogIter::next");
                    self.cur_lsn += Lsn::from(inline_len);
                }
                Ok(LogRead::Corrupted) => {
                    trace!(
                        "read corrupted msg in LogIter::next as lid {} lsn {}",
                        lid,
                        self.cur_lsn
                    );
                    return None;
                }
                Ok(LogRead::Cap(_segment_number)) => {
                    let _taken = self.segment_base.take().unwrap();

                    continue;
                }
                Ok(LogRead::DanglingBlob(_, blob_ptr, inline_len)) => {
                    debug!(
                        "encountered dangling blob \
                         pointer at lsn {} ptr {}",
                        self.cur_lsn, blob_ptr
                    );
                    self.cur_lsn += Lsn::from(inline_len);
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
    fn read_segment(&mut self, lsn: Lsn, offset: LogOffset) -> Result<()> {
        let _measure = Measure::new(&M.segment_read);
        trace!(
            "LogIter::read_segment lsn: {:?} cur_lsn: {:?}",
            lsn,
            self.cur_lsn
        );
        // we add segment_len to this check because we may be getting the
        // initial segment that is a bit behind where we left off before.
        assert!(lsn + self.config.segment_size as Lsn >= self.cur_lsn);
        let f = &self.config.file;
        let segment_header = read_segment_header(f, offset)?;
        if offset % self.config.segment_size as LogOffset != 0 {
            debug!("segment offset not divisible by segment length");
            return Err(Error::Corruption { at: DiskPtr::Inline(offset) });
        }
        if segment_header.lsn % self.config.segment_size as Lsn != 0 {
            debug!(
                "expected a segment header lsn that is divisible \
                 by the segment_size ({}) instead it was {}",
                self.config.segment_size, segment_header.lsn
            );
            return Err(Error::Corruption { at: DiskPtr::Inline(offset) });
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

        let mut buf = vec![0; self.config.segment_size];
        let size = pread_exact_or_eof(f, &mut buf, offset)?;

        trace!("setting stored segment buffer length to {} after read", size);
        buf.truncate(size);
        self.segment_base = Some(BasedBuf(buf, offset));

        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn fadvise_willneed(&self, lid: LogOffset) {
        use std::os::unix::io::AsRawFd;

        let f = &self.config.file;
        #[allow(unsafe_code)]
        let ret = unsafe {
            libc::posix_fadvise(
                f.as_raw_fd(),
                libc::off_t::try_from(lid).unwrap(),
                libc::off_t::try_from(self.config.segment_size).unwrap(),
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

fn valid_entry_offset(lid: LogOffset, segment_len: usize) -> bool {
    let seg_start = lid / segment_len as LogOffset * segment_len as LogOffset;

    let max_lid =
        seg_start + segment_len as LogOffset - MAX_MSG_HEADER_LEN as LogOffset;

    let min_lid = seg_start + SEG_HEADER_LEN as LogOffset;

    lid >= min_lid && lid <= max_lid
}

// Scan the log file if we don't know of any Lsn offsets yet,
// and recover the order of segments, and the highest Lsn.
fn scan_segment_lsns(
    min: Lsn,
    config: &RunningConfig,
) -> Result<(BTreeMap<Lsn, LogOffset>, Lsn, Vec<LogOffset>)> {
    fn fetch(
        idx: u64,
        min: Lsn,
        config: &RunningConfig,
    ) -> Option<(LogOffset, SegmentHeader)> {
        let segment_len = u64::try_from(config.segment_size).unwrap();
        let base_lid = idx * segment_len;
        let segment = read_segment_header(&config.file, base_lid).ok()?;
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
    };

    let segment_len = LogOffset::try_from(config.segment_size).unwrap();

    let f = &config.file;
    let file_len = f.metadata()?.len();
    let segments = (file_len / segment_len)
        + if file_len % segment_len
            < LogOffset::try_from(SEG_HEADER_LEN).unwrap()
        {
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

    let header_promises: Vec<OneShot<Option<(LogOffset, SegmentHeader)>>> = (0
        ..segments)
        .map({
            // let config = config.clone();
            move |idx| {
                threadpool::spawn({
                    let config = config.clone();
                    move || fetch(idx, min, &config)
                })
            }
        })
        .collect();

    let mut headers: Vec<(LogOffset, SegmentHeader)> = vec![];
    for promise in header_promises {
        let read_attempt =
            promise.wait().expect("thread pool should not crash");

        if let Some(completed_result) = read_attempt {
            headers.push(completed_result);
        }
    }

    let mut ordering = BTreeMap::new();
    let mut max_header_stable_lsn = min;

    for (lid, header) in headers {
        max_header_stable_lsn =
            std::cmp::max(header.max_stable_lsn, max_header_stable_lsn);

        if let Some(old) = ordering.insert(header.lsn, lid) {
            assert_eq!(
                old, lid,
                "duplicate segment LSN {} detected at both {} and {}, \
                 one should have been zeroed out during recovery",
                header.lsn, old, lid
            );
        }
    }

    debug!(
        "ordering before clearing tears: {:?}, \
         max_header_stable_lsn: {}",
        ordering, max_header_stable_lsn
    );

    // Check that the segments above max_header_stable_lsn
    // properly link their previous segment pointers.
    let (ordering, to_zero_after_snap_write) =
        clean_tail_tears(max_header_stable_lsn, ordering, config)?;

    Ok((ordering, max_header_stable_lsn, to_zero_after_snap_write))
}

// This ensures that the last <# io buffers> segments on
// disk connect via their previous segment pointers in
// the header. This is important because we expect that
// the last <# io buffers> segments will join up, and we
// never reuse buffers within this safety range.
fn clean_tail_tears(
    max_header_stable_lsn: Lsn,
    mut ordering: BTreeMap<Lsn, LogOffset>,
    config: &RunningConfig,
) -> Result<(BTreeMap<Lsn, LogOffset>, Vec<LogOffset>)> {
    let segment_size = config.segment_size as Lsn;

    // -1..(2 *  segment_size) - 1 => 0
    // otherwise the floor of the buffer
    let lowest_lsn_in_tail: Lsn =
        std::cmp::max(0, (max_header_stable_lsn / segment_size) * segment_size);

    let mut expected_present = lowest_lsn_in_tail;
    let mut missing_item_in_tail = None;

    let logical_tail = ordering
        .range(lowest_lsn_in_tail..)
        .map(|(lsn, lid)| (*lsn, *lid))
        .take_while(|(lsn, _lid)| {
            let matches = expected_present == *lsn;
            if !matches {
                debug!(
                    "failed to find expected segment \
                     at lsn {}, tear detected",
                    expected_present
                );
                missing_item_in_tail = Some(expected_present);
            }
            expected_present += segment_size;
            matches
        })
        .collect::<Vec<_>>();

    debug!(
        "in clean_tail_tears, found missing item in tail: {:?} \
         and we'll scan segments {:?} above lowest lsn {}",
        missing_item_in_tail, logical_tail, lowest_lsn_in_tail
    );

    let iter = LogIter {
        config: config.clone(),
        segment_iter: Box::new(logical_tail.into_iter()),
        segment_base: None,
        max_lsn: missing_item_in_tail.unwrap_or(Lsn::max_value()),
        cur_lsn: 0,
    };

    let tip: (Lsn, LogOffset) =
        iter.max_by_key(|(_kind, _pid, lsn, _ptr, _sz)| *lsn).map_or_else(
            || {
                if max_header_stable_lsn > 0 {
                    (lowest_lsn_in_tail, ordering[&lowest_lsn_in_tail])
                } else {
                    (0, 0)
                }
            },
            |(_, _, lsn, ptr, _)| (lsn, ptr.lid()),
        );

    debug!(
        "filtering out segments after detected tear at lsn {} lid {}",
        tip.0, tip.1
    );

    let mut to_zero_after_snap_write = vec![];

    for (lsn, lid) in ordering
        .range((std::ops::Bound::Excluded(tip.0), std::ops::Bound::Unbounded))
    {
        debug!(
            "marking torn segment with lsn {} at lid {} \
             as zeroable after snapshot is written",
            lsn, lid
        );
        to_zero_after_snap_write.push(*lid);
    }

    ordering =
        ordering.into_iter().filter(|&(lsn, _lid)| lsn <= tip.0).collect();

    Ok((ordering, to_zero_after_snap_write))
}

/// Returns a log iterator, the max stable lsn,
/// and a set of segments that can be
/// zeroed after the new snapshot is written,
/// but no sooner, otherwise it is not crash-safe.
pub fn raw_segment_iter_from(
    lsn: Lsn,
    config: &RunningConfig,
) -> Result<(LogIter, Lsn, Vec<LogOffset>)> {
    let segment_len = config.segment_size as Lsn;
    let normalized_lsn = lsn / segment_len * segment_len;

    let (ordering, max_header_stable_lsn, to_zero_after_snap_write) =
        scan_segment_lsns(normalized_lsn, config)?;

    // find the last stable tip, to properly handle batch manifests.
    let tip_segment_iter =
        Box::new(ordering.iter().map(|(a, b)| (*a, *b)).last().into_iter());
    trace!(
        "trying to find the max stable tip for \
         bounding batch manifests with segment iter {:?} \
         of segments >= max_header_stable_lsn {}",
        tip_segment_iter,
        max_header_stable_lsn
    );

    let mut tip_iter = LogIter {
        config: config.clone(),
        max_lsn: Lsn::max_value(),
        cur_lsn: 0,
        segment_base: None,
        segment_iter: tip_segment_iter,
    };

    // run the iterator to the end so
    // we can grab its current lsn, inclusive
    // of any zeroed messages and other
    // legit items it may not have returned
    // in the actual iterator.
    while let Some(_) = tip_iter.next() {}

    let tip = tip_iter.cur_lsn;

    trace!("found max stable tip: {}", tip);

    trace!(
        "generated iterator over segments {:?} with lsn >= {}",
        ordering,
        normalized_lsn,
    );

    let segment_iter = Box::new(
        ordering.into_iter().filter(move |&(l, _)| l >= normalized_lsn),
    );

    Ok((
        LogIter {
            config: config.clone(),
            max_lsn: tip,
            cur_lsn: 0,
            segment_base: None,
            segment_iter,
        },
        max_header_stable_lsn,
        to_zero_after_snap_write,
    ))
}

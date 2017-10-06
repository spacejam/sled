//! The `SegmentAccountant` is an allocator for equally-
//! sized chunks of the underlying storage file (segments).
//!
//! It must maintain these critical safety properties:
//!
//! A. We must not overwrite existing segments when they
//!    contain the most-recent stable state for a page.
//! B. We must not overwrite existing segments when active
//!    threads may have references to LogID's that point
//!    into those segments.
//!
//! To complicate matters, the `PageCache` only knows
//! when it has put a page into an IO buffer, but it
//! doesn't keep track of when that IO buffer is
//! stabilized (until write coalescing is implemented).
//!
//! To address these safety concerns, we rely on
//! these techniques:
//!
//! 1. We delay the reuse of any existing segment
//!    by ensuring there are at least <# io buffers>
//!    freed segments in front of the newly freed
//!    segment in the free list. This ensures that
//!    any pending IO buffer writes will hit
//!    stable storage before we overwrite the
//!    segment that may have contained the previous
//!    latest stable copy of a page's state.
//! 2. we use a `SegmentDropper` that guarantees
//!    any segment that has been logically freed
//!    or emptied by the `PageCache` will have its
//!    addition to the free segment list be delayed
//!    until any active threads that were acting on
//!    the shared state have checked-out.
//!
//! Another concern that arises due to the fact that
//! IO buffers may be written out-of-order is the
//! correct recovery of segments. If there is data
//! loss in recently written segments, we must be
//! careful to preserve linearizability in the log.
//! To do this, we must detect "torn segments" that
//! were not able to be fully written before a crash
//! happened. We detect torn individual segments by
//! writing a `SegmentTrailer` to the end of the
//! segment AFTER we have sync'd it. If the trailer
//! is not present during recovery, the recovery
//! process will not continue to a segment that
//! may contain logically later data.
//!
//! But what if we wrote a later segment, and its
//! trailer, before we were able to write its
//! immediate predecessor segment, and then a
//! crash happened? We must preserve linearizability,
//! so we can not accidentally recover the later
//! segment when its predecessor was lost in the crash.
//!
//! 3. This case is solved again by having used
//!    <# io buffers> segments before reuse. We guarantee
//!    that the last <# io buffers> segments will be
//!    present, which means we can write a "previous
//!    log sequence number pointer" to the header of
//!    each segment. During recovery, if these previous
//!    segment Lsn pointers don't match up, we know we
//!    have encountered a lost segment, and we will not
//!    continue the recovery past the detected gap.
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fs::File;
use std::sync::{Arc, Mutex};

use coco::epoch::{Owned, pin};

use super::*;

/// The segment accountant keeps track of the logical blocks
/// of storage. It scans through all segments quickly during
/// recovery and attempts to locate torn segments.
#[derive(Default, Debug)]
pub struct SegmentAccountant {
    // static or one-time set
    config: Config,
    recovered_lsn: Lsn,
    recovered_lid: LogID,

    // TODO these should be sharded to improve performance
    segments: Vec<Segment>,
    pending_clean: HashSet<PageID>,

    // TODO put behind a single mutex
    // NB MUST group pause_rewriting with ordering
    // and free!
    free: Arc<Mutex<VecDeque<LogID>>>,
    tip: LogID,
    to_clean: HashSet<LogID>,
    pause_rewriting: bool,
    last_given: LogID,
    ordering: BTreeMap<Lsn, LogID>,
}

// We use a `SegmentDropper` to ensure that we never
// add a segment's LogID to the free deque while any
// active thread could be acting on it. This is necessary
// despite the "safe buffer" in the free queue because
// the safe buffer only prevents the sole remaining
// copy of a page from being overwritten. This prevents
// dangling references to segments that were rewritten after
// the `LogID` was read.
struct SegmentDropper(LogID, Arc<Mutex<VecDeque<LogID>>>);

impl Drop for SegmentDropper {
    fn drop(&mut self) {
        let mut deque = self.1.lock().unwrap();
        deque.push_back(self.0);
    }
}

/// A `Segment` holds the bookkeeping information for
/// a contiguous block of the disk. It may contain many
/// fragments from different pages. Over time, we track
/// when segments become reusable and allow them to be
/// overwritten for new data.
#[derive(Default, Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub pids: HashSet<PageID>,
    pub pids_len: usize,
    pub lsn: Option<Lsn>,
    freed: bool,
}

impl SegmentAccountant {
    pub fn new(config: Config) -> SegmentAccountant {
        let mut ret = SegmentAccountant::default();
        ret.config = config;
        ret.scan_segment_lsns();
        ret
    }

    /// Called from the `PageCache` recovery logic, this initializes the
    /// `SegmentAccountant` based on recovered segment information.
    pub fn initialize_from_segments(&mut self, mut segments: Vec<Segment>) {
        let safety_buffer = self.config.get_io_bufs();
        let logical_tail: Vec<LogID> = self.ordering
            .iter()
            .rev()
            .take(safety_buffer)
            .map(|(_lsn, lid)| *lid)
            .collect();

        for (idx, ref mut segment) in segments.iter_mut().enumerate() {
            let segment_start = (idx * self.config.get_io_buf_size()) as LogID;

            // populate free and to_clean
            if segment.pids.is_empty() {
                // can be reused immediately
                segment.freed = true;
                trace!("pid {} freed @initialize_from_segments", segment_start);

                if logical_tail.contains(&segment_start) {
                    // we depend on the invariant that the last segments
                    // always link together, so that we can detect torn
                    // segments during recovery.
                    self.ensure_safe_free_distance();
                }

                self.free.lock().unwrap().push_back(segment_start);
            } else if segment.pids.len() as f64 / segment.pids_len as f64 <=
                       self.config.get_segment_cleanup_threshold()
            {
                // can be cleaned
                self.to_clean.insert(segment_start);
            }
        }

        self.segments = segments;
    }

    /// Scan the log file if we don't know of any Lsn offsets yet, and recover
    /// the order of segments, and the highest Lsn.
    fn scan_segment_lsns(&mut self) {
        if self.is_recovered() {
            return;
        }

        let segment_len = self.config.get_io_buf_size() as LogID;
        let mut cursor = 0;

        let cached_f = self.config.cached_file();
        let mut f = cached_f.borrow_mut();
        loop {
            // in the future this can be optimized to just read
            // the initial header at that position... but we need to
            // make sure the segment is not torn
            if let Ok(segment) = f.read_segment_header(cursor) {
                if segment.ok {
                    self.recover(segment.lsn, cursor);
                } else {
                    // TODO DATA CORRUPTION AAHHHHHH
                    // add to free list? can't break...
                    panic!("detected corrupt segment header at {}", cursor);
                }
                cursor += segment_len;
            } else {
                break;
            }
        }

        // Check that the last <# io buffers> segments properly
        // link their previous segment pointers.
        self.clean_tail_tears(&mut f);

        // Drop the file so that the `Iter` below is able to borrow
        // the thread's file handle.
        drop(f);

        let mut empty_tip = true;

        let max = self.ordering.iter().rev().nth(0);

        if let Some((base_lsn, lid)) = max {
            let segment_base = lid / segment_len * segment_len;
            assert_eq!(*lid, segment_base);

            self.last_given = *lid;
            let mut tip = lid + SEG_HEADER_LEN as LogID;

            let segment_ceiling = segment_base + segment_len -
                SEG_TRAILER_LEN as LogID -
                MSG_HEADER_LEN as LogID;

            trace!(
                "segment accountant recovering segment at lsn: {} \
                read_offset: {}",
                base_lsn,
                lid
            );

            let mut iter = Iter {
                config: &self.config,
                max_lsn: segment_ceiling,
                cur_lsn: tip,
                segment_base: None,
                segment_iter: Box::new(vec![(*base_lsn, *lid)].into_iter()),
                segment_len: segment_len as usize,
                use_compression: self.config.get_use_compression(),
                trailer: None,
            };

            while let Some((_lsn, lid, _buf)) = iter.next() {
                empty_tip = false;
                tip = lid;
                assert!(tip <= segment_ceiling);
            }

            if !empty_tip {
                // if we found any later
                let mut f = cached_f.borrow_mut();
                let (_, _, len) = f.read_message(
                    tip,
                    segment_len as usize,
                    self.config.get_use_compression(),
                ).unwrap()
                    .flush()
                    .unwrap();
                tip += MSG_HEADER_LEN as LogID + len as LogID;
                self.recovered_lid = tip;
            }

            let segment_overhang = self.recovered_lid %
                self.config.get_io_buf_size() as LogID;
            self.recovered_lsn = base_lsn + segment_overhang;
        } else {
            assert!(
                self.ordering.is_empty(),
                "should have found recovered lsn {} in ordering {:?}",
                self.recovered_lsn,
                self.ordering
            );
        }

        // determine the end of our valid entries
        for (_lsn, &lid) in &self.ordering {
            if lid >= self.tip {
                self.tip = lid + self.config.get_io_buf_size() as LogID;
            }
        }

        if empty_tip && max.is_some() {
            let lid = *max.unwrap().0;
            debug!("freed empty segment {} while recovering segments", lid);
            self.free.lock().unwrap().push_front(lid);
        }

        debug!(
            "segment accountant recovered max lsn:{}, lid: {}",
            self.recovered_lsn,
            self.recovered_lid
        );
    }

    // This ensures that the last <# io buffers> segments on
    // disk connect via their previous segment pointers in
    // the header. This is important because we expect that
    // the last <# io buffers> segments will join up, and we
    // never reuse buffers within this safety range.
    fn clean_tail_tears(&mut self, f: &mut File) {
        let safety_buffer = self.config.get_io_bufs();
        let logical_tail: Vec<(Lsn, LogID)> = self.ordering
            .iter()
            .rev()
            .take(safety_buffer)
            .map(|(lsn, lid)| (*lsn, *lid))
            .collect();

        let mut tear_at = None;

        for (i, &(_lsn, lid)) in logical_tail.iter().enumerate() {
            if i + 1 == logical_tail.len() {
                // we've reached the end, nothing to check after
                break;
            }

            // check link
            let segment_header = f.read_segment_header(lid).unwrap();
            if !segment_header.ok {
                error!(
                    "read corrupted segment header during recovery of segment {}",
                    lid
                );
                tear_at = Some(i);
                continue;
            }
            let expected_prev = segment_header.prev;
            let actual_prev = logical_tail[i + 1].1;

            if expected_prev != actual_prev {
                // detected a tear, everything after
                error!(
                    "detected corruption during recovery for segment at {}! \
                    expected prev lid: {} actual: {} in last chain {:?}",
                    lid,
                    expected_prev,
                    actual_prev,
                    logical_tail
                );
                tear_at = Some(i);
            }
        }

        if let Some(i) = tear_at {
            // we need to chop off the elements after the tear
            for j in 0..i {
                let (lsn_to_chop, lid_to_chop) = logical_tail[j];

                error!("clearing corrupted segment at lid {}", lid_to_chop);

                self.free.lock().unwrap().push_back(lid_to_chop);
                self.ordering.remove(&lsn_to_chop);
                // TODO write zeroes to these segments to reduce
                // false recovery.
            }
        }
    }

    pub fn recovered_lid(&self) -> LogID {
        self.recovered_lid
    }

    pub fn recovered_lsn(&self) -> Lsn {
        self.recovered_lsn
    }

    /// Causes all new allocations to occur at the end of the file, which
    /// is necessary to preserve consistency while concurrently iterating through
    /// the log during snapshot creation.
    pub fn pause_rewriting(&mut self) {
        self.pause_rewriting = true;
    }

    /// Re-enables segment rewriting after iteration is complete.
    pub fn resume_rewriting(&mut self) {
        self.pause_rewriting = false;
    }

    /// Called by the `PageCache` when a page has been freed. We
    /// mark each segment that contained its updates accordingly.
    /// If any of those segments are now reusable or candidates for
    /// accelerated cleaning, we mark the appropriate structures.
    pub fn freed(&mut self, pid: PageID, old_lids: Vec<LogID>, lsn: Lsn) {
        self.pending_clean.remove(&pid);

        for old_lid in old_lids.into_iter() {
            let idx = old_lid as usize / self.config.get_io_buf_size();

            if self.segments[idx].lsn.unwrap() > lsn {
                // has been replaced after this call already,
                // quite a big race happened.
                continue;
            }

            if self.segments[idx].pids_len == 0 {
                self.segments[idx].pids_len = self.segments[idx].pids.len();
            }

            self.segments[idx].pids.remove(&pid);

            let segment_start = (idx * self.config.get_io_buf_size()) as LogID;

            if self.segments[idx].pids.is_empty() && !self.segments[idx].freed {
                // can be reused immediately
                self.segments[idx].freed = true;
                self.to_clean.remove(&segment_start);
                trace!("freed segment {} while freeing page", segment_start);
                self.ensure_safe_free_distance();

                pin(|scope| {
                    let pd = Owned::new(
                        SegmentDropper(segment_start, self.free.clone()),
                    );
                    let ptr = pd.into_ptr(scope);
                    unsafe {
                        scope.defer_drop(ptr);
                        scope.flush();
                    }
                });
            } else if self.segments[idx].pids.len() as f64 /
                       self.segments[idx].pids_len as f64 <=
                       self.config.get_segment_cleanup_threshold()
            {
                // can be cleaned
                self.to_clean.insert(segment_start);
            }
        }
    }

    /// Called by the `PageCache` when a page has been rewritten completely.
    /// We mark all of the old segments that contained the previous state
    /// from the page, and if the old segments are empty or clear enough to
    /// begin accelerated cleaning we mark them as so.
    pub fn set(
        &mut self,
        pid: PageID,
        old_lids: Vec<LogID>,
        new_lid: LogID,
        lsn: Lsn,
    ) {
        self.pending_clean.remove(&pid);

        let new_idx = new_lid as usize / self.config.get_io_buf_size();

        for old_lid in old_lids.into_iter() {
            let idx = old_lid as usize / self.config.get_io_buf_size();

            if new_idx == idx {
                // we probably haven't flushed this segment yet, so don't
                // mark the pid as being removed from it
                continue;
            }

            if self.segments.len() <= idx {
                self.segments.resize(idx + 1, Segment::default());
            }

            if self.segments[idx].lsn.is_none() {
                self.segments[idx].lsn = Some(lsn);
            }

            if self.segments[idx].lsn.unwrap() > lsn {
                // has been replaced after this call already,
                // quite a big race happened
                continue;
            }

            if self.segments[idx].pids_len == 0 {
                self.segments[idx].pids_len = self.segments[idx].pids.len();
            }

            self.segments[idx].pids.remove(&pid);

            let segment_start = (idx * self.config.get_io_buf_size()) as LogID;

            if self.segments[idx].pids.is_empty() && !self.segments[idx].freed {
                // can be reused immediately
                self.segments[idx].freed = true;
                self.to_clean.remove(&segment_start);
                trace!("freed segment {} in set", segment_start);
                self.ensure_safe_free_distance();

                pin(|scope| {
                    let pd = Owned::new(
                        SegmentDropper(segment_start, self.free.clone()),
                    );
                    let ptr = pd.into_ptr(scope);
                    unsafe {
                        scope.defer_drop(ptr);
                        scope.flush();
                    }
                });
            } else if self.segments[idx].pids.len() as f64 /
                       self.segments[idx].pids_len as f64 <=
                       self.config.get_segment_cleanup_threshold()
            {
                // can be cleaned
                self.to_clean.insert(segment_start);
            }
        }

        self.merged(pid, new_lid, lsn);
    }

    /// Called from `PageCache` when some state has been added
    /// to a logical page at a particular offset. We ensure the
    /// page is present in the segment's page set.
    pub fn merged(&mut self, pid: PageID, lid: LogID, lsn: Lsn) {
        self.pending_clean.remove(&pid);

        let idx = lid as usize / self.config.get_io_buf_size();

        if self.segments.len() <= idx {
            self.segments.resize(idx + 1, Segment::default());
        }

        let segment = &mut self.segments[idx];
        if segment.lsn.is_none() {
            segment.lsn = Some(lsn);
        }

        if segment.lsn.unwrap() > lsn {
            // a race happened, and our Lsn does not apply anymore
            return;
        }

        segment.pids.insert(pid);
    }

    fn bump_tip(&mut self) -> LogID {
        let lid = self.tip;
        self.tip += self.config.get_io_buf_size() as LogID;
        trace!("advancing file tip from {} to {}", lid, self.tip);
        lid
    }

    fn ensure_safe_free_distance(&mut self) {
        // NB we must maintain a queue of free segments that
        // is at least as long as the number of io buffers.
        // This is so that we will never give out a segment
        // that has been placed on the free queue after its
        // contained pages have all had updates added to an
        // IO buffer during a PageCache set, but whose
        // replacing updates have not actually landed on disk
        // yet. If updates always have to wait in a queue
        // at least as long as the number of IO buffers, it
        // guarantees that the old updates are actually safe
        // somewhere else first. Note that we push_front here
        // so that the log tip is used first.
        while self.free.lock().unwrap().len() < self.config.get_io_bufs() {
            let new_lid = self.bump_tip();
            self.free.lock().unwrap().push_front(new_lid);
        }

    }

    /// Returns the next offset to write a new segment in,
    /// as well as the offset of the previous segment that
    /// was allocated, so that we can detect missing
    /// out-of-order segments during recovery.
    pub fn next(&mut self, lsn: Lsn) -> (LogID, LogID) {
        assert!(
            lsn % self.config.get_io_buf_size() as Lsn == 0,
            "unaligned Lsn provided to next!"
        );

        // pop free or add to end
        let lid = if self.pause_rewriting {
            self.bump_tip()
        } else {
            let res = self.free.lock().unwrap().pop_front();
            if res.is_none() {
                self.bump_tip()
            } else {
                res.unwrap()
            }
        };

        let last_given = self.last_given;

        // pin lsn to this segment
        let idx = lid as usize / self.config.get_io_buf_size();

        if self.segments.len() <= idx {
            self.segments.resize(idx + 1, Segment::default());
        }

        let segment = &mut self.segments[idx];
        assert!(segment.pids.is_empty());

        // remove the ordering from our list
        if let Some(old_lsn) = segment.lsn {
            self.ordering.remove(&old_lsn);
        }

        segment.lsn = Some(lsn);
        segment.freed = false;
        segment.pids_len = 0;

        self.ordering.insert(lsn, lid);

        debug!(
            "segment accountant returning offset {} last {}",
            lid,
            last_given
        );

        self.last_given = lid;

        (lid, last_given)
    }

    /// Called by the `PageCache` to find useful pages
    /// it should try to rewrite.
    pub fn clean(&mut self) -> Option<PageID> {
        if self.free.lock().unwrap().len() >=
            self.config.get_min_free_segments() ||
            self.to_clean.is_empty()
        {
            return None;
        }

        for lid in &self.to_clean {
            let idx = *lid as usize / self.config.get_io_buf_size();
            let segment = &self.segments[idx];
            for pid in &segment.pids {
                if self.pending_clean.contains(&pid) {
                    continue;
                }
                self.pending_clean.insert(*pid);
                return Some(*pid);
            }
        }

        None
    }

    /// Returns an iterator over a snapshot of current segment
    /// log sequence numbers and their corresponding file offsets.
    pub fn segment_snapshot_iter_from(
        &self,
        lsn: Lsn,
    ) -> Box<Iterator<Item = (Lsn, LogID)>> {
        let segment_len = self.config.get_io_buf_size() as Lsn;
        let normalized_lsn = lsn / segment_len * segment_len;
        Box::new(self.ordering.clone().into_iter().filter(move |&(l, _)| {
            l >= normalized_lsn
        }))
    }

    pub fn is_recovered(&self) -> bool {
        !self.segments.is_empty()
    }

    // Mark a specific segment as being present at a particular
    // file offset.
    fn recover(&mut self, lsn: Lsn, lid: LogID) {
        trace!("recovered segment lsn {} at lid {}", lsn, lid);
        let idx = lid as usize / self.config.get_io_buf_size();

        if self.segments.len() <= idx {
            self.segments.resize(idx + 1, Segment::default());
        }

        if lsn == 0 && lid != 0 {
            // TODO figure out why this is happening, stahp it
        } else {
            self.segments[idx].lsn = Some(lsn);
            self.ordering.insert(lsn, lid);
        }
    }
}

#[test]
fn basic_workflow() {
    // empty clean is None
    let conf = Config::default()
        .io_buf_size(1)
        .io_bufs(2)
        .segment_cleanup_threshold(0.2)
        .min_free_segments(3);
    let mut sa = SegmentAccountant::new(conf);

    let mut highest = 0;
    let mut lsn = || {
        highest += 1;
        highest
    };

    let first = sa.next(lsn()).0;
    let second = sa.next(lsn()).0;
    let third = sa.next(lsn()).0;

    sa.merged(0, first, lsn());

    // Assert that sets for the same pid don't yield anything to clean yet.
    sa.set(0, vec![first], first, lsn());
    assert_eq!(sa.clean(), None);
    sa.set(0, vec![first], first, lsn());
    assert_eq!(sa.clean(), None);
    sa.set(0, vec![first], first, lsn());
    assert_eq!(sa.clean(), None);
    sa.set(0, vec![first], first, lsn());
    assert_eq!(sa.clean(), None);

    // Assert that when we roll over to the next log, we can immediately
    // reuse first.
    let _fourth = sa.next(lsn()).0;
    sa.set(0, vec![first], second, lsn());
    assert_eq!(sa.clean(), None);
    sa.merged(1, second, lsn());
    sa.merged(2, second, lsn());
    sa.merged(3, second, lsn());
    sa.merged(4, second, lsn());
    sa.merged(5, second, lsn());

    // Move a page from second to third, and assert pid 1 can be cleaned.
    sa.set(0, vec![second], third, lsn());
    sa.set(2, vec![second], third, lsn());
    sa.set(3, vec![second], third, lsn());
    sa.set(4, vec![second], third, lsn());
    sa.set(5, vec![second], third, lsn());
    assert_eq!(sa.clean(), Some(1));
    assert_eq!(sa.clean(), None);
}

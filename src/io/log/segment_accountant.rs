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
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::mem;

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
    to_clean: BTreeSet<LogID>,
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
    present: BTreeSet<PageID>,
    removed: HashSet<PageID>,
    deferred_remove: HashSet<PageID>,
    lsn: Option<Lsn>,
    state: SegmentState,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum SegmentState {
    /// the segment is marked for reuse, should never receive
    /// new pids,
    /// TODO consider: but may receive removals for pids that were
    /// already removed?
    Free,

    /// the segment is being written to or actively recovered, and
    /// will have pages assigned to it
    Active,

    /// the segment is no longer being written to or recovered, and
    /// will have pages marked as relocated from it
    Inactive,

    /// the segment is having its resident pages relocated before
    /// becoming free
    Draining,
}

use self::SegmentState::*;

impl Default for SegmentState {
    fn default() -> SegmentState {
        Free
    }
}

impl Segment {
    fn _is_free(&self) -> bool {
        match self.state {
            Free => true,
            _ => false,
        }
    }

    fn is_inactive(&self) -> bool {
        match self.state {
            Inactive => true,
            _ => false,
        }
    }

    fn _is_active(&self) -> bool {
        match self.state {
            Active => true,
            _ => false,
        }
    }

    fn is_draining(&self) -> bool {
        match self.state {
            Draining => true,
            _ => false,
        }
    }

    fn free_to_active(&mut self, new_lsn: Lsn) {
        trace!(
            "setting Segment to Active with new lsn {:?}, was {:?}",
            new_lsn,
            self.lsn
        );
        assert_eq!(self.state, Free);
        self.present.clear();
        self.removed.clear();
        self.lsn = Some(new_lsn);
        self.state = Active;
    }

    /// Transitions a segment to being in the Inactive state.
    /// Called in:
    ///
    /// PageCache::advance_snapshot for marking when a
    /// segment has been completely read
    ///
    /// SegmentAccountant::recover for when
    pub fn active_to_inactive(&mut self, lsn: Lsn, from_recovery: bool) {
        trace!("setting Segment with lsn {:?} to Inactive", self.lsn());
        assert_eq!(self.state, Active);
        if from_recovery {
            assert!(lsn >= self.lsn());
        } else {
            assert_eq!(self.lsn.unwrap(), lsn);
        }
        self.state = Inactive;

        // now we can push any deferred removals to the removed set
        let deferred = mem::replace(&mut self.deferred_remove, HashSet::new());
        for pid in deferred {
            self.remove_pid(pid, lsn);
        }
    }

    pub fn inactive_to_draining(&mut self, lsn: Lsn) {
        trace!("setting Segment with lsn {:?} to Draining", self.lsn());
        assert_eq!(self.state, Inactive);
        assert!(lsn >= self.lsn());
        self.state = Draining;
    }

    pub fn draining_to_free(&mut self, lsn: Lsn) {
        trace!("setting Segment with lsn {:?} to Free", self.lsn());
        assert!(self.is_draining());
        assert!(lsn >= self.lsn());
        self.present.clear();
        self.removed.clear();
        self.state = Free;
    }

    pub fn recovery_ensure_initialized(&mut self, lsn: Lsn) {
        if let Some(current_lsn) = self.lsn {
            if current_lsn != lsn {
                trace!("(snapshot) resetting segment to have lsn {}", lsn);
                self.state = Free;
                self.free_to_active(lsn);
            }
        } else {
            trace!("(snapshot) resetting segment to have lsn {}", lsn);
            self.free_to_active(lsn);
        }
    }

    fn lsn(&self) -> Lsn {
        self.lsn.unwrap()
    }

    /// Add a pid to the Segment. The caller must provide
    /// the Segment's LSN.
    pub fn insert_pid(&mut self, pid: PageID, lsn: Lsn) {
        assert_eq!(lsn, self.lsn.unwrap());
        // if this breaks, we didn't implement the transition
        // logic right in write_to_log, and maybe a thread is
        // using the SA to add pids AFTER their calls to
        // res.complete() worked.
        assert_eq!(self.state, Active);
        assert!(!self.removed.contains(&pid));
        self.present.insert(pid);
    }

    /// Mark that a pid in this Segment has been relocated.
    /// The caller must provide the LSN of the removal.
    pub fn remove_pid(&mut self, pid: PageID, lsn: Lsn) {
        // TODO this could be racy?
        assert!(lsn >= self.lsn.unwrap());
        match self.state {
            Active => {
                // we have received a removal before
                // transferring this segment to Inactive, so
                // we defer this pid's removal until the transfer.
                self.deferred_remove.insert(pid);
            }
            Inactive | Draining => {
                self.present.remove(&pid);
                self.removed.insert(pid);
            }
            Free => panic!("remove_pid called on a Free Segment"),
        }
    }

    fn live_pct(&self) -> f64 {
        let total = self.present.len() + self.removed.len();
        self.present.len() as f64 / total as f64
    }

    fn can_free(&self) -> bool {
        self.state == Draining && self.is_empty()
    }

    pub fn is_empty(&self) -> bool {
        self.present.is_empty()
    }
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
            if segment.lsn.is_none() {
                continue;
            }
            let segment_start = idx as LogID *
                self.config.get_io_buf_size() as LogID;

            let lsn = segment.lsn();

            // populate free and to_clean if the segment has seen
            if segment.is_empty() {
                // can be reused immediately

                if segment.state == Active {
                    segment.active_to_inactive(lsn, true);
                }

                if segment.state == Inactive {
                    segment.inactive_to_draining(lsn);
                }

                self.to_clean.remove(&segment_start);
                trace!("pid {} freed @initialize_from_segments", segment_start);

                if logical_tail.contains(&segment_start) {
                    // we depend on the invariant that the last segments
                    // always link together, so that we can detect torn
                    // segments during recovery.
                    self.ensure_safe_free_distance();
                }

                segment.draining_to_free(lsn);
                if self.tip != segment_start &&
                    !self.free.lock().unwrap().contains(&segment_start)
                {
                    // don't give out this segment twice
                    trace!(
                        "freeing segment {} from initialize_from_segments, tip: {}",
                        segment_start,
                        self.tip
                    );
                    self.free_segment(segment_start, true);
                }
            } else if segment.live_pct() <=
                       self.config.get_segment_cleanup_threshold()
            {
                // can be cleaned
                trace!(
                    "setting segment {} to Draining from initialize_from_segments",
                    segment_start
                );

                if segment.state == Active {
                    segment.active_to_inactive(lsn, true);
                }

                segment.inactive_to_draining(lsn);
                self.to_clean.insert(segment_start);
                self.free.lock().unwrap().retain(|&s| s != segment_start);
            } else {
                self.free.lock().unwrap().retain(|&s| s != segment_start);
            }
        }

        if !segments.is_empty() {
            trace!("initialized self.segments to {:?}", segments);
            self.segments = segments;
        } else {
            // this is basically just for when we recover with a single
            // empty-yet-initialized segment
            debug!(
                "pagecache recovered no segments so not initializing from any"
            );
        }
    }

    // Mark a specific segment as being present at a particular
    // file offset.
    fn recover(&mut self, lsn: Lsn, lid: LogID) {
        trace!("recovered segment lsn {} at lid {}", lsn, lid);
        let io_buf_size = self.config.get_io_buf_size() as LogID;
        let idx = self.lid_to_idx(lid);

        assert!(!(lsn == 0 && lid != 0), "lsn 0 provided with non-zero lid");
        if !self.segments[idx].is_empty() {
            self.segments[idx].free_to_active(lsn);

            let segment_lsn = lsn / io_buf_size * io_buf_size;
            self.segments[idx].active_to_inactive(segment_lsn, true);
        }

        assert!(!self.ordering.contains_key(&lsn));
        self.ordering.insert(lsn, lid);
    }

    // Scan the log file if we don't know of any Lsn offsets yet, and recover
    // the order of segments, and the highest Lsn.
    fn scan_segment_lsns(&mut self) {
        assert!(self.segments.is_empty());

        let segment_len = self.config.get_io_buf_size() as LogID;
        let mut cursor = 0;

        let cached_f = self.config.cached_file();
        let mut f = cached_f.borrow_mut();
        while let Ok(segment) = f.read_segment_header(cursor) {
            // in the future this can be optimized to just read
            // the initial header at that position... but we need to
            // make sure the segment is not torn
            trace!("SA scanned header during startup {:?}", segment);
            if segment.ok && (segment.lsn != 0 || cursor == 0) {
                // if lsn is 0, this is free
                self.recover(segment.lsn, cursor);
            } else {
                // this segment was skipped or is free
                trace!(
                    "freeing segment {} from scan_segment_lsns",
                    cursor,
                );
                self.free_segment(cursor, true);
            }
            cursor += segment_len;
        }

        // Check that the last <# io buffers> segments properly
        // link their previous segment pointers.
        self.clean_tail_tears(&mut f);

        // Drop the file so that the `Iter` below is able to borrow
        // the thread's file handle.
        drop(f);

        let mut empty_tip = true;

        let max = self.ordering
            .iter()
            .rev()
            .nth(0)
            .map(|(lsn, lid)| (*lsn, *lid))
            .clone();

        if let Some((base_lsn, lid)) = max {
            let segment_base = lid / segment_len * segment_len;
            assert_eq!(lid, segment_base);

            self.last_given = lid;
            let mut tip = lid + SEG_HEADER_LEN as LogID;
            let cur_lsn = base_lsn + SEG_HEADER_LEN as Lsn;

            let segment_ceiling = base_lsn + segment_len -
                SEG_TRAILER_LEN as LogID -
                MSG_HEADER_LEN as LogID;

            trace!(
                "segment accountant recovering segment at lsn: {} \
                read_offset: {}, ceiling: {}, cur_lsn: {}",
                base_lsn,
                lid,
                segment_ceiling,
                cur_lsn
            );

            let iter = Iter {
                config: &self.config,
                max_lsn: segment_ceiling,
                cur_lsn: cur_lsn,
                segment_base: None,
                segment_iter: Box::new(vec![(base_lsn, lid)].into_iter()),
                segment_len: segment_len as usize,
                use_compression: self.config.get_use_compression(),
                trailer: None,
            };

            for (_lsn, lid, _buf) in iter {
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
        for &lid in self.ordering.values() {
            if lid >= self.tip {
                let new_tip = lid + self.config.get_io_buf_size() as LogID;
                self.tip = new_tip;
            }
        }

        if empty_tip && max.is_some() {
            let (_lsn, lid) = max.unwrap();
            debug!("freed empty segment {} while recovering segments", lid);
            self.free_segment(lid, true);
        }

        // make sure we don't double-allocate a segment
        while self.free.lock().unwrap().contains(&self.tip) {
            self.tip += self.config.get_io_buf_size() as LogID;
        }

        debug!(
            "segment accountant recovered max lsn:{}, lid: {}",
            self.recovered_lsn,
            self.recovered_lid
        );
    }

    fn free_segment(&mut self, lid: LogID, in_recovery: bool) {
        debug!("freeing segment {}", lid);
        let idx = self.lid_to_idx(lid);
        assert_eq!(self.segments[idx].state, Free);
        assert!(
            !self.free.lock().unwrap().contains(&lid),
            "double-free of a segment occurred"
        );

        if in_recovery {
            self.free.lock().unwrap().push_front(lid);

            // We only want to immediately remove the segment
            // mapping if we're in recovery because otherwise
            // we may be acting on updates relating to things
            // in IO buffers, before they have been flushed.
            // The latter will be removed from the mapping
            // before being reused, in the next() method.
            if let Some(old_lsn) = self.segments[idx].lsn {
                self.ordering.remove(&old_lsn);
            }
        } else {
            self.ensure_safe_free_distance();

            pin(|scope| {
                let pd = Owned::new(SegmentDropper(lid, self.free.clone()));
                let ptr = pd.into_ptr(scope);
                unsafe {
                    scope.defer_drop(ptr);
                    scope.flush();
                }
            });
        }
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
            for &(_lsn_to_chop, lid_to_chop) in &logical_tail[0..i] {
                error!("clearing corrupted segment at lid {}", lid_to_chop);

                self.free_segment(lid_to_chop, true);
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

    /// Called by the `PageCache` when a page has been rewritten completely.
    /// We mark all of the old segments that contained the previous state
    /// from the page, and if the old segments are empty or clear enough to
    /// begin accelerated cleaning we mark them as so.
    pub fn mark_replace(
        &mut self,
        pid: PageID,
        lsn: Lsn,
        old_lids: Vec<LogID>,
        new_lid: LogID,
    ) {
        trace!("mark_replace pid {} at lid {} with lsn {}", pid, new_lid, lsn);
        self.pending_clean.remove(&pid);

        let new_idx = new_lid as usize / self.config.get_io_buf_size();

        // make sure we're not actively trying to replace the destination
        let new_segment_start = new_idx as LogID *
            self.config.get_io_buf_size() as LogID;
        self.to_clean.remove(&new_segment_start);

        for old_lid in old_lids {
            let old_idx = self.lid_to_idx(old_lid);
            let segment_start = (old_idx * self.config.get_io_buf_size()) as
                LogID;

            if new_idx == old_idx {
                // we probably haven't flushed this segment yet, so don't
                // mark the pid as being removed from it
                continue;
            }

            if self.segments[old_idx].lsn() > lsn {
                // has been replaced after this call already,
                // quite a big race happened
                // TODO think about how this happens with our segment delay
                continue;
            }

            if self.segments[old_idx].state == Free {
                // this segment is already reused
                // TODO should this be a panic?
                continue;
            }

            self.segments[old_idx].remove_pid(pid, lsn);

            if self.segments[old_idx].can_free() {
                // can be reused immediately
                self.segments[old_idx].draining_to_free(lsn);
                self.to_clean.remove(&segment_start);
                trace!("freed segment {} in replace", segment_start);
                self.free_segment(segment_start, false);
            } else if self.segments[old_idx].is_inactive() &&
                       self.segments[old_idx].live_pct() <=
                           self.config.get_segment_cleanup_threshold()
            {
                // can be cleaned
                trace!(
                    "SA inserting {} into to_clean from mark_replace",
                    segment_start
                );
                self.segments[old_idx].inactive_to_draining(lsn);
                self.to_clean.insert(segment_start);
            }
        }

        self.mark_link(pid, lsn, new_lid);
    }

    /// Called by the `PageCache` to find useful pages
    /// it should try to rewrite.
    pub fn clean(&mut self) -> Option<PageID> {
        // try to maintain about twice the number of necessary
        // on-deck segments, to reduce the amount of log growth.
        if self.free.lock().unwrap().len() >=
            self.config.get_min_free_segments() * 2
        {
            return None;
        }

        let to_clean = self.to_clean.clone();

        for lid in to_clean {
            let idx = self.lid_to_idx(lid);
            let segment = &self.segments[idx];
            assert_eq!(segment.state, Draining);
            for pid in &segment.present {
                if self.pending_clean.contains(pid) {
                    continue;
                }
                self.pending_clean.insert(*pid);
                trace!(
                    "telling caller to clean {} from segment at {}",
                    pid,
                    lid,
                );
                return Some(*pid);
            }
        }

        None
    }

    /// Called from `PageCache` when some state has been added
    /// to a logical page at a particular offset. We ensure the
    /// page is present in the segment's page set.
    pub fn mark_link(&mut self, pid: PageID, lsn: Lsn, lid: LogID) {
        trace!("mark_link pid {} at lid {}", pid, lid);
        self.pending_clean.remove(&pid);

        let idx = self.lid_to_idx(lid);

        // make sure we're not actively trying to replace the destination
        let new_segment_start = idx as LogID *
            self.config.get_io_buf_size() as LogID;

        self.to_clean.remove(&new_segment_start);

        let segment = &mut self.segments[idx];

        if segment.lsn() > lsn {
            // a race happened, and our Lsn does not apply anymore
            // TODO think about how this happens with segment delay
            return;
        }

        let segment_lsn = lsn / self.config.get_io_buf_size() as Lsn *
            self.config.get_io_buf_size() as Lsn;

        segment.insert_pid(pid, segment_lsn);
    }

    /// Called after the trailer of a segment has been written to disk,
    /// indicating that no more pids will be added to a segment. Moves
    /// the segment into the Inactive state.
    ///
    /// # Panics
    /// The provided lsn and lid must exactly match the existing segment.
    pub fn deactivate_segment(&mut self, lsn: Lsn, lid: LogID) {
        let idx = self.lid_to_idx(lid);
        self.segments[idx].active_to_inactive(lsn, false);
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
        // IO buffer during a PageCache replace, but whose
        // replacing updates have not actually landed on disk
        // yet. If updates always have to wait in a queue
        // at least as long as the number of IO buffers, it
        // guarantees that the old updates are actually safe
        // somewhere else first. Note that we push_front here
        // so that the log tip is used first.
        while self.free.lock().unwrap().len() < self.config.get_io_bufs() {
            let new_lid = self.bump_tip();
            trace!(
                "pushing segment {} to free from ensure_safe_free_distance",
                new_lid
            );
            self.free.lock().unwrap().push_front(new_lid);
        }

    }

    /// Returns the next offset to write a new segment in,
    /// as well as the offset of the previous segment that
    /// was allocated, so that we can detect missing
    /// out-of-order segments during recovery.
    pub fn next(&mut self, lsn: Lsn) -> (LogID, LogID) {
        assert_eq!(
            lsn % self.config.get_io_buf_size() as Lsn,
            0,
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
        let idx = self.lid_to_idx(lid);

        assert_eq!(self.segments[idx].state, Free);

        // remove the ordering from our list
        if let Some(old_lsn) = self.segments[idx].lsn {
            self.ordering.remove(&old_lsn);
        }

        self.segments[idx].free_to_active(lsn);

        self.ordering.insert(lsn, lid);

        debug!(
            "segment accountant returning offset: {} paused: {} last: {} on deck: {:?}",
            lid,
            self.pause_rewriting,
            last_given,
            self.free
        );

        self.last_given = lid;

        (lid, last_given)
    }

    /// Returns an iterator over a snapshot of current segment
    /// log sequence numbers and their corresponding file offsets.
    pub fn segment_snapshot_iter_from(
        &mut self,
        lsn: Lsn,
    ) -> Box<Iterator<Item = (Lsn, LogID)>> {
        // assert!( self.pause_rewriting, "must pause rewriting before iterating over segments");

        let segment_len = self.config.get_io_buf_size() as Lsn;
        let normalized_lsn = lsn / segment_len * segment_len;
        Box::new(self.ordering.clone().into_iter().filter(move |&(l, _)| {
            l >= normalized_lsn
        }))
    }

    fn lid_to_idx(&mut self, lid: LogID) -> usize {
        let idx = lid as usize / self.config.get_io_buf_size();
        if self.segments.len() < idx + 1 {
            self.segments.resize(idx + 1, Segment::default());
        }
        idx
    }
}

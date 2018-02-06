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
//! 2. we use a `epoch::Guard::defer()` that guarantees
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

use epoch::pin;

use self::reader::LogReader;
use super::*;

/// The segment accountant keeps track of the logical blocks
/// of storage. It scans through all segments quickly during
/// recovery and attempts to locate torn segments.
#[derive(Debug)]
pub struct SegmentAccountant {
    // static or one-time set
    config: Config,

    // TODO these should be sharded to improve performance
    segments: Vec<Segment>,
    pending_clean: HashSet<PageID>,

    // TODO put behind a single mutex
    // NB MUST group pause_rewriting with ordering
    // and free!
    free: Arc<Mutex<VecDeque<(LogID, bool)>>>,
    tip: LogID,
    to_clean: BTreeSet<LogID>,
    pause_rewriting: bool,
    safety_buffer: Vec<LogID>,
    ordering: BTreeMap<Lsn, LogID>,
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
    fn len(&self) -> usize {
        std::cmp::max(self.present.len(), self.removed.len()) -
            self.removed.len()
    }

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
        self.deferred_remove.clear();
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
                assert!(lsn > current_lsn);
                trace!("(snapshot) recovering segment with base lsn {}", lsn);
                self.state = Free;
                self.free_to_active(lsn);
            }
        } else {
            trace!("(snapshot) recovering segment with base lsn {}", lsn);
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
        // FIXME Free, called from Tree::new -> replace -> mark_replace -> mark_link
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
    /// Create a new SegmentAccountant from previously recovered segments.
    pub fn start<R>(
        config: Config,
        snapshot: Snapshot<R>,
    ) -> CacheResult<SegmentAccountant, ()> {
        let mut ret = SegmentAccountant {
            config: config,
            segments: vec![],
            pending_clean: HashSet::default(),
            free: Arc::new(Mutex::new(VecDeque::new())),
            tip: 0,
            to_clean: BTreeSet::new(),
            pause_rewriting: false,
            safety_buffer: vec![],
            ordering: BTreeMap::new(),
        };

        if let SegmentMode::Linear = ret.config.segment_mode {
            // this is a hack to prevent segments from being overwritten
            // when operating without a `PageCache`
            ret.pause_rewriting();
        }
        if snapshot.last_lid > ret.tip {
            let io_buf_size = ret.config.get_io_buf_size();
            let last_idx = snapshot.last_lid / io_buf_size as LogID;
            let new_idx = last_idx + 1;
            let new_tip = new_idx * io_buf_size as LogID;
            ret.tip = new_tip;
        }

        ret.set_safety_buffer(snapshot.max_lsn)?;

        ret.initialize_from_snapshot(snapshot);

        Ok(ret)
    }

    /// Called from the `PageCache` recovery logic, this initializes the
    /// `SegmentAccountant` based on recovered segment information.
    fn initialize_from_snapshot<R>(&mut self, snapshot: Snapshot<R>) {
        let io_buf_size = self.config.get_io_buf_size();

        // generate segments from snapshot lids
        let mut segments = vec![];

        let add = |pid, lsn, lid, segments: &mut Vec<Segment>| {
            // add pid to segment
            let idx = lid as usize / io_buf_size;
            if segments.len() < idx + 1 {
                segments.resize(idx + 1, Segment::default());
            }

            let segment_lsn = lsn / io_buf_size as Lsn * io_buf_size as Lsn;
            segments[idx].recovery_ensure_initialized(segment_lsn);
            segments[idx].insert_pid(pid, segment_lsn);
        };

        for (pid, state) in snapshot.pt {
            match state {
                PageState::Present(coords) => {
                    for (lsn, lid) in coords {
                        add(pid, lsn, lid, &mut segments);
                    }
                }
                PageState::Allocated(lsn, lid) |
                PageState::Free(lsn, lid) => {
                    add(pid, lsn, lid, &mut segments);
                }
            }
        }

        for (idx, pids) in snapshot.replacements {
            if segments.len() <= idx {
                // segment doesn't have pids anyway,
                // and will be marked as free later
                continue;
            }
            if let Some(segment_lsn) = segments[idx].lsn {
                for (pid, lsn) in pids {
                    if lsn < segment_lsn {
                        // TODO is this avoidable? can punt more
                        // work to snapshot generation logic.
                        trace!(
                            "stale removed pid {} with lsn {}, on segment {} with current lsn: {:?}",
                            pid,
                            lsn,
                            idx,
                            segments[idx].lsn
                        );
                    } else {
                        segments[idx].remove_pid(pid, lsn);
                    }
                }
            }
        }

        self.initialize_from_segments(segments);
    }

    fn initialize_from_segments(&mut self, mut segments: Vec<Segment>) {
        // populate ordering from segments.
        // use last segment as active even if it's full
        let io_buf_size = self.config.get_io_buf_size();

        let highest_lsn = segments.iter().fold(0, |acc, segment| {
            std::cmp::max(acc, segment.lsn.unwrap_or(acc))
        });
        debug!("recovered highest_lsn in all segments: {}", highest_lsn);

        // NB set tip BEFORE any calls to free_segment, as when
        // we ensure the segment safety discipline, it is going to
        // bump the tip, which hopefully is already the final recovered
        // tip.
        self.tip = (io_buf_size * segments.len()) as LogID;

        // we need to make sure that we raise the tip over any
        // segments that are in the safety_buffer. The safety_buffer
        // may contain segments that are beyond what we have tracked
        // in self.segments, because they may have been fully replaced
        // in a later segment, or the next one, but they still need
        // to be in the safety buffer, in order to prevent us from
        // zeroing and recycling something in the safety buffer, breaking
        // the recovery of later segments if a tear is discovered.
        if self.tip != 0 {
            for &lid in &self.safety_buffer {
                if self.tip <= lid {
                    self.tip = lid + io_buf_size as LogID;
                }
            }
        }

        debug!("set self.tip to {}", self.tip);

        for (idx, ref mut segment) in segments.iter_mut().enumerate() {
            let segment_start = idx as LogID * io_buf_size as LogID;

            if segment.lsn.is_none() {
                self.free_segment(segment_start, true);
                continue;
            }

            let lsn = segment.lsn();

            if lsn != highest_lsn {
                segment.active_to_inactive(lsn, true);
            }

            self.ordering.insert(lsn, segment_start);

            // can we transition these segments?
            let cleanup_threshold = self.config.get_segment_cleanup_threshold();
            let min_items = self.config.get_min_items_per_segment();

            let segment_low_pct = segment.live_pct() <= cleanup_threshold;

            let segment_low_count = (segment.len() as f64) <
                min_items as f64 * cleanup_threshold;

            let can_free = segment.is_empty() && !self.pause_rewriting &&
                lsn != highest_lsn;

            let can_drain = (segment_low_pct || segment_low_count) &&
                !self.pause_rewriting &&
                lsn != highest_lsn;

            // populate free and to_clean if the segment has seen
            if can_free {
                // can be reused immediately
                if segment.state == Active {
                    segment.active_to_inactive(lsn, true);
                }

                if segment.state == Inactive {
                    segment.inactive_to_draining(lsn);
                }

                self.to_clean.remove(&segment_start);
                trace!("pid {} freed @initialize_from_snapshot", segment_start);

                segment.draining_to_free(lsn);

                trace!(
                    "freeing segment {} from initialize_from_snapshot, tip: {}",
                    segment_start,
                    self.tip
                );
                self.free_segment(segment_start, true);
            } else if can_drain {
                // hack! we check here for pause_rewriting to work with
                // raw logs, which are created with this set to true.

                // can be cleaned
                trace!(
                    "setting segment {} to Draining from initialize_from_snapshot",
                    segment_start
                );

                if segment.state == Active {
                    segment.active_to_inactive(lsn, true);
                }

                segment.inactive_to_draining(lsn);
                self.to_clean.insert(segment_start);
                self.free.lock().unwrap().retain(
                    |&(s, _)| s != segment_start,
                );
            } else {
                self.free.lock().unwrap().retain(
                    |&(s, _)| s != segment_start,
                );
            }
        }

        trace!("initialized self.segments to {:?}", segments);
        self.segments = segments;
        if self.segments.is_empty() {
            // this is basically just for when we recover with a single
            // empty-yet-initialized segment
            debug!(
                "recovered no segments so not initializing from any",
            );
        }
    }

    fn set_safety_buffer(
        &mut self,
        snapshot_max_lsn: Lsn,
    ) -> CacheResult<(), ()> {
        self.ensure_ordering_initialized()?;

        // if our ordering contains anything higher than
        // what our snapshot logic scanned, it means it's
        // empty, and we should nuke it to prevent incorrect
        // recoveries.
        let mut to_zero = vec![];
        for (&lsn, &lid) in &self.ordering {
            if lsn <= snapshot_max_lsn {
                continue;
            }
            warn!(
                "zeroing out empty segment header at lsn {} lid {}",
                lsn,
                lid
            );
            to_zero.push(lsn);
            let f = self.config.file()?;
            f.pwrite_all(&*vec![0; SEG_HEADER_LEN], lid)?;
            f.sync_all()?;
        }

        for lsn in to_zero.into_iter() {
            self.ordering.remove(&lsn);
        }

        self.ordering = self.ordering
            .clone()
            .into_iter()
            .filter(|&(lsn, _)| lsn <= snapshot_max_lsn)
            .collect();

        let safety_buffer_len = self.config.get_io_bufs();
        let mut safety_buffer: Vec<LogID> = self.ordering
            .iter()
            .rev()
            .take(safety_buffer_len)
            .map(|(_lsn, lid)| *lid)
            .collect();

        // we want the things written last to be last in this Vec
        safety_buffer.reverse();

        while safety_buffer.len() < safety_buffer_len {
            safety_buffer.insert(0, 0);
        }

        self.safety_buffer = safety_buffer;

        Ok(())
    }

    fn free_segment(&mut self, lid: LogID, in_recovery: bool) {
        debug!("freeing segment {}", lid);
        debug!("safety_buffer before free: {:?}", self.safety_buffer);
        debug!("free list before free {:?}", self.free);

        let idx = self.lid_to_idx(lid);
        assert_eq!(self.segments[idx].state, Free);
        assert!(
            !self.segment_in_free(lid),
            "double-free of a segment occurred"
        );

        // we depend on the invariant that the last segments
        // always link together, so that we can detect torn
        // segments during recovery.
        self.ensure_safe_free_distance(lid);

        if in_recovery {
            self.free.lock().unwrap().push_back((lid, false));

            // We only want to immediately remove the segment
            // mapping if we're in recovery because otherwise
            // we may be acting on updates relating to things
            // in IO buffers, before they have been flushed.
            // The latter will be removed from the mapping
            // before being reused, in the next() method.
            if let Some(old_lsn) = self.segments[idx].lsn {
                trace!(
                    "removing segment {} with lsn {} from ordering",
                    lid,
                    old_lsn
                );
                self.ordering.remove(&old_lsn);
            }
        } else {
            let free = self.free.clone();
            let guard = pin();
            unsafe {
                // We use a `epoch::Guard::defer()` to ensure that we never
                // add a segment's LogID to the free deque while any
                // active thread could be acting on it. This is necessary
                // despite the "safe buffer" in the free queue because
                // the safe buffer only prevents the sole remaining
                // copy of a page from being overwritten. This prevents
                // dangling references to segments that were rewritten after
                // the `LogID` was read.
                guard.defer(move || {
                    free.lock().unwrap().push_back((lid, false));
                });
                guard.flush();
            }
        }
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

            // can we transition these segments?
            let cleanup_threshold = self.config.get_segment_cleanup_threshold();
            let min_items = self.config.get_min_items_per_segment();

            let segment_low_pct = self.segments[old_idx].live_pct() <=
                cleanup_threshold;

            let segment_low_count = (self.segments[old_idx].len() as f64) <
                min_items as f64 * cleanup_threshold;

            let can_drain = self.segments[old_idx].is_inactive() &&
                (segment_low_pct || segment_low_count);

            if self.segments[old_idx].can_free() {
                // can be reused immediately
                self.segments[old_idx].draining_to_free(lsn);
                self.to_clean.remove(&segment_start);
                trace!("freed segment {} in replace", segment_start);
                self.free_segment(segment_start, false);
            } else if can_drain {
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
    pub fn clean(&mut self, ignore: Option<PageID>) -> Option<PageID> {
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
                if self.pending_clean.contains(pid) || ignore == Some(*pid) {
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
    pub(super) fn deactivate_segment(&mut self, lsn: Lsn, lid: LogID) {
        let idx = self.lid_to_idx(lid);
        self.segments[idx].active_to_inactive(lsn, false);
    }

    fn bump_tip(&mut self) -> LogID {
        let lid = self.tip;

        self.tip += self.config.get_io_buf_size() as LogID;

        trace!("advancing file tip from {} to {}", lid, self.tip);

        lid
    }

    fn ensure_safe_free_distance(&mut self, lid: LogID) {
        // NB If updates always have to wait in a queue
        // at least as long as the number of IO buffers, it
        // guarantees that the old updates are actually safe
        // somewhere else first. Note that we push_front here
        // so that the log tip is used first.
        // This is so that we will never give out a segment
        // that has been placed on the free queue after its
        // contained pages have all had updates added to an
        // IO buffer during a PageCache replace, but whose
        // replacing updates have not actually landed on disk
        // yet.
        let position = self.safety_buffer.iter().position(|&previous_lid| {
            previous_lid == lid
        });
        if let Some(position) = position {
            // if the segment was newest in the safety buffer
            // (which will always have # io bufs elements)
            // then the free list needs to contain at least
            // # io_bufs before we can push this segment. if
            // the segment is the oldest in the safety buffer,
            // we can just push one thing to the free list first.

            // 1 for 0-indexing, 1 for having at least safety buffer
            let min_free_len = position + 2;

            while self.free.lock().unwrap().len() < min_free_len {
                let new_lid = self.bump_tip();
                trace!(
                    "pushing segment {} to free from ensure_safe_free_distance",
                    new_lid
                );
                self.free.lock().unwrap().push_front((new_lid, true));
            }
        } else {
            // lid not in safety buffer, we don't need to pad anything
        }
    }

    /// Returns the next offset to write a new segment in,
    /// as well as the offset of the previous segment that
    /// was allocated, so that we can detect missing
    /// out-of-order segments during recovery.
    pub fn next(&mut self, lsn: Lsn) -> CacheResult<(LogID, LogID), ()> {
        assert_eq!(
            lsn % self.config.get_io_buf_size() as Lsn,
            0,
            "unaligned Lsn provided to next!"
        );

        // pop free or add to end
        let lid = if self.pause_rewriting {
            self.bump_tip()
        } else {
            loop {
                let res = self.free.lock().unwrap().pop_front();
                if res.is_none() {
                    break self.bump_tip();
                } else {
                    let (next, pushed_by_ensure_safe_free_distance) =
                        res.unwrap();

                    let next_next_in_safety_buffer = self.free
                        .lock()
                        .unwrap()
                        .get(0)
                        .cloned()
                        .map(|(lid, _)| self.safety_buffer.contains(&lid))
                        .unwrap_or(false);

                    // this will only be in safety_buffer if it's the last
                    // element
                    let truncate_prohibited =
                        pushed_by_ensure_safe_free_distance ||
                            next_next_in_safety_buffer;

                    if truncate_prohibited {
                        break next;
                    }

                    // if we just returned the last segment
                    // in the file, shrink the file.
                    let io_buf_size = self.config.get_io_buf_size() as LogID;
                    if next + io_buf_size == self.tip {
                        self.truncate(next)?;
                    } else {
                        break next;
                    }
                }
            }
        };

        debug!(
            "zeroing out segment beginning at {} for future lsn {}",
            lid,
            lsn
        );
        let f = self.config.file()?;
        f.pwrite_all(&*vec![0; self.config.get_io_buf_size()], lid)?;
        f.sync_all()?;

        let last_given = self.safety_buffer[self.config.get_io_bufs() - 1];

        // pin lsn to this segment
        let idx = self.lid_to_idx(lid);

        assert_eq!(self.segments[idx].state, Free);

        // remove the old ordering from our list
        if let Some(old_lsn) = self.segments[idx].lsn {
            self.ordering.remove(&old_lsn);
        }

        self.segments[idx].free_to_active(lsn);

        self.ordering.insert(lsn, lid);

        debug!(
            "segment accountant returning offset: {} \
            paused: {} last: {} on deck: {:?}",
            lid,
            self.pause_rewriting,
            last_given,
            self.free
        );

        if lid == 0 {
            let all_zeroes = self.safety_buffer ==
                vec![0; self.config.get_io_bufs()];
            let no_zeroes = !self.safety_buffer.contains(&0);
            assert!(
                all_zeroes || no_zeroes,
                "SA returning 0, and we expected \
                the safety buffer to either be all zeroes, or contain no other \
                zeroes, but it was {:?}",
                self.safety_buffer
            );
        } else {
            assert!(
                !self.safety_buffer.contains(&lid),
                "giving away segment {} that is in the safety buffer {:?}",
                lid,
                self.safety_buffer
            );
        }

        self.safety_buffer.push(lid);
        self.safety_buffer.remove(0);

        Ok((lid, last_given))
    }

    /// Returns an iterator over a snapshot of current segment
    /// log sequence numbers and their corresponding file offsets.
    pub fn segment_snapshot_iter_from(
        &mut self,
        lsn: Lsn,
    ) -> Box<Iterator<Item = (Lsn, LogID)>> {
        assert!(
            self.pause_rewriting,
            "must pause rewriting before \
            iterating over segments"
        );

        let segment_len = self.config.get_io_buf_size() as Lsn;
        let normalized_lsn = lsn / segment_len * segment_len;

        Box::new(self.ordering.clone().into_iter().filter(move |&(l, _)| {
            l >= normalized_lsn
        }))
    }

    // truncate the file to the desired length
    fn truncate(&mut self, at: LogID) -> CacheResult<(), ()> {
        assert_eq!(
            at % self.config.get_io_buf_size() as LogID,
            0,
            "new length must be io-buf-len aligned"
        );

        if self.safety_buffer.contains(&at) {
            panic!(
                "file tip {} to be truncated is in the safety buffer {:?}",
                at,
                self.safety_buffer
            );
        }

        self.tip = at;

        assert!(!self.segment_in_free(at), "double-free of a segment occurred");

        debug!("truncating file to length {}", at);

        let f = self.config.file()?;
        f.set_len(at)?;
        f.sync_all().map_err(|e| e.into())
    }

    fn ensure_ordering_initialized(&mut self) -> CacheResult<(), ()> {
        if !self.ordering.is_empty() {
            return Ok(());
        }

        self.ordering = scan_segment_lsns(0, &self.config)?;
        debug!("initialized ordering to {:?}", self.ordering);
        Ok(())
    }

    fn lid_to_idx(&mut self, lid: LogID) -> usize {
        let idx = lid as usize / self.config.get_io_buf_size();

        // TODO never resize like this, make it a single
        // responsibility when the tip is bumped / truncated.
        if self.segments.len() < idx + 1 {
            self.segments.resize(idx + 1, Segment::default());
        }

        idx
    }

    fn segment_in_free(&self, lid: LogID) -> bool {
        let free = self.free.lock().unwrap();
        for &(seg_lid, _) in &*free {
            if seg_lid == lid {
                return true;
            }
        }
        false
    }
}

// Scan the log file if we don't know of any Lsn offsets yet,
// and recover the order of segments, and the highest Lsn.
pub fn scan_segment_lsns(
    min: Lsn,
    config: &Config,
) -> CacheResult<BTreeMap<Lsn, LogID>, ()> {
    let mut ordering = BTreeMap::new();

    let segment_len = config.get_io_buf_size() as LogID;
    let mut cursor = 0;

    let f = config.file()?;
    while let Ok(segment) = f.read_segment_header(cursor) {
        // in the future this can be optimized to just read
        // the initial header at that position... but we need to
        // make sure the segment is not torn
        trace!("SA scanned header during startup {:?}", segment);
        if segment.ok && (segment.lsn != 0 || cursor == 0) &&
            segment.lsn >= min
        {
            // if lsn is 0, this is free
            assert!(
                !ordering.contains_key(&segment.lsn),
                "duplicate segment LSN detected, one should have \
                been zeroed out during recovery"
            );
            ordering.insert(segment.lsn, cursor);
        }
        cursor += segment_len;
    }

    debug!("ordering before clearing tears: {:?}", ordering);

    // Check that the last <# io buffers> segments properly
    // link their previous segment pointers.
    Ok(clean_tail_tears(ordering, config, &f))
}

// This ensures that the last <# io buffers> segments on
// disk connect via their previous segment pointers in
// the header. This is important because we expect that
// the last <# io buffers> segments will join up, and we
// never reuse buffers within this safety range.
fn clean_tail_tears(
    mut ordering: BTreeMap<Lsn, LogID>,
    config: &Config,
    f: &File,
) -> BTreeMap<Lsn, LogID> {
    let safety_buffer = config.get_io_bufs();
    let logical_tail: Vec<Lsn> = ordering
        .iter()
        .rev()
        .take(safety_buffer)
        .map(|(lsn, _lid)| *lsn)
        .collect();

    let io_buf_size = config.get_io_buf_size();

    let mut tear_at = None;

    // make sure the last <# io_bufs> segments are contiguous
    for window in logical_tail.windows(2) {
        if window[0] != window[1] + io_buf_size as Lsn {
            error!("detected torn segment somewhere after {}", window[1]);
            tear_at = Some(window[1]);
        }
    }

    // if any segment doesn't have a proper trailer, invalidate
    // everything after it, since we can't preserve linearizability
    // for segments after a tear.
    for (&lsn, &lid) in &ordering {
        let trailer_lid = lid + io_buf_size as LogID - SEG_TRAILER_LEN as LogID;
        let expected_trailer_lsn = lsn + io_buf_size as Lsn -
            SEG_TRAILER_LEN as Lsn;
        let trailer_res = f.read_segment_trailer(trailer_lid);

        if trailer_res.is_err() {
            // trailer could not be read
            debug!("could not read trailer of segment starting at {}", lid);
            if let Some(existing_tear) = tear_at {
                if existing_tear > lsn {
                    tear_at = Some(lsn);
                }
            } else {
                tear_at = Some(lsn);
            }
            break;
        }

        let trailer = trailer_res.unwrap();

        if !trailer.ok || trailer.lsn != expected_trailer_lsn ||
            (lsn == 0 && lid != 0)
        {
            // trailer's checksum failed, or
            // the lsn is outdated, or
            // the lsn is 0 but the lid isn't 0 (zeroed segment)
            debug!(
                "tear detected at expected lsn {} actual lsn {} \
                lid {} for trailer {:?}",
                expected_trailer_lsn,
                lsn,
                lid,
                trailer
            );
            if let Some(existing_tear) = tear_at {
                if existing_tear > lsn {
                    tear_at = Some(lsn);
                }
            } else {
                tear_at = Some(lsn);
            }
        }
    }

    if let Some(tear) = tear_at {
        // we need to chop off the elements after the tear
        debug!("filtering out segments after detected tear at {}", tear);
        for (&lsn, &lid) in &ordering {
            if lsn > tear {
                // TODO make this a panic during non-truncating tests
                error!("filtering out segment with lsn {} at lid {}", lsn, lid);
            }
        }
        ordering = ordering
            .into_iter()
            .filter(|&(lsn, _lid)| lsn <= tear)
            .collect();
    }

    ordering
}

/// The log may be configured to write data
/// in several different ways, depending on
/// the constraints of the system using it.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum SegmentMode {
    /// Write to the end of the log, always.
    Linear,
    /// Like linear, but also keep track of
    /// utilization, and try to use filesystem
    /// hole punching on empty segments.
    /// This is only supported on linux with
    /// filesystems that support hole punching.
    PunchedLinear,
    /// Keep track of segment utilization, and
    /// reuse segments when their contents are
    /// fully relocated elsewhere.
    Reuse,
    /// Like Reuse, but also will try to copy
    /// data out of segments once they reach a
    /// configurable threshold.
    Gc,
}

pub fn raw_segment_iter_from(
    lsn: Lsn,
    config: &Config,
) -> CacheResult<LogIter, ()> {
    let segment_len = config.get_io_buf_size() as Lsn;
    let normalized_lsn = lsn / segment_len * segment_len;

    let ordering = scan_segment_lsns(0, &config)?;

    trace!(
        "generated iterator over segments {:?} with lsn >= {}",
        ordering,
        normalized_lsn
    );

    let segment_iter = Box::new(ordering.into_iter().filter(
        move |&(l, _)| l >= normalized_lsn,
    ));

    Ok(LogIter {
        config: config.clone(),
        max_lsn: std::isize::MAX,
        cur_lsn: SEG_HEADER_LEN as Lsn,
        segment_base: None,
        segment_iter: segment_iter,
        segment_len: config.get_io_buf_size(),
        use_compression: config.get_use_compression(),
        trailer: None,
    })
}

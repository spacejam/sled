//! The `SegmentAccountant` is an allocator for equally-
//! sized chunks of the underlying storage file (segments).
//!
//! It must maintain these critical safety properties:
//!
//! A. We must not overwrite existing segments when they
//!    contain the most-recent stable state for a page.
//! B. We must not overwrite existing segments when active
//!    threads may have references to LogId's that point
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
//! writing a lsn-tagged pad to the end of the
//! segment, filling any unused space.
//!
//! But what if we wrote a later segment before we
//! were able to write its immediate predecessor segment,
//! and then a crash happened? We must preserve
//! linearizability, so we must not recover the later
//! segment when its predecessor was lost in the crash.
//!
//! 3. This case is solved again by having a concept of
//!    an "unstable tail" of segments that, during recovery,
//!    must appear consecutively among the recovered
//!    segments with the highest LSN numbers. We
//!    prevent reuse of segments while they remain in
//!    this "unstable tail" by only allowing them to be
//!    reallocated after another later segment has written
//!    a "stable consecutive lsn" into its own header
//!    that is higher than ours.
use std::{collections::BTreeMap, fs::File, mem};

use self::reader::LogReader;

use futures::{future::Future, oneshot, Oneshot};

use super::*;

/// The segment accountant keeps track of the logical blocks
/// of storage. It scans through all segments quickly during
/// recovery and attempts to locate torn segments.
#[derive(Debug)]
pub(super) struct SegmentAccountant {
    // static or one-time set
    config: Config,

    // TODO these should be sharded to improve performance
    segments: Vec<Segment>,
    clean_counter: usize,

    // TODO put behind a single mutex
    // NB MUST group pause_rewriting with ordering
    // and free!
    free: VecSet<LogId>,
    tip: LogId,
    max_stabilized_lsn: Lsn,
    to_clean: VecSet<LogId>,
    pause_rewriting: bool,
    ordering: BTreeMap<Lsn, LogId>,
    async_truncations: Vec<Oneshot<Result<()>>>,
}

/// A `Segment` holds the bookkeeping information for
/// a contiguous block of the disk. It may contain many
/// fragments from different pages. Over time, we track
/// when segments become reusable and allow them to be
/// overwritten for new data.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Segment {
    present: FastSet8<PageId>,
    // a copy of present that lets us make decisions
    // about draining without waiting for
    // deferred_replacements to take effect during
    // segment deactivation.
    not_yet_replaced: FastSet8<PageId>,
    removed: FastSet8<PageId>,
    deferred_rm_blob: FastSet8<BlobPointer>,
    // set of pages that we replaced from other segments
    deferred_replacements: FastSet8<(PageId, SegmentId)>,
    lsn: Option<Lsn>,
    state: SegmentState,
}

#[derive(
    Debug,
    Copy,
    Eq,
    Hash,
    Ord,
    PartialOrd,
    PartialEq,
    Clone,
    Serialize,
    Deserialize,
)]
pub(crate) enum SegmentState {
    /// the segment is marked for reuse, should never receive
    /// new pids,
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
        std::cmp::max(self.present.len(), self.removed.len())
            - self.removed.len()
    }

    fn is_free(&self) -> bool {
        self.state == Free
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
        self.not_yet_replaced.clear();
        self.removed.clear();
        self.deferred_rm_blob.clear();
        self.deferred_replacements.clear();
        self.lsn = Some(new_lsn);
        self.state = Active;
    }

    /// Transitions a segment to being in the Inactive state.
    /// Returns the set of page replacements that happened
    /// while this Segment was Active
    fn active_to_inactive(
        &mut self,
        lsn: Lsn,
        from_recovery: bool,
        config: &Config,
    ) -> Result<FastSet8<(PageId, usize)>> {
        trace!("setting Segment with lsn {:?} to Inactive", self.lsn());
        assert_eq!(
            self.state,
            Active,
            "segment {} should have been \
             Active before deactivating",
            self.lsn()
        );
        if from_recovery {
            assert!(lsn >= self.lsn());
        } else {
            assert_eq!(self.lsn.unwrap(), lsn);
        }
        self.state = Inactive;

        // now we can push any deferred blob removals to the removed set
        let deferred_rm_blob =
            mem::replace(&mut self.deferred_rm_blob, FastSet8::default());
        for ptr in deferred_rm_blob {
            trace!(
                "removing blob {} while transitioning \
                 segment lsn {:?} to Inactive",
                ptr,
                self.lsn,
            );
            remove_blob(ptr, config)?;
        }

        let deferred_replacements =
            mem::replace(&mut self.deferred_replacements, FastSet8::default());

        Ok(deferred_replacements)
    }

    fn inactive_to_draining(&mut self, lsn: Lsn) {
        trace!("setting Segment with lsn {:?} to Draining", self.lsn());
        assert_eq!(
            self.state, Inactive,
            "segment with lsn {:?} should have been \
             Inactive before draining",
            self.lsn
        );
        assert!(lsn >= self.lsn());
        self.state = Draining;
    }

    fn draining_to_free(&mut self, lsn: Lsn) {
        trace!("setting Segment with lsn {:?} to Free", self.lsn());
        assert!(self.is_draining());
        assert!(lsn >= self.lsn());
        self.present.clear();
        self.not_yet_replaced.clear();
        self.removed.clear();
        self.state = Free;
    }

    fn recovery_ensure_initialized(&mut self, lsn: Lsn) {
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
    fn insert_pid(&mut self, pid: PageId, lsn: Lsn) {
        assert_eq!(lsn, self.lsn.unwrap());
        // if this breaks, maybe we didn't implement the transition
        // logic right in write_to_log, and maybe a thread is
        // using the SA to add pids AFTER their calls to
        // res.complete() worked.
        assert_eq!(
            self.state, Active,
            "expected segment with lsn {} to be Active",
            lsn
        );
        assert!(!self.removed.contains(&pid));
        self.not_yet_replaced.insert(pid);
        self.present.insert(pid);
    }

    /// Mark that a pid in this Segment has been relocated.
    /// The caller must provide the LSN of the removal.
    fn remove_pid(&mut self, pid: PageId, lsn: Lsn, in_recovery: bool) {
        assert!(lsn >= self.lsn.unwrap());
        match self.state {
            Active => {
                // we have received a removal before
                // transferring this segment to Inactive.
                // This should have been deferred by the
                // segment that actually replaced this,
                // and if we're still Active, something is
                // wrong.
                if !in_recovery {
                    panic!("remove_pid called on Active segment");
                }
                assert!(
                    !self.present.contains(&pid),
                    "did not expect present to contain pid {} during recovery",
                    pid,
                );
                self.removed.insert(pid);
            }
            Inactive | Draining => {
                self.present.remove(&pid);
                self.removed.insert(pid);
            }
            Free => panic!("remove_pid called on a Free Segment"),
        }
    }

    fn defer_replace_pids(
        &mut self,
        deferred: FastSet8<(PageId, usize)>,
        lsn: Lsn,
    ) {
        assert!(lsn >= self.lsn.unwrap());
        self.deferred_replacements.extend(deferred);
    }

    fn remove_blob(
        &mut self,
        blob_ptr: BlobPointer,
        config: &Config,
    ) -> Result<()> {
        match self.state {
            Active => {
                // we have received a removal before
                // transferring this segment to Inactive, so
                // we defer this pid's removal until the transfer.
                self.deferred_rm_blob.insert(blob_ptr);
            }
            Inactive | Draining => {
                trace!(
                    "directly removing blob {} that was referred-to \
                     in a segment that has already been marked as Inactive \
                     or Draining.",
                    blob_ptr,
                );
                remove_blob(blob_ptr, config)?;
            }
            Free => panic!("remove_blob called on a Free Segment"),
        }

        Ok(())
    }

    // The live percentage between 0 and 100
    fn live_pct(&self) -> u8 {
        let total = self.present.len() + self.removed.len();
        if total == 0 {
            return 100;
        }

        let live = self.present.len() * 100 / total;
        assert!(live <= 100);
        live as u8
    }

    fn can_free(&self) -> bool {
        self.state == Draining && self.is_empty()
    }

    fn is_empty(&self) -> bool {
        self.present.is_empty()
    }
}

impl SegmentAccountant {
    /// Create a new SegmentAccountant from previously recovered segments.
    pub(super) fn start(
        config: Config,
        snapshot: Snapshot,
    ) -> Result<SegmentAccountant> {
        let mut ret = SegmentAccountant {
            config,
            segments: vec![],
            clean_counter: 0,
            free: Default::default(),
            tip: 0,
            max_stabilized_lsn: -1,
            to_clean: Default::default(),
            pause_rewriting: false,
            ordering: Default::default(),
            async_truncations: Default::default(),
        };

        if let SegmentMode::Linear = ret.config.segment_mode {
            // this is a hack to prevent segments from being overwritten
            // when operating without a `PageCache`
            ret.pause_rewriting();
        }

        if snapshot.max_lsn >= SEG_HEADER_LEN as Lsn && !snapshot.pt.is_empty()
        {
            ret.initialize_from_snapshot(snapshot)?;
        } else {
            trace!(
                "skipping initialization of SA \
                 for snapshot with max_lsn {} and pt {:?}",
                snapshot.max_lsn,
                snapshot.pt,
            );
        }

        Ok(ret)
    }

    fn initialize_from_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        let io_buf_size = self.config.io_buf_size;

        // generate segments from snapshot lids
        let mut segments = vec![];
        segments.resize_with(snapshot.replacements.len(), Segment::default);

        let add = |pid, lsn, lid: LogId, segments: &mut Vec<Segment>| {
            // add pid to segment
            let idx = assert_usize(lid / io_buf_size as LogId);
            if segments.len() <= idx {
                segments.resize_with(idx + 1, Segment::default);
            }
            trace!(
                "adding lsn: {} lid: {} for pid {} to segment {} during SA recovery",
                lsn,
                lid,
                pid,
                idx
            );
            let segment_lsn = lsn / io_buf_size as Lsn * io_buf_size as Lsn;
            segments[idx].recovery_ensure_initialized(segment_lsn);
            segments[idx].insert_pid(pid, segment_lsn);
        };

        for (pid, state) in snapshot.pt {
            match state {
                PageState::Present(coords) => {
                    for (lsn, ptr) in coords {
                        add(pid, lsn, ptr.lid(), &mut segments);
                    }
                }
                PageState::Free(lsn, ptr) => {
                    add(pid, lsn, ptr.lid(), &mut segments);
                }
            }
        }

        let mut ordering = snapshot
            .replacements
            .iter()
            .map(|(idx, (lsn, _))| (*lsn, *idx as LogId * io_buf_size as LogId))
            .collect::<Vec<(Lsn, LogId)>>();

        ordering.sort_unstable_by_key(|(_lsn, lid)| *lid);

        for (idx, (_lsn, segment_start)) in ordering.iter().enumerate() {
            if *segment_start >= self.tip {
                self.tip = segment_start + io_buf_size as LogId;
                debug!(
                    "set self.tip to {} based on encountered segment",
                    self.tip
                );
            }

            if self.tip <= *segment_start {
                self.tip = segment_start + io_buf_size as LogId;
                debug!("set self.tip to {} based on safety buffer", self.tip);
            }

            if let Some((replacement_lsn, replacements)) =
                snapshot.replacements.get(&idx)
            {
                for &(old_pid, old_idx) in replacements {
                    let replaced_current_lsn =
                        segments.get(old_idx).and_then(|s| s.lsn);
                    if replaced_current_lsn.unwrap_or(Lsn::max_value())
                        > *replacement_lsn
                    {
                        // we can skip this if it's already empty or
                        // has been rewritten since the replacement.
                        continue;
                    }

                    segments[old_idx].remove_pid(
                        old_pid,
                        *replacement_lsn,
                        true,
                    );
                }
            }
        }

        trace!("initialized self.segments to {:?}", segments);
        self.segments = segments;

        trace!("initialized self.ordering to {:?}", ordering);
        self.ordering = ordering.into_iter().collect();

        Ok(())
    }

    fn free_segment(&mut self, lid: LogId, in_recovery: bool) {
        debug!("freeing segment {}", lid);
        debug!("free list before free {:?}", self.free);

        let idx = self.lid_to_idx(lid);
        assert!(
            self.tip > lid,
            "freed a segment above our current file tip, \
             please report this bug!"
        );
        assert_eq!(self.segments[idx].state, Free);
        assert!(
            !self.free.contains(&lid),
            "double-free of a segment occurred"
        );

        if in_recovery {
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
        }

        self.free.insert(lid);
    }

    /// Causes all new allocations to occur at the end of the file, which
    /// is necessary to preserve consistency while concurrently iterating through
    /// the log during snapshot creation.
    pub(super) fn pause_rewriting(&mut self) {
        self.pause_rewriting = true;
    }

    /// Re-enables segment rewriting after iteration is complete.
    pub(super) fn resume_rewriting(&mut self) {
        // we never want to resume segment rewriting in Linear mode
        if self.config.segment_mode != SegmentMode::Linear {
            self.pause_rewriting = false;
        }
    }

    /// Called by the `PageCache` when a page has been rewritten completely.
    /// We mark all of the old segments that contained the previous state
    /// from the page, and if the old segments are empty or clear enough to
    /// begin accelerated cleaning we mark them as so.
    pub(super) fn mark_replace(
        &mut self,
        pid: PageId,
        lsn: Lsn,
        old_ptrs: Vec<DiskPtr>,
        new_ptr: DiskPtr,
    ) -> Result<()> {
        let _measure = Measure::new(&M.accountant_mark_replace);

        trace!(
            "mark_replace pid {} from ptrs {:?} to ptr {} with lsn {}",
            pid,
            old_ptrs,
            new_ptr,
            lsn
        );

        let new_idx = self.lid_to_idx(new_ptr.lid());

        // make sure we're not actively trying to replace the destination
        let new_segment_start =
            new_idx as LogId * self.config.io_buf_size as LogId;

        assert!(!self.to_clean.contains(&new_segment_start));

        // Do we need to schedule any blob cleanups?
        // Not if we just moved the pointer without changing
        // the underlying blob, as is the case with a single Blob
        // with nothing else.
        let schedule_rm_blob = !(old_ptrs.len() == 1 && old_ptrs[0].is_blob());

        let mut deferred_replacements = FastSet8::default();

        for old_ptr in old_ptrs {
            let old_lid = old_ptr.lid();

            if schedule_rm_blob && old_ptr.is_blob() {
                trace!(
                    "queueing blob removal for {} in our own segment",
                    old_ptr
                );
                self.segments[new_idx]
                    .remove_blob(old_ptr.blob().1, &self.config)?;
            }

            let old_idx = self.lid_to_idx(old_lid);
            if new_idx == old_idx {
                // we probably haven't flushed this segment yet, so don't
                // mark the pid as being removed from it

                continue;
            }

            if self.segments[old_idx].lsn() > lsn {
                // has been replaced after this call already,
                // quite a big race happened
                panic!(
                    "mark_replace called on previous version of segment. \
                     this means it was reused while other threads still \
                     had references to it."
                );
            }

            if self.segments[old_idx].state == Free {
                // this segment is already reused
                panic!(
                    "mark_replace called on Free segment with lid {}. \
                     this means it was dropped while other threads still had \
                     references to it.",
                    old_idx * self.config.io_buf_size
                );
            }

            self.segments[old_idx].not_yet_replaced.remove(&pid);

            deferred_replacements.insert((pid, old_idx));
        }

        self.segments[new_idx].defer_replace_pids(deferred_replacements, lsn);

        self.mark_link(pid, lsn, new_ptr);

        Ok(())
    }

    fn possibly_clean_or_free_segment(&mut self, idx: usize, lsn: Lsn) {
        let can_drain = segment_is_drainable(
            idx,
            self.segments.len(),
            self.segments[idx].live_pct(),
            self.segments[idx].len(),
            &self.config,
        ) && self.segments[idx].is_inactive();

        let segment_start = (idx * self.config.io_buf_size) as LogId;

        if can_drain {
            // can be cleaned
            trace!(
                "SA inserting {} into to_clean from possibly_clean_or_free_segment",
                segment_start
            );
            self.segments[idx].inactive_to_draining(lsn);
            self.to_clean.insert(segment_start);
        }

        if self.segments[idx].can_free() {
            // can be reused immediately
            self.segments[idx].draining_to_free(lsn);
            self.to_clean.remove(&segment_start);
            trace!(
                "freed segment {} in possibly_clean_or_free_segment",
                segment_start
            );
            self.free_segment(segment_start, false);
        }
    }

    /// Called by the `PageCache` to find pages that are in
    /// segments elligible for cleaning that it should
    /// try to rewrite elsewhere.
    pub(super) fn clean(&mut self, ignore_pid: PageId) -> Option<PageId> {
        let seg_offset = if self.to_clean.is_empty() || self.to_clean.len() == 1
        {
            0
        } else {
            self.clean_counter % self.to_clean.len()
        };

        let item = self.to_clean.get(seg_offset).cloned();

        if let Some(lid) = item {
            let idx = self.lid_to_idx(lid);
            let segment = &self.segments[idx];
            assert!(segment.state == Draining || segment.state == Inactive);

            let present = &segment.not_yet_replaced;

            if present.is_empty() {
                // This could legitimately be empty if it's completely
                // filled with failed flushes.
                return None;
            }

            self.clean_counter += 1;

            let offset = if present.len() == 1 {
                0
            } else {
                self.clean_counter % present.len()
            };

            let pid = present.iter().nth(offset).unwrap();
            if *pid == ignore_pid {
                return None;
            }
            trace!("telling caller to clean {} from segment at {}", pid, lid,);

            return Some(*pid);
        }

        None
    }

    /// Called from `PageCache` when some state has been added
    /// to a logical page at a particular offset. We ensure the
    /// page is present in the segment's page set.
    pub(super) fn mark_link(&mut self, pid: PageId, lsn: Lsn, ptr: DiskPtr) {
        let _measure = Measure::new(&M.accountant_mark_link);

        trace!("mark_link pid {} at ptr {}", pid, ptr);
        let idx = self.lid_to_idx(ptr.lid());

        // make sure we're not actively trying to replace the destination
        let new_segment_start = idx as LogId * self.config.io_buf_size as LogId;

        assert!(!self.to_clean.contains(&new_segment_start));

        let segment = &mut self.segments[idx];

        let segment_lsn = lsn / self.config.io_buf_size as Lsn
            * self.config.io_buf_size as Lsn;

        // a race happened, and our Lsn does not apply anymore
        assert_eq!(
            segment.lsn(),
            segment_lsn,
            "segment somehow got reused by the time a link was \
             marked on it. expected lsn: {} actual: {}",
            segment_lsn,
            segment.lsn()
        );

        segment.insert_pid(pid, segment_lsn);
    }

    pub(super) fn stabilize(&mut self, stable_lsn: Lsn) -> Result<()> {
        let io_buf_size = self.config.io_buf_size as Lsn;
        let lsn = ((stable_lsn / io_buf_size) - 1) * io_buf_size;
        println!(
            "stabilize({}), normalized: {}, last: {}",
            stable_lsn, lsn, self.max_stabilized_lsn
        );
        if self.max_stabilized_lsn >= lsn {
            println!(
                "expected stabilization lsn {} \
                 to be greater than the previous value of {}",
                lsn, self.max_stabilized_lsn
            );
            return Ok(());
        }

        let bounds = (
            std::ops::Bound::Excluded(self.max_stabilized_lsn),
            std::ops::Bound::Included(lsn),
        );

        let can_deactivate = self
            .ordering
            .range(bounds)
            .map(|(lsn, _lid)| *lsn)
            .collect::<Vec<_>>();

        self.max_stabilized_lsn = lsn;

        for lsn in can_deactivate {
            self.deactivate_segment(lsn)?;
        }

        Ok(())
    }

    /// Called after the trailer of a segment has been written to disk,
    /// indicating that no more pids will be added to a segment. Moves
    /// the segment into the Inactive state.
    ///
    /// # Panics
    /// The provided lsn and lid must exactly match the existing segment.
    fn deactivate_segment(&mut self, lsn: Lsn) -> Result<()> {
        let lid = self.ordering[&lsn];
        let idx = self.lid_to_idx(lid);

        trace!(
            "deactivating segment with lsn {}: {:?}",
            lsn,
            self.segments[idx]
        );

        let replacements =
            self.segments[idx].active_to_inactive(lsn, false, &self.config)?;

        let mut old_segments = FastSet8::default();

        for &(pid, old_idx) in &replacements {
            old_segments.insert(old_idx);

            let old_segment = &mut self.segments[old_idx];

            assert!(
                old_segment.state != Active && old_segment.state != Free,
                "segment {} is processing pid {} replacements for \
                 old segment {}, which is in the {:?} state. \
                 all replacements for pid: {:?}",
                lid,
                pid,
                old_idx * self.config.io_buf_size,
                old_segment.state,
                replacements
                    .iter()
                    .filter(|(p, _)| p == &pid)
                    .collect::<Vec<_>>()
            );

            #[cfg(feature = "event_log")]
            assert!(
                old_segment.present.contains(&pid),
                "we expect deferred replacements to provide \
                 all previous segments so we can clean them. \
                 pid {} old_ptr segment: {} segments with pid: {:?}",
                pid,
                old_idx * self.config.io_buf_size,
                self.segments
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| s.present.contains(&pid))
                    .map(|(i, s)| (
                        i * self.config.io_buf_size,
                        s.state,
                        s.present.clone(),
                    ))
                    .collect::<Vec<_>>()
            );

            old_segment.remove_pid(pid, lsn, false);
        }

        for old_idx in old_segments.into_iter() {
            self.possibly_clean_or_free_segment(old_idx, lsn);
        }

        // if we have a lot of free segments in our whole file,
        // let's start relocating the current tip to boil it down
        let free_segs = self.segments.iter().filter(|s| s.is_free()).count();
        let inactive_segs =
            self.segments.iter().filter(|s| s.is_inactive()).count();
        let free_ratio = (free_segs * 100) / (1 + free_segs + inactive_segs);

        if free_ratio >= (self.config.segment_cleanup_threshold * 100.) as usize
            && inactive_segs > 5
        {
            let last_index =
                self.segments.iter().rposition(|s| s.is_inactive()).unwrap();

            let segment_start = (last_index * self.config.io_buf_size) as LogId;

            self.to_clean.insert(segment_start);
        }

        Ok(())
    }

    fn bump_tip(&mut self) -> LogId {
        let truncations = mem::replace(&mut self.async_truncations, Vec::new());

        for truncation in truncations {
            match truncation.wait() {
                Ok(Ok(())) => {}
                error => {
                    error!("failed to shrink file: {:?}", error);
                }
            }
        }

        let lid = self.tip;

        self.tip += self.config.io_buf_size as LogId;

        trace!("advancing file tip from {} to {}", lid, self.tip);

        lid
    }

    /// Returns the next offset to write a new segment in.
    pub(super) fn next(&mut self, lsn: Lsn) -> Result<LogId> {
        let _measure = Measure::new(&M.accountant_next);

        assert_eq!(
            lsn % self.config.io_buf_size as Lsn,
            0,
            "unaligned Lsn provided to next!"
        );

        // truncate if possible
        loop {
            if self.tip == 0 {
                break;
            }
            let last_segment = self.tip - self.config.io_buf_size as LogId;
            if self.free.contains(&last_segment) {
                self.free.remove(&last_segment);
                self.truncate(last_segment)?;
            } else {
                break;
            }
        }

        // pop free or add to end
        let safe = self.free.peek_first();

        let lid = if self.pause_rewriting || safe.is_none() {
            self.bump_tip()
        } else {
            let next = *safe.unwrap();
            self.free.remove(&next);
            next
        };

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
             paused: {} on deck: {:?}",
            lid, self.pause_rewriting, self.free,
        );

        Ok(lid)
    }

    /// Returns an iterator over a snapshot of current segment
    /// log sequence numbers and their corresponding file offsets.
    pub(super) fn segment_snapshot_iter_from(
        &mut self,
        lsn: Lsn,
    ) -> Box<dyn Iterator<Item = (Lsn, LogId)>> {
        if let Err(e) = self.ensure_ordering_initialized() {
            error!("failed to load segment ordering: {:?}", e);
        }

        assert!(
            self.pause_rewriting,
            "must pause rewriting before \
             iterating over segments"
        );

        let segment_len = self.config.io_buf_size as Lsn;
        let normalized_lsn = lsn / segment_len * segment_len;

        trace!(
            "generated iterator over {:?} where lsn >= {}",
            self.ordering,
            normalized_lsn
        );

        Box::new(
            self.ordering
                .clone()
                .into_iter()
                .filter(move |&(l, _)| l >= normalized_lsn),
        )
    }

    // truncate the file to the desired length
    fn truncate(&mut self, at: LogId) -> Result<()> {
        assert_eq!(
            at % self.config.io_buf_size as LogId,
            0,
            "new length must be io-buf-len aligned"
        );

        self.tip = at;

        assert!(
            !self.free.contains(&at),
            "double-free of a segment occurred"
        );

        if let Some(ref thread_pool) = self.config.thread_pool {
            trace!("asynchronously truncating file to length {}", at);
            let (completer, oneshot) = oneshot();

            let f = self.config.file.clone();

            thread_pool.spawn(move || {
                debug!("truncating file to length {}", at);
                let res = f
                    .set_len(at)
                    .and_then(|_| f.sync_all())
                    .map_err(|e| e.into());
                if let Err(e) = completer.send(res) {
                    error!("failed to fill async truncation future: {:?}", e);
                }
            });

            self.async_truncations.push(oneshot);

            Ok(())
        } else {
            trace!("synchronously truncating file to length {}", at);
            let f = &self.config.file;
            f.set_len(at)?;
            f.sync_all()?;
            Ok(())
        }
    }

    fn ensure_ordering_initialized(&mut self) -> Result<()> {
        if !self.ordering.is_empty() {
            return Ok(());
        }

        self.ordering = scan_segment_lsns(0, &self.config)?;
        debug!("initialized ordering to {:?}", self.ordering);
        Ok(())
    }

    fn lid_to_idx(&mut self, lid: LogId) -> usize {
        let idx = assert_usize(lid / self.config.io_buf_size as LogId);

        // TODO never resize like this, make it a single
        // responsibility when the tip is bumped / truncated.
        if self.segments.len() < idx + 1 {
            self.segments.resize(idx + 1, Segment::default());
        }

        idx
    }
}

// Scan the log file if we don't know of any Lsn offsets yet,
// and recover the order of segments, and the highest Lsn.
fn scan_segment_lsns(
    min: Lsn,
    config: &Config,
) -> Result<BTreeMap<Lsn, LogId>> {
    let mut ordering = BTreeMap::new();

    let segment_len = config.io_buf_size as LogId;
    let mut cursor = 0;

    let f = &config.file;
    while let Ok(segment) = f.read_segment_header(cursor) {
        // in the future this can be optimized to just read
        // the initial header at that position... but we need to
        // make sure the segment is not torn
        trace!(
            "SA scanned header at lid {} during startup: {:?}",
            cursor,
            segment
        );
        if segment.ok && (segment.lsn != 0 || cursor == 0) && segment.lsn >= min
        {
            assert_ne!(segment.lsn, Lsn::max_value());

            // if lsn is 0, this is free
            assert!(
                !ordering.contains_key(&segment.lsn),
                "duplicate segment LSN {} detected at both {} and {}, \
                 one should have been zeroed out during recovery",
                segment.lsn,
                ordering[&segment.lsn],
                cursor
            );
            ordering.insert(segment.lsn, cursor);
        }
        cursor += segment_len;
    }

    debug!("ordering before clearing tears: {:?}", ordering);

    // Check that the last <# io buffers> segments properly
    // link their previous segment pointers.
    clean_tail_tears(ordering, config, &f)
}

// This ensures that the last <# io buffers> segments on
// disk connect via their previous segment pointers in
// the header. This is important because we expect that
// the last <# io buffers> segments will join up, and we
// never reuse buffers within this safety range.
fn clean_tail_tears(
    mut ordering: BTreeMap<Lsn, LogId>,
    config: &Config,
    f: &File,
) -> Result<BTreeMap<Lsn, LogId>> {
    let safety_buffer = config.io_bufs;
    let logical_tail: Vec<Lsn> = ordering
        .iter()
        .rev()
        .take(safety_buffer)
        .map(|(lsn, _lid)| *lsn)
        .collect();

    let io_buf_size = config.io_buf_size;

    let mut tear_at = None;

    // make sure the last <# io_bufs> segments are contiguous
    for window in logical_tail.windows(2) {
        if window[0] != window[1] + io_buf_size as Lsn {
            error!("detected torn segment somewhere after {}", window[1]);
            tear_at = Some(window[1]);
        }
    }

    if let Some(tear) = tear_at {
        // we need to chop off the elements after the tear
        debug!("filtering out segments after detected tear at {}", tear);
        let tears = ordering
            .iter()
            .filter(|&(lsn, _lid)| *lsn > tear)
            .collect::<Vec<_>>();

        if tears.len() > config.io_bufs {
            error!(
                "encountered corruption in the middle \
                 of the database file, before the \
                 section that would be impacted by \
                 a normal crash. tear at lsn {} \
                 segments after that: {:?}",
                tear, tears,
            );
            return Err(Error::Corruption {
                at: DiskPtr::Inline(ordering[&tear]),
            });
        }

        for (&lsn, &lid) in &tears {
            if lsn > tear {
                error!("filtering out segment with lsn {} at lid {}", lsn, lid);

                f.pwrite_all(
                    &*vec![MessageKind::Corrupted.into(); SEG_HEADER_LEN],
                    lid,
                )
                .expect(
                    "should be able to mark a linear-orphan \
                     segment as invalid",
                );
                f.sync_all().expect(
                    "should be able to sync data \
                     file after purging linear-orphan",
                );
            }
        }
        ordering = ordering
            .into_iter()
            .filter(|&(lsn, _lid)| lsn <= tear)
            .collect();
    }

    Ok(ordering)
}

/// The log may be configured to write data
/// in several different ways, depending on
/// the constraints of the system using it.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum SegmentMode {
    /// Write to the end of the log, always.
    Linear,
    /// Keep track of segment utilization, and
    /// reuse segments when their contents are
    /// fully relocated elsewhere.
    /// Will try to copy data out of segments
    /// once they reach a configurable threshold.
    Gc,
}

pub(super) fn raw_segment_iter_from(
    lsn: Lsn,
    config: &Config,
) -> Result<LogIter> {
    let segment_len = config.io_buf_size as Lsn;
    let normalized_lsn = lsn / segment_len * segment_len;

    let ordering = scan_segment_lsns(0, &config)?;

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

    Ok(LogIter {
        config: config.clone(),
        max_lsn: std::i64::MAX,
        cur_lsn: SEG_HEADER_LEN as Lsn,
        segment_base: None,
        segment_iter,
    })
}

fn segment_is_drainable(
    idx: usize,
    num_segments: usize,
    live_pct: u8,
    len: usize,
    config: &Config,
) -> bool {
    // we calculate the cleanup threshold in a skewed way,
    // which encourages earlier segments to be rewritten
    // more frequently.
    let base_cleanup_threshold =
        (config.segment_cleanup_threshold * 100.) as usize;
    let cleanup_skew = config.segment_cleanup_skew;

    let relative_prop = if num_segments == 0 {
        50
    } else {
        (idx * 100) / num_segments
    };

    // we bias to having a higher threshold closer to segment 0
    let inverse_prop = 100 - relative_prop;
    let relative_threshold = cleanup_skew * inverse_prop / 100;
    let computed_threshold = base_cleanup_threshold + relative_threshold;

    // We should always be below 100, or we will rewrite everything
    let cleanup_threshold = if computed_threshold == 0 {
        1
    } else if computed_threshold > 99 {
        99
    } else {
        computed_threshold
    };

    let segment_low_pct = live_pct as usize <= cleanup_threshold;

    let segment_low_count =
        len < MINIMUM_ITEMS_PER_SEGMENT * 100 / cleanup_threshold;

    segment_low_pct || segment_low_count
}

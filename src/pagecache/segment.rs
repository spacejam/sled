//! The `SegmentAccountant` is an allocator for equally-
//! sized chunks of the underlying storage file (segments).
//!
//! It must maintain these critical safety properties:
//!
//! A. We must not overwrite existing segments when they
//!    contain the most-recent stable state for a page.
//! B. We must not overwrite existing segments when active
//!    threads may have references to `LogOffset`'s that point
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
//!    by ensuring that we never deactivate a
//!    segment until all data written into it, as
//!    well as all data written to earlier segments,
//!    has been written to disk and fsynced.
//! 2. we use a `epoch::Guard::defer()` from
//!    `IoBufs::write_to_log` that guarantees
//!    that we defer all segment deactivation
//!    until all threads are finished that
//!    may have witnessed pointers into a segment
//!    that will be marked for reuse in the future.
//!
//! Another concern that arises due to the fact that
//! IO buffers may be written out-of-order is the
//! correct recovery of segments. If there is data
//! loss in recently written segments, we must be
//! careful to preserve linearizability in the log.
//! To do this, we must detect "torn segments" that
//! were not able to be fully written before a crash
//! happened.
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

#![allow(unused_results)]

use std::mem;

use super::PageState;

use crate::pagecache::*;
use crate::*;

/// The segment accountant keeps track of the logical blocks
/// of storage. It scans through all segments quickly during
/// recovery and attempts to locate torn segments.
#[derive(Debug)]
pub(crate) struct SegmentAccountant {
    // static or one-time set
    config: RunningConfig,

    // TODO these should be sharded to improve performance
    segments: Vec<Segment>,
    clean_counter: usize,

    // TODO put behind a single mutex
    // NB MUST group pause_rewriting with ordering
    // and free!
    free: VecSet<LogOffset>,
    tip: LogOffset,
    max_stabilized_lsn: Lsn,
    to_clean: VecSet<LogOffset>,
    pause_rewriting: bool,
    ordering: BTreeMap<Lsn, LogOffset>,
    async_truncations: BTreeMap<LogOffset, OneShot<Result<()>>>,
}

/// A `Segment` holds the bookkeeping information for
/// a contiguous block of the disk. It may contain many
/// fragments from different pages. Over time, we track
/// when segments become reusable and allow them to be
/// overwritten for new data.
#[derive(Default, Clone, Debug, PartialEq)]
struct Segment {
    present: FastSet8<PageId>,
    // a copy of present that lets us make decisions
    // about draining without waiting for
    // deferred_replacements to take effect during
    // segment deactivation.
    not_yet_replaced: FastSet8<PageId>,
    deferred_rm_blob: FastSet8<BlobPointer>,
    // set of pages that we replaced from other segments
    deferred_replacements: FastSet8<(PageId, SegmentId)>,
    lsn: Option<Lsn>,
    state: SegmentState,
}

#[derive(Debug, Copy, Eq, Hash, Ord, PartialOrd, PartialEq, Clone)]
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

use self::SegmentState::{Active, Draining, Free, Inactive};

impl Default for SegmentState {
    fn default() -> Self {
        Free
    }
}

impl Segment {
    fn len(&self) -> usize {
        self.present.len()
    }

    fn is_free(&self) -> bool {
        self.state == Free
    }

    fn is_inactive(&self) -> bool {
        self.state == Inactive
    }

    fn _is_active(&self) -> bool {
        self.state == Active
    }

    fn is_draining(&self) -> bool {
        self.state == Draining
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
        self.deferred_rm_blob.clear();
        self.deferred_replacements.clear();
        self.lsn = Some(new_lsn);
        self.state = Active;
    }

    /// Transitions a segment to being in the `Inactive` state.
    /// Returns the set of page replacements that happened
    /// while this Segment was Active
    fn active_to_inactive(
        &mut self,
        lsn: Lsn,
        from_recovery: bool,
        config: &Config,
    ) -> Result<FastSet8<(PageId, usize)>> {
        trace!("setting Segment with lsn {:?} to Inactive", self.lsn());
        assert!(
            self.state == Active || self.state == Draining,
            "segment {} should have been \
             Active or Draining, before deactivating, but was {:?}",
            self.lsn(),
            self.state
        );
        if from_recovery {
            assert!(lsn >= self.lsn());
        } else {
            assert_eq!(self.lsn(), lsn);
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
        assert_eq!(lsn, self.lsn());
        // if this breaks, maybe we didn't implement the transition
        // logic right in write_to_log, and maybe a thread is
        // using the SA to add pids AFTER their calls to
        // res.complete() worked.
        assert_eq!(
            self.state, Active,
            "expected segment with lsn {} to be Active",
            lsn
        );
        self.not_yet_replaced.insert(pid);
        self.present.insert(pid);
    }

    /// Mark that a pid in this Segment has been relocated.
    /// The caller must provide the LSN of the removal.
    fn remove_pid(&mut self, pid: PageId, lsn: Lsn, in_recovery: bool) {
        assert!(lsn >= self.lsn());
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
            }
            Inactive | Draining => {
                self.present.remove(&pid);
            }
            Free => panic!("remove_pid called on a Free Segment"),
        }
    }

    fn defer_replace_pids(
        &mut self,
        deferred: FastSet8<(PageId, SegmentId)>,
        lsn: Lsn,
    ) {
        assert!(lsn >= self.lsn());
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
        let total = self.present.len();
        if total == 0 {
            return 100;
        }

        let live = self.present.len() * 100 / total;
        assert!(live <= 100);
        u8::try_from(live).unwrap()
    }

    fn can_free(&self) -> bool {
        self.state == Draining && self.is_empty()
    }

    fn is_empty(&self) -> bool {
        self.present.is_empty()
    }
}

impl SegmentAccountant {
    /// Create a new `SegmentAccountant` from previously recovered segments.
    pub(super) fn start(
        config: RunningConfig,
        snapshot: &Snapshot,
    ) -> Result<Self> {
        let mut ret = Self {
            config,
            segments: vec![],
            clean_counter: 0,
            free: VecSet::default(),
            tip: 0,
            max_stabilized_lsn: -1,
            to_clean: VecSet::default(),
            pause_rewriting: false,
            ordering: BTreeMap::default(),
            async_truncations: BTreeMap::default(),
        };

        if let SegmentMode::Linear = ret.config.segment_mode {
            // this is a hack to prevent segments from being overwritten
            // when operating without a `PageCache`
            ret.pause_rewriting();
        }

        ret.initialize_from_snapshot(snapshot)?;

        Ok(ret)
    }

    fn initial_segments(
        &self,
        snapshot: &Snapshot,
    ) -> Result<(Vec<Segment>, Vec<u64>)> {
        let segment_size = self.config.segment_size;
        let file_len = self.config.file.metadata()?.len();
        let empty_snapshot = snapshot.pt.is_empty();
        let number_of_segments =
            usize::try_from(file_len / segment_size as u64).unwrap()
                + if empty_snapshot
                    || file_len % u64::try_from(segment_size).unwrap()
                        < u64::try_from(SEG_HEADER_LEN).unwrap()
                {
                    0
                } else {
                    1
                };

        if empty_snapshot {
            assert_eq!(number_of_segments, 0);
        }

        // generate segments from snapshot lids
        let mut segments = vec![Segment::default(); number_of_segments];
        let mut segment_sizes = vec![0_u64; number_of_segments];

        let mut add = |pid,
                       lsn,
                       sz,
                       lid: LogOffset,
                       segments: &mut Vec<Segment>| {
            let idx = assert_usize(lid / segment_size as LogOffset);
            trace!(
                "adding lsn: {} lid: {} for pid {} to segment {} during SA recovery",
                lsn,
                lid,
                pid,
                idx
            );
            let segment_lsn = lsn / segment_size as Lsn * segment_size as Lsn;
            segments[idx].recovery_ensure_initialized(segment_lsn);
            segments[idx].insert_pid(pid, segment_lsn);
            segment_sizes[idx] += sz;
        };

        for (pid, state) in &snapshot.pt {
            match state {
                PageState::Present(coords) => {
                    for (lsn, ptr, sz) in coords {
                        add(*pid, lsn, *sz, ptr.lid(), &mut segments);
                    }
                }
                PageState::Free(lsn, ptr) => {
                    add(
                        *pid,
                        lsn,
                        u64::try_from(MSG_HEADER_LEN).unwrap(),
                        ptr.lid(),
                        &mut segments,
                    );
                }
            }
        }

        Ok((segments, segment_sizes))
    }

    fn initialize_from_snapshot(&mut self, snapshot: &Snapshot) -> Result<()> {
        let segment_size = self.config.segment_size;
        let (mut segments, segment_sizes) = self.initial_segments(snapshot)?;

        let currently_active_segment = {
            // this logic allows us to free the last
            // active segment if it was empty.
            let prospective_currently_active_segment =
                usize::try_from(snapshot.last_lid / segment_size as LogOffset)
                    .unwrap();
            if let Some(segment) =
                segments.get(prospective_currently_active_segment)
            {
                if segment.is_empty() {
                    // we want to add this to the free list below,
                    // so don't skip freeing it for being active
                    usize::max_value()
                } else {
                    prospective_currently_active_segment
                }
            } else {
                // segment was not used yet
                usize::max_value()
            }
        };

        let cleanup_threshold =
            u64::from(self.config.segment_cleanup_threshold);
        let drain_sz =
            u64::try_from(segment_size).unwrap() * 100 / cleanup_threshold;

        for (idx, segment) in segments.iter_mut().enumerate() {
            let segment_base = idx as LogOffset * segment_size as LogOffset;

            if segment_base >= self.tip {
                // set tip above the beginning of any
                self.tip = segment_base + segment_size as LogOffset;
                trace!(
                    "raised self.tip to {} during SA initialization",
                    self.tip
                );
            }

            let segment_lsn = if let Some(lsn) = segment.lsn {
                lsn
            } else {
                // this segment was not used in the recovered
                // snapshot, so we can assume it is free
                let idx = self.segment_id(segment_base);
                self.segments[idx].state = Free;
                self.free_segment(segment_base, false);
                continue;
            };

            if idx != currently_active_segment
                && segment_lsn + segment_size as Lsn
                    <= snapshot.max_header_stable_lsn
            {
                if segment_sizes[idx] == 0 {
                    // can free
                    trace!(
                        "freeing segment with lid {} during SA initialization",
                        segment_base
                    );
                    if self.tip == segment_base + segment_size as LogOffset {
                        self.tip -= segment_size as LogOffset;
                    } else {
                        segment.state = Free;
                        self.free_segment(segment_base, true);
                    }
                } else if segment_sizes[idx] <= drain_sz {
                    trace!(
                        "SA draining segment at {} during startup \
                         with size {} being < drain size of {}",
                        segment_base,
                        segment_sizes[idx],
                        drain_sz
                    );
                    segment.state = Draining;
                    self.to_clean.insert(segment_base);
                }
            }
        }

        trace!("initialized self.segments to {:?}", segments);
        self.segments = segments;

        self.ordering = self
            .segments
            .iter()
            .enumerate()
            .filter_map(|(id, s)| {
                if s.lsn.is_some() {
                    Some((s.lsn(), id as LogOffset * segment_size as LogOffset))
                } else {
                    None
                }
            })
            .collect();

        trace!("initialized self.ordering to {:?}", self.ordering);

        Ok(())
    }

    fn free_segment(&mut self, lid: LogOffset, in_recovery: bool) {
        debug!("freeing segment {}", lid);
        debug!("free list before free {:?}", self.free);

        let idx = self.segment_id(lid);
        assert!(
            self.tip > lid,
            "freed a segment above our current file tip, \
             please report this bug!"
        );
        assert_eq!(self.segments[idx].state, Free);
        assert!(!self.free.contains(&lid), "double-free of a segment occurred");

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
    /// is necessary to preserve consistency while concurrently iterating
    /// through the log during snapshot creation.
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

        let new_idx = self.segment_id(new_ptr.lid());

        // make sure we're not actively trying to replace the destination
        let new_segment_start =
            new_idx as LogOffset * self.config.segment_size as LogOffset;

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

            let old_idx = self.segment_id(old_lid);
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
                    old_idx * self.config.segment_size
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

        let segment_start = (idx * self.config.segment_size) as LogOffset;

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
    /// segments eligible for cleaning that it should
    /// try to rewrite elsewhere.
    pub(super) fn clean(
        &mut self,
        ignore_pid: Option<PageId>,
    ) -> Option<PageId> {
        let seg_offset = if self.to_clean.is_empty() || self.to_clean.len() == 1
        {
            0
        } else {
            self.clean_counter % self.to_clean.len()
        };

        let item = self.to_clean.get(seg_offset).cloned();

        if let Some(lid) = item {
            let idx = self.segment_id(lid);
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
            if Some(*pid) == ignore_pid {
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
        let idx = self.segment_id(ptr.lid());

        // make sure we're not actively trying to replace the destination
        let new_segment_start =
            idx as LogOffset * self.config.segment_size as LogOffset;

        assert!(!self.to_clean.contains(&new_segment_start));

        let segment = &mut self.segments[idx];

        let segment_lsn = lsn / self.config.segment_size as Lsn
            * self.config.segment_size as Lsn;

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
        let segment_size = self.config.segment_size as Lsn;
        let lsn = ((stable_lsn / segment_size) - 1) * segment_size;
        trace!(
            "stabilize({}), normalized: {}, last: {}",
            stable_lsn,
            lsn,
            self.max_stabilized_lsn
        );
        if self.max_stabilized_lsn >= lsn {
            trace!(
                "expected stabilization lsn {} \
                 to be greater than the previous value of {}",
                lsn,
                self.max_stabilized_lsn
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
        let idx = self.segment_id(lid);

        trace!(
            "deactivating segment with lsn {}: {:?}",
            lsn,
            self.segments[idx]
        );

        let replacements = if self.segments[idx].state == Active {
            self.segments[idx].active_to_inactive(lsn, false, &self.config)?
        } else {
            Default::default()
        };

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
                old_idx * self.config.segment_size,
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
                 pid {} old_ptr segment: {} segments with (offset, state, present): {:?}",
                pid,
                old_idx * self.config.segment_size,
                self.segments
                    .iter()
                    .enumerate()
                    .filter_map(|(i, s)| {
                        if s.present.contains(&pid) {
                            Some((
                                i * self.config.segment_size,
                                s.state,
                                s.present.clone(),
                            ))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            );

            old_segment.remove_pid(pid, lsn, false);
        }

        for old_idx in old_segments {
            self.possibly_clean_or_free_segment(old_idx, lsn);
        }

        // if we have a lot of free segments in our whole file,
        // let's start relocating the current tip to boil it down
        let free_segs = self.segments.iter().filter(|s| s.is_free()).count();
        let inactive_segs =
            self.segments.iter().filter(|s| s.is_inactive()).count();
        let free_ratio = (free_segs * 100) / (1 + free_segs + inactive_segs);

        if free_ratio >= usize::from(self.config.segment_cleanup_threshold)
            && inactive_segs > 5
        {
            let last_index =
                self.segments.iter().rposition(Segment::is_inactive).unwrap();

            let segment_start =
                (last_index * self.config.segment_size) as LogOffset;

            self.to_clean.insert(segment_start);
        }

        Ok(())
    }

    fn bump_tip(&mut self) -> LogOffset {
        let lid = self.tip;

        let truncations = self.async_truncations.split_off(&lid);

        for (_at, truncation) in truncations {
            match truncation.wait() {
                Some(Ok(())) => {}
                error => {
                    error!("failed to shrink file: {:?}", error);
                }
            }
        }

        self.tip += self.config.segment_size as LogOffset;

        trace!("advancing file tip from {} to {}", lid, self.tip);

        lid
    }

    /// Returns the next offset to write a new segment in.
    pub(super) fn next(&mut self, lsn: Lsn) -> Result<LogOffset> {
        let _measure = Measure::new(&M.accountant_next);

        assert_eq!(
            lsn % self.config.segment_size as Lsn,
            0,
            "unaligned Lsn provided to next!"
        );

        let free: Vec<LogOffset> = self
            .free
            .iter()
            .filter(|lid| {
                let idx = usize::try_from(
                    *lid / self.config.segment_size as LogOffset,
                )
                .unwrap();
                if let Some(last_lsn) = self.segments[idx].lsn {
                    last_lsn < self.max_stabilized_lsn
                } else {
                    true
                }
            })
            .copied()
            .collect();

        trace!("evaluating free list {:?} in SA::next", free);

        // truncate if possible
        while self.tip != 0 && self.free.len() > 1 {
            let last_segment = self.tip - self.config.segment_size as LogOffset;
            if free.contains(&last_segment) {
                self.free.remove(&last_segment);
                self.truncate(last_segment)?;
            } else {
                break;
            }
        }

        // pop free or add to end
        let safe = free.first();

        let lid = match (self.pause_rewriting, safe) {
            (true, _) | (_, None) => self.bump_tip(),
            (_, Some(&next)) => {
                self.free.remove(&next);
                next
            }
        };

        // pin lsn to this segment
        let idx = self.segment_id(lid);

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

        assert!(
            lsn >= Lsn::try_from(lid).unwrap(),
            "lsn {} should always be greater than or equal to lid {}",
            lsn,
            lid
        );

        Ok(lid)
    }

    /// Returns an iterator over a snapshot of current segment
    /// log sequence numbers and their corresponding file offsets.
    pub(super) fn segment_snapshot_iter_from(
        &mut self,
        lsn: Lsn,
    ) -> Box<dyn Iterator<Item = (Lsn, LogOffset)>> {
        assert!(
            !self.ordering.is_empty(),
            "expected ordering to have been initialized already"
        );

        assert!(
            self.pause_rewriting,
            "must pause rewriting before \
             iterating over segments"
        );

        let segment_len = self.config.segment_size as Lsn;
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
    fn truncate(&mut self, at: LogOffset) -> Result<()> {
        assert_eq!(
            at % self.config.segment_size as LogOffset,
            0,
            "new length must be io-buf-len aligned"
        );

        self.tip = at;

        assert!(!self.free.contains(&at), "double-free of a segment occurred");

        trace!("asynchronously truncating file to length {}", at);
        let (completer, promise) = OneShot::pair();

        let config = self.config.clone();

        let _result = threadpool::spawn(move || {
            debug!("truncating file to length {}", at);
            let res = config
                .file
                .set_len(at)
                .and_then(|_| config.file.sync_all())
                .map_err(|e| e.into());
            completer.fill(res);
        });

        #[cfg(test)]
        _result.unwrap();

        if self.async_truncations.insert(at, promise).is_some() {
            panic!(
                "somehow segment {} was truncated before \
                 the previous truncation completed",
                at
            );
        }

        Ok(())
    }

    fn segment_id(&mut self, lid: LogOffset) -> SegmentId {
        let idx = assert_usize(lid / self.config.segment_size as LogOffset);

        // TODO never resize like this, make it a single
        // responsibility when the tip is bumped / truncated.
        if self.segments.len() < idx + 1 {
            self.segments.resize(idx + 1, Segment::default());
        }

        idx
    }
}

/// The log may be configured to write data
/// in several different ways, depending on
/// the constraints of the system using it.
#[derive(Debug, PartialEq, Clone, Copy)]
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
    let base_cleanup_threshold = usize::from(config.segment_cleanup_threshold);
    let cleanup_skew = config.segment_cleanup_skew;

    let relative_prop =
        if num_segments == 0 { 50 } else { (idx * 100) / num_segments };

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

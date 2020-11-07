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

use std::{collections::BTreeSet, mem};

use super::PageState;

use crate::pagecache::*;
use crate::*;

/// A operation that can be applied asynchronously.
#[derive(Debug, Clone)]
pub(crate) enum SegmentOp {
    Link {
        pid: PageId,
        cache_info: CacheInfo,
    },
    Replace {
        pid: PageId,
        lsn: Lsn,
        old_cache_infos: Vec<CacheInfo>,
        new_cache_info: CacheInfo,
    },
}

/// The segment accountant keeps track of the logical blocks
/// of storage. It scans through all segments quickly during
/// recovery and attempts to locate torn segments.
#[derive(Debug)]
pub(crate) struct SegmentAccountant {
    // static or one-time set
    config: RunningConfig,

    // TODO these should be sharded to improve performance
    segments: Vec<Segment>,

    // TODO put behind a single mutex
    free: BTreeSet<LogOffset>,
    tip: LogOffset,
    max_stabilized_lsn: Lsn,
    segment_cleaner: SegmentCleaner,
    ordering: BTreeMap<Lsn, LogOffset>,
    async_truncations: BTreeMap<LogOffset, OneShot<Result<()>>>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SegmentCleaner {
    inner: Arc<Mutex<BTreeMap<LogOffset, BTreeSet<PageId>>>>,
}

impl SegmentCleaner {
    pub(crate) fn pop(&self) -> Option<(PageId, LogOffset)> {
        let mut inner = self.inner.lock();
        let offset = {
            let (offset, pids) = inner.iter_mut().next()?;
            if !pids.is_empty() {
                let pid = pids.iter().next().copied().unwrap();
                pids.remove(&pid);
                return Some((pid, *offset));
            }
            *offset
        };
        inner.remove(&offset);
        None
    }

    fn add_pids(&self, offset: LogOffset, pids: BTreeSet<PageId>) {
        let mut inner = self.inner.lock();
        let prev = inner.insert(offset, pids);
        assert!(prev.is_none());
    }

    fn remove_pids(&self, offset: LogOffset) {
        let mut inner = self.inner.lock();
        inner.remove(&offset);
    }
}

impl Drop for SegmentAccountant {
    fn drop(&mut self) {
        for segment in &self.segments {
            let segment_utilization = match segment {
                Segment::Free(_) | Segment::Draining(_) => 0,
                Segment::Active(Active { rss, .. })
                | Segment::Inactive(Inactive { rss, .. }) => *rss,
            };
            M.segment_utilization_shutdown.measure(segment_utilization as u64);
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Free {
    previous_lsn: Option<Lsn>,
}

#[derive(Debug, Clone, Default)]
struct Active {
    lsn: Lsn,
    rss: usize,
    deferred_replaced_rss: usize,
    deferred_replaced_pids: BTreeSet<PageId>,
    pids: BTreeSet<PageId>,
    latest_replacement_lsn: Lsn,
    can_free_upon_deactivation: FastSet8<Lsn>,
    deferred_rm_blob: FastSet8<BlobPointer>,
}

#[derive(Debug, Clone, Default)]
struct Inactive {
    lsn: Lsn,
    rss: usize,
    pids: BTreeSet<PageId>,
    max_pids: usize,
    replaced_pids: usize,
    latest_replacement_lsn: Lsn,
}

#[derive(Debug, Clone, Default)]
struct Draining {
    lsn: Lsn,
    max_pids: usize,
    replaced_pids: usize,
    latest_replacement_lsn: Lsn,
}

/// A `Segment` holds the bookkeeping information for
/// a contiguous block of the disk. It may contain many
/// fragments from different pages. Over time, we track
/// when segments become reusable and allow them to be
/// overwritten for new data.
#[derive(Clone, Debug)]
enum Segment {
    /// the segment is marked for reuse, should never receive
    /// new pids,
    Free(Free),

    /// the segment is being written to or actively recovered, and
    /// will have pages assigned to it
    Active(Active),

    /// the segment is no longer being written to or recovered, and
    /// will have pages marked as relocated from it
    Inactive(Inactive),

    /// the segment is having its resident pages relocated before
    /// becoming free
    Draining(Draining),
}

impl Default for Segment {
    fn default() -> Self {
        Segment::Free(Free { previous_lsn: None })
    }
}

impl Segment {
    fn is_free(&self) -> bool {
        if let Segment::Free(_) = self {
            true
        } else {
            false
        }
    }

    fn is_active(&self) -> bool {
        if let Segment::Active { .. } = self {
            true
        } else {
            false
        }
    }

    fn is_inactive(&self) -> bool {
        if let Segment::Inactive { .. } = self {
            true
        } else {
            false
        }
    }

    fn free_to_active(&mut self, new_lsn: Lsn) {
        trace!("setting Segment to Active with new lsn {:?}", new_lsn,);
        assert!(self.is_free());

        *self = Segment::Active(Active {
            lsn: new_lsn,
            rss: 0,
            deferred_replaced_rss: 0,
            deferred_replaced_pids: BTreeSet::default(),
            pids: BTreeSet::default(),
            latest_replacement_lsn: 0,
            can_free_upon_deactivation: FastSet8::default(),
            deferred_rm_blob: FastSet8::default(),
        })
    }

    /// Transitions a segment to being in the `Inactive` state.
    /// Returns the set of page replacements that happened
    /// while this Segment was Active
    fn active_to_inactive(
        &mut self,
        lsn: Lsn,
        config: &Config,
    ) -> Result<FastSet8<Lsn>> {
        trace!("setting Segment with lsn {:?} to Inactive", self.lsn());

        let (inactive, ret) = if let Segment::Active(active) = self {
            assert!(lsn >= active.lsn);

            // now we can push any deferred blob removals to the removed set
            for ptr in &active.deferred_rm_blob {
                trace!(
                    "removing blob {} while transitioning \
                     segment lsn {:?} to Inactive",
                    ptr,
                    active.lsn,
                );
                remove_blob(*ptr, config)?;
            }

            let inactive = Segment::Inactive(Inactive {
                lsn: active.lsn,
                rss: active
                    .rss
                    .checked_sub(active.deferred_replaced_rss)
                    .unwrap(),
                max_pids: active.pids.len(),
                replaced_pids: active.deferred_replaced_pids.len(),
                pids: &active.pids - &active.deferred_replaced_pids,
                latest_replacement_lsn: active.latest_replacement_lsn,
            });

            let can_free = mem::replace(
                &mut active.can_free_upon_deactivation,
                Default::default(),
            );

            (inactive, can_free)
        } else {
            panic!("called active_to_inactive on {:?}", self);
        };

        *self = inactive;
        Ok(ret)
    }

    fn inactive_to_draining(&mut self, lsn: Lsn) -> BTreeSet<PageId> {
        trace!("setting Segment with lsn {:?} to Draining", self.lsn());

        if let Segment::Inactive(inactive) = self {
            assert!(lsn >= inactive.lsn);
            let ret = mem::replace(&mut inactive.pids, Default::default());
            *self = Segment::Draining(Draining {
                lsn: inactive.lsn,
                max_pids: inactive.max_pids,
                replaced_pids: inactive.replaced_pids,
                latest_replacement_lsn: inactive.latest_replacement_lsn,
            });
            ret
        } else {
            panic!("called inactive_to_draining on {:?}", self);
        }
    }

    fn defer_free_lsn(&mut self, lsn: Lsn) {
        if let Segment::Active(active) = self {
            active.can_free_upon_deactivation.insert(lsn);
        } else {
            panic!("called defer_free_lsn on segment {:?}", self);
        }
    }

    fn draining_to_free(&mut self, lsn: Lsn) -> Lsn {
        trace!("setting Segment with lsn {:?} to Free", self.lsn());

        if let Segment::Draining(draining) = self {
            let old_lsn = draining.lsn;
            assert!(lsn >= old_lsn);
            let replacement_lsn = draining.latest_replacement_lsn;
            *self = Segment::Free(Free { previous_lsn: Some(old_lsn) });
            replacement_lsn
        } else {
            panic!("called draining_to_free on {:?}", self);
        }
    }

    fn recovery_ensure_initialized(&mut self, lsn: Lsn) {
        if self.is_free() {
            trace!("(snapshot) recovering segment with base lsn {}", lsn);
            self.free_to_active(lsn);
        }
    }

    fn lsn(&self) -> Lsn {
        match self {
            Segment::Active(Active { lsn, .. })
            | Segment::Inactive(Inactive { lsn, .. })
            | Segment::Draining(Draining { lsn, .. }) => *lsn,
            Segment::Free(_) => panic!("called lsn on Segment::Free"),
        }
    }

    /// Add a pid to the Segment. The caller must provide
    /// the Segment's LSN.
    fn insert_pid(&mut self, pid: PageId, lsn: Lsn, size: usize) {
        // if this breaks, maybe we didn't implement the transition
        // logic right in write_to_log, and maybe a thread is
        // using the SA to add pids AFTER their calls to
        // res.complete() worked.
        if let Segment::Active(active) = self {
            assert_eq!(
                lsn, active.lsn,
                "insert_pid specified lsn {} for pid {} in segment {:?}",
                lsn, pid, active
            );
            active.pids.insert(pid);
            active.rss += size;
        } else {
            panic!("called insert_pid on {:?}", self);
        }
    }

    fn remove_pid(&mut self, pid: PageId, replacement_lsn: Lsn, sz: usize) {
        trace!(
            "removing pid {} at lsn {} from segment {:?}",
            pid,
            replacement_lsn,
            self
        );
        match self {
            Segment::Active(active) => {
                assert!(active.lsn <= replacement_lsn);
                if replacement_lsn != active.lsn {
                    active.deferred_replaced_pids.insert(pid);
                }
                active.deferred_replaced_rss += sz;
                if replacement_lsn > active.latest_replacement_lsn {
                    active.latest_replacement_lsn = replacement_lsn;
                }
            }
            Segment::Inactive(Inactive {
                pids,
                lsn,
                rss,
                latest_replacement_lsn,
                replaced_pids,
                ..
            }) => {
                assert!(*lsn <= replacement_lsn);
                if replacement_lsn != *lsn {
                    pids.remove(&pid);
                    *replaced_pids += 1;
                }
                *rss = rss.checked_sub(sz).unwrap();
                if replacement_lsn > *latest_replacement_lsn {
                    *latest_replacement_lsn = replacement_lsn;
                }
            }
            Segment::Draining(Draining {
                lsn,
                latest_replacement_lsn,
                replaced_pids,
                ..
            }) => {
                assert!(*lsn <= replacement_lsn);
                if replacement_lsn != *lsn {
                    *replaced_pids += 1;
                }
                if replacement_lsn > *latest_replacement_lsn {
                    *latest_replacement_lsn = replacement_lsn;
                }
            }
            Segment::Free(_) => {
                panic!("called remove pid {} on Segment::Free", pid)
            }
        }
    }

    fn remove_blob(
        &mut self,
        blob_ptr: BlobPointer,
        config: &Config,
    ) -> Result<()> {
        match self {
            Segment::Active(active) => {
                // we have received a removal before
                // transferring this segment to Inactive, so
                // we defer this pid's removal until the transfer.
                active.deferred_rm_blob.insert(blob_ptr);
            }
            Segment::Inactive(_) | Segment::Draining(_) => {
                trace!(
                    "directly removing blob {} that was referred-to \
                     in a segment that has already been marked as Inactive \
                     or Draining.",
                    blob_ptr,
                );
                remove_blob(blob_ptr, config)?;
            }
            Segment::Free(_) => panic!("remove_blob called on a Free Segment"),
        }

        Ok(())
    }

    fn can_free(&self) -> bool {
        if let Segment::Draining(draining) = self {
            draining.replaced_pids == draining.max_pids
        } else {
            false
        }
    }
}

impl SegmentAccountant {
    /// Create a new `SegmentAccountant` from previously recovered segments.
    pub(super) fn start(
        config: RunningConfig,
        snapshot: &Snapshot,
        segment_cleaner: SegmentCleaner,
    ) -> Result<Self> {
        let _measure = Measure::new(&M.start_segment_accountant);
        let mut ret = Self {
            config,
            segments: vec![],
            free: BTreeSet::default(),
            tip: 0,
            max_stabilized_lsn: -1,
            segment_cleaner,
            ordering: BTreeMap::default(),
            async_truncations: BTreeMap::default(),
        };

        ret.initialize_from_snapshot(snapshot)?;

        if let Some(max_free) = ret.free.iter().max() {
            assert!(
                ret.tip > *max_free,
                "expected recovered tip {} to \
                be above max item in recovered \
                free list {:?}",
                ret.tip,
                ret.free
            );
        }

        debug!(
            "SA starting with tip {} stable {} free {:?}",
            ret.tip, ret.max_stabilized_lsn, ret.free,
        );

        Ok(ret)
    }

    fn initial_segments(&self, snapshot: &Snapshot) -> Result<Vec<Segment>> {
        let segment_size = self.config.segment_size;
        let file_len = self.config.file.metadata()?.len();
        let number_of_segments =
            usize::try_from(file_len / segment_size as u64).unwrap()
                + if file_len % segment_size as u64 == 0 { 0 } else { 1 };

        // generate segments from snapshot lids
        let mut segments = vec![Segment::default(); number_of_segments];

        // sometimes the current segment is still empty, after only
        // recovering the segment header but no valid messages yet
        if let Some(tip_lid) = snapshot.active_segment {
            let tip_idx =
                usize::try_from(tip_lid / segment_size as LogOffset).unwrap();
            if tip_idx == number_of_segments {
                segments.push(Segment::default());
            }
            if segments.len() <= tip_idx {
                error!("failed to properly initialize segments, suspected disk corruption");
                return Err(Error::corruption(None));
            }
            trace!(
                "setting segment for tip_lid {} to stable_lsn {}",
                tip_lid,
                self.config.normalize(snapshot.stable_lsn.unwrap_or(0))
            );
            segments[tip_idx].recovery_ensure_initialized(
                self.config.normalize(snapshot.stable_lsn.unwrap_or(0)),
            );
        }

        let add =
            |pid, lsn: Lsn, sz, lid: LogOffset, segments: &mut Vec<Segment>| {
                let idx = assert_usize(lid / segment_size as LogOffset);
                trace!(
                    "adding lsn: {} lid: {} sz: {} for \
                     pid {} to segment {} during SA recovery",
                    lsn,
                    lid,
                    sz,
                    pid,
                    idx
                );
                let segment_lsn = self.config.normalize(lsn);
                segments[idx].recovery_ensure_initialized(segment_lsn);
                segments[idx].insert_pid(
                    pid,
                    segment_lsn,
                    usize::try_from(sz).unwrap(),
                );
            };

        for (pid, state) in snapshot.pt.iter().enumerate() {
            match state {
                PageState::Present { base, frags } => {
                    add(
                        pid as PageId,
                        base.0,
                        base.2,
                        base.1.lid(),
                        &mut segments,
                    );
                    for (lsn, ptr, sz) in frags {
                        add(pid as PageId, *lsn, *sz, ptr.lid(), &mut segments);
                    }
                }
                PageState::Free(lsn, ptr) => {
                    add(
                        pid as PageId,
                        *lsn,
                        u64::try_from(MAX_MSG_HEADER_LEN).unwrap(),
                        ptr.lid(),
                        &mut segments,
                    );
                }
                _ => panic!("tried to recover pagestate from a {:?}", state),
            }
        }

        Ok(segments)
    }

    fn initialize_from_snapshot(&mut self, snapshot: &Snapshot) -> Result<()> {
        let segment_size = self.config.segment_size;
        let segments = self.initial_segments(snapshot)?;

        self.segments = segments;

        let mut to_free = vec![];
        let mut maybe_clean = vec![];

        let currently_active_segment = snapshot
            .active_segment
            .map(|tl| usize::try_from(tl / segment_size as LogOffset).unwrap());

        for (idx, segment) in self.segments.iter_mut().enumerate() {
            let segment_base = idx as LogOffset * segment_size as LogOffset;

            if segment_base >= self.tip {
                // set tip above the beginning of any
                self.tip = segment_base + segment_size as LogOffset;
                trace!(
                    "raised self.tip to {} during SA initialization",
                    self.tip
                );
            }

            if segment.is_free() {
                // this segment was not used in the recovered
                // snapshot, so we can assume it is free
                to_free.push(segment_base);
                continue;
            }

            let segment_lsn = segment.lsn();

            if let Some(tip_idx) = currently_active_segment {
                if tip_idx != idx {
                    maybe_clean.push((idx, segment_lsn));
                }
            }
        }

        for segment_base in to_free {
            self.free_segment(segment_base)?;
            io_fail!(self.config, "zero garbage segment SA");
            pwrite_all(
                &self.config.file,
                &*vec![MessageKind::Corrupted.into(); self.config.segment_size],
                segment_base,
            )?;
        }

        // we want to complete all truncations because
        // they could cause calls to `next` to block.
        for (_, promise) in self.async_truncations.split_off(&0) {
            promise.wait().expect("threadpool should not crash")?;
        }

        for (idx, segment_lsn) in maybe_clean {
            self.possibly_clean_or_free_segment(idx, segment_lsn)?;
        }

        trace!("initialized self.segments to {:?}", self.segments);

        self.ordering = self
            .segments
            .iter()
            .enumerate()
            .filter_map(|(id, s)| {
                if s.is_free() {
                    None
                } else {
                    Some((s.lsn(), id as LogOffset * segment_size as LogOffset))
                }
            })
            .collect();

        trace!("initialized self.ordering to {:?}", self.ordering);

        Ok(())
    }

    fn free_segment(&mut self, lid: LogOffset) -> Result<()> {
        debug!("freeing segment {}", lid);
        trace!("free list before free {:?}", self.free);
        self.segment_cleaner.remove_pids(lid);

        let idx = self.segment_id(lid);
        assert!(
            self.tip > lid,
            "freed a segment at {} above our current file tip {}, \
             please report this bug!",
            lid,
            self.tip,
        );
        assert!(self.segments[idx].is_free());
        assert!(!self.free.contains(&lid), "double-free of a segment occurred");

        self.free.insert(lid);

        // remove the old ordering from our list
        if let Segment::Free(Free { previous_lsn: Some(last_lsn) }) =
            self.segments[idx]
        {
            trace!(
                "removing segment {} with lsn {} from ordering",
                lid,
                last_lsn
            );
            self.ordering.remove(&last_lsn);
        }

        // we want to avoid aggressive truncation because it can cause
        // blocking if we allocate a segment that was just truncated.
        let laziness_factor = 1;

        // truncate if possible
        while self.tip != 0 && self.free.len() > laziness_factor {
            let last_segment = self.tip - self.config.segment_size as LogOffset;
            if self.free.contains(&last_segment) {
                self.free.remove(&last_segment);
                self.truncate(last_segment)?;
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Asynchronously apply a GC-related operation. Used in a flat-combining
    /// style that allows callers to avoid blocking while sending these
    /// messages to this module.
    pub(super) fn apply_op(&mut self, op: &SegmentOp) -> Result<()> {
        use SegmentOp::*;
        match op {
            Link { pid, cache_info } => self.mark_link(*pid, *cache_info),
            Replace { pid, lsn, old_cache_infos, new_cache_info } => {
                self.mark_replace(*pid, *lsn, old_cache_infos, *new_cache_info)?
            }
        }
        Ok(())
    }

    /// Called by the `PageCache` when a page has been rewritten completely.
    /// We mark all of the old segments that contained the previous state
    /// from the page, and if the old segments are empty or clear enough to
    /// begin accelerated cleaning we mark them as so.
    pub(super) fn mark_replace(
        &mut self,
        pid: PageId,
        lsn: Lsn,
        old_cache_infos: &[CacheInfo],
        new_cache_info: CacheInfo,
    ) -> Result<()> {
        let _measure = Measure::new(&M.accountant_mark_replace);

        self.mark_link(pid, new_cache_info);

        trace!(
            "mark_replace pid {} from cache infos {:?} to cache info {:?} with lsn {}",
            pid,
            old_cache_infos,
            new_cache_info,
            self.config.normalize(lsn)
        );

        let new_idx = self.segment_id(new_cache_info.pointer.lid());

        // Do we need to schedule any blob cleanups?
        // Not if we just moved the pointer without changing
        // the underlying blob, as is the case with a single Blob
        // with nothing else.
        let schedule_rm_blob = !(old_cache_infos.len() == 1
            && old_cache_infos[0].pointer.is_blob());

        let mut removals = FastMap8::default();

        for old_cache_info in old_cache_infos {
            let old_ptr = &old_cache_info.pointer;
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
            let entry = removals.entry(old_idx).or_insert(0);
            *entry += old_cache_info.log_size;
        }

        for (old_idx, replaced_size) in removals {
            self.segments[old_idx].remove_pid(
                pid,
                self.config.normalize(lsn),
                usize::try_from(replaced_size).unwrap(),
            );
            self.possibly_clean_or_free_segment(old_idx, new_cache_info.lsn)?;
        }

        Ok(())
    }

    /// Called from `PageCache` when some state has been added
    /// to a logical page at a particular offset. We ensure the
    /// page is present in the segment's page set.
    pub(super) fn mark_link(&mut self, pid: PageId, cache_info: CacheInfo) {
        let _measure = Measure::new(&M.accountant_mark_link);

        trace!("mark_link pid {} at cache info {:?}", pid, cache_info);
        let idx = self.segment_id(cache_info.pointer.lid());

        let segment = &mut self.segments[idx];

        let segment_lsn = cache_info.lsn / self.config.segment_size as Lsn
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

        segment.insert_pid(
            pid,
            segment_lsn,
            usize::try_from(cache_info.log_size).unwrap(),
        );
    }

    fn possibly_clean_or_free_segment(
        &mut self,
        idx: usize,
        lsn: Lsn,
    ) -> Result<()> {
        let cleanup_threshold = SEGMENT_CLEANUP_THRESHOLD;

        let segment_start = (idx * self.config.segment_size) as LogOffset;

        if let Segment::Inactive(inactive) = &mut self.segments[idx] {
            let live_pct = inactive.rss * 100 / self.config.segment_size;

            let can_drain = live_pct <= cleanup_threshold;

            if can_drain {
                // can be cleaned
                trace!(
                    "SA inserting {} into to_clean from possibly_clean_or_free_segment",
                    segment_start
                );
                let to_clean = self.segments[idx].inactive_to_draining(lsn);
                self.segment_cleaner.add_pids(segment_start, to_clean);
            }
        }

        let segment_lsn = self.segments[idx].lsn();

        if self.segments[idx].can_free() {
            // can be reused immediately
            let replacement_lsn = self.segments[idx].draining_to_free(lsn);

            if self.ordering.contains_key(&replacement_lsn) {
                let replacement_lid = self.ordering[&replacement_lsn];
                let replacement_idx = usize::try_from(
                    replacement_lid / self.config.segment_size as u64,
                )
                .unwrap();

                if self.segments[replacement_idx].is_active() {
                    trace!(
                        "deferring free of segment {} in possibly_clean_or_free_segment",
                        segment_start
                    );
                    self.segments[replacement_idx].defer_free_lsn(segment_lsn);
                } else {
                    assert!(replacement_lsn <= self.max_stabilized_lsn);
                    self.free_segment(segment_start)?;
                }
            } else {
                // replacement segment has already been freed, so we can
                // go right to freeing this one too
                self.free_segment(segment_start)?;
            }
        }

        Ok(())
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
            "deactivating segment with lid {} lsn {}: {:?}",
            lid,
            lsn,
            self.segments[idx]
        );

        let freeable_segments = if self.segments[idx].is_active() {
            self.segments[idx].active_to_inactive(lsn, &self.config)?
        } else {
            Default::default()
        };

        for lsn in freeable_segments {
            let segment_start = self.ordering[&lsn];
            assert_ne!(segment_start, lid);
            self.free_segment(segment_start)?;
        }

        self.possibly_clean_or_free_segment(idx, lsn)?;

        // if we have a lot of free segments in our whole file,
        // let's start relocating the current tip to boil it down
        let free_segs = self.segments.iter().filter(|s| s.is_free()).count();
        let inactive_segs =
            self.segments.iter().filter(|s| s.is_inactive()).count();
        let free_ratio = (free_segs * 100) / (1 + free_segs + inactive_segs);

        if free_ratio >= SEGMENT_CLEANUP_THRESHOLD && inactive_segs > 5 {
            let last_index =
                self.segments.iter().rposition(Segment::is_inactive).unwrap();

            let segment_start =
                (last_index * self.config.segment_size) as LogOffset;

            let to_clean = self.segments[last_index].inactive_to_draining(lsn);
            self.segment_cleaner.add_pids(segment_start, to_clean);
        }

        Ok(())
    }

    fn bump_tip(&mut self) -> Result<LogOffset> {
        let lid = self.tip;

        let truncations = self.async_truncations.split_off(&lid);

        for (_at, truncation) in truncations {
            match truncation.wait().unwrap() {
                Ok(()) => {}
                Err(error) => {
                    error!("failed to shrink file: {:?}", error);
                    return Err(error);
                }
            }
        }

        self.tip += self.config.segment_size as LogOffset;

        trace!("advancing file tip from {} to {}", lid, self.tip);

        Ok(lid)
    }

    /// Returns the next offset to write a new segment in.
    pub(super) fn next(&mut self, lsn: Lsn) -> Result<LogOffset> {
        let _measure = Measure::new(&M.accountant_next);

        assert_eq!(
            lsn % self.config.segment_size as Lsn,
            0,
            "unaligned Lsn provided to next!"
        );

        trace!("evaluating free list {:?} in SA::next", &self.free);

        // pop free or add to end
        let safe = self.free.iter().next().copied();

        let lid = if let Some(next) = safe {
            self.free.remove(&next);
            next
        } else {
            self.bump_tip()?
        };

        // pin lsn to this segment
        let idx = self.segment_id(lid);

        self.segments[idx].free_to_active(lsn);

        self.ordering.insert(lsn, lid);

        debug!(
            "segment accountant returning offset: {} for lsn {} \
             on deck: {:?}",
            lid, lsn, self.free,
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
    ) -> BTreeMap<Lsn, LogOffset> {
        assert!(
            !self.ordering.is_empty(),
            "expected ordering to have been initialized already"
        );

        let normalized_lsn = self.config.normalize(lsn);

        trace!(
            "generated iterator over {:?} where lsn >= {}",
            self.ordering,
            normalized_lsn
        );

        self.ordering
            .iter()
            .filter_map(move |(l, r)| {
                if *l >= normalized_lsn {
                    Some((*l, *r))
                } else {
                    None
                }
            })
            .collect()
    }

    // truncate the file to the desired length
    fn truncate(&mut self, at: LogOffset) -> Result<()> {
        trace!("asynchronously truncating file to length {}", at);

        assert_eq!(
            at % self.config.segment_size as LogOffset,
            0,
            "new length must be io-buf-len aligned"
        );

        self.tip = at;

        assert!(!self.free.contains(&at), "double-free of a segment occurred");

        let (completer, promise) = OneShot::pair();

        let config = self.config.clone();

        io_fail!(&config, "file truncation");
        threadpool::spawn(move || {
            debug!("truncating file to length {}", at);
            let res = config
                .file
                .set_len(at)
                .and_then(|_| config.file.sync_all())
                .map_err(|e| e.into());
            completer.fill(res);
        })?;

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

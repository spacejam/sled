use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fs::File;
use std::sync::{Arc, Mutex};

use coco::epoch::{Owned, pin};

use super::*;

#[derive(Default, Debug)]
pub struct SegmentAccountant {
    // static or one-time set
    config: Config,
    recovered_lsn: Lsn,
    recovered_lid: LogID,

    // can be sharded
    segments: Vec<Segment>,
    pending_clean: HashSet<PageID>,

    // can be behing a mutex, rarely touched
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
                // println!("pushing free {} to free list in initialization", segment_start);

                if logical_tail.contains(&segment_start) {
                    // we depend on the invariant that the last segments always link together,
                    // so that we can detect torn segments during recovery.
                    self.ensure_safe_free_distance();
                }

                // println!("pushing segment {} to free from initialize", segment_start);
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

    /// Scan the log file if we don't know of any
    /// Lsn offsets yet, and recover the order of
    /// segments, and the highest Lsn.
    fn scan_segment_lsns(&mut self) {
        if self.is_recovered() {
            return;
        }

        let segment_len = self.config.get_io_buf_size() as LogID;
        let mut cursor = 0;

        {
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
                        panic!("detected corrupt segment header at {}", cursor);
                    }
                    cursor += segment_len;
                } else {
                    break;
                }
            }

            // println!("scanned in lsns {:?}", self.ordering);

            self.clean_tail_tears(&mut f);
            // NB implicit drop of f
        }

        // println!("trying to get highest lsn in segment for lsn {}", self.recovered_lsn);
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
            println!("got a segment... lsn: {} read_offset: {}", base_lsn, lid);

            let mut iter = Iter {
                config: &self.config,
                max_lsn: segment_ceiling,
                cur_lsn: tip,
                segment_base: Some(*base_lsn),
                segment_iter: Box::new(vec![].into_iter()),
                segment_len: segment_len as usize,
                trailer: None,
            };

            while let Some((_lsn, lid, _buf)) = iter.next() {
                println!("got a thing...");
                empty_tip = false;
                tip = lid;

                println!("ceiling in recover: {}", segment_ceiling);
                assert!(tip <= segment_ceiling);
            }

            if !empty_tip {
                let cached_f = self.config.cached_file();
                let mut f = cached_f.borrow_mut();
                let (_, _, len) = f.read_entry(tip, segment_len as usize)
                    .unwrap()
                    .flush()
                    .unwrap();
                tip += len as LogID;
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
            // println!("pushing free {} to free list in new", lid);
            self.free.lock().unwrap().push_front(lid);
        }

        println!(
            "after relative thing our recovered_lsn:{}, lid: {}",
            self.recovered_lsn,
            self.recovered_lid
        );
    }

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
                // println!("breaking clean_tail_tears");
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
                    "detected corruption during recovery for segment at {}! expected prev lid: {} actual: {} in last chain {:?}",
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
                // TODO write zeroes to these segments
            }
        }
    }

    pub fn recovered_lid(&self) -> LogID {
        self.recovered_lid
    }

    pub fn recovered_lsn(&self) -> Lsn {
        self.recovered_lsn
    }

    /// this will cause all new allocations to occur at the end of the log, which
    /// is necessary to preserve consistency while concurrently iterating through
    /// the log during snapshot creation.
    pub fn pause_rewriting(&mut self) {
        self.pause_rewriting = true;
    }

    /// this re-enables segment rewriting after they no longer contain fresh data.
    pub fn resume_rewriting(&mut self) {
        self.pause_rewriting = false;
    }

    pub fn freed(&mut self, pid: PageID, old_lids: Vec<LogID>, lsn: Lsn) {
        self.pending_clean.remove(&pid);

        for old_lid in old_lids.into_iter() {
            let idx = old_lid as usize / self.config.get_io_buf_size();

            if self.segments[idx].lsn.unwrap() > lsn {
                // has been replaced after this call already,
                // quite a big race happened.
                // FIXME this is an unsafe leak when operating under zero-copy mode
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
                // println!("pushing free {} to free list in freed", segment_start);
                self.ensure_safe_free_distance();

                // println!("pushing to free from freed: {}", segment_start);
                pin(|scope| {
                    let pd = Owned::new(
                        SegmentDropper(segment_start, self.free.clone()),
                    );
                    let ptr = pd.into_ptr(scope);
                    unsafe {
                        scope.defer_drop(ptr);
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
                // println!("pushing free {} to free list from set", segment_start);
                self.ensure_safe_free_distance();

                // println!("pushing to free from set: {}", segment_start);
                pin(|scope| {
                    let pd = Owned::new(
                        SegmentDropper(segment_start, self.free.clone()),
                    );
                    let ptr = pd.into_ptr(scope);
                    unsafe {
                        scope.defer_drop(ptr);
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

        //assert_ne!(segment.lsn, 0);

        segment.pids.insert(pid);
    }

    fn bump_tip(&mut self) -> LogID {
        let lid = self.tip;
        self.tip += self.config.get_io_buf_size() as LogID;
        // println!("SA advancing tip from {} to {}", lid, self.tip);
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

    pub fn clean(&mut self) -> Option<PageID> {
        if self.free.lock().unwrap().len() >
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

    pub fn segment_snapshot_iter_from(
        &self,
        lsn: Lsn,
    ) -> Box<Iterator<Item = (Lsn, LogID)>> {
        let segment_len = self.config.get_io_buf_size() as Lsn;
        let normalized_lsn = lsn / segment_len * segment_len;
        // println!("ordering >= {}: {:?}", lsn, self.ordering);
        Box::new(self.ordering.clone().into_iter().filter(move |&(l, _)| {
            l >= normalized_lsn
        }))
    }

    pub fn is_recovered(&self) -> bool {
        !self.segments.is_empty()
    }

    pub fn recover(&mut self, lsn: Lsn, lid: LogID) {
        // println!("recovered lsn {} at lid {}", lsn, lid);
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

    // assert that sets for the same pid don't yield anything to clean yet
    sa.set(0, vec![first], first, lsn());
    assert_eq!(sa.clean(), None);
    sa.set(0, vec![first], first, lsn());
    assert_eq!(sa.clean(), None);
    sa.set(0, vec![first], first, lsn());
    assert_eq!(sa.clean(), None);
    sa.set(0, vec![first], first, lsn());
    assert_eq!(sa.clean(), None);

    // assert that when we roll over to the next log, we can immediately reuse first
    let _fourth = sa.next(lsn()).0;
    sa.set(0, vec![first], second, lsn());
    assert_eq!(sa.clean(), None);
    sa.merged(1, second, lsn());
    sa.merged(2, second, lsn());
    sa.merged(3, second, lsn());
    sa.merged(4, second, lsn());
    sa.merged(5, second, lsn());

    // now move a page from second to third, and assert pid 1 can be cleaned
    sa.set(0, vec![second], third, lsn());
    sa.set(2, vec![second], third, lsn());
    sa.set(3, vec![second], third, lsn());
    sa.set(4, vec![second], third, lsn());
    sa.set(5, vec![second], third, lsn());
    assert_eq!(sa.clean(), Some(1));
    assert_eq!(sa.clean(), None);
}

use std::collections::{BTreeMap, HashSet, VecDeque};
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

        let mut cursor = 0;
        loop {
            // in the future this can be optimized to just read
            // the initial header at that position... but we need to
            // make sure the segment is not torn
            if let Ok(segment) = self.config.read_segment(cursor) {
                println!("YEP");
                self.recover(segment.lsn, segment.position);
                cursor += self.config.get_io_buf_size() as LogID;

                // NB we set tip AFTER bumping cursor, as we want this to
                // eventually extend 1 segment width above the highest valid
                // (partial or complete) segment's base.
                self.tip = cursor;

                if segment.lsn > self.recovered_lsn {
                    self.recovered_lsn = segment.lsn;
                }
            } else {
                println!("NOPE");
                break;
            }
        }

        println!("scanned in lsns {:?}", self.ordering);

        self.clean_tail_tears();

        println!("trying to get highest lsn in segment for lsn {}", self.recovered_lsn);
        if let Some(max_cursor) = self.ordering.get(&self.recovered_lsn) {
            let mut empty_tip = true;
            if let Ok(mut segment) = self.config.read_segment(*max_cursor) {
                println!(
                    "got a segment... lsn: {} read_offset: {}",
                    segment.lsn,
                    segment.read_offset
                );
                self.recovered_lsn += segment.read_offset as LogID;
                while let Some(log_read) = segment.read_next() {
                    println!("got a thing...");
                    match log_read {
                        LogRead::Zeroed(_len) => {
                            // println!("got a zeroed of len {}", _len);
                            continue;
                        }
                        LogRead::Flush(lsn, _, len) => {
                            println!("got lsn {} with len {}", lsn, len);
                            empty_tip = false;
                            let tip = lsn + MSG_HEADER_LEN as Lsn + len as Lsn;
                            if tip > self.recovered_lsn {
                                self.recovered_lsn = tip;
                            } else {
                                break;
                            }
                        }
                        LogRead::Corrupted(_) => break,
                    }
                }
                let segment_overhang = self.recovered_lsn % self.config.get_io_buf_size() as LogID;
                self.recovered_lid = segment.position + segment_overhang;
            }
            if empty_tip {
                println!("pushing free {} to free list in new", *max_cursor);
                // TODO properly set last_given
                self.free.lock().unwrap().push_back(*max_cursor);
            } else {
                println!("tip NOT empty!!!");
            }
        } else {
            assert!(self.ordering.is_empty());
        }

        println!(
            "after relative thing our recovered_lsn:{}, lid: {}",
            self.recovered_lsn,
            self.recovered_lid
        );
    }

    fn clean_tail_tears(&mut self) {
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
                println!("breaking clean_tail_tears");
                break;
            }

            // check link
            let segment_iter = self.config.read_segment(lid).unwrap();
            let expected_prev = segment_iter.prev;
            let actual_prev = logical_tail[i + 1].1;

            if expected_prev != actual_prev {
                // detected a tear, everything after
                #[cfg(feature = "log")]
                error!("detected corruption during recovery!");
                tear_at = Some(i);
            }
        }

        if let Some(i) = tear_at {
            // we need to chop off the elements after the tear
            for j in 0..i {
                let (lsn_to_chop, lid_to_chop) = logical_tail[j];
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

                pin(|scope| {
                    let pd = Owned::new(SegmentDropper(segment_start, self.free.clone()));
                    let ptr = pd.into_ptr(scope);
                    unsafe {
                        scope.defer_drop(ptr);
                    }
                });
            } else if self.segments[idx].pids.len() as f64 / self.segments[idx].pids_len as f64 <=
                       self.config.get_segment_cleanup_threshold()
            {
                // can be cleaned
                self.to_clean.insert(segment_start);
            }
        }
    }

    pub fn set(&mut self, pid: PageID, old_lids: Vec<LogID>, new_lid: LogID, lsn: Lsn) {
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

                pin(|scope| {
                    let pd = Owned::new(SegmentDropper(segment_start, self.free.clone()));
                    let ptr = pd.into_ptr(scope);
                    unsafe {
                        scope.defer_drop(ptr);
                    }
                });
            } else if self.segments[idx].pids.len() as f64 / self.segments[idx].pids_len as f64 <=
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

        #[cfg(feature = "log")]
        debug!("segment accountant returning offset {} last {}", lid, last_given);

        self.last_given = lid;

        (lid, last_given)
    }

    pub fn clean(&mut self) -> Option<PageID> {
        if self.free.lock().unwrap().len() > self.config.get_min_free_segments() ||
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

    pub fn segment_snapshot_iter_from(&self, lsn: Lsn) -> Box<Iterator<Item = (Lsn, LogID)>> {
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
        let idx = lid as usize / self.config.get_io_buf_size();

        if self.segments.len() <= idx {
            self.segments.resize(idx + 1, Segment::default());
        }

        self.segments[idx].lsn = Some(lsn);
        self.ordering.insert(lsn, lid);
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

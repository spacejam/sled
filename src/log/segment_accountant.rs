use std::collections::{BTreeMap, HashSet, VecDeque};

use super::*;

#[derive(Default, Debug)]
pub struct SegmentAccountant {
    tip: LogID,
    segments: Vec<Segment>,
    to_clean: HashSet<LogID>,
    pending_clean: HashSet<PageID>,
    free: VecDeque<LogID>,
    config: Config,
    pause_rewriting: bool,
    ordering: BTreeMap<Lsn, LogID>,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pids: HashSet<PageID>,
    pids_len: usize,
    lsn: Option<Lsn>,
    freed: bool,
}

// concurrency properties:
//  the pagecache will ask this (maybe through the log) about what it should rewrite
//
//  open questions:
//  * how do we avoid contention
//      * pending zone?
//  * how do we maximize rewrite effectiveness?
//      * tell them to rewrite pages present in a large number of segments?
//      * tell them to rewrite small pages?
//      * just focus on lowest one
//  * how often do we rewrite pages?
//      * if we read a page that is in a segment that is being cleaned!
//
//
//  pagecache: set
//      we replaced this page, used to be at these spots, now it's here, the set's lsn is _
//
//  pagecache: merge
//      we added a frag for this page at this log, with this lsn
//
//  log: write_to_log
//      we need the next log offset, which gets this lsn

impl SegmentAccountant {
    pub fn new(config: Config, tip: LogID) -> SegmentAccountant {
        let mut ret = SegmentAccountant::default();
        ret.config = config;
        ret.tip = tip;
        ret
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

            let segment = &mut self.segments[idx];

            if segment.lsn.unwrap() > lsn {
                // has been replaced after this call already,
                // quite a big race happened
                continue;
            }

            if segment.pids_len == 0 {
                segment.pids_len = segment.pids.len();
            }

            segment.pids.remove(&pid);

            let segment_start = (idx * self.config.get_io_buf_size()) as LogID;

            if segment.pids.is_empty() && !segment.freed {
                // can be reused immediately
                segment.freed = true;
                self.to_clean.remove(&segment_start);
                self.free.push_back(segment_start);
            } else if segment.pids.len() as f64 / segment.pids_len as f64 <=
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

            let segment = &mut self.segments[idx];

            if segment.lsn.unwrap() > lsn {
                // has been replaced after this call already,
                // quite a big race happened
                continue;
            }

            if segment.pids_len == 0 {
                segment.pids_len = segment.pids.len();
            }

            segment.pids.remove(&pid);

            let segment_start = (idx * self.config.get_io_buf_size()) as LogID;

            if segment.pids.is_empty() && !segment.freed {
                // can be reused immediately
                segment.freed = true;
                self.to_clean.remove(&segment_start);
                self.free.push_back(segment_start);
            } else if segment.pids.len() as f64 / segment.pids_len as f64 <=
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

        if segment.lsn.unwrap() > lsn {
            // a race happened, and our Lsn does not apply anymore
            return;
        }

        //assert_ne!(segment.lsn, 0);

        segment.pids.insert(pid);
    }

    pub fn next(&mut self, lsn: Lsn) -> LogID {
        // pop free or add to end
        let lid = if self.pause_rewriting {
            None
        } else {
            self.free.pop_front()
        }.unwrap_or_else(|| {
            let lid = self.tip;
            println!("tip: {} + {}", self.tip, self.config.get_io_buf_size());
            self.tip += self.config.get_io_buf_size() as LogID;
            lid
        });

        // pin lsn to this segment
        let idx = lid as usize / self.config.get_io_buf_size();

        if self.segments.len() <= idx {
            self.segments.resize(idx + 1, Segment::default());
        }

        let segment = &mut self.segments[idx];
        assert!(segment.pids.is_empty());

        if let Some(old_lsn) = segment.lsn {
            self.ordering.remove(&old_lsn);
        }

        segment.lsn = Some(lsn);
        segment.freed = false;
        segment.pids_len = 0;

        self.ordering.insert(lsn, lid);

        println!("returning lid: {}", lid);
        lid
    }

    pub fn clean(&mut self) -> Option<PageID> {
        if self.free.len() > self.config.get_min_free_segments() || self.to_clean.is_empty() {
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
        Box::new(self.ordering.clone().into_iter().filter(
            move |&(l, _)| l >= lsn,
        ))
    }
}

#[test]
fn basic_workflow() {
    // empty clean is None
    let conf = Config::default()
        .io_buf_size(1000)
        .segment_cleanup_threshold(0.2)
        .min_free_segments(3);
    let mut sa = SegmentAccountant::new(conf, 0);

    let mut highest = 0;
    let mut lsn = || {
        highest += 1;
        highest
    };

    let first = sa.next(lsn());
    let second = sa.next(lsn());
    let third = sa.next(lsn());

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
    let _fourth = sa.next(lsn());
    sa.set(0, vec![first], second, lsn());
    assert_eq!(sa.clean(), None);
    let fifth = sa.next(lsn());
    assert_eq!(fifth, first);
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

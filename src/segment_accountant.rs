use std::collections::{BTreeMap, VecDequeue};

use super::*;

pub(super) struct SegmentAccountant {
    tip: LogID,
    segments: Vec<Segment>,
    to_clean: VecDequeue<LogID>,
    free: VecDequeue<LogID>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
struct Segment {
    pids: Vec<PageID>,
    original_pid_len: usize,
    pad: usize,
    lsn: Lsn,
}

impl SegmentAccountant {
    pub fn wrote(&mut pids: Vec<PageID>, to: LogID, lsn: Lsn, pad: usize) {
        let segment = Segment {
            original_pid_len: pids.len(),
            pids: pids,
            pad: pad,
            lsn: lsn,
        };

        // extend segments if too short
    }

    pub fn cleaned(&mut self, pid: PageID, lids: Vec<LogID>, lsn: Lsn) {
        // for each lid, look up the segment, and if the segment's lsn is lower,
        // clear out pid, otherwise the pid was concurrently rewritten...

        // TODO think about correctness when that's the case...
    }

    pub fn next(&mut self) -> LogID {
        // try to pop the freelist

        //
    }
}

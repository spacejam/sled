//! The `EventLog` lets us cheaply record and query behavior
//! in a concurrent system. It lets us reconstruct stories about
//! what happened to our data. It lets us write tests like:
//! 1. no keys are lost through tree structural modifications
//! 2. no nodes are made inaccessible through structural modifications
//! 3. no segments are zeroed and reused before all resident
//!    pages have been relocated and stabilized.
//! 4. recovery does not skip active segments
//! 5. no page is double-allocated or double-freed
//! 6. pages before restart match pages after restart
//!
//! What does it mean for data to be accessible?
//! 1. key -> page
//! 2. page -> lid
//! 3. lid ranges get stabiized over time
//! 4. lid ranges get zeroed over time
//! 5. segment trailers get written over time
//! 6. if a page's old location is zeroed before
//!    `io_bufs` segment trailers have been written,
//!    we are vulnerable to data loss
//! 3. segments have lifespans from fsync to zero
//! 4.
#![allow(missing_docs)]

use std::collections::HashMap;

use super::*;

/// A lock-free queue of Events.
#[derive(Default, Debug)]
pub struct EventLog {
    inner: Stack<Event>,
}

impl EventLog {
    fn iter<'a>(&self, guard: &'a Guard) -> StackIter<'a, Event> {
        let head = self.inner.head(guard);
        StackIter::from_ptr(head, guard)
    }

    fn verify(&self) {
        let guard = pin();
        let iter = self.iter(&guard);

        // if we encounter a `PagesAfterRestart`, then we should
        // compare it to any subsequent `PagesBeforeRestart`

        let mut par = None;

        for event in iter {
            match event {
                Event::PagesAfterRestart { pages } => {
                    par = Some(pages.clone());
                }
                Event::PagesBeforeRestart { pages } => {
                    if let Some(ref par) = par {
                        assert_eq!(par, pages);
                    }
                }
                _ => {}
            }
        }
    }

    pub fn pages_before_restart(
        &self,
        pages: HashMap<PageId, Vec<DiskPtr>>,
    ) {
        self.inner.push(Event::PagesBeforeRestart { pages });
    }

    pub fn pages_after_restart(
        &self,
        pages: HashMap<PageId, Vec<DiskPtr>>,
    ) {
        self.inner.push(Event::PagesAfterRestart { pages });
    }

    /*

    fn segments_before_restart(&self, snapshot: &Snapshot) {}

    fn segments_after_restart(&self, snapshot: &Snapshot) {}

    fn key_to_page(&self, Lsn, key: &[u8], page: PageId) {}

    fn page_to_segment(
        &self,
        lsn: Lsn,
        page: PageId,
        segment: LogId,
    ) {
    }

    fn segment_zeroed(&self, lsn: Lsn, segment: LogId) {}
    */
}

impl Drop for EventLog {
    fn drop(&mut self) {
        self.verify();
        panic!("dropping event log");
    }
}

/// A thing that happens at a certain time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    SegmentAllocate {
        lsn: Lsn,
        lid: LogId,
    },
    SegmentFree {
        lsn: Lsn,
        lid: LogId,
    },
    SegmentZero {
        lsn: Lsn,
        lid: LogId,
    },
    Replace {
        pid: PageId,
        lsn: Lsn,
        lid: LogId,
        old_lids: Vec<LogId>,
    },
    Link {
        pid: PageId,
        lsn: Lsn,
        lid: LogId,
    },
    SegmentsBeforeRestart {
        segments: HashMap<LogId, Lsn>,
    },
    SegmentsAfterRestart {
        segments: HashMap<LogId, Lsn>,
    },
    PagesBeforeRestart {
        pages: HashMap<PageId, Vec<DiskPtr>>,
    },
    PagesAfterRestart {
        pages: HashMap<PageId, Vec<DiskPtr>>,
    },
    TreeRootBeforeRestart {
        pid: PageId,
    },
    TreeRootAfterRestart {
        pid: PageId,
    },
}

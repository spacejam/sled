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

use std::collections::{BTreeSet, HashMap};

use super::*;

/// A thing that happens at a certain time.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum Event {
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
        segments: HashMap<Lsn, (segment::SegmentState, u8, LogId)>,
    },
    SegmentsAfterRestart {
        segments: HashMap<Lsn, (segment::SegmentState, u8, LogId)>,
    },
    PagesBeforeRestart {
        pages: HashMap<PageId, Vec<DiskPtr>>,
    },
    PagesAfterRestart {
        pages: HashMap<PageId, Vec<DiskPtr>>,
    },
    MetaBeforeRestart {
        meta: Meta,
    },
    MetaAfterRestart {
        meta: Meta,
    },
}

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

        let mut recovered_pages = None;
        let mut recovered_segments = None;
        let mut recovered_meta = None;

        for event in iter {
            match event {
                Event::PagesAfterRestart { pages } => {
                    recovered_pages = Some(pages.clone());
                }
                Event::PagesBeforeRestart { pages } => {
                    if let Some(ref par) = recovered_pages {
                        assert_eq!(pages, par);
                    }
                }
                Event::SegmentsAfterRestart { segments } => {
                    recovered_segments = Some(segments.clone());
                }
                Event::SegmentsBeforeRestart { segments } => {
                    if let Some(ref segs) = recovered_segments {
                        if segments != segs {
                            let before: BTreeSet<_> = segments
                                .iter()
                                .map(|(lsn, (state, live, lid))| {
                                    (*lsn, (*state, *live, *lid))
                                })
                                .collect();
                            let after: BTreeSet<(
                                Lsn,
                                (segment::SegmentState, u8, LogId),
                            )> = segs
                                .iter()
                                .map(|(lsn, (state, live, lid))| {
                                    (*lsn, (*state, *live, *lid))
                                })
                                .collect();

                            let only_before: Vec<_> =
                                before.difference(&after).collect();
                            let only_after: Vec<_> =
                                after.difference(&before).collect();

                            panic!(
                                "segments failed to recover properly. \n \
                                 before restart: \n{:?} \n \
                                 after restart: \n{:?}",
                                only_before, only_after
                            );
                        }
                    }
                }
                Event::MetaAfterRestart { meta } => {
                    recovered_meta = Some(meta);
                }
                Event::MetaBeforeRestart { meta } => {
                    if let Some(ref rec_meta) = recovered_meta {
                        assert_eq!(meta, *rec_meta);
                    }
                }
                _ => {}
            }
        }
    }

    pub(crate) fn pages_before_restart(
        &self,
        pages: HashMap<PageId, Vec<DiskPtr>>,
    ) {
        self.inner.push(Event::PagesBeforeRestart { pages });
    }

    pub(crate) fn pages_after_restart(
        &self,
        pages: HashMap<PageId, Vec<DiskPtr>>,
    ) {
        self.inner.push(Event::PagesAfterRestart { pages });
    }

    pub(crate) fn segments_before_restart(
        &self,
        segments: HashMap<Lsn, (segment::SegmentState, u8, LogId)>,
    ) {
        self.inner.push(Event::SegmentsBeforeRestart { segments });
    }

    pub(crate) fn segments_after_restart(
        &self,
        segments: HashMap<Lsn, (segment::SegmentState, u8, LogId)>,
    ) {
        self.inner.push(Event::SegmentsAfterRestart { segments });
    }

    pub fn meta_before_restart(&self, meta: Meta) {
        self.inner.push(Event::MetaBeforeRestart { meta });
    }

    pub fn meta_after_restart(&self, meta: Meta) {
        self.inner.push(Event::MetaAfterRestart { meta });
    }
}

impl Drop for EventLog {
    fn drop(&mut self) {
        self.verify();
    }
}

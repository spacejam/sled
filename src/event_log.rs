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

use crate::pagecache::DiskPtr;
use crate::*;

use crate::stack::{Iter as StackIter, Stack};

/// A thing that happens at a certain time.
#[derive(Debug, Clone)]
enum Event {
    PagesOnShutdown { pages: HashMap<PageId, Vec<DiskPtr>> },
    PagesOnRecovery { pages: HashMap<PageId, Vec<DiskPtr>> },
    MetaOnShutdown { meta: Meta },
    MetaOnRecovery { meta: Meta },
    RecoveredLsn(Lsn),
}

/// A lock-free queue of Events.
#[derive(Default, Debug)]
pub struct EventLog {
    inner: Stack<Event>,
}

impl EventLog {
    pub(crate) fn reset(&self) {
        self.verify();
        let guard = pin();
        while let Some(_) = self.inner.pop(&guard) {}
    }

    fn iter<'a>(&self, guard: &'a Guard) -> StackIter<'a, Event> {
        let head = self.inner.head(guard);
        StackIter::from_ptr(head, guard)
    }

    pub(crate) fn verify(&self) {
        let guard = pin();
        let iter = self.iter(&guard);

        // if we encounter a `PagesOnRecovery`, then we should
        // compare it to any subsequent `PagesOnShutdown`

        let mut recovered_pages = None;
        let mut recovered_meta = None;
        let mut recovered_lsn = None;

        for event in iter {
            match event {
                Event::RecoveredLsn(lsn) => {
                    if let Some(later_lsn) = recovered_lsn {
                        assert!(
                            later_lsn >= lsn,
                            "lsn must never go down between recoveries"
                        );
                    }
                    recovered_lsn = Some(lsn);
                }
                Event::PagesOnRecovery { pages } => {
                    recovered_pages = Some(pages.clone());
                }
                Event::PagesOnShutdown { pages } => {
                    if let Some(ref par) = recovered_pages {
                        let pids = par
                            .iter()
                            .map(|(pid, _frag_locations)| *pid)
                            .chain(
                                pages.iter().map(|(pid, _frag_locations)| *pid),
                            )
                            .collect::<std::collections::HashSet<_>>()
                            .into_iter();

                        for pid in pids {
                            let locations_before_restart = pages.get(&pid);
                            let locations_after_restart = par.get(&pid);
                            assert_eq!(
                                locations_before_restart,
                                locations_after_restart,
                                "page {} had frag locations {:?} before \
                                 restart, but {:?} after restart",
                                pid,
                                locations_before_restart,
                                locations_after_restart
                            );
                        }

                        assert_eq!(pages, par);
                    }
                }
                Event::MetaOnRecovery { meta } => {
                    recovered_meta = Some(meta);
                }
                Event::MetaOnShutdown { meta } => {
                    if let Some(rec_meta) = recovered_meta {
                        assert_eq!(meta, rec_meta);
                    }
                }
            }
        }
    }

    pub(crate) fn recovered_lsn(&self, lsn: Lsn) {
        let guard = pin();
        self.inner.push(Event::RecoveredLsn(lsn), &guard);
    }

    pub(crate) fn pages_before_restart(
        &self,
        pages: HashMap<PageId, Vec<DiskPtr>>,
    ) {
        let guard = pin();
        self.inner.push(Event::PagesOnShutdown { pages }, &guard);
    }

    pub(crate) fn pages_after_restart(
        &self,
        pages: HashMap<PageId, Vec<DiskPtr>>,
    ) {
        let guard = pin();
        self.inner.push(Event::PagesOnRecovery { pages }, &guard);
    }

    pub fn meta_before_restart(&self, meta: Meta) {
        let guard = pin();
        self.inner.push(Event::MetaOnShutdown { meta }, &guard);
    }

    pub fn meta_after_restart(&self, meta: Meta) {
        let guard = pin();
        self.inner.push(Event::MetaOnRecovery { meta }, &guard);
    }
}

impl Drop for EventLog {
    fn drop(&mut self) {
        self.verify();
    }
}

use std::collections::BTreeMap;
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use bincode::{Infinite, deserialize, serialize};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::*;

mod page_cache;
mod snapshot;

pub use self::page_cache::PageCache;
pub use self::snapshot::Snapshot;

/// A user of a `PageCache` needs to provide a `Materializer` which
/// handles the merging of page fragments.
pub trait Materializer {
    /// The possibly fragmented page, written to log storage sequentially, and
    /// read in parallel from multiple locations on disk when serving
    /// a request to read the page. These will be merged to a single version
    /// at read time, and possibly cached.
    type PageFrag;

    /// The state returned by a call to `PageCache::recover`, as
    /// described by `Materializer::recover`
    type Recovery;

    /// Used to compress long chains of partial pages into a condensed form
    /// during compaction.
    fn merge(&self, &[&Self::PageFrag]) -> Self::PageFrag;

    /// Used to feed custom recovery information back to a higher-level abstraction
    /// during startup. For example, a B-Link tree must know what the current
    /// root node is before it can start serving requests.
    fn recover(&self, &Self::PageFrag) -> Option<Self::Recovery>;
}

/// Points to either a memory location or a disk location to page-in data from.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheEntry<M: Send + Sync> {
    /// A cache item that contains the most recent fully-merged page state, also in secondary
    /// storage.
    MergedResident(M, Lsn, LogID),
    /// A cache item that is in memory, and also in secondary storage.
    Resident(M, Lsn, LogID),
    /// A cache item that is present in secondary storage.
    PartialFlush(Lsn, LogID),
    /// A cache item that is present in secondary storage, and is the base segment
    /// of a page.
    Flush(Lsn, LogID),
}

/// `LoggedUpdate` is for writing blocks of `Update`'s to disk
/// sequentially, to reduce IO during page reads.
#[serde(bound(deserialize = ""))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct LoggedUpdate<PageFrag>
    where PageFrag: Serialize + DeserializeOwned
{
    pid: PageID,
    update: Update<PageFrag>,
}

#[serde(bound(deserialize = ""))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Update<PageFrag>
    where PageFrag: DeserializeOwned + Serialize
{
    Append(PageFrag),
    Compact(PageFrag),
    Free,
    Alloc,
}

struct PidDropper(PageID, Arc<Stack<PageID>>);

impl Drop for PidDropper {
    fn drop(&mut self) {
        self.1.push(self.0);
    }
}

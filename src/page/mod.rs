/// A lock-free pagecache which supports fragmented pages for dramatically
/// improving write throughput. Reads need to scatter-gather, so this is
/// built with the assumption that it will be run on something like an
/// SSD that can efficiently handle random reads.
use std::fmt::{self, Debug};
use std::ptr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use bincode::{Infinite, deserialize, serialize};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::*;

mod page_cache;

pub use self::page_cache::PageCache;

/// A tenant of a `PageCache` needs to provide a `Materializer` which
/// handles the processing of pages.
pub trait Materializer: Send + Sync + Clone {
    /// The "complete" page, returned to clients who want to retrieve the
    /// logical page state.
    type MaterializedPage;

    /// The "partial" page, written to log storage sequentially, and
    /// read in parallel from multiple locations on disk when serving
    /// a request to read the page.
    type PartialPage: Serialize + DeserializeOwned + Clone + Debug + PartialEq;

    /// The state returned by a call to `PageCache::recover`, as
    /// described by `Materializer::recover`
    type Recovery;

    /// Used to generate the result of `get` requests on the `PageCache`
    fn materialize(&self, &Vec<Self::PartialPage>) -> Self::MaterializedPage;

    /// Used to compress long chains of partial pages into a condensed form
    /// during compaction.
    fn consolidate(&self, &Vec<Self::PartialPage>) -> Vec<Self::PartialPage>;

    /// Used to feed custom recovery information back to a higher-level abstraction
    /// during startup. For example, a B-Link tree must know what the current
    /// root node is before it can start serving requests.
    fn recover(&mut self, &Self::PartialPage) -> Option<Self::Recovery>;
}

/// Points to either a memory location or a disk location to page-in data from.
#[derive(Clone, PartialEq)]
pub enum CacheEntry<M>
    where M: Materializer
{
    /// A cache item that is in memory, and maybe also in secondary storage.
    Resident(Vec<M::PartialPage>, Option<LogID>),
    /// A cache item that is present in secondary storage.
    PartialFlush(LogID),
    /// A cache item that is present in secondary storage, and is the base segment
    /// of a page.
    Flush(LogID),
}

unsafe impl<M: Materializer> Send for CacheEntry<M> {}

/// `LoggedUpdate` is for writing blocks of `Update`'s to disk
/// sequentially, to reduce IO during page reads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct LoggedUpdate<M>
    where M: Materializer
{
    pid: PageID,
    update: Update<M>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Update<M>
    where M: Materializer
{
    Append(Vec<M::PartialPage>),
    Compact(Vec<M::PartialPage>),
    Del,
    Alloc,
}

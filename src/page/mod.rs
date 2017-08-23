/// A lock-free pagecache which supports fragmented pages for dramatically
/// improving write throughput. Reads need to scatter-gather, so this is
/// built with the assumption that it will be run on something like an
/// SSD that can efficiently handle random reads.
use std::collections::BTreeMap;
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use bincode::{Infinite, deserialize, serialize};
use serde::de::DeserializeOwned;
use serde::Serialize;
use coco::epoch::Ptr;

use super::*;

mod page_cache;
mod hash;
mod lru;

pub use self::page_cache::PageCache;

/// A tenant of a `PageCache` needs to provide a `Materializer` which
/// handles the processing of pages.
pub trait Materializer {
    /// The "complete" page, returned to clients who want to retrieve the
    /// logical page state.
    type MaterializedPage;

    /// The "partial" page, written to log storage sequentially, and
    /// read in parallel from multiple locations on disk when serving
    /// a request to read the page.
    type PartialPage;

    /// The state returned by a call to `PageCache::recover`, as
    /// described by `Materializer::recover`
    type Recovery;

    /// Used to generate the result of `get` requests on the `PageCache`
    fn materialize(&self, &[Self::PartialPage]) -> Self::MaterializedPage;

    /// Used to compress long chains of partial pages into a condensed form
    /// during compaction.
    fn consolidate(&self, &[Self::PartialPage]) -> Vec<Self::PartialPage>;

    /// Used to feed custom recovery information back to a higher-level abstraction
    /// during startup. For example, a B-Link tree must know what the current
    /// root node is before it can start serving requests.
    fn recover(&self, &Self::PartialPage) -> Option<Self::Recovery>;
}

/// Points to either a memory location or a disk location to page-in data from.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheEntry<M> {
    /// A cache item that is in memory, and also in secondary storage.
    Resident(Vec<M>, LogID),
    /// A cache item that is present in secondary storage.
    PartialFlush(LogID),
    /// A cache item that is present in secondary storage, and is the base segment
    /// of a page.
    Flush(LogID),
}

unsafe impl<M: Send> Send for CacheEntry<M> {}

#[derive(Debug, Clone)]
pub struct CasKey<P> {
    ptr: *const stack::Node<CacheEntry<P>>,
    tag: usize,
}

impl<'s, P> From<Ptr<'s, stack::Node<CacheEntry<P>>>> for CasKey<P> {
    fn from(ptr: Ptr<'s, stack::Node<CacheEntry<P>>>) -> CasKey<P> {
        CasKey {
            ptr: ptr.as_raw(),
            tag: ptr.tag(),
        }
    }
}

impl<'s, P> Into<Ptr<'s, stack::Node<CacheEntry<P>>>> for CasKey<P> {
    fn into(self) -> Ptr<'s, stack::Node<CacheEntry<P>>> {
        unsafe { Ptr::from_raw(self.ptr).with_tag(self.tag) }
    }
}

/// `LoggedUpdate` is for writing blocks of `Update`'s to disk
/// sequentially, to reduce IO during page reads.
#[serde(bound(deserialize = ""))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct LoggedUpdate<PartialPage>
    where PartialPage: Serialize + DeserializeOwned
{
    pid: PageID,
    update: Update<PartialPage>,
}

#[serde(bound(deserialize = ""))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Update<PartialPage>
    where PartialPage: DeserializeOwned + Serialize
{
    Append(Vec<PartialPage>),
    Compact(Vec<PartialPage>),
    Del,
    Alloc,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Snapshot<R> {
    pub max_lid: LogID,
    pub max_pid: PageID,
    pub pt: BTreeMap<PageID, Vec<LogID>>,
    pub free: Vec<PageID>,
    pub recovery: Option<R>,
}

impl<R> Default for Snapshot<R> {
    fn default() -> Snapshot<R> {
        Snapshot {
            max_lid: 0,
            max_pid: 0,
            pt: BTreeMap::new(),
            free: vec![],
            recovery: None,
        }
    }
}

struct PidDropper(PageID, Arc<Stack<PageID>>);

impl Drop for PidDropper {
    fn drop(&mut self) {
        self.1.push(self.0);
    }
}

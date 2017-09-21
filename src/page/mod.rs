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

pub use self::page_cache::PageCache;

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
    MergedResident(M, LogID),
    /// A cache item that is in memory, and also in secondary storage.
    Resident(M, LogID),
    /// A cache item that is present in secondary storage.
    PartialFlush(LogID),
    /// A cache item that is present in secondary storage, and is the base segment
    /// of a page.
    Flush(LogID),
}

/// A wrapper struct for a pointer to a (possibly invalid, hence inaccessible)
/// `PageFrag` used for applying updates atomically to shared pages.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CasKey<P>
    where P: 'static + Send + Sync
{
    ptr: *const ds::stack::Node<CacheEntry<P>>,
    tag: usize,
}

impl<'s, P> From<Ptr<'s, ds::stack::Node<CacheEntry<P>>>> for CasKey<P>
    where P: 'static + Send + Sync
{
    fn from(ptr: Ptr<'s, ds::stack::Node<CacheEntry<P>>>) -> CasKey<P> {
        CasKey {
            ptr: ptr.as_raw(),
            tag: ptr.tag(),
        }
    }
}

impl<'s, P> Into<Ptr<'s, ds::stack::Node<CacheEntry<P>>>> for CasKey<P>
    where P: 'static + Send + Sync
{
    fn into(self) -> Ptr<'s, ds::stack::Node<CacheEntry<P>>> {
        unsafe { Ptr::from_raw(self.ptr).with_tag(self.tag) }
    }
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
    Del,
    Alloc,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Snapshot<R> {
    pub max_lsn: Lsn,
    pub max_pid: PageID,
    pub pt: BTreeMap<PageID, Vec<LogID>>,
    pub segments: Vec<log::Segment>,
    pub free: Vec<PageID>,
    pub recovery: Option<R>,
}

impl<R> Default for Snapshot<R> {
    fn default() -> Snapshot<R> {
        Snapshot {
            max_lsn: 0,
            max_pid: 0,
            pt: BTreeMap::new(),
            segments: vec![],
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

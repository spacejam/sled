//! `pagecache` is a lock-free pagecache and log for building high-performance
//! databases.
#![allow(unsafe_code)]

pub mod constants;
pub mod logger;

mod blob_io;
mod disk_pointer;
mod iobuf;
mod iterator;
mod pagetable;
mod parallel_io;
mod reader;
mod reservation;
mod segment;
mod snapshot;

use crate::*;
use std::{
    borrow::Cow,
    collections::BinaryHeap,
    ops::{Deref, DerefMut},
};

use self::{
    blob_io::{gc_blobs, read_blob, remove_blob, write_blob},
    constants::{
        BATCH_MANIFEST_PID, COUNTER_PID, META_PID, PAGE_CONSOLIDATION_THRESHOLD,
    },
    iobuf::{IoBuf, IoBufs},
    iterator::{raw_segment_iter_from, LogIter},
    pagetable::PageTable,
    parallel_io::Pio,
    segment::SegmentAccountant,
    snapshot::advance_snapshot,
};

pub(crate) use self::{
    logger::{MessageHeader, SegmentHeader},
    reader::{read_message, read_segment_header},
    reservation::Reservation,
    snapshot::{read_snapshot_or_default, PageState, Snapshot},
};

pub use self::{
    constants::{
        BATCH_MANIFEST_INLINE_LEN, BLOB_INLINE_LEN, MAX_SPACE_AMPLIFICATION,
        MINIMUM_ITEMS_PER_SEGMENT, MSG_HEADER_LEN, SEG_HEADER_LEN,
    },
    disk_pointer::DiskPtr,
    logger::{Log, LogRead},
    segment::SegmentMode,
};

/// The offset of a segment. This equals its `LogOffset` (or the offset of any
/// item contained inside it) divided by the configured `segment_size`.
pub type SegmentId = usize;

/// A file offset in the database log.
pub type LogOffset = u64;

/// A pointer to an blob blob.
pub type BlobPointer = Lsn;

/// The logical sequence number of an item in the database log.
pub type Lsn = i64;

/// A page identifier.
pub type PageId = u64;

/// A byte used to disambiguate log message types
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum MessageKind {
    /// The EVIL_BYTE is written as a canary to help
    /// detect torn writes.
    Corrupted = 0,
    /// Indicates that the following buffer corresponds
    /// to a reservation for an in-memory operation that
    /// failed to complete. It should be skipped during
    /// recovery.
    Cancelled = 1,
    /// Indicates that the following buffer is used
    /// as padding to fill out the rest of the segment
    /// before sealing it.
    Pad = 2,
    /// Indicates that the following buffer contains
    /// an Lsn for the last write in an atomic writebatch.
    BatchManifest = 3,
    /// Indicates that this page was freed from the pagetable.
    Free = 4,
    /// Indicates that the last persisted ID was at least
    /// this high.
    Counter = 5,
    /// The meta page, stored inline
    InlineMeta = 6,
    /// The meta page, stored blobly
    BlobMeta = 7,
    /// A consolidated page replacement, stored inline
    InlineNode = 10,
    /// A consolidated page replacement, stored blobly
    BlobNode = 11,
    /// A partial page update, stored inline
    InlineLink = 12,
    /// A partial page update, stored blobly
    BlobLink = 13,
}

impl MessageKind {
    pub(crate) const fn into(self) -> u8 {
        self as u8
    }
}

impl From<u8> for MessageKind {
    fn from(byte: u8) -> Self {
        use MessageKind::*;
        match byte {
            0 => Corrupted,
            1 => Cancelled,
            2 => Pad,
            3 => BatchManifest,
            4 => Free,
            5 => Counter,
            6 => InlineMeta,
            7 => BlobMeta,
            8 => InlineNode,
            9 => BlobNode,
            10 => InlineLink,
            11 => BlobLink,
            other => {
                debug!("encountered unexpected message kind byte {}", other);
                Corrupted
            }
        }
    }
}

/// The high-level types of stored information
/// about pages and their mutations
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogKind {
    /// Persisted data containing a page replacement
    Replace,
    /// Persisted immutable update
    Link,
    /// Freeing of a page
    Free,
    /// Some state indicating this should be skipped
    Skip,
    /// Unexpected corruption
    Corrupted,
}

fn log_kind_from_update(update: &Update) -> LogKind {
    use Update::*;

    match update {
        Free => LogKind::Free,
        Link(..) => LogKind::Link,
        Node(..) | Counter(..) | Meta(..) => LogKind::Replace,
    }
}

impl From<MessageKind> for LogKind {
    fn from(kind: MessageKind) -> Self {
        use MessageKind::*;
        match kind {
            Free => LogKind::Free,

            InlineNode | Counter | BlobNode | InlineMeta | BlobMeta => {
                LogKind::Replace
            }

            InlineLink | BlobLink => LogKind::Link,

            Cancelled | Pad | BatchManifest => LogKind::Skip,
            other => {
                debug!("encountered unexpected message kind byte {:?}", other);
                LogKind::Corrupted
            }
        }
    }
}

fn assert_usize<T>(from: T) -> usize
where
    usize: TryFrom<T, Error = std::num::TryFromIntError>,
{
    usize::try_from(from).expect("lost data cast while converting to usize")
}

// TODO remove this when atomic fetch_max stabilizes in #48655
fn bump_atomic_lsn(atomic_lsn: &AtomicLsn, to: Lsn) {
    let mut current = atomic_lsn.load(SeqCst);
    loop {
        if current >= to {
            return;
        }
        let last = atomic_lsn.compare_and_swap(current, to, SeqCst);
        if last == current {
            // we succeeded.
            return;
        }
        current = last;
    }
}

use std::convert::{TryFrom, TryInto};

#[cfg(feature = "compression")]
use zstd::block::decompress;

#[inline]
pub(crate) fn lsn_to_arr(number: Lsn) -> [u8; 8] {
    number.to_le_bytes()
}

#[inline]
pub(crate) fn arr_to_lsn(arr: &[u8]) -> Lsn {
    arr.try_into().map(Lsn::from_le_bytes).unwrap()
}

#[inline]
pub(crate) fn u64_to_arr(number: u64) -> [u8; 8] {
    number.to_le_bytes()
}

#[inline]
pub(crate) fn arr_to_u64(arr: &[u8]) -> u64 {
    arr.try_into().map(u64::from_le_bytes).unwrap()
}

#[inline]
pub(crate) fn arr_to_u32(arr: &[u8]) -> u32 {
    arr.try_into().map(u32::from_le_bytes).unwrap()
}

#[inline]
pub(crate) fn u32_to_arr(number: u32) -> [u8; 4] {
    number.to_le_bytes()
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn maybe_decompress(buf: Vec<u8>) -> std::io::Result<Vec<u8>> {
    #[cfg(feature = "compression")]
    {
        use super::*;

        static MAX_COMPRESSION_RATIO: AtomicUsize = AtomicUsize::new(1);

        let _measure = Measure::new(&M.decompress);
        loop {
            let ratio = MAX_COMPRESSION_RATIO.load(Acquire);
            match decompress(&*buf, buf.len() * ratio) {
                Err(ref e) if e.kind() == std::io::ErrorKind::Other => {
                    debug!(
                        "bumping expected compression \
                         ratio up from {} to {}: {:?}",
                        ratio,
                        ratio + 1,
                        e
                    );
                    let _who_cares = MAX_COMPRESSION_RATIO.compare_and_swap(
                        ratio,
                        ratio + 1,
                        Release,
                    );
                }
                other => return other,
            }
        }
    }

    #[cfg(not(feature = "compression"))]
    Ok(buf)
}

#[derive(Debug)]
pub struct PageView<'g> {
    pub(crate) read: Shared<'g, Page>,
    pub(crate) entry: &'g Atomic<Page>,
}

unsafe impl<'g> Send for PageView<'g> {}

unsafe impl<'g> Sync for PageView<'g> {}

impl<'g> PageView<'g> {
    fn rcu<'b, F, B>(
        &self,
        f: F,
        guard: &'b Guard,
    ) -> std::result::Result<B, Shared<'b, Page>>
    where
        F: FnMut(&mut Page) -> B,
    {
        let mut old_pointer = self.read;
        loop {
            let mut clone: Owned<Page> = Owned::new(self.deref().clone());
            let b = f(clone.deref_mut());

            let result =
                self.entry.compare_and_set(self.read, clone, SeqCst, guard);

            match result {
                Ok(_) => return Ok(b),
                Err(cas_error)
                    if cas_error.current.deref().ts() == self.ts() =>
                {
                    // we got here because the page was moved to a new
                    // location.
                    old_pointer = cas_error.current;
                    continue;
                }
                Err(cas_error) => {
                    return Err(cas_error.current);
                }
            }
        }
    }

    pub(crate) fn as_link(&self) -> &Link {
        self.update.as_ref().unwrap().as_link()
    }

    pub(crate) fn as_meta(&self) -> &Meta {
        self.update.as_ref().unwrap().as_meta()
    }

    pub(crate) fn as_counter(&self) -> u64 {
        self.update.as_ref().unwrap().as_counter()
    }

    pub(crate) fn is_free(&self) -> bool {
        self.update == Some(Update::Free) || self.cache_infos.is_empty()
    }

    pub(crate) fn ts(&self) -> u64 {
        self.cache_infos.last().map(|ci| ci.ts).unwrap_or(0)
    }
}

impl<'g> Deref for PageView<'g> {
    type Target = Page;

    fn deref(&self) -> &Page {
        unsafe { self.read.deref() }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheInfo {
    pub ts: u64,
    pub lsn: Lsn,
    pub pointer: DiskPtr,
    pub log_size: usize,
}

/// Update<PageLinkment> denotes a state or a change in a sequence of updates
/// of which a page consists.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Update {
    Link(Link),
    Node(Node),
    Free,
    Counter(u64),
    Meta(Meta),
}

impl Update {
    fn into_node(self) -> Node {
        match self {
            Update::Node(node) => node,
            other => panic!("called into_node on non-Node: {:?}", other),
        }
    }

    fn as_node(&self) -> &Node {
        match self {
            Update::Node(node) => node,
            other => panic!("called as_node on non-Node: {:?}", other),
        }
    }

    fn into_link(self) -> Link {
        match self {
            Update::Link(link) => link,
            other => panic!("called into_link on non-Link: {:?}", other),
        }
    }

    fn as_link(&self) -> &Link {
        match self {
            Update::Link(link) => link,
            other => panic!("called as_link on non-Link: {:?}", other),
        }
    }

    pub(crate) fn as_meta(&self) -> &Meta {
        if let Update::Meta(meta) = self {
            &meta
        } else {
            panic!("called as_meta on {:?}", self)
        }
    }

    pub(crate) fn as_counter(&self) -> u64 {
        if let Update::Counter(counter) = self {
            *counter
        } else {
            panic!("called as_counter on {:?}", self)
        }
    }

    fn is_replace(&self) -> bool {
        if let Update::Node(_) = self {
            true
        } else {
            false
        }
    }

    fn is_free(&self) -> bool {
        if let Update::Free = self {
            true
        } else {
            false
        }
    }
}

/// Ensures that any operations that are written to disk between the
/// creation of this guard and its destruction will be recovered
/// atomically. When this guard is dropped, it marks in an earlier
/// reservation where the stable tip must be in order to perform
/// recovery. If this is beyond where the system successfully
/// wrote before crashing, then the recovery will stop immediately
/// before any of the atomic batch can be partially recovered.
///
/// Must call `seal_batch` to complete the atomic batch operation.
///
/// If this is dropped without calling `seal_batch`, the complete
/// recovery effect will not occur.
pub struct RecoveryGuard<'a> {
    batch_res: Reservation<'a>,
}

impl<'a> RecoveryGuard<'a> {
    /// Writes the last LSN for a batch into an earlier
    /// reservation, releasing it.
    pub fn seal_batch(mut self) -> Result<()> {
        let max_reserved =
            self.batch_res.log.iobufs.max_reserved_lsn.load(Acquire);
        self.batch_res.mark_writebatch(max_reserved);
        self.batch_res.complete().map(|_| ())
    }
}

/// A page consists of a sequence of state transformations
/// with associated storage parameters like disk pos, lsn, time.
#[derive(Debug, Clone)]
pub struct Page {
    pub(crate) update: Option<Update>,
    pub(crate) cache_infos: Vec<CacheInfo>,
}

impl Page {
    fn log_size(&self) -> usize {
        self.cache_infos.iter().map(|ci| ci.log_size).sum()
    }

    fn ts(&self) -> u64 {
        self.cache_infos.last().map(|ci| ci.ts).unwrap_or(0)
    }

    pub(crate) fn last_cache_info(&self) -> Option<&CacheInfo> {
        self.cache_infos.last()
    }

    pub(crate) fn unwrap_link(&self) -> &Link {
        self.update.unwrap().as_link()
    }

    pub(crate) fn unwrap_node(&self) -> &Node {
        self.update.unwrap().as_node()
    }
}

/// A lock-free pagecache which supports linkmented pages
/// for dramatically improving write throughput.
pub struct PageCache {
    config: RunningConfig,
    inner: PageTable,
    next_pid_to_allocate: AtomicU64,
    free: Arc<Mutex<BinaryHeap<PageId>>>,
    log: Log,
    lru: Lru,
    updates: AtomicU64,
    last_snapshot: Arc<Mutex<Option<Snapshot>>>,
    idgen: Arc<AtomicU64>,
    idgen_persists: Arc<AtomicU64>,
    idgen_persist_mu: Arc<Mutex<()>>,
    was_recovered: bool,
}

unsafe impl Send for PageCache {}

unsafe impl Sync for PageCache {}

impl Debug for PageCache {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        f.write_str(&*format!(
            "PageCache {{ max: {:?} free: {:?} }}\n",
            self.next_pid_to_allocate.load(Acquire),
            self.free
        ))
    }
}

#[cfg(feature = "event_log")]
impl Drop for PageCache {
    fn drop(&mut self) {
        use std::collections::HashMap;

        trace!("dropping pagecache");

        // we can't as easily assert recovery
        // invariants across failpoints for now
        if self.log.iobufs.config.global_error().is_ok() {
            let mut pages_before_restart = HashMap::new();

            let guard = pin();

            self.config.event_log.meta_before_restart(
                self.meta(&guard).expect("should get meta under test").clone(),
            );

            for pid in 0..self.next_pid_to_allocate.load(Acquire) {
                let pte = self.inner.get(pid);
                if pte.is_none() {
                    continue;
                }
                let head = pte.unwrap().deref().head(&guard);
                let pointers = pointers_from_stack(head, &guard);
                pages_before_restart.insert(pid, pointers);
            }

            self.config.event_log.pages_before_restart(pages_before_restart);
        }

        trace!("pagecache dropped");
    }
}

impl PageCache {
    /// Instantiate a new `PageCache`.
    pub fn start(config: RunningConfig) -> Result<Self> {
        trace!("starting pagecache");

        config.reset_global_error();

        // try to pull any existing snapshot off disk, and
        // apply any new data to it to "catch-up" the
        // snapshot before loading it.
        let snapshot = read_snapshot_or_default(&config)?;

        let cache_capacity = config.cache_capacity;
        let lru = Lru::new(cache_capacity);

        let mut pc = Self {
            config: config.clone(),
            inner: PageTable::default(),
            next_pid_to_allocate: AtomicU64::new(0),
            free: Arc::new(Mutex::new(BinaryHeap::new())),
            log: Log::start(config, &snapshot)?,
            lru,
            updates: AtomicU64::new(0),
            last_snapshot: Arc::new(Mutex::new(Some(snapshot))),
            idgen_persist_mu: Arc::new(Mutex::new(())),
            idgen: Arc::new(AtomicU64::new(0)),
            idgen_persists: Arc::new(AtomicU64::new(0)),
            was_recovered: false,
        };

        // now we read it back in
        pc.load_snapshot();

        #[cfg(feature = "event_log")]
        {
            use std::collections::HashMap;

            // NB this must be before idgen/meta are initialized
            // because they may cas_page on initial page-in.
            let guard = pin();

            let mut pages_after_restart = HashMap::new();

            for pid in 0..pc.next_pid_to_allocate.load(Acquire) {
                let pte = pc.inner.get(pid);
                if pte.is_none() {
                    continue;
                }
                let head = pte.unwrap().deref().head(&guard);
                let pointers = pointers_from_stack(head, &guard);
                pages_after_restart.insert(pid, pointers);
            }

            pc.config.event_log.pages_after_restart(pages_after_restart);
        }

        let mut was_recovered = true;

        {
            // subscope required because pc.begin() borrows pc

            let guard = pin();

            if let Err(Error::ReportableBug(..)) = pc.get_meta(&guard) {
                // set up meta
                was_recovered = false;

                let meta_update = Update::Meta(Meta::default());

                let (meta_id, _) = pc.allocate_inner(meta_update, &guard)?;

                assert_eq!(
                    meta_id, META_PID,
                    "we expect the meta page to have pid {}, but it had pid {} instead",
                    META_PID, meta_id,
                );
            }

            if let Err(Error::ReportableBug(..)) = pc.get_idgen(&guard) {
                // set up idgen
                was_recovered = false;

                let counter_update = Update::Counter(0);

                let (counter_id, _) =
                    pc.allocate_inner(counter_update, &guard)?;

                assert_eq!(
                    counter_id, COUNTER_PID,
                    "we expect the counter to have pid {}, but it had pid {} instead",
                    COUNTER_PID, counter_id,
                );
            }

            let (_, counter) = pc.get_idgen(&guard)?;
            let idgen_recovery = if was_recovered {
                counter + (2 * pc.config.idgen_persist_interval)
            } else {
                0
            };
            let idgen_persists = counter / pc.config.idgen_persist_interval
                * pc.config.idgen_persist_interval;

            pc.idgen.store(idgen_recovery, Release);
            pc.idgen_persists.store(idgen_persists, Release);
        }

        pc.was_recovered = was_recovered;

        #[cfg(feature = "event_log")]
        {
            let guard = pin();

            pc.config.event_log.meta_after_restart(
                pc.meta(&guard)
                    .expect("should be able to get meta under test")
                    .clone(),
            );
        }

        trace!("pagecache started");

        Ok(pc)
    }

    /// Flushes any pending IO buffers to disk to ensure durability.
    /// Returns the number of bytes written during this call.
    pub fn flush(&self) -> Result<usize> {
        self.log.flush()
    }

    /// Create a new page, trying to reuse old freed pages if possible
    /// to maximize underlying `PageTable` pointer density. Returns
    /// the page ID and its pointer for use in future atomic `replace`
    /// and `link` operations.
    pub fn allocate<'g>(
        &self,
        new: Node,
        guard: &'g Guard,
    ) -> Result<(PageId, PageView<'g>)> {
        self.allocate_inner(Update::Node(new), guard)
    }

    /// Attempt to opportunistically rewrite data from a Draining
    /// segment of the file to help with space amplification.
    /// Returns Ok(true) if we had the opportunity to attempt to
    /// move a page. Returns Ok(false) if there were no pages
    /// to GC. Returns an Err if we encountered an IO problem
    /// while performing this GC.
    pub fn attempt_gc(&self) -> Result<bool> {
        if self.config.read_only {
            return Ok(false);
        }
        let guard = pin();
        let to_clean = self.log.with_sa(|sa| sa.clean(COUNTER_PID));
        let ret = if let Some(to_clean) = to_clean {
            self.rewrite_page(to_clean, &guard).map(|_| true)
        } else {
            Ok(false)
        };
        guard.flush();
        ret
    }

    /// Initiate an atomic sequence of writes to the
    /// underlying log. Returns a `RecoveryGuard` which,
    /// when dropped, will record the current max reserved
    /// LSN into an earlier log reservation. During recovery,
    /// when we hit this early atomic LSN marker, if the
    /// specified LSN is beyond the contiguous tip of the log,
    /// we immediately halt recovery, preventing the recovery
    /// of partial transactions or write batches. This is
    /// a relatively low-level primitive that can be used
    /// to facilitate transactions and write batches when
    /// combined with a concurrency control system in another
    /// component.
    pub fn pin_log(&self) -> Result<RecoveryGuard<'_>> {
        let batch_res = self.log.reserve(
            LogKind::Skip,
            BATCH_MANIFEST_PID,
            &[0; std::mem::size_of::<Lsn>()],
        )?;
        Ok(RecoveryGuard { batch_res })
    }

    #[doc(hidden)]
    #[cfg(feature = "failpoints")]
    pub fn set_failpoint(&self, e: Error) {
        if let Error::FailPoint = e {
            self.config.set_global_error(e);

            // wake up any waiting threads
            // so they don't stall forever
            let _ = self.log.iobufs.intervals.lock();
            let _notified = self.log.iobufs.interval_updated.notify_all();
        }
    }

    fn allocate_inner<'g>(
        &self,
        new: Update,
        guard: &'g Guard,
    ) -> Result<(PageId, PageView<'g>)> {
        let (pid, page_view) = if let Some(pid) = self.free.lock().pop() {
            trace!("re-allocating pid {}", pid);

            let page_view = match self.inner.get(pid, guard) {
                None => panic!(
                    "expected to find existing stack \
                     for re-allocated pid {}",
                    pid
                ),
                Some(p) => p,
            };
            assert_eq!(
                Some(Update::Free),
                page_view.update,
                "failed to re-allocate pid {} which \
                 contained unexpected state {:?}",
                pid,
                page_view,
            );
            (pid, page_view)
        } else {
            let pid = self.next_pid_to_allocate.fetch_add(1, Relaxed);

            trace!("allocating pid {} for the first time", pid);

            let new_page = Page { update: Some(new), cache_infos: vec![] };

            let page_view = self.inner.insert(pid, new_page, guard);

            (pid, page_view)
        };

        let new_pointer = self
            .cas_page(pid, page_view, new, false, guard)?
            .unwrap_or_else(|e| {
                panic!(
                    "should always be able to install \
                     a new page during allocation, but \
                     failed for pid {}: {:?}",
                    pid, e
                )
            });

        Ok((pid, new_pointer))
    }

    /// Free a particular page.
    pub fn free<'g>(
        &self,
        pid: PageId,
        old: PageView<'g>,
        guard: &'g Guard,
    ) -> Result<CasResult<'g, ()>> {
        trace!("attempting to free pid {}", pid);

        if pid == COUNTER_PID || pid == META_PID || pid == BATCH_MANIFEST_PID {
            return Err(Error::Unsupported(
                "you are not able to free the first \
                 couple pages, which are allocated \
                 for system internal purposes"
                    .into(),
            ));
        }

        let new_pointer =
            self.cas_page(pid, old, Update::Free, false, guard)?;

        if new_pointer.is_ok() {
            let free = self.free.clone();
            guard.defer(move || {
                let mut free = free.lock();
                // panic if we double-freed a page
                if free.iter().any(|e| e == &pid) {
                    panic!("pid {} was double-freed", pid);
                }

                free.push(pid);
            });
        }

        Ok(new_pointer.map_err(|o| o.map(|(pointer, _)| (pointer, ()))))
    }

    /// Try to atomically add a `PageLink` to the page.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns
    /// `Err(Some(actual_key))` if the atomic link fails.
    pub fn link<'g>(
        &'g self,
        pid: PageId,
        mut old: PageView<'g>,
        new: Link,
        guard: &'g Guard,
    ) -> Result<CasResult<'g, Link>> {
        let _measure = Measure::new(&M.link_page);

        trace!("linking pid {} with {:?}", pid, new);

        // A failure injector that fails links randomly
        // during test to ensure interleaving coverage.
        #[cfg(any(test, feature = "lock_free_delays"))]
        {
            use std::cell::RefCell;
            use std::time::{SystemTime, UNIX_EPOCH};

            thread_local! {
                pub static COUNT: RefCell<u32> = RefCell::new(1);
            }

            let time_now =
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

            #[allow(clippy::cast_possible_truncation)]
            let fail_seed = std::cmp::max(3, time_now.as_nanos() as u32 % 128);

            let inject_failure = COUNT.with(|c| {
                let mut cr = c.borrow_mut();
                *cr += 1;
                *cr % fail_seed == 0
            });

            if inject_failure {
                debug!(
                    "injecting a randomized failure in the link of pid {}",
                    pid
                );
                if let Some((current_pointer, _link, _sz)) =
                    self.get(pid, guard)?
                {
                    return Ok(Err(Some((current_pointer, new))));
                } else {
                    return Ok(Err(None));
                }
            }
        }

        let page_cell = match self.inner.get(pid, guard) {
            None => return Ok(Err(None)),
            Some(p) => p,
        };

        let mut link: Link = page_cell.clone();
        link.apply(new);

        // see if we should short-circuit replace
        let head = stack.head(guard);
        let stack_iter = StackIter::from_ptr(head, guard);
        let stack_len = stack_iter.size_hint().1.unwrap();
        if stack_len >= PAGE_CONSOLIDATION_THRESHOLD {
            return self.replace(pid, old, link, guard);
        }

        let bytes = measure(&M.serialize, || serialize(&new).unwrap());

        let mut new = {
            let update = Update::Link(new);

            let cache_info = CacheInfo {
                lsn: -1,
                pointer: DiskPtr::Inline(666_666_666),
                ts: 0,
                log_size: 0,
            };

            let page =
                Page { update: Some(update), cache_infos: vec![cache_info] };

            Some(Owned::new(page))
        };

        loop {
            let log_reservation =
                self.log.reserve(LogKind::Link, pid, &bytes)?;

            let lsn = log_reservation.lsn();
            let pointer = log_reservation.pointer();

            // NB the setting of the timestamp is quite
            // correctness-critical! We use the ts to
            // ensure that fundamentally new data causes
            // high-level link and replace operations
            // to fail when the data in the pagecache
            // actually changes. When we just rewrite
            // the page for the purposes of moving it
            // to a new location on disk, however, we
            // don't want to cause threads that are
            // basing the correctness of their new
            // writes on the unchanged state to fail.
            // Here, we bump it by 1, to signal that
            // the underlying state is fundamentally
            // changing.
            let ts = old.ts + 1;

            let mut node = new.take().unwrap();

            let cache_info = CacheInfo {
                lsn,
                pointer,
                ts,
                log_size: log_reservation.reservation_len(),
            };

            if let (Some(Update::Link(_)), ref mut stored_cache_info) =
                node.inner
            {
                *stored_cache_info = cache_info;
            } else {
                panic!("should only be working with Resident entries");
            }

            debug_delay();
            let result = stack.cap_node(old.cached_pointer, node, guard);

            match result {
                Ok(cached_pointer) => {
                    trace!("link of pid {} succeeded", pid);

                    // if the last update for this page was also
                    // sent to this segment, we can skip marking it
                    let previous_head_lsn = old.last_lsn();

                    assert_ne!(previous_head_lsn, 0);

                    let previous_lsn_segment =
                        previous_head_lsn / self.config.segment_size as i64;
                    let new_lsn_segment = lsn / self.config.segment_size as i64;

                    let to_clean = if previous_lsn_segment == new_lsn_segment {
                        // can skip mark_link because we've
                        // already accounted for this page
                        // being resident on this segment
                        self.log.with_sa(|sa| sa.clean(pid))
                    } else {
                        self.log.with_sa(|sa| {
                            sa.mark_link(pid, lsn, pointer);
                            sa.clean(pid)
                        })
                    };

                    // NB complete must happen AFTER calls to SA, because
                    // when the iobuf's n_writers hits 0, we may transition
                    // the segment to inactive, resulting in a race otherwise.
                    // FIXME can result in deadlock if a node that holds SA
                    // is waiting to acquire a new reservation blocked by this?
                    log_reservation.complete()?;

                    if let Some(to_clean) = to_clean {
                        self.rewrite_page(to_clean, guard)?;
                    }

                    let count = self.updates.fetch_add(1, Relaxed) + 1;
                    let should_snapshot =
                        count % self.config.snapshot_after_ops == 0;
                    if should_snapshot {
                        self.advance_snapshot()?;
                    }

                    return Ok(Ok(PageView { cached_pointer, ts }));
                }
                Err((actual_pointer, returned_new)) => {
                    trace!("link of pid {} failed", pid);
                    log_reservation.abort()?;
                    let actual_ts = unsafe { actual_pointer.deref().1.ts };
                    if actual_ts == old.ts {
                        new = Some(returned_new);
                        old = PageView {
                            cached_pointer: actual_pointer,
                            ts: actual_ts,
                        };
                    } else {
                        let returned_update = returned_new.0.clone().unwrap();
                        let returned_link = returned_update.into_link();
                        return Ok(Err(Some((
                            PageView {
                                cached_pointer: actual_pointer,
                                ts: actual_ts,
                            },
                            returned_link,
                        ))));
                    }
                }
            }
        }
    }

    /// Node an existing page with a different set of `PageLink`s.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns
    /// `Err(Some(actual_key))` if the atomic swap fails.
    pub fn replace<'g>(
        &self,
        pid: PageId,
        old: PageView<'g>,
        new: Node,
        guard: &'g Guard,
    ) -> Result<CasResult<'g, Link>> {
        let _measure = Measure::new(&M.replace_page);

        trace!("replacing pid {} with {:?}", pid, new);

        // A failure injector that fails replace calls randomly
        // during test to ensure interleaving coverage.
        #[cfg(any(test, feature = "lock_free_delays"))]
        {
            use std::cell::RefCell;
            use std::time::{SystemTime, UNIX_EPOCH};

            thread_local! {
                pub static COUNT: RefCell<u32> = RefCell::new(1);
            }

            let time_now =
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

            #[allow(clippy::cast_possible_truncation)]
            let fail_seed = std::cmp::max(3, time_now.as_nanos() as u32 % 128);

            let inject_failure = COUNT.with(|c| {
                let mut cr = c.borrow_mut();
                *cr += 1;
                *cr % fail_seed == 0
            });

            if inject_failure {
                debug!(
                    "injecting a randomized failure in the replace of pid {}",
                    pid
                );
                if let Some((current_pointer, _link, _sz)) =
                    self.get(pid, guard)?
                {
                    return Ok(Err(Some((current_pointer, new))));
                } else {
                    return Ok(Err(None));
                }
            }
        }

        let result =
            self.cas_page(pid, old, Update::Node(new), false, guard)?;

        let to_clean = self.log.with_sa(|sa| sa.clean(pid));

        if let Some(to_clean) = to_clean {
            assert_ne!(pid, to_clean);
            self.rewrite_page(to_clean, guard)?;
        }

        let count = self.updates.fetch_add(1, Relaxed) + 1;
        let should_snapshot = count % self.config.snapshot_after_ops == 0;
        if should_snapshot {
            self.advance_snapshot()?;
        }

        Ok(result.map_err(|fail| {
            let (pointer, shared) = fail.unwrap();
            if let Update::Node(rejected_new) = shared {
                Some((pointer, rejected_new))
            } else {
                unreachable!();
            }
        }))
    }

    // rewrite a page so we can reuse the segment that it is
    // (at least partially) located in. This happens when a
    // segment has had enough resident page linkments moved
    // away to trigger the `segment_cleanup_threshold`.
    fn rewrite_page(&self, pid: PageId, guard: &Guard) -> Result<()> {
        let _measure = Measure::new(&M.rewrite_page);

        trace!("rewriting pid {}", pid);

        let stack = match self.inner.get(pid) {
            None => {
                trace!("rewriting pid {} failed (no longer exists)", pid);
                return Ok(());
            }
            Some(p) => p,
        };

        debug_delay();
        let head = stack.head(guard);
        let stack_iter = StackIter::from_ptr(head, guard);
        let cache_entries: Vec<_> = stack_iter.collect();

        // if the page is just a single blob pointer, rewrite it.
        if cache_entries.len() == 1 && cache_entries[0].1.pointer.is_blob() {
            trace!("rewriting blob with pid {}", pid);
            let blob_pointer = cache_entries[0].1.pointer.blob().1;

            let log_reservation =
                self.log.rewrite_blob_pointer(pid, blob_pointer)?;

            let new_pointer = log_reservation.pointer();
            let mut new_cache_entry = cache_entries[0].clone();

            new_cache_entry.1.pointer = new_pointer;

            let node = node_from_link_vec(vec![new_cache_entry]);

            debug_delay();
            let result = stack.cas(head, node, guard);

            if result.is_ok() {
                let pointers = pointers_from_stack(head, guard);
                let lsn = log_reservation.lsn();

                self.log.with_sa(|sa| {
                    sa.mark_replace(pid, lsn, pointers, new_pointer)
                })?;

                // NB complete must happen AFTER calls to SA, because
                // when the iobuf's n_writers hits 0, we may transition
                // the segment to inactive, resulting in a race otherwise.
                let _pointer = log_reservation.complete()?;

                trace!("rewriting pid {} succeeded", pid);

                Ok(())
            } else {
                let _pointer = log_reservation.abort()?;

                trace!("rewriting pid {} failed", pid);

                Ok(())
            }
        } else {
            trace!("rewriting page with pid {}", pid);

            // page-in whole page with a get
            let (key, update): (_, Update) = if pid == META_PID {
                let (key, meta) = self.get_meta(guard)?;
                (key, Update::Meta(meta.clone()))
            } else if pid == COUNTER_PID {
                let (key, counter) = self.get_idgen(guard)?;
                (key, Update::Counter(counter))
            } else if let Some((key, link, _sz)) = self.get(pid, guard)? {
                (key, Update::Node(link.clone()))
            } else {
                let head = stack.head(guard);

                let mut stack_iter = StackIter::from_ptr(head, guard);

                match stack_iter.next() {
                    Some((Some(Update::Free), cache_info)) => (
                        PageView { cached_pointer: head, ts: cache_info.ts },
                        Update::Free,
                    ),
                    other => {
                        debug!(
                            "when rewriting pid {} \
                             we encountered a rewritten \
                             node with a link {:?} that \
                             we previously witnessed a Free \
                             for (PageCache::get returned None), \
                             assuming we can just return now since \
                             the Free was replace'd",
                            pid, other
                        );
                        return Ok(());
                    }
                }
            };

            self.cas_page(pid, key, update, true, guard).map(|res| {
                trace!("rewriting pid {} success: {}", pid, res.is_ok());
            })
        }
    }

    /// Traverses all files and calculates their total physical
    /// size, then traverses all pages and calculates their
    /// total logical size, then divides the physical size
    /// by the logical size.
    #[allow(clippy::cast_precision_loss)]
    #[doc(hidden)]
    pub fn space_amplification(&self) -> Result<f64> {
        let on_disk_bytes = self.size_on_disk()? as f64;
        let logical_size = self.logical_size_of_all_pages()? as f64;
        let discount = self.config.segment_size as f64 * 8.;

        Ok(on_disk_bytes / (logical_size + discount))
    }

    fn size_on_disk(&self) -> Result<u64> {
        let mut size = self.config.file.metadata()?.len();

        let stable = self.config.blob_path(0);
        let blob_dir = stable.parent().unwrap();
        let blob_files = std::fs::read_dir(blob_dir)?;

        for blob_file in blob_files {
            size += blob_file?.metadata()?.len();
        }

        Ok(size)
    }

    fn logical_size_of_all_pages(&self) -> Result<u64> {
        let guard = pin();
        let meta_size = self.meta(&guard)?.size_in_bytes();
        let idgen_size = std::mem::size_of::<u64>() as u64;
        let config_size = self.get_persisted_config(&guard)?.1.size_in_bytes();

        let mut ret = meta_size + idgen_size + config_size;
        let min_pid = COUNTER_PID + 1;
        let next_pid_to_allocate = self.next_pid_to_allocate.load(Acquire);
        for pid in min_pid..next_pid_to_allocate {
            if let Some((_, _, sz)) = self.get(pid, &guard)? {
                ret += sz;
            }
        }
        Ok(ret)
    }

    fn cas_page<'g>(
        &self,
        pid: PageId,
        mut old: PageView<'g>,
        update: Update,
        is_rewrite: bool,
        guard: &'g Guard,
    ) -> Result<CasResult<'g, Update>> {
        trace!(
            "cas_page called on pid {} to {:?} with old ts {:?}",
            pid,
            update,
            old.ts
        );
        let stack = match self.inner.get(pid) {
            None => {
                trace!("early-returning from cas_page, no stack found");
                return Ok(Err(None));
            }
            Some(p) => p,
        };

        let log_kind = log_kind_from_update(&update);
        let serialize_latency = Measure::new(&M.serialize);
        let bytes = match &update {
            Update::Counter(c) => serialize(&c).unwrap(),
            Update::Meta(m) => serialize(&m).unwrap(),
            Update::Free => vec![],
            other => serialize(other.as_link()).unwrap(),
        };
        drop(serialize_latency);
        let mut update_opt = Some(update);

        loop {
            let log_reservation = self.log.reserve(log_kind, pid, &bytes)?;
            let lsn = log_reservation.lsn();
            let new_pointer = log_reservation.pointer();

            // NB the setting of the timestamp is quite
            // correctness-critical! We use the ts to
            // ensure that fundamentally new data causes
            // high-level link and replace operations
            // to fail when the data in the pagecache
            // actually changes. When we just rewrite
            // the page for the purposes of moving it
            // to a new location on disk, however, we
            // don't want to cause threads that are
            // basing the correctness of their new
            // writes on the unchanged state to fail.
            // Here, we only bump it up by 1 if the
            // update represents a fundamental change
            // that SHOULD cause CAS failures.
            // Here, we only bump it up by 1 if the
            // update represents a fundamental change
            // that SHOULD cause CAS failures.
            let ts = if is_rewrite { old.ts } else { old.ts + 1 };

            let cache_info = CacheInfo {
                ts,
                lsn,
                pointer: new_pointer,
                log_size: log_reservation.reservation_len(),
            };

            let node = node_from_link_vec(vec![(
                Some(update_opt.take().unwrap()),
                cache_info,
            )]);

            debug_delay();
            let result = stack.cas(old.cached_pointer, node, guard);

            match result {
                Ok(cached_pointer) => {
                    trace!("cas_page succeeded on pid {}", pid);
                    let pointers =
                        pointers_from_stack(old.cached_pointer, guard);

                    self.log.with_sa(|sa| {
                        sa.mark_replace(pid, lsn, pointers, new_pointer)
                    })?;

                    // NB complete must happen AFTER calls to SA, because
                    // when the iobuf's n_writers hits 0, we may transition
                    // the segment to inactive, resulting in a race otherwise.
                    let _pointer = log_reservation.complete()?;
                    return Ok(Ok(PageView { cached_pointer, ts }));
                }
                Err((actual_pointer, returned_entry)) => {
                    trace!("cas_page failed on pid {}", pid);
                    let _pointer = log_reservation.abort()?;

                    let returned_update =
                        returned_entry.into_box().inner.0.take().unwrap();

                    let actual_ts = unsafe { actual_pointer.deref().1.ts };

                    if actual_ts != old.ts || is_rewrite {
                        return Ok(Err(Some((
                            PageView {
                                cached_pointer: actual_pointer,
                                ts: actual_ts,
                            },
                            returned_update,
                        ))));
                    }
                    trace!(
                        "retrying CAS on pid {} with same ts of {}",
                        pid,
                        old.ts
                    );
                    old =
                        PageView { cached_pointer: actual_pointer, ts: old.ts };
                    update_opt = Some(returned_update);
                }
            } // match cas result
        } // loop
    }

    /// Retrieve the current meta page
    pub(crate) fn get_meta<'g>(
        &self,
        guard: &'g Guard,
    ) -> Result<(PageView<'g>, &'g Meta)> {
        trace!("getting page iter for META");

        let page_cell = match self.inner.get(META_PID, guard) {
            None => {
                return Err(Error::ReportableBug(
                    "failed to retrieve META page \
                     which should always be present"
                        .into(),
                ));
            }
            Some(p) => p,
        };

        let cache_info = page_cell.last_cache_info().unwrap();
        let page_ptr =
            PageView { cached_pointer: page_cell.read, ts: cache_info.ts };

        if page_cell.update.is_some() {
            let meta_ref = page_cell.as_meta();
            Ok((page_ptr, meta_ref))
        } else {
            Err(Error::ReportableBug(
                "failed to retrieve META page \
                 which should always be present"
                    .into(),
            ))
        }
    }

    /// Retrieve the current persisted IDGEN value
    pub(crate) fn get_idgen<'g>(
        &self,
        guard: &'g Guard,
    ) -> Result<(PageView<'g>, u64)> {
        trace!("getting page iter for idgen");

        let page_cell = match self.inner.get(COUNTER_PID, guard) {
            None => {
                return Err(Error::ReportableBug(
                    "failed to retrieve counter page \
                     which should always be present"
                        .into(),
                ));
            }
            Some(p) => p,
        };

        if page_cell.update.is_some() {
            let counter = page_cell.as_counter();
            Ok((page_cell.page_ptr(), counter))
        } else {
            Err(Error::ReportableBug(
                "failed to retrieve counter page \
                 which should always be present"
                    .into(),
            ))
        }
    }

    /// Try to retrieve a page by its logical ID.
    pub fn get<'g>(
        &self,
        pid: PageId,
        guard: &'g Guard,
    ) -> Result<Option<(PageView<'g>, &'g Link, u64)>> {
        trace!("getting page iterator for pid {}", pid);
        let _measure = Measure::new(&M.get_page);

        if pid == COUNTER_PID || pid == META_PID || pid == BATCH_MANIFEST_PID {
            return Err(Error::Unsupported(
                "you are not able to iterate over \
                 the first couple pages, which are \
                 reserved for storing metadata and \
                 monotonic ID generator info"
                    .into(),
            ));
        }

        let page_cell = match self.inner.get(pid, guard) {
            None => return Ok(None),
            Some(p) => p,
        };

        if page_cell.is_free() {
            return Ok(None);
        }

        let total_page_size = page_cell.log_size();

        if page_cell.update.is_some() {
            let page_ptr = page_cell.page_ptr();
            let replace = page_cell.as_link();
            return Ok(Some((page_ptr, replace, total_page_size)));
        }
        let initial_base = match entries[0] {
            (Some(Update::Node(replace)), cache_info) => {
                // short circuit
                return Ok(Some((
                    PageView { cached_pointer: head, ts: cache_info.ts },
                    replace,
                    total_page_size,
                )));
            }
            (Some(Update::Link(_)), _) => {
                // merge to next item
                let base_idx = entries.iter().position(|(e, _)| {
                    e.is_some() && e.as_ref().unwrap().is_replace()
                });
                if let Some(base_idx) = base_idx {
                    let mut base =
                        entries[base_idx].0.as_ref().unwrap().as_link().clone();
                    for (link, _) in entries[0..base_idx].iter().rev() {
                        base.merge(link.as_ref().unwrap().as_link());
                    }
                    Some(base)
                } else {
                    None
                }
            }
            _ => {
                // need to pull everything from disk and merge
                None
            }
        };

        let base = if let Some(initial_base) = initial_base {
            initial_base
        } else {
            // we were not able to short-circuit, so we should
            // fix-up the stack.
            let pulled = entries.iter().map(|entry| match entry {
                (Some(Update::Node(replace)), _)
                | (Some(Update::Link(replace)), _) => {
                    Ok(Cow::Borrowed(replace))
                }
                (None, cache_info) => {
                    let res = self
                        .pull(pid, cache_info.lsn, cache_info.pointer)
                        .map(|pg| pg)?;
                    Ok(Cow::Owned(res.into_link()))
                }
                other => {
                    panic!("iterating over unexpected update: {:?}", other);
                }
            });

            // if any of our pulls failed, bail here
            let mut successes: Vec<Cow<'_, Link>> = match pulled.collect() {
                Ok(success) => success,
                Err(Error::Io(ref error))
                    if error.kind() == std::io::ErrorKind::NotFound =>
                {
                    // blob has been removed
                    // TODO is this possible to hit if it's just rewritten?
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };

            let mut base = successes.pop().unwrap().into_owned();

            while let Some(link) = successes.pop() {
                base.merge(&link);
            }

            base
        };

        // fix up the stack to include our pulled items
        let mut links: Vec<(Option<Update>, CacheInfo)> =
            entries.iter().map(|(_, cache_info)| (None, *cache_info)).collect();

        links[0].0 = Some(Update::Node(base));

        let node = node_from_link_vec(links).into_shared(guard);

        #[cfg(feature = "event_log")]
        assert_eq!(
            pointers_from_stack(head, guard),
            pointers_from_stack(node, guard),
        );

        let node = unsafe { node.into_owned() };

        debug_delay();
        let res = stack.cas(head, node, guard);
        if let Ok(new_pointer) = res {
            trace!("fix-up for pid {} succeeded", pid);

            // possibly evict an item now that our cache has grown
            let to_evict = self.lru.accessed(pid, total_page_size);
            trace!("accessed pid {} -> paging out pids {:?}", pid, to_evict);
            if !to_evict.is_empty() {
                self.page_out(to_evict, guard)?;
            }

            let page_ref = unsafe {
                let item = &new_pointer.deref().inner;
                if let (Some(Update::Node(replace)), _) = item {
                    replace
                } else {
                    panic!()
                }
            };

            let pointer =
                PageView { cached_pointer: new_pointer, ts: entries[0].1.ts };

            Ok(Some((pointer, page_ref, total_page_size)))
        } else {
            trace!("fix-up for pid {} failed", pid);

            self.get(pid, guard)
        }
    }

    /// The highest known stable Lsn on disk.
    pub fn stable_lsn(&self) -> Lsn {
        self.log.stable_offset()
    }

    /// Blocks until the provided Lsn is stable on disk,
    /// triggering necessary flushes in the process.
    /// Returns the number of bytes written during
    /// this call.
    pub fn make_stable(&self, lsn: Lsn) -> Result<usize> {
        self.log.make_stable(lsn)
    }

    /// Returns `true` if the database was
    /// recovered from a previous process.
    /// Note that database state is only
    /// guaranteed to be present up to the
    /// last call to `flush`! Otherwise state
    /// is synced to disk periodically if the
    /// `sync_every_ms` configuration option
    /// is set to `Some(number_of_ms_between_syncs)`
    /// or if the IO buffer gets filled to
    /// capacity before being rotated.
    pub const fn was_recovered(&self) -> bool {
        self.was_recovered
    }

    /// Generate a monotonic ID. Not guaranteed to be
    /// contiguous. Written to disk every `idgen_persist_interval`
    /// operations, followed by a blocking flush. During recovery, we
    /// take the last recovered generated ID and add 2x
    /// the `idgen_persist_interval` to it. While persisting, if the
    /// previous persisted counter wasn't synced to disk yet, we will do
    /// a blocking flush to fsync the latest counter, ensuring
    /// that we will never give out the same counter twice.
    pub fn generate_id(&self) -> Result<u64> {
        let ret = self.idgen.fetch_add(1, Relaxed);

        let interval = self.config.idgen_persist_interval;
        let necessary_persists = ret / interval * interval;
        let mut persisted = self.idgen_persists.load(Acquire);

        while persisted < necessary_persists {
            let _mu = self.idgen_persist_mu.lock();
            persisted = self.idgen_persists.load(Acquire);
            if persisted < necessary_persists {
                // it's our responsibility to persist up to our ID
                let guard = pin();
                let (key, current) = self.get_idgen(&guard)?;

                assert_eq!(current, persisted);

                let counter_update = Update::Counter(necessary_persists);

                let old = self.idgen_persists.swap(necessary_persists, Release);
                assert_eq!(old, persisted);

                if self
                    .cas_page(
                        COUNTER_PID,
                        key.clone(),
                        counter_update,
                        false,
                        &guard,
                    )?
                    .is_err()
                {
                    // CAS failed
                    continue;
                }

                // during recovery we add 2x the interval. we only
                // need to block if the last one wasn't stable yet.
                let gap = (necessary_persists - persisted) / interval;
                if gap > 1 {
                    // this is the most pessimistic case, hopefully
                    // we only ever hit this on the first ID generation
                    // of a process's lifetime
                    let _written = self.flush()?;
                } else if key.last_lsn() > self.stable_lsn() {
                    let _written = self.make_stable(key.last_lsn())?;
                }
            }
        }

        Ok(ret)
    }

    /// Returns the current `Meta` map, which contains a convenient
    /// mapping from identifiers to `PageId`'s that the `PageCache`
    /// owner may use for storing metadata about their higher-level
    /// collections.
    pub fn meta<'a>(&self, guard: &'a Guard) -> Result<&'a Meta> {
        self.get_meta(guard).map(|(_k, m)| m)
    }

    /// Look up a `PageId` for a given identifier in the `Meta`
    /// mapping. This is pretty cheap, but in some cases
    /// you may prefer to maintain your own atomic references
    /// to collection roots instead of relying on this. See
    /// sled's `Tree` root tracking for an example of
    /// avoiding this in a lock-free way that handles
    /// various race conditions.
    pub fn meta_pid_for_name(
        &self,
        name: &[u8],
        guard: &Guard,
    ) -> Result<PageId> {
        let m = self.meta(guard)?;
        if let Some(root) = m.get_root(name) {
            Ok(root)
        } else {
            Err(Error::CollectionNotFound(name.to_vec()))
        }
    }

    /// Compare-and-swap the `Meta` mapping for a given
    /// identifier.
    pub fn cas_root_in_meta<'g>(
        &self,
        name: &[u8],
        old: Option<PageId>,
        new: Option<PageId>,
        guard: &'g Guard,
    ) -> Result<std::result::Result<(), Option<PageId>>> {
        loop {
            let (meta_key, meta) = self.get_meta(guard)?;

            let actual = meta.get_root(name);
            if actual != old {
                return Ok(Err(actual));
            }

            let mut new_meta = (*meta).clone();
            if let Some(new) = new {
                new_meta.set_root(name.to_vec(), new);
            } else {
                new_meta.del_root(name);
            }

            let new_meta_link = Update::Meta(new_meta);

            let res = self.cas_page(
                META_PID,
                meta_key.clone(),
                new_meta_link,
                false,
                guard,
            )?;

            match res {
                Ok(_worked) => return Ok(Ok(())),
                Err(Some((_current_pointer, _rejected))) => {}
                Err(None) => {
                    return Err(Error::ReportableBug(
                        "replacing the META page has failed because \
                         the pagecache does not think it currently exists."
                            .into(),
                    ));
                }
            }
        }
    }

    fn page_out(&self, to_evict: Vec<PageId>, guard: &Guard) -> Result<()> {
        let _measure = Measure::new(&M.page_out);
        'different_page_eviction: for pid in to_evict {
            if pid == COUNTER_PID
                || pid == META_PID
                || pid == BATCH_MANIFEST_PID
            {
                // should not page these suckas out
                continue;
            }

            let stack = match self.inner.get(pid) {
                None => continue 'different_page_eviction,
                Some(p) => p,
            };

            debug_delay();
            let head = stack.head(guard);
            let stack_iter = StackIter::from_ptr(head, guard);
            let stack_len = stack_iter.size_hint().1.unwrap();
            let mut new_stack = Vec::with_capacity(stack_len);

            for (update_opt, cache_info) in stack_iter {
                match update_opt {
                    None | Some(Update::Free) => {
                        // already paged out
                        continue 'different_page_eviction;
                    }
                    Some(_) => {
                        new_stack.push((None, *cache_info));
                    }
                }
            }

            let node = node_from_link_vec(new_stack);

            debug_delay();
            let result = stack.cas(head, node, guard);
            if result.is_ok() {
                // TODO record cache difference
            } else {
                trace!("failed to page-out pid {}", pid)
            }
        }
        Ok(())
    }

    fn pull(&self, pid: PageId, lsn: Lsn, pointer: DiskPtr) -> Result<Update> {
        use MessageKind::*;

        trace!("pulling lsn {} pointer {} from disk", lsn, pointer);
        let _measure = Measure::new(&M.pull);
        let (header, bytes) = match self.log.read(pid, lsn, pointer) {
            Ok(LogRead::Inline(header, buf, _len)) => {
                assert_eq!(
                    header.pid, pid,
                    "expected pid {} on pull of pointer {}, \
                     but got {} instead",
                    pid, pointer, header.pid
                );
                assert_eq!(
                    header.lsn, lsn,
                    "expected lsn {} on pull of pointer {}, \
                     but got lsn {} instead",
                    lsn, pointer, header.lsn
                );
                Ok((header, buf))
            }
            Ok(LogRead::Blob(header, buf, _blob_pointer)) => {
                assert_eq!(
                    header.pid, pid,
                    "expected pid {} on pull of pointer {}, \
                     but got {} instead",
                    pid, pointer, header.pid
                );
                assert_eq!(
                    header.lsn, lsn,
                    "expected lsn {} on pull of pointer {}, \
                     but got lsn {} instead",
                    lsn, pointer, header.lsn
                );

                Ok((header, buf))
            }
            Ok(other) => {
                debug!("read unexpected page: {:?}", other);
                Err(Error::Corruption { at: pointer })
            }
            Err(e) => {
                debug!("failed to read page: {:?}", e);
                Err(e)
            }
        }?;

        let deserialize_latency = Measure::new(&M.deserialize);
        let update_res = match header.kind {
            Counter => deserialize::<u64>(&bytes).map(Update::Counter),
            BlobMeta | InlineMeta => {
                deserialize::<Meta>(&bytes).map(Update::Meta)
            }
            BlobLink | InlineLink => {
                deserialize::<Link>(&bytes).map(Update::Link)
            }
            BlobNode | InlineNode => {
                deserialize::<Node>(&bytes).map(Update::Node)
            }
            Free => Ok(Update::Free),
            Corrupted | Cancelled | Pad | BatchManifest => {
                panic!("unexpected pull: {:?}", header.kind)
            }
        };
        drop(deserialize_latency);

        let update =
            update_res.map_err(|_| ()).expect("failed to deserialize data");

        // TODO this feels racy, test it better?
        if let Update::Free = update {
            Err(Error::ReportableBug(
                "non-link/replace found in pull".to_owned(),
            ))
        } else {
            Ok(update)
        }
    }

    // caller is expected to have instantiated self.last_snapshot
    // in recovery already.
    fn advance_snapshot(&self) -> Result<()> {
        let snapshot_mu = self.last_snapshot.clone();
        let config = self.config.clone();
        let iobufs = self.log.iobufs.clone();

        let gen_snapshot = move || {
            let snapshot_opt_res = snapshot_mu.try_lock();
            if snapshot_opt_res.is_none() {
                // some other thread is snapshotting
                debug!(
                    "snapshot skipped because previous attempt \
                     appears not to have completed"
                );
                return Ok(());
            }

            let mut snapshot_opt = snapshot_opt_res.unwrap();
            let last_snapshot = snapshot_opt
                .take()
                .expect("PageCache::advance_snapshot called before recovery");

            if let Err(e) = iobuf::flush(&iobufs) {
                error!("failed to flush log during advance_snapshot: {}", e);
                iobufs.with_sa(SegmentAccountant::resume_rewriting);
                *snapshot_opt = Some(last_snapshot);
                return Err(e);
            }

            // we disable rewriting so that our log becomes link-only,
            // allowing us to iterate through it without corrupting ourselves.
            // NB must be called after taking the snapshot mutex.
            iobufs.with_sa(SegmentAccountant::pause_rewriting);

            let last_lsn = last_snapshot.last_lsn;
            let start_lsn = last_lsn - (last_lsn % config.segment_size as Lsn);

            let iter = iobufs.iter_from(start_lsn);

            debug!(
                "snapshot starting from offset {} to the segment containing ~{}",
                last_snapshot.last_lsn,
                iobufs.stable(),
            );

            let res = advance_snapshot(iter, last_snapshot, &config);

            // NB it's important to resume writing before replacing the snapshot
            // into the mutex, otherwise we create a race condition where the SA
            // is not actually paused when a snapshot happens.
            iobufs.with_sa(SegmentAccountant::resume_rewriting);

            match res {
                Err(e) => {
                    *snapshot_opt = Some(Snapshot::default());
                    error!("failed to generate snapshot: {:?}", e);
                    Err(e)
                }
                Ok(next_snapshot) => {
                    *snapshot_opt = Some(next_snapshot);
                    Ok(())
                }
            }
        };

        if let Err(e) = self.config.global_error() {
            let _notified = self.log.iobufs.interval_updated.notify_all();
            return Err(e);
        }

        debug!("asynchronously spawning snapshot generation task");
        let config = self.config.clone();
        let _result = threadpool::spawn(move || {
            let result = gen_snapshot();
            match &result {
                Ok(_) => {}
                Err(Error::Io(ref ioe))
                    if ioe.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    error!(
                        "encountered error while generating snapshot: {:?}",
                        error,
                    );
                    config.set_global_error(error.clone());
                }
            }
            result
        });

        #[cfg(test)]
        _result.unwrap()?;

        // TODO add future for waiting on the result of this if desired
        Ok(())
    }

    fn load_snapshot(&mut self) {
        // panic if not set
        let snapshot_mu = self.last_snapshot.try_lock().unwrap();
        let snapshot = snapshot_mu.as_ref().unwrap();

        let next_pid_to_allocate = if snapshot.pt.is_empty() {
            0
        } else {
            *snapshot.pt.keys().max().unwrap() + 1
        };

        self.next_pid_to_allocate = AtomicU64::from(next_pid_to_allocate);

        debug!("load_snapshot loading pages from 0..{}", next_pid_to_allocate);
        for pid in 0..next_pid_to_allocate {
            let state = if let Some(state) = snapshot.pt.get(&pid) {
                state
            } else {
                panic!(
                    "load_snapshot pid {} not found, despite being below the max pid {}",
                    pid, next_pid_to_allocate
                );
            };

            trace!("load_snapshot pid {} {:?}", pid, state);

            let stack = Stack::default();

            let guard = pin();

            match *state {
                PageState::Present(ref pointers) => {
                    for &(lsn, pointer, sz) in pointers {
                        let cache_info =
                            CacheInfo { lsn, pointer, log_size: sz, ts: 0 };

                        stack.push((None, cache_info), &guard);
                    }
                }
                PageState::Free(lsn, pointer) => {
                    // blow away any existing state
                    trace!("load_snapshot freeing pid {}", pid);
                    let cache_info = CacheInfo {
                        lsn,
                        pointer,
                        log_size: MSG_HEADER_LEN,
                        ts: 0,
                    };
                    stack.push((Some(Update::Free), cache_info), &guard);
                    self.free.lock().push(pid);
                }
            }

            // Set up new stack

            trace!("installing stack for pid {}", pid);

            self.inner.insert(pid, stack);
        }
    }
}

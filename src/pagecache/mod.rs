//! `pagecache` is a lock-free pagecache and log for building high-performance
//! databases.
#![allow(unsafe_code)]

pub mod constants;
pub mod logger;

mod blob_io;
mod disk_pointer;
mod header;
mod iobuf;
mod iterator;
mod pagetable;
#[cfg(any(all(not(unix), not(windows)), miri))]
mod parallel_io_polyfill;
#[cfg(all(unix, not(miri)))]
mod parallel_io_unix;
#[cfg(all(windows, not(miri)))]
mod parallel_io_windows;
mod reservation;
mod segment;
mod snapshot;

use std::{collections::BinaryHeap, ops::Deref};

use crate::*;

#[cfg(any(all(not(unix), not(windows)), miri))]
use parallel_io_polyfill::{pread_exact, pread_exact_or_eof, pwrite_all};

#[cfg(all(unix, not(miri)))]
use parallel_io_unix::{pread_exact, pread_exact_or_eof, pwrite_all};

#[cfg(all(windows, not(miri)))]
use parallel_io_windows::{pread_exact, pread_exact_or_eof, pwrite_all};

use self::{
    blob_io::{gc_blobs, read_blob, remove_blob, write_blob},
    constants::{
        BATCH_MANIFEST_PID, COUNTER_PID, META_PID,
        PAGE_CONSOLIDATION_THRESHOLD, SEGMENT_CLEANUP_THRESHOLD,
    },
    header::Header,
    iobuf::{roll_iobuf, IoBuf, IoBufs},
    iterator::{raw_segment_iter_from, LogIter},
    pagetable::PageTable,
    segment::{SegmentAccountant, SegmentCleaner, SegmentOp},
};

pub(crate) use self::{
    logger::{
        read_message, read_segment_header, MessageHeader, SegmentHeader,
        SegmentNumber,
    },
    reservation::Reservation,
    snapshot::{read_snapshot_or_default, PageState, Snapshot},
};

pub use self::{
    constants::{
        MAX_MSG_HEADER_LEN, MAX_SPACE_AMPLIFICATION, MINIMUM_ITEMS_PER_SEGMENT,
        SEG_HEADER_LEN,
    },
    disk_pointer::DiskPtr,
    logger::{Log, LogRead},
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

/// Uses a non-varint `Lsn` to mark offsets.
#[derive(Default, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub struct BatchManifest(pub Lsn);

/// A buffer with an associated offset. Useful for
/// batching many reads over a file segment.
#[derive(Debug)]
pub struct BasedBuf {
    pub buf: Vec<u8>,
    pub offset: LogOffset,
}

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
    Canceled = 1,
    /// Indicates that the following buffer is used
    /// as padding to fill out the rest of the segment
    /// before sealing it.
    Cap = 2,
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
    InlineNode = 8,
    /// A consolidated page replacement, stored blobly
    BlobNode = 9,
    /// A partial page update, stored inline
    InlineLink = 10,
    /// A partial page update, stored blobly
    BlobLink = 11,
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
            1 => Canceled,
            2 => Cap,
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
    match update {
        Update::Free => LogKind::Free,
        Update::Link(..) => LogKind::Link,
        Update::Node(..) | Update::Counter(..) | Update::Meta(..) => {
            LogKind::Replace
        }
    }
}

impl From<MessageKind> for LogKind {
    fn from(kind: MessageKind) -> Self {
        match kind {
            MessageKind::Free => LogKind::Free,
            MessageKind::InlineNode
            | MessageKind::Counter
            | MessageKind::BlobNode
            | MessageKind::InlineMeta
            | MessageKind::BlobMeta => LogKind::Replace,
            MessageKind::InlineLink | MessageKind::BlobLink => LogKind::Link,
            MessageKind::Canceled
            | MessageKind::Cap
            | MessageKind::BatchManifest => LogKind::Skip,
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
    let mut current = atomic_lsn.load(Acquire);
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

#[inline]
pub(crate) fn lsn_to_arr(number: Lsn) -> [u8; 8] {
    number.to_le_bytes()
}

#[inline]
pub(crate) fn arr_to_lsn(arr: &[u8]) -> Lsn {
    Lsn::from_le_bytes(arr.try_into().unwrap())
}

#[inline]
pub(crate) fn u64_to_arr(number: u64) -> [u8; 8] {
    number.to_le_bytes()
}

#[inline]
pub(crate) fn arr_to_u32(arr: &[u8]) -> u32 {
    u32::from_le_bytes(arr.try_into().unwrap())
}

#[inline]
pub(crate) fn u32_to_arr(number: u32) -> [u8; 4] {
    number.to_le_bytes()
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn maybe_decompress(in_buf: Vec<u8>) -> std::io::Result<Vec<u8>> {
    #[cfg(feature = "compression")]
    {
        use zstd::stream::decode_all;

        let scootable_in_buf = &mut &*in_buf;
        let _ivec_varint = u64::deserialize(scootable_in_buf)
            .expect("this had to be serialized with an extra length frame");
        let _measure = Measure::new(&M.decompress);
        let out_buf = decode_all(scootable_in_buf).expect(
            "failed to decompress data. \
             This is not expected, please open an issue on \
             https://github.com/spacejam/sled so we can \
             fix this critical issue ASAP. Thank you :)",
        );

        Ok(out_buf)
    }

    #[cfg(not(feature = "compression"))]
    Ok(in_buf)
}

#[derive(Debug, Clone, Copy)]
pub struct NodeView<'g>(pub(crate) PageView<'g>);

impl<'g> Deref for NodeView<'g> {
    type Target = Node;
    fn deref(&self) -> &Node {
        self.0.as_node()
    }
}

unsafe impl<'g> Send for NodeView<'g> {}
unsafe impl<'g> Sync for NodeView<'g> {}

#[derive(Debug, Clone, Copy)]
pub struct MetaView<'g>(PageView<'g>);

impl<'g> Deref for MetaView<'g> {
    type Target = Meta;
    fn deref(&self) -> &Meta {
        self.0.as_meta()
    }
}

unsafe impl<'g> Send for MetaView<'g> {}
unsafe impl<'g> Sync for MetaView<'g> {}

#[derive(Debug, Clone, Copy)]
pub struct PageView<'g> {
    pub(crate) read: Shared<'g, Page>,
    pub(crate) entry: &'g Atomic<Page>,
}

unsafe impl<'g> Send for PageView<'g> {}
unsafe impl<'g> Sync for PageView<'g> {}

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
    pub log_size: u64,
}

#[cfg(test)]
impl quickcheck::Arbitrary for CacheInfo {
    fn arbitrary<G: quickcheck::Gen>(g: &mut G) -> CacheInfo {
        use rand::Rng;

        CacheInfo {
            ts: g.gen(),
            lsn: g.gen(),
            pointer: DiskPtr::arbitrary(g),
            log_size: g.gen(),
        }
    }
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
    fn as_node(&self) -> &Node {
        match self {
            Update::Node(node) => node,
            other => panic!("called as_node on non-Node: {:?}", other),
        }
    }

    fn as_node_mut(&mut self) -> &mut Node {
        match self {
            Update::Node(node) => node,
            other => panic!("called as_node_mut on non-Node: {:?}", other),
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
            meta
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
#[derive(Debug)]
pub struct RecoveryGuard<'a> {
    batch_res: Reservation<'a>,
}

impl<'a> RecoveryGuard<'a> {
    /// Writes the last LSN for a batch into an earlier
    /// reservation, releasing it.
    pub(crate) fn seal_batch(self) -> Result<()> {
        let max_reserved =
            self.batch_res.log.iobufs.max_reserved_lsn.load(Acquire);
        self.batch_res.mark_writebatch(max_reserved).map(|_| ())
    }
}

/// A page consists of a sequence of state transformations
/// with associated storage parameters like disk pos, lsn, time.
#[derive(Debug, Clone)]
pub struct Page {
    pub(crate) update: Option<Box<Update>>,
    pub(crate) cache_infos: Vec<CacheInfo>,
}

impl Page {
    pub(crate) fn to_page_state(&self) -> PageState {
        let base = &self.cache_infos[0];
        if self.is_free() {
            PageState::Free(base.lsn, base.pointer)
        } else {
            let mut frags: Vec<(Lsn, DiskPtr, u64)> = vec![];

            for cache_info in self.cache_infos.iter().skip(1) {
                frags.push((
                    cache_info.lsn,
                    cache_info.pointer,
                    cache_info.log_size,
                ));
            }

            PageState::Present {
                base: (base.lsn, base.pointer, base.log_size),
                frags,
            }
        }
    }

    pub(crate) fn as_node(&self) -> &Node {
        self.update.as_ref().unwrap().as_node()
    }

    pub(crate) fn as_meta(&self) -> &Meta {
        self.update.as_ref().unwrap().as_meta()
    }

    pub(crate) fn as_counter(&self) -> u64 {
        self.update.as_ref().unwrap().as_counter()
    }

    pub(crate) fn is_free(&self) -> bool {
        self.update.as_ref().map_or(false, |u| u.is_free())
            || self.cache_infos.is_empty()
    }

    pub(crate) fn last_lsn(&self) -> Lsn {
        self.cache_infos.last().map(|ci| ci.lsn).unwrap()
    }

    pub(crate) fn log_size(&self) -> u64 {
        self.cache_infos.iter().map(|ci| ci.log_size).sum()
    }

    fn ts(&self) -> u64 {
        self.cache_infos.last().map_or(0, |ci| ci.ts)
    }

    fn lone_blob(&self) -> Option<DiskPtr> {
        if self.cache_infos.len() == 1 && self.cache_infos[0].pointer.is_blob()
        {
            Some(self.cache_infos[0].pointer)
        } else {
            None
        }
    }
}

/// A lock-free pagecache which supports linkmented pages
/// for dramatically improving write throughput.
pub struct PageCache {
    pub(crate) config: RunningConfig,
    inner: PageTable,
    next_pid_to_allocate: Mutex<PageId>,
    free: Arc<Mutex<BinaryHeap<PageId>>>,
    #[doc(hidden)]
    pub log: Log,
    lru: Lru,
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
            *self.next_pid_to_allocate.lock(),
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
                self.get_meta(&guard)
                    .expect("should get meta under test")
                    .deref()
                    .clone(),
            );

            for pid in 0..*self.next_pid_to_allocate.lock() {
                let pte = if let Some(pte) = self.inner.get(pid, &guard) {
                    pte
                } else {
                    continue;
                };
                let pointers =
                    pte.cache_infos.iter().map(|ci| ci.pointer).collect();
                pages_before_restart.insert(pid, pointers);
            }

            self.config.event_log.pages_before_restart(pages_before_restart);
        }

        trace!("pagecache dropped");
    }
}

impl PageCache {
    /// Instantiate a new `PageCache`.
    pub(crate) fn start(config: RunningConfig) -> Result<Self> {
        trace!("starting pagecache");

        config.reset_global_error();

        // try to pull any existing snapshot off disk, and
        // apply any new data to it to "catch-up" the
        // snapshot before loading it.
        let snapshot = read_snapshot_or_default(&config)?;

        #[cfg(feature = "testing")]
        {
            // these checks are in place to catch non-idempotent
            // recovery which could trigger feedback loops and
            // emergent behavior.
            trace!(
                "\n\n~~~~ regenerating snapshot for idempotency test ~~~~\n"
            );

            let snapshot2 = read_snapshot_or_default(&config)
                .expect("second read snapshot");
            assert_eq!(
                snapshot.active_segment, snapshot2.active_segment,
                "snapshot active_segment diverged across recoveries.\n\n \
                first: {:?}\n\n
                second: {:?}\n\n",
                snapshot, snapshot2
            );
            assert_eq!(
                snapshot.stable_lsn, snapshot2.stable_lsn,
                "snapshot stable_lsn diverged across recoveries.\n\n \
                first: {:?}\n\n
                second: {:?}\n\n",
                snapshot, snapshot2
            );
            for (pid, (p1, p2)) in
                snapshot.pt.iter().zip(snapshot2.pt.iter()).enumerate()
            {
                assert_eq!(
                    p1, p2,
                    "snapshot pid {} diverged across recoveries.\n\n \
                first: {:?}\n\n
                second: {:?}\n\n",
                    pid, p1, p2
                );
            }
            assert_eq!(
                snapshot.pt.len(),
                snapshot2.pt.len(),
                "snapshots number of pages diverged across recoveries.\n\n \
                first: {:?}\n\n
                second: {:?}\n\n",
                snapshot.pt,
                snapshot2.pt
            );
            assert_eq!(
                snapshot, snapshot2,
                "snapshots diverged across recoveries.\n\n \
                first: {:?}\n\n
                second: {:?}\n\n",
                snapshot, snapshot2
            );
        }

        let _measure = Measure::new(&M.start_pagecache);

        let cache_capacity = config.cache_capacity;
        let lru = Lru::new(cache_capacity);

        let mut pc = Self {
            config: config.clone(),
            inner: PageTable::default(),
            next_pid_to_allocate: Mutex::new(0),
            free: Arc::new(Mutex::new(BinaryHeap::new())),
            log: Log::start(config, &snapshot)?,
            lru,
            idgen_persist_mu: Arc::new(Mutex::new(())),
            idgen: Arc::new(AtomicU64::new(0)),
            idgen_persists: Arc::new(AtomicU64::new(0)),
            was_recovered: false,
        };

        // now we read it back in
        pc.load_snapshot(&snapshot)?;

        #[cfg(feature = "testing")]
        {
            use std::collections::HashMap;

            // NB this must be before idgen/meta are initialized
            // because they may cas_page on initial page-in.
            let guard = pin();

            let mut pages_after_restart = HashMap::new();

            for pid in 0..*pc.next_pid_to_allocate.lock() {
                let pte = if let Some(pte) = pc.inner.get(pid, &guard) {
                    pte
                } else {
                    continue;
                };
                let pointers =
                    pte.cache_infos.iter().map(|ci| ci.pointer).collect();
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
                pc.get_meta(&guard)
                    .expect("should be able to get meta under test")
                    .deref()
                    .clone(),
            );
        }

        trace!("pagecache started");

        Ok(pc)
    }

    /// Flushes any pending IO buffers to disk to ensure durability.
    /// Returns the number of bytes written during this call.
    pub(crate) fn flush(&self) -> Result<usize> {
        self.log.flush()
    }

    /// Create a new page, trying to reuse old freed pages if possible
    /// to maximize underlying `PageTable` pointer density. Returns
    /// the page ID and its pointer for use in future atomic `replace`
    /// and `link` operations.
    pub(crate) fn allocate<'g>(
        &self,
        new: Node,
        guard: &'g Guard,
    ) -> Result<(PageId, PageView<'g>)> {
        self.allocate_inner(Update::Node(new), guard)
    }

    fn allocate_inner<'g>(
        &self,
        new: Update,
        guard: &'g Guard,
    ) -> Result<(PageId, PageView<'g>)> {
        let mut allocation_serializer;

        let free_opt = self.free.lock().pop();

        let (pid, page_view) = if let Some(pid) = free_opt {
            trace!("re-allocating pid {}", pid);

            let page_view = match self.inner.get(pid, guard) {
                None => panic!(
                    "expected to find existing stack \
                     for re-allocated pid {}",
                    pid
                ),
                Some(p) => p,
            };
            assert!(
                page_view.is_free(),
                "failed to re-allocate pid {} which \
                 contained unexpected state {:?}",
                pid,
                page_view,
            );
            (pid, page_view)
        } else {
            // we need to hold the allocation mutex because
            // we have to maintain the invariant that our
            // recoverable allocated pages will be contiguous.
            // If we did not hold this mutex, it would be
            // possible (especially under high thread counts)
            // to persist pages non-monotonically to disk,
            // which would break our recovery invariants.
            // While we could just remove that invariant,
            // because it is overly-strict, it allows us
            // to flag corruption and bugs during testing
            // much more easily.
            allocation_serializer = self.next_pid_to_allocate.lock();
            let pid = *allocation_serializer;
            *allocation_serializer += 1;

            trace!("allocating pid {} for the first time", pid);

            let new_page = Page { update: None, cache_infos: Vec::default() };

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

    /// Attempt to opportunistically rewrite data from a Draining
    /// segment of the file to help with space amplification.
    /// Returns Ok(true) if we had the opportunity to attempt to
    /// move a page. Returns Ok(false) if there were no pages
    /// to GC. Returns an Err if we encountered an IO problem
    /// while performing this GC.
    #[cfg(all(
        not(miri),
        any(
            windows,
            target_os = "linux",
            target_os = "macos",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
        )
    ))]
    pub(crate) fn attempt_gc(&self) -> Result<bool> {
        let guard = pin();
        let cc = concurrency_control::read();
        let to_clean = self.log.iobufs.segment_cleaner.pop();
        let ret = if let Some((pid_to_clean, segment_to_clean)) = to_clean {
            self.rewrite_page(pid_to_clean, segment_to_clean, &guard)
                .map(|_| true)
        } else {
            Ok(false)
        };
        drop(cc);
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
    pub(crate) fn pin_log(&self, guard: &Guard) -> Result<RecoveryGuard<'_>> {
        // HACK: we are rolling the io buffer before AND
        // after taking out the reservation pin to avoid
        // a deadlock where the batch reservation causes
        // writes to fail to flush to disk. in the future,
        // this may be addressed in a nicer way by representing
        // transactions with a begin and end message, rather
        // than a single beginning message that needs to
        // be held until we know the final batch LSN.
        self.log.roll_iobuf()?;

        let batch_res = self.log.reserve(
            LogKind::Skip,
            BATCH_MANIFEST_PID,
            &BatchManifest::default(),
            guard,
        )?;

        iobuf::maybe_seal_and_write_iobuf(
            &self.log.iobufs,
            &batch_res.iobuf,
            batch_res.iobuf.get_header(),
            false,
        )?;

        Ok(RecoveryGuard { batch_res })
    }

    #[doc(hidden)]
    #[cfg(feature = "failpoints")]
    #[cfg(all(
        not(miri),
        any(
            windows,
            target_os = "linux",
            target_os = "macos",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
        )
    ))]
    pub(crate) fn set_failpoint(&self, e: Error) {
        if let Error::FailPoint = e {
            self.config.set_global_error(e);

            // wake up any waiting threads
            // so they don't stall forever
            let intervals = self.log.iobufs.intervals.lock();

            // having held the mutex makes this linearized
            // with the notify below.
            drop(intervals);

            let _notified = self.log.iobufs.interval_updated.notify_all();
        }
    }

    /// Free a particular page.
    pub(crate) fn free<'g>(
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
    pub(crate) fn link<'g>(
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
                if let Some(current_pointer) = self.get(pid, guard)? {
                    return Ok(Err(Some((current_pointer.0, new))));
                } else {
                    return Ok(Err(None));
                }
            }
        }

        let mut node: Node = old.as_node().clone();
        node.apply(&new);

        // see if we should short-circuit replace
        if old.cache_infos.len() >= PAGE_CONSOLIDATION_THRESHOLD {
            let short_circuit = self.replace(pid, old, node, guard)?;
            return Ok(short_circuit.map_err(|a| a.map(|b| (b.0, new))));
        }

        let mut new_page = Some(Owned::new(Page {
            update: Some(Box::new(Update::Node(node))),
            cache_infos: Vec::default(),
        }));

        loop {
            // TODO handle replacement on threshold here instead

            let log_reservation =
                self.log.reserve(LogKind::Link, pid, &new, guard)?;
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
            let ts = old.ts() + 1;

            let cache_info = CacheInfo {
                lsn,
                pointer,
                ts,
                log_size: log_reservation.reservation_len() as u64,
            };

            let mut new_cache_infos =
                Vec::with_capacity(old.cache_infos.len() + 1);
            new_cache_infos.extend_from_slice(&old.cache_infos);
            new_cache_infos.push(cache_info);

            let mut page_ptr = new_page.take().unwrap();
            page_ptr.cache_infos = new_cache_infos;

            debug_delay();
            let result =
                old.entry.compare_and_set(old.read, page_ptr, SeqCst, guard);

            match result {
                Ok(new_shared) => {
                    trace!("link of pid {} succeeded", pid);

                    unsafe {
                        guard.defer_destroy(old.read);
                    }

                    assert_ne!(old.last_lsn(), 0);

                    self.log.iobufs.sa_mark_link(pid, cache_info, guard);

                    // NB complete must happen AFTER calls to SA, because
                    // when the iobuf's n_writers hits 0, we may transition
                    // the segment to inactive, resulting in a race otherwise.
                    // FIXME can result in deadlock if a node that holds SA
                    // is waiting to acquire a new reservation blocked by this?
                    log_reservation.complete()?;

                    // possibly evict an item now that our cache has grown
                    let total_page_size =
                        unsafe { new_shared.deref().log_size() };
                    let to_evict =
                        self.lru.accessed(pid, total_page_size, guard);
                    trace!(
                        "accessed pid {} -> paging out pids {:?}",
                        pid,
                        to_evict
                    );
                    if !to_evict.is_empty() {
                        self.page_out(to_evict, guard)?;
                    }

                    old.read = new_shared;

                    return Ok(Ok(old));
                }
                Err(cas_error) => {
                    log_reservation.abort()?;
                    let actual = cas_error.current;
                    let actual_ts = unsafe { actual.deref().ts() };
                    if actual_ts == old.ts() {
                        trace!(
                            "link of pid {} failed due to movement, retrying",
                            pid
                        );
                        new_page = Some(cas_error.new);

                        old.read = actual;
                    } else {
                        trace!("link of pid {} failed due to new update", pid);
                        let mut page_view = old;
                        page_view.read = actual;
                        return Ok(Err(Some((page_view, new))));
                    }
                }
            }
        }
    }

    /// Node an existing page with a different set of `PageLink`s.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns
    /// `Err(Some(actual_key))` if the atomic swap fails.
    pub(crate) fn replace<'g>(
        &self,
        pid: PageId,
        old: PageView<'g>,
        new: Node,
        guard: &'g Guard,
    ) -> Result<CasResult<'g, Node>> {
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
                if let Some(current_pointer) = self.get(pid, guard)? {
                    return Ok(Err(Some((current_pointer.0, new))));
                } else {
                    return Ok(Err(None));
                }
            }
        }

        let result =
            self.cas_page(pid, old, Update::Node(new), false, guard)?;

        if let Some((pid_to_clean, segment_to_clean)) =
            self.log.iobufs.segment_cleaner.pop()
        {
            self.rewrite_page(pid_to_clean, segment_to_clean, guard)?;
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
    // segment has had enough resident page replacements moved
    // away to trigger the `segment_cleanup_threshold`.
    fn rewrite_page(
        &self,
        pid: PageId,
        segment_to_purge: LogOffset,
        guard: &Guard,
    ) -> Result<()> {
        let _measure = Measure::new(&M.rewrite_page);

        trace!("rewriting pid {}", pid);

        let purge_segment_id =
            segment_to_purge / self.config.segment_size as u64;

        loop {
            let page_view = if let Some(page_view) = self.inner.get(pid, guard)
            {
                page_view
            } else {
                panic!("rewriting pid {} failed (no longer exists)", pid);
            };

            let already_moved = !unsafe { page_view.read.deref() }
                .cache_infos
                .iter()
                .any(|ce| {
                    ce.pointer.lid() / self.config.segment_size as u64
                        == purge_segment_id
                });
            if already_moved {
                return Ok(());
            }

            // if the page is just a single blob pointer, rewrite it.
            if let Some(disk_pointer) = page_view.lone_blob() {
                trace!("rewriting blob with pid {}", pid);
                let blob_pointer = disk_pointer.blob().1;

                let log_reservation =
                    self.log.rewrite_blob_pointer(pid, blob_pointer, guard)?;

                let cache_info = CacheInfo {
                    ts: page_view.ts(),
                    lsn: log_reservation.lsn,
                    pointer: log_reservation.pointer,
                    log_size: u64::try_from(log_reservation.reservation_len())
                        .unwrap(),
                };

                let new_page = Owned::new(Page {
                    update: page_view.update.clone(),
                    cache_infos: vec![cache_info],
                });

                debug_delay();
                let result = page_view.entry.compare_and_set(
                    page_view.read,
                    new_page,
                    SeqCst,
                    guard,
                );

                if let Ok(new_shared) = result {
                    unsafe {
                        guard.defer_destroy(page_view.read);
                    }

                    let lsn = log_reservation.lsn();

                    self.log.iobufs.sa_mark_replace(
                        pid,
                        lsn,
                        &page_view.cache_infos,
                        cache_info,
                        guard,
                    )?;

                    // NB complete must happen AFTER calls to SA, because
                    // when the iobuf's n_writers hits 0, we may transition
                    // the segment to inactive, resulting in a race otherwise.
                    let _pointer = log_reservation.complete()?;

                    // possibly evict an item now that our cache has grown
                    let total_page_size =
                        unsafe { new_shared.deref().log_size() };
                    let to_evict =
                        self.lru.accessed(pid, total_page_size, guard);
                    trace!(
                        "accessed pid {} -> paging out pids {:?}",
                        pid,
                        to_evict
                    );
                    if !to_evict.is_empty() {
                        self.page_out(to_evict, guard)?;
                    }

                    trace!("rewriting pid {} succeeded", pid);

                    return Ok(());
                } else {
                    let _pointer = log_reservation.abort()?;

                    trace!("rewriting pid {} failed", pid);
                }
            } else {
                trace!("rewriting page with pid {}", pid);

                // page-in whole page with a get
                let (key, update): (_, Update) = if pid == META_PID {
                    let meta_view = self.get_meta(guard)?;
                    (meta_view.0, Update::Meta(meta_view.deref().clone()))
                } else if pid == COUNTER_PID {
                    let (key, counter) = self.get_idgen(guard)?;
                    (key, Update::Counter(counter))
                } else if let Some(node_view) = self.get(pid, guard)? {
                    (node_view.0, Update::Node(node_view.deref().clone()))
                } else {
                    let page_view = match self.inner.get(pid, guard) {
                        None => panic!("expected page missing in rewrite"),
                        Some(p) => p,
                    };

                    if page_view.is_free() {
                        (page_view, Update::Free)
                    } else {
                        debug!(
                            "when rewriting pid {} \
                             we encountered a rewritten \
                             node with a link {:?} that \
                             we previously witnessed a Free \
                             for (PageCache::get returned None), \
                             assuming we can just return now since \
                             the Free was replace'd",
                            pid, page_view.update
                        );
                        return Ok(());
                    }
                };

                let res = self.cas_page(pid, key, update, true, guard).map(
                    |res| {
                        trace!(
                            "rewriting pid {} success: {}",
                            pid,
                            res.is_ok()
                        );
                        res
                    },
                )?;
                if res.is_ok() {
                    return Ok(());
                }
            }
        }
    }

    /// Traverses all files and calculates their total physical
    /// size, then traverses all pages and calculates their
    /// total logical size, then divides the physical size
    /// by the logical size.
    #[allow(clippy::cast_precision_loss)]
    #[allow(clippy::float_arithmetic)]
    #[doc(hidden)]
    pub(crate) fn space_amplification(&self) -> Result<f64> {
        let on_disk_bytes = self.size_on_disk()? as f64;
        let logical_size = (self.logical_size_of_all_pages()?
            + self.config.segment_size as u64)
            as f64;

        Ok(on_disk_bytes / logical_size)
    }

    pub(crate) fn size_on_disk(&self) -> Result<u64> {
        let mut size = self.config.file.metadata()?.len();

        let stable = self.config.blob_path(0);
        let blob_dir = stable.parent().expect(
            "should be able to determine the parent for the blob directory",
        );
        let blob_files = std::fs::read_dir(blob_dir)?;

        for blob_file in blob_files {
            let blob_file = if let Ok(bf) = blob_file {
                bf
            } else {
                continue;
            };

            // it's possible the blob file was removed lazily
            // in the background and no longer exists
            #[cfg(not(miri))]
            {
                size += blob_file.metadata().map(|m| m.len()).unwrap_or(0);
            }

            // workaround to avoid missing `dirfd` shim
            #[cfg(miri)]
            {
                size += std::fs::metadata(blob_file.path())
                    .map(|m| m.len())
                    .unwrap_or(0);
            }
        }

        Ok(size)
    }

    fn logical_size_of_all_pages(&self) -> Result<u64> {
        let guard = pin();
        let meta_size = self.get_meta(&guard)?.rss();
        let idgen_size = std::mem::size_of::<u64>() as u64;

        let mut ret = meta_size + idgen_size;
        let min_pid = COUNTER_PID + 1;
        let next_pid_to_allocate = *self.next_pid_to_allocate.lock();
        for pid in min_pid..next_pid_to_allocate {
            if let Some(node_cell) = self.get(pid, &guard)? {
                ret += node_cell.rss();
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
            old.ts()
        );

        let log_kind = log_kind_from_update(&update);
        trace!("cas_page on pid {} has log kind: {:?}", pid, log_kind);

        let mut new_page = Some(Owned::new(Page {
            update: Some(Box::new(update)),
            cache_infos: Vec::default(),
        }));

        loop {
            let mut page_ptr = new_page.take().unwrap();
            let log_reservation = match &**page_ptr.update.as_ref().unwrap() {
                Update::Counter(ref c) => {
                    self.log.reserve(log_kind, pid, c, guard)?
                }
                Update::Meta(ref m) => {
                    self.log.reserve(log_kind, pid, m, guard)?
                }
                Update::Free => self.log.reserve(log_kind, pid, &(), guard)?,
                Update::Node(ref node) => {
                    self.log.reserve(log_kind, pid, node, guard)?
                }
                other => {
                    panic!("non-replacement used in cas_page: {:?}", other)
                }
            };
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
            let ts = if is_rewrite { old.ts() } else { old.ts() + 1 };

            let cache_info = CacheInfo {
                ts,
                lsn,
                pointer: new_pointer,
                log_size: u64::try_from(log_reservation.reservation_len())
                    .unwrap(),
            };

            page_ptr.cache_infos = vec![cache_info];

            debug_delay();
            let result =
                old.entry.compare_and_set(old.read, page_ptr, SeqCst, guard);

            match result {
                Ok(new_shared) => {
                    unsafe {
                        guard.defer_destroy(old.read);
                    }

                    trace!("cas_page succeeded on pid {}", pid);
                    self.log.iobufs.sa_mark_replace(
                        pid,
                        lsn,
                        &old.cache_infos,
                        cache_info,
                        guard,
                    )?;

                    // NB complete must happen AFTER calls to SA, because
                    // when the iobuf's n_writers hits 0, we may transition
                    // the segment to inactive, resulting in a race otherwise.
                    let _pointer = log_reservation.complete()?;

                    // possibly evict an item now that our cache has grown
                    let total_page_size =
                        unsafe { new_shared.deref().log_size() };
                    let to_evict =
                        self.lru.accessed(pid, total_page_size, guard);
                    trace!(
                        "accessed pid {} -> paging out pids {:?}",
                        pid,
                        to_evict
                    );
                    if !to_evict.is_empty() {
                        self.page_out(to_evict, guard)?;
                    }

                    return Ok(Ok(PageView {
                        read: new_shared,
                        entry: old.entry,
                    }));
                }
                Err(cas_error) => {
                    trace!("cas_page failed on pid {}", pid);
                    let _pointer = log_reservation.abort()?;

                    let current: Shared<'_, _> = cas_error.current;
                    let actual_ts = unsafe { current.deref().ts() };

                    let mut returned_update: Owned<_> = cas_error.new;

                    if actual_ts != old.ts() || is_rewrite {
                        return Ok(Err(Some((
                            PageView { read: current, entry: old.entry },
                            *returned_update.update.take().unwrap(),
                        ))));
                    }
                    trace!(
                        "retrying CAS on pid {} with same ts of {}",
                        pid,
                        old.ts()
                    );
                    old.read = current;
                    new_page = Some(returned_update);
                }
            } // match cas result
        } // loop
    }

    /// Retrieve the current meta page
    pub(crate) fn get_meta<'g>(
        &self,
        guard: &'g Guard,
    ) -> Result<MetaView<'g>> {
        trace!("getting page iter for META");

        let page_view = match self.inner.get(META_PID, guard) {
            None => {
                return Err(Error::ReportableBug(
                    "failed to retrieve META page \
                     which should always be present"
                        .into(),
                ));
            }
            Some(p) => p,
        };

        if page_view.update.is_some() {
            Ok(MetaView(page_view))
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

        let page_view = match self.inner.get(COUNTER_PID, guard) {
            None => {
                return Err(Error::ReportableBug(
                    "failed to retrieve counter page \
                     which should always be present"
                        .into(),
                ));
            }
            Some(p) => p,
        };

        if page_view.update.is_some() {
            let counter = page_view.as_counter();
            Ok((page_view, counter))
        } else {
            Err(Error::ReportableBug(
                "failed to retrieve counter page \
                 which should always be present"
                    .into(),
            ))
        }
    }

    /// Try to retrieve a page by its logical ID.
    pub(crate) fn get<'g>(
        &self,
        pid: PageId,
        guard: &'g Guard,
    ) -> Result<Option<NodeView<'g>>> {
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

        let mut last_attempted_cache_info = None;
        let mut last_err = None;
        let mut page_view;

        let mut updates: Vec<Update> = loop {
            // we loop here because if the page we want to
            // pull is moved, we want to retry. but if we
            // get a corruption and then
            page_view = match self.inner.get(pid, guard) {
                None => return Ok(None),
                Some(p) => p,
            };

            if page_view.is_free() {
                return Ok(None);
            }

            if page_view.update.is_some() {
                // possibly evict an item now that our cache has grown
                let total_page_size = page_view.log_size();
                let to_evict = self.lru.accessed(pid, total_page_size, guard);
                trace!(
                    "accessed pid {} -> paging out pids {:?}",
                    pid,
                    to_evict
                );
                if !to_evict.is_empty() {
                    self.page_out(to_evict, guard)?;
                }
                return Ok(Some(NodeView(page_view)));
            }

            trace!(
                "pulling pid {} view {:?} deref {:?}",
                pid,
                page_view,
                page_view.deref()
            );
            if page_view.cache_infos.first()
                == last_attempted_cache_info.as_ref()
            {
                return Err(last_err.unwrap());
            } else {
                last_attempted_cache_info =
                    page_view.cache_infos.first().copied();
            }

            // need to page-in
            let updates_result: Result<Vec<Update>> = page_view
                .cache_infos
                .iter()
                .map(|ci| self.pull(pid, ci.lsn, ci.pointer))
                .collect();

            last_err = if let Ok(updates) = updates_result {
                break updates;
            } else {
                Some(updates_result.unwrap_err())
            };
        };

        let (base_slice, links) = updates.split_at_mut(1);

        let base: &mut Node = base_slice[0].as_node_mut();

        for link_update in links {
            let link: &Link = link_update.as_link();
            base.apply(link);
        }

        updates.truncate(1);
        let base = updates.pop().unwrap();

        let page = Owned::new(Page {
            update: Some(Box::new(base)),
            cache_infos: page_view.cache_infos.clone(),
        });

        debug_delay();
        let result = page_view.entry.compare_and_set(
            page_view.read,
            page,
            SeqCst,
            guard,
        );

        if let Ok(new_shared) = result {
            trace!("fix-up for pid {} succeeded", pid);

            unsafe {
                guard.defer_destroy(page_view.read);
            }

            // possibly evict an item now that our cache has grown
            let total_page_size = unsafe { new_shared.deref().log_size() };
            let to_evict = self.lru.accessed(pid, total_page_size, guard);
            trace!("accessed pid {} -> paging out pids {:?}", pid, to_evict);
            if !to_evict.is_empty() {
                self.page_out(to_evict, guard)?;
            }

            let mut page_view = page_view;
            page_view.read = new_shared;

            Ok(Some(NodeView(page_view)))
        } else {
            trace!("fix-up for pid {} failed", pid);

            self.get(pid, guard)
        }
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
    pub(crate) fn generate_id_inner(&self) -> Result<u64> {
        let ret = self.idgen.fetch_add(1, Release);

        trace!("generating ID {}", ret);

        let interval = self.config.idgen_persist_interval;
        let necessary_persists = ret / interval * interval;
        let mut persisted = self.idgen_persists.load(Acquire);

        while persisted < necessary_persists {
            let _mu = self.idgen_persist_mu.lock();
            persisted = self.idgen_persists.load(Acquire);
            if persisted < necessary_persists {
                // it's our responsibility to persist up to our ID
                trace!(
                    "persisting ID gen, as persist count {} \
                    is below necessary persists {}",
                    persisted,
                    necessary_persists
                );
                let guard = pin();
                let (key, current) = self.get_idgen(&guard)?;

                assert_eq!(current, persisted);

                let counter_update = Update::Counter(necessary_persists);

                let old = self.idgen_persists.swap(necessary_persists, Release);
                assert_eq!(old, persisted);

                if self
                    .cas_page(COUNTER_PID, key, counter_update, false, &guard)?
                    .is_err()
                {
                    // CAS failed
                    continue;
                }

                // during recovery we add 2x the interval. we only
                // need to block if the last one wasn't stable yet.
                // we only call make_durable instead of make_stable
                // because we took out the initial reservation
                // outside of a writebatch (guaranteed by using the reader
                // concurrency control) and it's possible we
                // could cyclically wait if the reservation for
                // a replacement happened inside a writebatch.
                iobuf::make_durable(&self.log.iobufs, key.last_lsn())?;
            }
        }

        Ok(ret)
    }

    /// Look up a `PageId` for a given identifier in the `Meta`
    /// mapping. This is pretty cheap, but in some cases
    /// you may prefer to maintain your own atomic references
    /// to collection roots instead of relying on this. See
    /// sled's `Tree` root tracking for an example of
    /// avoiding this in a lock-free way that handles
    /// various race conditions.
    pub(crate) fn meta_pid_for_name(
        &self,
        name: &[u8],
        guard: &Guard,
    ) -> Result<PageId> {
        let m = self.get_meta(guard)?;
        if let Some(root) = m.get_root(name) {
            Ok(root)
        } else {
            Err(Error::CollectionNotFound(name.into()))
        }
    }

    /// Compare-and-swap the `Meta` mapping for a given
    /// identifier.
    pub(crate) fn cas_root_in_meta<'g>(
        &self,
        name: &[u8],
        old: Option<PageId>,
        new: Option<PageId>,
        guard: &'g Guard,
    ) -> Result<std::result::Result<(), Option<PageId>>> {
        loop {
            let meta_view = self.get_meta(guard)?;

            let actual = meta_view.get_root(name);
            if actual != old {
                return Ok(Err(actual));
            }

            let mut new_meta = meta_view.deref().clone();
            if let Some(new) = new {
                new_meta.set_root(name.into(), new);
            } else {
                new_meta.del_root(name);
            }

            let new_meta_link = Update::Meta(new_meta);

            let res = self.cas_page(
                META_PID,
                meta_view.0,
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
        for pid in to_evict {
            if pid == COUNTER_PID
                || pid == META_PID
                || pid == BATCH_MANIFEST_PID
            {
                // should not page these suckas out
                continue;
            }
            loop {
                if let Some(page_view) = self.inner.get(pid, guard) {
                    if page_view.is_free() {
                        // don't page-out Freed suckas
                        break;
                    }
                    let new_page = Owned::new(Page {
                        update: None,
                        cache_infos: page_view.cache_infos.clone(),
                    });

                    debug_delay();
                    if page_view
                        .entry
                        .compare_and_set(
                            page_view.read,
                            new_page,
                            SeqCst,
                            guard,
                        )
                        .is_ok()
                    {
                        unsafe {
                            guard.defer_destroy(page_view.read);
                        }

                        break;
                    }
                    // keep looping until we page this sucka out
                }
            }
        }
        Ok(())
    }

    fn pull(&self, pid: PageId, lsn: Lsn, pointer: DiskPtr) -> Result<Update> {
        use MessageKind::*;

        trace!("pulling pid {} lsn {} pointer {} from disk", pid, lsn, pointer);
        let _measure = Measure::new(&M.pull);

        let expected_segment_number: SegmentNumber = SegmentNumber(
            u64::try_from(lsn).unwrap()
                / u64::try_from(self.config.segment_size).unwrap(),
        );

        let (header, bytes) = match self.log.read(pid, lsn, pointer) {
            Ok(LogRead::Inline(header, buf, _len)) => {
                assert_eq!(
                    header.pid, pid,
                    "expected pid {} on pull of pointer {}, \
                     but got {} instead",
                    pid, pointer, header.pid
                );
                assert_eq!(
                    header.segment_number, expected_segment_number,
                    "expected segment number {:?} on pull of pointer {}, \
                     but got segment number {:?} instead",
                    expected_segment_number, pointer, header.segment_number
                );
                Ok((header, buf))
            }
            Ok(LogRead::Blob(header, buf, _blob_pointer, _inline_len)) => {
                assert_eq!(
                    header.pid, pid,
                    "expected pid {} on pull of pointer {}, \
                     but got {} instead",
                    pid, pointer, header.pid
                );
                assert_eq!(
                    header.segment_number, expected_segment_number,
                    "expected segment number {:?} on pull of pointer {}, \
                     but got segment number {:?} instead",
                    expected_segment_number, pointer, header.segment_number
                );

                Ok((header, buf))
            }
            Ok(other) => {
                debug!("read unexpected page: {:?}", other);
                Err(Error::corruption(Some(pointer)))
            }
            Err(e) => {
                debug!("failed to read page: {:?}", e);
                Err(e)
            }
        }?;

        // We create this &mut &[u8] to assist the `Serializer`
        // implementation that incrementally consumes bytes
        // without taking ownership of them.
        let buf = &mut bytes.as_slice();

        let update_res = {
            let _deserialize_latency = Measure::new(&M.deserialize);

            match header.kind {
                Counter => u64::deserialize(buf).map(Update::Counter),
                BlobMeta | InlineMeta => {
                    Meta::deserialize(buf).map(Update::Meta)
                }
                BlobLink | InlineLink => {
                    Link::deserialize(buf).map(Update::Link)
                }
                BlobNode | InlineNode => {
                    Node::deserialize(buf).map(Update::Node)
                }
                Free => Ok(Update::Free),
                Corrupted | Canceled | Cap | BatchManifest => {
                    panic!("unexpected pull: {:?}", header.kind)
                }
            }
        };

        let update = update_res.expect("failed to deserialize data");

        // TODO this feels racy, test it better?
        if let Update::Free = update {
            Err(Error::ReportableBug(format!(
                "non-link/replace found in pull of pid {}",
                pid
            )))
        } else {
            Ok(update)
        }
    }

    fn load_snapshot(&mut self, snapshot: &Snapshot) -> Result<()> {
        let next_pid_to_allocate = snapshot.pt.len() as PageId;

        self.next_pid_to_allocate = Mutex::new(next_pid_to_allocate);

        debug!("load_snapshot loading pages from 0..{}", next_pid_to_allocate);
        for pid in 0..next_pid_to_allocate {
            let state = if let Some(state) =
                snapshot.pt.get(usize::try_from(pid).unwrap())
            {
                state
            } else {
                panic!(
                    "load_snapshot pid {} not found, despite being below the max pid {}",
                    pid, next_pid_to_allocate
                );
            };

            trace!("load_snapshot pid {} {:?}", pid, state);

            let mut cache_infos = Vec::default();

            let guard = pin();

            match *state {
                PageState::Present { base, ref frags } => {
                    cache_infos.push(CacheInfo {
                        lsn: base.0,
                        pointer: base.1,
                        log_size: base.2,
                        ts: 0,
                    });
                    for (lsn, pointer, sz) in frags {
                        let cache_info = CacheInfo {
                            lsn: *lsn,
                            pointer: *pointer,
                            log_size: *sz,
                            ts: 0,
                        };

                        cache_infos.push(cache_info);
                    }
                }
                PageState::Free(lsn, pointer) => {
                    // blow away any existing state
                    trace!("load_snapshot freeing pid {}", pid);
                    let cache_info = CacheInfo {
                        lsn,
                        pointer,
                        log_size: u64::try_from(MAX_MSG_HEADER_LEN).unwrap(),
                        ts: 0,
                    };
                    cache_infos.push(cache_info);
                    self.free.lock().push(pid);
                }
                _ => panic!("tried to load a {:?}", state),
            }

            // Set up new page
            trace!("installing page for pid {}", pid);

            let update = if pid == META_PID || pid == COUNTER_PID {
                let update =
                    self.pull(pid, cache_infos[0].lsn, cache_infos[0].pointer)?;
                Some(Box::new(update))
            } else if state.is_free() {
                Some(Box::new(Update::Free))
            } else {
                None
            };
            let page = Page { update, cache_infos };

            self.inner.insert(pid, page, &guard);
        }

        Ok(())
    }

    /// A snapshot is to recover the pageTable at a point in time.
    ///
    /// This is called fuzzy snapshot because while
    /// we are taking a snapshot, the ongoing inserts
    /// into the database will keep bumping up the Lsn.
    /// Therefore, the `stable_lsn` gives us the state
    /// of the world, when the snapshot was taken.
    #[allow(unused)]
    fn take_fuzzy_snapshot(self) -> Snapshot {
        let stable_lsn_now: Lsn = self.log.stable_offset();

        // This is how we determine the number of the pages we will snapshot.
        let pid_bound = *self.next_pid_to_allocate.lock();

        let pid_bound_usize = assert_usize(pid_bound);

        let mut page_states = Vec::<PageState>::with_capacity(pid_bound_usize);
        let guard = pin();
        for pid in 0..pid_bound {
            'inner: loop {
                if let Some(pg_view) = self.inner.get(pid, &guard) {
                    if pg_view.cache_infos.is_empty() {
                        // there is a benign race with the thread
                        // that is allocating this page. the allocating
                        // thread has not yet written the new page to disk,
                        // and it does not yet have any storage tracking
                        // information.
                        std::thread::yield_now();
                    } else {
                        let page_state = pg_view.to_page_state();
                        page_states.push(page_state);
                        break 'inner;
                    }
                } else {
                    // there is a benign race with the thread
                    // that bumped the next_pid_to_allocate
                    // atomic counter above. it has not yet
                    // installed the page that it is allocating.
                    std::thread::yield_now();
                }
            }
        }

        Snapshot {
            stable_lsn: Some(stable_lsn_now),
            active_segment: None,
            pt: page_states,
        }
    }
}

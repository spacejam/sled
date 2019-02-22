use std::{
    collections::BinaryHeap,
    ops::Deref,
    sync::{Arc, Mutex},
};

use pagetable::PageTable;
use sled_sync::{Owned, Shared};

use rayon::prelude::*;

use super::*;

type PagePtrInner<'g, P> = Shared<'g, Node<CacheEntry<P>>>;

/// A pointer to shared lock-free state bound by a pinned epoch's lifetime.
#[derive(Debug, Clone, PartialEq)]
pub struct PagePtr<'g, P>(PagePtrInner<'g, P>)
where
    P: 'static + Send;

impl<'g, P> PagePtr<'g, P>
where
    P: 'static + Send,
{
    /// Create a null `PagePtr`
    pub fn allocated() -> PagePtr<'g, P> {
        PagePtr(Shared::null())
    }

    /// Whether this pointer is Allocated
    pub fn is_allocated(&self) -> bool {
        self.0.is_null()
    }

    /// The last Lsn number for the head of this page
    pub fn last_lsn(&self) -> Lsn {
        unsafe { self.0.deref().deref().lsn() }
    }

    unsafe fn deref_merged_resident(&self) -> &'g P
    where
        P: Debug,
    {
        match self.0.deref().deref() {
            CacheEntry::MergedResident(m, ..) => m,
            other => {
                panic!("called deref_merged_resident on {:?}", other);
            }
        }
    }
}

unsafe impl<'g, P> Send for PagePtr<'g, P> where P: Send {}
unsafe impl<'g, P> Sync for PagePtr<'g, P> where P: Send + Sync {}

/// Points to either a memory location or a disk location to page-in data from.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheEntry<M: Send> {
    /// A cache item that contains the most recent fully-merged page state, also in secondary
    /// storage.
    MergedResident(M, Lsn, DiskPtr),
    /// A cache item that is in memory, and also in secondary storage.
    Resident(M, Lsn, DiskPtr),
    /// A cache item that is present in secondary storage.
    PartialFlush(Lsn, DiskPtr),
    /// A cache item that is present in secondary storage, and is the base segment
    /// of a page.
    Flush(Lsn, DiskPtr),
    /// A freed page tombstone.
    Free(Lsn, DiskPtr),
    /// An allocated page that doesn't have user data linked/replaced into it yet
    Allocated(Lsn, DiskPtr),
}

impl<M: Send> CacheEntry<M> {
    fn ptr(&self) -> DiskPtr {
        use self::CacheEntry::*;

        match self {
            MergedResident(_, _, ptr)
            | Resident(_, _, ptr)
            | PartialFlush(_, ptr)
            | Flush(_, ptr)
            | Free(_, ptr)
            | Allocated(_, ptr) => *ptr,
        }
    }

    fn lsn(&self) -> Lsn {
        use self::CacheEntry::*;

        match self {
            MergedResident(_, lsn, ..)
            | Resident(_, lsn, ..)
            | PartialFlush(lsn, ..)
            | Flush(lsn, ..)
            | Free(lsn, ..)
            | Allocated(lsn, ..) => *lsn,
        }
    }

    fn ptr_ref_mut(&mut self) -> &mut DiskPtr {
        use self::CacheEntry::*;

        match self {
            MergedResident(_, _, ptr)
            | Resident(_, _, ptr)
            | PartialFlush(_, ptr)
            | Flush(_, ptr)
            | Free(_, ptr)
            | Allocated(_, ptr) => ptr,
        }
    }

    fn is_free(&self) -> bool {
        use self::CacheEntry::*;

        if let Free(..) = self {
            true
        } else {
            false
        }
    }
}

/// `LoggedUpdate` is for writing blocks of `Update`'s to disk
/// sequentially, to reduce IO during page reads.
#[serde(bound(deserialize = ""))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct LoggedUpdate<PageFrag>
where
    PageFrag: Serialize + DeserializeOwned,
{
    pub(super) pid: PageId,
    pub(super) update: Update<PageFrag>,
}

#[serde(bound(deserialize = ""))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum Update<PageFrag>
where
    PageFrag: DeserializeOwned + Serialize,
{
    Append(PageFrag),
    Compact(PageFrag),
    Free,
    Allocate,
}

/// The result of a `get` call in the `PageCache`.
#[derive(Clone, Debug, PartialEq)]
pub enum PageGet<'a, PageFrag>
where
    PageFrag: 'static + DeserializeOwned + Serialize + Send + Sync,
{
    /// This page contains data and has been prepared
    /// for presentation to the caller by the `PageCache`'s
    /// `Materializer`.
    Materialized(&'a PageFrag, PagePtr<'a, PageFrag>),
    /// This page has been Freed
    Free(PagePtr<'a, PageFrag>),
    /// This page has been allocated, but will become
    /// Free after restarting the system unless some
    /// data gets written to it.
    Allocated(PagePtr<'a, PageFrag>),
    /// This page was never allocated.
    Unallocated,
}

unsafe impl<'a, P> Send for PageGet<'a, P> where
    P: DeserializeOwned + Serialize + Send + Sync
{
}

unsafe impl<'a, P> Sync for PageGet<'a, P> where
    P: DeserializeOwned + Serialize + Send + Sync
{
}

impl<'a, P> PageGet<'a, P>
where
    P: DeserializeOwned + Serialize + Send + Sync,
{
    /// unwraps the `PageGet` into its inner `Materialized`
    /// form.
    ///
    /// # Panics
    /// Panics if it is a variant other than Materialized.
    pub fn unwrap(self) -> (&'a P, PagePtr<'a, P>) {
        match self {
            PageGet::Materialized(pr, hptr) => (pr, hptr),
            _ => panic!("unwrap called on non-Materialized"),
        }
    }

    /// unwraps the `PageGet` into its inner `Materialized`
    /// form, or panics with the specified error message.
    ///
    /// # Panics
    /// Panics if it is a variant other than Materialized.
    pub fn expect(
        self,
        msg: &'static str,
    ) -> (&'a P, PagePtr<'a, P>) {
        match self {
            PageGet::Materialized(pr, hptr) => (pr, hptr),
            _ => panic!(msg),
        }
    }

    /// Returns true if the `PageGet` is `Materialized`.
    pub fn is_materialized(&self) -> bool {
        match *self {
            PageGet::Materialized(..) => true,
            _ => false,
        }
    }

    /// Returns true if the `PageGet` is `Free`.
    pub fn is_free(&self) -> bool {
        match *self {
            PageGet::Free(_) => true,
            _ => false,
        }
    }

    /// Returns true if the `PageGet` is `Allocated`.
    pub fn is_allocated(&self) -> bool {
        match *self {
            PageGet::Allocated(_) => true,
            _ => false,
        }
    }

    /// Returns true if the `PageGet` is `Unallocated`.
    pub fn is_unallocated(&self) -> bool {
        match *self {
            PageGet::Unallocated => true,
            _ => false,
        }
    }

    fn into_ptr(self) -> PagePtr<'a, P> {
        match self {
            PageGet::Materialized(_, ptr)
            | PageGet::Free(ptr)
            | PageGet::Allocated(ptr) => ptr,
            PageGet::Unallocated => {
                panic!("into_ptr called on PageGet::Unallocated")
            }
        }
    }
}

/// A lock-free pagecache which supports fragmented pages
/// for dramatically improving write throughput.
///
/// # Working with the `PageCache`
///
/// ```
/// use pagecache::{pin, Materializer};
///
/// pub struct TestMaterializer;
///
/// impl Materializer for TestMaterializer {
///     // The possibly fragmented page, written to log storage sequentially, and
///     // read in parallel from multiple locations on disk when serving
///     // a request to read the page. These will be merged to a single version
///     // at read time, and possibly cached.
///     type PageFrag = String;
///
///     // The state returned by a call to `PageCache::recover`, as
///     // described by `Materializer::recover`
///     type Recovery = ();
///
///     // Create a new `Materializer` with the previously recovered
///     // state if any existed.
///     fn new(config: pagecache::Config, last_recovery: &Option<Self::Recovery>) -> Self {
///         TestMaterializer
///     }
///
///     // Used to merge chains of partial pages into a form
///     // that is useful for the `PageCache` owner.
///     fn merge<'a, I>(&'a self, frags: I) -> Self::PageFrag
///     where
///         I: IntoIterator<Item = &'a Self::PageFrag>,
///     {
///         frags.into_iter().fold(String::new(), |mut acc, ref s| {
///             acc.push_str(&*s);
///             acc
///         })
///     }
///
///     // Used to feed custom recovery information back to a higher-level abstraction
///     // during startup. For example, a B-Link tree must know what the current
///     // root node is before it can start serving requests.
///     fn recover(&self, _: &Self::PageFrag) -> Option<Self::Recovery> {
///         None
///     }
///
///     // Used to determine the resident size for this item in cache.
///     fn size_in_bytes(&self, frag: &String) -> usize {
///         std::mem::size_of::<String>() + frag.as_bytes().len()
///     }
///
/// }
///
/// fn main() {
///     let config = pagecache::ConfigBuilder::new().temporary(true).build();
///     let pc: pagecache::PageCache<TestMaterializer, _, _> =
///         pagecache::PageCache::start(config).unwrap();
///     {
///         let guard = pin();
///         let id = pc.allocate(&guard).unwrap();
///         let mut key = pagecache::PagePtr::allocated();
///
///         // The first item in a page should be set using replace,
///         // which signals that this is the beginning of a new
///         // page history, and that any previous items associated
///         // with this page should be forgotten.
///         key = pc.replace(id, key, "a".to_owned(), &guard).unwrap();
///
///         // Subsequent atomic updates should be added with link.
///         key = pc.link(id, key, "b".to_owned(), &guard).unwrap();
///         key = pc.link(id, key, "c".to_owned(), &guard).unwrap();
///
///         // When getting a page, the provided `Materializer` is
///         // used to merge all pages together.
///         let (consolidated, key) = pc.get(id, &guard).unwrap().unwrap();
///
///         assert_eq!(*consolidated, "abc".to_owned());
///     }
/// }
/// ```
pub struct PageCache<PM, P, R>
where
    P: 'static + Send + Sync,
{
    t: Arc<PM>,
    config: Config,
    inner: Arc<PageTable<PageTableEntry<P>>>,
    max_pid: AtomicUsize,
    free: Arc<Mutex<BinaryHeap<PageId>>>,
    log: Log,
    lru: Lru,
    updates: AtomicUsize,
    last_snapshot: Arc<Mutex<Option<Snapshot<R>>>>,
}

struct PageTableEntry<P>
where
    P: 'static + Send + Sync,
{
    stack: Stack<CacheEntry<P>>,
    rts: AtomicUsize,
}

unsafe impl<PM, P, R> Send for PageCache<PM, P, R>
where
    PM: Send + Sync,
    P: 'static + Send + Sync,
    R: Send + Sync,
{
}

unsafe impl<PM, P, R> Sync for PageCache<PM, P, R>
where
    PM: Send + Sync,
    P: 'static + Send + Sync,
    R: Send + Sync,
{
}

impl<PM, P, R> Debug for PageCache<PM, P, R>
where
    PM: Send + Sync,
    P: Debug + Send + Sync,
    R: Debug + Send + Sync,
{
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        f.write_str(&*format!(
            "PageCache {{ max: {:?} free: {:?} }}\n",
            self.max_pid.load(SeqCst),
            self.free
        ))
    }
}

#[cfg(feature = "event_log")]
impl<PM, P, R> Drop for PageCache<PM, P, R>
where
    P: 'static + Send + Sync,
{
    fn drop(&mut self) {
        use std::collections::HashMap;
        let mut pages_before_restart: HashMap<PageId, Vec<DiskPtr>> =
            HashMap::new();

        let guard = pin();

        for pid in 0..self.max_pid.load(SeqCst) {
            let pte = self.inner.get(pid, &guard);
            if pte.is_none() {
                continue;
            }
            let head =
                unsafe { pte.unwrap().deref().stack.head(&guard) };
            let ptrs = ptrs_from_stack(head, &guard);
            pages_before_restart.insert(pid, ptrs);
        }

        self.config
            .event_log
            .pages_before_restart(pages_before_restart);
    }
}

impl<PM, P, R> PageCache<PM, P, R>
where
    PM: Materializer<PageFrag = P, Recovery = R>,
    PM: 'static + Send + Sync,
    P: 'static
        + Debug
        + Clone
        + Serialize
        + DeserializeOwned
        + Send
        + Sync,
    R: 'static
        + Debug
        + Clone
        + Serialize
        + DeserializeOwned
        + Send
        + Sync,
{
    /// Instantiate a new `PageCache`.
    pub fn start(config: Config) -> Result<PageCache<PM, P, R>, ()> {
        let cache_capacity = config.cache_capacity;
        let cache_shard_bits = config.cache_bits;
        let lru = Lru::new(cache_capacity, cache_shard_bits);

        // try to pull any existing snapshot off disk, and
        // apply any new data to it to "catch-up" the
        // snapshot before loading it.
        let snapshot = read_snapshot_or_default::<PM, P, R>(&config)?;

        let materializer =
            Arc::new(PM::new(config.clone(), &snapshot.recovery));

        let mut pc = PageCache {
            t: materializer,
            config: config.clone(),
            inner: Arc::new(PageTable::default()),
            max_pid: AtomicUsize::new(0),
            free: Arc::new(Mutex::new(BinaryHeap::new())),
            log: Log::start(config, snapshot.clone())?,
            lru,
            updates: AtomicUsize::new(0),
            last_snapshot: Arc::new(Mutex::new(Some(snapshot))),
        };

        // now we read it back in
        pc.load_snapshot();

        Ok(pc)
    }

    /// Flushes any pending IO buffers to disk to ensure durability.
    pub fn flush(&self) -> Result<(), ()> {
        self.log.flush()
    }

    /// Return the recovered state from the snapshot
    pub fn recovered_state(&self) -> Option<R> {
        let mu = match self.last_snapshot.lock() {
            Ok(mu) => mu,
            Err(_) => return None,
        };

        if let Some(ref snapshot) = *mu {
            snapshot.recovery.clone()
        } else {
            None
        }
    }

    /// Create a new page, trying to reuse old freed pages if possible
    /// to maximize underlying `Radix` pointer density.
    pub fn allocate<'g>(
        &self,
        guard: &'g Guard,
    ) -> Result<PageId, ()> {
        let (pid, mut key) = if let Some(pid) =
            self.free.lock().unwrap().pop()
        {
            trace!("re-allocating pid {}", pid);

            let key = match self.get(pid, guard)? {
                PageGet::Free(key) => key.0,
                other => {
                    panic!("reallocating page set to {:?}", other)
                }
            };

            (pid, key)
        } else {
            let pid = self.max_pid.fetch_add(1, SeqCst);

            trace!("allocating pid {}", pid);

            let new_stack = Stack::default();
            new_stack.push(CacheEntry::Allocated(
                Lsn::max_value(),
                DiskPtr::Inline(LogId::max_value()),
            ));

            let key = new_stack.head(guard);

            let new_pte = PageTableEntry {
                stack: new_stack,
                rts: AtomicUsize::new(0),
            };

            let pte_ptr = Owned::new(new_pte).into_shared(guard);

            self.inner.cas(pid, Shared::null(), pte_ptr, guard)
                .expect("allocating new page should never encounter existing data");

            (pid, key)
        };

        loop {
            // we need to loop because the pagecache may relocate
            // our page due to it being free
            match self.cas_page(pid, key, Update::Allocate, guard) {
                Ok(_) => break,
                Err(Error::CasFailed(Some(other))) => unsafe {
                    let o = other.0.deref().deref();
                    if o.is_free() {
                        key = other.0;
                        continue;
                    } else {
                        panic!(
                            "failed to install new Update::Allocate \
                             for new page because we encountered \
                             a non-Free: {:?}",
                            o,
                        );
                    }
                },
                Err(Error::CasFailed(None)) => {
                    panic!(
                        "failed to allocate pid {} due to \
                         the stack disappearing before we could \
                         install our Update::Allocate",
                        pid,
                    );
                }
                Err(other) => return Err(other.danger_cast()),
            }
        }

        Ok(pid)
    }

    /// Free a particular page.
    #[allow(clippy::needless_pass_by_value)]
    pub fn free<'g>(
        &self,
        pid: PageId,
        old: PagePtr<'g, P>,
        guard: &'g Guard,
    ) -> Result<(), Option<PagePtr<'g, P>>> {
        trace!("attempting to free pid {}", pid);

        self.cas_page(pid, old.0, Update::Free, guard)?;

        let free = self.free.clone();
        guard.defer(move || {
            let mut free = free.lock().unwrap();
            // panic if we double-freed a page
            if free.iter().any(|e| e == &pid) {
                panic!("page {} was double-freed", pid);
            }

            free.push(pid);
        });
        Ok(())
    }

    /// Try to atomically add a `PageFrag` to the page.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the atomic append fails.
    #[allow(clippy::needless_pass_by_value)]
    pub fn link<'g>(
        &self,
        pid: PageId,
        old: PagePtr<'g, P>,
        new: P,
        guard: &'g Guard,
    ) -> Result<PagePtr<'g, P>, Option<PagePtr<'g, P>>> {
        trace!("linking pid {}", pid);

        if old.is_allocated() {
            return self.replace(pid, old, new, guard);
        }

        let pte_ptr = match self.inner.get(pid, guard) {
            None => return Err(Error::CasFailed(None)),
            Some(p) => p,
        };

        let prepend: LoggedUpdate<P> = LoggedUpdate {
            pid,
            update: Update::Append(new),
        };

        let bytes =
            measure(&M.serialize, || serialize(&prepend).unwrap());
        let log_reservation =
            self.log.reserve(&bytes).map_err(|e| e.danger_cast())?;

        let new = if let Update::Append(new) = prepend.update {
            new
        } else {
            unreachable!()
        };

        let lsn = log_reservation.lsn();
        let ptr = log_reservation.ptr();

        let cache_entry = CacheEntry::Resident(new, lsn, ptr);

        debug_delay();
        let result = unsafe {
            pte_ptr.deref().stack.cap(old.0, cache_entry, guard)
        };

        if result.is_err() {
            trace!("link of pid {} failed", pid);
            log_reservation.abort().map_err(|e| e.danger_cast())?;
        } else {
            trace!("link of pid {} succeeded", pid);
            let skip_mark = {
                // if the last update for this page was also
                // sent to this segment, we can skip marking it
                let previous_head_lsn =
                    unsafe { old.0.deref().lsn() };

                assert_ne!(previous_head_lsn, 0);

                let previous_lsn_segment = previous_head_lsn
                    / self.config.io_buf_size as i64;
                let new_lsn_segment =
                    lsn / self.config.io_buf_size as i64;

                previous_lsn_segment == new_lsn_segment
            };
            let to_clean = if skip_mark {
                self.log.with_sa(|sa| {
                    sa.mark_link(pid, lsn, ptr);
                    sa.clean(pid)
                })
            } else {
                self.log.with_sa(|sa| {
                    sa.mark_link(pid, lsn, ptr);
                    sa.clean(pid)
                })
            };

            // NB complete must happen AFTER calls to SA, because
            // when the iobuf's n_writers hits 0, we may transition
            // the segment to inactive, resulting in a race otherwise.
            // FIXME can result in deadlock if a node that holds SA
            // is waiting to acquire a new reservation blocked by this?
            log_reservation
                .complete()
                .map_err(|e| e.danger_cast())?;

            if let Some(to_clean) = to_clean {
                match self.rewrite_page(to_clean, guard) {
                    Ok(_) => {}
                    Err(Error::CasFailed(_)) => {}
                    other => other?,
                }
            }

            let count = self.updates.fetch_add(1, SeqCst) + 1;
            let should_snapshot =
                count % self.config.snapshot_after_ops == 0;
            if should_snapshot {
                self.advance_snapshot()
                    .map_err(|e| e.danger_cast())?;
            }
        }

        result
            .map(PagePtr)
            .map_err(|e| Error::CasFailed(Some(PagePtr(e))))
    }

    /// Replace an existing page with a different set of `PageFrag`s.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the atomic swap fails.
    #[allow(clippy::needless_pass_by_value)]
    pub fn replace<'g>(
        &self,
        pid: PageId,
        old: PagePtr<'g, P>,
        new: P,
        guard: &'g Guard,
    ) -> Result<PagePtr<'g, P>, Option<PagePtr<'g, P>>> {
        trace!("replacing pid {}", pid);

        let result =
            self.cas_page(pid, old.0, Update::Compact(new), guard);

        if result.is_ok() {
            let to_clean = self.log.with_sa(|sa| sa.clean(pid));

            if let Some(to_clean) = to_clean {
                assert_ne!(pid, to_clean);
                match self.rewrite_page(to_clean, guard) {
                    Ok(_) => {}
                    Err(Error::CasFailed(_)) => {}
                    other => other?,
                }
            }

            let count = self.updates.fetch_add(1, SeqCst) + 1;
            let should_snapshot =
                count % self.config.snapshot_after_ops == 0;
            if should_snapshot {
                self.advance_snapshot()
                    .map_err(|e| e.danger_cast())?;
            }
        }

        result.map(PagePtr)
    }

    // rewrite a page so we can reuse the segment that it is
    // (at least partially) located in. This happens when a
    // segment has had enough resident page fragments moved
    // away to trigger the `segment_cleanup_threshold`.
    fn rewrite_page<'g>(
        &self,
        pid: PageId,
        guard: &'g Guard,
    ) -> Result<(), Option<PagePtr<'g, P>>> {
        let _measure = Measure::new(&M.rewrite_page);

        let pte_ptr = match self.inner.get(pid, guard) {
            None => return Ok(()),
            Some(p) => p,
        };

        debug_delay();
        let head = unsafe { pte_ptr.deref().stack.head(guard) };
        let stack_iter = StackIter::from_ptr(head, guard);
        let cache_entries: Vec<_> = stack_iter.collect();

        // if the page is just a single blob pointer, rewrite it.
        if cache_entries.len() == 1
            && cache_entries[0].ptr().is_blob()
        {
            trace!("rewriting blob with pid {}", pid);
            let blob_ptr = cache_entries[0].ptr().blob().1;

            let log_reservation = self
                .log
                .reserve_blob(blob_ptr)
                .map_err(|e| e.danger_cast())?;

            let new_ptr = log_reservation.ptr();
            let mut new_cache_entry = cache_entries[0].clone();

            *new_cache_entry.ptr_ref_mut() = new_ptr;

            let node = node_from_frag_vec(vec![new_cache_entry])
                .into_shared(guard);

            debug_delay();
            let result = unsafe {
                pte_ptr.deref().stack.cas(head, node, guard)
            };

            if result.is_ok() {
                let ptrs = ptrs_from_stack(head, guard);
                let lsn = log_reservation.lsn();

                self.log
                    .with_sa(|sa| {
                        sa.mark_replace(pid, lsn, ptrs, new_ptr)
                    })
                    .map_err(|e| e.danger_cast())?;

                // NB complete must happen AFTER calls to SA, because
                // when the iobuf's n_writers hits 0, we may transition
                // the segment to inactive, resulting in a race otherwise.
                log_reservation
                    .complete()
                    .map_err(|e| e.danger_cast())?;
            } else {
                log_reservation
                    .abort()
                    .map_err(|e| e.danger_cast())?;
            }

            result
                .map(|_| ())
                .map_err(|e| Error::CasFailed(Some(PagePtr(e))))
        } else {
            trace!("rewriting page with pid {}", pid);

            // page-in whole page with a get,
            let (key, update) = match self
                .get(pid, guard)
                .map_err(|e| e.danger_cast())?
            {
                PageGet::Materialized(data, key) => {
                    (key, Update::Compact(data.clone()))
                }
                PageGet::Free(key) => (key, Update::Free),
                PageGet::Allocated(key) => (key, Update::Allocate),
                PageGet::Unallocated => {
                    // TODO when merge functionality is added,
                    // this may break
                    warn!("page stack deleted from pagetable before page could be rewritten");
                    return Ok(());
                }
            };

            self.cas_page(pid, key.0, update, guard).map(|_| ())
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn cas_page<'g>(
        &self,
        pid: PageId,
        mut old: PagePtrInner<'g, P>,
        new: Update<P>,
        guard: &'g Guard,
    ) -> Result<PagePtrInner<'g, P>, Option<PagePtr<'g, P>>> {
        trace!("cas_page called on pid {}", pid);
        let pte_ptr = match self.inner.get(pid, guard) {
            None => {
                trace!(
                    "early-returning from cas_page, no stack found"
                );
                return Err(Error::CasFailed(None));
            }
            Some(p) => p,
        };

        if old.is_null() {
            match self.get(pid, guard).map_err(|e| e.danger_cast())? {
                PageGet::Allocated(current_key) => {
                    old = current_key.0;
                }
                other => {
                    return Err(Error::CasFailed(Some(
                        other.into_ptr(),
                    )));
                }
            }
        }

        let replace: LoggedUpdate<P> =
            LoggedUpdate { pid, update: new };
        let bytes =
            measure(&M.serialize, || serialize(&replace).unwrap());
        let log_reservation =
            self.log.reserve(&bytes).map_err(|e| e.danger_cast())?;
        let lsn = log_reservation.lsn();
        let new_ptr = log_reservation.ptr();

        let cache_entry = match replace.update {
            Update::Compact(m) => {
                CacheEntry::MergedResident(m, lsn, new_ptr)
            }
            Update::Free => CacheEntry::Free(lsn, new_ptr),
            Update::Allocate => CacheEntry::Allocated(lsn, new_ptr),
            Update::Append(_) => {
                panic!("tried to cas a page using an Append")
            }
        };

        let node =
            node_from_frag_vec(vec![cache_entry]).into_shared(guard);

        debug_delay();
        let result =
            unsafe { pte_ptr.deref().stack.cas(old, node, guard) };

        if result.is_ok() {
            trace!("cas_page succeeded on pid {}", pid);
            let ptrs = ptrs_from_stack(old, guard);

            self.log
                .with_sa(|sa| {
                    sa.mark_replace(pid, lsn, ptrs, new_ptr)
                })
                .map_err(|e| e.danger_cast())?;

            // NB complete must happen AFTER calls to SA, because
            // when the iobuf's n_writers hits 0, we may transition
            // the segment to inactive, resulting in a race otherwise.
            log_reservation
                .complete()
                .map_err(|e| e.danger_cast())?;
        } else {
            trace!("cas_page failed on pid {}", pid);
            log_reservation.abort().map_err(|e| e.danger_cast())?;
        }

        result.map_err(|e| Error::CasFailed(Some(PagePtr(e))))
    }

    /// Try to retrieve a page by its logical ID.
    pub fn get<'g>(
        &self,
        pid: PageId,
        guard: &'g Guard,
    ) -> Result<PageGet<'g, PM::PageFrag>, ()> {
        loop {
            let pte_ptr = match self.inner.get(pid, guard) {
                None => {
                    return Ok(PageGet::Unallocated);
                }
                Some(p) => p,
            };

            let inner_res = self
                .page_in(pid, pte_ptr, guard)
                .map_err(|e| e.danger_cast())?;
            if let Some(res) = inner_res {
                return Ok(res);
            }
            // loop until we succeed
        }
    }

    /// The highest known stable Lsn on disk.
    pub fn stable_lsn(&self) -> Lsn {
        self.log.stable_offset()
    }

    /// Blocks until the provided Lsn is stable on disk,
    /// triggering necessary flushes in the process.
    pub fn make_stable(&self, lsn: Lsn) -> Result<(), ()> {
        self.log.make_stable(lsn)
    }

    /// Increase a page's associated transactional read
    /// timestamp (RTS) to as high as the specified timestamp.
    pub fn bump_page_rts(&self, pid: PageId, ts: u64, guard: &Guard) {
        let pte_ptr = if let Some(p) = self.inner.get(pid, guard) {
            p
        } else {
            return;
        };

        let pte = unsafe { pte_ptr.deref() };

        let mut current = pte.rts.load(SeqCst);
        loop {
            if current as u64 >= ts {
                return;
            }
            let last = pte.rts.compare_and_swap(
                current,
                ts as usize,
                SeqCst,
            );
            if last == current {
                // we succeeded.
                return;
            }
            current = last;
        }
    }

    /// Retrieves the current transactional read timestamp
    /// for a page.
    pub fn get_page_rts(
        &self,
        pid: PageId,
        guard: &Guard,
    ) -> Option<u64> {
        let pte_ptr = if let Some(p) = self.inner.get(pid, guard) {
            p
        } else {
            return None;
        };

        let pte = unsafe { pte_ptr.deref() };

        Some(pte.rts.load(SeqCst) as u64)
    }

    fn page_in<'g>(
        &self,
        pid: PageId,
        pte_ptr: Shared<'g, PageTableEntry<P>>,
        guard: &'g Guard,
    ) -> Result<
        Option<PageGet<'g, PM::PageFrag>>,
        Option<PagePtr<'g, P>>,
    > {
        let _measure = Measure::new(&M.page_in);

        debug_delay();
        let mut head = unsafe { pte_ptr.deref().stack.head(guard) };

        let mut stack_iter =
            StackIter::from_ptr(head, guard).peekable();

        if let Some(CacheEntry::MergedResident { .. }) =
            stack_iter.peek()
        {
            // Short circuit merging and fix-up if we only
            // have one frag.
            let ptr = PagePtr(head);
            let mr = unsafe { ptr.deref_merged_resident() };

            return Ok(Some(PageGet::Materialized(mr, ptr)));
        }

        let mut to_merge = vec![];
        let mut merged_resident = false;
        let mut ptrs = Vec::with_capacity(
            self.config.page_consolidation_threshold + 2,
        );
        let mut fix_up_length = 0;

        for cache_entry_ptr in stack_iter {
            match *cache_entry_ptr {
                CacheEntry::Resident(ref page_frag, lsn, ptr) => {
                    if !merged_resident {
                        to_merge.push(page_frag);
                    }
                    ptrs.push((lsn, ptr));
                }
                CacheEntry::MergedResident(
                    ref page_frag,
                    lsn,
                    ptr,
                ) => {
                    if !merged_resident {
                        to_merge.push(page_frag);
                        merged_resident = true;
                        fix_up_length = ptrs.len();
                    }
                    ptrs.push((lsn, ptr));
                }
                CacheEntry::PartialFlush(lsn, ptr)
                | CacheEntry::Flush(lsn, ptr) => {
                    ptrs.push((lsn, ptr));
                }
                CacheEntry::Free(_, _) => {
                    return Ok(Some(PageGet::Free(PagePtr(head))));
                }
                CacheEntry::Allocated(_, _) => {
                    return Ok(Some(PageGet::Allocated(PagePtr(
                        head,
                    ))));
                }
            }
        }

        if ptrs.is_empty() {
            return Ok(Some(PageGet::Unallocated));
        }

        let mut fetched = Vec::with_capacity(ptrs.len());

        // Did not find a previously merged value in memory,
        // may need to go to disk.
        if !merged_resident {
            let to_pull = &ptrs[to_merge.len()..];

            let pulled_res: Vec<_> = to_pull
                .par_iter()
                .map(|&(lsn, ptr)| self.pull(lsn, ptr))
                .collect();

            for res in pulled_res {
                let item_res = res.map_err(|e| e.danger_cast());

                if item_res.is_err() {
                    // check to see if the page head pointer is the same.
                    // if not, we may have failed our pull because a blob
                    // is no longer present that was replaced by another
                    // thread and removed.

                    let current_head =
                        unsafe { pte_ptr.deref().stack.head(guard) };
                    if current_head != head {
                        debug!(
                            "pull failed for item for pid {}, but we'll \
                             retry because the stack has changed",
                             pid,
                        );
                        return Ok(None);
                    }

                    let current_pte_ptr = match self
                        .inner
                        .get(pid, guard)
                    {
                        None => {
                            debug!(
                                "pull failed for item for pid {}, but we'll \
                                just return Unallocated because the pid is \
                                no longer present in the pagetable.",
                                pid,
                            );
                            return Ok(Some(PageGet::Unallocated));
                        }
                        Some(p) => p,
                    };

                    if current_pte_ptr != pte_ptr {
                        panic!(
                            "pull failed for item for pid {}, and somehow \
                             the page's entire stack has changed due to \
                             being reallocated while we were still \
                             witnessing it. This is probably a failure in the \
                             way that EBR is being used to handle page frees.",
                             pid,
                        );
                    }

                    debug!(
                        "pull failed for item for pid {}, but our stack of \
                        items has remained intact since we initially observed it, \
                        so there is probably a corruption issue or race condition.",
                        pid,
                    );
                }

                fetched.push(item_res?);
            }
        }

        let merged = if to_merge.is_empty() && fetched.len() == 1 {
            // don't perform any merging logic if we have
            // no found `Resident`s and only a single fetched
            // `Frag`.
            fetched.pop().unwrap()
        } else {
            let _measure = Measure::new(&M.merge_page);

            let combined_iter =
                to_merge.into_iter().chain(fetched.iter()).rev();

            self.t.merge(combined_iter)
        };

        let size = self.t.size_in_bytes(&merged);

        let to_evict = self.lru.accessed(pid, size);
        trace!(
            "accessed pid {} -> paging out pids {:?}",
            pid,
            to_evict
        );

        trace!("accessed page: {:?}", merged);
        self.page_out(to_evict, guard)
            .map_err(|e| e.danger_cast())?;

        if ptrs.len() > self.config.page_consolidation_threshold {
            trace!(
                "consolidating pid {} with len {}!",
                pid,
                ptrs.len()
            );
            match self.cas_page(
                pid,
                head,
                Update::Compact(merged),
                guard,
            ) {
                Ok(new_head) => head = new_head,
                Err(Error::CasFailed(None)) => {
                    // This page was unallocated since we
                    // read the head pointer.
                    return Ok(Some(PageGet::Unallocated));
                }
                Err(Error::CasFailed(Some(_))) => {
                    // our consolidation failed,
                    // so we communicate to the
                    // caller that they should retry
                    return Ok(None);
                }
                Err(other) => {
                    // we need to propagate this error
                    // beyond the caller
                    return Err(other);
                }
            }
        } else {
            trace!(
                "fixing up pid {} with {} traversed frags",
                pid,
                fix_up_length
            );
            let mut new_entries = Vec::with_capacity(ptrs.len());

            let (head_lsn, head_ptr) = ptrs.remove(0);
            let head_entry = CacheEntry::MergedResident(
                merged, head_lsn, head_ptr,
            );
            new_entries.push(head_entry);

            let mut tail = if let Some((lsn, ptr)) = ptrs.pop() {
                Some(CacheEntry::Flush(lsn, ptr))
            } else {
                None
            };

            for (lsn, ptr) in ptrs {
                new_entries.push(CacheEntry::PartialFlush(lsn, ptr));
            }

            if let Some(tail) = tail.take() {
                new_entries.push(tail);
            }

            let node =
                node_from_frag_vec(new_entries).into_shared(guard);

            debug_assert_eq!(
                ptrs_from_stack(head, guard),
                ptrs_from_stack(node, guard),
            );

            debug_delay();
            let res = unsafe {
                pte_ptr.deref().stack.cas(head, node, guard)
            };
            if let Ok(new_head) = res {
                head = new_head;
            } else {
                // we're out of date, retry
                return Ok(None);
            }
        }

        let ret_ptr = PagePtr(head);
        let mr = unsafe { ret_ptr.deref_merged_resident() };

        Ok(Some(PageGet::Materialized(mr, ret_ptr)))
    }

    fn page_out<'g>(
        &self,
        to_evict: Vec<PageId>,
        guard: &'g Guard,
    ) -> Result<(), ()> {
        let _measure = Measure::new(&M.page_out);
        for pid in to_evict {
            let pte_ptr = match self.inner.get(pid, guard) {
                None => continue,
                Some(p) => p,
            };

            debug_delay();
            let head = unsafe { pte_ptr.deref().stack.head(guard) };
            let stack_iter = StackIter::from_ptr(head, guard);

            let mut cache_entries: Vec<CacheEntry<P>> =
                stack_iter.map(|ptr| (*ptr).clone()).collect();

            // ensure the last entry is a Flush
            let last_ce = match cache_entries.pop() {
                None => return Ok(()),
                Some(c) => c,
            };

            let last = match last_ce {
                CacheEntry::MergedResident(_, lsn, ptr)
                | CacheEntry::Resident(_, lsn, ptr)
                | CacheEntry::Flush(lsn, ptr) => {
                    // NB stabilize the most recent LSN before
                    // paging out! This SHOULD very rarely block...
                    self.log.make_stable(lsn)?;
                    CacheEntry::Flush(lsn, ptr)
                }
                CacheEntry::PartialFlush(_, _) => {
                    panic!("got PartialFlush at end of stack...")
                }
                CacheEntry::Allocated(_, _)
                | CacheEntry::Free(_, _) => {
                    // don't actually evict this. this leads to
                    // a discrepency in the Lru perceived size
                    // and the real size, but this should be
                    // minimal in anticipated workloads.
                    return Ok(());
                }
            };

            let mut new_stack =
                Vec::with_capacity(cache_entries.len() + 1);
            for entry in cache_entries {
                match entry {
                    CacheEntry::PartialFlush(lsn, ptr)
                    | CacheEntry::MergedResident(_, lsn, ptr)
                    | CacheEntry::Resident(_, lsn, ptr) => {
                        new_stack
                            .push(CacheEntry::PartialFlush(lsn, ptr));
                    }
                    CacheEntry::Flush(_, _) => {
                        panic!("got Flush in middle of stack...")
                    }
                    CacheEntry::Allocated(_, _)
                    | CacheEntry::Free(_, _) => panic!(
                        "encountered {:?} in middle of stack...",
                        entry
                    ),
                }
            }
            new_stack.push(last);
            let node = node_from_frag_vec(new_stack);

            debug_delay();
            unsafe {
                if pte_ptr
                    .deref()
                    .stack
                    .cas(head, node.into_shared(guard), guard)
                    .is_err()
                {}
            }
        }
        Ok(())
    }

    fn pull<'g>(
        &self,
        lsn: Lsn,
        ptr: DiskPtr,
    ) -> Result<P, Option<PagePtr<'g, P>>> {
        trace!("pulling lsn {} ptr {} from disk", lsn, ptr);
        let _measure = Measure::new(&M.pull);
        let bytes = match self.log.read(lsn, ptr).map_err(|_| ()) {
            Ok(LogRead::Inline(read_lsn, buf, _len)) => {
                assert_eq!(
                    read_lsn, lsn,
                    "expected lsn {} on pull of ptr {}, \
                     but got lsn {} instead",
                    lsn, ptr, read_lsn
                );
                Ok(buf)
            }
            Ok(LogRead::Blob(read_lsn, buf, _blob_pointer)) => {
                assert_eq!(
                    read_lsn, lsn,
                    "expected lsn {} on pull of ptr {}, \
                     but got lsn {} instead",
                    lsn, ptr, read_lsn
                );

                Ok(buf)
            }
            other => {
                debug!("failed to read page: {:?}", other);
                Err(Error::Corruption { at: ptr })
            }
        }?;

        let logged_update = measure(&M.deserialize, || {
            deserialize::<LoggedUpdate<P>>(&*bytes)
                .map_err(|_| ())
                .expect("failed to deserialize data")
        });

        match logged_update.update {
            Update::Compact(page_frag)
            | Update::Append(page_frag) => Ok(page_frag),
            _ => Err(Error::ReportableBug(
                "non-append/compact found in pull".to_owned(),
            )),
        }
    }

    // caller is expected to have instantiated self.last_snapshot
    // in recovery already.
    fn advance_snapshot(&self) -> Result<(), ()> {
        let snapshot_mu = self.last_snapshot.clone();
        let iobufs = self.log.iobufs.clone();
        let config = self.config.clone();

        let gen_snapshot = move || {
            let snapshot_opt_res = snapshot_mu.try_lock();
            if snapshot_opt_res.is_err() {
                // some other thread is snapshotting
                warn!(
                    "snapshot skipped because previous attempt \
                     appears not to have completed"
                );
                return Ok(());
            }

            let mut snapshot_opt = snapshot_opt_res.unwrap();
            let last_snapshot = snapshot_opt.take().expect(
                "PageCache::advance_snapshot called before recovery",
            );

            if let Err(e) = iobufs.flush() {
                error!(
                    "failed to flush log during advance_snapshot: {}",
                    e
                );
                iobufs.with_sa(|sa| sa.resume_rewriting());
                *snapshot_opt = Some(last_snapshot);
                return Err(e);
            }

            // we disable rewriting so that our log becomes append-only,
            // allowing us to iterate through it without corrupting ourselves.
            // NB must be called after taking the snapshot mutex.
            iobufs.with_sa(|sa| sa.pause_rewriting());

            let max_lsn = last_snapshot.max_lsn;
            let start_lsn =
                max_lsn - (max_lsn % config.io_buf_size as Lsn);

            let iter = iobufs.iter_from(start_lsn);

            debug!(
                "snapshot starting from offset {} to the segment containing ~{}",
                last_snapshot.max_lsn,
                iobufs.stable(),
            );

            let res = advance_snapshot::<PM, P, R>(
                iter,
                last_snapshot,
                &config,
            );

            // NB it's important to resume writing before replacing the snapshot
            // into the mutex, otherwise we create a race condition where the SA is
            // not actually paused when a snapshot happens.
            iobufs.with_sa(|sa| sa.resume_rewriting());

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

        if let Some(e) = self.config.global_error() {
            return Err(e);
        }

        if let Some(ref thread_pool) = self.config.thread_pool {
            debug!(
                "asynchronously spawning snapshot generation task"
            );
            let config = self.config.clone();
            thread_pool.spawn(move || {
                if let Err(e) = gen_snapshot() {
                    match e {
                        Error::Io(ref ioe)
                            if ioe.kind() == std::io::ErrorKind::NotFound => {},
                        error => {
                            error!(
                                "encountered error while generating snapshot: {:?}",
                                error,
                            );
                            config.set_global_error(error);
                        }
                    }
                }
            });
        } else {
            debug!("synchronously generating a new snapshot");
            gen_snapshot()?;
        }

        // TODO add future for waiting on the result of this if desired
        Ok(())
    }

    fn load_snapshot(&mut self) {
        // panic if not set
        let snapshot =
            self.last_snapshot.try_lock().unwrap().clone().unwrap();

        self.max_pid.store(snapshot.max_pid, SeqCst);

        let mut snapshot_free = snapshot.free.clone();

        for (pid, state) in &snapshot.pt {
            trace!("load_snapshot page {} {:?}", pid, state);

            let stack = Stack::default();

            match *state {
                PageState::Present(ref ptrs) => {
                    let (base_lsn, base_ptr) = ptrs[0];

                    stack.push(CacheEntry::Flush(base_lsn, base_ptr));

                    for &(lsn, ptr) in &ptrs[1..] {
                        stack
                            .push(CacheEntry::PartialFlush(lsn, ptr));
                    }
                }
                PageState::Free(lsn, ptr) => {
                    // blow away any existing state
                    trace!("load_snapshot freeing pid {}", *pid);
                    stack.push(CacheEntry::Free(lsn, ptr));
                    self.free.lock().unwrap().push(*pid);
                    snapshot_free.remove(&pid);
                }
                PageState::Allocated(lsn, ptr) => {
                    assert!(!snapshot.free.contains(pid));
                    stack.push(CacheEntry::Allocated(lsn, ptr));
                }
            }

            let guard = pin();

            // Set up new stack

            let pte = PageTableEntry {
                stack,
                rts: AtomicUsize::new(0),
            };

            let new_pte = Owned::new(pte).into_shared(&guard);

            self.inner
                .cas(*pid, Shared::null(), new_pte, &guard)
                .unwrap();
        }

        assert!(
            snapshot_free.is_empty(),
            "pages present in Snapshot free list \
                ({:?})
                not found in recovered page table",
            snapshot_free
        );

        #[cfg(feature = "event_log")]
        {
            use std::collections::HashMap;
            let mut pages_after_restart: HashMap<
                PageId,
                Vec<DiskPtr>,
            > = HashMap::new();

            let guard = pin();

            for pid in 0..self.max_pid.load(SeqCst) {
                let pte = self.inner.get(pid, &guard);
                if pte.is_none() {
                    continue;
                }
                let head = unsafe {
                    pte.unwrap().deref().stack.head(&guard)
                };
                let ptrs = ptrs_from_stack(head, &guard);
                pages_after_restart.insert(pid, ptrs);
            }

            self.config
                .event_log
                .pages_after_restart(pages_after_restart);
        }
    }
}

fn ptrs_from_stack<'g, P: Send + Sync>(
    head_ptr: PagePtrInner<'g, P>,
    guard: &'g Guard,
) -> Vec<DiskPtr> {
    // generate a list of the old log ID's
    let stack_iter = StackIter::from_ptr(head_ptr, guard);

    let mut ptrs = vec![];
    for cache_entry_ptr in stack_iter {
        match *cache_entry_ptr {
            CacheEntry::Resident(_, _, ref ptr)
            | CacheEntry::MergedResident(_, _, ref ptr)
            | CacheEntry::PartialFlush(_, ref ptr)
            | CacheEntry::Free(_, ref ptr)
            | CacheEntry::Flush(_, ref ptr)
            | CacheEntry::Allocated(_, ref ptr) => {
                ptrs.push(*ptr);
            }
        }
    }
    ptrs
}

use std::collections::BinaryHeap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

use epoch::{Owned, Shared};

#[cfg(feature = "rayon")]
use rayon::prelude::*;

use pagetable::PageTable;

use super::*;

type PagePtrInner<'g, P> = epoch::Shared<'g, Node<CacheEntry<P>>>;

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
        PagePtr(epoch::Shared::null())
    }

    /// Whether this pointer is Allocated
    pub fn is_allocated(&self) -> bool {
        self.0.is_null()
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
}

impl<M: Send> CacheEntry<M> {
    fn ptr(&self) -> DiskPtr {
        use CacheEntry::*;
        match self {
            MergedResident(_, _, ptr)
            | Resident(_, _, ptr)
            | PartialFlush(_, ptr)
            | Flush(_, ptr)
            | Free(_, ptr) => *ptr,
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    Allocated,
    /// This page was never allocated.
    Unallocated,
}

unsafe impl<'a, P> Send for PageGet<'a, P> where
    P: DeserializeOwned + Serialize + Send + Sync
{}

unsafe impl<'a, P> Sync for PageGet<'a, P> where
    P: DeserializeOwned + Serialize + Send + Sync
{}

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
            PageGet::Allocated => true,
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
}

/// A lock-free pagecache which supports fragmented pages
/// for dramatically improving write throughput.
///
/// # Working with the `PageCache`
///
/// ```
/// extern crate pagecache;
///
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
///     fn merge(&self, frags: &[&Self::PageFrag]) -> Self::PageFrag {
///         let mut consolidated = String::new();
///         for frag in frags.into_iter() {
///             consolidated.push_str(&*frag);
///         }
///
///         consolidated
///     }
///
///     // Used to feed custom recovery information back to a higher-level abstraction
///     // during startup. For example, a B-Link tree must know what the current
///     // root node is before it can start serving requests.
///     fn recover(&self, _: &Self::PageFrag) -> Option<Self::Recovery> {
///         None
///     }
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
    inner: Arc<PageTable<Stack<CacheEntry<P>>>>,
    max_pid: AtomicUsize,
    free: Arc<Mutex<BinaryHeap<PageId>>>,
    log: Log,
    lru: Lru,
    updates: AtomicUsize,
    last_snapshot: Arc<Mutex<Option<Snapshot<R>>>>,
}

unsafe impl<PM, P, R> Send for PageCache<PM, P, R>
where
    PM: Send + Sync,
    P: 'static + Send + Sync,
    R: Send + Sync,
{}

unsafe impl<PM, P, R> Sync for PageCache<PM, P, R>
where
    PM: Send + Sync,
    P: 'static + Send + Sync,
    R: Send + Sync,
{}

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
    R: Debug + Clone + Serialize + DeserializeOwned + Send + Sync,
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
            lru: lru,
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
        let pid = if let Some(pid) = self.free.lock().unwrap().pop() {
            trace!("re-allocating pid {}", pid);
            let p = self.inner.del(pid, guard).unwrap();
            unsafe {
                match p.deref().head(guard).deref().deref() {
                    CacheEntry::Free(..) => (),
                    _ => panic!("expected page {} to be Free", pid),
                }
            };
            pid
        } else {
            let pid = self.max_pid.fetch_add(1, SeqCst);
            trace!("allocating pid {}", pid);

            pid
        };

        let new_stack = Stack::default();
        let stack_ptr = Owned::new(new_stack).into_shared(guard);

        self.inner
                .cas(pid, Shared::null(), stack_ptr, guard)
                .expect("allocating new page should never encounter existing data");

        self.cas_page(pid, Shared::null(), Update::Allocate, &guard)
            .map_err(|e| e.danger_cast())?;

        Ok(pid)
    }

    /// Free a particular page.
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
            for &e in free.iter() {
                assert_ne!(e, pid, "page {} was double-freed", pid);
            }

            free.push(pid);
        });
        Ok(())
    }

    /// Try to atomically add a `PageFrag` to the page.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the atomic append fails.
    pub fn link<'g>(
        &self,
        pid: PageId,
        old: PagePtr<'g, P>,
        new: P,
        guard: &'g Guard,
    ) -> Result<PagePtr<'g, P>, Option<PagePtr<'g, P>>> {
        if old.is_allocated() {
            return self.replace(pid, old, new, guard);
        }

        let stack_ptr = match self.inner.get(pid, guard) {
            None => return Err(Error::CasFailed(None)),
            Some(s) => s,
        };

        let prepend: LoggedUpdate<P> = LoggedUpdate {
            pid: pid,
            update: Update::Append(new.clone()),
        };

        let bytes =
            measure(&M.serialize, || serialize(&prepend).unwrap());
        let log_reservation =
            self.log.reserve(bytes).map_err(|e| e.danger_cast())?;
        let lsn = log_reservation.lsn();
        let ptr = log_reservation.ptr();

        let cache_entry = CacheEntry::Resident(new, lsn, ptr);

        debug_delay();
        let result = unsafe {
            stack_ptr.deref().cap(old.0, cache_entry, guard)
        };

        if result.is_err() {
            log_reservation.abort().map_err(|e| e.danger_cast())?;
        } else {
            let to_clean = self.log.with_sa(|sa| {
                sa.mark_link(pid, lsn, ptr);
                sa.clean(None)
            });

            // NB complete must happen AFTER calls to SA, because
            // when the iobuf's n_writers hits 0, we may transition
            // the segment to inactive, resulting in a race otherwise.
            // FIXME can result in deadlock if a node that holds SA
            // is waiting to acquire a new reservation blocked by this?
            log_reservation
                .complete()
                .map_err(|e| e.danger_cast())?;

            if let Some(to_clean) = to_clean {
                let _ = self.rewrite_page(to_clean, guard);
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
            .map(|p| PagePtr(p))
            .map_err(|e| Error::CasFailed(Some(PagePtr(e))))
    }

    /// Replace an existing page with a different set of `PageFrag`s.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the atomic swap fails.
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
            let to_clean = self.log.with_sa(|sa| sa.clean(Some(pid)));

            if let Some(to_clean) = to_clean {
                assert_ne!(pid, to_clean);
                let _ = self.rewrite_page(to_clean, guard);
            }

            let count = self.updates.fetch_add(1, SeqCst) + 1;
            let should_snapshot =
                count % self.config.snapshot_after_ops == 0;
            if should_snapshot {
                self.advance_snapshot()
                    .map_err(|e| e.danger_cast())?;
            }
        }

        result.map(|p| PagePtr(p))
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

        let stack_ptr = match self.inner.get(pid, guard) {
            None => return Ok(()),
            Some(s) => s,
        };

        debug_delay();
        let head = unsafe { stack_ptr.deref().head(guard) };
        let stack_iter = StackIter::from_ptr(head, guard);
        let cache_entries: Vec<_> = stack_iter.collect();

        // if the page is just a single blob pointer, rewrite it.
        if cache_entries.len() == 1
            && cache_entries[0].ptr().is_blob()
        {
            let blob_ptr = cache_entries[0].ptr().blob().1;

            let log_reservation = self
                .log
                .reserve_blob(blob_ptr)
                .map_err(|e| e.danger_cast())?;

            let node =
                node_from_frag_vec(vec![cache_entries[0].clone()])
                    .into_shared(guard);

            debug_delay();
            let result =
                unsafe { stack_ptr.deref().cas(head, node, guard) };

            if result.is_ok() {
                let ptrs = ptrs_from_stack(head, guard);
                let lsn = log_reservation.lsn();
                let new_ptr = log_reservation.ptr();

                self.log
                    .with_sa(|sa| {
                        sa.mark_replace(pid, lsn, ptrs, new_ptr)
                    }).map_err(|e| e.danger_cast())?;

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
            // page-in whole page with a get,
            let (key, update) = match self.get(pid, guard)? {
                PageGet::Materialized(data, key) => {
                    (key, Update::Compact(data.clone()))
                }
                PageGet::Free(key) => (key, Update::Free),
                PageGet::Allocated => {
                    (PagePtr(Shared::null()), Update::Allocate)
                }
                PageGet::Unallocated => {
                    // TODO when merge functionality is added,
                    // this may break
                    panic!("get returned Unallocated");
                }
            };

            self.cas_page(pid, key.0, update, guard).map(|_| ())?;

            Ok(())
        }
    }

    fn cas_page<'g>(
        &self,
        pid: PageId,
        old: PagePtrInner<'g, P>,
        new: Update<P>,
        guard: &'g Guard,
    ) -> Result<PagePtrInner<'g, P>, Option<PagePtr<'g, P>>> {
        let stack_ptr = match self.inner.get(pid, guard) {
            None => {
                trace!(
                    "early-returning from cas_page, no stack found"
                );
                return Err(Error::CasFailed(None));
            }
            Some(s) => s,
        };

        let replace: LoggedUpdate<P> = LoggedUpdate {
            pid: pid,
            update: new.clone(),
        };
        let bytes =
            measure(&M.serialize, || serialize(&replace).unwrap());
        let log_reservation =
            self.log.reserve(bytes).map_err(|e| e.danger_cast())?;
        let lsn = log_reservation.lsn();
        let new_ptr = log_reservation.ptr();

        let cache_entry = match new {
            Update::Compact(m) => {
                Some(CacheEntry::MergedResident(m, lsn, new_ptr))
            }
            Update::Free => Some(CacheEntry::Free(lsn, new_ptr)),
            Update::Allocate => None,
            Update::Append(_) => {
                panic!("tried to cas a page using an Append")
            }
        };

        let node = cache_entry
            .map(|cache_entry| {
                node_from_frag_vec(vec![cache_entry])
                    .into_shared(guard)
            }).unwrap_or_else(|| Shared::null());

        debug_delay();
        let result =
            unsafe { stack_ptr.deref().cas(old, node, guard) };

        if result.is_ok() {
            let ptrs = ptrs_from_stack(old, guard);

            self.log
                .with_sa(|sa| {
                    sa.mark_replace(pid, lsn, ptrs, new_ptr)
                }).map_err(|e| e.danger_cast())?;

            // NB complete must happen AFTER calls to SA, because
            // when the iobuf's n_writers hits 0, we may transition
            // the segment to inactive, resulting in a race otherwise.
            log_reservation
                .complete()
                .map_err(|e| e.danger_cast())?;
        } else {
            log_reservation.abort().map_err(|e| e.danger_cast())?;
        }

        result.map_err(|e| Error::CasFailed(Some(PagePtr(e))))
    }

    /// Try to retrieve a page by its logical ID.
    pub fn get<'g>(
        &self,
        pid: PageId,
        guard: &'g Guard,
    ) -> Result<PageGet<'g, PM::PageFrag>, Option<PagePtr<'g, P>>>
    {
        let stack_ptr = match self.inner.get(pid, guard) {
            None => return Ok(PageGet::Unallocated),
            Some(s) => s,
        };

        self.page_in(pid, stack_ptr, guard)
    }

    fn page_in<'g>(
        &self,
        pid: PageId,
        stack_ptr: Shared<'g, Stack<CacheEntry<P>>>,
        guard: &'g Guard,
    ) -> Result<PageGet<'g, PM::PageFrag>, Option<PagePtr<'g, P>>>
    {
        loop {
            let inner_res =
                self.page_in_inner(pid, stack_ptr, guard)?;
            if let Some(res) = inner_res {
                return Ok(res);
            }
            // loop until we succeed
        }
    }

    fn page_in_inner<'g>(
        &self,
        pid: PageId,
        stack_ptr: Shared<'g, Stack<CacheEntry<P>>>,
        guard: &'g Guard,
    ) -> Result<
        Option<PageGet<'g, PM::PageFrag>>,
        Option<PagePtr<'g, P>>,
    > {
        let _measure = Measure::new(&M.page_in);

        debug_delay();
        let mut head = unsafe { stack_ptr.deref().head(guard) };

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
        let mut ptrs = vec![];
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
            }
        }

        if ptrs.is_empty() {
            return Ok(Some(PageGet::Allocated));
        }

        let mut fetched = Vec::with_capacity(ptrs.len());

        // Did not find a previously merged value in memory,
        // may need to go to disk.
        if !merged_resident {
            let to_pull = &ptrs[to_merge.len()..];

            #[cfg(feature = "rayon")]
            {
                let pulled_res: Vec<_> = to_pull
                    .par_iter()
                    .map(|&(lsn, ptr)| self.rayon_pull(lsn, ptr))
                    .collect();

                for res in pulled_res {
                    let item = res.map_err(|e| e.danger_cast())?;
                    fetched.push(item);
                }
            }

            #[cfg(not(feature = "rayon"))]
            for &(lsn, ptr) in to_pull {
                fetched.push(self.pull(lsn, ptr)?);
            }
        }

        let combined: Vec<&P> = to_merge
            .iter()
            .cloned()
            .chain(fetched.iter())
            .rev()
            .collect();

        let merged =
            measure(&M.merge_page, || self.t.merge(&*combined));

        let size = std::mem::size_of_val(&merged);
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
                _ => {
                    // we need to loop in the caller
                    return Ok(None);
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

            let node = node_from_frag_vec(new_entries);

            debug_delay();
            let res = unsafe {
                stack_ptr.deref().cas(
                    head,
                    node.into_shared(guard),
                    guard,
                )
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
            let stack_ptr = match self.inner.get(pid, guard) {
                None => continue,
                Some(s) => s,
            };

            debug_delay();
            let head = unsafe { stack_ptr.deref().head(guard) };
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
                CacheEntry::Free(_, _) => {
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
                    CacheEntry::PartialFlush(lsn, ptr) |
                    CacheEntry::MergedResident(_, lsn, ptr) |
                    CacheEntry::Resident(_, lsn, ptr) => {
                        new_stack.push(CacheEntry::PartialFlush(lsn, ptr));
                    }
                    CacheEntry::Flush(_, _) => {
                        panic!("got Flush in middle of stack...")
                    }
                    CacheEntry::Free(_, _) => {
                        panic!(
                            "encountered a Free tombstone page in middle of stack..."
                        )
                    }
                }
            }
            new_stack.push(last);
            let node = node_from_frag_vec(new_stack);

            debug_delay();
            unsafe {
                if stack_ptr
                    .deref()
                    .cas(head, node.into_shared(guard), guard)
                    .is_err()
                {}
            }
        }
        Ok(())
    }

    #[cfg(feature = "rayon")]
    fn rayon_pull<'g>(
        &self,
        lsn: Lsn,
        ptr: DiskPtr,
    ) -> Result<P, Option<PagePtr<'g, P>>> {
        self.pull(lsn, ptr).map_err(|e1| e1.danger_cast())
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
            // FIXME 'read invalid data at lid 66244182' in cycle test
            _other => Err(Error::Corruption { at: ptr }),
        }?;

        let logged_update = measure(&M.deserialize, || {
            deserialize::<LoggedUpdate<P>>(&*bytes)
                .map_err(|_| ())
                .expect("failed to deserialize data")
        });

        match logged_update.update {
            Update::Compact(page_frag)
            | Update::Append(page_frag) => Ok(page_frag),
            _ => {
                return Err(Error::ReportableBug(
                    "non-append/compact found in pull".to_owned(),
                ))
            }
        }
    }

    // caller is expected to have instantiated self.last_snapshot
    // in recovery already.
    fn advance_snapshot(&self) -> Result<(), ()> {
        let snapshot_opt_res = self.last_snapshot.try_lock();
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

        if let Err(e) = self.log.flush() {
            error!(
                "failed to flush log during advance_snapshot: {}",
                e
            );
            self.log.with_sa(|sa| sa.resume_rewriting());
            *snapshot_opt = Some(last_snapshot);
            return Err(e);
        }

        // we disable rewriting so that our log becomes append-only,
        // allowing us to iterate through it without corrupting ourselves.
        // NB must be called after taking the snapshot mutex.
        self.log.with_sa(|sa| sa.pause_rewriting());

        let max_lsn = last_snapshot.max_lsn;
        let start_lsn =
            max_lsn - (max_lsn % self.config.io_buf_size as Lsn);

        debug!(
            "snapshot starting from offset {} to the segment containing ~{}",
            last_snapshot.max_lsn,
            self.log.stable_offset(),
        );

        let iter = self.log.iter_from(start_lsn);

        let res = advance_snapshot::<PM, P, R>(
            iter,
            last_snapshot,
            &self.config,
        );

        // NB it's important to resume writing before replacing the snapshot
        // into the mutex, otherwise we create a race condition where the SA is
        // not actually paused when a snapshot happens.
        self.log.with_sa(|sa| sa.resume_rewriting());

        match res {
            Err(e) => {
                *snapshot_opt = Some(Snapshot::default());
                Err(e)
            }
            Ok(next_snapshot) => {
                *snapshot_opt = Some(next_snapshot);
                Ok(())
            }
        }
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

            match state {
                &PageState::Present(ref ptrs) => {
                    let (base_lsn, base_ptr) = ptrs[0];

                    stack.push(CacheEntry::Flush(base_lsn, base_ptr));

                    for &(lsn, ptr) in &ptrs[1..] {
                        stack
                            .push(CacheEntry::PartialFlush(lsn, ptr));
                    }
                }
                &PageState::Free(lsn, ptr) => {
                    // blow away any existing state
                    trace!("load_snapshot freeing pid {}", *pid);
                    stack.push(CacheEntry::Free(lsn, ptr));
                    self.free.lock().unwrap().push(*pid);
                    snapshot_free.remove(&pid);
                }
                &PageState::Allocated(_lsn, _ptr) => {
                    assert!(!snapshot.free.contains(pid));
                    // empty stack with null ptr head implies Allocated
                }
            }

            let guard = pin();

            // Set up new stack

            let shared_stack = Owned::new(stack).into_shared(&guard);

            self.inner
                .cas(*pid, Shared::null(), shared_stack, &guard)
                .unwrap();
        }

        assert!(
            snapshot_free.is_empty(),
            "pages present in Snapshot free list \
                ({:?})
                not found in recovered page table",
            snapshot_free
        );
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
            | CacheEntry::Flush(_, ref ptr) => {
                ptrs.push(*ptr);
            }
        }
    }
    ptrs
}

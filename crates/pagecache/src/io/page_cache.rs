use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};

use epoch::{Guard, Owned, Shared};

#[cfg(feature = "rayon")]
use rayon::prelude::*;

use super::*;

/// Points to either a memory location or a disk location to page-in data from.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheEntry<M: Send> {
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
    /// A freed page tombstone.
    Free(Lsn, LogID),
}

/// `LoggedUpdate` is for writing blocks of `Update`'s to disk
/// sequentially, to reduce IO during page reads.
#[serde(bound(deserialize = ""))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct LoggedUpdate<PageFrag>
    where PageFrag: Serialize + DeserializeOwned
{
    pub(super) pid: PageID,
    pub(super) update: Update<PageFrag>,
}

#[serde(bound(deserialize = ""))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) enum Update<PageFrag>
    where PageFrag: DeserializeOwned + Serialize
{
    Append(PageFrag),
    Compact(PageFrag),
    Free,
    Allocate,
}

/// The result of a `get` call in the `PageCache`.
#[derive(Clone, Debug, PartialEq)]
pub enum PageGet<'a, PageFrag>
    where PageFrag: 'static + DeserializeOwned + Serialize + Send + Sync
{
    /// This page contains data and has been prepared
    /// for presentation to the caller by the `PageCache`'s
    /// `Materializer`.
    Materialized(PageFrag, PagePtr<'a, PageFrag>),
    /// This page has been Freed
    Free,
    /// This page has been allocated, but will become
    /// Free after restarting the system unless some
    /// data gets written to it.
    Allocated,
    /// This page was never allocated.
    Unallocated,
}

unsafe impl<'a, P> Send for PageGet<'a, P>
    where P: DeserializeOwned + Serialize + Send + Sync
{
}

unsafe impl<'a, P> Sync for PageGet<'a, P>
    where P: DeserializeOwned + Serialize + Send + Sync
{
}

impl<'a, P> PageGet<'a, P>
    where P: DeserializeOwned + Serialize + Send + Sync
{
    /// unwraps the `PageGet` into its inner `Materialized`
    /// form.
    ///
    /// # Panics
    /// Panics if it is a variant other than Materialized.
    pub fn unwrap(self) -> (P, PagePtr<'a, P>) {
        match self {
            PageGet::Materialized(p, hptr) => (p, hptr),
            _ => panic!("unwrap called on non-Materialized"),
        }
    }

    /// Returns true if the `PageGet` is `Materialized`.
    pub fn is_materialized(&self) -> bool {
        match *self {
            PageGet::Materialized(_, _) => true,
            _ => false,
        }
    }

    /// Returns true if the `PageGet` is `Free`.
    pub fn is_free(&self) -> bool {
        match *self {
            PageGet::Free => true,
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
/// extern crate crossbeam_epoch as epoch;
///
/// use pagecache::Materializer;
///
/// use epoch::{Shared, pin};
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
///     fn new(last_recovery: &Option<Self::Recovery>) -> Self {
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
///     let conf = pagecache::ConfigBuilder::new().temporary(true).build();
///     let pc: pagecache::PageCache<TestMaterializer, _, _> =
///         pagecache::PageCache::start(conf).unwrap();
///     {
///         let guard = pin();
///         let id = pc.allocate(&guard).unwrap();
///
///         // The first item in a page should be set using replace,
///         // which signals that this is the beginning of a new
///         // page history, and that any previous items associated
///         // with this page should be forgotten.
///         let key = pc.replace(id, Shared::null(), "a".to_owned(), &guard).unwrap();
///
///         // Subsequent atomic updates should be added with link.
///         let key = pc.link(id, key, "b".to_owned(), &guard).unwrap();
///         let _key = pc.link(id, key, "c".to_owned(), &guard).unwrap();
///
///         // When getting a page, the provide `Materializer` is
///         // used to merge all pages together.
///         let (consolidated, _key) = pc.get(id, &guard).unwrap().unwrap();
///
///         assert_eq!(consolidated, "abc".to_owned());
///     }
/// }
/// ```
pub struct PageCache<PM, P, R>
    where P: 'static + Send + Sync
{
    t: Arc<PM>,
    config: Config,
    inner: Radix<Stack<CacheEntry<P>>>,
    max_pid: AtomicUsize,
    free: Arc<Mutex<BinaryHeap<PageID>>>,
    log: Log,
    lru: Lru,
    updates: AtomicUsize,
    last_snapshot: Arc<Mutex<Option<Snapshot<R>>>>,
}

unsafe impl<PM, P, R> Send for PageCache<PM, P, R>
    where PM: Send + Sync,
          P: 'static + Send + Sync,
          R: Send
{
}

impl<PM, P, R> Debug for PageCache<PM, P, R>
    where PM: Send + Sync,
          P: Debug + Send + Sync,
          R: Debug + Send
{
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str(&*format!(
            "PageCache {{ max: {:?} free: {:?} }}\n",
            self.max_pid.load(SeqCst),
            self.free
        ))
    }
}

impl<PM, P, R> PageCache<PM, P, R>
    where PM: Materializer<PageFrag = P, Recovery = R>,
          PM: 'static + Send + Sync,
          P: 'static
                 + Debug
                 + Clone
                 + Serialize
                 + DeserializeOwned
                 + Send
                 + Sync,
          R: Debug + Clone + Serialize + DeserializeOwned + Send
{
    /// Instantiate a new `PageCache`.
    pub fn start(config: Config) -> CacheResult<PageCache<PM, P, R>, ()> {
        let cache_capacity = config.cache_capacity;
        let cache_shard_bits = config.cache_bits;
        let lru = Lru::new(cache_capacity, cache_shard_bits);

        // try to pull any existing snapshot off disk, and
        // apply any new data to it to "catch-up" the
        // snapshot before loading it.
        let snapshot = read_snapshot_or_default::<PM, P, R>(&config)?;

        let materializer = Arc::new(PM::new(&snapshot.recovery));

        let mut pc = PageCache {
            t: materializer,
            config: config.clone(),
            inner: Radix::default(),
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
    pub fn flush(&self) -> CacheResult<(), ()> {
        self.log.flush()
    }

    /// Return the recovered state from the snapshot
    pub fn recovered_state(&self) -> Option<R> {
        let mu = &self.last_snapshot.lock().unwrap();

        if let Some(ref snapshot) = **mu {
            snapshot.recovery.clone()
        } else {
            None
        }
    }

    /// Create a new page, trying to reuse old freed pages if possible
    /// to maximize underlying `Radix` pointer density.
    pub fn allocate<'g>(&self, guard: &'g Guard) -> CacheResult<PageID, ()> {
        let pid = self.free.lock().unwrap().pop().unwrap_or_else(|| {
            self.max_pid.fetch_add(1, SeqCst)
        });
        trace!("allocating pid {}", pid);

        // set up new stack
        let stack = Stack::default();

        self.inner.del(pid, guard);
        self.inner.insert(pid, stack).unwrap();

        // serialize log update
        let prepend: LoggedUpdate<P> = LoggedUpdate {
            pid: pid,
            update: Update::Allocate,
        };
        let bytes =
            measure(&M.serialize, || serialize(&prepend, Infinite).unwrap());

        // reserve slot in log
        // FIXME not threadsafe?
        self.log.write(bytes)?;

        Ok(pid)
    }

    /// Free a particular page.
    pub fn free<'g>(
        &self,
        pid: PageID,
        guard: &'g Guard,
    ) -> CacheResult<(), Option<PagePtr<'g, P>>> {
        let old_stack_opt = self.inner.get(pid, &guard);

        if old_stack_opt.is_none() {
            // already freed or never allocated
            return Ok(());
        }

        match self.get(pid, &guard)? {
            PageGet::Free =>
                // already freed or never allocated
                return Ok(()),
            PageGet::Allocated |
            PageGet::Unallocated |
            PageGet::Materialized(_, _) => (),
        }

        let old_stack = old_stack_opt.unwrap();

        // serialize log update
        let prepend: LoggedUpdate<P> = LoggedUpdate {
            pid: pid,
            update: Update::Free,
        };
        let bytes =
            measure(&M.serialize, || serialize(&prepend, Infinite).unwrap());

        // reserve slot in log
        let res = self.log.reserve(bytes).map_err(|e| e.danger_cast())?;
        let lsn = res.lsn();
        let lid = res.lid();

        // set up new stack
        let stack = Stack::default();
        let cache_entry = CacheEntry::Free(lsn, lid);
        stack.cap(Shared::null(), cache_entry, &guard).unwrap();

        let new_stack = Owned::new(stack).into_shared(&guard);

        // add pid to free stack to reduce fragmentation over time
        unsafe {
            let cas_key = old_stack.deref().head(&guard);
            // FIXME this is not threadsafe
            self.inner.cas(pid, old_stack, new_stack, &guard).unwrap();

            self.log.with_sa(|sa| {
                sa.mark_replace(pid, lsn, lids_from_stack(cas_key, &guard), lid)
            });
        }

        // NB complete must happen AFTER calls to SA, because
        // when the iobuf's n_writers hits 0, we may transition
        // the segment to inactive, resulting in a race otherwise.
        res.complete().map_err(|e| e.danger_cast())?;

        let free = self.free.clone();
        unsafe {
            guard.defer(move || {
                let mut free = free.lock().unwrap();
                // panic if we were able to double-free a page
                for &e in free.iter() {
                    assert_ne!(e, pid, "page was double-freed");
                }
                free.push(pid);
            });
            guard.flush();
        }
        Ok(())
    }

    /// Try to atomically add a `PageFrag` to the page.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the atomic append fails.
    pub fn link<'g>(
        &self,
        pid: PageID,
        old: PagePtr<'g, P>,
        new: P,
        guard: &'g Guard,
    ) -> CacheResult<PagePtr<'g, P>, Option<PagePtr<'g, P>>> {
        let stack_ptr = self.inner.get(pid, guard);
        if stack_ptr.is_none() {
            return Err(Error::CasFailed(None));
        }
        let stack_ptr = stack_ptr.unwrap();

        let prepend: LoggedUpdate<P> = LoggedUpdate {
            pid: pid,
            update: if old.is_null() {
                Update::Compact(new.clone())
            } else {
                Update::Append(new.clone())
            },
        };

        let bytes =
            measure(&M.serialize, || serialize(&prepend, Infinite).unwrap());
        let log_reservation =
            self.log.reserve(bytes).map_err(|e| e.danger_cast())?;
        let lsn = log_reservation.lsn();
        let lid = log_reservation.lid();

        let cache_entry = CacheEntry::Resident(new, lsn, lid);

        let result = unsafe { stack_ptr.deref().cap(old, cache_entry, guard) };

        if result.is_err() {
            log_reservation.abort().map_err(|e| e.danger_cast())?;
        } else {
            let to_clean = self.log.with_sa(|sa| {
                sa.mark_link(pid, lsn, lid);
                sa.clean(None)
            });

            // NB complete must happen AFTER calls to SA, because
            // when the iobuf's n_writers hits 0, we may transition
            // the segment to inactive, resulting in a race otherwise.
            // FIXME can result in deadlock if a node that holds SA
            // is waiting to acquire a new reservation blocked by this?
            log_reservation.complete().map_err(|e| e.danger_cast())?;

            if let Some(to_clean) = to_clean {
                match self.get(to_clean, guard)? {
                    PageGet::Materialized(page, key) => {
                        let _ = self.replace_recurse_once(
                            to_clean,
                            key,
                            Update::Compact(page),
                            guard,
                            true,
                        );
                    }
                    PageGet::Free => {
                        let _ = self.replace_recurse_once(
                            to_clean,
                            Shared::null(),
                            Update::Free,
                            guard,
                            true,
                        );
                    }
                    PageGet::Allocated => {
                        let _ = self.replace_recurse_once(
                            to_clean,
                            Shared::null(),
                            Update::Allocate,
                            guard,
                            true,
                        );
                    }
                    PageGet::Unallocated => {
                        panic!("get returned Unallocated");
                    }
                }
            }

            let count = self.updates.fetch_add(1, SeqCst) + 1;
            let should_snapshot = count % self.config.snapshot_after_ops == 0;
            if should_snapshot {
                self.advance_snapshot().map_err(|e| e.danger_cast())?;
            }
        }

        result.map_err(|e| Error::CasFailed(Some(e)))
    }

    /// Replace an existing page with a different set of `PageFrag`s.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the atomic swap fails.
    pub fn replace<'g>(
        &self,
        pid: PageID,
        old: PagePtr<'g, P>,
        new: P,
        guard: &'g Guard,
    ) -> CacheResult<PagePtr<'g, P>, Option<PagePtr<'g, P>>> {
        self.replace_recurse_once(pid, old, Update::Compact(new), guard, false)
    }

    fn replace_recurse_once<'g>(
        &self,
        pid: PageID,
        old: PagePtr<'g, P>,
        new: Update<P>,
        guard: &'g Guard,
        recursed: bool,
    ) -> CacheResult<PagePtr<'g, P>, Option<PagePtr<'g, P>>> {
        trace!("replacing pid {}", pid);
        let stack_ptr = self.inner.get(pid, guard);
        if stack_ptr.is_none() {
            return Err(Error::CasFailed(None));
        }
        let stack_ptr = stack_ptr.unwrap();

        let replace: LoggedUpdate<P> = LoggedUpdate {
            pid: pid,
            update: new.clone(),
        };
        let bytes =
            measure(&M.serialize, || serialize(&replace, Infinite).unwrap());
        let log_reservation =
            self.log.reserve(bytes).map_err(|e| e.danger_cast())?;
        let lsn = log_reservation.lsn();
        let lid = log_reservation.lid();

        let cache_entry = match new {
            Update::Compact(m) => Some(CacheEntry::MergedResident(m, lsn, lid)),
            Update::Free => Some(CacheEntry::Free(lsn, lid)),
            Update::Allocate => None,
            _ => unimplemented!(),
        };

        let node = cache_entry
            .map(|cache_entry| {
                node_from_frag_vec(vec![cache_entry]).into_shared(guard)
            })
            .unwrap_or_else(|| Shared::null());

        debug_delay();
        let result = unsafe { stack_ptr.deref().cas(old, node, guard) };

        if result.is_ok() {
            let lid = log_reservation.lid();
            let lsn = log_reservation.lsn();
            let lids = lids_from_stack(old, guard);

            let to_clean = self.log.with_sa(|sa| {
                sa.mark_replace(pid, lsn, lids, lid);
                if recursed { None } else { sa.clean(Some(pid)) }
            });

            // NB complete must happen AFTER calls to SA, because
            // when the iobuf's n_writers hits 0, we may transition
            // the segment to inactive, resulting in a race otherwise.
            log_reservation.complete().map_err(|e| e.danger_cast())?;

            if let Some(to_clean) = to_clean {
                assert_ne!(pid, to_clean);
                match self.get(to_clean, guard)? {
                    PageGet::Materialized(page, key) => {
                        let _ = self.replace_recurse_once(
                            to_clean,
                            key,
                            Update::Compact(page),
                            guard,
                            true,
                        );
                    }
                    PageGet::Free => {
                        let _ = self.replace_recurse_once(
                            to_clean,
                            Shared::null(),
                            Update::Free,
                            guard,
                            true,
                        );
                    }
                    PageGet::Allocated => {
                        let _ = self.replace_recurse_once(
                            to_clean,
                            Shared::null(),
                            Update::Allocate,
                            guard,
                            true,
                        );
                    }
                    PageGet::Unallocated => {
                        panic!("get returned Unallocated");
                    }
                }
            }

            let count = self.updates.fetch_add(1, SeqCst) + 1;
            let should_snapshot = count % self.config.snapshot_after_ops == 0;
            if should_snapshot {
                self.advance_snapshot().map_err(|e| e.danger_cast())?;
            }
        } else {
            log_reservation.abort().map_err(|e| e.danger_cast())?;
        }

        result.map_err(|e| Error::CasFailed(Some(e)))
    }

    /// Try to retrieve a page by its logical ID.
    pub fn get<'g>(
        &self,
        pid: PageID,
        guard: &'g Guard,
    ) -> CacheResult<PageGet<'g, PM::PageFrag>, Option<PagePtr<'g, P>>> {
        let stack_ptr = self.inner.get(pid, guard);
        if stack_ptr.is_none() {
            return Ok(PageGet::Unallocated);
        }

        let stack_ptr = stack_ptr.unwrap();

        let head = unsafe { stack_ptr.deref().head(guard) };

        self.page_in(pid, head, stack_ptr, guard)
    }

    fn page_in<'g>(
        &self,
        pid: PageID,
        mut head: Shared<'g, ds::stack::Node<CacheEntry<P>>>,
        stack_ptr: Shared<'g, ds::stack::Stack<CacheEntry<P>>>,
        guard: &'g Guard,
    ) -> CacheResult<PageGet<'g, PM::PageFrag>, Option<PagePtr<'g, P>>> {
        let _measure = Measure::new(&M.page_in);
        let stack_iter = StackIter::from_ptr(head, guard);

        let mut to_merge = vec![];
        let mut merged_resident = false;
        let mut lids = vec![];
        let mut fix_up_length = 0;

        for cache_entry_ptr in stack_iter {
            match *cache_entry_ptr {
                CacheEntry::Resident(ref page_frag, lsn, lid) => {
                    if !merged_resident {
                        to_merge.push(page_frag);
                    }
                    lids.push((lsn, lid));
                }
                CacheEntry::MergedResident(ref page_frag, lsn, lid) => {
                    if lids.is_empty() {
                        // Short circuit merging and fix-up if we only
                        // have one frag.
                        return Ok(
                            PageGet::Materialized(page_frag.clone(), head),
                        );
                    }
                    if !merged_resident {
                        to_merge.push(page_frag);
                        merged_resident = true;
                        fix_up_length = lids.len();
                    }
                    lids.push((lsn, lid));
                }
                CacheEntry::PartialFlush(lsn, lid) |
                CacheEntry::Flush(lsn, lid) => {
                    lids.push((lsn, lid));
                }
                CacheEntry::Free(_, _) => return Ok(PageGet::Free),
            }
        }

        if lids.is_empty() {
            return Ok(PageGet::Allocated);
        }

        let mut fetched = Vec::with_capacity(lids.len());

        // Did not find a previously merged value in memory,
        // may need to go to disk.
        if !merged_resident {
            let to_pull = &lids[to_merge.len()..];

            #[cfg(feature = "rayon")]
            {
                let pulled_res: Vec<_> = to_pull
                    .par_iter()
                    .map(|&(lsn, lid)| self.rayon_pull(lsn, lid))
                    .collect();

                for res in pulled_res {
                    let item = res.map_err(|e| e.danger_cast())?;
                    fetched.push(item);
                }
            }

            #[cfg(not(feature = "rayon"))]
            for &(lsn, lid) in to_pull {
                fetched.push(self.pull(lsn, lid)?);
            }
        }

        let combined: Vec<&P> = to_merge
            .iter()
            .cloned()
            .chain(fetched.iter())
            .rev()
            .collect();

        let merged = measure(&M.merge_page, || self.t.merge(&*combined));

        let size = std::mem::size_of_val(&merged);
        let to_evict = self.lru.accessed(pid, size);
        trace!("accessed pid {} -> paging out pid {:?}", pid, to_evict);
        self.page_out(to_evict, guard).map_err(|e| e.danger_cast())?;

        if lids.len() > self.config.page_consolidation_threshold {
            trace!("consolidating pid {} with len {}!", pid, lids.len());
            match self.replace_recurse_once(
                pid,
                head,
                Update::Compact(merged.clone()),
                guard,
                true,
            ) {
                Ok(new_head) => head = new_head,
                Err(Error::CasFailed(None)) => return Ok(PageGet::Free),
                _ => (),
            }
        } else if !fetched.is_empty() ||
                   fix_up_length >= self.config.cache_fixup_threshold
        {
            trace!(
                "fixing up pid {} with {} traversed frags",
                pid,
                fix_up_length
            );
            let mut new_entries = Vec::with_capacity(lids.len());

            let (head_lsn, head_lid) = lids.remove(0);
            let head_entry =
                CacheEntry::MergedResident(merged.clone(), head_lsn, head_lid);
            new_entries.push(head_entry);

            let mut tail = if let Some((lsn, lid)) = lids.pop() {
                Some(CacheEntry::Flush(lsn, lid))
            } else {
                None
            };

            for (lsn, lid) in lids {
                new_entries.push(CacheEntry::PartialFlush(lsn, lid));
            }

            if let Some(tail) = tail.take() {
                new_entries.push(tail);
            }

            let node = node_from_frag_vec(new_entries);

            debug_delay();
            let res = unsafe {
                stack_ptr.deref().cas(head, node.into_shared(guard), guard)
            };
            if let Ok(new_head) = res {
                head = new_head;
            } else {
                // NB explicitly DON'T update head, as our witnessed
                // entries do NOT contain the latest state. This
                // may not matter to callers who only care about
                // reading, but maybe we should signal that it's
                // out of date for those who page_in in an attempt
                // to modify!
            }
        }

        Ok(PageGet::Materialized(merged, head))
    }

    fn page_out<'g>(
        &self,
        to_evict: Vec<PageID>,
        guard: &'g Guard,
    ) -> CacheResult<(), ()> {
        let _measure = Measure::new(&M.page_out);
        for pid in to_evict {
            let stack_ptr = self.inner.get(pid, guard);
            if stack_ptr.is_none() {
                continue;
            }

            let stack_ptr = stack_ptr.unwrap();

            let head = unsafe { stack_ptr.deref().head(guard) };
            let stack_iter = StackIter::from_ptr(head, guard);

            let mut cache_entries: Vec<CacheEntry<P>> =
                stack_iter.map(|ptr| (*ptr).clone()).collect();

            // ensure the last entry is a Flush
            let last = cache_entries.pop().and_then(|last_ce| match last_ce {
                CacheEntry::MergedResident(_, lsn, lid) |
                CacheEntry::Resident(_, lsn, lid) |
                CacheEntry::Flush(lsn, lid) => {
                    // NB stabilize the most recent LSN before
                    // paging out! This SHOULD very rarely block...
                    // TODO should propagate error instead of unwrap
                    self.log.make_stable(lsn).unwrap();
                    Some(CacheEntry::Flush(lsn, lid))
                }
                CacheEntry::PartialFlush(_, _) => {
                    panic!("got PartialFlush at end of stack...")
                }
                CacheEntry::Free(_, _) => {
                    // don't actually evict this. this leads to
                    // a discrepency in the Lru perceived size
                    // and the real size, but this should be
                    // minimal in anticipated workloads.
                    None
                }
            });

            if last.is_none() {
                return Ok(());
            }

            let mut new_stack = Vec::with_capacity(cache_entries.len() + 1);
            for entry in cache_entries {
                match entry {
                    CacheEntry::PartialFlush(lsn, lid) |
                    CacheEntry::MergedResident(_, lsn, lid) |
                    CacheEntry::Resident(_, lsn, lid) => {
                        new_stack.push(CacheEntry::PartialFlush(lsn, lid));
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
            new_stack.push(last.unwrap());
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
        lid: LogID,
    ) -> CacheResult<P, Option<RayonPagePtr<'g, P>>> {
        self.pull(lsn, lid).map_err(|e1| e1.danger_cast())
    }

    fn pull<'g>(
        &self,
        lsn: Lsn,
        lid: LogID,
    ) -> CacheResult<P, Option<PagePtr<'g, P>>> {
        trace!("pulling lsn {} lid {} from disk", lsn, lid);
        let _measure = Measure::new(&M.pull);
        let bytes = match self.log.read(lsn, lid).map_err(|_| ()) {
            Ok(LogRead::Flush(read_lsn, data, _len)) => {
                assert_eq!(
                    read_lsn,
                    lsn,
                    "expected lsn {} on pull of lid {}, \
                    but got lsn {} instead",
                    lsn,
                    lid,
                    read_lsn
                );
                Ok(data)
            }
            // FIXME 'read invalid data at lid 66244182' in cycle test
            _other => Err(Error::Corruption {
                at: lid,
            }),
        }?;

        let logged_update = measure(&M.deserialize, || {
            deserialize::<LoggedUpdate<P>>(&*bytes)
                .map_err(|_| ())
                .expect("failed to deserialize data")
        });

        match logged_update.update {
            Update::Compact(page_frag) |
            Update::Append(page_frag) => Ok(page_frag),
            _ => {
                return Err(Error::ReportableBug(
                    "non-append/compact found in pull".to_owned(),
                ))
            }
        }
    }

    // caller is expected to have instantiated self.last_snapshot
    // in recovery already.
    fn advance_snapshot(&self) -> CacheResult<(), ()> {
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
            error!("failed to flush log during advance_snapshot: {}", e);
            self.log.with_sa(|sa| sa.resume_rewriting());
            *snapshot_opt = Some(last_snapshot);
            return Err(e);
        }

        // we disable rewriting so that our log becomes append-only,
        // allowing us to iterate through it without corrupting ourselves.
        // NB must be called after taking the snapshot mutex.
        self.log.with_sa(|sa| sa.pause_rewriting());

        let max_lsn = last_snapshot.max_lsn;
        let start_lsn = max_lsn - (max_lsn % self.config.io_buf_size as Lsn);

        debug!(
            "snapshot starting from offset {} to the segment containing ~{}",
            last_snapshot.max_lsn,
            self.log.stable_offset(),
        );

        let iter = self.log.iter_from(start_lsn);

        let res =
            advance_snapshot::<PM, P, R>(iter, last_snapshot, &self.config);

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
        let snapshot = self.last_snapshot.try_lock().unwrap().clone().unwrap();

        self.max_pid.store(snapshot.max_pid, SeqCst);

        let mut snapshot_free = snapshot.free.clone();

        for (pid, state) in &snapshot.pt {
            trace!("load_snapshot page {} {:?}", pid, state);

            let stack = Stack::default();

            match state {
                &PageState::Present(ref lids) => {
                    let (base_lsn, base_lid) = lids[0];

                    stack.push(CacheEntry::Flush(base_lsn, base_lid));

                    for &(lsn, lid) in &lids[1..] {
                        stack.push(CacheEntry::PartialFlush(lsn, lid));
                    }
                }
                &PageState::Free(lsn, lid) => {
                    self.free.lock().unwrap().push(*pid);
                    stack.push(CacheEntry::Free(lsn, lid));
                    snapshot_free.remove(&pid);
                }
                &PageState::Allocated(_lsn, _lid) => {
                    assert!(!snapshot.free.contains(pid));
                    // empty stack with null ptr head implies Allocated
                }
            }

            self.inner.insert(*pid, stack).unwrap();
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

fn lids_from_stack<'g, P: Send + Sync>(
    head_ptr: PagePtr<'g, P>,
    guard: &'g Guard,
) -> Vec<LogID> {
    // generate a list of the old log ID's
    let stack_iter = StackIter::from_ptr(head_ptr, guard);

    let mut lids = vec![];
    for cache_entry_ptr in stack_iter {
        match *cache_entry_ptr {
            CacheEntry::Resident(_, _, ref lid) |
            CacheEntry::MergedResident(_, _, ref lid) |
            CacheEntry::PartialFlush(_, ref lid) |
            CacheEntry::Free(_, ref lid) |
            CacheEntry::Flush(_, ref lid) => {
                lids.push(*lid);
            }
        }
    }
    lids
}

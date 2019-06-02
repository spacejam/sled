use std::{
    borrow::Cow,
    collections::BinaryHeap,
    marker::PhantomData,
    ops::Deref,
    sync::{Arc, Mutex},
};

use rayon::prelude::*;

use super::*;

type PagePtrInner<'g, P> = Shared<'g, Node<CacheEntry<P>>>;

/// A pointer to shared lock-free state bound by a pinned epoch's lifetime.
#[derive(Debug, Clone, PartialEq, Copy)]
pub struct PagePtr<'g, P>
where
    P: 'static + Send,
{
    cached_ptr: PagePtrInner<'g, P>,
    wts: u64,
}

impl<'g, P> PagePtr<'g, P>
where
    P: 'static + Send,
{
    /// The last Lsn number for the head of this page
    pub fn last_lsn(&self) -> Lsn {
        unsafe { self.cached_ptr.deref().deref().lsn() }
    }

    unsafe fn deref_merged_resident(&self) -> &'g P
    where
        P: Debug,
    {
        self.cached_ptr.deref().deref().deref_merged_resident()
    }
}

unsafe impl<'g, P> Send for PagePtr<'g, P> where P: Send {}
unsafe impl<'g, P> Sync for PagePtr<'g, P> where P: Send + Sync {}

/// Points to either a memory location or a disk location to page-in data from.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheEntry<M: Send> {
    /// A cache item that contains the most recent fully-merged page state, also in secondary
    /// storage.
    MergedResident(M, u64, Lsn, DiskPtr),
    /// A cache item that is in memory, and also in secondary storage.
    Resident(M, u64, Lsn, DiskPtr),
    /// A cache item that is present in secondary storage.
    PartialFlush(u64, Lsn, DiskPtr),
    /// A cache item that is present in secondary storage, and is the base segment
    /// of a page.
    Flush(u64, Lsn, DiskPtr),
    /// A freed page tombstone.
    Free(u64, Lsn, DiskPtr),
    /// The persisted counter page
    Counter(u64, u64, Lsn, DiskPtr),
    /// The persisted meta page
    Meta(Meta, u64, Lsn, DiskPtr),
}

impl<M: Send> CacheEntry<M> {
    fn ptr(&self) -> DiskPtr {
        use self::CacheEntry::*;

        match self {
            MergedResident(.., ptr)
            | Resident(.., ptr)
            | PartialFlush(.., ptr)
            | Flush(.., ptr)
            | Free(.., ptr)
            | Counter(.., ptr)
            | Meta(.., ptr) => *ptr,
        }
    }

    fn lsn(&self) -> Lsn {
        use self::CacheEntry::*;

        match self {
            MergedResident(_, _, lsn, ..)
            | Resident(_, _, lsn, ..)
            | PartialFlush(_, lsn, ..)
            | Flush(_, lsn, ..)
            | Free(_, lsn, ..)
            | Counter(.., lsn, _)
            | Meta(.., lsn, _) => *lsn,
        }
    }

    fn ts(&self) -> u64 {
        use self::CacheEntry::*;

        match self {
            MergedResident(_, ts, ..)
            | Resident(_, ts, ..)
            | PartialFlush(ts, ..)
            | Flush(ts, ..)
            | Free(ts, ..)
            | Counter(_, ts, ..)
            | Meta(_, ts, ..) => *ts,
        }
    }

    fn ptr_ref_mut(&mut self) -> &mut DiskPtr {
        use self::CacheEntry::*;

        match self {
            MergedResident(.., ptr)
            | Resident(.., ptr)
            | PartialFlush(.., ptr)
            | Flush(.., ptr)
            | Free(.., ptr)
            | Counter(.., ptr)
            | Meta(.., ptr) => ptr,
        }
    }

    fn is_free(&self) -> bool {
        if let CacheEntry::Free(..) = self {
            true
        } else {
            false
        }
    }

    unsafe fn deref_merged_resident(&self) -> &M
    where
        M: Debug,
    {
        match self {
            CacheEntry::MergedResident(m, ..) => m,
            other => {
                panic!("called deref_merged_resident on {:?}", other);
            }
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
    Counter(u64),
    Meta(Meta),
}

impl<P> Update<P>
where
    P: DeserializeOwned + Serialize,
    Self: Debug,
{
    pub(crate) fn is_compact(&self) -> bool {
        if let Update::Compact(_) = self {
            true
        } else {
            false
        }
    }

    fn into_frag(self) -> P {
        match self {
            Update::Append(frag) | Update::Compact(frag) => frag,
            other => {
                panic!("called into_frag on non-Append/Compact: {:?}", other)
            }
        }
    }

    fn as_frag(&self) -> &P {
        match self {
            Update::Append(frag) | Update::Compact(frag) => frag,
            other => {
                panic!("called as_frag on non-Append/Compact: {:?}", other)
            }
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
pub struct RecoveryGuard<'a> {
    batch_res: Reservation<'a>,
}

impl<'a> RecoveryGuard<'a> {
    /// Writes the last LSN for a batch into an earlier
    /// reservation, releasing it.
    pub fn seal_batch(mut self) -> Result<()> {
        let max_reserved = self
            .batch_res
            .log
            .iobufs
            .max_reserved_lsn
            .load(std::sync::atomic::Ordering::Acquire);
        self.batch_res.mark_writebatch(max_reserved);
        self.batch_res.complete().map(|_| ())
    }

    /// Returns the LSN representing the beginning of this
    /// batch.
    pub fn lsn(&self) -> Lsn {
        self.batch_res.lsn()
    }
}

/// A lock-free pagecache which supports fragmented pages
/// for dramatically improving write throughput.
///
/// # Working with the `PageCache`
///
/// ```
/// use pagecache::{pin, Materializer, Config};
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
///     // Used to merge chains of partial pages into a form
///     // that is useful for the `PageCache` owner.
///     fn merge<'a, I>(frags: I, _config: &Config) -> Self::PageFrag
///     where
///         I: IntoIterator<Item = &'a Self::PageFrag>,
///     {
///         frags.into_iter().fold(String::new(), |mut acc, ref s| {
///             acc.push_str(&*s);
///             acc
///         })
///     }
///
///     // Used to determine the resident size for this item in cache.
///     fn size_in_bytes(frag: &String) -> u64 {
///         (std::mem::size_of::<String>() + frag.as_bytes().len()) as u64
///     }
///
/// }
///
/// fn main() {
///     let config = pagecache::ConfigBuilder::new().temporary(true).build();
///     let pc: pagecache::PageCache<TestMaterializer, _> =
///         pagecache::PageCache::start(config).unwrap();
///     {
///         // We begin by initiating a new transaction, which
///         // will prevent any witnessable memory from being
///         // reclaimed before we drop this object.
///         let tx = pc.begin().unwrap();
///
///         // The first item in a page should be set using allocate,
///         // which signals that this is the beginning of a new
///         // page history.
///         let (id, mut key) = pc.allocate("a".to_owned(), &tx).unwrap();
///
///         // Subsequent atomic updates should be added with link.
///         key = pc.link(id, key, "b".to_owned(), &tx).unwrap().unwrap();
///         key = pc.link(id, key, "c".to_owned(), &tx).unwrap().unwrap();
///
///         // When getting a page, the provided `Materializer` is
///         // used to merge all pages together.
///         let (consolidated, mut key) = pc.get(id, &tx).unwrap().unwrap();
///
///         assert_eq!(*consolidated, "abc".to_owned());
///
///         // You can completely rewrite a page by using `replace`:
///         key = pc.replace(id, key, "d".into(), &tx).unwrap().unwrap();
///
///         let (consolidated, key) = pc.get(id, &tx).unwrap().unwrap();
///
///         assert_eq!(*consolidated, "d".to_owned());
///     }
/// }
/// ```
pub struct PageCache<PM, P>
where
    P: Clone + 'static + Send + Sync + Serialize + DeserializeOwned,
{
    _materializer: PhantomData<PM>,
    config: Config,
    inner: Arc<PageTable<PageTableEntry<P>>>,
    max_pid: AtomicU64,
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

struct PageTableEntry<P>
where
    P: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    stack: Stack<CacheEntry<P>>,
    rts: AtomicLsn,
    pending: AtomicLsn,
}

unsafe impl<PM, P> Send for PageCache<PM, P>
where
    PM: Send + Sync,
    P: Clone + 'static + Send + Sync + Serialize + DeserializeOwned,
{
}

unsafe impl<PM, P> Sync for PageCache<PM, P>
where
    PM: Send + Sync,
    P: Clone + 'static + Send + Sync + Serialize + DeserializeOwned,
{
}

impl<PM, P> Debug for PageCache<PM, P>
where
    PM: Send + Sync,
    P: Clone + Debug + Send + Sync + Serialize + DeserializeOwned,
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
impl<PM, P> Drop for PageCache<PM, P>
where
    P: Clone + 'static + Send + Sync + Serialize + DeserializeOwned,
{
    fn drop(&mut self) {
        use std::collections::HashMap;
        let mut pages_before_restart: HashMap<PageId, Vec<DiskPtr>> =
            HashMap::new();

        let tx = Tx::new(0);

        for pid in 0..self.max_pid.load(SeqCst) {
            let pte = self.inner.get(pid, &tx);
            if pte.is_none() {
                continue;
            }
            let head = unsafe { pte.unwrap().deref().stack.head(&tx) };
            let ptrs = ptrs_from_stack(head, &tx);
            pages_before_restart.insert(pid, ptrs);
        }

        self.config
            .event_log
            .pages_before_restart(pages_before_restart);
    }
}

impl<PM, P> PageCache<PM, P>
where
    PM: Materializer<PageFrag = P>,
    PM: 'static + Send + Sync,
    P: 'static + Debug + Clone + Serialize + DeserializeOwned + Send + Sync,
{
    /// Instantiate a new `PageCache`.
    pub fn start(config: Config) -> Result<PageCache<PM, P>> {
        config.reset_global_error();

        let cache_capacity = config.cache_capacity;
        let cache_shard_bits = config.cache_bits;
        let lru = Lru::new(cache_capacity, cache_shard_bits);

        // try to pull any existing snapshot off disk, and
        // apply any new data to it to "catch-up" the
        // snapshot before loading it.
        let snapshot = read_snapshot_or_default::<PM, P>(&config)?;

        let mut pc = PageCache {
            _materializer: PhantomData,
            config: config.clone(),
            inner: Arc::new(PageTable::default()),
            max_pid: AtomicU64::new(0),
            free: Arc::new(Mutex::new(BinaryHeap::new())),
            log: Log::start(config, snapshot.clone())?,
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

        let mut was_recovered = true;

        {
            // subscope required because pc.begin() borrows pc

            let tx = pc.begin()?;

            if let Err(Error::ReportableBug(..)) = pc.get_meta(&tx) {
                // set up meta
                was_recovered = false;

                let meta_update = Update::Meta(Meta::default());

                let (meta_id, _) = pc.allocate_inner(meta_update, &tx)?;

                assert_eq!(
                    meta_id,
                    META_PID,
                    "we expect the meta page to have pid {}, but it had pid {} instead",
                    META_PID,
                    meta_id,
                );
            }

            if let Err(Error::ReportableBug(..)) = pc.get_idgen(&tx) {
                // set up idgen
                was_recovered = false;

                let counter_update = Update::Counter(0);

                let (counter_id, _) = pc.allocate_inner(counter_update, &tx)?;

                assert_eq!(
                    counter_id,
                    COUNTER_PID,
                    "we expect the counter to have pid {}, but it had pid {} instead",
                    COUNTER_PID,
                    counter_id,
                );
            }

            let (_, counter) = pc.get_idgen(&tx)?;
            let idgen_recovery =
                counter + (2 * pc.config.idgen_persist_interval);
            let idgen_persists = counter / pc.config.idgen_persist_interval
                * pc.config.idgen_persist_interval;

            pc.idgen.store(idgen_recovery, SeqCst);
            pc.idgen_persists.store(idgen_persists, SeqCst);
        }

        pc.was_recovered = was_recovered;

        Ok(pc)
    }

    /// Flushes any pending IO buffers to disk to ensure durability.
    /// Returns the number of bytes written during this call.
    pub fn flush(&self) -> Result<usize> {
        self.log.flush()
    }

    /// Begins a transaction.
    pub fn begin(&self) -> Result<Tx<PM, P>> {
        Ok(Tx::new(&self, self.generate_id()?))
    }

    /// Create a new page, trying to reuse old freed pages if possible
    /// to maximize underlying `PageTable` pointer density. Returns
    /// the page ID and its pointer for use in future atomic `replace`
    /// and `link` operations.
    pub fn allocate<'g>(
        &self,
        new: P,
        tx: &'g Tx<PM, P>,
    ) -> Result<(PageId, PagePtr<'g, P>)> {
        self.allocate_inner(Update::Compact(new), tx)
    }

    /// Attempt to opportunistically rewrite data from a Draining
    /// segment of the file to help with space amplification.
    /// Returns Ok(true) if we had the opportunity to attempt to
    /// move a page. Returns Ok(false) if there were no pages
    /// to GC. Returns an Err if we encountered an IO problem
    /// while performing this GC.
    pub fn attempt_gc(&self) -> Result<bool> {
        let tx = Tx::new(&self, 0);
        let to_clean = self.log.with_sa(|sa| sa.clean(COUNTER_PID));
        if let Some(to_clean) = to_clean {
            self.rewrite_page(to_clean, &tx).map(|_| true)
        } else {
            Ok(false)
        }
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
    pub fn pin_log<'a>(&'a self) -> Result<RecoveryGuard<'a>> {
        let batch_res = self.log.reserve(&[0; std::mem::size_of::<Lsn>()])?;
        Ok(RecoveryGuard { batch_res })
    }

    #[doc(hidden)]
    #[cfg(feature = "failpoints")]
    pub fn set_failpoint(&self, e: Error) {
        if let Error::FailPoint = e {
            self.config.set_global_error(e);

            // wake up any waiting threads
            // so they don't stall forever
            self.log.iobufs.interval_updated.notify_all();
        }
    }

    fn allocate_inner<'g>(
        &self,
        new: Update<P>,
        tx: &'g Tx<PM, P>,
    ) -> Result<(PageId, PagePtr<'g, P>)> {
        let (pid, key) = if let Some(pid) = self.free.lock().unwrap().pop() {
            trace!("re-allocating pid {}", pid);

            let pte_ptr = match self.inner.get(pid, &tx.guard) {
                None => panic!(
                    "expected to find existing stack \
                     for re-allocated pid {}",
                    pid
                ),
                Some(p) => p,
            };

            let head = unsafe { pte_ptr.deref().stack.head(&tx.guard) };

            let mut stack_iter = StackIter::from_ptr(head, &tx.guard);

            match stack_iter.next() {
                Some(CacheEntry::Free(wts, _lsn, _ptr)) => (
                    pid,
                    PagePtr {
                        cached_ptr: head,
                        wts: *wts,
                    },
                ),
                other => panic!(
                    "failed to re-allocate pid {} which \
                     contained unexpected state {:?}",
                    pid, other
                ),
            }
        } else {
            let pid = self.max_pid.fetch_add(1, SeqCst);

            trace!("allocating pid {} for the first time", pid);

            let new_stack = Stack::default();
            let new_pte = PageTableEntry {
                stack: new_stack,
                rts: AtomicLsn::new(0),
                pending: AtomicLsn::new(0),
            };

            let pte_ptr = Owned::new(new_pte).into_shared(&tx.guard);

            self.inner
                .cas(pid, Shared::null(), pte_ptr, &tx.guard)
                .expect(
                    "allocating a fresh new page should \
                     never conflict on existing data",
                );

            (
                pid,
                PagePtr {
                    cached_ptr: Shared::null(),
                    wts: 0,
                },
            )
        };

        let new_ptr =
            self.cas_page(pid, key, new, false, tx)?
                .unwrap_or_else(|e| {
                    panic!(
                        "should always be able to install \
                         a new page during allocation, but \
                         failed for pid {}: {:?}",
                        pid, e
                    )
                });

        Ok((pid, new_ptr))
    }

    /// Free a particular page.
    pub fn free<'g>(
        &self,
        pid: PageId,
        old: PagePtr<'g, P>,
        tx: &'g Tx<PM, P>,
    ) -> Result<CasResult<'g, P, ()>> {
        trace!("attempting to free pid {}", pid);

        if pid == META_PID || pid == COUNTER_PID {
            return Err(Error::Unsupported(
                "you are not able to free the first \
                 couple pages, which are allocated \
                 for system internal purposes"
                    .into(),
            ));
        }

        let new_ptr = self.cas_page(pid, old, Update::Free, false, tx)?;

        let free = self.free.clone();
        tx.guard.defer(move || {
            let mut free = free.lock().unwrap();
            // panic if we double-freed a page
            if free.iter().any(|e| e == &pid) {
                panic!("page {} was double-freed", pid);
            }

            free.push(pid);
        });

        Ok(new_ptr.map_err(|o| o.map(|(ptr, _)| (ptr, ()))))
    }

    /// Try to atomically add a `PageFrag` to the page.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the atomic append fails.
    pub fn link<'g>(
        &'g self,
        pid: PageId,
        mut old: PagePtr<'g, P>,
        new: P,
        tx: &'g Tx<PM, P>,
    ) -> Result<CasResult<'g, P, P>> {
        let _measure = Measure::new(&M.link_page);

        trace!("linking pid {} with {:?}", pid, new);

        let pte_ptr = match self.inner.get(pid, &tx.guard) {
            None => return Ok(Err(None)),
            Some(p) => p,
        };

        let prepend: LoggedUpdate<P> = LoggedUpdate {
            pid,
            update: Update::Append(new),
        };

        let bytes = measure(&M.serialize, || serialize(&prepend).unwrap());

        let mut new = if let Update::Append(new) = prepend.update {
            let cache_entry =
                CacheEntry::Resident(new, 0, 0, DiskPtr::Inline(0));

            let node = Node {
                inner: cache_entry,
                // NB this must be null
                // to prevent double-frees
                // if we encounter an IO error
                // and fee our Owned version!
                next: Atomic::null(),
            };

            Some(Owned::new(node))
        } else {
            unreachable!();
        };

        loop {
            let log_reservation = self.log.reserve(&bytes)?;

            let lsn = log_reservation.lsn();
            let ptr = log_reservation.ptr();

            // NB the setting of the timestamp is quite
            // correctness-critical! We use the wts to
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
            let ts = old.wts + 1;

            let mut cache_entry = new.take().unwrap();

            match cache_entry.inner {
                CacheEntry::Resident(
                    _,
                    ref mut ce_ts,
                    ref mut ce_lsn,
                    ref mut ce_ptr,
                ) => {
                    *ce_ts = ts;
                    *ce_lsn = lsn;
                    *ce_ptr = ptr;
                }
                _ => panic!("should only be working with Resident entries"),
            }

            debug_delay();
            let result = unsafe {
                pte_ptr.deref().stack.cap_node(
                    old.cached_ptr,
                    cache_entry,
                    &tx.guard,
                )
            };

            match result {
                Ok(cached_ptr) => {
                    trace!("link of pid {} succeeded", pid);
                    let skip_mark = {
                        // if the last update for this page was also
                        // sent to this segment, we can skip marking it
                        let previous_head_lsn = old.last_lsn();

                        assert_ne!(previous_head_lsn, 0);

                        let previous_lsn_segment =
                            previous_head_lsn / self.config.io_buf_size as i64;
                        let new_lsn_segment =
                            lsn / self.config.io_buf_size as i64;

                        previous_lsn_segment == new_lsn_segment
                    };
                    let to_clean = if skip_mark {
                        self.log.with_sa(|sa| sa.clean(pid))
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
                    log_reservation.complete()?;

                    if let Some(to_clean) = to_clean {
                        self.rewrite_page(to_clean, tx)?;
                    }

                    let count = self.updates.fetch_add(1, SeqCst) + 1;
                    let should_snapshot =
                        count % self.config.snapshot_after_ops == 0;
                    if should_snapshot {
                        self.advance_snapshot()?;
                    }

                    return Ok(Ok(PagePtr {
                        cached_ptr,
                        wts: ts,
                    }));
                }
                Err((actual_ptr, returned_new)) => {
                    trace!("link of pid {} failed", pid);
                    log_reservation.abort()?;
                    let returned = if let CacheEntry::Resident(ref new, ..) =
                        returned_new.deref().inner
                    {
                        new
                    } else {
                        panic!("should only return Resident entries");
                    };

                    let actual_ts = unsafe { actual_ptr.deref().ts() };
                    if actual_ts != old.wts {
                        return Ok(Err(Some((
                            PagePtr {
                                cached_ptr: actual_ptr,
                                wts: actual_ts,
                            },
                            returned.clone(),
                        ))));
                    }
                    new = Some(returned_new);
                    old = PagePtr {
                        cached_ptr: actual_ptr,
                        wts: actual_ts,
                    };
                }
            }
        }
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
        tx: &'g Tx<PM, P>,
    ) -> Result<CasResult<'g, P, P>> {
        let _measure = Measure::new(&M.replace_page);

        trace!("replacing pid {} with {:?}", pid, new);

        let result =
            self.cas_page(pid, old, Update::Compact(new), false, tx)?;

        let to_clean = self.log.with_sa(|sa| sa.clean(pid));

        if let Some(to_clean) = to_clean {
            assert_ne!(pid, to_clean);
            self.rewrite_page(to_clean, tx)?;
        }

        let count = self.updates.fetch_add(1, SeqCst) + 1;
        let should_snapshot = count % self.config.snapshot_after_ops == 0;
        if should_snapshot {
            self.advance_snapshot()?;
        }

        Ok(result.map_err(|fail| {
            let (ptr, shared) = fail.unwrap();
            if let Update::Compact(rejected_new) = shared {
                Some((ptr, rejected_new))
            } else {
                unreachable!();
            }
        }))
    }

    // rewrite a page so we can reuse the segment that it is
    // (at least partially) located in. This happens when a
    // segment has had enough resident page fragments moved
    // away to trigger the `segment_cleanup_threshold`.
    fn rewrite_page<'g>(&self, pid: PageId, tx: &'g Tx<PM, P>) -> Result<()> {
        let _measure = Measure::new(&M.rewrite_page);

        trace!("rewriting pid {}", pid);

        let pte_ptr = match self.inner.get(pid, &tx.guard) {
            None => {
                trace!("rewriting pid {} failed (no longer exists)", pid);
                return Ok(());
            }
            Some(p) => p,
        };

        debug_delay();
        let head = unsafe { pte_ptr.deref().stack.head(&tx.guard) };
        let stack_iter = StackIter::from_ptr(head, &tx.guard);
        let cache_entries: Vec<_> = stack_iter.collect();

        // if the page is just a single blob pointer, rewrite it.
        if cache_entries.len() == 1 && cache_entries[0].ptr().is_blob() {
            trace!("rewriting blob with pid {}", pid);
            let blob_ptr = cache_entries[0].ptr().blob().1;

            let log_reservation = self.log.rewrite_blob_ptr(blob_ptr)?;

            let new_ptr = log_reservation.ptr();
            let mut new_cache_entry = cache_entries[0].clone();

            *new_cache_entry.ptr_ref_mut() = new_ptr;

            let node = node_from_frag_vec(vec![new_cache_entry]);

            debug_delay();
            let result =
                unsafe { pte_ptr.deref().stack.cas(head, node, &tx.guard) };

            if result.is_ok() {
                let ptrs = ptrs_from_stack(head, tx);
                let lsn = log_reservation.lsn();

                self.log
                    .with_sa(|sa| sa.mark_replace(pid, lsn, ptrs, new_ptr))?;

                // NB complete must happen AFTER calls to SA, because
                // when the iobuf's n_writers hits 0, we may transition
                // the segment to inactive, resulting in a race otherwise.
                log_reservation.complete()?;

                trace!("rewriting pid {} succeeded", pid);

                Ok(())
            } else {
                log_reservation.abort()?;

                trace!("rewriting pid {} failed", pid);

                Ok(())
            }
        } else {
            trace!("rewriting page with pid {}", pid);

            // page-in whole page with a get
            let (key, update) = if pid == META_PID {
                let (key, meta) = self.get_meta(tx)?;
                (key, Update::Meta(meta.clone()))
            } else if pid == COUNTER_PID {
                let (key, counter) = self.get_idgen(tx)?;
                (key, Update::Counter(counter))
            } else {
                match self.get(pid, tx)? {
                    Some((key, entries)) => {
                        let _measure = Measure::new(&M.merge_page);

                        assert!(!entries.is_empty());

                        let combined_iter = entries.into_iter().rev();

                        let merged = PM::merge(combined_iter, &self.config);

                        (key, Update::Compact(merged))
                    }
                    None => {
                        let pte_ptr = match self.inner.get(pid, &tx.guard) {
                            None => panic!(
                                "expected to find existing stack \
                                 for freed pid {}",
                                pid
                            ),
                            Some(p) => p,
                        };

                        let head =
                            unsafe { pte_ptr.deref().stack.head(&tx.guard) };

                        let mut stack_iter =
                            StackIter::from_ptr(head, &tx.guard);

                        match stack_iter.next() {
                            Some(CacheEntry::Free(wts, _lsn, _ptr)) => (
                                PagePtr {
                                    cached_ptr: head,
                                    wts: *wts,
                                },
                                Update::Free,
                            ),
                            other => {
                                debug!(
                                    "when rewriting pid {} \
                                     we encountered a rewritten \
                                     node with a frag {:?} that \
                                     we previously witnessed a Free \
                                     for (PageCache::get returned None), \
                                     assuming we can just return now since \
                                     the Free was replace'd",
                                    pid, other
                                );
                                return Ok(());
                            }
                        }
                    }
                }
            };

            self.cas_page(pid, key, update, true, tx).map(|res| {
                trace!("rewriting pid {} success: {}", pid, res.is_ok());
            })
        }
    }

    fn cas_page<'g>(
        &self,
        pid: PageId,
        mut old: PagePtr<'g, P>,
        new: Update<P>,
        is_rewrite: bool,
        tx: &'g Tx<PM, P>,
    ) -> Result<CasResult<'g, P, Update<P>>> {
        trace!(
            "cas_page called on pid {} to {:?} with old ts {:?}",
            pid,
            new,
            old.wts
        );
        let pte_ptr = match self.inner.get(pid, &tx.guard) {
            None => {
                trace!("early-returning from cas_page, no stack found");
                return Ok(Err(None));
            }
            Some(p) => p,
        };

        let replace: LoggedUpdate<P> = LoggedUpdate { pid, update: new };
        let bytes = measure(&M.serialize, || serialize(&replace).unwrap());
        let mut new = Some(replace.update);

        loop {
            let log_reservation = self.log.reserve(&bytes)?;
            let lsn = log_reservation.lsn();
            let new_ptr = log_reservation.ptr();

            // NB the setting of the timestamp is quite
            // correctness-critical! We use the wts to
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
            let ts = if is_rewrite { old.wts } else { old.wts + 1 };

            let cache_entry = match new.take().unwrap() {
                Update::Compact(m) => {
                    CacheEntry::MergedResident(m, ts, lsn, new_ptr)
                }
                Update::Free => CacheEntry::Free(ts, lsn, new_ptr),
                Update::Counter(counter) => {
                    CacheEntry::Counter(counter, ts, lsn, new_ptr)
                }
                Update::Meta(meta) => CacheEntry::Meta(meta, ts, lsn, new_ptr),
                Update::Append(_) => {
                    panic!("tried to cas a page using an Append")
                }
            };

            let node = node_from_frag_vec(vec![cache_entry]);

            debug_delay();
            let result = unsafe {
                pte_ptr.deref().stack.cas(old.cached_ptr, node, &tx.guard)
            };

            match result {
                Ok(cached_ptr) => {
                    trace!("cas_page succeeded on pid {}", pid);
                    let ptrs = ptrs_from_stack(old.cached_ptr, tx);

                    self.log.with_sa(|sa| {
                        sa.mark_replace(pid, lsn, ptrs, new_ptr)
                    })?;

                    // NB complete must happen AFTER calls to SA, because
                    // when the iobuf's n_writers hits 0, we may transition
                    // the segment to inactive, resulting in a race otherwise.
                    log_reservation.complete()?;
                    return Ok(Ok(PagePtr {
                        cached_ptr,
                        wts: ts,
                    }));
                }
                Err((actual_ptr, returned_new)) => {
                    trace!("cas_page failed on pid {}", pid);
                    log_reservation.abort()?;

                    let returned_new =
                        match returned_new.into_box().inner.clone() {
                            CacheEntry::MergedResident(m, ..) => {
                                Update::Compact(m)
                            }
                            CacheEntry::Free(..) => Update::Free,
                            CacheEntry::Counter(counter, ..) => {
                                Update::Counter(counter)
                            }
                            CacheEntry::Meta(meta, ..) => Update::Meta(meta),
                            _ => panic!("tried to cas a page using an Append"),
                        };
                    let actual_ts = unsafe { actual_ptr.deref().ts() };

                    if actual_ts != old.wts {
                        return Ok(Err(Some((
                            PagePtr {
                                cached_ptr: actual_ptr,
                                wts: actual_ts,
                            },
                            returned_new,
                        ))));
                    }
                    trace!(
                        "retrying CAS on pid {} with same wts of {}",
                        pid,
                        old.wts
                    );
                    old = PagePtr {
                        cached_ptr: actual_ptr,
                        wts: old.wts,
                    };
                    new = Some(returned_new);
                }
            } // match cas result
        } // loop
    }

    /// Retrieve the current meta page
    pub(crate) fn get_meta<'g>(
        &self,
        tx: &'g Tx<PM, P>,
    ) -> Result<(PagePtr<'g, P>, &'g Meta)> {
        trace!("getting page iter for META");

        let pte_ptr = match self.inner.get(META_PID, &tx.guard) {
            None => {
                return Err(Error::ReportableBug(
                    "failed to retrieve META page \
                     which should always be present"
                        .into(),
                ));
            }
            Some(p) => p,
        };

        let head = unsafe { pte_ptr.deref().stack.head(&tx.guard) };

        match StackIter::from_ptr(head, &tx.guard).next() {
            Some(CacheEntry::Meta(m, wts, ..)) => Ok((
                PagePtr {
                    cached_ptr: head,
                    wts: *wts,
                },
                m,
            )),
            Some(CacheEntry::Flush(wts, lsn, disk_ptr)) => {
                let update = self.pull(*lsn, *disk_ptr)?;
                let ptr = PagePtr {
                    cached_ptr: head,
                    wts: *wts,
                };
                let _ = self.cas_page(META_PID, ptr, update, false, tx)?;
                self.get_meta(tx)
            }
            _ => Err(Error::ReportableBug(
                "failed to retrieve META page \
                 which should always be present"
                    .into(),
            )),
        }
    }

    /// Retrieve the current persisted IDGEN value
    pub(crate) fn get_idgen<'g>(
        &self,
        tx: &'g Tx<PM, P>,
    ) -> Result<(PagePtr<'g, P>, u64)> {
        trace!("getting page iter for idgen");

        let pte_ptr = match self.inner.get(COUNTER_PID, &tx.guard) {
            None => {
                return Err(Error::ReportableBug(
                    "failed to retrieve idgen page \
                     which should always be present"
                        .into(),
                ))
            }
            Some(p) => p,
        };

        let head = unsafe { pte_ptr.deref().stack.head(&tx.guard) };

        match StackIter::from_ptr(head, &tx.guard).next() {
            Some(CacheEntry::Counter(counter, wts, ..)) => Ok((
                PagePtr {
                    cached_ptr: head,
                    wts: *wts,
                },
                *counter,
            )),
            Some(CacheEntry::Flush(wts, lsn, disk_ptr)) => {
                let update = self.pull(*lsn, *disk_ptr)?;
                let ptr = PagePtr {
                    cached_ptr: head,
                    wts: *wts,
                };
                let _ = self.cas_page(COUNTER_PID, ptr, update, false, tx)?;
                self.get_idgen(tx)
            }
            _ => Err(Error::ReportableBug(
                "failed to retrieve idgen page \
                 which should always be present"
                    .into(),
            )),
        }
    }

    /// Try to retrieve a page by its logical ID.
    pub fn get<'g>(
        &self,
        pid: PageId,
        tx: &'g Tx<PM, P>,
    ) -> Result<Option<(PagePtr<'g, P>, Vec<&'g P>)>> {
        trace!("getting page iter for pid {}", pid);

        if pid == COUNTER_PID || pid == META_PID {
            return Err(Error::Unsupported(
                "you are not able to iterate over \
                 the first couple pages, which are \
                 reserved for storing metadata and \
                 monotonic ID generator info"
                    .into(),
            ));
        }

        let pte_ptr = match self.inner.get(pid, &tx.guard) {
            None => return Ok(None),
            Some(p) => p,
        };

        let head = unsafe { pte_ptr.deref().stack.head(&tx.guard) };

        let entries: Vec<_> = StackIter::from_ptr(head, &tx.guard).collect();

        if entries.is_empty() || entries[0].is_free() {
            return Ok(None);
        }

        if let CacheEntry::MergedResident(mr, ts, ..) = entries[0] {
            // short circuit

            return Ok(Some((
                PagePtr {
                    cached_ptr: head,
                    wts: *ts,
                },
                vec![mr],
            )));
        }

        let pulled = entries.par_iter().map(|ce| match ce {
            CacheEntry::MergedResident(mr, ts, lsn, ptr) => {
                let sz = PM::size_in_bytes(mr);
                Ok((Cow::Borrowed(mr), *ts, *lsn, *ptr, sz))
            }
            CacheEntry::Resident(r, ts, lsn, ptr) => {
                let sz = PM::size_in_bytes(r);
                Ok((Cow::Borrowed(r), *ts, *lsn, *ptr, sz))
            }
            CacheEntry::Flush(ts, lsn, ptr) => {
                let res = self.pull(*lsn, *ptr).map(|pg| pg)?;
                let sz = PM::size_in_bytes(res.as_frag());
                Ok((Cow::Owned(res.into_frag()), *ts, *lsn, *ptr, sz))
            }
            CacheEntry::PartialFlush(ts, lsn, ptr) => {
                let res = self.pull(*lsn, *ptr).map(|pg| pg)?;
                let sz = PM::size_in_bytes(res.as_frag());
                Ok((Cow::Owned(res.into_frag()), *ts, *lsn, *ptr, sz))
            }
            other => {
                panic!("iterating over unexpected CacheEntry: {:?}", other);
            }
        });

        // if any of our pulls failed, bail here
        let mut successes: Vec<(Cow<P>, u64, Lsn, DiskPtr, u64)> =
            match pulled.collect() {
                Ok(s) => s,
                Err(e) => return Err(e),
            };

        if successes.len() > self.config.page_consolidation_threshold {
            trace!("consolidating pid {} with len {}!", pid, successes.len());
            let update = {
                let _measure = Measure::new(&M.merge_page);

                let combined_iter = successes.iter().map(|(c, ..)| &**c).rev();

                Update::Compact(PM::merge(combined_iter, &self.config))
            };

            let ptr = PagePtr {
                cached_ptr: head,
                wts: successes[0].1,
            };

            match self.cas_page(pid, ptr, update, true, tx)? {
                Ok(new_head) => {
                    let head = new_head.cached_ptr;

                    return Ok(Some((new_head, unsafe {
                        vec![head.deref().deref().deref_merged_resident()]
                    })));
                }
                Err(None) => {
                    // This page was unallocated since we
                    // read the head pointer.
                    return Ok(Some((
                        PagePtr {
                            cached_ptr: head,
                            wts: 0,
                        },
                        vec![],
                    )));
                }
                Err(Some(_)) => {
                    // our consolidation failed, recurse
                    // TODO don't recurse in the released version
                    return self.get(pid, tx);
                }
            }
        }

        let pulled = successes.iter().any(|(c, ..)| {
            if let Cow::Owned(_) = c {
                true
            } else {
                false
            }
        });

        if pulled {
            // fix up the stack to include our pulled items
            let (tail_frag, ts, lsn, ptr, mut total_page_size) =
                successes.pop().unwrap();
            let tail = CacheEntry::MergedResident(
                tail_frag.into_owned(),
                ts,
                lsn,
                ptr,
            );

            let mut frags = Vec::with_capacity(successes.len() + 1);

            for (item, ts, lsn, ptr, sz) in successes.into_iter() {
                total_page_size += sz;
                frags.push(CacheEntry::Resident(
                    item.into_owned(),
                    ts,
                    lsn,
                    ptr,
                ));
            }

            frags.push(tail);

            let node = node_from_frag_vec(frags).into_shared(&tx.guard);

            debug_assert_eq!(
                ptrs_from_stack(head, tx),
                ptrs_from_stack(node, tx),
            );

            let node = unsafe { node.into_owned() };

            debug_delay();
            let res =
                unsafe { pte_ptr.deref().stack.cas(head, node, &tx.guard) };
            if res.is_ok() {
                trace!("fix-up for pid {} succeeded", pid);

                // possibly evict an item now that our cache has grown
                let to_evict = self.lru.accessed(pid, total_page_size);
                trace!(
                    "accessed pid {} -> paging out pids {:?}",
                    pid,
                    to_evict
                );
                self.page_out(to_evict, tx)?;
            } else {
                trace!("fix-up for pid {} failed", pid);
            }
            self.get(pid, tx)
        } else {
            let wts = successes[0].1;

            let refs = successes
                .into_iter()
                .map(|(item, ..)| {
                    if let Cow::Borrowed(i) = item {
                        i
                    } else {
                        panic!("somehow got Cow::Owned");
                    }
                })
                .collect();

            Ok(Some((
                PagePtr {
                    cached_ptr: head,
                    wts,
                },
                refs,
            )))
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

    /// Increase a page's associated transactional read
    /// timestamp (RTS) to as high as the specified timestamp.
    pub fn bump_page_rts(&self, pid: PageId, ts: Lsn, tx: &Tx<PM, P>) {
        let pte_ptr = if let Some(p) = self.inner.get(pid, &tx.guard) {
            p
        } else {
            return;
        };

        let pte = unsafe { pte_ptr.deref() };

        let mut current = pte.rts.load(SeqCst);
        loop {
            if current >= ts {
                return;
            }
            let last = pte.rts.compare_and_swap(current, ts, SeqCst);
            if last == current {
                // we succeeded.
                return;
            }
            current = last;
        }
    }

    /// Retrieves the current transactional read timestamp
    /// for a page.
    pub fn get_page_rts(&self, pid: PageId, tx: &Tx<PM, P>) -> Option<u64> {
        let pte_ptr = if let Some(p) = self.inner.get(pid, &tx.guard) {
            p
        } else {
            return None;
        };

        let pte = unsafe { pte_ptr.deref() };

        Some(pte.rts.load(SeqCst) as u64)
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
    pub fn was_recovered(&self) -> bool {
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
        let ret = self.idgen.fetch_add(1, SeqCst);

        let interval = self.config.idgen_persist_interval;
        let necessary_persists = ret / interval * interval;
        let mut persisted = self.idgen_persists.load(SeqCst);

        while persisted < necessary_persists {
            let _mu = self.idgen_persist_mu.lock().unwrap();
            persisted = self.idgen_persists.load(SeqCst);
            if persisted < necessary_persists {
                // it's our responsibility to persist up to our ID
                let tx = Tx::new(&self, u64::max_value());
                let (key, current) = self.get_idgen(&tx)?;

                assert_eq!(current, persisted);

                let counter_update = Update::Counter(necessary_persists);

                let old = self.idgen_persists.swap(necessary_persists, SeqCst);
                assert_eq!(old, persisted);

                let res = self.cas_page(
                    COUNTER_PID,
                    key.clone(),
                    counter_update,
                    false,
                    &tx,
                );

                if res?.is_err() {
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
                    self.flush()?;
                } else if key.last_lsn() > self.stable_lsn() {
                    self.make_stable(key.last_lsn())?;
                }

                tx.flush();
            }
        }

        Ok(ret as u64)
    }

    /// Returns the current `Meta` map, which contains a convenient
    /// mapping from identifiers to PageId's that the `PageCache`
    /// owner may use for storing metadata about their higher-level
    /// collections.
    pub fn meta<'a>(&self, tx: &'a Tx<PM, P>) -> Result<&'a Meta> {
        self.get_meta(tx).map(|(_k, m)| m)
    }

    /// Look up a PageId for a given identifier in the `Meta`
    /// mapping. This is pretty cheap, but in some cases
    /// you may prefer to maintain your own atomic references
    /// to collection roots instead of relying on this. See
    /// sled's `Tree` root tracking for an example of
    /// avoiding this in a lock-free way that handles
    /// various race conditions.
    pub fn meta_pid_for_name(
        &self,
        name: &[u8],
        tx: &Tx<PM, P>,
    ) -> Result<PageId> {
        let m = self.meta(tx)?;
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
        name: Vec<u8>,
        old: Option<PageId>,
        new: Option<PageId>,
        tx: &'g Tx<PM, P>,
    ) -> Result<std::result::Result<(), Option<PageId>>> {
        loop {
            let (meta_key, meta) = self.get_meta(tx)?;

            let actual = meta.get_root(&name);
            if actual != old {
                return Ok(Err(actual));
            }

            let mut new_meta = (*meta).clone();
            if let Some(new) = new {
                new_meta.set_root(name.clone(), new);
            } else {
                new_meta.del_root(&name);
            }

            let new_meta_frag = Update::Meta(new_meta);

            let res = self.cas_page(
                META_PID,
                meta_key.clone(),
                new_meta_frag,
                false,
                &tx,
            )?;

            match res {
                Ok(_worked) => return Ok(Ok(())),
                Err(Some((_current_ptr, _rejected))) => {}
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

    fn page_out<'g>(
        &self,
        to_evict: Vec<PageId>,
        tx: &'g Tx<PM, P>,
    ) -> Result<()> {
        let _measure = Measure::new(&M.page_out);
        for pid in to_evict {
            let pte_ptr = match self.inner.get(pid, &tx.guard) {
                None => continue,
                Some(p) => p,
            };

            debug_delay();
            let head = unsafe { pte_ptr.deref().stack.head(&tx.guard) };
            let stack_iter = StackIter::from_ptr(head, &tx.guard);

            let mut cache_entries: Vec<CacheEntry<P>> =
                stack_iter.map(|ptr| (*ptr).clone()).collect();

            // ensure the last entry is a Flush
            let last_ce = match cache_entries.pop() {
                None => continue,
                Some(c) => c,
            };

            let last = match last_ce {
                CacheEntry::MergedResident(_, ts, lsn, ptr)
                | CacheEntry::Resident(_, ts, lsn, ptr)
                | CacheEntry::Flush(ts, lsn, ptr) => {
                    // NB stabilize the most recent LSN before
                    // paging out! This SHOULD very rarely block...
                    self.log.make_stable(lsn)?;
                    CacheEntry::Flush(ts, lsn, ptr)
                }
                CacheEntry::PartialFlush(..) => {
                    panic!("got PartialFlush at end of stack...")
                }
                CacheEntry::Meta(..)
                | CacheEntry::Counter(..)
                | CacheEntry::Free(..) => {
                    // don't actually evict this. this leads to
                    // a discrepency in the Lru perceived size
                    // and the real size, but this should be
                    // minimal in anticipated workloads.
                    continue;
                }
            };

            let mut new_stack = Vec::with_capacity(cache_entries.len() + 1);
            for entry in cache_entries {
                match entry {
                    CacheEntry::PartialFlush(ts, lsn, ptr)
                    | CacheEntry::MergedResident(_, ts, lsn, ptr)
                    | CacheEntry::Resident(_, ts, lsn, ptr) => {
                        new_stack.push(CacheEntry::PartialFlush(ts, lsn, ptr));
                    }
                    CacheEntry::Flush(..)
                    | CacheEntry::Free(..)
                    | CacheEntry::Counter(..)
                    | CacheEntry::Meta(..) => {
                        panic!("encountered {:?} in middle of stack...", entry)
                    }
                }
            }
            new_stack.push(last);
            let node = node_from_frag_vec(new_stack);

            debug_delay();
            unsafe {
                if pte_ptr.deref().stack.cas(head, node, &tx.guard).is_err() {
                    trace!("failed to page-out pid {}", pid)
                }
            }
        }
        Ok(())
    }

    fn pull(&self, lsn: Lsn, ptr: DiskPtr) -> Result<Update<P>> {
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
            Update::Free => Err(Error::ReportableBug(
                "non-append/compact found in pull".to_owned(),
            )),
            update => Ok(update),
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
            if snapshot_opt_res.is_err() {
                // some other thread is snapshotting
                warn!(
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
                iobufs.with_sa(|sa| sa.resume_rewriting());
                *snapshot_opt = Some(last_snapshot);
                return Err(e);
            }

            // we disable rewriting so that our log becomes append-only,
            // allowing us to iterate through it without corrupting ourselves.
            // NB must be called after taking the snapshot mutex.
            iobufs.with_sa(|sa| sa.pause_rewriting());

            let max_lsn = last_snapshot.max_lsn;
            let start_lsn = max_lsn - (max_lsn % config.io_buf_size as Lsn);

            let iter = iobufs.iter_from(start_lsn);

            debug!(
                "snapshot starting from offset {} to the segment containing ~{}",
                last_snapshot.max_lsn,
                iobufs.stable(),
            );

            let res = advance_snapshot::<PM, P>(iter, last_snapshot, &config);

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

        if let Err(e) = self.config.global_error() {
            self.log.iobufs.interval_updated.notify_all();
            return Err(e);
        }

        if let Some(ref thread_pool) = self.config.thread_pool {
            debug!("asynchronously spawning snapshot generation task");
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
        let snapshot = self.last_snapshot.try_lock().unwrap().clone().unwrap();

        self.max_pid.store(snapshot.max_pid, SeqCst);

        let mut snapshot_free = snapshot.free.clone();

        for (pid, state) in &snapshot.pt {
            trace!("load_snapshot page {} {:?}", pid, state);

            let stack = Stack::default();

            match *state {
                PageState::Present(ref ptrs) => {
                    let (base_lsn, base_ptr) = ptrs[0];

                    stack.push(CacheEntry::Flush(0, base_lsn, base_ptr));

                    for &(lsn, ptr) in &ptrs[1..] {
                        stack.push(CacheEntry::PartialFlush(0, lsn, ptr));
                    }
                }
                PageState::Free(lsn, ptr) => {
                    // blow away any existing state
                    trace!("load_snapshot freeing pid {}", *pid);
                    stack.push(CacheEntry::Free(0, lsn, ptr));
                    self.free.lock().unwrap().push(*pid);
                    snapshot_free.remove(&pid);
                }
            }

            let guard = pin();

            // Set up new stack

            let pte = PageTableEntry {
                stack,
                rts: AtomicLsn::new(0),
                pending: AtomicLsn::new(0),
            };

            let new_pte = Owned::new(pte).into_shared(&guard);

            trace!("installing stack for pid {}", pid);

            self.inner
                .cas(*pid, Shared::null(), new_pte, &guard)
                .expect("should be able to install initial stack");
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
            let mut pages_after_restart: HashMap<PageId, Vec<DiskPtr>> =
                HashMap::new();

            let tx = Tx::new(0);

            for pid in 0..self.max_pid.load(SeqCst) {
                let pte = self.inner.get(pid, &tx);
                if pte.is_none() {
                    continue;
                }
                let head = unsafe { pte.unwrap().deref().stack.head(&tx) };
                let ptrs = ptrs_from_stack(head, &tx);
                pages_after_restart.insert(pid, ptrs);
            }

            self.config
                .event_log
                .pages_after_restart(pages_after_restart);
        }
    }
}

fn ptrs_from_stack<'g, PM, P>(
    head_ptr: PagePtrInner<'g, P>,
    tx: &'g Tx<'g, PM, P>,
) -> Vec<DiskPtr>
where
    P: Clone + Send + Sync + DeserializeOwned + Serialize,
{
    // generate a list of the old log ID's
    let stack_iter = StackIter::from_ptr(head_ptr, &tx.guard);

    let mut ptrs = vec![];
    for cache_entry_ptr in stack_iter {
        match *cache_entry_ptr {
            CacheEntry::Resident(.., ref ptr)
            | CacheEntry::MergedResident(.., ref ptr)
            | CacheEntry::PartialFlush(.., ref ptr)
            | CacheEntry::Free(.., ref ptr)
            | CacheEntry::Flush(.., ref ptr)
            | CacheEntry::Counter(.., ref ptr)
            | CacheEntry::Meta(.., ref ptr) => {
                ptrs.push(*ptr);
            }
        }
    }
    ptrs
}

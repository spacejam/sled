use std::{borrow::Cow, collections::BinaryHeap, ops::Deref, sync::Arc};

use parking_lot::Mutex;

use super::*;

type PagePtrInner<'g, P> = Shared<'g, Node<(Option<Update<P>>, CacheInfo)>>;

/// A pointer to shared lock-free state bound by a pinned epoch's lifetime.
#[derive(Debug, Clone, PartialEq, Copy)]
pub struct PagePtr<'g, P>
where
    P: 'static + Send,
{
    cached_ptr: PagePtrInner<'g, P>,
    ts: u64,
}

impl<'g, P> PagePtr<'g, P>
where
    P: 'static + Send,
{
    /// The last Lsn number for the head of this page
    pub fn last_lsn(&self) -> Lsn {
        unsafe { self.cached_ptr.deref().deref().1.lsn }
    }
}

unsafe impl<'g, P> Send for PagePtr<'g, P> where P: Send {}
unsafe impl<'g, P> Sync for PagePtr<'g, P> where P: Send + Sync {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheInfo {
    pub ts: u64,
    pub lsn: Lsn,
    pub ptr: DiskPtr,
    pub log_size: usize,
}

/// Update<PageFragment> denotes a state or a change in a sequence of updates
/// of which a page consists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Update<PageFrag> {
    Append(PageFrag),
    Compact(PageFrag),
    Free,
    Counter(u64),
    Meta(Meta),
    Config(PersistedConfig),
}

impl<P> Update<P>
where
    P: DeserializeOwned + Serialize,
    Self: Debug,
{
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

    fn is_compact(&self) -> bool {
        if let Update::Compact(_) = self {
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
/// use {
///     pagecache::{pin, Config, Materializer},
///     serde::{Deserialize, Serialize},
/// };
///
/// #[derive(
///     Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize,
/// )]
/// pub struct TestState(String);
///
/// impl Materializer for TestState {
///     // Used to merge chains of partial pages into a form
///     // that is useful for the `PageCache` owner.
///     fn merge(&mut self, other: &TestState) {
///         self.0.push_str(&other.0);
///     }
/// }
///
/// fn main() {
///     let config = pagecache::ConfigBuilder::new().temporary(true).build();
///     let pc: pagecache::PageCache<TestState> =
///         pagecache::PageCache::start(config).unwrap();
///     {
///         // We begin by initiating a new transaction, which
///         // will prevent any witnessable memory from being
///         // reclaimed before we drop this object.
///         let guard = pin();
///
///         // The first item in a page should be set using allocate,
///         // which signals that this is the beginning of a new
///         // page history.
///         let (id, mut key) =
///             pc.allocate(TestState("a".to_owned()), &guard).unwrap();
///
///         // Subsequent atomic updates should be added with link.
///         key = pc
///             .link(id, key, TestState("b".to_owned()), &guard)
///             .unwrap()
///             .unwrap();
///         key = pc
///             .link(id, key, TestState("c".to_owned()), &guard)
///             .unwrap()
///             .unwrap();
///
///         // When getting a page, the provided `Materializer` is
///         // used to merge all pages together.
///         let (mut key, page, size_on_disk) =
///             pc.get(id, &guard).unwrap().unwrap();
///
///         assert_eq!(page.0, "abc".to_owned());
///
///         // You can completely rewrite a page by using `replace`:
///         key = pc
///             .replace(id, key, TestState("d".into()), &guard)
///             .unwrap()
///             .unwrap();
///
///         let (key, page, size_on_disk) =
///             pc.get(id, &guard).unwrap().unwrap();
///
///         assert_eq!(page.0, "d".to_owned());
///     }
/// }
/// ```
pub struct PageCache<P>
where
    P: Materializer,
{
    config: Config,
    inner: PageTable<Page<P>>,
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

/// A page consists of a sequence state updates with associated
/// storage parameters like disk pos, lsn, time.
type Page<P> = Stack<(Option<Update<P>>, CacheInfo)>;

unsafe impl<P> Send for PageCache<P> where P: Materializer {}

unsafe impl<P> Sync for PageCache<P> where P: Materializer {}

impl<P> Debug for PageCache<P>
where
    P: Materializer,
{
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
impl<P> Drop for PageCache<P>
where
    P: Materializer,
{
    fn drop(&mut self) {
        use std::collections::HashMap;

        trace!("dropping pagecache");

        // we can't as easily assert recovery
        // invariants across failpoints for now
        if self.log.iobufs.config.global_error().is_ok() {
            let mut pages_before_restart = HashMap::new();

            let guard = pin();

            self.config.event_log.meta_before_restart(
                self.meta(&guard)
                    .expect("should get meta under test")
                    .clone(),
            );

            for pid in 0..self.next_pid_to_allocate.load(Acquire) {
                let pte = self.inner.get(pid, &guard);
                if pte.is_none() {
                    continue;
                }
                let head = unsafe { pte.unwrap().deref().head(&guard) };
                let ptrs = ptrs_from_stack(head, &guard);
                pages_before_restart.insert(pid, ptrs);
            }

            self.config
                .event_log
                .pages_before_restart(pages_before_restart);
        }

        trace!("pagecache dropped");
    }
}

impl<P> PageCache<P>
where
    P: Materializer,
{
    /// Instantiate a new `PageCache`.
    pub fn start(config: Config) -> Result<Self> {
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

        #[cfg(feature = "event_log")]
        {
            use std::collections::HashMap;

            // NB this must be before idgen/meta are initialized
            // because they may cas_page on initial page-in.
            let guard = pin();

            let mut pages_after_restart = HashMap::new();

            for pid in 0..pc.next_pid_to_allocate.load(Acquire) {
                let pte = pc.inner.get(pid, &guard);
                if pte.is_none() {
                    continue;
                }
                let head = unsafe { pte.unwrap().deref().head(&guard) };
                let ptrs = ptrs_from_stack(head, &guard);
                pages_after_restart.insert(pid, ptrs);
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
                    meta_id,
                    META_PID,
                    "we expect the meta page to have pid {}, but it had pid {} instead",
                    META_PID,
                    meta_id,
                );
            }

            if let Err(Error::ReportableBug(..)) = pc.get_idgen(&guard) {
                // set up idgen
                was_recovered = false;

                let counter_update = Update::Counter(0);

                let (counter_id, _) =
                    pc.allocate_inner(counter_update, &guard)?;

                assert_eq!(
                    counter_id,
                    COUNTER_PID,
                    "we expect the counter to have pid {}, but it had pid {} instead",
                    COUNTER_PID,
                    counter_id,
                );
            }

            if let Err(Error::ReportableBug(..)) =
                pc.get_persisted_config(&guard)
            {
                // set up idgen
                was_recovered = false;

                let config_update = Update::Config(PersistedConfig);

                let (config_id, _) =
                    pc.allocate_inner(config_update, &guard)?;

                assert_eq!(
                    config_id,
                    CONFIG_PID,
                    "we expect the counter to have pid {}, but it had pid {} instead",
                    CONFIG_PID,
                    config_id,
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
        new: P,
        guard: &'g Guard,
    ) -> Result<(PageId, PagePtr<'g, P>)> {
        self.allocate_inner(Update::Compact(new), guard)
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
            self.log.iobufs.interval_updated.notify_all();
        }
    }

    fn allocate_inner<'g>(
        &self,
        new: Update<P>,
        guard: &'g Guard,
    ) -> Result<(PageId, PagePtr<'g, P>)> {
        let (pid, key) = if let Some(pid) = self.free.lock().pop() {
            trace!("re-allocating pid {}", pid);

            let head_ptr = match self.inner.get(pid, &guard) {
                None => panic!(
                    "expected to find existing stack \
                     for re-allocated pid {}",
                    pid
                ),
                Some(p) => p,
            };

            let head = unsafe { head_ptr.deref().head(&guard) };

            let mut stack_iter = StackIter::from_ptr(head, &guard);

            match stack_iter.next() {
                Some((Some(Update::Free), cache_info)) => (
                    pid,
                    PagePtr {
                        cached_ptr: head,
                        ts: cache_info.ts,
                    },
                ),
                other => panic!(
                    "failed to re-allocate pid {} which \
                     contained unexpected state {:?}",
                    pid, other
                ),
            }
        } else {
            let pid = self.next_pid_to_allocate.fetch_add(1, Relaxed);

            trace!("allocating pid {} for the first time", pid);

            let new_stack = Stack::default();

            let head_ptr = Owned::new(new_stack).into_shared(&guard);

            self.inner
                .cas(pid, Shared::null(), head_ptr, &guard)
                .expect(
                    "allocating a fresh new page should \
                     never conflict on existing data",
                );

            (
                pid,
                PagePtr {
                    cached_ptr: Shared::null(),
                    ts: 0,
                },
            )
        };

        let new_ptr = self
            .cas_page(pid, key, new, false, guard)?
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
        guard: &'g Guard,
    ) -> Result<CasResult<'g, P, ()>> {
        trace!("attempting to free pid {}", pid);

        if pid == COUNTER_PID
            || pid == META_PID
            || pid == CONFIG_PID
            || pid == BATCH_MANIFEST_PID
        {
            return Err(Error::Unsupported(
                "you are not able to free the first \
                 couple pages, which are allocated \
                 for system internal purposes"
                    .into(),
            ));
        }

        let new_ptr = self.cas_page(pid, old, Update::Free, false, guard)?;

        if new_ptr.is_ok() {
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

        Ok(new_ptr.map_err(|o| o.map(|(ptr, _)| (ptr, ()))))
    }

    /// Try to atomically add a `PageFrag` to the page.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns
    /// `Err(Some(actual_key))` if the atomic append fails.
    pub fn link<'g>(
        &'g self,
        pid: PageId,
        mut old: PagePtr<'g, P>,
        new: P,
        guard: &'g Guard,
    ) -> Result<CasResult<'g, P, P>> {
        let _measure = Measure::new(&M.link_page);

        trace!("linking pid {} with {:?}", pid, new);
        let head_ptr = match self.inner.get(pid, &guard) {
            None => return Ok(Err(None)),
            Some(p) => p,
        };

        // see if we should short-circuit replace
        let head = unsafe { head_ptr.deref().head(&guard) };
        let stack_iter = StackIter::from_ptr(head, &guard);
        let stack_len = stack_iter.size_hint().1.unwrap();
        if stack_len >= self.config.page_consolidation_threshold {
            let current_frag =
                if let Some((current_ptr, frag, _sz)) = self.get(pid, guard)? {
                    if old.ts != current_ptr.ts
                        && old.cached_ptr != current_ptr.cached_ptr
                    {
                        // the page has changed in the mean time,
                        // and merging frags may violate correctness
                        // invariants
                        return Ok(Err(Some((current_ptr, new))));
                    }
                    frag
                } else {
                    return Ok(Err(None));
                };

            let update: P = {
                let _measure = Measure::new(&M.merge_page);

                let mut update = current_frag.clone();
                update.merge(&new);
                update
            };

            return self.replace(pid, old, update, guard);
        }

        let bytes = measure(&M.serialize, || serialize(&new).unwrap());

        let mut new = {
            let update = Update::Append(new);

            let cache_info = CacheInfo {
                lsn: -1,
                ptr: DiskPtr::Inline(666_666_666),
                ts: 0,
                log_size: 0,
            };

            let node = Node {
                inner: (Some(update), cache_info),
                // NB this must be null
                // to prevent double-frees
                // if we encounter an IO error
                // and fee our Owned version!
                next: Atomic::null(),
            };

            Some(Owned::new(node))
        };

        loop {
            let log_reservation =
                self.log.reserve(LogKind::Append, pid, &bytes)?;

            let lsn = log_reservation.lsn();
            let ptr = log_reservation.ptr();

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
                ptr,
                ts,
                log_size: log_reservation.reservation_len(),
            };

            if let (Some(Update::Append(_)), ref mut stored_cache_info) =
                node.inner
            {
                *stored_cache_info = cache_info;
            } else {
                panic!("should only be working with Resident entries");
            }

            debug_delay();
            let result = unsafe {
                head_ptr.deref().cap_node(old.cached_ptr, node, &guard)
            };

            match result {
                Ok(cached_ptr) => {
                    trace!("link of pid {} succeeded", pid);

                    // if the last update for this page was also
                    // sent to this segment, we can skip marking it
                    let previous_head_lsn = old.last_lsn();

                    assert_ne!(previous_head_lsn, 0);

                    let previous_lsn_segment =
                        previous_head_lsn / self.config.io_buf_size as i64;
                    let new_lsn_segment = lsn / self.config.io_buf_size as i64;

                    let to_clean = if previous_lsn_segment == new_lsn_segment {
                        // can skip mark_link because we've
                        // already accounted for this page
                        // being resident on this segment
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
                        self.rewrite_page(to_clean, guard)?;
                    }

                    let count = self.updates.fetch_add(1, Relaxed) + 1;
                    let should_snapshot =
                        count % self.config.snapshot_after_ops == 0;
                    if should_snapshot {
                        self.advance_snapshot()?;
                    }

                    return Ok(Ok(PagePtr { cached_ptr, ts }));
                }
                Err((actual_ptr, returned_new)) => {
                    trace!("link of pid {} failed", pid);
                    log_reservation.abort()?;
                    let actual_ts = unsafe { actual_ptr.deref().1.ts };
                    if actual_ts == old.ts {
                        new = Some(returned_new);
                        old = PagePtr {
                            cached_ptr: actual_ptr,
                            ts: actual_ts,
                        };
                    } else {
                        let returned_update = returned_new.0.clone().unwrap();
                        let returned_frag = returned_update.into_frag();
                        return Ok(Err(Some((
                            PagePtr {
                                cached_ptr: actual_ptr,
                                ts: actual_ts,
                            },
                            returned_frag,
                        ))));
                    }
                }
            }
        }
    }

    /// Replace an existing page with a different set of `PageFrag`s.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns
    /// `Err(Some(actual_key))` if the atomic swap fails.
    pub fn replace<'g>(
        &self,
        pid: PageId,
        old: PagePtr<'g, P>,
        new: P,
        guard: &'g Guard,
    ) -> Result<CasResult<'g, P, P>> {
        let _measure = Measure::new(&M.replace_page);

        trace!("replacing pid {} with {:?}", pid, new);

        let result =
            self.cas_page(pid, old, Update::Compact(new), false, guard)?;

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
    fn rewrite_page(&self, pid: PageId, guard: &Guard) -> Result<()> {
        let _measure = Measure::new(&M.rewrite_page);

        trace!("rewriting pid {}", pid);

        let head_ptr = match self.inner.get(pid, &guard) {
            None => {
                trace!("rewriting pid {} failed (no longer exists)", pid);
                return Ok(());
            }
            Some(p) => p,
        };

        debug_delay();
        let head = unsafe { head_ptr.deref().head(&guard) };
        let stack_iter = StackIter::from_ptr(head, &guard);
        let cache_entries: Vec<_> = stack_iter.collect();

        // if the page is just a single blob pointer, rewrite it.
        if cache_entries.len() == 1 && cache_entries[0].1.ptr.is_blob() {
            trace!("rewriting blob with pid {}", pid);
            let blob_ptr = cache_entries[0].1.ptr.blob().1;

            let log_reservation = self.log.rewrite_blob_ptr(pid, blob_ptr)?;

            let new_ptr = log_reservation.ptr();
            let mut new_cache_entry = cache_entries[0].clone();

            new_cache_entry.1.ptr = new_ptr;

            let node = node_from_frag_vec(vec![new_cache_entry]);

            debug_delay();
            let result = unsafe { head_ptr.deref().cas(head, node, &guard) };

            if result.is_ok() {
                let ptrs = ptrs_from_stack(head, guard);
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
            let (key, update): (_, Update<P>) = if pid == META_PID {
                let (key, meta) = self.get_meta(guard)?;
                (key, Update::Meta(meta.clone()))
            } else if pid == COUNTER_PID {
                let (key, counter) = self.get_idgen(guard)?;
                (key, Update::Counter(counter))
            } else if pid == CONFIG_PID {
                let (key, config) = self.get_persisted_config(guard)?;
                (key, Update::Config(config.clone()))
            } else if let Some((key, frag, _sz)) = self.get(pid, guard)? {
                (key, Update::Compact(frag.clone()))
            } else {
                let head_ptr = match self.inner.get(pid, &guard) {
                    None => panic!(
                        "expected to find existing stack \
                         for freed pid {}",
                        pid
                    ),
                    Some(p) => p,
                };

                let head = unsafe { head_ptr.deref().head(&guard) };

                let mut stack_iter = StackIter::from_ptr(head, &guard);

                match stack_iter.next() {
                    Some((Some(Update::Free), cache_info)) => (
                        PagePtr {
                            cached_ptr: head,
                            ts: cache_info.ts,
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
    #[doc(hidden)]
    pub fn space_amplification(&self) -> Result<f64> {
        let on_disk_bytes = self.size_on_disk()? as f64;
        let logical_size = self.logical_size_of_all_pages()? as f64;
        let discount = self.config.io_buf_size as f64 * 8.;

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
        let min_pid = CONFIG_PID + 1;
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
        mut old: PagePtr<'g, P>,
        update: Update<P>,
        is_rewrite: bool,
        guard: &'g Guard,
    ) -> Result<CasResult<'g, P, Update<P>>> {
        trace!(
            "cas_page called on pid {} to {:?} with old ts {:?}",
            pid,
            update,
            old.ts
        );
        let head_ptr = match self.inner.get(pid, &guard) {
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
            Update::Config(c) => serialize(&c).unwrap(),
            Update::Free => vec![],
            other => serialize(other.as_frag()).unwrap(),
        };
        drop(serialize_latency);
        let mut update_opt = Some(update);

        loop {
            let log_reservation = self.log.reserve(log_kind, pid, &bytes)?;
            let lsn = log_reservation.lsn();
            let new_ptr = log_reservation.ptr();

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
                ptr: new_ptr,
                log_size: log_reservation.reservation_len(),
            };

            let node = node_from_frag_vec(vec![(
                Some(update_opt.take().unwrap()),
                cache_info,
            )]);

            debug_delay();
            let result =
                unsafe { head_ptr.deref().cas(old.cached_ptr, node, &guard) };

            match result {
                Ok(cached_ptr) => {
                    trace!("cas_page succeeded on pid {}", pid);
                    let pointers = ptrs_from_stack(old.cached_ptr, guard);

                    self.log.with_sa(|sa| {
                        sa.mark_replace(pid, lsn, pointers, new_ptr)
                    })?;

                    // NB complete must happen AFTER calls to SA, because
                    // when the iobuf's n_writers hits 0, we may transition
                    // the segment to inactive, resulting in a race otherwise.
                    log_reservation.complete()?;
                    return Ok(Ok(PagePtr { cached_ptr, ts }));
                }
                Err((actual_ptr, returned_entry)) => {
                    trace!("cas_page failed on pid {}", pid);
                    log_reservation.abort()?;

                    let returned_update =
                        returned_entry.into_box().inner.0.take().unwrap();

                    let actual_ts = unsafe { actual_ptr.deref().1.ts };

                    if actual_ts != old.ts || is_rewrite {
                        return Ok(Err(Some((
                            PagePtr {
                                cached_ptr: actual_ptr,
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
                    old = PagePtr {
                        cached_ptr: actual_ptr,
                        ts: old.ts,
                    };
                    update_opt = Some(returned_update);
                }
            } // match cas result
        } // loop
    }

    /// Retrieve the current meta page
    pub(crate) fn get_meta<'g>(
        &self,
        guard: &'g Guard,
    ) -> Result<(PagePtr<'g, P>, &'g Meta)> {
        trace!("getting page iter for META");

        let head_ptr = match self.inner.get(META_PID, &guard) {
            None => {
                return Err(Error::ReportableBug(
                    "failed to retrieve META page \
                     which should always be present"
                        .into(),
                ));
            }
            Some(pointer) => pointer,
        };

        let head = unsafe { head_ptr.deref().head(&guard) };

        match StackIter::from_ptr(head, &guard).next() {
            Some((Some(Update::Meta(m)), cache_info)) => Ok((
                PagePtr {
                    cached_ptr: head,
                    ts: cache_info.ts,
                },
                m,
            )),
            Some((None, cache_info)) => {
                let update =
                    self.pull(META_PID, cache_info.lsn, cache_info.ptr)?;
                let ptr = PagePtr {
                    cached_ptr: head,
                    ts: cache_info.ts,
                };
                let _ = self.cas_page(META_PID, ptr, update, false, guard)?;
                self.get_meta(guard)
            }
            _ => Err(Error::ReportableBug(
                "failed to retrieve META page \
                 which should always be present"
                    .into(),
            )),
        }
    }

    /// Retrieve the current meta page
    pub(crate) fn get_persisted_config<'g>(
        &self,
        guard: &'g Guard,
    ) -> Result<(PagePtr<'g, P>, &'g PersistedConfig)> {
        trace!("getting page iter for persisted config");

        let head_ptr = match self.inner.get(CONFIG_PID, &guard) {
            None => {
                return Err(Error::ReportableBug(
                    "failed to retrieve persisted config page \
                     which should always be present"
                        .into(),
                ));
            }
            Some(pointer) => pointer,
        };

        let head = unsafe { head_ptr.deref().head(&guard) };

        match StackIter::from_ptr(head, &guard).next() {
            Some((Some(Update::Config(config)), cache_info)) => Ok((
                PagePtr {
                    cached_ptr: head,
                    ts: cache_info.ts,
                },
                config,
            )),
            Some((None, cache_info)) => {
                let update =
                    self.pull(CONFIG_PID, cache_info.lsn, cache_info.ptr)?;
                let ptr = PagePtr {
                    cached_ptr: head,
                    ts: cache_info.ts,
                };
                let _ = self.cas_page(CONFIG_PID, ptr, update, false, guard)?;
                self.get_persisted_config(guard)
            }
            _ => Err(Error::ReportableBug(
                "failed to retrieve CONFIG page \
                 which should always be present"
                    .into(),
            )),
        }
    }

    /// Retrieve the current persisted IDGEN value
    pub(crate) fn get_idgen<'g>(
        &self,
        guard: &'g Guard,
    ) -> Result<(PagePtr<'g, P>, u64)> {
        trace!("getting page iter for idgen");

        let head_ptr = match self.inner.get(COUNTER_PID, &guard) {
            None => {
                return Err(Error::ReportableBug(
                    "failed to retrieve idgen page \
                     which should always be present"
                        .into(),
                ))
            }
            Some(pointer) => pointer,
        };

        let head = unsafe { head_ptr.deref().head(&guard) };

        match StackIter::from_ptr(head, &guard).next() {
            Some((Some(Update::Counter(counter)), cache_info)) => Ok((
                PagePtr {
                    cached_ptr: head,
                    ts: cache_info.ts,
                },
                *counter,
            )),
            Some((None, cache_info)) => {
                let update =
                    self.pull(COUNTER_PID, cache_info.lsn, cache_info.ptr)?;
                let ptr = PagePtr {
                    cached_ptr: head,
                    ts: cache_info.ts,
                };
                let _ =
                    self.cas_page(COUNTER_PID, ptr, update, false, guard)?;
                self.get_idgen(guard)
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
        guard: &'g Guard,
    ) -> Result<Option<(PagePtr<'g, P>, &'g P, u64)>> {
        trace!("getting page iterator for pid {}", pid);
        let _measure = Measure::new(&M.get_page);

        if pid == COUNTER_PID
            || pid == META_PID
            || pid == CONFIG_PID
            || pid == BATCH_MANIFEST_PID
        {
            return Err(Error::Unsupported(
                "you are not able to iterate over \
                 the first couple pages, which are \
                 reserved for storing metadata and \
                 monotonic ID generator info"
                    .into(),
            ));
        }

        let head_ptr = match self.inner.get(pid, &guard) {
            None => return Ok(None),
            Some(p) => p,
        };

        let head = unsafe { head_ptr.deref().head(&guard) };

        let entries: Vec<_> = StackIter::from_ptr(head, &guard).collect();

        let is_free = if let Some((Some(entry), _)) = entries.first() {
            entry.is_free()
        } else {
            false
        };

        if entries.is_empty() || is_free {
            return Ok(None);
        }

        let total_page_size = entries
            .iter()
            .map(|(_, cache_info)| cache_info.log_size as u64)
            .sum();

        let initial_base = match entries[0] {
            (Some(Update::Compact(compact)), cache_info) => {
                // short circuit
                return Ok(Some((
                    PagePtr {
                        cached_ptr: head,
                        ts: cache_info.ts,
                    },
                    compact,
                    total_page_size,
                )));
            }
            (Some(Update::Append(_)), _) => {
                // merge to next item
                let base_idx = entries.iter().position(|(e, _)| {
                    e.is_some() && e.as_ref().unwrap().is_compact()
                });
                if let Some(base_idx) = base_idx {
                    let mut base =
                        entries[base_idx].0.as_ref().unwrap().as_frag().clone();
                    for (append, _) in entries[0..base_idx].iter().rev() {
                        base.merge(append.as_ref().unwrap().as_frag());
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
                (Some(Update::Compact(compact)), _)
                | (Some(Update::Append(compact)), _) => {
                    Ok(Cow::Borrowed(compact))
                }
                (None, cache_info) => {
                    let res = self
                        .pull(pid, cache_info.lsn, cache_info.ptr)
                        .map(|pg| pg)?;
                    Ok(Cow::Owned(res.into_frag()))
                }
                other => {
                    panic!("iterating over unexpected update: {:?}", other);
                }
            });

            // if any of our pulls failed, bail here
            let mut successes: Vec<Cow<'_, P>> = match pulled.collect() {
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

            while let Some(frag) = successes.pop() {
                base.merge(&frag);
            }

            base
        };

        // fix up the stack to include our pulled items
        let mut frags: Vec<(Option<Update<P>>, CacheInfo)> = entries
            .iter()
            .map(|(_, cache_info)| (None, *cache_info))
            .collect();

        frags[0].0 = Some(Update::Compact(base));

        let node = node_from_frag_vec(frags).into_shared(&guard);

        #[cfg(feature = "event_log")]
        assert_eq!(ptrs_from_stack(head, guard), ptrs_from_stack(node, guard),);

        let node = unsafe { node.into_owned() };

        debug_delay();
        let res = unsafe { head_ptr.deref().cas(head, node, &guard) };
        if let Ok(new_ptr) = res {
            trace!("fix-up for pid {} succeeded", pid);

            // possibly evict an item now that our cache has grown
            let to_evict = self.lru.accessed(pid, total_page_size);
            trace!("accessed pid {} -> paging out pids {:?}", pid, to_evict);
            if !to_evict.is_empty() {
                self.page_out(to_evict, guard)?;
            }

            let page_ref = unsafe {
                let item = &new_ptr.deref().inner;
                if let (Some(Update::Compact(compact)), _) = item {
                    compact
                } else {
                    panic!()
                }
            };

            let ptr = PagePtr {
                cached_ptr: new_ptr,
                ts: entries[0].1.ts,
            };

            Ok(Some((ptr, page_ref, total_page_size)))
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
                    self.flush()?;
                } else if key.last_lsn() > self.stable_lsn() {
                    self.make_stable(key.last_lsn())?;
                }
            }
        }

        Ok(ret as u64)
    }

    /// Returns the current `Meta` map, which contains a convenient
    /// mapping from identifiers to PageId's that the `PageCache`
    /// owner may use for storing metadata about their higher-level
    /// collections.
    pub fn meta<'a>(&self, guard: &'a Guard) -> Result<&'a Meta> {
        self.get_meta(guard).map(|(_k, m)| m)
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

            let actual = meta.get_root(&name);
            if actual != old {
                return Ok(Err(actual));
            }

            let mut new_meta = (*meta).clone();
            if let Some(new) = new {
                new_meta.set_root(name.to_vec(), new);
            } else {
                new_meta.del_root(&name);
            }

            let new_meta_frag = Update::Meta(new_meta);

            let res = self.cas_page(
                META_PID,
                meta_key.clone(),
                new_meta_frag,
                false,
                &guard,
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

    fn page_out(&self, to_evict: Vec<PageId>, guard: &Guard) -> Result<()> {
        let _measure = Measure::new(&M.page_out);
        'different_page_eviction: for pid in to_evict {
            if pid == COUNTER_PID
                || pid == META_PID
                || pid == CONFIG_PID
                || pid == BATCH_MANIFEST_PID
            {
                // should not page these suckas out
                continue;
            }

            let head_ptr = match self.inner.get(pid, &guard) {
                None => continue 'different_page_eviction,
                Some(ptr) => ptr,
            };

            debug_delay();
            let head = unsafe { head_ptr.deref().head(&guard) };
            let stack_iter = StackIter::from_ptr(head, &guard);
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

            let node = node_from_frag_vec(new_stack);

            debug_delay();
            let result = unsafe { head_ptr.deref().cas(head, node, &guard) };
            if result.is_ok() {
                // TODO record cache difference
            } else {
                trace!("failed to page-out pid {}", pid)
            }
        }
        Ok(())
    }

    fn pull(&self, pid: PageId, lsn: Lsn, ptr: DiskPtr) -> Result<Update<P>> {
        use MessageKind::*;

        trace!("pulling lsn {} ptr {} from disk", lsn, ptr);
        let _measure = Measure::new(&M.pull);
        let (header, bytes) = match self.log.read(pid, lsn, ptr) {
            Ok(LogRead::Inline(header, buf, _len)) => {
                assert_eq!(
                    header.pid, pid,
                    "expected pid {} on pull of ptr {}, \
                     but got {} instead",
                    pid, ptr, header.pid
                );
                assert_eq!(
                    header.lsn, lsn,
                    "expected lsn {} on pull of ptr {}, \
                     but got lsn {} instead",
                    lsn, ptr, header.lsn
                );
                Ok((header, buf))
            }
            Ok(LogRead::Blob(header, buf, _blob_pointer)) => {
                assert_eq!(
                    header.pid, pid,
                    "expected pid {} on pull of ptr {}, \
                     but got {} instead",
                    pid, ptr, header.pid
                );
                assert_eq!(
                    header.lsn, lsn,
                    "expected lsn {} on pull of ptr {}, \
                     but got lsn {} instead",
                    lsn, ptr, header.lsn
                );

                Ok((header, buf))
            }
            Ok(other) => {
                debug!("read unexpected page: {:?}", other);
                Err(Error::Corruption { at: ptr })
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
            BlobConfig | InlineConfig => {
                deserialize::<PersistedConfig>(&bytes).map(Update::Config)
            }
            BlobAppend | InlineAppend => {
                deserialize::<P>(&bytes).map(Update::Append)
            }
            BlobReplace | InlineReplace => {
                deserialize::<P>(&bytes).map(Update::Compact)
            }
            Free => Ok(Update::Free),
            other => panic!("unexpected pull: {:?}", other),
        };
        drop(deserialize_latency);

        let update = update_res
            .map_err(|_| ())
            .expect("failed to deserialize data");

        match update {
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

            // we disable rewriting so that our log becomes append-only,
            // allowing us to iterate through it without corrupting ourselves.
            // NB must be called after taking the snapshot mutex.
            iobufs.with_sa(SegmentAccountant::pause_rewriting);

            let last_lsn = last_snapshot.last_lsn;
            let start_lsn = last_lsn - (last_lsn % config.io_buf_size as Lsn);

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
            self.log.iobufs.interval_updated.notify_all();
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

        #[cfg(any(test, feature = "check_snapshot_integrity"))]
        _result.unwrap()?;

        // TODO add future for waiting on the result of this if desired
        Ok(())
    }

    fn load_snapshot(&mut self) {
        // panic if not set
        let snapshot = self.last_snapshot.try_lock().unwrap().clone().unwrap();

        let next_pid_to_allocate = if snapshot.pt.is_empty() {
            0
        } else {
            *snapshot.pt.keys().max().unwrap() + 1
        };

        self.next_pid_to_allocate = AtomicU64::from(next_pid_to_allocate);

        debug!(
            "load_snapshot loading pages from 0..{}",
            next_pid_to_allocate
        );
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

            match *state {
                PageState::Present(ref ptrs) => {
                    for &(lsn, ptr, sz) in ptrs {
                        let cache_info = CacheInfo {
                            lsn,
                            ptr,
                            log_size: sz,
                            ts: 0,
                        };

                        stack.push((None, cache_info));
                    }
                }
                PageState::Free(lsn, ptr) => {
                    // blow away any existing state
                    trace!("load_snapshot freeing pid {}", pid);
                    let cache_info = CacheInfo {
                        lsn,
                        ptr,
                        log_size: MSG_HEADER_LEN,
                        ts: 0,
                    };
                    stack.push((Some(Update::Free), cache_info));
                    self.free.lock().push(pid);
                }
            }

            let guard = pin();

            // Set up new stack

            let new_stack = Owned::new(stack).into_shared(&guard);

            trace!("installing stack for pid {}", pid);

            self.inner
                .cas(pid, Shared::null(), new_stack, &guard)
                .expect("should be able to install initial stack");
        }
    }
}

fn ptrs_from_stack<'g, P>(
    head_ptr: PagePtrInner<'g, P>,
    guard: &'g Guard,
) -> Vec<DiskPtr>
where
    P: Materializer,
{
    // generate a list of the old log ID's
    let stack_iter = StackIter::from_ptr(head_ptr, &guard);

    let mut ptrs = vec![];
    for (_, cache_info) in stack_iter {
        ptrs.push(cache_info.ptr);
    }
    ptrs
}

use std::sync::{Arc, Mutex};

use epoch::{Guard, Owned, Shared, pin};

#[cfg(feature = "rayon")]
use rayon::prelude::*;

use super::*;

/// Points to either a memory location or a disk location to page-in data from.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheEntry<M: Send + Sync> {
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
    Alloc,
}

struct PidDropper(PageID, Arc<Stack<PageID>>);

impl Drop for PidDropper {
    fn drop(&mut self) {
        self.1.push(self.0);
    }
}

/// A lock-free pagecache which supports fragmented pages
/// for dramatically improving write throughput.
///
/// # Working with the `PageCache`
///
/// ```
/// extern crate sled;
/// extern crate crossbeam_epoch as epoch;
///
/// use sled::Materializer;
///
/// use epoch::pin;
///
/// pub struct TestMaterializer;
///
/// impl Materializer for TestMaterializer {
///     type PageFrag = String;
///     type Recovery = ();
///
///     fn merge(&self, frags: &[&String]) -> String {
///         let mut consolidated = String::new();
///         for frag in frags.into_iter() {
///             consolidated.push_str(&*frag);
///         }
///
///         consolidated
///     }
///
///     fn recover(&self, _: &String) -> Option<()> {
///         None
///     }
/// }
///
/// fn main() {
///     let path = "test_pagecache_doc.log";
///     let conf = sled::Config::default().path(path.to_owned());
///     let pc = sled::PageCache::start(TestMaterializer,
///                                     conf.build());
///     {
///         let guard = pin();
///         let (id, key) = pc.allocate(&guard);
///
///         // The first item in a page should be set using replace,
///         // which signals that this is the beginning of a new
///         // page history, and that any previous items associated
///         // with this page should be forgotten.
///         let key = pc.replace(id, key, "a".to_owned(), &guard).unwrap();
///
///         // Subsequent atomic updates should be added with link.
///         let key = pc.link(id, key, "b".to_owned(), &guard).unwrap();
///         let _key = pc.link(id, key, "c".to_owned(), &guard).unwrap();
///
///         // When getting a page, the provide `Materializer` is
///         // used to merge all pages together.
///         let (consolidated, _key) = pc.get(id, &guard).unwrap();
///
///         assert_eq!(consolidated, "abc".to_owned());
///     }
///
///     drop(pc);
///     std::fs::remove_file(path).unwrap();
/// }
/// ```
pub struct PageCache<PM, P, R>
    where P: 'static + Send + Sync
{
    t: Arc<PM>,
    config: FinalConfig,
    inner: Radix<Stack<CacheEntry<P>>>,
    max_pid: AtomicUsize,
    free: Arc<Stack<PageID>>,
    log: Arc<Log>,
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

unsafe impl<PM, P, R> Sync for PageCache<PM, P, R>
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
    pub fn start(pm: PM, config: FinalConfig) -> PageCache<PM, P, R> {
        let cache_capacity = config.get_cache_capacity();
        let cache_shard_bits = config.get_cache_bits();
        let lru = Lru::new(cache_capacity, cache_shard_bits);

        let materializer = Arc::new(pm);

        // try to pull any existing snapshot off disk, and
        // apply any new data to it to "catch-up" the
        // snapshot before loading it.
        let snapshot =
            read_snapshot_or_default(&config, Some(materializer.clone()));

        let mut pc = PageCache {
            t: materializer,
            config: config.clone(),
            inner: Radix::default(),
            max_pid: AtomicUsize::new(0),
            free: Arc::new(Stack::default()),
            log: Arc::new(Log::start(config, snapshot.clone())),
            lru: lru,
            updates: AtomicUsize::new(0),
            last_snapshot: Arc::new(Mutex::new(Some(snapshot))),
        };

        // now we read it back in
        pc.load_snapshot();

        pc
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
    pub fn allocate<'g>(&self, _: &'g Guard) -> (PageID, HPtr<'g, P>) {
        let pid = self.free.pop().unwrap_or_else(
            || self.max_pid.fetch_add(1, SeqCst),
        );
        // FIXME unwrap called on Err value
        // suspect: recovery issue?
        self.inner.insert(pid, Stack::default()).unwrap();

        // write info to log
        let prepend: LoggedUpdate<P> = LoggedUpdate {
            pid: pid,
            update: Update::Alloc,
        };
        let serialize_start = clock();
        let bytes = serialize(&prepend, Infinite).unwrap();
        M.serialize.measure(clock() - serialize_start);

        let (lsn, lid) = self.log.write(bytes);
        trace!("allocating pid {} at lsn {} lid {}", pid, lsn, lid);

        (pid, Shared::null())
    }

    /// Free a particular page.
    pub fn free(&self, pid: PageID) {
        let guard = pin();
        let deleted = self.inner.del(pid, &guard);
        if deleted.is_none() {
            return;
        }

        // write info to log
        let prepend: LoggedUpdate<P> = LoggedUpdate {
            pid: pid,
            update: Update::Free,
        };
        let serialize_start = clock();
        let bytes = serialize(&prepend, Infinite).unwrap();
        M.serialize.measure(clock() - serialize_start);

        let res = self.log.reserve(bytes);

        // add pid to free stack to reduce fragmentation over time
        unsafe {
            let cas_key = deleted.unwrap().deref().head(&guard);

            let lsn = res.lsn();
            let lid = res.lid();

            self.log.with_sa(|sa| {
                sa.mark_replace(pid, lsn, lids_from_stack(cas_key, &guard), lid)
            });
        }

        // NB complete must happen AFTER calls to SA, because
        // when the iobuf's n_writers hits 0, we may transition
        // the segment to inactive, resulting in a race otherwise.
        res.complete();

        let pd = Owned::new(PidDropper(pid, Arc::clone(&self.free)));
        let ptr = pd.into_shared(&guard);
        unsafe {
            guard.defer(move || ptr.into_owned());
            guard.flush();
        }
    }

    /// Try to retrieve a page by its logical ID.
    pub fn get<'g>(
        &self,
        pid: PageID,
        guard: &'g Guard,
    ) -> Option<(PM::PageFrag, HPtr<'g, P>)> {
        let stack_ptr = self.inner.get(pid, guard);
        if stack_ptr.is_none() {
            return None;
        }

        let stack_ptr = stack_ptr.unwrap();

        let head = unsafe { stack_ptr.deref().head(guard) };

        self.page_in(pid, head, stack_ptr, guard)
    }

    fn page_out<'g>(&self, to_evict: Vec<PageID>, guard: &'g Guard) {
        let start = clock();
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
            let last = cache_entries.pop().map(|last_ce| match last_ce {
                CacheEntry::MergedResident(_, lsn, lid) |
                CacheEntry::Resident(_, lsn, lid) |
                CacheEntry::Flush(lsn, lid) => {
                    // NB stabilize the most recent LSN before
                    // paging out! This SHOULD very rarely block...
                    // TODO measure to make sure
                    self.log.make_stable(lsn);
                    CacheEntry::Flush(lsn, lid)
                }
                CacheEntry::PartialFlush(_, _) => {
                    panic!("got PartialFlush at end of stack...")
                }
            });

            if last.is_none() {
                M.page_out.measure(clock() - start);
                return;
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
        M.page_out.measure(clock() - start);
    }

    fn pull(&self, lsn: Lsn, lid: LogID) -> P {
        trace!("pulling lsn {} lid {} from disk", lsn, lid);
        let start = clock();
        let bytes = match self.log.read(lsn, lid).map_err(|_| ()) {
            Ok(LogRead::Flush(_lsn, data, _len)) => data,
            // FIXME 'read invalid data at lid 66244182' in cycle test
            _ => panic!("read invalid data at lid {}", lid),
        };

        let deserialize_start = clock();
        let logged_update = deserialize::<LoggedUpdate<P>>(&*bytes)
            .map_err(|_| ())
            .expect("failed to deserialize data");
        M.deserialize.measure(clock() - deserialize_start);

        M.pull.measure(clock() - start);
        match logged_update.update {
            Update::Compact(page_frag) |
            Update::Append(page_frag) => page_frag,
            _ => panic!("non-append/compact found in pull"),
        }
    }

    fn page_in<'g>(
        &self,
        pid: PageID,
        mut head: Shared<'g, ds::stack::Node<CacheEntry<P>>>,
        stack_ptr: Shared<'g, ds::stack::Stack<CacheEntry<P>>>,
        guard: &'g Guard,
    ) -> Option<(PM::PageFrag, HPtr<'g, P>)> {
        let start = clock();
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
                        return Some((page_frag.clone(), head));
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
            }
        }

        if lids.is_empty() {
            M.page_in.measure(clock() - start);
            return None;
        }

        let mut fetched = Vec::with_capacity(lids.len());

        // Did not find a previously merged value in memory,
        // may need to go to disk.
        if !merged_resident {
            let to_pull = &lids[to_merge.len()..];

            #[cfg(feature = "rayon")]
            {
                let mut pulled: Vec<P> = to_pull
                    .par_iter()
                    .map(|&(lsn, lid)| self.pull(lsn, lid))
                    .collect();
                fetched.append(&mut pulled);
            }

            #[cfg(not(feature = "rayon"))]
            for &(lsn, lid) in to_pull {
                fetched.push(self.pull(lsn, lid));
            }
        }

        let combined: Vec<&P> = to_merge
            .iter()
            .cloned()
            .chain(fetched.iter())
            .rev()
            .collect();

        let before_merge = clock();
        let merged = self.t.merge(&*combined);
        M.merge_page.measure(clock() - before_merge);

        let size = std::mem::size_of_val(&merged);
        let to_evict = self.lru.accessed(pid, size);
        trace!("accessed pid {} -> paging out pid {:?}", pid, to_evict);
        self.page_out(to_evict, guard);

        if lids.len() > self.config.get_page_consolidation_threshold() {
            trace!("consolidating pid {} with len {}!", pid, lids.len());
            match self.replace_recurse_once(
                pid,
                head,
                merged.clone(),
                guard,
                true,
            ) {
                Ok(new_head) => head = new_head,
                Err(None) => return None,
                _ => (),
            }
        } else if !fetched.is_empty() ||
                   fix_up_length >= self.config.get_cache_fixup_threshold()
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

        M.page_in.measure(clock() - start);

        Some((merged, head))
    }

    /// Replace an existing page with a different set of `PageFrag`s.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the atomic swap fails.
    pub fn replace<'g>(
        &self,
        pid: PageID,
        old: HPtr<'g, P>,
        new: P,
        guard: &'g Guard,
    ) -> Result<HPtr<'g, P>, Option<HPtr<'g, P>>> {
        self.replace_recurse_once(pid, old, new, guard, false)
    }

    fn replace_recurse_once<'g>(
        &self,
        pid: PageID,
        old: HPtr<'g, P>,
        new: P,
        guard: &'g Guard,
        recursed: bool,
    ) -> Result<HPtr<'g, P>, Option<HPtr<'g, P>>> {
        trace!("replacing pid {}", pid);
        let stack_ptr = self.inner.get(pid, guard);
        if stack_ptr.is_none() {
            return Err(None);
        }
        let stack_ptr = stack_ptr.unwrap();

        let replace: LoggedUpdate<P> = LoggedUpdate {
            pid: pid,
            update: Update::Compact(new.clone()),
        };
        let serialize_start = clock();
        let bytes = serialize(&replace, Infinite).unwrap();
        M.serialize.measure(clock() - serialize_start);
        let log_reservation = self.log.reserve(bytes);
        let lsn = log_reservation.lsn();
        let lid = log_reservation.lid();

        let cache_entry = CacheEntry::MergedResident(new, lsn, lid);

        let node = node_from_frag_vec(vec![cache_entry]).into_shared(guard);

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
            if let Some(to_clean) = to_clean {
                assert_ne!(pid, to_clean);
                if let Some((page, key)) = self.get(to_clean, guard) {
                    let _ = self.replace_recurse_once(
                        to_clean,
                        key,
                        page,
                        guard,
                        true,
                    );
                }
            }

            // NB complete must happen AFTER calls to SA, because
            // when the iobuf's n_writers hits 0, we may transition
            // the segment to inactive, resulting in a race otherwise.
            log_reservation.complete();

            let count = self.updates.fetch_add(1, SeqCst) + 1;
            let should_snapshot =
                count % self.config.get_snapshot_after_ops() == 0;
            if should_snapshot {
                self.advance_snapshot();
            }
        } else {
            log_reservation.abort();
        }

        result.map_err(Some)
    }

    // caller is expected to have instantiated self.last_snapshot
    // in recovery already.
    fn advance_snapshot(&self) {
        let snapshot_opt_res = self.last_snapshot.try_lock();
        if snapshot_opt_res.is_err() {
            // some other thread is snapshotting
            warn!(
                "snapshot skipped because previous attempt \
            appears not to have completed"
            );
            return;
        }

        let mut snapshot_opt = snapshot_opt_res.unwrap();
        let last_snapshot = snapshot_opt.take().expect(
            "PageCache::advance_snapshot called before recovery",
        );

        self.log.flush();

        // we disable rewriting so that our log becomes append-only,
        // allowing us to iterate through it without corrupting ourselves.
        // NB must be called after taking the snapshot mutex.
        self.log.with_sa(|sa| sa.pause_rewriting());

        let max_lsn = last_snapshot.max_lsn;
        let start_lsn = max_lsn -
            (max_lsn % self.config.get_io_buf_size() as Lsn);

        debug!(
            "snapshot starting from offset {} to the segment containing ~{}",
            last_snapshot.max_lsn,
            self.log.stable_offset(),
        );

        let iter = self.log.iter_from(start_lsn);

        let next_snapshot = advance_snapshot(
            iter,
            last_snapshot,
            Some(self.t.clone()),
            &self.config,
        );

        self.log.with_sa(|sa| sa.resume_rewriting());

        // NB it's important to resume writing before replacing the snapshot
        // into the mutex, otherwise we create a race condition where the SA is
        // not actually paused when a snapshot happens.
        *snapshot_opt = Some(next_snapshot);
    }


    /// Try to atomically add a `PageFrag` to the page.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the atomic append fails.
    pub fn link<'g>(
        &self,
        pid: PageID,
        old: HPtr<'g, P>,
        new: P,
        guard: &'g Guard,
    ) -> Result<HPtr<'g, P>, Option<HPtr<'g, P>>> {
        let stack_ptr = self.inner.get(pid, guard);
        if stack_ptr.is_none() {
            return Err(None);
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
        let serialize_start = clock();
        let bytes = serialize(&prepend, Infinite).unwrap();
        M.serialize.measure(clock() - serialize_start);
        let log_reservation = self.log.reserve(bytes);
        let lsn = log_reservation.lsn();
        let lid = log_reservation.lid();

        let cache_entry = CacheEntry::Resident(new, lsn, lid);

        let result = unsafe { stack_ptr.deref().cap(old, cache_entry, guard) };

        if result.is_err() {
            log_reservation.abort();
        } else {
            let to_clean = self.log.with_sa(|sa| {
                sa.mark_link(pid, lsn, lid);
                sa.clean(None)
            });
            if let Some(to_clean) = to_clean {
                if let Some((page, key)) = self.get(to_clean, guard) {
                    let _ = self.replace_recurse_once(
                        to_clean,
                        key,
                        page,
                        guard,
                        true,
                    );
                }
            }

            // NB complete must happen AFTER calls to SA, because
            // when the iobuf's n_writers hits 0, we may transition
            // the segment to inactive, resulting in a race otherwise.
            log_reservation.complete();

            let count = self.updates.fetch_add(1, SeqCst) + 1;
            let should_snapshot =
                count % self.config.get_snapshot_after_ops() == 0;
            if should_snapshot {
                self.advance_snapshot();
            }
        }

        result.map_err(Some)
    }

    fn load_snapshot(&mut self) {
        // panic if not set
        let snapshot = self.last_snapshot.try_lock().unwrap().clone().unwrap();

        self.max_pid.store(snapshot.max_pid, SeqCst);

        let mut free = snapshot.free.clone();
        free.sort();
        free.reverse();
        for pid in free {
            trace!("adding {} to free during load_snapshot", pid);
            self.free.push(pid);
        }

        for (pid, lids) in &snapshot.pt {
            trace!("loading pid {} in load_snapshot", pid);

            let mut lids = lids.clone();
            let stack = Stack::default();

            if !lids.is_empty() {
                let (base_lsn, base_lid) = lids.remove(0);
                stack.push(CacheEntry::Flush(base_lsn, base_lid));

                for (lsn, lid) in lids {
                    stack.push(CacheEntry::PartialFlush(lsn, lid));
                }
            }

            self.inner.insert(*pid, stack).unwrap();
        }
    }
}

fn lids_from_stack<'g, P: Send + Sync>(
    head_ptr: HPtr<'g, P>,
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
            CacheEntry::Flush(_, ref lid) => {
                lids.push(*lid);
            }
        }
    }
    lids
}

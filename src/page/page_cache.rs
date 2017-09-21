use std::io::{Read, Seek, Write};
use std::path::Path;
use std::sync::Arc;

use crossbeam::sync::AtomicOption;
use coco::epoch::{Owned, Ptr, Scope, pin};

#[cfg(feature = "rayon")]
use rayon::prelude::*;

#[cfg(feature = "zstd")]
use zstd::block::{compress, decompress};

use super::*;

/// A lock-free pagecache which supports fragmented pages for dramatically
/// improving write throughput.
///
/// # Working with the `PageCache`
///
/// ```
/// extern crate sled;
///
/// use sled::Materializer;
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
///     let pc = sled::PageCache::new(TestMaterializer, conf.clone());
///     let (id, key) = pc.allocate();
///
///     // The first item in a page should be set using replace, which
///     // signals that this is the beginning of a new page history, and
///     // that any previous items associated with this page should be
///     // forgotten.
///     let key = pc.set(id, key, "a".to_owned()).unwrap();
///     let key = pc.merge(id, key, "b".to_owned()).unwrap();
///     let _key = pc.merge(id, key, "c".to_owned()).unwrap();
///
///     let (consolidated, _key) = pc.get(id).unwrap();
///
///     assert_eq!(consolidated, "abc".to_owned());
///
///     drop(pc);
///     std::fs::remove_file(path).unwrap();
/// }
/// ```
pub struct PageCache<PM, P, R>
    where PM: Materializer<PageFrag = P, Recovery = R>,
          PM: Send + Sync,
          P: 'static + Debug + PartialEq + Clone + Serialize + DeserializeOwned + Send + Sync,
          R: Debug + PartialEq + Clone + Serialize + DeserializeOwned + Send
{
    t: PM,
    inner: Radix<Stack<CacheEntry<P>>>,
    max_pid: AtomicUsize,
    free: Arc<Stack<PageID>>,
    log: LockFreeLog,
    lru: Lru,
    updates: AtomicUsize,
    last_snapshot: AtomicOption<Snapshot<R>>,
}

impl<PM, P, R> Drop for PageCache<PM, P, R>
    where PM: Materializer<PageFrag = P, Recovery = R>,
          PM: Send + Sync,
          P: 'static + Debug + PartialEq + Clone + Serialize + DeserializeOwned + Send + Sync,
          R: Debug + PartialEq + Clone + Serialize + DeserializeOwned + Send
{
    fn drop(&mut self) {
        // this is necessary because AtomicOption leaks
        // the inner object if left on its own
        self.last_snapshot.take(SeqCst);

        #[cfg(feature = "log")]
        trace!("PageCache dropped");
    }
}

unsafe impl<PM, P, R> Send for PageCache<PM, P, R>
    where PM: Materializer<PageFrag = P, Recovery = R>,
          PM: Send + Sync,
          P: 'static + Debug + PartialEq + Clone + Serialize + DeserializeOwned + Send + Sync,
          R: Debug + PartialEq + Clone + Serialize + DeserializeOwned + Send
{
}

unsafe impl<PM, P, R> Sync for PageCache<PM, P, R>
    where PM: Materializer<PageFrag = P, Recovery = R>,
          PM: Send + Sync,
          P: 'static + Debug + PartialEq + Clone + Serialize + DeserializeOwned + Send + Sync,
          R: Debug + PartialEq + Clone + Serialize + DeserializeOwned + Send
{
}

impl<PM, P, R> Debug for PageCache<PM, P, R>
    where PM: Materializer<PageFrag = P, Recovery = R>,
          PM: Send + Sync,
          P: 'static + Debug + PartialEq + Clone + Serialize + DeserializeOwned + Send + Sync,
          R: Debug + PartialEq + Clone + Serialize + DeserializeOwned + Send
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
          PM: Send + Sync,
          P: 'static + Debug + PartialEq + Clone + Serialize + DeserializeOwned + Send + Sync,
          R: Debug + PartialEq + Clone + Serialize + DeserializeOwned + Send
{
    /// Instantiate a new `PageCache`.
    pub fn new(pm: PM, config: Config) -> PageCache<PM, P, R> {
        let cache_capacity = config.get_cache_capacity();
        let cache_shard_bits = config.get_cache_bits();
        let lru = Lru::new(cache_capacity, cache_shard_bits);

        let snapshot = Snapshot::default();
        let last_snapshot = AtomicOption::new();
        last_snapshot.swap(snapshot, SeqCst);

        PageCache {
            t: pm,
            inner: Radix::default(),
            max_pid: AtomicUsize::new(0),
            free: Arc::new(Stack::default()),
            log: LockFreeLog::start_system(config),
            lru: lru,
            updates: AtomicUsize::new(0),
            last_snapshot: last_snapshot,
        }
    }

    /// Return the configuration used by the underlying system.
    pub fn config(&self) -> &Config {
        self.log.config()
    }

    /// Read updates from the log, apply them to our pagecache.
    pub fn recover(&mut self) -> Option<R> {
        // we call write_snapshot here to "catch-up" the snapshot before
        // recovering from it. this allows us to reuse the snapshot
        // generation logic as initial log parsing logic. this is also
        // important for ensuring that we feed the provided `Materializer`
        // a single, linearized history, rather than going back in time
        // when generating a snapshot.
        self.write_snapshot();

        // we then read it back,
        let snapshot = self.read_snapshot();
        let recovery = snapshot.recovery.clone();
        self.last_snapshot.swap(snapshot, SeqCst);

        #[cfg(feature = "log")]
        debug!(
            "recovery complete, returning recovery state to PageCache owner: {:?}",
            recovery
        );
        recovery
    }

    /// Create a new page, trying to reuse old freed pages if possible
    /// to maximize underlying `Radix` pointer density.
    pub fn allocate(&self) -> (PageID, CasKey<P>) {
        let pid = self.free.pop().unwrap_or_else(
            || self.max_pid.fetch_add(1, SeqCst),
        );
        #[cfg(feature = "log")]
        trace!("allocating pid {}", pid);
        self.inner.insert(pid, Stack::default()).unwrap();

        // write info to log
        let prepend: LoggedUpdate<P> = LoggedUpdate {
            pid: pid,
            update: Update::Alloc,
        };
        let serialize_start = clock();
        let bytes = serialize(&prepend, Infinite).unwrap();
        M.serialize.measure(clock() - serialize_start);

        self.log.write(bytes);

        (pid, Ptr::null().into())
    }

    /// Free a particular page.
    pub fn free(&self, pid: PageID) {
        pin(|scope| {
            let deleted = self.inner.del(pid, scope);
            if deleted.is_none() {
                return;
            }

            // write info to log
            let prepend: LoggedUpdate<P> = LoggedUpdate {
                pid: pid,
                update: Update::Del,
            };
            let serialize_start = clock();
            let bytes = serialize(&prepend, Infinite).unwrap();
            M.serialize.measure(clock() - serialize_start);

            let res = self.log.reserve(bytes);
            let lsn = res.lsn();
            res.complete();

            // add pid to free stack to reduce fragmentation over time
            unsafe {
                let cas_key = deleted.unwrap().deref().head(scope).into();

                let start = clock();
                let mut sa = self.log.iobufs.segment_accountant.lock().unwrap();
                let locked = clock();
                M.accountant_lock.measure(locked - start);
                sa.freed(pid, lids_from_stack(cas_key, scope), lsn);
                M.accountant_hold.measure(clock() - locked);
            }

            let pd = Owned::new(PidDropper(pid, self.free.clone()));
            let ptr = pd.into_ptr(scope);
            unsafe {
                scope.defer_drop(ptr);
            }
        });
    }

    /// Try to retrieve a page by its logical ID.
    pub fn get(&self, pid: PageID) -> Option<(PM::PageFrag, CasKey<P>)> {
        pin(|scope| {
            let stack_ptr = self.inner.get(pid, scope);
            if stack_ptr.is_none() {
                return None;
            }

            let stack_ptr = stack_ptr.unwrap();

            let head = unsafe { stack_ptr.deref().head(scope) };

            self.page_in(pid, head, stack_ptr, scope)
        })
    }

    fn page_out<'s>(&self, to_evict: Vec<PageID>, scope: &'s Scope) {
        let start = clock();
        for pid in to_evict {
            let stack_ptr = self.inner.get(pid, scope);
            if stack_ptr.is_none() {
                continue;
            }

            let stack_ptr = stack_ptr.unwrap();

            let head = unsafe { stack_ptr.deref().head(scope) };
            let stack_iter = StackIter::from_ptr(head, scope);

            let mut cache_entries: Vec<CacheEntry<P>> =
                stack_iter.map(|ptr| (*ptr).clone()).collect();

            // ensure the last entry is a Flush
            let last = cache_entries.pop().map(|last_ce| match last_ce {
                CacheEntry::MergedResident(_, lid) |
                CacheEntry::Resident(_, lid) |
                CacheEntry::Flush(lid) => CacheEntry::Flush(lid),
                CacheEntry::PartialFlush(_) => panic!("got PartialFlush at end of stack..."),
            });

            if last.is_none() {
                M.page_out.measure(clock() - start);
                return;
            }

            let mut new_stack = Vec::with_capacity(cache_entries.len() + 1);
            for entry in cache_entries {
                match entry {
                    CacheEntry::PartialFlush(lid) |
                    CacheEntry::MergedResident(_, lid) |
                    CacheEntry::Resident(_, lid) => {
                        new_stack.push(CacheEntry::PartialFlush(lid));
                    }
                    CacheEntry::Flush(_) => panic!("got Flush in middle of stack..."),
                }
            }
            new_stack.push(last.unwrap());
            let node = node_from_frag_vec(new_stack);

            unsafe {
                if stack_ptr
                    .deref()
                    .cas(head, node.into_ptr(scope), scope)
                    .is_err()
                {}
            }
        }
        M.page_out.measure(clock() - start);
    }

    fn pull(&self, lid: LogID) -> P {
        let start = clock();
        let bytes = match self.log.read(lid).map_err(|_| ()) {
            Ok(LogRead::Flush(_lsn, data, _len)) => data,
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

    fn page_in<'s>(
        &self,
        pid: PageID,
        mut head: Ptr<'s, ds::stack::Node<CacheEntry<P>>>,
        stack_ptr: Ptr<'s, ds::stack::Stack<CacheEntry<P>>>,
        scope: &'s Scope,
    ) -> Option<(PM::PageFrag, CasKey<P>)> {
        let start = clock();
        let stack_iter = StackIter::from_ptr(head, scope);

        let mut to_merge = vec![];
        let mut merged_resident = false;
        let mut lids = vec![];
        let mut fix_up_length = 0;

        for cache_entry_ptr in stack_iter {
            match *cache_entry_ptr {
                CacheEntry::Resident(ref page_frag, ref lid) => {
                    if !merged_resident {
                        to_merge.push(page_frag);
                    }
                    lids.push(lid);
                }
                CacheEntry::MergedResident(ref page_frag, ref lid) => {
                    if lids.is_empty() {
                        // Short circuit merging and fix-up if we only
                        // have one frag.
                        return Some((page_frag.clone(), head.into()));
                    }
                    if !merged_resident {
                        to_merge.push(page_frag);
                        merged_resident = true;
                        fix_up_length = lids.len();
                    }
                    lids.push(lid);
                }
                CacheEntry::PartialFlush(ref lid) |
                CacheEntry::Flush(ref lid) => {
                    lids.push(lid);
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
                let mut pulled: Vec<P> = to_pull.par_iter().map(|&&lid| self.pull(lid)).collect();
                fetched.append(&mut pulled);
            }

            #[cfg(not(feature = "rayon"))]
            for &&lid in to_pull {
                fetched.push(self.pull(lid));
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
        self.page_out(to_evict, scope);

        if lids.len() > self.config().get_page_consolidation_threshold() {
            match self.set(pid, head.into(), merged.clone()) {
                Ok(new_head) => head = new_head.into(),
                Err(None) => return None,
                _ => {}
            }
        } else if !fetched.is_empty() ||
                   fix_up_length >= self.config().get_cache_fixup_threshold()
        {
            let mut new_entries = Vec::with_capacity(lids.len());

            let head_lid = lids.remove(0);
            let head_entry = CacheEntry::MergedResident(merged.clone(), *head_lid);
            new_entries.push(head_entry);

            let mut tail = if let Some(lid) = lids.pop() {
                Some(CacheEntry::Flush(*lid))
            } else {
                None
            };

            for &&lid in &lids {
                new_entries.push(CacheEntry::PartialFlush(lid));
            }

            if let Some(tail) = tail.take() {
                new_entries.push(tail);
            }

            let node = node_from_frag_vec(new_entries);

            let res = unsafe { stack_ptr.deref().cas(head, node.into_ptr(scope), scope) };
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

        Some((merged, head.into()))
    }

    /// Replace an existing page with a different set of `PageFrag`s.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the page has changed since the provided `CasKey` was created.
    pub fn set(&self, pid: PageID, old: CasKey<P>, new: P) -> Result<CasKey<P>, Option<CasKey<P>>> {
        pin(|scope| {
            let stack_ptr = self.inner.get(pid, scope);
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
            let lid = log_reservation.lid();

            let cache_entry = CacheEntry::MergedResident(new, lid);

            let node = node_from_frag_vec(vec![cache_entry]).into_ptr(scope);

            let result = unsafe { stack_ptr.deref().cas(old.clone().into(), node, scope) };

            if result.is_ok() {
                let lid = log_reservation.lid();
                let lsn = log_reservation.lsn();
                log_reservation.complete();

                {
                    let start = clock();
                    let mut sa = self.log.iobufs.segment_accountant.lock().unwrap();
                    let locked = clock();
                    M.accountant_lock.measure(locked - start);
                    sa.set(pid, lids_from_stack(old, scope), lid, lsn);
                    M.accountant_hold.measure(clock() - locked);
                }

                let count = self.updates.fetch_add(1, SeqCst) + 1;
                let should_snapshot = count % self.config().get_snapshot_after_ops() == 0;
                if should_snapshot {
                    self.write_snapshot();
                }
            } else {
                log_reservation.abort();
            }

            result.map(|ok| ok.into()).map_err(|e| Some(e.into()))
        })
    }


    /// Try to atomically add a `PageFrag` to the page.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the page has changed since the provided `CasKey` was created.
    pub fn merge(
        &self,
        pid: PageID,
        old: CasKey<P>,
        new: P,
    ) -> Result<CasKey<P>, Option<CasKey<P>>> {
        pin(|scope| {
            let stack_ptr = self.inner.get(pid, scope);
            if stack_ptr.is_none() {
                return Err(None);
            }
            let stack_ptr = stack_ptr.unwrap();

            let prepend: LoggedUpdate<P> = LoggedUpdate {
                pid: pid,
                update: Update::Append(new.clone()),
            };
            let serialize_start = clock();
            let bytes = serialize(&prepend, Infinite).unwrap();
            M.serialize.measure(clock() - serialize_start);
            let log_reservation = self.log.reserve(bytes);
            let lid = log_reservation.lid();

            let cache_entry = CacheEntry::Resident(new, lid);

            let result = unsafe { stack_ptr.deref().cap(old.into(), cache_entry, scope) };

            if result.is_err() {
                log_reservation.abort();
            } else {
                let lsn = log_reservation.lsn();
                log_reservation.complete();

                {
                    let start = clock();
                    let mut sa = self.log.iobufs.segment_accountant.lock().unwrap();
                    let locked = clock();
                    M.accountant_lock.measure(locked - start);
                    sa.merged(pid, lid, lsn);
                    M.accountant_hold.measure(clock() - locked);
                }

                let count = self.updates.fetch_add(1, SeqCst) + 1;
                let should_snapshot = count % self.config().get_snapshot_after_ops() == 0;
                if should_snapshot {
                    self.write_snapshot();
                }
            }

            result.map(|ok| ok.into()).map_err(|e| Some(e.into()))
        })
    }

    fn write_snapshot(&self) {
        let start = clock();

        self.log.flush();

        let prefix = self.config().snapshot_prefix();

        let snapshot_opt = self.last_snapshot.take(SeqCst);
        if snapshot_opt.is_none() {
            // some other thread is snapshotting
            #[cfg(feature = "log")]
            warn!(
                "snapshot skipped because previous attempt \
                  appears not to have completed"
            );
            M.write_snapshot.measure(clock() - start);
            return;
        }

        // we disable rewriting so that our log becomes append-only,
        // allowing us to iterate through it without corrupting ourselves.
        {
            let start = clock();
            let mut sa = self.log.iobufs.segment_accountant.lock().unwrap();
            let locked = clock();
            M.accountant_lock.measure(locked - start);
            sa.pause_rewriting();
            M.accountant_hold.measure(clock() - locked);
        }

        let mut snapshot = snapshot_opt.unwrap();
        let end_lsn = self.log.stable_offset();

        #[cfg(feature = "log")]
        info!(
            "snapshot starting from offset {} to the segment containing {}",
            snapshot.max_segment_lsn,
            end_lsn,
        );

        let io_buf_size = self.config().get_io_buf_size();

        let mut recovery = snapshot.recovery.take();
        let mut max_lsn = snapshot.max_lsn;
        let start_lsn = max_lsn - (max_lsn % io_buf_size as Lsn);

        for (segment_lsn, log_id, bytes) in self.log.iter_from(start_lsn + 1) {
            let lsn = segment_lsn + (log_id % io_buf_size as Lsn);

            if lsn > end_lsn {
                break;
            } else if lsn <= max_lsn {
                // println!("continuing in write_snapshot, lsn {} max_lsn {}", lsn, max_lsn);
                continue;
            }

            // println!( "lsn {} max_lsn {} log_id {} end_lsn {}", lsn, max_lsn, log_id, end_lsn);
            assert!(lsn > max_lsn);
            max_lsn = lsn;

            let idx = log_id as usize / io_buf_size;
            if snapshot.segments.len() <= idx {
                snapshot.segments.resize(idx + 1, log::Segment::default());
            }
            snapshot.segments[idx].lsn = Some(segment_lsn);

            assert_eq!(
                segment_lsn / io_buf_size as Lsn * io_buf_size as Lsn,
                segment_lsn,
                "segment lsn is unaligned! fix above lsn statement..."
            );

            if let Ok(prepend) = deserialize::<LoggedUpdate<P>>(&*bytes) {
                if prepend.pid >= snapshot.max_pid {
                    snapshot.max_pid = prepend.pid + 1;
                }

                match prepend.update {
                    Update::Append(partial_page) => {
                        snapshot.segments[idx].pids.insert(prepend.pid);

                        let r = self.t.recover(&partial_page);
                        if r.is_some() {
                            recovery = r;
                        }

                        let lids = snapshot.pt.get_mut(&prepend.pid).unwrap();
                        lids.push(log_id);
                    }
                    Update::Compact(partial_page) => {
                        if let Some(lids) = snapshot.pt.get(&prepend.pid) {
                            for lid in lids {
                                let old_idx = *lid as usize / io_buf_size;
                                let old_segment = &mut snapshot.segments[old_idx];
                                if old_segment.pids_len == 0 {
                                    old_segment.pids_len = old_segment.pids.len();
                                }
                                old_segment.pids.remove(&(*lid as PageID));
                            }
                        }

                        snapshot.segments[idx].pids.insert(prepend.pid);

                        let r = self.t.recover(&partial_page);
                        if r.is_some() {
                            recovery = r;
                        }

                        snapshot.pt.insert(prepend.pid, vec![log_id]);
                    }
                    Update::Del => {
                        if let Some(lids) = snapshot.pt.get(&prepend.pid) {
                            for lid in lids {
                                let old_idx = *lid as usize / io_buf_size;
                                let old_segment = &mut snapshot.segments[old_idx];
                                if old_segment.pids_len == 0 {
                                    old_segment.pids_len = old_segment.pids.len();
                                }
                                old_segment.pids.remove(&(*lid as PageID));
                            }
                        }

                        snapshot.pt.remove(&prepend.pid);
                        snapshot.free.push(prepend.pid);
                    }
                    Update::Alloc => {
                        snapshot.pt.insert(prepend.pid, vec![]);
                        snapshot.free.retain(|&pid| pid != prepend.pid);
                    }
                }
            }
        }
        snapshot.free.sort();
        snapshot.free.reverse();
        snapshot.max_lsn = max_lsn;
        snapshot.recovery = recovery;

        let raw_bytes = serialize(&snapshot, Infinite).unwrap();


        #[cfg(feature = "zstd")]
        let bytes = if self.config().get_use_compression() {
            compress(&*raw_bytes, 5).unwrap()
        } else {
            raw_bytes
        };

        #[cfg(not(feature = "zstd"))]
        let bytes = raw_bytes;

        let crc64: [u8; 8] = unsafe { std::mem::transmute(crc64(&*bytes)) };

        self.last_snapshot.swap(snapshot, SeqCst);

        let path_1 = format!("{}.{}.in___motion", prefix, max_lsn);
        let path_2 = format!("{}.{}", prefix, max_lsn);
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path_1)
            .unwrap();

        // write the snapshot bytes, followed by a crc64 checksum at the end
        f.write_all(&*bytes).unwrap();
        f.write_all(&crc64).unwrap();
        f.sync_all().unwrap();
        drop(f);

        std::fs::rename(path_1, &path_2).expect("failed to write snapshot");

        // clean up any old snapshots
        let candidates = self.config().get_snapshot_files();
        for path in candidates {
            let path_str = Path::new(&path).file_name().unwrap().to_str().unwrap();
            if !path_2.ends_with(&*path_str) {
                #[cfg(feature = "log")]
                info!("removing old snapshot file {:?}", path);

                if let Err(_e) = std::fs::remove_file(&path) {
                    #[cfg(feature = "log")]
                    warn!("failed to remove old snapshot file, maybe snapshot race? {}", _e);
                }
            }
        }
        {
            let start = clock();
            let mut sa = self.log.iobufs.segment_accountant.lock().unwrap();
            let locked = clock();
            M.accountant_lock.measure(locked - start);
            sa.resume_rewriting();
            M.accountant_hold.measure(clock() - locked);
        }
        M.write_snapshot.measure(clock() - start);
    }

    fn read_snapshot(&mut self) -> Snapshot<R> {
        let mut candidates = self.config().get_snapshot_files();
        if candidates.is_empty() {
            #[cfg(feature = "log")]
            info!("no previous snapshot found");
            return Snapshot::default();
        }

        candidates.sort_by_key(|path| std::fs::metadata(path).unwrap().created().unwrap());

        let path = candidates.pop().unwrap();

        let mut f = std::fs::OpenOptions::new().read(true).open(&path).unwrap();

        let mut buf = vec![];
        f.read_to_end(&mut buf).unwrap();
        let len = buf.len();
        buf.split_off(len - 8);

        let mut crc_expected_bytes = [0u8; 8];
        f.seek(std::io::SeekFrom::End(-8)).unwrap();
        f.read_exact(&mut crc_expected_bytes).unwrap();

        let crc_expected: u64 = unsafe { std::mem::transmute(crc_expected_bytes) };
        let crc_actual = crc64(&*buf);

        if crc_expected != crc_actual {
            panic!("crc for snapshot file {:?} failed!", path);
        }

        #[cfg(feature = "zstd")]
        let bytes = if self.config().get_use_compression() {
            decompress(&*buf, 1_000_000).unwrap()
        } else {
            buf
        };

        #[cfg(not(feature = "zstd"))]
        let bytes = buf;

        let snapshot = deserialize::<Snapshot<R>>(&*bytes).unwrap();

        self.load_snapshot(&snapshot);

        snapshot
    }

    fn load_snapshot(&mut self, snapshot: &Snapshot<R>) {
        self.max_pid.store(snapshot.max_pid, SeqCst);

        let mut free = snapshot.free.clone();
        free.sort();
        free.reverse();
        for pid in free {
            #[cfg(feature = "log")]
            trace!("adding {} to free during load_snapshot", pid);
            self.free.push(pid);
        }

        for (pid, lids) in &snapshot.pt {
            #[cfg(feature = "log")]
            trace!("loading pid {} in load_snapshot", pid);
            let mut lids = lids.clone();
            let stack = Stack::default();

            if !lids.is_empty() {
                let base = lids.remove(0);
                stack.push(CacheEntry::Flush(base));

                for lid in lids {
                    stack.push(CacheEntry::PartialFlush(lid));
                }
            }

            self.inner.insert(*pid, stack).unwrap();
        }

        let mut sa = self.log.iobufs.segment_accountant.lock().unwrap();
        sa.initialize_from_segments(snapshot.segments.clone());
    }
}

fn lids_from_stack<P: Send + Sync>(stack_ptr: CasKey<P>, scope: &Scope) -> Vec<LogID> {
    // generate a list of the old log ID's
    let stack_iter = StackIter::from_ptr(stack_ptr.into(), scope);

    let mut lids = vec![];
    for cache_entry_ptr in stack_iter {
        match *cache_entry_ptr {
            CacheEntry::Resident(_, ref lid) |
            CacheEntry::MergedResident(_, ref lid) |
            CacheEntry::PartialFlush(ref lid) |
            CacheEntry::Flush(ref lid) => {
                lids.push(*lid);
            }
        }
    }
    lids
}

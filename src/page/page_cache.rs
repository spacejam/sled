use std::io::{Read, Seek, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use coco::epoch::{Owned, Ptr, Scope, pin};

#[cfg(feature = "rayon")]
use rayon::prelude::*;

#[cfg(feature = "zstd")]
use zstd::block::{compress, decompress};

use super::*;

/// A lock-free pagecache which supports fragmented pages
/// for dramatically improving write throughput.
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
///     let pc = sled::PageCache::new(TestMaterializer,
///                                   conf.clone());
///     let (id, key) = pc.allocate();
///
///     // The first item in a page should be set using replace,
///     // which signals that this is the beginning of a new
///     // page history, and that any previous items associated
///     // with this page should be forgotten.
///     let key = pc.replace(id, key, "a".to_owned()).unwrap();
///
///     // Subsequent atomic updates should be added with link.
///     let key = pc.link(id, key, "b".to_owned()).unwrap();
///     let _key = pc.link(id, key, "c".to_owned()).unwrap();
///
///     // When getting a page, the provide `Materializer` is
///     // used to merge all pages together.
///     let (consolidated, _key) = pc.get(id).unwrap();
///
///     assert_eq!(consolidated, "abc".to_owned());
///
///     drop(pc);
///     std::fs::remove_file(path).unwrap();
/// }
/// ```
pub struct PageCache<PM, P, R>
    where P: 'static + Send + Sync
{
    t: PM,
    config: Config,
    inner: Radix<Stack<CacheEntry<P>>>,
    max_pid: AtomicUsize,
    free: Arc<Stack<PageID>>,
    log: Log,
    lru: Lru,
    updates: AtomicUsize,
    last_snapshot: Mutex<Option<Snapshot<R>>>,
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
          PM: Send + Sync,
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
    pub fn new(pm: PM, config: Config) -> PageCache<PM, P, R> {
        let cache_capacity = config.get_cache_capacity();
        let cache_shard_bits = config.get_cache_bits();
        let lru = Lru::new(cache_capacity, cache_shard_bits);

        PageCache {
            t: pm,
            config: config.clone(),
            inner: Radix::default(),
            max_pid: AtomicUsize::new(0),
            free: Arc::new(Stack::default()),
            log: Log::start_system(config),
            lru: lru,
            updates: AtomicUsize::new(0),
            last_snapshot: Mutex::new(None),
        }
    }

    /// Read updates from the log, apply them to our pagecache.
    pub fn recover(&mut self) -> Option<R> {
        // we call advance_snapshot here to "catch-up" the snapshot using the
        // logged updates before recovering from it. this allows us to reuse
        // the snapshot generation logic as initial log parsing logic. this is
        // also important for ensuring that we feed the provided `Materializer`
        // a single, linearized history, rather than going back in time
        // when generating a snapshot.
        self.advance_snapshot();

        // now we read it back in
        self.read_snapshot();
        self.load_snapshot();

        let mu = &self.last_snapshot.lock().unwrap();

        let recovery = if let Some(ref snapshot) = **mu {
            snapshot.recovery.clone()
        } else {
            None
        };

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

                self.log.with_sa(|sa| {
                    sa.freed(pid, lids_from_stack(cas_key, scope), lsn)
                });
            }

            let pd = Owned::new(PidDropper(pid, self.free.clone()));
            let ptr = pd.into_ptr(scope);
            unsafe {
                scope.defer_drop(ptr);
                scope.flush();
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

    fn pull(&self, lsn: Lsn, lid: LogID) -> P {
        trace!("pulling lsn {} lid {} from disk", lsn, lid);
        let start = clock();
        let bytes = match self.log.read(lsn, lid).map_err(|_| ()) {
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
                        println!("short circuiting");
                        return Some((page_frag.clone(), head.into()));
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
        self.page_out(to_evict, scope);

        if lids.len() > self.config.get_page_consolidation_threshold() {
            println!("consolidating pid {}!", pid);
            match self.replace(pid, head.into(), merged.clone()) {
                Ok(new_head) => {
                    println!("setting new head to {:?}", new_head);
                    head = new_head.into()
                }
                Err(None) => return None,
                _ => println!("some other thing beat us or something"),
            }
        } else if !fetched.is_empty() ||
                   fix_up_length >= self.config.get_cache_fixup_threshold()
        {
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

            let res = unsafe {
                stack_ptr.deref().cas(head, node.into_ptr(scope), scope)
            };
            if let Ok(new_head) = res {
                println!("another cas worked!");
                head = new_head;
            } else {
                println!(
                    "our fix-up didn't work, something else won: {:?}",
                    res
                );
                // NB explicitly DON'T update head, as our witnessed
                // entries do NOT contain the latest state. This
                // may not matter to callers who only care about
                // reading, but maybe we should signal that it's
                // out of date for those who page_in in an attempt
                // to modify!
            }
        }

        M.page_in.measure(clock() - start);

        println!("page_in returning key {:?}", head);
        Some((merged, head.into()))
    }

    // If we can relocate any pages to clean a segment,
    // try to do so.
    fn try_cleaning_segment(&self) {
        let to_clean = self.log.with_sa(|sa| sa.clean());

        if let Some(pid) = to_clean {
            if let Some((page, key)) = self.get(pid) {
                let _ = self.replace(pid, key, page);
            }
        }
    }

    /// Replace an existing page with a different set of `PageFrag`s.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the page has changed since the provided `CasKey` was created.
    pub fn replace(
        &self,
        pid: PageID,
        old: CasKey<P>,
        new: P,
    ) -> Result<CasKey<P>, Option<CasKey<P>>> {
        println!("replacing pid {}", pid);
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
            let lsn = log_reservation.lsn();
            let lid = log_reservation.lid();

            let cache_entry = CacheEntry::MergedResident(new, lsn, lid);

            let node = node_from_frag_vec(vec![cache_entry]).into_ptr(scope);

            let result = unsafe {
                stack_ptr.deref().cas(old.clone().into(), node, scope)
            };

            if result.is_ok() {
                let lid = log_reservation.lid();
                let lsn = log_reservation.lsn();
                log_reservation.complete();

                self.log.with_sa(|sa| {
                    println!("mark_replace {}", pid);
                    sa.mark_replace(pid, lids_from_stack(old, scope), lid, lsn)
                });

                let count = self.updates.fetch_add(1, SeqCst) + 1;
                let should_snapshot =
                    count % self.config.get_snapshot_after_ops() == 0;
                if should_snapshot {
                    println!("before snapshot");
                    self.advance_snapshot();
                    println!("after snapshot");
                }
            } else {
                log_reservation.abort();
            }

            self.try_cleaning_segment();

            result.map(|ok| ok.into()).map_err(|e| Some(e.into()))
        })
    }


    /// Try to atomically add a `PageFrag` to the page.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the page has changed since the provided `CasKey` was created.
    pub fn link(
        &self,
        pid: PageID,
        old: CasKey<P>,
        new: P,
    ) -> Result<CasKey<P>, Option<CasKey<P>>> {
        pin(|scope| {
            println!("a");
            let stack_ptr = self.inner.get(pid, scope);
            if stack_ptr.is_none() {
                return Err(None);
            }
            let stack_ptr = stack_ptr.unwrap();

            let old_key: Ptr<_> = old.into();

            let prepend: LoggedUpdate<P> = LoggedUpdate {
                pid: pid,
                update: if old_key.is_null() {
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

            println!("b");
            let result =
                unsafe { stack_ptr.deref().cap(old_key, cache_entry, scope) };

            if result.is_err() {
                log_reservation.abort();
            } else {
                let lsn = log_reservation.lsn();
                let lid = log_reservation.lid();
                log_reservation.complete();

                self.log.with_sa(|sa| sa.mark_link(pid, lid, lsn));

                let count = self.updates.fetch_add(1, SeqCst) + 1;
                let should_snapshot =
                    count % self.config.get_snapshot_after_ops() == 0;
                if should_snapshot {
                    self.advance_snapshot();
                }
            }

            self.try_cleaning_segment();

            result.map(|ok| ok.into()).map_err(|e| Some(e.into()))
        })
    }

    fn advance_snapshot(&self) {
        let start = clock();

        self.log.flush();

        let snapshot_opt_res = self.last_snapshot.try_lock();
        if snapshot_opt_res.is_err() {
            // some other thread is snapshotting
            warn!(
                "snapshot skipped because previous attempt \
                  appears not to have completed"
            );
            M.advance_snapshot.measure(clock() - start);
            return;
        }
        let mut snapshot_opt = snapshot_opt_res.unwrap();
        let mut snapshot =
            snapshot_opt.take().unwrap_or_else(Snapshot::default);

        // we disable rewriting so that our log becomes append-only,
        // allowing us to iterate through it without corrupting ourselves.
        self.log.with_sa(|sa| sa.pause_rewriting());
        println!("disabled rewriting in advance_snapshot");


        println!("building on top of old snapshot: {:?}", snapshot);
        trace!("building on top of old snapshot: {:?}", snapshot);

        info!(
            "snapshot starting from offset {} to the segment containing ~{}",
            snapshot.max_lsn,
            self.log.stable_offset(),
        );

        let io_buf_size = self.config.get_io_buf_size();

        let mut recovery = snapshot.recovery.take();
        let mut max_lsn = snapshot.max_lsn;
        let start_lsn = max_lsn - (max_lsn % io_buf_size as Lsn);
        let stop_lsn = self.log.stable_offset();

        for (lsn, log_id, bytes) in self.log.iter_from(start_lsn) {
            if stop_lsn > 0 && lsn > stop_lsn {
                // we've gone past the known-stable offset.
                break;
            }
            let segment_lsn = lsn / io_buf_size as Lsn * io_buf_size as Lsn;

            trace!(
                "in advance_snapshot looking at item: segment lsn {} lsn {} lid {}",
                segment_lsn,
                lsn,
                log_id
            );

            if lsn <= max_lsn {
                // don't process alread-processed Lsn's.
                trace!(
                    "continuing in advance_snapshot, lsn {} log_id {} max_lsn {}",
                    lsn,
                    log_id,
                    max_lsn
                );
                continue;
            }

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

            // unwrapping this because it's already passed the crc check
            // in the log iterator
            trace!("trying to deserialize buf for lid {} lsn {}", log_id, lsn);
            let deserialization = deserialize::<LoggedUpdate<P>>(&*bytes);

            if let Err(e) = deserialization {
                error!(
                    "failed to deserialize buffer for item in log: lsn {} \
                    lid {}: {:?}",
                    lsn,
                    log_id,
                    e
                );
                continue;
            }

            let prepend = deserialization.unwrap();

            if prepend.pid >= snapshot.max_pid {
                snapshot.max_pid = prepend.pid + 1;
            }

            match prepend.update {
                Update::Append(partial_page) => {
                    // Because we rewrite pages over time, we may have relocated
                    // a page's initial Compact to a later segment. We should skip
                    // over pages here unless we've encountered a Compact or Alloc
                    // for them.
                    if let Some(lids) = snapshot.pt.get_mut(&prepend.pid) {
                        trace!(
                            "append of pid {} at lid {} lsn {}",
                            prepend.pid,
                            log_id,
                            lsn
                        );

                        snapshot.segments[idx].pids.insert(prepend.pid);

                        let r = self.t.recover(&partial_page);
                        if r.is_some() {
                            recovery = r;
                        }

                        lids.push((lsn, log_id));
                    }
                }
                Update::Compact(partial_page) => {
                    trace!(
                        "compact of pid {} at lid {} lsn {}",
                        prepend.pid,
                        log_id,
                        lsn
                    );
                    if let Some(lids) = snapshot.pt.remove(&prepend.pid) {
                        for (_lsn, lid) in lids {
                            let old_idx = lid as usize / io_buf_size;
                            let old_segment = &mut snapshot.segments[old_idx];
                            // FIXME this pids_len is borked
                            if old_segment.pids_len == 0 {
                                old_segment.pids_len = old_segment.pids.len();
                            }
                            old_segment.pids.remove(&prepend.pid);
                        }
                    }

                    snapshot.segments[idx].pids.insert(prepend.pid);

                    let r = self.t.recover(&partial_page);
                    if r.is_some() {
                        recovery = r;
                    }

                    snapshot.pt.insert(prepend.pid, vec![(lsn, log_id)]);
                }
                Update::Del => {
                    trace!(
                        "del of pid {} at lid {} lsn {}",
                        prepend.pid,
                        log_id,
                        lsn
                    );
                    if let Some(lids) = snapshot.pt.remove(&prepend.pid) {
                        // this could fail if our Alloc was nuked
                        for (_lsn, lid) in lids {
                            let old_idx = lid as usize / io_buf_size;
                            let old_segment = &mut snapshot.segments[old_idx];
                            // FIXME this pids_len is borked
                            if old_segment.pids_len == 0 {
                                old_segment.pids_len = old_segment.pids.len();
                            }
                            old_segment.pids.remove(&prepend.pid);
                        }
                    }

                    snapshot.free.push(prepend.pid);
                }
                Update::Alloc => {
                    trace!(
                        "alloc of pid {} at lid {} lsn {}",
                        prepend.pid,
                        log_id,
                        lsn
                    );

                    snapshot.pt.insert(prepend.pid, vec![]);
                    snapshot.free.retain(|&pid| pid != prepend.pid);
                }
            }
        }

        snapshot.free.sort();
        snapshot.free.reverse();
        snapshot.max_lsn = max_lsn;
        snapshot.recovery = recovery;

        self.write_snapshot(&snapshot);

        trace!("generated new snapshot: {:?}", snapshot);
        println!("generated new snapshot: {:?}", snapshot);

        self.log.with_sa(|sa| sa.resume_rewriting());
        println!("resumed rewriting in advance_snapshot");

        // NB replacing the snapshot must come after the resume_rewriting call
        // otherwise we create a race condition where we corrupt an in-progress
        // snapshot generating iterator.
        *snapshot_opt = Some(snapshot);

        M.advance_snapshot.measure(clock() - start);
    }

    fn write_snapshot(&self, snapshot: &Snapshot<R>) {
        let raw_bytes = serialize(&snapshot, Infinite).unwrap();

        #[cfg(feature = "zstd")]
        let bytes = if self.config.get_use_compression() {
            compress(&*raw_bytes, 5).unwrap()
        } else {
            raw_bytes
        };

        #[cfg(not(feature = "zstd"))]
        let bytes = raw_bytes;

        let crc64: [u8; 8] = unsafe { std::mem::transmute(crc64(&*bytes)) };

        let prefix = self.config.snapshot_prefix();

        let path_1 = format!("{}.{}.in___motion", prefix, snapshot.max_lsn);
        let path_2 = format!("{}.{}", prefix, snapshot.max_lsn);
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

        trace!("wrote snapshot to {}", path_1);

        std::fs::rename(path_1, &path_2).expect("failed to write snapshot");

        trace!("renamed snapshot to {}", path_2);

        // clean up any old snapshots
        let candidates = self.config.get_snapshot_files();
        for path in candidates {
            let path_str =
                Path::new(&path).file_name().unwrap().to_str().unwrap();
            if !path_2.ends_with(&*path_str) {
                info!("removing old snapshot file {:?}", path);

                if let Err(_e) = std::fs::remove_file(&path) {
                    warn!(
                        "failed to remove old snapshot file, maybe snapshot race? {}",
                        _e
                    );
                }
            }
        }
    }

    fn read_snapshot(&self) {
        let mut candidates = self.config.get_snapshot_files();
        if candidates.is_empty() {
            info!("no previous snapshot found");
            return;
        }

        candidates.sort_by_key(
            |path| std::fs::metadata(path).unwrap().created().unwrap(),
        );

        let path = candidates.pop().unwrap();

        let mut f = std::fs::OpenOptions::new().read(true).open(&path).unwrap();

        let mut buf = vec![];
        f.read_to_end(&mut buf).unwrap();
        let len = buf.len();
        buf.split_off(len - 8);

        let mut crc_expected_bytes = [0u8; 8];
        f.seek(std::io::SeekFrom::End(-8)).unwrap();
        f.read_exact(&mut crc_expected_bytes).unwrap();

        let crc_expected: u64 =
            unsafe { std::mem::transmute(crc_expected_bytes) };
        let crc_actual = crc64(&*buf);

        if crc_expected != crc_actual {
            panic!("crc for snapshot file {:?} failed!", path);
        }

        #[cfg(feature = "zstd")]
        let bytes = if self.config.get_use_compression() {
            decompress(&*buf, self.config.get_io_buf_size()).unwrap()
        } else {
            buf
        };

        #[cfg(not(feature = "zstd"))]
        let bytes = buf;

        let snapshot = deserialize::<Snapshot<R>>(&*bytes).unwrap();

        let mut mu = self.last_snapshot.lock().unwrap();
        *mu = Some(snapshot);
    }

    fn load_snapshot(&mut self) {
        let mu = self.last_snapshot.lock().unwrap();
        if let Some(ref snapshot) = *mu {
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

            self.log.with_sa(
                |sa| sa.initialize_from_segments(snapshot.segments.clone()),
            );
        } else {
            panic!("no snapshot present in load_snapshot");
        }
    }
}

fn lids_from_stack<P: Send + Sync>(
    stack_ptr: CasKey<P>,
    scope: &Scope,
) -> Vec<LogID> {
    // generate a list of the old log ID's
    let stack_iter = StackIter::from_ptr(stack_ptr.into(), scope);

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

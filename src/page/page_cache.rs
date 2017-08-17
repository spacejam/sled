use std::io::{Read, Seek, Write};

use crossbeam::sync::AtomicOption;
use rayon::prelude::*;
use zstd::block::{compress, decompress};
use coco::epoch::{Ptr, Scope, pin};

use super::*;

/// A lock-free pagecache.
pub struct PageCache<PM, L, P, R> {
    t: PM,
    inner: Radix<Stack<CacheEntry<P>>>,
    max_pid: AtomicUsize,
    free: Stack<PageID>,
    log: L,
    lru: lru::Lru,
    updates: AtomicUsize,
    last_snapshot: AtomicOption<Snapshot<R>>,
}

unsafe impl<PM, L, P, R> Send for PageCache<PM, L, P, R>
    where PM: Send,
          L: Send,
          R: Send
{
}

unsafe impl<PM, L, P, R> Sync for PageCache<PM, L, P, R>
    where PM: Sync,
          L: Sync
{
}

impl<PM, L, P, R> Debug for PageCache<PM, L, P, R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str(&*format!(
            "PageCache {{ max: {:?} free: {:?} }}\n",
            self.max_pid.load(SeqCst),
            self.free
        ))
    }
}

impl<PM, P, R> PageCache<PM, LockFreeLog, P, R>
    where PM: Materializer<PartialPage = P, Recovery = R>,
          PM: Send + Sync,
          P: Debug + PartialEq + Clone + Serialize + DeserializeOwned + Send,
          R: Debug + PartialEq + Clone + Serialize + DeserializeOwned,
          PM::MaterializedPage: Debug
{
    /// Instantiate a new `PageCache`.
    pub fn new(pm: PM, config: Config) -> PageCache<PM, LockFreeLog, P, R> {
        let cache_capacity = config.get_cache_capacity();
        let cache_shard_bits = config.get_cache_bits();
        let lru = lru::Lru::new(cache_capacity, cache_shard_bits);

        let snapshot = Snapshot::default();
        let last_snapshot = AtomicOption::new();
        last_snapshot.swap(snapshot, SeqCst);

        PageCache {
            t: pm,
            inner: Radix::default(),
            max_pid: AtomicUsize::new(0),
            free: Stack::default(),
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

        debug!(
            "recovery complete, returning recovery state to PageCache owner: {:?}",
            recovery
        );
        recovery
    }

    /// Create a new page, trying to reuse old freed pages if possible
    /// to maximize underlying `Radix` pointer density.
    pub fn allocate<'s>(&self) -> (PageID, CasKey<P>) {
        let pid = self.free.pop().unwrap_or_else(
            || self.max_pid.fetch_add(1, SeqCst),
        );
        trace!("allocating pid {}", pid);
        self.inner.insert(pid, Stack::default()).unwrap();

        // write info to log
        let prepend: LoggedUpdate<P> = LoggedUpdate {
            pid: pid,
            update: Update::Alloc,
        };
        let bytes = serialize(&prepend, Infinite).unwrap();
        self.log.write(bytes);

        (pid, Ptr::null().into())
    }

    /// Free a particular page.
    pub fn free(&self, pid: PageID) {
        // TODO epoch-based gc for reusing pid & freeing stack
        // TODO iter through flushed pages, punch hole
        self.inner.del(pid);

        // write info to log
        let prepend: LoggedUpdate<P> = LoggedUpdate {
            pid: pid,
            update: Update::Del,
        };
        let bytes = serialize(&prepend, Infinite).unwrap();
        self.log.write(bytes);

        // add pid to free stack to reduce fragmentation over time
        self.free.push(pid);
    }

    /// Try to retrieve a page by its logical ID.
    pub fn get<'s>(&self, pid: PageID) -> Option<(PM::MaterializedPage, CasKey<P>)> {
        pin(|scope| {
            let stack_ptr = self.inner.get(pid, scope);
            if stack_ptr.is_none() {
                return None;
            }

            let stack_ptr = stack_ptr.unwrap();

            let head = unsafe { stack_ptr.deref().head(scope) };

            Some(self.page_in(pid, head, stack_ptr, scope))
        })
    }

    #[doc(hidden)]
    pub fn __delete_all_files(self) {
        let prefix = self.snapshot_prefix();
        let prefix_glob = format!("{}.[0-9]*", prefix);
        for entry in glob::glob(&*prefix_glob).unwrap() {
            if let Ok(entry) = entry {
                info!("removing old snapshot file {:?}", entry);
                std::fs::remove_file(entry).unwrap();
            }
        }

        if let Some(path) = self.config().get_path() {
            info!("removing data file {:?}", path);
            std::fs::remove_file(&path).unwrap();
        }
    }

    fn pull(&self, lid: LogID) -> Result<Vec<P>, ()> {
        let bytes = match self.log.read(lid).map_err(|_| ())? {
            LogRead::Flush(data, _len) => data,
            _ => return Err(()),
        };
        let logged_update = deserialize::<LoggedUpdate<P>>(&*bytes).map_err(|_| ())?;

        match logged_update.update {
            Update::Compact(pps) |
            Update::Append(pps) => Ok(pps),
            _ => panic!("non-append/compact found in pull"),
        }
    }

    fn page_out<'s>(&self, to_evict: Vec<PageID>, scope: &'s Scope) {
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
                CacheEntry::Resident(_, ref lid) => CacheEntry::Flush(*lid),
                CacheEntry::PartialFlush(_) => panic!("got PartialFlush at end of stack..."),
                CacheEntry::Flush(lid) => CacheEntry::Flush(lid),
            });

            if last.is_none() {
                return;
            }

            let mut new_stack = vec![];
            for entry in cache_entries {
                match entry {
                    CacheEntry::Resident(_, ref lid) => {
                        new_stack.push(CacheEntry::PartialFlush(*lid));
                    }
                    CacheEntry::Flush(_) => panic!("got Flush in middle of stack..."),
                    CacheEntry::PartialFlush(lid) => {
                        new_stack.push(CacheEntry::PartialFlush(lid));
                    }
                }
            }
            new_stack.push(last.unwrap());
            let node = node_from_frag_vec(new_stack);
            let res = unsafe { stack_ptr.deref().cas(head, node.into_ptr(scope), scope) };
            if res.is_ok() {
                self.lru.page_out_succeeded(pid);
            } else {
                // if this failed, it's because something wrote to the page in the mean time
            }
        }
    }

    fn page_in<'s>(
        &self,
        pid: PageID,
        mut head: Ptr<'s, stack::Node<CacheEntry<P>>>,
        stack_ptr: Ptr<'s, stack::Stack<CacheEntry<P>>>,
        scope: &'s Scope,
    ) -> (PM::MaterializedPage, CasKey<P>) {
        let stack_iter = StackIter::from_ptr(head, scope);
        let mut cache_entries: Vec<CacheEntry<P>> = stack_iter.map(|ptr| (*ptr).clone()).collect();

        // read items off of disk in parallel if anything isn't already resident
        let contains_non_resident = cache_entries.iter().any(|cache_entry| match *cache_entry {
            CacheEntry::Resident(_, _) => false,
            _ => true,
        });
        if contains_non_resident {
            cache_entries.par_iter_mut().for_each(
                |ref mut cache_entry| {
                    let (pp, lid) = match **cache_entry {
                        CacheEntry::Resident(ref pp, ref lid) => (pp.clone(), *lid),
                        CacheEntry::Flush(lid) |
                        CacheEntry::PartialFlush(lid) => (self.pull(lid).unwrap(), lid),
                    };
                    **cache_entry = CacheEntry::Resident(pp, lid);
                },
            );

            let node = node_from_frag_vec(cache_entries.clone());
            let res = unsafe { stack_ptr.deref().cas(head, node.into_ptr(scope), scope) };
            if let Ok(new_head) = res {
                head = new_head;
            }
        }

        let mut partial_pages: Vec<P> = cache_entries
            .into_iter()
            .flat_map(|cache_entry| match cache_entry {
                CacheEntry::Resident(pp, _lid) => pp,
                _ => panic!("non-resident entry found, after trying to page-in all entries"),
            })
            .collect();

        partial_pages.reverse();
        let partial_pages = partial_pages;

        let to_evict = self.lru.accessed(
            pid,
            std::mem::size_of_val(&partial_pages),
        );
        self.page_out(to_evict, scope);

        if partial_pages.len() > self.config().get_page_consolidation_threshold() {
            let consolidated = self.t.consolidate(&partial_pages);
            if let Ok(new_head) = self.replace(pid, head.into(), consolidated) {
                head = new_head.into();
            }
        }

        let materialized = self.t.materialize(&partial_pages);

        (materialized, head.into())
    }

    /// Replace an existing page with a different set of `PartialPage`s.
    pub fn replace<'s>(
        &self,
        pid: PageID,
        old: CasKey<P>,
        new: Vec<P>,
    ) -> Result<CasKey<P>, CasKey<P>> {
        let replace: LoggedUpdate<P> = LoggedUpdate {
            pid: pid,
            update: Update::Compact(new.clone()),
        };
        let bytes = serialize(&replace, Infinite).unwrap();
        let log_reservation = self.log.reserve(bytes);
        let log_offset = log_reservation.log_id();

        let cache_entry = CacheEntry::Resident(new, log_offset);

        pin(|scope| {
            let node = node_from_frag_vec(vec![cache_entry]).into_ptr(scope);

            let stack_ptr = self.inner.get(pid, scope).unwrap();

            let result = unsafe { stack_ptr.deref().cas(old.clone().into(), node, scope) };

            if result.is_ok() {
                unsafe { scope.defer_drop(old.into()) };
                log_reservation.complete();
            } else {
                log_reservation.abort();
            }

            result.map(|ok| ok.into()).map_err(|e| e.into())
        })
    }


    /// Try to atomically prepend a `Materializer::PartialPage` to the page.
    pub fn prepend<'s>(&self, pid: PageID, old: CasKey<P>, new: P) -> Result<CasKey<P>, CasKey<P>> {
        let prepend: LoggedUpdate<P> = LoggedUpdate {
            pid: pid,
            update: Update::Append(vec![new.clone()]),
        };
        let bytes = serialize(&prepend, Infinite).unwrap();
        let log_reservation = self.log.reserve(bytes);
        let log_offset = log_reservation.log_id();

        let cache_entry = CacheEntry::Resident(vec![new], log_offset);

        pin(|scope| {
            let stack_ptr = self.inner.get(pid, scope).unwrap();
            let result = unsafe { stack_ptr.deref().cap(old.into(), cache_entry, scope) };

            if let Err(ptr) = result {
                // TODO GC
                unsafe { scope.defer_drop(ptr) };
                log_reservation.abort();
            } else {
                log_reservation.complete();

                let count = self.updates.fetch_add(1, SeqCst) + 1;
                let should_snapshot = self.config().get_path().is_some() &&
                    count % self.config().get_snapshot_after_ops() == 0;
                if should_snapshot {
                    self.write_snapshot();
                }
            }

            result.map(|ok| ok.into()).map_err(|e| e.into())
        })
    }

    fn write_snapshot(&self) {
        self.log.flush();

        let snapshot_opt = self.last_snapshot.take(SeqCst);
        if snapshot_opt.is_none() {
            // some other thread is snapshotting
            warn!(
                "snapshot skipped because previous attempt \
                  appears not to have completed"
            );
            return;
        }

        let mut snapshot = snapshot_opt.unwrap();

        let current_stable = self.log.stable_offset();

        info!(
            "snapshot starting from offset {} to {}",
            snapshot.max_lid,
            current_stable
        );

        let mut recovery = snapshot.recovery.take();

        for (log_id, bytes) in self.log.iter_from(snapshot.max_lid) {
            if log_id >= current_stable {
                // don't need to go farther
                break;
            }
            if let Ok(prepend) = deserialize::<LoggedUpdate<P>>(&*bytes) {
                if prepend.pid >= snapshot.max_pid {
                    snapshot.max_pid = prepend.pid + 1;
                }

                match prepend.update {
                    Update::Append(partial_pages) => {
                        for partial_page in partial_pages {
                            let r = self.t.recover(&partial_page);
                            if r.is_some() {
                                recovery = r;
                            }
                        }

                        let mut lids = snapshot.pt.get_mut(&prepend.pid).unwrap();
                        lids.push(log_id);
                    }
                    Update::Compact(partial_pages) => {
                        for partial_page in partial_pages {
                            let r = self.t.recover(&partial_page);
                            if r.is_some() {
                                recovery = r;
                            }
                        }

                        snapshot.pt.insert(prepend.pid, vec![log_id]);
                    }
                    Update::Del => {
                        snapshot.pt.remove(&prepend.pid);
                        snapshot.free.push(prepend.pid);
                    }
                    Update::Alloc => {
                        snapshot.free.retain(|&pid| pid != prepend.pid);
                    }
                }
            }
        }
        snapshot.free.sort();
        snapshot.free.reverse();
        snapshot.max_lid = current_stable;
        snapshot.recovery = recovery;

        let raw_bytes = serialize(&snapshot, Infinite).unwrap();
        let bytes = if self.config().get_use_compression() {
            compress(&*raw_bytes, 5).unwrap()
        } else {
            raw_bytes
        };
        let crc64: [u8; 8] = unsafe { std::mem::transmute(crc64::crc64(&*bytes)) };

        self.last_snapshot.swap(snapshot, SeqCst);

        let prefix = self.snapshot_prefix();
        let path_1 = format!("{}.{}.in___motion", prefix, current_stable);
        let path_2 = format!("{}.{}", prefix, current_stable);
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

        if let Err(e) = std::fs::rename(path_1, &path_2) {
            error!("failed to write snapshot: {}", e);
        }

        // clean up any old snapshots
        let prefix_glob = format!("{}.[0-9]*", prefix);
        for entry in glob::glob(&*prefix_glob).unwrap() {
            if let Ok(entry) = entry {
                if entry.to_str().unwrap() != &*path_2 {
                    info!("removing old snapshot file {:?}", entry);
                    std::fs::remove_file(entry).unwrap();
                }
            }
        }
    }

    fn snapshot_prefix(&self) -> String {
        let config = self.config();
        let snapshot_path = config.get_snapshot_path();
        let path = config.get_path();
        snapshot_path.or(path).unwrap_or_else(|| "rsdb".to_owned())
    }

    fn read_snapshot(&mut self) -> Snapshot<R> {
        let prefix = self.snapshot_prefix();
        let prefix_glob = format!("{}.[0-9]*", prefix);
        let mut candidates = vec![];
        for entry_res in glob::glob(&*prefix_glob).unwrap() {
            if let Ok(entry_pb) = entry_res {
                candidates.push(entry_pb);
            }
        }
        if candidates.is_empty() {
            info!("no previous snapshot found");
            return Snapshot::default();
        }

        candidates.sort_by_key(|path| path.metadata().unwrap().created().unwrap());

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
        let crc_actual = crc64::crc64(&*buf);

        if crc_expected != crc_actual {
            panic!("crc for snapshot file {:?} failed!", path);
        }

        let bytes = if self.config().get_use_compression() {
            decompress(&*buf, 1_000_000).unwrap()
        } else {
            buf
        };

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
            info!("adding {} to free during load_snapshot", pid);
            self.free.push(pid);
        }

        for (pid, lids) in &snapshot.pt {
            info!("loading pid {} in load_snapshot", pid);
            let mut lids = lids.clone();
            let stack = Stack::default();

            let base = lids.remove(0);
            stack.push(CacheEntry::Flush(base));

            for lid in lids {
                stack.push(CacheEntry::PartialFlush(lid));
            }

            self.inner.insert(*pid, stack).unwrap();
        }
    }
}

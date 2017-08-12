use std::io::Write;

use crossbeam::sync::AtomicOption;
use rayon::prelude::*;
use zstd::block::compress;

use super::*;

/// A lock-free pagecache.
pub struct PageCache<L: Log, M>
    where M: Materializer + Sized,
          L: Log + Sized
{
    t: M,
    inner: Radix<Stack<CacheEntry<M>>>,
    max_id: AtomicUsize,
    free: Stack<PageID>,
    log: L,
    lru: lru::Lru,
    updates: AtomicUsize,
    last_snapshot: AtomicOption<Snapshot>,
}

unsafe impl<L: Log, M: Materializer> Send for PageCache<L, M> {}
unsafe impl<L: Log, M: Materializer> Sync for PageCache<L, M> {}

impl<L: Log, M: Materializer> Debug for PageCache<L, M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str(&*format!(
            "PageCache {{ max: {:?} free: {:?} }}\n",
            self.max_id.load(SeqCst),
            self.free
        ))
    }
}

impl<M> PageCache<LockFreeLog, M>
    where M: Materializer,
          M::PartialPage: Clone,
          M: DeserializeOwned,
          M: Serialize
{
    /// Instantiate a new `PageCache`.
    pub fn new(pm: M, config: Config) -> PageCache<LockFreeLog, M> {
        let cache_capacity = config.get_cache_capacity();
        let cache_shard_bits = config.get_cache_bits();
        PageCache {
            t: pm,
            inner: Radix::default(),
            max_id: AtomicUsize::new(0),
            free: Stack::default(),
            log: LockFreeLog::start_system(config),
            lru: lru::Lru::new(cache_capacity, cache_shard_bits),
            updates: AtomicUsize::new(0),
            last_snapshot: AtomicOption::new(),
        }
    }

    /// Return the configuration used by the underlying system.
    pub fn config(&self) -> &Config {
        self.log.config()
    }

    /// Read updates from the log, apply them to our pagecache.
    pub fn recover(&mut self) -> Option<M::Recovery> {
        let mut last_good_id = self.read_snapshot();

        let mut free_pids = vec![];
        let mut recovery = None;

        for (log_id, bytes) in self.log.iter_from(last_good_id) {
            if log_id == last_good_id && log_id != 0 {
                // we are reading from a real snapshot, which included
                // this ID already, so skip the first entry
                continue;
            }

            if let Ok(prepend) = deserialize::<LoggedUpdate<M>>(&*bytes) {
                // keep track of the highest valid LogID
                last_good_id = log_id + (bytes.len() + HEADER_LEN) as LogID;

                match prepend.update {
                    Update::Append(ref prepends) => {
                        for prepend in prepends {
                            let r = self.t.recover(prepend);
                            if r.is_some() {
                                recovery = r;
                            }
                        }

                        let stack = self.inner.get(prepend.pid).unwrap();

                        let flush = CacheEntry::PartialFlush(log_id);

                        unsafe {
                            (*stack).push(flush);
                        }
                    }
                    Update::Compact(ref prepends) => {
                        for prepend in prepends {
                            let r = self.t.recover(prepend);
                            if r.is_some() {
                                recovery = r;
                            }
                        }

                        // TODO GC previous stack
                        // TODO feed compacted page to recover?
                        self.inner.del(prepend.pid);

                        let stack = raw(Stack::default());
                        self.inner.insert(prepend.pid, stack).unwrap();

                        let flush = CacheEntry::Flush(log_id);

                        unsafe { (*stack).push(flush) }
                    }
                    Update::Del => {
                        self.inner.del(prepend.pid);
                        free_pids.push(prepend.pid);
                    }
                    Update::Alloc => {
                        let stack = raw(Stack::default());
                        self.inner.insert(prepend.pid, stack).unwrap();
                        free_pids.retain(|&pid| pid != prepend.pid);
                        if self.max_id.load(SeqCst) < prepend.pid {
                            self.max_id.store(prepend.pid, SeqCst);
                        }
                    }
                }
            }
        }
        free_pids.sort();
        free_pids.reverse();
        for free_pid in free_pids {
            self.free.push(free_pid);
        }

        self.max_id.store(last_good_id as usize, SeqCst);

        recovery
    }

    /// Create a new page, trying to reuse old freed pages if possible
    /// to maximize underlying `Radix` pointer density.
    pub fn allocate(&self) -> (PageID, *const stack::Node<CacheEntry<M>>) {
        let pid = self.free.pop().unwrap_or_else(
            || self.max_id.fetch_add(1, SeqCst),
        );
        let stack = raw(Stack::default());
        self.inner.insert(pid, stack).unwrap();

        // write info to log
        let prepend: LoggedUpdate<M> = LoggedUpdate {
            pid: pid,
            update: Update::Alloc,
        };
        let bytes = serialize(&prepend, Infinite).unwrap();
        self.log.write(bytes);

        (pid, ptr::null())
    }

    /// Free a particular page.
    pub fn free(&self, pid: PageID) {
        // TODO epoch-based gc for reusing pid & freeing stack
        // TODO iter through flushed pages, punch hole
        let stack_ptr = self.inner.del(pid);

        // write info to log
        let prepend: LoggedUpdate<M> = LoggedUpdate {
            pid: pid,
            update: Update::Del,
        };
        let bytes = serialize(&prepend, Infinite).unwrap();
        self.log.write(bytes);

        // add pid to free stack to reduce fragmentation over time
        self.free.push(pid);

        unsafe {
            (*stack_ptr).pop_all();
        }
    }

    /// Try to retrieve a page by its logical ID.
    pub fn get(
        &self,
        pid: PageID,
    ) -> Option<(M::MaterializedPage, *const stack::Node<CacheEntry<M>>)> {
        let stack_ptr = self.inner.get(pid);
        if stack_ptr.is_none() {
            return None;
        }

        let stack_ptr = stack_ptr.unwrap();

        let head = unsafe { (*stack_ptr).head() };

        Some(self.page_in(pid, head, stack_ptr))
    }

    fn pull(&self, lid: LogID) -> Result<Vec<M::PartialPage>, ()> {
        let bytes = match self.log.read(lid).map_err(|_| ())? {
            LogRead::Flush(data, _len) => data,
            _ => return Err(()),
        };
        let logged_update = deserialize::<LoggedUpdate<M>>(&*bytes).map_err(|_| ())?;

        match logged_update.update {
            Update::Compact(pps) |
            Update::Append(pps) => Ok(pps),
            _ => panic!("non-append/compact found in pull"),
        }
    }

    fn page_out(&self, to_evict: Vec<PageID>) {
        for pid in to_evict {
            let stack_ptr = self.inner.get(pid);
            if stack_ptr.is_none() {
                continue;
            }

            let stack_ptr = stack_ptr.unwrap();
            let (head, stack_iter) = unsafe { (*stack_ptr).iter_at_head() };
            let mut cache_entries: Vec<CacheEntry<M>> =
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
            let res = unsafe { (*stack_ptr).cas(head, node) };
            if res.is_ok() {
                self.lru.page_out_succeeded(pid);
            } else {
                // if this failed, it's because something wrote to the page in the mean time
            }
        }
    }

    fn page_in(
        &self,
        pid: PageID,
        mut head: *const stack::Node<CacheEntry<M>>,
        stack_ptr: *const stack::Stack<CacheEntry<M>>,
    ) -> (M::MaterializedPage, *const stack::Node<CacheEntry<M>>) {
        let stack_iter = StackIter::from_ptr(head);
        let mut cache_entries: Vec<CacheEntry<M>> = stack_iter.map(|ptr| (*ptr).clone()).collect();

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
            let res = unsafe { (*stack_ptr).cas(head, node) };
            if let Ok(new_head) = res {
                head = new_head;
            }
        }

        let mut partial_pages: Vec<M::PartialPage> = cache_entries
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
        self.page_out(to_evict);

        if partial_pages.len() > self.config().get_page_consolidation_threshold() {
            let consolidated = self.t.consolidate(&partial_pages);
            if let Ok(new_head) = self.replace(pid, head, consolidated) {
                head = new_head;
            }
        }

        let materialized = self.t.materialize(&partial_pages);
        (materialized, head)
    }

    /// Replace an existing page with a different set of `PartialPage`s.
    pub fn replace(
        &self,
        pid: PageID,
        old: *const stack::Node<CacheEntry<M>>,
        new: Vec<M::PartialPage>,
    ) -> Result<*const stack::Node<CacheEntry<M>>, *const stack::Node<CacheEntry<M>>> {
        let replace: LoggedUpdate<M> = LoggedUpdate {
            pid: pid,
            update: Update::Compact(new.clone()),
        };
        let bytes = serialize(&replace, Infinite).unwrap();
        let log_reservation = self.log.reserve(bytes);
        let log_offset = log_reservation.log_id();

        let cache_entry = CacheEntry::Resident(new, log_offset);
        let node = node_from_frag_vec(vec![cache_entry]);

        let stack_ptr = self.inner.get(pid).unwrap();
        let result = unsafe { (*stack_ptr).cas(old, node) };

        if let Err(_ptr) = result {
            // TODO GC
            log_reservation.abort();
        } else {
            log_reservation.complete();
        }
        result
    }


    /// Try to atomically prepend a `Materializer::PartialPage` to the page.
    pub fn prepend(
        &self,
        pid: PageID,
        old: *const stack::Node<CacheEntry<M>>,
        new: M::PartialPage,
    ) -> Result<*const stack::Node<CacheEntry<M>>, *const stack::Node<CacheEntry<M>>> {
        let prepend: LoggedUpdate<M> = LoggedUpdate {
            pid: pid,
            update: Update::Append(vec![new.clone()]),
        };
        let bytes = serialize(&prepend, Infinite).unwrap();
        let log_reservation = self.log.reserve(bytes);
        let log_offset = log_reservation.log_id();

        let cache_entry = CacheEntry::Resident(vec![new], log_offset);

        let stack_ptr = self.inner.get(pid).unwrap();
        let result = unsafe { (*stack_ptr).cap(old, cache_entry) };

        if let Err(_ptr) = result {
            // TODO GC
            log_reservation.abort();
        } else {
            log_reservation.complete();
            self.maybe_snapshot();
        }
        result
    }

    fn maybe_snapshot(&self) {
        let count = self.updates.fetch_add(1, SeqCst) + 1;
        let should_snapshot = count % self.config().get_snapshot_after_ops() == 0;
        if !should_snapshot {
            return;
        }
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
            snapshot.log_tip,
            current_stable
        );

        for (log_id, bytes) in self.log.iter_from(snapshot.log_tip) {
            if log_id >= current_stable {
                // don't need to go farther
                break;
            }
            if let Ok(prepend) = deserialize::<LoggedUpdate<M>>(&*bytes) {
                match prepend.update {
                    Update::Append(_) => {
                        let mut lids = snapshot.pt.get_mut(&prepend.pid).unwrap();
                        lids.push(log_id);
                    }
                    Update::Compact(_) => {
                        snapshot.pt.insert(prepend.pid, vec![log_id]);
                    }
                    Update::Del => {
                        snapshot.pt.remove(&prepend.pid);
                        snapshot.free.push(prepend.pid);
                    }
                    Update::Alloc => {
                        snapshot.free.retain(|&pid| pid != prepend.pid);
                        if prepend.pid > snapshot.max_id {
                            snapshot.max_id = prepend.pid;
                        }
                    }
                }
            }
        }
        snapshot.free.sort();
        snapshot.free.reverse();
        snapshot.log_tip = current_stable;

        let raw_bytes = serialize(&snapshot, Infinite).unwrap();
        let bytes = if self.config().get_use_compression() {
            compress(&*raw_bytes, 5).unwrap()
        } else {
            raw_bytes
        };
        let crc64: [u8; 8] = unsafe { std::mem::transmute(crc64::crc64(&*bytes)) };

        self.last_snapshot.swap(snapshot, SeqCst);

        let prefix = self.snapshot_prefix();
        let path_1 = format!("{}_{}.snapshot.in_motion", prefix, current_stable);
        let path_2 = format!("{}_{}.snapshot", prefix, current_stable);
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
        let prefix_glob = format!("{}*", prefix);
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
        self.config().get_snapshot_path_prefix().unwrap_or(
            "rsdb".to_owned(),
        )
    }

    fn read_snapshot(&mut self) -> LogID {
        let prefix = self.snapshot_prefix();
        let _prefix_glob = format!("{}*", prefix);

        let snapshot = Snapshot::default();
        let ret = snapshot.log_tip;

        self.last_snapshot.swap(snapshot, SeqCst);

        ret
    }
}

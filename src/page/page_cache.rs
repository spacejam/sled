use super::*;

use rayon::prelude::*;

/// A lock-free pagecache.
pub struct PageCache<L: Log, M>
    where M: Materializer + Sized,
          L: Log + Sized
{
    t: M,
    inner: Radix<Stack<CacheEntry<M>>>,
    max_id: AtomicUsize,
    free: Stack<PageID>,
    log: Box<L>,
    lru: lru::Lru,
}

unsafe impl<L: Log, M: Materializer> Send for PageCache<L, M> {}
unsafe impl<L: Log, M: Materializer> Sync for PageCache<L, M> {}

impl<L: Log, M: Materializer> Debug for PageCache<L, M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str(&*format!("PageCache {{ max: {:?} free: {:?} }}\n",
                              self.max_id.load(SeqCst),
                              self.free))
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
            log: Box::new(LockFreeLog::start_system(config)),
            lru: lru::Lru::new(cache_capacity, cache_shard_bits),
        }
    }

    /// Return the configuration used by the underlying system.
    pub fn config(&self) -> &Config {
        self.log.config()
    }

    /// Read updates from the log, apply them to our pagecache.
    pub fn recover(&mut self, from: LogID) -> Option<M::Recovery> {

        let mut last_good_id = 0;
        let mut free_pids = vec![];
        let mut recovery = None;

        for (log_id, bytes) in self.log.iter_from(from) {
            if let Ok(append) = deserialize::<LoggedUpdate<M>>(&*bytes) {
                // keep track of the highest valid LogID
                last_good_id = log_id + (bytes.len() + HEADER_LEN) as LogID;


                match append.update {
                    Update::Append(ref appends) => {
                        for ref append in appends {
                            let r = self.t.recover(&append);
                            if r.is_some() {
                                recovery = r;
                            }
                        }

                        let stack = self.inner.get(append.pid).unwrap();

                        let flush = CacheEntry::PartialFlush(log_id);

                        unsafe {
                            (*stack).push(flush);
                        }
                    }
                    Update::Compact(ref appends) => {
                        for ref append in appends {
                            let r = self.t.recover(&append);
                            if r.is_some() {
                                recovery = r;
                            }
                        }

                        // TODO GC previous stack
                        // TODO feed compacted page to recover?
                        self.inner.del(append.pid);

                        let stack = raw(Stack::default());
                        self.inner.insert(append.pid, stack).unwrap();

                        let flush = CacheEntry::Flush(log_id);

                        unsafe { (*stack).push(flush) }
                    }
                    Update::Del => {
                        self.inner.del(append.pid);
                        free_pids.push(append.pid);
                    }
                    Update::Alloc => {
                        let stack = raw(Stack::default());
                        self.inner.insert(append.pid, stack).unwrap();
                        free_pids.retain(|&pid| pid != append.pid);
                        if self.max_id.load(SeqCst) < append.pid {
                            self.max_id.store(append.pid, SeqCst);
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
        let pid = self.free.pop().unwrap_or_else(|| self.max_id.fetch_add(1, SeqCst));
        let stack = raw(Stack::default());
        self.inner.insert(pid, stack).unwrap();

        // write info to log
        let append: LoggedUpdate<M> = LoggedUpdate {
            pid: pid,
            update: Update::Alloc,
        };
        let bytes = serialize(&append, Infinite).unwrap();
        self.log.write(bytes);

        (pid, ptr::null())
    }

    /// Free a particular page.
    pub fn free(&self, pid: PageID) {
        // TODO epoch-based gc for reusing pid & freeing stack
        // TODO iter through flushed pages, punch hole
        let stack_ptr = self.inner.del(pid);

        // write info to log
        let append: LoggedUpdate<M> = LoggedUpdate {
            pid: pid,
            update: Update::Del,
        };
        let bytes = serialize(&append, Infinite).unwrap();
        self.log.write(bytes);

        // add pid to free stack to reduce fragmentation over time
        self.free.push(pid);

        unsafe {
            (*stack_ptr).pop_all();
        }
    }

    /// Try to retrieve a page by its logical ID.
    pub fn get(&self,
               pid: PageID)
               -> Option<(M::MaterializedPage, *const stack::Node<CacheEntry<M>>)> {
        let stack_ptr = self.inner.get(pid);
        if stack_ptr.is_none() {
            return None;
        }

        let stack_ptr = stack_ptr.unwrap();

        let head = unsafe { (*stack_ptr).head() };

        Some(self.page_in(pid, head))
    }

    fn pull(&self, lid: LogID) -> Result<Vec<M::PartialPage>, ()> {
        let bytes = self.log.read(lid).map_err(|_| ())?.map_err(|_| ())?;
        let sz = bytes.len();
        let logged_update = deserialize::<LoggedUpdate<M>>(&*bytes).map_err(|_| ())?;

        let to_evict = self.lru.paged_in(logged_update.pid, lid, sz);
        self.page_out(to_evict);

        match logged_update.update {
            Update::Append(pps) => Ok(pps),
            _ => panic!("non-apped found in pull"),
        }
    }

    fn page_out(&self, to_evict: Vec<(PageID, LogID)>) {
        // TODO flip each item to a (Partial)Flush
    }

    fn page_in(&self,
               pid: PageID,
               head: *const stack::Node<CacheEntry<M>>)
               -> (M::MaterializedPage, *const stack::Node<CacheEntry<M>>) {
        let stack_iter = StackIter::from_ptr(head);
        let mut cache_entries: Vec<CacheEntry<M>> = stack_iter.map(|ptr| (*ptr).clone()).collect();

        // "fill-in" any flushes
        cache_entries.par_iter_mut()
            .for_each(|ref mut ce| {
                let (pp, lid) = match *ce {
                    &mut CacheEntry::Resident(ref pp, ref lid) => {
                        if let &Some(l) = lid {
                            self.lru.accessed(l);
                        }
                        (pp.clone(), lid.clone())
                    }
                    &mut CacheEntry::Flush(lid) => (self.pull(lid).unwrap(), Some(lid)),
                    &mut CacheEntry::PartialFlush(lid) => (self.pull(lid).unwrap(), Some(lid)),
                };
                **ce = CacheEntry::Resident(pp, lid);
            });

        let mut partial_pages: Vec<M::PartialPage> = cache_entries.iter()
            .flat_map(|ce| {
                match *ce {
                    CacheEntry::Resident(ref pp, _lid) => pp.clone(),
                    _ => panic!("non-resident entry found, after trying to page-in all entries"),
                }
            })
            .collect();

        partial_pages.reverse();
        let partial_pages = partial_pages;

        let mut head = head;
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
    pub fn replace
        (&self,
         pid: PageID,
         old: *const stack::Node<CacheEntry<M>>,
         new: Vec<M::PartialPage>)
         -> Result<*const stack::Node<CacheEntry<M>>, *const stack::Node<CacheEntry<M>>> {
        let replace: LoggedUpdate<M> = LoggedUpdate {
            pid: pid,
            update: Update::Compact(new.clone()),
        };
        let bytes = serialize(&replace, Infinite).unwrap();
        let log_reservation = self.log.reserve(bytes);
        let log_offset = log_reservation.log_id();

        let cache_entry = CacheEntry::Resident(new, Some(log_offset));
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


    /// Try to atomically append a `Materializer::PartialPage` to the page.
    pub fn append
        (&self,
         pid: PageID,
         old: *const stack::Node<CacheEntry<M>>,
         new: M::PartialPage)
         -> Result<*const stack::Node<CacheEntry<M>>, *const stack::Node<CacheEntry<M>>> {
        let append: LoggedUpdate<M> = LoggedUpdate {
            pid: pid,
            update: Update::Append(vec![new.clone()]),
        };
        let bytes = serialize(&append, Infinite).unwrap();
        let log_reservation = self.log.reserve(bytes);
        let log_offset = log_reservation.log_id();

        let cache_entry = CacheEntry::Resident(vec![new], Some(log_offset));

        let stack_ptr = self.inner.get(pid).unwrap();
        let result = unsafe { (*stack_ptr).cap(old, cache_entry) };

        if let Err(_ptr) = result {
            // TODO GC
            log_reservation.abort();
        } else {
            log_reservation.complete();
        }
        result
    }
}

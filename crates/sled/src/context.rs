use std::sync::{
    atomic::{
        AtomicUsize,
        Ordering::{Acquire, Relaxed, Release},
    },
    Arc, Mutex, RwLock,
};

use pagecache::{FastMap8, PagePtr};

use super::*;

pub(crate) struct Context {
    // TODO file from config should be in here
    config: Config,
    pagecache: PageCache<BLinkMaterializer, Frag, Recovery>,
    idgen: Arc<AtomicUsize>,
    idgen_persists: Arc<AtomicUsize>,
    idgen_persist_mu: Arc<Mutex<()>>,
    tenants: Arc<RwLock<FastMap8<Vec<u8>, Arc<Tree>>>>,
    was_recovered: bool,
}

impl std::ops::Deref for Context {
    type Target = Config;

    fn deref(&self) -> &Config {
        &self.config
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        if let Err(e) = self.pagecache.flush() {
            error!(
                "failed to flush underlying \
                 pagecache during drop: {:?}",
                e
            );
        }

        #[cfg(feature = "event_log")]
        {
            let guard = pin();

            self.config.event_log.meta_before_restart(
                meta::meta(&*self.pagecache, &guard)
                    .expect("should get meta under test")
                    .clone(),
            );
        }
    }
}

impl Context {
    pub(crate) fn start(config: Config) -> Result<Context, ()> {
        #[cfg(any(test, feature = "check_snapshot_integrity"))]
        match config
            .verify_snapshot::<BLinkMaterializer, Frag, Recovery>()
        {
            Ok(_) => {}
            #[cfg(feature = "failpoints")]
            Err(Error::FailPoint) => {}
            other => panic!("failed to verify snapshot: {:?}", other),
        }

        let pagecache = Arc::new(PageCache::start(config.clone())?);

        let recovery: Recovery =
            pagecache.recovered_state().unwrap_or_default();
        let idgen_recovery =
            recovery.counter + (2 * config.idgen_persist_interval);
        let idgen_persists = recovery.counter
            / config.idgen_persist_interval
            * config.idgen_persist_interval;

        let guard = pin();

        let was_recovered: bool;

        if pagecache
            .get(META_PID, &guard)
            .map_err(|e| e.danger_cast())?
            .is_unallocated()
        {
            // set up meta
            let meta_id = pagecache.allocate(&guard)?;

            assert_eq!(
                meta_id,
                META_PID,
                "we expect the meta page to have pid {}, but it had pid {} instead",
                META_PID,
                meta_id,
            );
        }

        if pagecache
            .get(META_PID, &guard)
            .map_err(|e| e.danger_cast())?
            .is_allocated()
        {
            was_recovered = false;

            let meta = Frag::Meta(Meta::default());
            pagecache
                .replace(META_PID, PagePtr::allocated(), meta, &guard)
                .map_err(|e| e.danger_cast())?;

            // set up idgen
            let counter_id = pagecache.allocate(&guard)?;

            assert_eq!(
                counter_id,
                COUNTER_PID,
                "we expect the counter to have pid {}, but it had pid {} instead",
                COUNTER_PID,
                counter_id,
            );

            let counter = Frag::Counter(0);
            pagecache
                .replace(
                    counter_id,
                    PagePtr::allocated(),
                    counter,
                    &guard,
                )
                .map_err(|e| e.danger_cast())?;
        } else {
            was_recovered = true;
        }

        let mut tenants = ret.tenants.write().unwrap();

        for (id, _root) in
            meta::meta(&*pagecache, &guard)?.tenants().into_iter()
        {
            let tree = Tree {
                tree_id: id.clone(),
                subscriptions: Arc::new(Subscriptions::default()),
                config: ret.config.clone(),
                pagecache: pagecache.clone(),
            };
            tenants.insert(id, Arc::new(tree));
        }

        drop(tenants);

        #[cfg(feature = "event_log")]
        ret.config.event_log.meta_after_restart(
            meta::meta(&*pagecache, &guard)
                .expect("should be able to get meta under test")
                .clone(),
        );

        Context {
            tenants: Arc::new(RwLock::new(FastMap8::default())),
            idgen: Arc::new(AtomicUsize::new(idgen_recovery)),
            idgen_persists: Arc::new(AtomicUsize::new(
                idgen_persists,
            )),
            idgen_persist_mu: Arc::new(Mutex::new(())),
        }
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
    pub fn generate_id(&self) -> Result<usize, ()> {
        let ret = self.idgen.fetch_add(1, Relaxed);

        let interval = self.config.idgen_persist_interval;
        let necessary_persists = ret / interval * interval;
        let mut persisted = self.idgen_persists.load(Acquire);

        while persisted < necessary_persists {
            let _mu = self.idgen_persist_mu.lock().unwrap();
            persisted = self.idgen_persists.load(Acquire);
            if persisted < necessary_persists {
                // it's our responsibility to persist up to our ID
                let guard = pin();
                let (current, key) = self
                    .pagecache
                    .get(COUNTER_PID, &guard)
                    .map_err(|e| e.danger_cast())?
                    .unwrap();

                if let Frag::Counter(current) = current {
                    assert_eq!(*current, persisted);
                } else {
                    panic!(
                        "counter pid contained non-Counter: {:?}",
                        current
                    );
                }

                let counter_frag = Frag::Counter(necessary_persists);

                let old = self
                    .idgen_persists
                    .swap(necessary_persists, Release);
                assert_eq!(old, persisted);

                self.pagecache
                    .replace(
                        COUNTER_PID,
                        key.clone(),
                        counter_frag,
                        &guard,
                    )
                    .map_err(|e| e.danger_cast())?;

                // during recovery we add 2x the interval. we only
                // need to block if the last one wasn't stable yet.
                if key.last_lsn() > self.pagecache.stable_lsn() {
                    self.pagecache.make_stable(key.last_lsn())?;
                }

                guard.flush();
            }
        }

        Ok(ret)
    }

    /// Open or create a new disk-backed Tree with its own keyspace,
    /// accessible from the `Db` via the provided identifier.
    pub fn open_tree(&self, name: Vec<u8>) -> Result<Arc<Tree>, ()> {
        let guard = pin();

        let tenants = self.tenants.read().unwrap();
        if let Some(tree) = tenants.get(&name) {
            return Ok(tree.clone());
        }
        // drop reader lock
        drop(tenants);

        // set up empty leaf
        let leaf_id = self.pagecache.allocate(&guard)?;
        trace!(
            "allocated pid {} for leaf in open_tree for namespace {:?}",
            leaf_id,
            name,
        );

        let leaf = Frag::Base(Node {
            id: leaf_id,
            data: Data::Leaf(vec![]),
            next: None,
            lo: vec![].into(),
            hi: vec![].into(),
        });

        self.pagecache
            .replace(leaf_id, PagePtr::allocated(), leaf, &guard)
            .map_err(|e| e.danger_cast())?;

        // set up root index
        let root_id = self.pagecache.allocate(&guard)?;

        debug!(
            "allocated pid {} for root of new_tree {:?}",
            root_id, name
        );

        // vec![0] represents a prefix-encoded empty prefix
        let root_index_vec = vec![(vec![0].into(), leaf_id)];

        let root = Frag::Base(Node {
            id: root_id,
            data: Data::Index(root_index_vec),
            next: None,
            lo: vec![].into(),
            hi: vec![].into(),
        });

        self.pagecache
            .replace(root_id, PagePtr::allocated(), root, &guard)
            .map_err(|e| e.danger_cast())?;

        meta::cas_root(
            &*self.pagecache,
            name.clone(),
            None,
            Some(root_id),
            &guard,
        )
        .map_err(|e| e.danger_cast())?;

        let mut tenants = self.tenants.write().unwrap();
        let tree = Arc::new(Tree {
            tree_id: name.clone(),
            subscriptions: Arc::new(Subscriptions::default()),
            config: self.config.clone(),
            pagecache: self.pagecache.clone(),
        });
        tenants.insert(name, tree.clone());

        Ok(tree)
    }

    /// Remove a disk-backed collection.
    pub fn drop_tree(&self, name: &[u8]) -> Result<bool, ()> {
        if name == DEFAULT_TREE_ID || name == TX_TREE_ID {
            return Err(Error::Unsupported(
                "cannot remove the core structures".into(),
            ));
        }
        trace!("dropping tree {:?}", name,);

        let mut tenants = self.tenants.write().unwrap();

        let tree = if let Some(tree) = tenants.remove(&*name) {
            tree
        } else {
            return Ok(false);
        };

        let guard = pin();

        let mut root_id =
            meta::pid_for_name(&self.pagecache, &name, &guard)?;

        let leftmost_chain: Vec<PageId> = tree
            .path_for_key(b"", &guard)?
            .into_iter()
            .map(|(frag, _tp)| frag.unwrap_base().id)
            .collect();

        loop {
            let res = meta::cas_root(
                &self.pagecache,
                name.to_vec(),
                Some(root_id),
                None,
                &guard,
            )
            .map_err(|e| e.danger_cast());

            match res {
                Ok(_) => break,
                Err(Error::CasFailed(actual)) => root_id = actual,
                Err(other) => return Err(other.danger_cast()),
            }
        }

        // drop writer lock
        drop(tenants);

        guard.defer(move || tree.gc_pagecache(leftmost_chain));

        guard.flush();

        Ok(true)
    }
}

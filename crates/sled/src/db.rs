use std::{
    collections::HashMap,
    ops::Deref,
    sync::{
        atomic::{
            AtomicUsize,
            Ordering::{Acquire, Relaxed, Release},
        },
        Arc, Mutex, RwLock,
    },
};

use pagecache::PagePtr;

use super::*;

/// A collection of disk-backed data structures
/// sharing a common pagecache, monotonic ID generator,
/// and transactional domain.
pub struct Db {
    config: Config,
    pub(crate) pages:
        Arc<PageCache<BLinkMaterializer, Frag, Recovery>>,

    tenants: Arc<RwLock<HashMap<Vec<u8>, Arc<Tree>>>>,

    idgen: Arc<AtomicUsize>,
    idgen_persists: Arc<AtomicUsize>,
    idgen_persist_mu: Arc<Mutex<()>>,
}

#[cfg(feature = "event_log")]
impl Drop for Db {
    fn drop(&mut self) {
        let guard = pin();

        self.config.event_log.meta_before_restart(
            self.meta(&guard)
                .expect("should get meta under test")
                .clone(),
        );
    }
}

// We can assume we always have a default Tree.
impl Deref for Db {
    type Target = Tree;

    fn deref(&self) -> &Tree {
        let r = self.tenants.read().unwrap();
        let k: &[u8] = b"default";
        let tree_arc: &Arc<Tree> = &r[k];
        let tree_ref: &Tree = &*tree_arc;
        let tp = tree_ref as *const Tree;
        unsafe {
            // lol
            &*tp
        }
    }
}

impl Db {
    /// Load existing or create a new `Db` with a default configuration.
    pub fn start_default<P: AsRef<std::path::Path>>(
        path: P,
    ) -> Result<Db, ()> {
        let config = ConfigBuilder::new().path(path).build();
        Self::start(config)
    }

    /// Load existing or create a new `Db`.
    pub fn start(config: Config) -> Result<Db, ()> {
        let _measure = Measure::new(&M.tree_start);

        #[cfg(any(test, feature = "check_snapshot_integrity"))]
        match config
            .verify_snapshot::<BLinkMaterializer, Frag, Recovery>()
        {
            Ok(_) => {}
            #[cfg(feature = "failpoints")]
            Err(Error::FailPoint) => {}
            other => panic!("failed to verify snapshot: {:?}", other),
        }

        let pages = PageCache::start(config.clone())?;

        let recovery: Recovery =
            pages.recovered_state().unwrap_or_default();
        let idgen_recovery =
            recovery.counter + (2 * config.idgen_persist_interval);
        let idgen_persists = recovery.counter
            / config.idgen_persist_interval
            * config.idgen_persist_interval;

        let guard = pin();

        if pages
            .get(META_PID, &guard)
            .map_err(|e| e.danger_cast())?
            .is_unallocated()
        {
            // set up meta
            let meta_id = pages.allocate(&guard)?;

            assert_eq!(
                meta_id,
                META_PID,
                "we expect the meta page to have pid {}, but it had pid {} instead",
                META_PID,
                meta_id,
            );

            let meta = Frag::Meta(Meta::default());
            pages
                .replace(meta_id, PagePtr::allocated(), meta, &guard)
                .map_err(|e| e.danger_cast())?;

            // set up idgen
            let counter_id = pages.allocate(&guard)?;

            assert_eq!(
                counter_id,
                COUNTER_PID,
                "we expect the counter to have pid {}, but it had pid {} instead",
                COUNTER_PID,
                counter_id,
            );

            let counter = Frag::Counter(0);
            pages
                .replace(
                    counter_id,
                    PagePtr::allocated(),
                    counter,
                    &guard,
                )
                .map_err(|e| e.danger_cast())?;
        };

        let ret = Db {
            pages: Arc::new(pages),
            config,
            tenants: Arc::new(RwLock::new(HashMap::new())),
            idgen: Arc::new(AtomicUsize::new(idgen_recovery)),
            idgen_persists: Arc::new(AtomicUsize::new(
                idgen_persists,
            )),
            idgen_persist_mu: Arc::new(Mutex::new(())),
        };

        #[cfg(feature = "event_log")]
        ret.config.event_log.meta_after_restart(
            ret.meta(&guard)
                .expect("should be able to get meta under test")
                .clone(),
        );

        if ret.pid_for_name(b"default", &guard)?.is_none() {
            // set up initial tree

            ret.open_tree(b"default".to_vec(), &guard)?;
            assert_eq!(
                ret.pid_for_name(b"default", &guard)?,
                Some(ORIGINAL_ROOT_PID)
            );
        }

        let mut tenants = HashMap::new();

        for (id, _root) in ret.meta(&guard)?.tenants().into_iter() {
            let tree = Tree {
                subscriptions: Arc::new(Subscriptions::default()),
                config: ret.config.clone(),
                pages: ret.pages.clone(),
            };
            tenants.insert(id, Arc::new(tree));
        }

        let mut ret = ret;

        ret.tenants = Arc::new(RwLock::new(tenants));

        Ok(ret)
    }

    fn pid_for_name(
        &self,
        name: &[u8],
        guard: &Guard,
    ) -> Result<Option<usize>, ()> {
        let meta = self.meta(guard)?;
        Ok(meta.get_root(name))
    }

    fn meta<'a>(&self, guard: &'a Guard) -> Result<&'a Meta, ()> {
        let meta_page_get = self
            .pages
            .get(META_PID, guard)
            .map_err(|e| e.danger_cast())?;

        let meta = match meta_page_get {
            PageGet::Materialized(ref meta_ptr, ref _ptr) => {
                meta_ptr.unwrap_meta()
            }
            broken => panic!(
                "pagecache returned non-base node: {:?}",
                broken
            ),
        };

        Ok(meta)
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
                    .pages
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

                self.pages
                    .replace(
                        COUNTER_PID,
                        key.clone(),
                        counter_frag,
                        &guard,
                    )
                    .map_err(|e| e.danger_cast())?;

                // during recovery we add 2x the interval. we only
                // need to block if the last one wasn't stable yet.
                if key.last_lsn() > self.pages.stable_lsn() {
                    self.pages.make_stable(key.last_lsn())?;
                }

                guard.flush();
            }
        }

        Ok(ret)
    }

    /// Openor create a new disk-backed Tree with its own keyspace,
    /// accessible from the `Db` via the provided identifier.
    pub fn open_tree<'a>(
        &self,
        name: Vec<u8>,
        guard: &'a Guard,
    ) -> Result<Arc<Tree>, ()> {
        let tenants = self.tenants.read().unwrap();
        if let Some(tree) = tenants.get(&name) {
            return Ok(tree.clone());
        }
        // drop reader lock
        drop(tenants);

        // set up empty leaf
        let leaf_id = self.pages.allocate(&guard)?;
        trace!("allocated pid {} for leaf in new_tree for namespace {:?}", leaf_id, name);

        let leaf = Frag::Base(Node {
            id: leaf_id,
            data: Data::Leaf(vec![]),
            next: None,
            lo: vec![],
            hi: vec![],
        });

        self.pages
            .replace(leaf_id, PagePtr::allocated(), leaf, &guard)
            .map_err(|e| e.danger_cast())?;

        // set up root index
        let root_id = self.pages.allocate(&guard)?;

        debug!(
            "allocated pid {} for root of new_tree {:?}",
            root_id, name
        );

        // vec![0] represents a prefix-encoded empty prefix
        let root_index_vec = vec![(vec![0], leaf_id)];

        let root = Frag::Base(Node {
            id: root_id,
            data: Data::Index(root_index_vec),
            next: None,
            lo: vec![],
            hi: vec![],
        });

        self.pages
            .replace(root_id, PagePtr::allocated(), root, &guard)
            .map_err(|e| e.danger_cast())?;

        self.cas_root(name.clone(), None, root_id, guard)
            .map_err(|e| e.danger_cast())?;

        let mut tenants = self.tenants.write().unwrap();
        let tree = Arc::new(Tree {
            subscriptions: Arc::new(Subscriptions::default()),
            config: self.config.clone(),
            pages: self.pages.clone(),
        });
        tenants.insert(name, tree.clone());

        Ok(tree)
    }

    fn cas_root(
        &self,
        name: Vec<u8>,
        old: Option<usize>,
        new: usize,
        guard: &Guard,
    ) -> Result<(), Option<usize>> {
        let meta_page_get = self
            .pages
            .get(META_PID, guard)
            .map_err(|e| e.danger_cast())?;

        let (meta_key, meta) = match meta_page_get {
            PageGet::Materialized(ref meta_ptr, ref key) => {
                let meta = meta_ptr.unwrap_meta();
                (key, meta)
            }
            broken => panic!(
                "pagecache returned non-base node: {:?}",
                broken
            ),
        };

        let mut current: PagePtr<_> = meta_key.clone();
        loop {
            let actual = meta.get_root(&name);
            if actual != old {
                return Err(Error::CasFailed(actual));
            }

            let mut new_meta = meta.clone();
            new_meta.set_root(name.clone(), new);

            let new_meta_frag = Frag::Meta(new_meta);
            let res = self
                .pages
                .replace(META_PID, current, new_meta_frag, &guard)
                .map_err(|e| e.danger_cast());

            match res {
                Ok(_) => return Ok(()),
                Err(Error::CasFailed(actual)) => current = actual,
                Err(other) => return Err(other.danger_cast()),
            }
        }
    }
}

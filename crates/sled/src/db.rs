use std::{
    ops::Deref,
    sync::{atomic::AtomicUsize, Arc, RwLock},
};

use pagecache::FastMap8;

use super::*;

/// The `sled` embedded database!
#[derive(Clone)]
pub struct Db {
    context: Arc<Context>,
    default: Arc<Tree>,
    tenants: Arc<RwLock<FastMap8<Vec<u8>, Arc<Tree>>>>,
}

unsafe impl Send for Db {}

unsafe impl Sync for Db {}

impl Deref for Db {
    type Target = Tree;

    fn deref(&self) -> &Tree {
        &self.default
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

        let guard = pin();

        let context = Arc::new(Context::start(config)?);

        let default = Arc::new(meta::open_tree(
            context.clone(),
            DEFAULT_TREE_ID.to_vec(),
            &guard,
        )?);

        let ret = Db {
            context: context.clone(),
            default,
            tenants: Arc::new(RwLock::new(FastMap8::default())),
        };

        let mut tenants = ret.tenants.write().unwrap();

        for (id, root) in
            context.pagecache.meta(&guard)?.tenants().into_iter()
        {
            let tree = Tree {
                tree_id: id.clone(),
                subscriptions: Arc::new(Subscriptions::default()),
                context: context.clone(),
                root: Arc::new(AtomicUsize::new(root)),
            };
            tenants.insert(id, Arc::new(tree));
        }

        drop(tenants);

        #[cfg(feature = "event_log")]
        context.event_log.meta_after_restart(
            context
                .pagecache
                .meta(&guard)
                .expect("should be able to get meta under test")
                .clone(),
        );

        Ok(ret)
    }

    /// Open or create a new disk-backed Tree with its own keyspace,
    /// accessible from the `Db` via the provided identifier.
    pub fn open_tree(&self, name: Vec<u8>) -> Result<Arc<Tree>, ()> {
        let tenants = self.tenants.read().unwrap();
        if let Some(tree) = tenants.get(&name) {
            return Ok(tree.clone());
        }
        drop(tenants);

        let guard = pin();

        let mut tenants = self.tenants.write().unwrap();
        let tree = Arc::new(meta::open_tree(
            self.context.clone(),
            name.clone(),
            &guard,
        )?);
        tenants.insert(name, tree.clone());
        drop(tenants);
        Ok(tree)
    }

    /// Remove a disk-backed collection.
    pub fn drop_tree(&self, name: &[u8]) -> Result<bool, ()> {
        if name == DEFAULT_TREE_ID {
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

        let mut root_id = self
            .context
            .pagecache
            .meta_pid_for_name(&name, &guard)?;

        let leftmost_chain: Vec<PageId> = tree
            .path_for_key(b"", &guard)?
            .into_iter()
            .map(|(frag, _tp)| frag.unwrap_base().id)
            .collect();

        loop {
            let res = self
                .context
                .pagecache
                .cas_root_in_meta(
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

        tree.root.store(
            usize::max_value(),
            std::sync::atomic::Ordering::SeqCst,
        );

        // drop writer lock
        drop(tenants);

        tree.gc_pages(leftmost_chain)?;

        guard.flush();

        Ok(true)
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
        self.context.was_recovered()
    }

    /// Generate a monotonic ID. Not guaranteed to be
    /// contiguous. Written to disk every `idgen_persist_interval`
    /// operations, followed by a blocking flush. During recovery, we
    /// take the last recovered generated ID and add 2x
    /// the `idgen_persist_interval` to it. While persisting, if the
    /// previous persisted counter wasn't synced to disk yet, we will do
    /// a blocking flush to fsync the latest counter, ensuring
    /// that we will never give out the same counter twice.
    pub fn generate_id(&self) -> Result<u64, ()> {
        self.context.generate_id()
    }
}

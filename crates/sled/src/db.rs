use std::{ops::Deref, sync::Arc};

use super::*;

/// The `sled` embedded database!
#[derive(Clone)]
pub struct Db {
    context: Arc<Context>,
    default: Arc<Tree>,
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

        let default = context
            .pagecache
            .open_tree(DEFAULT_TREE_ID.to_vec(), context.clone())?;

        let mut tenants = context.tenants.write().unwrap();

        for (id, _root) in meta::meta(&context.pagecache, &guard)?
            .tenants()
            .into_iter()
        {
            let tree = Tree {
                tree_id: id.clone(),
                subscriptions: Arc::new(Subscriptions::default()),
                context: context.clone(),
            };
            tenants.insert(id, Arc::new(tree));
        }

        drop(tenants);

        #[cfg(feature = "event_log")]
        context.event_log.meta_after_restart(
            meta::meta(&*pagecache, &guard)
                .expect("should be able to get meta under test")
                .clone(),
        );

        Ok(Db { context, default })
    }

    /// Open or create a new disk-backed Tree with its own keyspace,
    /// accessible from the `Db` via the provided identifier.
    pub fn open_tree(&self, name: Vec<u8>) -> Result<Arc<Tree>, ()> {
        self.context.pagecache.open_tree(name, self.context.clone())
    }

    /// Remove a disk-backed collection.
    pub fn drop_tree(&self, name: &[u8]) -> Result<bool, ()> {
        self.context.pagecache.drop_tree(name, &self.context)
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
    pub fn generate_id(&self) -> Result<usize, ()> {
        self.context.generate_id()
    }
}

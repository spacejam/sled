use std::{
    ops::Deref,
    sync::{atomic::AtomicUsize, Arc, Mutex, RwLock},
};

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

        let context = Arc::new(Context::start(config)?);

        let default = context.open_tree(DEFAULT_TREE_ID.to_vec())?;

        Ok(Db { context, default })
    }

    /// Open or create a new disk-backed Tree with its own keyspace,
    /// accessible from the `Db` via the provided identifier.
    pub fn open_tree(&self, name: Vec<u8>) -> Result<Arc<Tree>, ()> {
        self.context.open_tree(name)
    }

    /// Remove a disk-backed collection.
    pub fn drop_tree(&self, name: &[u8]) -> Result<bool, ()> {
        self.context.drop_tree(name)
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
}

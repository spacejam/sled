use std::{
    ops::Deref,
    sync::{atomic::AtomicU64, Arc, RwLock},
};

use pagecache::FastMap8;

use super::*;

/// The `sled` embedded database!
#[derive(Clone)]
pub struct Db {
    context: Context,
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

impl std::fmt::Debug for Db {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::result::Result<(), std::fmt::Error> {
        let tenants = self.tenants.read().unwrap();
        write!(f, "Db {{")?;
        for (raw_name, tree) in tenants.iter() {
            let name = std::str::from_utf8(&raw_name)
                .map(String::from)
                .unwrap_or_else(|_| format!("{:?}", raw_name));
            write!(f, "tree: {:?} contents: {:?}", name, tree)?;
        }
        write!(f, "}}")?;
        Ok(())
    }
}

impl Db {
    /// Load existing or create a new `Db` with a default configuration.
    pub fn start_default<P: AsRef<std::path::Path>>(path: P) -> Result<Db> {
        let config = ConfigBuilder::new().path(path).build();
        Self::start(config)
    }

    /// Load existing or create a new `Db`.
    pub fn start(config: Config) -> Result<Db> {
        let _measure = Measure::new(&M.tree_start);

        let context = Context::start(config)?;

        let flusher_pagecache = context.pagecache.clone();
        let flusher = context.flush_every_ms.map(move |fem| {
            flusher::Flusher::new(
                "log flusher".to_owned(),
                flusher_pagecache,
                fem,
            )
        });
        *context._flusher.lock().unwrap() = flusher;

        // create or open the default tree
        let tx = context.pagecache.begin()?;
        let default = Arc::new(meta::open_tree(
            context.clone(),
            DEFAULT_TREE_ID.to_vec(),
            &tx,
        )?);

        let ret = Db {
            context: context.clone(),
            default,
            tenants: Arc::new(RwLock::new(FastMap8::default())),
        };

        let mut tenants = ret.tenants.write().unwrap();

        for (id, root) in context.pagecache.meta(&tx)?.tenants().into_iter() {
            let tree = Tree {
                tree_id: id.clone(),
                subscriptions: Arc::new(Subscriptions::default()),
                context: context.clone(),
                root: Arc::new(AtomicU64::new(root)),
                concurrency_control: Arc::new(RwLock::new(())),
            };
            tenants.insert(id, Arc::new(tree));
        }

        drop(tenants);

        Ok(ret)
    }

    /// Open or create a new disk-backed Tree with its own keyspace,
    /// accessible from the `Db` via the provided identifier.
    pub fn open_tree<V: AsRef<[u8]>>(&self, name: V) -> Result<Arc<Tree>> {
        let name = name.as_ref();
        let tenants = self.tenants.read().unwrap();
        if let Some(tree) = tenants.get(name) {
            return Ok(tree.clone());
        }
        drop(tenants);

        let tx = self.context.pagecache.begin()?;

        let mut tenants = self.tenants.write().unwrap();
        let tree = Arc::new(meta::open_tree(
            self.context.clone(),
            name.to_vec(),
            &tx,
        )?);
        tenants.insert(name.to_vec(), tree.clone());
        drop(tenants);
        Ok(tree)
    }

    /// Remove a disk-backed collection.
    pub fn drop_tree(&self, name: &[u8]) -> Result<bool> {
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

        let tx = self.context.pagecache.begin()?;

        let mut root_id =
            Some(self.context.pagecache.meta_pid_for_name(&name, &tx)?);

        let mut leftmost_chain: Vec<PageId> = vec![root_id.unwrap()];
        let mut cursor = root_id.unwrap();
        loop {
            let view_opt = self.view_for_pid(cursor, &tx)?;
            let view = if let Some(view) = view_opt {
                view
            } else {
                break;
            };

            let node = view.compact(&self.context);

            if let Some(index) = node.data.index_ref() {
                let leftmost_child = index[0].1;
                leftmost_chain.push(leftmost_child);
                cursor = leftmost_child;
            } else {
                break;
            }
        }

        loop {
            let res = self.context.pagecache.cas_root_in_meta(
                name.to_vec(),
                root_id,
                None,
                &tx,
            )?;

            if let Err(actual_root) = res {
                root_id = actual_root;
            } else {
                break;
            }
        }

        tree.root
            .store(u64::max_value(), std::sync::atomic::Ordering::SeqCst);

        // drop writer lock
        drop(tenants);

        tree.gc_pages(leftmost_chain)?;

        tx.flush();

        Ok(true)
    }

    /// Returns the trees names saved in this Db.
    pub fn tree_names(&self) -> Vec<Vec<u8>> {
        let tenants = self.tenants.read().unwrap();
        tenants.iter().map(|(name, _)| name.clone()).collect()
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
    pub fn generate_id(&self) -> Result<u64> {
        self.context.generate_id()
    }

    /// Traverses all files and calculates their total physical
    /// size, then traverses all pages and calculates their
    /// total logical size, then divides the physical size
    /// by the logical size.
    #[doc(hidden)]
    pub fn space_amplification(&self) -> Result<f64> {
        self.context.pagecache.space_amplification()
    }
}

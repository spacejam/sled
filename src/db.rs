use std::ops::Deref;

use crate::*;

/// The `sled` embedded database!
#[derive(Clone)]
pub struct Db {
    context: Context,
    pub(crate) default: Tree,
    tenants: Arc<RwLock<FastMap8<Vec<u8>, Tree>>>,
}

#[allow(unsafe_code)]
unsafe impl Send for Db {}

#[allow(unsafe_code)]
unsafe impl Sync for Db {}

impl Deref for Db {
    type Target = Tree;

    fn deref(&self) -> &Tree {
        &self.default
    }
}

impl Debug for Db {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        let tenants = self.tenants.read();
        write!(f, "Db {{")?;
        for (raw_name, tree) in tenants.iter() {
            let name = std::str::from_utf8(&raw_name)
                .ok()
                .map_or_else(|| format!("{:?}", raw_name), String::from);
            write!(f, "tree: {:?} contents: {:?}", name, tree)?;
        }
        write!(f, "}}")?;
        Ok(())
    }
}

impl Db {
    /// Load existing or create a new `Db` with a default configuration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use sled::Db;
    ///
    /// let t = Db::open("my_db").unwrap();
    /// ```
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let config = ConfigBuilder::new().path(path).build();
        Self::start(config)
    }

    #[doc(hidden)]
    #[deprecated(since = "0.24.2", note = "replaced by `Db:open`")]
    pub fn start_default<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        Self::open(path)
    }

    /// Load existing or create a new `Db`.
    pub fn start(config: Config) -> Result<Self> {
        let _measure = Measure::new(&M.tree_start);

        let context = Context::start(config)?;

        #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
        {
            if !context.read_only {
                let flusher_pagecache = context.pagecache.clone();
                let flusher = context.flush_every_ms.map(move |fem| {
                    flusher::Flusher::new(
                        "log flusher".to_owned(),
                        flusher_pagecache,
                        fem,
                    )
                });
                *context._flusher.lock() = flusher;
            }
        }

        // create or open the default tree
        let guard = pin();
        let default =
            meta::open_tree(&context, DEFAULT_TREE_ID.to_vec(), &guard)?;

        let ret = Self {
            context: context.clone(),
            default,
            tenants: Arc::new(RwLock::new(FastMap8::default())),
        };

        let mut tenants = ret.tenants.write();

        for (id, root) in context.pagecache.meta(&guard)?.tenants() {
            let tree = Tree(Arc::new(TreeInner {
                tree_id: id.clone(),
                subscriptions: Subscriptions::default(),
                context: context.clone(),
                root: AtomicU64::new(root),
                concurrency_control: RwLock::new(()),
                merge_operator: RwLock::new(None),
            }));
            tenants.insert(id, tree);
        }

        drop(tenants);

        Ok(ret)
    }

    /// Open or create a new disk-backed Tree with its own keyspace,
    /// accessible from the `Db` via the provided identifier.
    pub fn open_tree<V: AsRef<[u8]>>(&self, name: V) -> Result<Tree> {
        let name = name.as_ref();
        let tenants = self.tenants.read();
        if let Some(tree) = tenants.get(name) {
            return Ok(tree.clone());
        }
        drop(tenants);

        let guard = pin();

        let mut tenants = self.tenants.write();

        // we need to check this again in case another
        // thread opened it concurrently.
        if let Some(tree) = tenants.get(name) {
            return Ok(tree.clone());
        }

        let tree = meta::open_tree(&self.context, name.to_vec(), &guard)?;

        tenants.insert(name.to_vec(), tree.clone());

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

        let mut tenants = self.tenants.write();

        let tree = if let Some(tree) = tenants.remove(&*name) {
            tree
        } else {
            return Ok(false);
        };

        let guard = pin();

        let mut root_id =
            Some(self.context.pagecache.meta_pid_for_name(&name, &guard)?);

        let mut leftmost_chain: Vec<PageId> = vec![root_id.unwrap()];
        let mut cursor = root_id.unwrap();
        while let Some(view) = self.view_for_pid(cursor, &guard)? {
            if let Some(index) = view.data.index_ref() {
                let leftmost_child = index[0].1;
                leftmost_chain.push(leftmost_child);
                cursor = leftmost_child;
            } else {
                break;
            }
        }

        loop {
            let res = self
                .context
                .pagecache
                .cas_root_in_meta(&name, root_id, None, &guard)?;

            if let Err(actual_root) = res {
                root_id = actual_root;
            } else {
                break;
            }
        }

        tree.root.store(u64::max_value(), SeqCst);

        // drop writer lock
        drop(tenants);

        tree.gc_pages(leftmost_chain)?;

        guard.flush();

        Ok(true)
    }

    /// Returns the trees names saved in this Db.
    pub fn tree_names(&self) -> Vec<Vec<u8>> {
        let tenants = self.tenants.read();
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

    /// A database export method for all collections in the `Db`,
    /// for use in sled version upgrades. Can be used in combination
    /// with the `import` method below on a database running a later
    /// version.
    ///
    /// # Panics
    ///
    /// Panics if any IO problems occur while trying
    /// to perform the export.
    pub fn export(
        &self,
    ) -> Vec<(
        CollectionType,
        CollectionName,
        impl Iterator<Item = Vec<Vec<u8>>>,
    )> {
        let tenants = self.tenants.read();

        let mut ret = vec![];

        for (name, tree) in tenants.iter() {
            ret.push((
                b"tree".to_vec(),
                name.to_vec(),
                tree.iter().map(|kv| {
                    let kv = kv.unwrap();
                    vec![kv.0.to_vec(), kv.1.to_vec()]
                }),
            ));
        }

        ret
    }

    /// Imports the collections from a previous database.
    ///
    /// # Panics
    ///
    /// Panics if any IO problems occur while trying
    /// to perform the import.
    pub fn import(
        &self,
        export: Vec<(
            CollectionType,
            CollectionName,
            impl Iterator<Item = Vec<Vec<u8>>>,
        )>,
    ) {
        for (collection_type, collection_name, collection_iter) in export {
            match collection_type {
                ref t if t == b"tree" => {
                    let tree = self
                        .open_tree(collection_name)
                        .expect("failed to open new tree during import");
                    for mut kv in collection_iter {
                        let v = kv
                            .pop()
                            .expect("failed to get value from tree export");
                        let k = kv
                            .pop()
                            .expect("failed to get key from tree export");
                        tree.insert(k, v).expect(
                            "failed to insert value during tree import",
                        );
                    }
                }
                other => panic!("unknown collection type {:?}", other),
            }
        }
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

/// These types provide the information that allows an entire
/// system to be exported and imported to facilitate
/// major upgrades. It is comprised entirely
/// of standard library types to be forward compatible.
/// NB this definitions are expensive to change, because
/// they impact the migration path.
type CollectionType = Vec<u8>;
type CollectionName = Vec<u8>;

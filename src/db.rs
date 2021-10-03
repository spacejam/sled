use std::ops::Deref;

use crate::*;

const DEFAULT_TREE_ID: &[u8] = b"__sled__default";

/// The `sled` embedded database! Implements
/// `Deref<Target = sled::Tree>` to refer to
/// a default keyspace / namespace / bucket.
///
/// When dropped, all buffered writes are flushed
/// to disk, using the same method used by
/// `Tree::flush`.
#[derive(Clone)]
#[doc(alias = "database")]
pub struct Db {
    #[doc(hidden)]
    pub context: Context,
    pub(crate) default: Tree,
    tenants: Arc<RwLock<FastMap8<IVec, Tree>>>,
}

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
        writeln!(f, "Db {{")?;
        for (raw_name, tree) in tenants.iter() {
            let name = std::str::from_utf8(raw_name)
                .ok()
                .map_or_else(|| format!("{:?}", raw_name), String::from);
            write!(f, "    Tree: {:?} contents: {:?}", name, tree)?;
        }
        write!(f, "}}")?;
        Ok(())
    }
}

impl Db {
    pub(crate) fn start_inner(config: RunningConfig) -> Result<Self> {
        #[cfg(feature = "metrics")]
        let _measure = Measure::new(&M.tree_start);

        let context = Context::start(config)?;

        #[cfg(all(
            not(miri),
            any(
                windows,
                target_os = "linux",
                target_os = "macos",
                target_os = "dragonfly",
                target_os = "freebsd",
                target_os = "openbsd",
                target_os = "netbsd",
                target_os = "ios",
            )
        ))]
        {
            let flusher_pagecache = context.pagecache.clone();
            let flusher = context.flush_every_ms.map(move |fem| {
                flusher::Flusher::new(
                    "log flusher".to_owned(),
                    flusher_pagecache,
                    fem,
                )
            });
            *context.flusher.lock() = flusher;
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

        for (id, root) in &context.pagecache.get_meta(&guard).inner {
            let tree = Tree(Arc::new(TreeInner {
                tree_id: id.clone(),
                subscribers: Subscribers::default(),
                context: context.clone(),
                root: AtomicU64::new(*root),
                merge_operator: RwLock::new(None),
            }));
            assert!(tenants.insert(id.clone(), tree).is_none());
        }

        drop(tenants);

        #[cfg(feature = "event_log")]
        {
            for (_name, tree) in ret.tenants.read().iter() {
                tree.verify_integrity()?;
            }
            ret.context.event_log.verify();
        }

        Ok(ret)
    }

    /// Open or create a new disk-backed Tree with its own keyspace,
    /// accessible from the `Db` via the provided identifier.
    pub fn open_tree<V: AsRef<[u8]>>(&self, name: V) -> Result<Tree> {
        let name_ref = name.as_ref();
        let tenants = self.tenants.read();
        if let Some(tree) = tenants.get(name_ref) {
            return Ok(tree.clone());
        }
        drop(tenants);

        let guard = pin();

        let mut tenants = self.tenants.write();

        // we need to check this again in case another
        // thread opened it concurrently.
        if let Some(tree) = tenants.get(name_ref) {
            return Ok(tree.clone());
        }

        let tree = meta::open_tree(&self.context, name_ref.to_vec(), &guard)?;

        assert!(tenants.insert(name_ref.into(), tree.clone()).is_none());

        Ok(tree)
    }

    /// Remove a disk-backed collection. This is blocking and fairly slow.
    pub fn drop_tree<V: AsRef<[u8]>>(&self, name: V) -> Result<bool> {
        let name_ref = name.as_ref();
        if name_ref == DEFAULT_TREE_ID {
            return Err(Error::Unsupported("cannot remove the default tree"));
        }
        trace!("dropping tree {:?}", name_ref,);

        let mut tenants = self.tenants.write();

        let tree = if let Some(tree) = tenants.remove(&*name_ref) {
            tree
        } else {
            return Ok(false);
        };

        // signal to all threads that this tree is no longer valid
        tree.root.store(u64::max_value(), SeqCst);

        let guard = pin();

        let mut root_id =
            Some(self.context.pagecache.meta_pid_for_name(name_ref, &guard)?);

        let mut leftmost_chain: Vec<PageId> = vec![root_id.unwrap()];
        let mut cursor = root_id.unwrap();
        while let Some(view) = self.view_for_pid(cursor, &guard)? {
            if view.is_index {
                let leftmost_child = view.iter_index_pids().next().unwrap();
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
                .cas_root_in_meta(name_ref, root_id, None, &guard)?;

            if let Err(actual_root) = res {
                root_id = actual_root;
            } else {
                break;
            }
        }

        // drop writer lock and asynchronously
        drop(tenants);

        guard.flush();

        drop(guard);

        self.gc_pages(leftmost_chain)?;

        Ok(true)
    }

    // Remove all pages for this tree from the underlying
    // PageCache. This will leave orphans behind if
    // the tree crashes during gc.
    fn gc_pages(&self, mut leftmost_chain: Vec<PageId>) -> Result<()> {
        let mut guard = pin();

        let mut ops = 0;
        while let Some(mut pid) = leftmost_chain.pop() {
            loop {
                ops += 1;
                if ops % 64 == 0 {
                    // we re-pin here to avoid memory blow-ups during
                    // long-running tree removals.
                    guard = pin();
                }
                let cursor_view =
                    if let Some(view) = self.view_for_pid(pid, &guard)? {
                        view
                    } else {
                        trace!(
                            "encountered Free node pid {} while GC'ing tree",
                            pid
                        );
                        break;
                    };

                let ret = self.context.pagecache.free(
                    pid,
                    cursor_view.node_view.0,
                    &guard,
                )?;

                if ret.is_ok() {
                    let next_pid = if let Some(next_pid) = cursor_view.next {
                        next_pid
                    } else {
                        break;
                    };
                    assert_ne!(pid, next_pid.get());
                    pid = next_pid.get();
                }
            }
        }

        Ok(())
    }

    /// Returns the trees names saved in this Db.
    pub fn tree_names(&self) -> Vec<IVec> {
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
    ///
    /// # Examples
    ///
    /// If you want to migrate from one version of sled
    /// to another, you need to pull in both versions
    /// by using version renaming:
    ///
    /// `Cargo.toml`:
    ///
    /// ```toml
    /// [dependencies]
    /// sled = "0.32"
    /// old_sled = { version = "0.31", package = "sled" }
    /// ```
    ///
    /// and in your code, remember that old versions of
    /// sled might have a different way to open them
    /// than the current `sled::open` method:
    ///
    /// ```
    /// # use sled as old_sled;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let old = old_sled::open("my_old__db")?;
    ///
    /// // may be a different version of sled,
    /// // the export type is version agnostic.
    /// let new = sled::open("my_new__db")?;
    ///
    /// let export = old.export();
    /// new.import(export);
    ///
    /// assert_eq!(old.checksum()?, new.checksum()?);
    /// # drop(old);
    /// # drop(new);
    /// # std::fs::remove_file("my_old__db");
    /// # std::fs::remove_file("my_new__db");
    /// # Ok(()) }
    /// ```
    pub fn export(
        &self,
    ) -> Vec<(CollectionType, CollectionName, impl Iterator<Item = Vec<Vec<u8>>>)>
    {
        let tenants = self.tenants.read();

        let mut ret = vec![];

        for (name, tree) in tenants.iter() {
            ret.push((
                b"tree".to_vec(),
                name.to_vec(),
                tree.iter().map(|kv_opt| {
                    let kv = kv_opt.unwrap();
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
    ///
    /// # Examples
    ///
    /// If you want to migrate from one version of sled
    /// to another, you need to pull in both versions
    /// by using version renaming:
    ///
    /// `Cargo.toml`:
    ///
    /// ```toml
    /// [dependencies]
    /// sled = "0.32"
    /// old_sled = { version = "0.31", package = "sled" }
    /// ```
    ///
    /// and in your code, remember that old versions of
    /// sled might have a different way to open them
    /// than the current `sled::open` method:
    ///
    /// ```
    /// # use sled as old_sled;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let old = old_sled::open("my_old_db")?;
    ///
    /// // may be a different version of sled,
    /// // the export type is version agnostic.
    /// let new = sled::open("my_new_db")?;
    ///
    /// let export = old.export();
    /// new.import(export);
    ///
    /// assert_eq!(old.checksum()?, new.checksum()?);
    /// # drop(old);
    /// # drop(new);
    /// # std::fs::remove_file("my_old_db");
    /// # std::fs::remove_file("my_new_db");
    /// # Ok(()) }
    /// ```
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
                        let old = tree.insert(k, v).expect(
                            "failed to insert value during tree import",
                        );
                        assert!(
                            old.is_none(),
                            "import is overwriting existing data"
                        );
                    }
                }
                other => panic!("unknown collection type {:?}", other),
            }
        }
    }

    /// Returns the CRC32 of all keys and values
    /// in this Db.
    ///
    /// This is O(N) and locks all underlying Trees
    /// for the duration of the entire scan.
    pub fn checksum(&self) -> Result<u32> {
        let tenants_mu = self.tenants.write();

        // we use a btreemap to ensure lexicographic
        // iteration over tree names to have consistent
        // checksums.
        let tenants: BTreeMap<_, _> = tenants_mu.iter().collect();

        let mut hasher = crc32fast::Hasher::new();

        for (name, tree) in &tenants {
            hasher.update(name);

            let mut iter = tree.iter();
            while let Some(kv_res) = iter.next_inner() {
                let (k, v) = kv_res?;
                hasher.update(&k);
                hasher.update(&v);
            }
        }

        Ok(hasher.finalize())
    }

    /// Returns the on-disk size of the storage files
    /// for this database.
    pub fn size_on_disk(&self) -> Result<u64> {
        self.context.pagecache.size_on_disk()
    }

    /// Traverses all files and calculates their total physical
    /// size, then traverses all pages and calculates their
    /// total logical size, then divides the physical size
    /// by the logical size.
    #[doc(hidden)]
    pub fn space_amplification(&self) -> Result<f64> {
        self.context.pagecache.space_amplification()
    }

    /// Returns a true value if one of the tree names linked
    /// to the database is found, if not a false will be returned.
    pub fn contains_tree<V: AsRef<[u8]>>(&self, name: V) -> bool {
        self.tenants.read().contains_key(name.as_ref())
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

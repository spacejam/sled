use std::collections::HashMap;
use std::fmt;
use std::io;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::*;

/// sled 1.0 alpha :)
///
/// One of the main differences between this and sled 0.34 is that
/// `Db` and `Tree` now have a `LEAF_FANOUT` const generic parameter.
/// This parameter is an interesting single-knob performance tunable
/// that allows users to traverse the performance-vs-efficiency
/// trade-off spectrum. The default value of `1024` causes keys and
/// values to be more efficiently compressed when stored on disk,
/// but for larger-than-memory random workloads it may be advantageous
/// to lower `LEAF_FANOUT` to between `16` to `256`, depending on your
/// efficiency requirements. A lower value will also cause contention
/// to be reduced for frequently accessed data. This value cannot be
/// changed after creating the database.
///
/// As an alpha release, please do not expect this to be safe for
/// business-critical use cases. However, if you would like this to
/// serve your business-critical use cases over time, please give it
/// a shot in a low-risk non-production environment and report any
/// issues you encounter in a github issue.
///
/// Note that `Db` implements `Deref` for the default `Tree` (sled's
/// version of namespaces / keyspaces / buckets), but you can create
/// and use others using `Db::open_tree`.
#[derive(Clone)]
pub struct Db<const LEAF_FANOUT: usize = 1024> {
    config: Config,
    _shutdown_dropper: Arc<ShutdownDropper<LEAF_FANOUT>>,
    pc: PageCache<LEAF_FANOUT>,
    trees: Arc<Mutex<HashMap<CollectionId, Tree<LEAF_FANOUT>>>>,
    collection_id_allocator: Arc<Allocator>,
    collection_name_mapping: Tree<LEAF_FANOUT>,
    default_tree: Tree<LEAF_FANOUT>,
    was_recovered: bool,
    #[cfg(feature = "for-internal-testing-only")]
    event_verifier: Arc<crate::event_verifier::EventVerifier>,
}

impl<const LEAF_FANOUT: usize> std::ops::Deref for Db<LEAF_FANOUT> {
    type Target = Tree<LEAF_FANOUT>;
    fn deref(&self) -> &Tree<LEAF_FANOUT> {
        &self.default_tree
    }
}

impl<const LEAF_FANOUT: usize> fmt::Debug for Db<LEAF_FANOUT> {
    fn fmt(&self, w: &mut fmt::Formatter<'_>) -> fmt::Result {
        let alternate = w.alternate();

        let mut debug_struct = w.debug_struct(&format!("Db<{}>", LEAF_FANOUT));

        if alternate {
            debug_struct
                .field("global_error", &self.check_error())
                .field(
                    "data",
                    &format!("{:?}", self.iter().collect::<Vec<_>>()),
                )
                .finish()
        } else {
            debug_struct.field("global_error", &self.check_error()).finish()
        }
    }
}

fn flusher<const LEAF_FANOUT: usize>(
    pc: PageCache<LEAF_FANOUT>,
    shutdown_signal: mpsc::Receiver<mpsc::Sender<()>>,
    flush_every_ms: usize,
) {
    let interval = Duration::from_millis(flush_every_ms as _);
    let mut last_flush_duration = Duration::default();

    let flush = || {
        if let Err(e) = pc.flush() {
            log::error!("Db flusher encountered error while flushing: {:?}", e);
            pc.set_error(&e);

            std::process::abort();
        }
    };

    loop {
        let recv_timeout = interval
            .saturating_sub(last_flush_duration)
            .max(Duration::from_millis(1));
        if let Ok(shutdown_sender) = shutdown_signal.recv_timeout(recv_timeout)
        {
            flush();

            // this is probably unnecessary but it will avoid issues
            // if egregious bugs get introduced that trigger it
            pc.set_error(&io::Error::new(
                io::ErrorKind::Other,
                "system has been shut down".to_string(),
            ));

            assert!(pc.is_clean());

            drop(pc);

            if let Err(e) = shutdown_sender.send(()) {
                log::error!(
                    "Db flusher could not ack shutdown to requestor: {e:?}"
                );
            }
            log::debug!(
                "flush thread terminating after signalling to requestor"
            );
            return;
        }

        let before_flush = Instant::now();

        flush();

        last_flush_duration = before_flush.elapsed();
    }
}

impl<const LEAF_FANOUT: usize> Drop for Db<LEAF_FANOUT> {
    fn drop(&mut self) {
        if self.config.flush_every_ms.is_none() {
            if let Err(e) = self.flush() {
                log::error!("failed to flush Db on Drop: {e:?}");
            }
        } else {
            // otherwise, it is expected that the flusher thread will
            // flush while shutting down the final Db/Tree instance
        }
    }
}

impl<const LEAF_FANOUT: usize> Db<LEAF_FANOUT> {
    pub fn size_on_disk(&self) -> io::Result<u64> {
        use std::fs::read_dir;

        fn recurse(mut dir: std::fs::ReadDir) -> io::Result<u64> {
            dir.try_fold(0, |acc, file| {
                let file = file?;
                let size = match file.metadata()? {
                    data if data.is_dir() => recurse(read_dir(file.path())?)?,
                    data => data.len(),
                };
                Ok(acc + size)
            })
        }

        recurse(read_dir(&self.pc.config.path)?)
    }

    /// Returns `true` if the database was
    /// recovered from a previous process.
    /// Note that database state is only
    /// guaranteed to be present up to the
    /// last call to `flush`! Otherwise state
    /// is synced to disk periodically if the
    /// `Config.sync_every_ms` configuration option
    /// is set to `Some(number_of_ms_between_syncs)`
    /// or if the IO buffer gets filled to
    /// capacity before being rotated.
    pub fn was_recovered(&self) -> bool {
        self.was_recovered
    }

    pub fn open_with_config(config: &Config) -> io::Result<Db<LEAF_FANOUT>> {
        let (shutdown_tx, shutdown_rx) = mpsc::channel();

        let (pc, indices, was_recovered) = PageCache::recover(&config)?;

        let _shutdown_dropper = Arc::new(ShutdownDropper {
            shutdown_sender: Mutex::new(shutdown_tx),
            pc: Mutex::new(pc.clone()),
        });

        #[cfg(feature = "for-internal-testing-only")]
        let event_verifier = Arc::default();

        let mut allocated_collection_ids = fnv::FnvHashSet::default();

        let mut trees: HashMap<CollectionId, Tree<LEAF_FANOUT>> = indices
            .into_iter()
            .map(|(collection_id, index)| {
                assert!(
                    allocated_collection_ids.insert(collection_id.0),
                    "allocated_collection_ids already contained {:?}",
                    collection_id
                );
                (
                    collection_id,
                    Tree::new(
                        collection_id,
                        pc.clone(),
                        index,
                        _shutdown_dropper.clone(),
                        #[cfg(feature = "for-internal-testing-only")]
                        event_verifier.clone(),
                    ),
                )
            })
            .collect();

        let collection_name_mapping =
            trees.get(&NAME_MAPPING_COLLECTION_ID).unwrap().clone();

        let default_tree = trees.get(&DEFAULT_COLLECTION_ID).unwrap().clone();

        for kv_res in collection_name_mapping.iter() {
            let (_collection_name, collection_id_buf) = kv_res.unwrap();
            let collection_id = CollectionId(u64::from_le_bytes(
                collection_id_buf.as_ref().try_into().unwrap(),
            ));

            if trees.contains_key(&collection_id) {
                continue;
            }

            // need to initialize tree leaf for empty collection

            assert!(
                allocated_collection_ids.insert(collection_id.0),
                "allocated_collection_ids already contained {:?}",
                collection_id
            );

            let empty_node = pc.allocate_node();

            let index = Index::default();

            assert!(index.insert(InlineArray::default(), empty_node).is_none());

            let tree = Tree::new(
                collection_id,
                pc.clone(),
                index,
                _shutdown_dropper.clone(),
                #[cfg(feature = "for-internal-testing-only")]
                event_verifier.clone(),
            );

            trees.insert(collection_id, tree);
        }

        let collection_id_allocator =
            Arc::new(Allocator::from_allocated(&allocated_collection_ids));

        assert_eq!(collection_name_mapping.len() + 2, trees.len());

        let ret = Db {
            config: config.clone(),
            pc: pc.clone(),
            default_tree,
            collection_name_mapping,
            collection_id_allocator,
            trees: Arc::new(Mutex::new(trees)),
            _shutdown_dropper,
            was_recovered,
            #[cfg(feature = "for-internal-testing-only")]
            event_verifier,
        };

        if let Some(flush_every_ms) = ret.pc.config.flush_every_ms {
            let spawn_res = std::thread::Builder::new()
                .name("sled_flusher".into())
                .spawn(move || flusher(pc, shutdown_rx, flush_every_ms));

            if let Err(e) = spawn_res {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("unable to spawn flusher thread for sled database: {:?}", e)
                ));
            }
        }
        Ok(ret)
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
    /// # Ok(()) }
    /// ```
    pub fn export(
        &self,
    ) -> Vec<(
        CollectionType,
        CollectionName,
        impl Iterator<Item = Vec<Vec<u8>>> + '_,
    )> {
        let trees = self.trees.lock();

        let mut ret = vec![];

        for kv_res in self.collection_name_mapping.iter() {
            let (collection_name, collection_id_buf) = kv_res.unwrap();
            let collection_id = CollectionId(u64::from_le_bytes(
                collection_id_buf.as_ref().try_into().unwrap(),
            ));
            let tree = trees.get(&collection_id).unwrap().clone();

            ret.push((
                b"tree".to_vec(),
                collection_name.to_vec(),
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

    pub fn contains_tree<V: AsRef<[u8]>>(&self, name: V) -> io::Result<bool> {
        Ok(self.collection_name_mapping.get(name.as_ref())?.is_some())
    }

    pub fn drop_tree<V: AsRef<[u8]>>(&self, name: V) -> io::Result<bool> {
        let name_ref = name.as_ref();
        let mut trees = self.trees.lock();

        let tree = if let Some(collection_id_buf) =
            self.collection_name_mapping.get(name_ref)?
        {
            let collection_id = CollectionId(u64::from_le_bytes(
                collection_id_buf.as_ref().try_into().unwrap(),
            ));

            trees.get(&collection_id).unwrap()
        } else {
            return Ok(false);
        };

        tree.clear()?;

        self.collection_name_mapping.remove(name_ref)?;

        Ok(true)
    }
    /// Open or create a new disk-backed Tree with its own keyspace,
    /// accessible from the `Db` via the provided identifier.
    pub fn open_tree<V: AsRef<[u8]>>(
        &self,
        name: V,
    ) -> io::Result<Tree<LEAF_FANOUT>> {
        let name_ref = name.as_ref();
        let mut trees = self.trees.lock();

        if let Some(collection_id_buf) =
            self.collection_name_mapping.get(name_ref)?
        {
            let collection_id = CollectionId(u64::from_le_bytes(
                collection_id_buf.as_ref().try_into().unwrap(),
            ));

            let tree = trees.get(&collection_id).unwrap();

            return Ok(tree.clone());
        }

        let collection_id =
            CollectionId(self.collection_id_allocator.allocate());

        let empty_node = self.pc.allocate_node();

        let index = Index::default();

        assert!(index.insert(InlineArray::default(), empty_node).is_none());

        let tree = Tree::new(
            collection_id,
            self.pc.clone(),
            index,
            self._shutdown_dropper.clone(),
            #[cfg(feature = "for-internal-testing-only")]
            event_verifier.clone(),
        );

        self.collection_name_mapping
            .insert(name_ref, &collection_id.0.to_le_bytes())?;

        trees.insert(collection_id, tree.clone());

        Ok(tree)
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

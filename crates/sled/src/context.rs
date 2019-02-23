use std::sync::{atomic::AtomicUsize, Arc, Mutex, RwLock};

use pagecache::FastMap8;

use super::*;

use crate::PageCacheExt;

pub(crate) struct Context {
    // TODO file from config should be in here
    config: Config,
    pub(crate) pagecache:
        PageCache<BLinkMaterializer, Frag, Recovery>,
    pub(crate) idgen: Arc<AtomicUsize>,
    pub(crate) idgen_persists: Arc<AtomicUsize>,
    pub(crate) idgen_persist_mu: Arc<Mutex<()>>,
    pub(crate) tenants: Arc<RwLock<FastMap8<Vec<u8>, Arc<Tree>>>>,
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
                meta::meta(&self.pagecache, &guard)
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

        let pagecache = PageCache::start(config.clone())?;

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
                .replace(META_PID, TreePtr::allocated(), meta, &guard)
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
                    TreePtr::allocated(),
                    counter,
                    &guard,
                )
                .map_err(|e| e.danger_cast())?;
        } else {
            was_recovered = true;
        }

        Ok(Context {
            config,
            pagecache,
            tenants: Arc::new(RwLock::new(FastMap8::default())),
            idgen: Arc::new(AtomicUsize::new(idgen_recovery)),
            idgen_persists: Arc::new(AtomicUsize::new(
                idgen_persists,
            )),
            was_recovered,
            idgen_persist_mu: Arc::new(Mutex::new(())),
        })
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
        self.pagecache.generate_id(self)
    }
}

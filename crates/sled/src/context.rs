use std::sync::{Arc, Mutex};

use super::*;

#[derive(Clone)]
pub(crate) struct Context {
    // TODO file from config should be in here
    config: Config,
    /// Periodically flushes dirty data.
    pub(crate) _flusher: Arc<Mutex<Option<flusher::Flusher>>>,
    pub(crate) pagecache: Arc<PageCache<BLinkMaterializer, Frag>>,
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
            let tx = Tx::new(0);

            self.config.event_log.meta_before_restart(
                self.pagecache
                    .meta(&tx)
                    .expect("should get meta under test")
                    .clone(),
            );
        }
    }
}

impl Context {
    pub(crate) fn start(config: Config) -> Result<Context> {
        #[cfg(any(test, feature = "check_snapshot_integrity"))]
        match config.verify_snapshot::<BLinkMaterializer, Frag>() {
            Ok(_) => {}
            #[cfg(feature = "failpoints")]
            Err(Error::FailPoint) => {}
            other => panic!("failed to verify snapshot: {:?}", other),
        }

        let pagecache = Arc::new(PageCache::start(config.clone())?);

        Ok(Context {
            config,
            pagecache,
            _flusher: Arc::new(Mutex::new(None)),
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
        self.pagecache.was_recovered()
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
        self.pagecache.generate_id()
    }
}

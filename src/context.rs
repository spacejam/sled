use std::sync::Arc;

use super::*;

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct Context {
    // TODO file from config should be in here
    config: RunningConfig,
    /// Periodically flushes dirty data. We keep this in an
    /// Arc separate from the PageCache below to separate
    /// "high-level" references from Db, Tree etc... from
    /// "low-level" references like background threads.
    /// When the last high-level reference is dropped, it
    /// should trigger all background threads to clean
    /// up synchronously.
    #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
    pub(crate) flusher: Arc<Mutex<Option<flusher::Flusher>>>,
    #[doc(hidden)]
    pub pagecache: Arc<PageCache>,
}

impl std::ops::Deref for Context {
    type Target = RunningConfig;

    fn deref(&self) -> &RunningConfig {
        &self.config
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        loop {
            match self.pagecache.flush() {
                Ok(0) => return,
                Ok(_) => continue,
                Err(e) => {
                    error!(
                        "failed to flush underlying \
                         pagecache during drop: {:?}",
                        e
                    );
                    return;
                }
            }
        }
    }
}

impl Context {
    pub(crate) fn start(config: RunningConfig) -> Result<Self> {
        trace!("starting context");

        let pagecache = Arc::new(PageCache::start(config.clone())?);

        Ok(Self {
            config,
            pagecache,
            #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
            flusher: Arc::new(parking_lot::Mutex::new(None)),
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

    pub(crate) fn pin_log(&self, guard: &Guard) -> Result<RecoveryGuard<'_>> {
        self.pagecache.pin_log(guard)
    }
}

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fault_injection::{annotate, fallible};
use tempdir::TempDir;

use crate::Db;

macro_rules! builder {
    ($(($name:ident, $t:ty, $desc:expr)),*) => {
        $(
            #[doc=$desc]
            pub fn $name(mut self, to: $t) -> Self {
                self.$name = to;
                self
            }
        )*
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    /// The base directory for storing the database.
    pub path: PathBuf,
    /// Cache size in **bytes**. Default is 512mb.
    pub cache_capacity_bytes: usize,
    /// The percentage of the cache that is dedicated to the
    /// scan-resistant entry cache.
    pub entry_cache_percent: u8,
    /// Start a background thread that flushes data to disk
    /// every few milliseconds. Defaults to every 200ms.
    pub flush_every_ms: Option<usize>,
    /// The zstd compression level to use when writing data to disk. Defaults to 3.
    pub zstd_compression_level: i32,
    /// This is only set to `Some` for objects created via
    /// `Config::tmp`, and will remove the storage directory
    /// when the final Arc drops.
    pub tempdir_deleter: Option<Arc<TempDir>>,
    /// A float between 0.0 and 1.0 that controls how much fragmentation can
    /// exist in a file before GC attempts to recompact it.
    pub target_heap_file_fill_ratio: f32,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            path: "bloodstone.default".into(),
            flush_every_ms: Some(200),
            cache_capacity_bytes: 512 * 1024 * 1024,
            entry_cache_percent: 20,
            zstd_compression_level: 3,
            tempdir_deleter: None,
            target_heap_file_fill_ratio: 0.9,
        }
    }
}

impl Config {
    /// Returns a default `Config`
    pub fn new() -> Config {
        Config::default()
    }

    /// Returns a config with the `path` initialized to a system
    /// temporary directory that will be deleted when this `Config`
    /// is dropped.
    pub fn tmp() -> io::Result<Config> {
        let tempdir = fallible!(tempdir::TempDir::new("sled_tmp"));

        Ok(Config {
            path: tempdir.path().into(),
            tempdir_deleter: Some(Arc::new(tempdir)),
            ..Config::default()
        })
    }

    /// Set the path of the database (builder).
    pub fn path<P: AsRef<Path>>(mut self, path: P) -> Config {
        self.path = path.as_ref().to_path_buf();
        self
    }

    builder!(
        (flush_every_ms, Option<usize>, "Start a background thread that flushes data to disk every few milliseconds. Defaults to every 200ms."),
        (cache_capacity_bytes, usize, "Cache size in **bytes**. Default is 512mb."),
        (entry_cache_percent, u8, "The percentage of the cache that is dedicated to the scan-resistant entry cache."),
        (zstd_compression_level, i32, "The zstd compression level to use when writing data to disk. Defaults to 3.")
    );

    pub fn open<const LEAF_FANOUT: usize>(
        &self,
    ) -> io::Result<Db<LEAF_FANOUT>> {
        if LEAF_FANOUT < 3 {
            return Err(annotate!(io::Error::new(
                io::ErrorKind::Unsupported,
                "Db's LEAF_FANOUT const generic must be 3 or greater."
            )));
        }
        Db::open_with_config(self)
    }
}

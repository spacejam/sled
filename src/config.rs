use std::path::{Path, PathBuf};

use fault_injection::annotate;

use crate::Db;

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
}

impl Default for Config {
    fn default() -> Config {
        Config {
            path: "bloodstone.default".into(),
            flush_every_ms: Some(200),
            cache_capacity_bytes: 512 * 1024 * 1024,
            entry_cache_percent: 20,
            zstd_compression_level: 3,
        }
    }
}

impl Config {
    /// Returns a default `Config`
    pub fn new() -> Config {
        Config::default()
    }

    /// Returns a config with the `path` initialized to a system
    /// temporary directory that will be deleted when this process
    /// terminates.
    pub fn tmp() -> std::io::Result<Config> {
        thread_local! {
            static TMP_DIR: std::io::Result<tempdir::TempDir> =
                tempdir::TempDir::new("sled_tmp");
        }

        pub fn tmp_path() -> std::io::Result<std::path::PathBuf> {
            TMP_DIR.with(|t| match t.as_ref() {
                Ok(tmp) => Ok(tmp.path().into()),
                Err(e) => {
                    let e: std::io::Error = annotate!(*e);
                    Err(e)
                }
            })
        }

        Ok(Config { path: tmp_path()?, ..Config::default() })
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

    pub fn open<
        const INDEX_FANOUT: usize,
        const LEAF_FANOUT: usize,
        const EBR_LOCAL_GC_BUFFER_SIZE: usize,
    >(
        &self,
    ) -> std::io::Result<Db<INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>>
    {
        Db::open_with_config(self)
    }
}

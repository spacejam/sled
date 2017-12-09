use std::fs;
use std::cell::RefCell;
use std::ops::Deref;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use super::*;

/// Top-level configuration for the system.
///
/// # Examples
///
/// ```
/// let _config = sled::Config::default()
///     .path("/path/to/data".to_owned())
///     .cache_capacity(10_000_000_000)
///     .use_compression(true)
///     .flush_every_ms(Some(1000))
///     .snapshot_after_ops(100_000);
/// ```
///
/// Read-only mode
/// ```
/// let _config = sled::Config::default()
///     .path("/path/to/data".to_owned())
///     .read_only(true);
/// ```
#[derive(Debug, Clone)]
pub struct Config {
    io_bufs: usize,
    io_buf_size: usize,
    blink_fanout: usize,
    page_consolidation_threshold: usize,
    path: String,
    cache_bits: usize,
    cache_capacity: usize,
    use_os_cache: bool,
    use_compression: bool,
    flush_every_ms: Option<u64>,
    snapshot_after_ops: usize,
    snapshot_path: Option<String>,
    cache_fixup_threshold: usize,
    segment_cleanup_threshold: f64,
    min_free_segments: usize,
    zero_copy_storage: bool,
    tc: ThreadCache<fs::File>,
    tmp_path: String,
    read_only: bool,
    log_mode: SegmentMode,
}

unsafe impl Send for Config {}
unsafe impl Sync for Config {}

impl Default for Config {
    fn default() -> Config {
        let now = uptime();
        let nanos = (now.as_secs() * 1_000_000_000) +
            u64::from(now.subsec_nanos());

        // use shared memory for temporary linux files
        #[cfg(target_os = "linux")]
        let tmp_path = format!("/dev/shm/sled.tmp.{}", nanos);

        #[cfg(not(target_os = "linux"))]
        let tmp_path = format!("sled.tmp.{}", nanos);

        Config {
            io_bufs: 3,
            io_buf_size: 2 << 22, // 8mb
            blink_fanout: 32,
            page_consolidation_threshold: 10,
            path: tmp_path.to_owned(),
            read_only: false,
            cache_bits: 6, // 64 shards
            cache_capacity: 1024 * 1024 * 1024, // 1gb
            use_os_cache: true,
            use_compression: true,
            flush_every_ms: Some(500),
            snapshot_after_ops: 1_000_000,
            snapshot_path: None,
            cache_fixup_threshold: 1,
            segment_cleanup_threshold: 0.2,
            min_free_segments: 3,
            zero_copy_storage: false,
            tc: ThreadCache::default(),
            tmp_path: tmp_path.to_owned(),
            log_mode: SegmentMode::Gc,
        }
    }
}

macro_rules! builder {
    ($(($name:ident, $get:ident, $set:ident, $t:ty, $desc:expr)),*) => {
        $(
            #[doc="Get "]
            #[doc=$desc]
            pub fn $get(&self) -> $t {
                self.$name.clone()
            }

            #[doc="Set "]
            #[doc=$desc]
            pub fn $set(&mut self, to: $t) {
                self.$name = to;
            }

            #[doc="Builder, set "]
            #[doc=$desc]
            pub fn $name(&self, to: $t) -> Config {
                let mut ret = self.clone();
                ret.$name = to;
                ret
            }
        )*
    }
}

impl Config {
    builder!(
        (io_bufs, get_io_bufs, set_io_bufs, usize, "number of io buffers"),
        (io_buf_size, get_io_buf_size, set_io_buf_size, usize, "size of each io flush buffer. MUST be multiple of 512!"),
        (blink_fanout, get_blink_fanout, set_blink_fanout, usize, "b-link node fanout, minimum of 2"),
        (page_consolidation_threshold, get_page_consolidation_threshold, set_page_consolidation_threshold, usize, "page consolidation threshold"),
        (path, get_path, set_path, String, "path for the main storage file"),
        (read_only, get_read_only, set_read_only, bool, "whether to run in read-only mode"),
        (cache_bits, get_cache_bits, set_cache_bits, usize, "log base 2 of the number of cache shards"),
        (cache_capacity, get_cache_capacity, set_cache_capacity, usize, "maximum size for the system page cache"),
        (use_os_cache, get_use_os_cache, set_use_os_cache, bool, "whether to use the OS page cache"),
        (use_compression, get_use_compression, set_use_compression, bool, "whether to use zstd compression"),
        (flush_every_ms, get_flush_every_ms, set_flush_every_ms, Option<u64>, "number of ms between IO buffer flushes"),
        (snapshot_after_ops, get_snapshot_after_ops, set_snapshot_after_ops, usize, "number of operations between page table snapshots"),
        (snapshot_path, get_snapshot_path, set_snapshot_path, Option<String>, "snapshot file location"),
        (cache_fixup_threshold, get_cache_fixup_threshold, set_cache_fixup_threshold, usize, "the maximum length of a cached page fragment chain"),
        (segment_cleanup_threshold, get_segment_cleanup_threshold, set_segment_cleanup_threshold, f64, "the proportion of remaining valid pages in the segment"),
        (min_free_segments, get_min_free_segments, set_min_free_segments, usize, "the minimum number of free segments to have on-deck before a compaction occurs"),
        (zero_copy_storage, get_zero_copy_storage, set_zero_copy_storage, bool, "disabling of the log segment copy cleaner")
    );

    /// Retrieve a thread-local file handle to the
    /// configured underlying storage,
    /// or create a new one if this is the first time the
    /// thread is accessing it.
    pub fn cached_file(&self) -> Rc<RefCell<fs::File>> {
        self.tc.get_or_else(|| {
            let path = self.get_path();
            let mut options = fs::OpenOptions::new();
            options.create(true);
            options.read(true);
            options.write(true);
            options.open(path).unwrap()
        })
    }

    /// Get the temporary path of the database, used as temporary
    /// storage if none is provided.
    pub fn get_tmp_path(&self) -> String {
        self.tmp_path.clone()
    }

    /// returns the current snapshot file prefix
    pub fn snapshot_prefix(&self) -> String {
        let snapshot_path = self.get_snapshot_path();
        let path = self.get_path();
        snapshot_path.unwrap_or(path)
    }

    /// returns the snapshot file paths for this system
    pub fn get_snapshot_files(&self) -> Vec<String> {
        let mut prefix = self.snapshot_prefix();

        prefix.push_str(".");

        let err_msg = format!("could not read snapshot directory ({})", prefix);


        let abs_prefix: String = if Path::new(&prefix).is_absolute() {
            prefix
        } else {
            let mut abs_path = std::env::current_dir().expect(&*err_msg);
            abs_path.push(prefix.clone());
            abs_path.to_str().unwrap().to_owned()
        };

        let filter = |dir_entry: std::io::Result<std::fs::DirEntry>| {
            if let Ok(de) = dir_entry {
                let path_buf = de.path();
                let path = path_buf.as_path();
                let path_str = path.to_str().unwrap();
                if path_str.starts_with(&abs_prefix) &&
                    !path_str.ends_with(".in___motion")
                {
                    Some(path_str.to_owned())
                } else {
                    None
                }
            } else {
                None
            }
        };

        let snap_dir = Path::new(&abs_prefix).parent().expect(&*err_msg);

        if !snap_dir.exists() {
            std::fs::create_dir_all(snap_dir).unwrap();
        }

        snap_dir
            .read_dir()
            .expect(&*err_msg)
            .filter_map(filter)
            .collect()
    }

    /// Finalize the configuration.
    pub fn build(self) -> FinalConfig {
        FinalConfig(Arc::new(self))
    }

    /// Consumes the `Config` and produces a `Tree` from it.
    pub fn tree(self) -> Tree {
        self.build().tree()
    }

    /// Consumes the `Config` and produces a `Log` from it.
    pub fn log(self) -> Log {
        self.build().log()
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        let ephemeral = self.get_path() == self.get_tmp_path();
        if !ephemeral {
            return;
        }

        // Our files are temporary, so nuke them.
        let _res = fs::remove_file(self.tmp_path.clone());

        let candidates = self.get_snapshot_files();
        for path in candidates {
            if let Err(_e) = std::fs::remove_file(path) {
                warn!("failed to remove old snapshot file, maybe snapshot race? {}", _e);
            }
        }
    }
}

/// A finalized `Config` that can be use multiple times
/// to open a `Tree` or `Log`.
#[derive(Clone, Debug, Default)]
pub struct FinalConfig(Arc<Config>);

impl Deref for FinalConfig {
    type Target = Config;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl FinalConfig {
    /// Start a `Tree` using this finalized configuration.
    pub fn tree(&self) -> Tree {
        Tree::start(self.clone())
    }

    /// Start a `Log` using this finalized configuration.
    pub fn log(&self) -> Log {
        Log::start_raw_log(self.clone())
    }
}

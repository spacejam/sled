use std::fmt::Debug;
use std::fs;
use std::ffi::{OsStr, OsString};
use std::io::{Read, Seek, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use bincode::{Infinite, deserialize, serialize};

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
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Config {
    io_bufs: usize,
    io_buf_size: usize,
    min_items_per_segment: usize,
    blink_fanout: usize,
    page_consolidation_threshold: usize,
    path: OsString,
    cache_bits: usize,
    cache_capacity: usize,
    use_os_cache: bool,
    use_compression: bool,
    zstd_compression_factor: i32,
    flush_every_ms: Option<u64>,
    snapshot_after_ops: usize,
    snapshot_path: Option<OsString>,
    cache_fixup_threshold: usize,
    segment_cleanup_threshold: f64,
    min_free_segments: usize,
    zero_copy_storage: bool,
    tmp_path: OsString,
    temporary: bool,
    read_only: bool,
    pub(super) segment_mode: SegmentMode,
}

unsafe impl Send for Config {}

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
            min_items_per_segment: 4, // capacity for >=4 pages/segment
            blink_fanout: 32,
            page_consolidation_threshold: 10,
            path: "sled".to_owned().into(),
            read_only: false,
            cache_bits: 6, // 64 shards
            cache_capacity: 1024 * 1024 * 1024, // 1gb
            use_os_cache: true,
            use_compression: true,
            zstd_compression_factor: 5,
            flush_every_ms: Some(500),
            snapshot_after_ops: 1_000_000,
            snapshot_path: None,
            cache_fixup_threshold: 1,
            segment_cleanup_threshold: 0.2,
            min_free_segments: 3,
            zero_copy_storage: false,
            tmp_path: tmp_path.to_owned().into(),
            temporary: false,
            segment_mode: SegmentMode::Gc,
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
            pub fn $name(mut self, to: $t) -> Config {
                self.$name = to;
                self
            }
        )*
    }
}

impl Config {
    builder!(
        (io_bufs, get_io_bufs, set_io_bufs, usize, "number of io buffers"),
        (io_buf_size, get_io_buf_size, set_io_buf_size, usize, "size of each io flush buffer. MUST be multiple of 512!"),
        (min_items_per_segment, get_min_items_per_segment, set_min_items_per_segment, usize, "minimum data chunks/pages in a segment."),
        (blink_fanout, get_blink_fanout, set_blink_fanout, usize, "b-link node fanout, minimum of 2"),
        (page_consolidation_threshold, get_page_consolidation_threshold, set_page_consolidation_threshold, usize, "page consolidation threshold"),
        (temporary, get_temporary, set_temporary, bool, "if this database should be removed after the Config is dropped"),
        (read_only, get_read_only, set_read_only, bool, "whether to run in read-only mode"),
        (cache_bits, get_cache_bits, set_cache_bits, usize, "log base 2 of the number of cache shards"),
        (cache_capacity, get_cache_capacity, set_cache_capacity, usize, "maximum size for the system page cache"),
        (use_os_cache, get_use_os_cache, set_use_os_cache, bool, "whether to use the OS page cache"),
        (use_compression, get_use_compression, set_use_compression, bool, "whether to use zstd compression"),
        (zstd_compression_factor, get_zstd_compression_factor, set_zstd_compression_factor, i32, "the compression factor to use with zstd compression"),
        (flush_every_ms, get_flush_every_ms, set_flush_every_ms, Option<u64>, "number of ms between IO buffer flushes"),
        (snapshot_after_ops, get_snapshot_after_ops, set_snapshot_after_ops, usize, "number of operations between page table snapshots"),
        (cache_fixup_threshold, get_cache_fixup_threshold, set_cache_fixup_threshold, usize, "the maximum length of a cached page fragment chain"),
        (segment_cleanup_threshold, get_segment_cleanup_threshold, set_segment_cleanup_threshold, f64, "the proportion of remaining valid pages in the segment"),
        (min_free_segments, get_min_free_segments, set_min_free_segments, usize, "the minimum number of free segments to have on-deck before a compaction occurs"),
        (zero_copy_storage, get_zero_copy_storage, set_zero_copy_storage, bool, "disabling of the log segment copy cleaner"),
        (segment_mode, get_segment_mode, set_segment_mode, SegmentMode, "the file segment selection mode"),
        (snapshot_path, get_snapshot_path, set_snapshot_path, Option<OsString>, "snapshot file location")
    );

    /// Returns a default `Config`
    pub fn new() -> Config {
        Self::default()
    }

    /// Set the path of the database
    pub fn path<P: AsRef<Path>>(mut self, path: P) -> Config {
        let path_ref: &Path = path.as_ref();
        let os_str_ref: &OsStr = path_ref.as_ref();

        self.path = os_str_ref.to_os_string();
        self
    }

    /// Get the path of the database
    pub fn get_path(&self) -> OsString {
        if self.temporary {
            self.tmp_path.clone()
        } else {
            self.path.clone()
        }
    }

    /// returns the current snapshot file prefix
    pub fn snapshot_prefix(&self) -> OsString {
        let snapshot_path = self.get_snapshot_path();
        let path = self.get_path().as_os_str().to_os_string();

        snapshot_path
            .map(|sp| sp.as_os_str().to_os_string())
            .unwrap_or(path)
    }

    /// returns the snapshot file paths for this system
    pub fn get_snapshot_files(&self) -> Vec<PathBuf> {
        let mut prefix = self.snapshot_prefix();

        prefix.push(".snap.");

        let err_msg = format!("could not read snapshot directory ({})", prefix.to_string_lossy());

        let abs_prefix: OsString = if Path::new(&prefix).is_absolute() {
            prefix
        } else {
            let mut abs_path = std::env::current_dir().expect(&*err_msg);
            abs_path.push(prefix.clone());
            abs_path.as_os_str().to_os_string()
        };

        let filter = |dir_entry: std::io::Result<std::fs::DirEntry>| {
            if let Ok(de) = dir_entry {
                let path_buf = de.path();
                let path = path_buf.as_path();
                let path_str = &*path.to_string_lossy();
                if path_str.starts_with(&*abs_prefix.to_string_lossy()) &&
                    !path_str.ends_with(".in___motion")
                {
                    Some(path.to_path_buf())
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
        self.validate();

        let path = self.db_path();

        // panic if we can't parse the path
        let dir = Path::new(&path).parent().expect(
            "could not parse provided path",
        );

        // create data directory if it doesn't exist yet
        if dir != Path::new("") {
            if dir.is_file() {
                panic!(
                    "provided parent directory is a file, \
                    not a directory: {:?}",
                    dir
                );
            }

            if !dir.exists() {
                std::fs::create_dir_all(dir).unwrap();
            }
        }

        self.verify_conf_changes_ok();

        // open the data file
        let mut options = fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);
        let file = options.open(&path).unwrap();

        // seal config in a FinalConfig
        FinalConfig {
            inner: Arc::new(self),
            file: Arc::new(file),
        }
    }

    /// Consumes the `Config` and produces a `Tree` from it.
    pub fn tree(self) -> Tree {
        self.build().tree()
    }

    /// Consumes the `Config` and produces a `Log` from it.
    pub fn log(mut self) -> Log {
        self.segment_mode = SegmentMode::Linear;
        Log::start_raw_log(self.build())
    }

    // panics if conf options are outside of advised range
    fn validate(&self) {
        assert!(self.io_bufs <= 32, "too many configured io_bufs");
        assert!(self.io_buf_size >= 1000, "io_buf_size too small");
        assert!(self.io_buf_size <= 1 << 24, "io_buf_size should be <= 16mb");
        assert!(self.min_items_per_segment >= 4);
        assert!(self.min_items_per_segment < 128);
        assert!(self.blink_fanout >= 2, "tree nodes must have at least 2 children");
        assert!(self.blink_fanout < 1024, "tree nodes should not have so many children");
        assert!(self.page_consolidation_threshold >= 1, "must consolidate pages after a non-zero number of updates");
        assert!(self.page_consolidation_threshold < 1 << 20, "must consolidate pages after fewer than 1 million updates");
        assert!(self.cache_bits <= 20, "# LRU shards = 2^cache_bits. set this to 20 or less.");
        assert!(self.min_free_segments <= 32, "min_free_segments need not be higher than the number IO buffers (io_bufs)");
        assert!(self.min_free_segments >= 1, "min_free_segments must be nonzero or the database will never reclaim storage");
        assert!(self.cache_fixup_threshold >= 1, "cache_fixup_threshold must be nonzero.");
        assert!(self.cache_fixup_threshold < 1 << 20, "cache_fixup_threshold must be fewer than 1 million updates.");
        assert!(self.segment_cleanup_threshold >= 0.01, "segment_cleanup_threshold must be >= 1%");
        assert!(self.zstd_compression_factor >= 1);
        assert!(self.zstd_compression_factor <= 22);
    }

    fn verify_conf_changes_ok(&self) {
        if let Ok(Some(mut old)) = self.read_config() {
            old.tmp_path = self.tmp_path.clone();
            assert_eq!(self, &old, "changing the configuration \
                       between usages is currently unsupported");
        } else {
            self.write_config().expect(
                "unable to open file for writing",
            );
        }
    }

    fn write_config(&self) -> std::io::Result<()> {
        let bytes = serialize(&self, Infinite).unwrap();
        let crc64: [u8; 8] = unsafe { std::mem::transmute(crc64(&*bytes)) };

        let path = self.conf_path();

        let mut f = std::fs::OpenOptions::new().write(true).create(true).open(
            path,
        )?;

        f.write_all(&*bytes)?;
        f.write_all(&crc64)?;
        f.sync_all()
    }

    fn read_config(&self) -> std::io::Result<Option<Config>> {
        let path = self.conf_path();

        let mut f = std::fs::OpenOptions::new().read(true).open(&path)?;
        if f.metadata().unwrap().len() <= 8 {
            warn!("empty/corrupt configuration file found");
            return Ok(None);
        }

        let mut buf = vec![];
        f.read_to_end(&mut buf).unwrap();
        let len = buf.len();
        buf.split_off(len - 8);

        let mut crc_expected_bytes = [0u8; 8];
        f.seek(std::io::SeekFrom::End(-8)).unwrap();
        f.read_exact(&mut crc_expected_bytes).unwrap();
        let crc_expected: u64 =
            unsafe { std::mem::transmute(crc_expected_bytes) };

        let crc_actual = crc64(&*buf);

        if crc_expected != crc_actual {
            warn!("crc for settings file {:?} failed! can't verify that config is safe", path);
        }

        Ok(deserialize::<Config>(&*buf).ok())
    }

    fn db_path(&self) -> OsString {
        let mut path = self.get_path();
        path.push(".db");
        path
    }

    fn conf_path(&self) -> OsString {
        let mut path = self.get_path();
        path.push(".conf");
        path
    }
}

impl Drop for FinalConfig {
    fn drop(&mut self) {
        if !self.get_temporary() {
            return;
        }

        // Our files are temporary, so nuke them.
        warn!("removing ephemeral storage file {}", self.tmp_path.to_string_lossy());

        let db_path = self.db_path();
        let conf_path = self.conf_path();

        let _res = fs::remove_file(db_path);
        let _res = fs::remove_file(conf_path);

        let candidates = self.get_snapshot_files();
        for path in candidates {
            warn!("removing old snapshot file {}", path.to_string_lossy());
            if let Err(_e) = std::fs::remove_file(path) {
                error!("failed to remove old snapshot file, maybe snapshot race? {}", _e);
            }
        }
    }
}

/// A finalized `Config` that can be use multiple times
/// to open a `Tree` or `Log`.
#[derive(Clone, Debug)]
pub struct FinalConfig {
    inner: Arc<Config>,
    file: Arc<fs::File>,
}

unsafe impl Send for FinalConfig {}
unsafe impl Sync for FinalConfig {}

impl Deref for FinalConfig {
    type Target = Config;

    fn deref(&self) -> &Self::Target {
        &*self.inner
    }
}

impl FinalConfig {
    /// Start a `Tree` using this finalized configuration.
    pub fn tree(&self) -> Tree {
        Tree::start(self.clone())
    }

    /// Start a `Log` using this finalized configuration.
    pub fn log(&self) -> Log {
        assert_eq!(self.inner.segment_mode, SegmentMode::Linear, "must use SegmentMode::Linear with log!");
        Log::start_raw_log(self.clone())
    }

    /// Retrieve a thread-local file handle to the
    /// configured underlying storage,
    /// or create a new one if this is the first time the
    /// thread is accessing it.
    pub fn file(&self) -> Arc<fs::File> {
        self.file.clone()
    }

    #[doc(hidden)]
    pub fn verify_snapshot<PM, P, R>(&self)
        where PM: Materializer<Recovery = R, PageFrag = P>,
              P: 'static
                     + Debug
                     + Clone
                     + Serialize
                     + DeserializeOwned
                     + Send
                     + Sync,
              R: Debug + Clone + Serialize + DeserializeOwned + Send + PartialEq
    {
        let incremental = read_snapshot_or_default::<PM, P, R>(&self);

        for snapshot_path in self.get_snapshot_files() {
            std::fs::remove_file(snapshot_path).unwrap();
        }

        let regenerated = read_snapshot_or_default::<PM, P, R>(&self);

        for (k, v) in &regenerated.pt {
            if !incremental.pt.contains_key(&k) {
                panic!("page only present in regenerated pagetable: {} -> {:?}", k, v);
            }
            assert_eq!(incremental.pt.get(&k), Some(v), "page tables differ for pid {}", k);
        }

        for (k, v) in &incremental.pt {
            if !regenerated.pt.contains_key(&k) {
                panic!("page only present in incremental pagetable: {} -> {:?}", k, v);
            }
            assert_eq!(Some(v), regenerated.pt.get(&k), "page tables differ for pid {}", k);
        }

        assert_eq!(incremental.pt, regenerated.pt, "snapshot pagetable diverged");
        assert_eq!(incremental.max_pid, regenerated.max_pid, "snapshot max_pid diverged");
        assert_eq!(incremental.max_lsn, regenerated.max_lsn, "snapshot max_lsn diverged");
        assert_eq!(incremental.last_lid, regenerated.last_lid, "snapshot last_lid diverged");
        assert_eq!(incremental.free, regenerated.free, "snapshot free list diverged");
        assert_eq!(incremental.recovery, regenerated.recovery, "snapshot recovery diverged");

        /*
        for (k, v) in &regenerated.replacements {
            if !incremental.replacements.contains_key(&k) {
                panic!("page only present in regenerated replacement map: {}", k);
            }
            assert_eq!(
                Some(v), 
                incremental.replacements.get(&k),
                "replacement tables differ for pid {}",
                k
            );
        }

        for (k, v) in &incremental.replacements {
            if !regenerated.replacements.contains_key(&k) {
                panic!("page only present in incremental replacement map: {}", k);
            }
            assert_eq!(
                Some(v),
                regenerated.replacements.get(&k),
                "replacement tables differ for pid {}", 
                k,
            );
        }

        assert_eq!(
            incremental,
            regenerated,
            "snapshots have diverged!"
        );
        */
    }
}

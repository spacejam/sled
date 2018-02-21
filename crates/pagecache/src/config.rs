use std::fmt::Debug;
use std::fs;
use std::ops::Deref;
use std::ffi::{OsStr, OsString};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use serde::Serialize;
use serde::de::DeserializeOwned;

use bincode::{Infinite, deserialize, serialize};

use super::*;
use io::LogReader;

impl Deref for Config {
    type Target = ConfigBuilder;
    fn deref(&self) -> &Self::Target {
        &*self.inner
    }
}

/// Top-level configuration for the system.
///
/// # Examples
///
/// ```
/// let _config = pagecache::ConfigBuilder::default()
///     .path("/path/to/data".to_owned())
///     .cache_capacity(10_000_000_000)
///     .use_compression(true)
///     .flush_every_ms(Some(1000))
///     .snapshot_after_ops(100_000);
/// ```
///
/// Read-only mode
/// ```
/// let _config = pagecache::ConfigBuilder::default()
///     .path("/path/to/data".to_owned())
///     .read_only(true);
/// ```
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ConfigBuilder {
    #[doc(hidden)]
    pub blink_fanout: usize,
    #[doc(hidden)]
    pub cache_bits: usize,
    #[doc(hidden)]
    pub cache_capacity: usize,
    #[doc(hidden)]
    pub cache_fixup_threshold: usize,
    #[doc(hidden)]
    pub flush_every_ms: Option<u64>,
    #[doc(hidden)]
    pub io_bufs: usize,
    #[doc(hidden)]
    pub io_buf_size: usize,
    #[doc(hidden)]
    pub min_free_segments: usize,
    #[doc(hidden)]
    pub min_items_per_segment: usize,
    #[doc(hidden)]
    pub page_consolidation_threshold: usize,
    #[doc(hidden)]
    pub path: OsString,
    #[doc(hidden)]
    pub read_only: bool,
    #[doc(hidden)]
    pub segment_cleanup_threshold: f64,
    #[doc(hidden)]
    pub segment_mode: SegmentMode,
    #[doc(hidden)]
    pub snapshot_after_ops: usize,
    #[doc(hidden)]
    pub snapshot_path: Option<OsString>,
    #[doc(hidden)]
    pub temporary: bool,
    #[doc(hidden)]
    pub tmp_path: OsString,
    #[doc(hidden)]
    pub use_compression: bool,
    #[doc(hidden)]
    pub use_os_cache: bool,
    #[doc(hidden)]
    pub zero_copy_storage: bool,
    #[doc(hidden)]
    pub zstd_compression_factor: i32,
}

unsafe impl Send for ConfigBuilder {}

impl Default for ConfigBuilder {
    fn default() -> ConfigBuilder {
        let now = uptime();
        let nanos = (now.as_secs() * 1_000_000_000) +
            u64::from(now.subsec_nanos());

        // use shared memory for temporary linux files
        #[cfg(target_os = "linux")]
        let tmp_path = format!("/dev/shm/pagecache.tmp.{}", nanos);

        #[cfg(not(target_os = "linux"))]
        let tmp_path = format!("pagecache.tmp.{}", nanos);

        ConfigBuilder {
            io_bufs: 3,
            io_buf_size: 2 << 22, // 8mb
            min_items_per_segment: 4, // capacity for >=4 pages/segment
            blink_fanout: 32,
            page_consolidation_threshold: 10,
            path: "pagecache".to_owned().into(),
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
macro_rules! supported {
    ($cond:expr, $msg:expr) => {
        if !$cond {
            return Err(Error::Unsupported($msg.to_owned()));
        }
    }
}

macro_rules! builder {
    ($(($name:ident, $get:ident, $set:ident, $t:ty, $desc:expr)),*) => {
        $(
            #[doc=$desc]
            pub fn $set(&mut self, to: $t) {
                self.$name = to;
            }

            #[doc=$desc]
            pub fn $name(mut self, to: $t) -> ConfigBuilder {
                self.$name = to;
                self
            }
        )*
    }
}

impl ConfigBuilder {
    /// Returns a default `ConfigBuilder`
    pub fn new() -> ConfigBuilder {
        Self::default()
    }

    /// Set the path of the database (builder).
    pub fn path<P: AsRef<Path>>(mut self, path: P) -> ConfigBuilder {
        let path_ref: &Path = path.as_ref();
        let os_str_ref: &OsStr = path_ref.as_ref();

        self.path = os_str_ref.to_os_string();
        self
    }

    /// Set the path of the database
    pub fn set_path<P: AsRef<Path>>(&mut self, path: P) {
        let path_ref: &Path = path.as_ref();
        let os_str_ref: &OsStr = path_ref.as_ref();

        self.path = os_str_ref.to_os_string();
    }

    /// Finalize the configuration.
    pub fn build(self) -> Config {
        // seal config in a Config
        Config {
            inner: Arc::new(self),
            file: Arc::new(AtomicPtr::default()),
            build_locker: Arc::new(Mutex::new(())),
            refs: Arc::new(AtomicUsize::new(0)),
        }
    }

    builder!(
        (io_bufs, get_io_bufs, set_io_bufs, usize, "number of io buffers"),
        (io_buf_size, get_io_buf_size, set_io_buf_size, usize, "size of each io flush buffer. MUST be multiple of 512!"),
        (min_items_per_segment, get_min_items_per_segment, set_min_items_per_segment, usize, "minimum data chunks/pages in a segment."),
        (blink_fanout, get_blink_fanout, set_blink_fanout, usize, "b-link node fanout, minimum of 2"),
        (page_consolidation_threshold, get_page_consolidation_threshold, set_page_consolidation_threshold, usize, "page consolidation threshold"),
        (temporary, get_temporary, set_temporary, bool, "if this database should be removed after the ConfigBuilder is dropped"),
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
}

/// A finalized `ConfigBuilder` that can be use multiple times
/// to open a `Tree` or `Log`.
#[derive(Debug)]
pub struct Config {
    inner: Arc<ConfigBuilder>,
    file: Arc<AtomicPtr<Arc<fs::File>>>,
    build_locker: Arc<Mutex<()>>,
    refs: Arc<AtomicUsize>,
}

unsafe impl Send for Config {}
unsafe impl Sync for Config {}

impl Clone for Config {
    fn clone(&self) -> Config {
        self.refs.fetch_add(1, Ordering::Relaxed);
        Config {
            inner: self.inner.clone(),
            file: self.file.clone(),
            build_locker: self.build_locker.clone(),
            refs: self.refs.clone(),
        }
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        // if our ref count is 0 we can drop and close our file properly.
        if self.refs.fetch_sub(1, Ordering::Relaxed) == 0 {
            let f_ptr: *mut Arc<fs::File> =
                self.file.swap(std::ptr::null_mut(), Ordering::Relaxed);
            if !f_ptr.is_null() {
                let f: Box<Arc<fs::File>> = unsafe { Box::from_raw(f_ptr) };
                drop(f);
            }
        }

        if !self.temporary {
            return;
        }

        // Our files are temporary, so nuke them.
        warn!("removing ephemeral storage file {}", self.inner.tmp_path.to_string_lossy());

        let db_path = self.db_path();
        let conf_path = self.conf_path();

        let _res = fs::remove_file(db_path);
        let _res = fs::remove_file(conf_path);

        let candidates = self.get_snapshot_files().unwrap();
        for path in candidates {
            warn!("removing old snapshot file {}", path.to_string_lossy());
            if let Err(_e) = std::fs::remove_file(path) {
                error!("failed to remove old snapshot file, maybe snapshot race? {}", _e);
            }
        }
    }
}

impl Config {
    // Retrieve a thread-local file handle to the
    // configured underlying storage,
    // or create a new one if this is the first time the
    // thread is accessing it.
    #[doc(hidden)]
    pub fn file(&self) -> CacheResult<Arc<fs::File>, ()> {
        if self.file.load(Ordering::Relaxed).is_null() {
            let _lock = self.build_locker.lock().unwrap();
            if self.file.load(Ordering::Relaxed).is_null() {
                self.initialize()?;
            }
        }

        Ok(unsafe { (*self.file.load(Ordering::Relaxed)).clone() })
    }

    // Get the path of the database
    #[doc(hidden)]
    pub fn get_path(&self) -> OsString {
        if self.inner.temporary {
            self.inner.tmp_path.clone()
        } else {
            self.inner.path.clone()
        }
    }

    // returns the current snapshot file prefix
    #[doc(hidden)]
    pub fn snapshot_prefix(&self) -> OsString {
        let snapshot_path = self.snapshot_path.clone();
        let path = self.get_path().as_os_str().to_os_string();

        snapshot_path
            .map(|sp| sp.as_os_str().to_os_string())
            .unwrap_or(path)
    }

    // returns the snapshot file paths for this system
    #[doc(hidden)]
    pub fn get_snapshot_files(&self) -> std::io::Result<Vec<PathBuf>> {
        let mut prefix = self.snapshot_prefix();

        prefix.push(".snap.");

        let abs_prefix: OsString = if Path::new(&prefix).is_absolute() {
            prefix
        } else {
            let mut abs_path = std::env::current_dir()?;
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

        let snap_dir = Path::new(&abs_prefix).parent().unwrap();

        if !snap_dir.exists() {
            std::fs::create_dir_all(snap_dir)?;
        }

        Ok(snap_dir.read_dir()?.filter_map(filter).collect())
    }

    fn initialize(&self) -> CacheResult<(), ()> {
        // only validate, setup directory, and open file once
        self.validate()?;

        let path = self.db_path();

        // panic if we can't parse the path
        let dir = match Path::new(&path).parent() {
            None => {
                return Err(Error::Unsupported(
                    format!("could not determine parent directory of {:?}", path),
                ));
            }
            Some(dir) => dir,
        };

        // create data directory if it doesn't exist yet
        if dir != Path::new("") {
            if dir.is_file() {
                return Err(Error::Unsupported(
                    format!("provided parent directory is a file, \
                                not a directory: {:?}",
                        dir),
                ));
            }

            if !dir.exists() {
                let res: std::io::Result<()> = std::fs::create_dir_all(dir);
                res.map_err(|e: std::io::Error| {
                    let ret: Error<()> = e.into();
                    ret
                })?;
            }
        }

        self.verify_conf_changes_ok()?;

        // open the data file
        let mut options = fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);

        match options.open(&path) {
            Ok(file) => {
                // turn file into a raw pointer for future use
                let file_ptr = Box::into_raw(Box::new(Arc::new(file)));
                self.file.store(file_ptr, Ordering::SeqCst);
            }
            Err(e) => {
                return Err(e.into());
            }
        }
        Ok(())
    }

    // panics if conf options are outside of advised range
    fn validate(&self) -> CacheResult<(), ()> {
        supported!(self.inner.io_bufs <= 32, "too many configured io_bufs");
        supported!(self.inner.io_buf_size >= 100, "io_buf_size should be hundreds of kb at minimum");
        supported!(self.inner.io_buf_size <= 1 << 24, "io_buf_size should be <= 16mb");
        supported!(self.inner.min_items_per_segment >= 1, "min_items_per_segment must be >= 4");
        supported!(self.inner.min_items_per_segment < 128, "min_items_per_segment must be < 128");
        supported!(self.inner.blink_fanout >= 2, "tree nodes must have at least 2 children");
        supported!(self.inner.blink_fanout < 1024, "tree nodes should not have so many children");
        supported!(self.inner.page_consolidation_threshold >= 1, "must consolidate pages after a non-zero number of updates");
        supported!(self.inner.page_consolidation_threshold < 1 << 20, "must consolidate pages after fewer than 1 million updates");
        supported!(self.inner.cache_bits <= 20, "# LRU shards = 2^cache_bits. set this to 20 or less.");
        supported!(self.inner.min_free_segments <= 32, "min_free_segments need not be higher than the number IO buffers (io_bufs)");
        supported!(self.inner.min_free_segments >= 1, "min_free_segments must be nonzero or the database will never reclaim storage");
        supported!(self.inner.cache_fixup_threshold >= 1, "cache_fixup_threshold must be nonzero.");
        supported!(self.inner.cache_fixup_threshold < 1 << 20, "cache_fixup_threshold must be fewer than 1 million updates.");
        supported!(self.inner.segment_cleanup_threshold >= 0.01, "segment_cleanup_threshold must be >= 1%");
        supported!(self.inner.zstd_compression_factor >= 1, "compression factor must be >= 0");
        supported!(self.inner.zstd_compression_factor <= 22, "compression factor must be <= 22");
        Ok(())
    }

    fn verify_conf_changes_ok(&self) -> CacheResult<(), ()> {
        match self.read_config() {
            Ok(Some(mut old)) => {
                let old_tmp = old.tmp_path;
                old.tmp_path = self.inner.tmp_path.clone();
                supported!(&*self.inner == &old, "changing the configuration \
                       between usages is currently unsupported");
                // need to keep the old path so that when old gets
                // dropped we don't remove our tmp_path (but it
                // might not matter even if we did, since it just
                // becomes anonymous as long as we keep a reference
                // open to it in the Config)
                old.tmp_path = old_tmp;
                Ok(())
            }
            Ok(None) => self.write_config().map_err(|e| e.into()),
            Err(e) => Err(e.into()),
        }
    }

    fn write_config(&self) -> CacheResult<(), ()> {
        let bytes = serialize(&*self.inner, Infinite).unwrap();
        let crc64: [u8; 8] = unsafe { std::mem::transmute(crc64(&*bytes)) };

        let path = self.conf_path();

        let mut f = std::fs::OpenOptions::new().write(true).create(true).open(
            path,
        )?;

        maybe_fail!("write_config bytes");
        f.write_all(&*bytes)?;
        maybe_fail!("write_config crc");
        f.write_all(&crc64)?;
        f.sync_all()?;
        maybe_fail!("write_config post");
        Ok(())
    }

    fn read_config(&self) -> std::io::Result<Option<ConfigBuilder>> {
        let path = self.conf_path();

        let f_res = std::fs::OpenOptions::new().read(true).open(&path);

        let mut f = match f_res {
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(other) => {
                return Err(other);
            }
            Ok(f) => f,
        };

        if f.metadata()?.len() <= 8 {
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

        Ok(deserialize::<ConfigBuilder>(&*buf).ok())
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

    #[doc(hidden)]
    pub fn verify_snapshot<PM, P, R>(&self) -> CacheResult<(), ()>
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
        let incremental = read_snapshot_or_default::<PM, P, R>(&self)?;

        for snapshot_path in self.get_snapshot_files()? {
            std::fs::remove_file(snapshot_path)?;
        }

        let regenerated = read_snapshot_or_default::<PM, P, R>(&self)?;

        let f = self.file()?;

        for (k, v) in &regenerated.pt {
            if !incremental.pt.contains_key(&k) {
                panic!("page only present in regenerated pagetable: {} -> {:?}", k, v);
            }
            assert_eq!(incremental.pt.get(&k), Some(v), "page tables differ for pid {}", k);
            for (lsn, lid) in v.iter() {
                f.read_message(
                    lid,
                    self.io_buf_size,
                    self.use_compression
                ).unwrap()
                .expect(&*format!("could not read log data for pid {} at lsn {} lid {}", k, lsn, lid));
            }
        }

        for (k, v) in &incremental.pt {
            if !regenerated.pt.contains_key(&k) {
                panic!("page only present in incremental pagetable: {} -> {:?}", k, v);
            }
            assert_eq!(Some(v), regenerated.pt.get(&k), "page tables differ for pid {}", k);
            for (lsn, lid) in v.iter() {
                f.read_message(
                    lid,
                    self.io_buf_size,
                    self.use_compression
                ).unwrap()
                .expect(&*format!("could not read log data for pid {} at lsn {} lid {}", k, lsn, lid));
            }
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
        Ok(())
    }
}

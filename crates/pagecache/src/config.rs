use std::{
    fmt::Debug,
    fs,
    io::{Read, Seek, Write},
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        atomic::{
            AtomicPtr, AtomicUsize, Ordering, ATOMIC_USIZE_INIT,
        },
        Arc, Mutex,
    },
};

use bincode::{deserialize, serialize};
use fs2::FileExt;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::*;

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
/// ```
/// // Read-only mode
/// let _config = pagecache::ConfigBuilder::default()
///     .path("/path/to/data".to_owned())
///     .read_only(true);
/// ```
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ConfigBuilder {
    #[doc(hidden)]
    pub blink_node_split_size: usize,
    #[doc(hidden)]
    pub cache_bits: usize,
    #[doc(hidden)]
    pub cache_capacity: usize,
    #[doc(hidden)]
    pub flush_every_ms: Option<u64>,
    #[doc(hidden)]
    pub io_bufs: usize,
    #[doc(hidden)]
    pub io_buf_size: usize,
    #[doc(hidden)]
    pub page_consolidation_threshold: usize,
    #[doc(hidden)]
    pub path: PathBuf,
    #[doc(hidden)]
    pub read_only: bool,
    #[doc(hidden)]
    pub segment_cleanup_threshold: f64,
    #[doc(hidden)]
    pub segment_cleanup_skew: usize,
    #[doc(hidden)]
    pub segment_mode: SegmentMode,
    #[doc(hidden)]
    pub snapshot_after_ops: usize,
    #[doc(hidden)]
    pub snapshot_path: Option<PathBuf>,
    #[doc(hidden)]
    pub temporary: bool,
    #[doc(hidden)]
    pub tmp_path: PathBuf,
    #[doc(hidden)]
    pub use_compression: bool,
    #[doc(hidden)]
    pub zstd_compression_factor: i32,
    #[doc(hidden)]
    pub merge_operator: Option<usize>,
    #[doc(hidden)]
    pub print_profile_on_drop: bool,
}

unsafe impl Send for ConfigBuilder {}

impl Default for ConfigBuilder {
    fn default() -> ConfigBuilder {
        #[cfg(unix)]
        let salt = {
            static SALT_COUNTER: AtomicUsize = ATOMIC_USIZE_INIT;
            let pid = unsafe { libc::getpid() };
            ((pid as u64) << 32)
                + SALT_COUNTER.fetch_add(1, Ordering::SeqCst) as u64
        };

        #[cfg(not(unix))]
        let salt = {
            let now = uptime();
            (now.as_secs() * 1_000_000_000)
                + now.subsec_nanos() as u64
        };

        // use shared memory for temporary linux files
        #[cfg(target_os = "linux")]
        let tmp_path = format!("/dev/shm/pagecache.tmp.{}", salt);

        #[cfg(not(target_os = "linux"))]
        let tmp_path = format!("/tmp/pagecache.tmp.{}", salt);

        ConfigBuilder {
            io_bufs: 3,
            io_buf_size: 2 << 22, // 8mb
            blink_node_split_size: 4096,
            page_consolidation_threshold: 10,
            path: PathBuf::from("default.sled"),
            read_only: false,
            cache_bits: 6, // 64 shards
            cache_capacity: 1024 * 1024 * 1024, // 1gb
            use_compression: true,
            zstd_compression_factor: 5,
            flush_every_ms: Some(500),
            snapshot_after_ops: 1_000_000,
            snapshot_path: None,
            segment_cleanup_threshold: 0.2,
            segment_cleanup_skew: 10,
            tmp_path: PathBuf::from(tmp_path),
            temporary: false,
            segment_mode: SegmentMode::Gc,
            merge_operator: None,
            print_profile_on_drop: false,
        }
    }
}
macro_rules! supported {
    ($cond:expr, $msg:expr) => {
        if !$cond {
            return Err(Error::Unsupported($msg.to_owned()));
        }
    };
}

macro_rules! builder {
    ($(($name:ident, $t:ty, $desc:expr)),*) => {
        $(
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
        self.path = path.as_ref().to_path_buf();
        self
    }

    /// Set the merge operator that can be relied on during merges in
    /// the `PageCache`.
    pub fn merge_operator(
        mut self,
        mo: MergeOperator,
    ) -> ConfigBuilder {
        self.merge_operator = Some(mo as usize);
        self
    }

    /// Finalize the configuration.
    pub fn build(self) -> Config {
        // seal config in a Config
        Config {
            inner: Arc::new(self),
            file: Arc::new(AtomicPtr::default()),
            build_locker: Arc::new(Mutex::new(())),
            refs: Arc::new(AtomicUsize::new(0)),
            #[cfg(feature = "event_log")]
            events: Arc::new(crate::event_log::EventLog::default()),
        }
    }

    builder!(
        (io_bufs, usize, "number of io buffers"),
        (io_buf_size, usize, "size of each io flush buffer. MUST be multiple of 512!"),
        (blink_node_split_size, usize, "b-link tree node size in bytes before splitting"),
        (page_consolidation_threshold, usize, "page consolidation threshold"),
        (temporary, bool, "if this database should be removed after the ConfigBuilder is dropped"),
        (read_only, bool, "whether to run in read-only mode"),
        (cache_bits, usize, "log base 2 of the number of cache shards"),
        (cache_capacity, usize, "maximum size for the system page cache"),
        (use_compression, bool, "whether to use zstd compression"),
        (zstd_compression_factor, i32, "the compression factor to use with zstd compression"),
        (flush_every_ms, Option<u64>, "number of ms between IO buffer flushes"),
        (snapshot_after_ops, usize, "number of operations between page table snapshots"),
        (segment_cleanup_threshold, f64, "the proportion of remaining valid pages in the segment"),
        (segment_cleanup_skew, usize, "the cleanup threshold skew in percentage points between the first and last segments"),
        (segment_mode, SegmentMode, "the file segment selection mode"),
        (snapshot_path, Option<PathBuf>, "snapshot file location"),
        (print_profile_on_drop, bool, "print a performance profile when the Config is dropped")
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
    #[cfg(feature = "event_log")]
    events: Arc<event_log::EventLog>,
}

unsafe impl Send for Config {}
unsafe impl Sync for Config {}

impl Clone for Config {
    fn clone(&self) -> Config {
        self.refs.fetch_add(1, Ordering::SeqCst);
        Config {
            inner: self.inner.clone(),
            file: self.file.clone(),
            build_locker: self.build_locker.clone(),
            refs: self.refs.clone(),
            #[cfg(feature = "event_log")]
            events: self.events.clone(),
        }
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        // if our ref count is 0 we can drop and close our file properly.
        if self.refs.fetch_sub(1, Ordering::SeqCst) == 0 {
            let f_ptr: *mut Arc<fs::File> = self
                .file
                .swap(std::ptr::null_mut(), Ordering::Relaxed);
            if !f_ptr.is_null() {
                let f: Box<Arc<fs::File>> =
                    unsafe { Box::from_raw(f_ptr) };
                drop(f);
            }

            if self.print_profile_on_drop {
                M.print_profile();
            }

            if !self.temporary {
                return;
            }

            // Our files are temporary, so nuke them.
            warn!(
                "removing ephemeral storage file {}",
                self.inner.tmp_path.to_string_lossy()
            );
            let _res = fs::remove_dir_all(&self.tmp_path);
        }
    }
}

impl Config {
    // Retrieve a thread-local file handle to the
    // configured underlying storage,
    // or create a new one if this is the first time the
    // thread is accessing it.
    #[doc(hidden)]
    pub fn file(&self) -> Result<Arc<fs::File>, ()> {
        let loaded = self.file.load(Ordering::Relaxed);

        if loaded.is_null() {
            let _lock = self.build_locker.lock().unwrap();
            if self.file.load(Ordering::SeqCst).is_null() {
                self.initialize()?;
            }
            Ok(unsafe { (*self.file.load(Ordering::SeqCst)).clone() })
        } else {
            Ok(unsafe { (*loaded).clone() })
        }
    }

    // Get the path of the database
    #[doc(hidden)]
    pub fn get_path(&self) -> PathBuf {
        if self.inner.temporary {
            self.inner.tmp_path.clone()
        } else {
            self.inner.path.clone()
        }
    }

    // returns the current snapshot file prefix
    #[doc(hidden)]
    pub fn snapshot_prefix(&self) -> PathBuf {
        let snapshot_path = self.snapshot_path.clone();
        let path = self.get_path();

        snapshot_path.unwrap_or(path)
    }

    // returns the snapshot file paths for this system
    #[doc(hidden)]
    pub fn get_snapshot_files(
        &self,
    ) -> std::io::Result<Vec<PathBuf>> {
        let mut prefix = self.snapshot_prefix();

        prefix.push("snap.");

        let abs_prefix: PathBuf = if Path::new(&prefix).is_absolute()
        {
            prefix
        } else {
            let mut abs_path = std::env::current_dir()?;
            abs_path.push(prefix.clone());
            abs_path
        };

        let filter =
            |dir_entry: std::io::Result<std::fs::DirEntry>| {
                if let Ok(de) = dir_entry {
                    let path_buf = de.path();
                    let path = path_buf.as_path();
                    let path_str = &*path.to_string_lossy();
                    if path_str
                        .starts_with(&*abs_prefix.to_string_lossy())
                        && !path_str.ends_with(".in___motion")
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

    fn initialize(&self) -> Result<(), ()> {
        // only validate, setup directory, and open file once
        self.validate()?;

        let path = self.db_path();

        // panic if we can't parse the path
        let dir = match Path::new(&path).parent() {
            None => {
                return Err(Error::Unsupported(format!(
                    "could not determine parent directory of {:?}",
                    path
                )));
            }
            Some(dir) => dir.join("blobs"),
        };

        // create data directory if it doesn't exist yet
        if dir.is_file() {
            return Err(Error::Unsupported(format!(
                "provided parent directory is a file, \
                 not a directory: {:?}",
                dir
            )));
        }

        if !dir.exists() {
            let res: std::io::Result<()> =
                std::fs::create_dir_all(dir);
            res.map_err(|e: std::io::Error| {
                let ret: Error<()> = e.into();
                ret
            })?;
        }

        self.verify_config_changes_ok()?;

        // open the data file
        let mut options = fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);

        match options.open(&path) {
            Ok(file) => {
                // try to exclusively lock the file
                if let Err(_) = file.try_lock_exclusive() {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "could not acquire exclusive file lock",
                    )));
                }

                // turn file into a raw pointer for future use
                let file_ptr =
                    Box::into_raw(Box::new(Arc::new(file)));
                self.file.store(file_ptr, Ordering::SeqCst);
            }
            Err(e) => {
                return Err(e.into());
            }
        }
        Ok(())
    }

    // panics if config options are outside of advised range
    fn validate(&self) -> Result<(), ()> {
        supported!(
            self.inner.io_bufs <= 32,
            "too many configured io_bufs"
        );
        supported!(
            self.inner.io_buf_size >= 100,
            "io_buf_size should be hundreds of kb at minimum"
        );
        supported!(
            self.inner.io_buf_size <= 1 << 24,
            "io_buf_size should be <= 16mb"
        );
        supported!(self.inner.page_consolidation_threshold >= 1, "must consolidate pages after a non-zero number of updates");
        supported!(self.inner.page_consolidation_threshold < 1 << 20, "must consolidate pages after fewer than 1 million updates");
        supported!(
            self.inner.cache_bits <= 20,
            "# LRU shards = 2^cache_bits. set this to 20 or less."
        );
        supported!(
            self.inner.segment_cleanup_threshold >= 0.01,
            "segment_cleanup_threshold must be >= 1%"
        );
        supported!(
            self.inner.segment_cleanup_skew < 99,
            "cleanup skew cannot be greater than 99%"
        );
        supported!(
            self.inner.zstd_compression_factor >= 1,
            "compression factor must be >= 0"
        );
        supported!(
            self.inner.zstd_compression_factor <= 22,
            "compression factor must be <= 22"
        );
        Ok(())
    }

    fn verify_config_changes_ok(&self) -> Result<(), ()> {
        match self.read_config() {
            Ok(Some(old)) => {
                if old.merge_operator.is_some() {
                    supported!(self.inner.merge_operator.is_some(),
                        "this system was previously opened with a \
                        merge operator. must supply one FOREVER after \
                        choosing to do so once, BWAHAHAHAHAHAHA!!!!");
                }

                supported!(
                    self.inner.io_buf_size == old.io_buf_size,
                    format!(
                        "cannot change the io buffer size across restarts. \
                        please change it back to {}",
                        old.io_buf_size
                    )
                );
                Ok(())
            }
            Ok(None) => self.write_config().map_err(|e| e.into()),
            Err(e) => Err(e.into()),
        }
    }

    fn write_config(&self) -> Result<(), ()> {
        let bytes = serialize(&*self.inner).unwrap();
        let crc: u64 = crc64(&*bytes);
        let crc_arr = u64_to_arr(crc);

        let path = self.config_path();

        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)?;

        maybe_fail!("write_config bytes");
        f.write_all(&*bytes)?;
        maybe_fail!("write_config crc");
        f.write_all(&crc_arr)?;
        f.sync_all()?;
        maybe_fail!("write_config post");
        Ok(())
    }

    fn read_config(&self) -> std::io::Result<Option<ConfigBuilder>> {
        let path = self.config_path();

        let f_res =
            std::fs::OpenOptions::new().read(true).open(&path);

        let mut f = match f_res {
            Err(ref e)
                if e.kind() == std::io::ErrorKind::NotFound =>
            {
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

        let mut crc_arr = [0u8; 8];
        f.seek(std::io::SeekFrom::End(-8)).unwrap();
        f.read_exact(&mut crc_arr).unwrap();
        let crc_expected = arr_to_u64(crc_arr);

        let crc_actual = crc64(&*buf);

        if crc_expected != crc_actual {
            warn!(
                "crc for settings file {:?} failed! \
                 can't verify that config is safe",
                path
            );
        }

        Ok(deserialize::<ConfigBuilder>(&*buf).ok())
    }

    pub(crate) fn blob_path(&self, id: Lsn) -> PathBuf {
        let mut path = self.get_path();
        path.push("blobs");
        path.push(format!("{}", id));
        path
    }

    fn db_path(&self) -> PathBuf {
        let mut path = self.get_path();
        path.push("db");
        path
    }

    fn config_path(&self) -> PathBuf {
        let mut path = self.get_path();
        path.push("conf");
        path
    }

    #[doc(hidden)]
    pub fn verify_snapshot<PM, P, R>(&self) -> Result<(), ()>
    where
        PM: Materializer<Recovery = R, PageFrag = P>,
        P: 'static
            + Debug
            + Clone
            + Serialize
            + DeserializeOwned
            + Send
            + Sync,
        R: Debug
            + Clone
            + Serialize
            + DeserializeOwned
            + Send
            + PartialEq,
    {
        debug!("generating incremental snapshot");

        let incremental =
            read_snapshot_or_default::<PM, P, R>(&self)?;

        for snapshot_path in self.get_snapshot_files()? {
            std::fs::remove_file(snapshot_path)?;
        }

        debug!("generating snapshot without the previous one");
        let regenerated =
            read_snapshot_or_default::<PM, P, R>(&self)?;

        for (k, v) in &regenerated.pt {
            if !incremental.pt.contains_key(&k) {
                panic!(
                    "page only present in regenerated \
                     pagetable: {} -> {:?}",
                    k, v
                );
            }
            assert_eq!(
                incremental.pt.get(&k),
                Some(v),
                "page tables differ for pid {}",
                k
            );
            for (lsn, ptr) in v.iter() {
                let read = ptr.read(&self);
                if let Err(e) = read {
                    panic!(
                        "could not read log data for \
                         pid {} at lsn {} ptr {}: {}",
                        k, lsn, ptr, e
                    );
                }
            }
        }

        for (k, v) in &incremental.pt {
            if !regenerated.pt.contains_key(&k) {
                panic!(
                    "page only present in incremental \
                     pagetable: {} -> {:?}",
                    k, v
                );
            }
            assert_eq!(
                Some(v),
                regenerated.pt.get(&k),
                "page tables differ for pid {}",
                k
            );
            for (lsn, ptr) in v.iter() {
                let read = ptr.read(&self);
                if let Err(e) = read {
                    panic!(
                        "could not read log data for \
                         pid {} at lsn {} ptr {}: {}",
                        k, lsn, ptr, e
                    );
                }
            }
        }

        assert_eq!(
            incremental.pt, regenerated.pt,
            "snapshot pagetable diverged"
        );
        assert_eq!(
            incremental.max_pid, regenerated.max_pid,
            "snapshot max_pid diverged"
        );
        assert_eq!(
            incremental.max_lsn, regenerated.max_lsn,
            "snapshot max_lsn diverged"
        );
        assert_eq!(
            incremental.last_lid, regenerated.last_lid,
            "snapshot last_lid diverged"
        );
        assert_eq!(
            incremental.free, regenerated.free,
            "snapshot free list diverged"
        );
        assert_eq!(
            incremental.recovery, regenerated.recovery,
            "snapshot recovery diverged"
        );

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

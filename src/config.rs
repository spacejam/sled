use std::{
    io,
    fs,
    fs::File,
    io::{ErrorKind, Read, Seek, Write},
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[cfg(any(windows, target_os = "linux", target_os = "macos"))]
use fs2::FileExt;

use crate::*;
use crate::pagecache::{
    u32_to_arr, arr_to_u32, PageState, SegmentMode, Lsn,
    read_snapshot_or_default, read_message
};

const DEFAULT_PATH: &str = "default.sled";

/// A persisted configuration about high-level
/// storage file information
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct PersistedConfig;

impl PersistedConfig {
    pub fn size_in_bytes(&self) -> u64 {
        0
    }
}

impl Deref for ConfigInner {
    type Target = ConfigBuilder;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Top-level configuration for the system.
///
/// # Examples
///
/// ```
/// let _config = sled::ConfigBuilder::default()
///     .path("/path/to/data".to_owned())
///     .cache_capacity(10_000_000_000)
///     .flush_every_ms(Some(1000))
///     .snapshot_after_ops(100_000);
/// ```
///
/// ```
/// // Read-only mode
/// let _config = sled::ConfigBuilder::default()
///     .path("/path/to/data".to_owned())
///     .read_only(true);
/// ```
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ConfigBuilder {
    #[doc(hidden)]
    pub cache_capacity: u64,
    #[doc(hidden)]
    pub flush_every_ms: Option<u64>,
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
    pub snapshot_after_ops: u64,
    #[doc(hidden)]
    pub snapshot_path: Option<PathBuf>,
    #[doc(hidden)]
    pub temporary: bool,
    #[doc(hidden)]
    pub use_compression: bool,
    #[doc(hidden)]
    pub compression_factor: i32,
    #[doc(hidden)]
    pub print_profile_on_drop: bool,
    #[doc(hidden)]
    pub idgen_persist_interval: u64,
    #[doc(hidden)]
    pub version: (usize, usize),
}

unsafe impl Send for ConfigBuilder {}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self {
            io_buf_size: 2 << 22, // 8mb
            page_consolidation_threshold: 10,
            path: PathBuf::from(DEFAULT_PATH),
            read_only: false,
            cache_capacity: 1024 * 1024 * 1024, // 1gb
            use_compression: false,
            compression_factor: 5,
            flush_every_ms: Some(500),
            snapshot_after_ops: 1_000_000,
            snapshot_path: None,
            segment_cleanup_threshold: 0.40,
            segment_cleanup_skew: 10,
            temporary: false,
            segment_mode: SegmentMode::Gc,
            print_profile_on_drop: false,
            idgen_persist_interval: 1_000_000,
            version: crate_version(),
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
            pub fn $name(mut self, to: $t) -> Self {
                self.$name = to;
                self
            }
        )*
    }
}

impl ConfigBuilder {
    /// Returns a default `ConfigBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the path of the database (builder).
    pub fn path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.path = path.as_ref().to_path_buf();
        self
    }

    /// Finalize the configuration.
    ///
    /// # Panics
    ///
    /// This function will panic if it is not possible
    /// to open the files for performing database IO,
    /// or if the provided configuration fails some
    /// basic sanity checks.
    pub fn build(mut self) -> Config {
        // only validate, setup directory, and open file once
        self.validate().unwrap();

        if self.temporary && self.path == PathBuf::from(DEFAULT_PATH) {
            self.path = Self::gen_temp_path();
        }

        self.limit_cache_max_memory();

        let file = self.open_file().unwrap_or_else(|e| {
            panic!("open file at {:?}: {}", self.db_path(), e);
        });

        // seal config in a Config
        Config(Arc::new(ConfigInner {
            inner: self,
            file,
            global_error: Atomic::default(),
            #[cfg(feature = "event_log")]
            event_log: crate::event_log::EventLog::default(),
        }))
    }

    fn gen_temp_path() -> PathBuf {
        use std::time::SystemTime;

        static SALT_COUNTER: AtomicUsize = AtomicUsize::new(0);

        let seed = SALT_COUNTER.fetch_add(1, Ordering::SeqCst) as u64;

        let now = (SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            << 48) as u64;

        let pid = std::process::id() as u64;

        let salt = (pid << 16) + now + seed;

        if cfg!(target_os = "linux") {
            // use shared memory for temporary linux files
            format!("/dev/shm/pagecache.tmp.{}", salt).into()
        } else {
            let mut pb = std::env::temp_dir();
            pb.push(format!("pagecache.tmp.{}", salt));
            pb
        }
    }

    fn limit_cache_max_memory(&mut self) {
        let limit = get_memory_limit();
        if limit > 0 && self.cache_capacity > limit {
            self.cache_capacity = limit;
            error!(
                "cache capacity is limited to the cgroup memory \
                 limit: {} bytes",
                self.cache_capacity
            );
        }
    }

    builder!(
        (io_buf_size, usize, "size of each io flush buffer. MUST be multiple of 512!"),
        (page_consolidation_threshold, usize, "page consolidation threshold"),
        (temporary, bool, "deletes the database after drop. if no path is set, uses /dev/shm on linux"),
        (read_only, bool, "whether to run in read-only mode"),
        (cache_capacity, u64, "maximum size for the system page cache"),
        (use_compression, bool, "whether to use zstd compression"),
        (compression_factor, i32, "the compression factor to use with zstd compression"),
        (flush_every_ms, Option<u64>, "number of ms between IO buffer flushes"),
        (snapshot_after_ops, u64, "number of operations between page table snapshots"),
        (segment_cleanup_threshold, f64, "the proportion of remaining valid pages in the segment before GC defragments it"),
        (segment_cleanup_skew, usize, "the cleanup threshold skew in percentage points between the first and last segments"),
        (segment_mode, SegmentMode, "the file segment selection mode"),
        (snapshot_path, Option<PathBuf>, "snapshot file location"),
        (print_profile_on_drop, bool, "print a performance profile when the Config is dropped"),
        (idgen_persist_interval, u64, "generated IDs are persisted at this interval. during recovery we skip twice this number")
    );

    // panics if config options are outside of advised range
    fn validate(&self) -> Result<()> {
        supported!(
            self.io_buf_size >= 100,
            "io_buf_size should be hundreds of kb at minimum, and we won't start if below 100"
        );
        supported!(
            self.io_buf_size <= 1 << 24,
            "io_buf_size should be <= 16mb"
        );
        supported!(
            self.page_consolidation_threshold >= 1,
            "must consolidate pages after a non-zero number of updates"
        );
        supported!(
            self.page_consolidation_threshold < 1 << 20,
            "must consolidate pages after fewer than 1 million updates"
        );
        supported!(
            match self.segment_cleanup_threshold.partial_cmp(&0.01) {
                Some(std::cmp::Ordering::Equal)
                | Some(std::cmp::Ordering::Greater) => true,
                Some(std::cmp::Ordering::Less) | None => false,
            },
            "segment_cleanup_threshold must be >= 1%"
        );
        supported!(
            self.segment_cleanup_skew < 99,
            "segment_cleanup_skew cannot be greater than 99%"
        );
        if self.use_compression {
            supported!(
                cfg!(feature = "compression"),
                "the compression feature must be enabled"
            );
        }
        supported!(
            self.compression_factor >= 1,
            "compression_factor must be >= 1"
        );
        supported!(
            self.compression_factor <= 22,
            "compression_factor must be <= 22"
        );
        supported!(
            self.idgen_persist_interval > 0,
            "idgen_persist_interval must be above 0"
        );
        Ok(())
    }

    fn open_file(&mut self) -> Result<File> {
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
            let res = fs::create_dir_all(dir);
            res.map_err(Error::from)?;
        }

        self.verify_config_changes_ok()?;

        // open the data file
        let mut options = fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        if !self.read_only {
            options.write(true);
        }

        match options.open(&path) {
            Ok(file) => self.try_lock(file),
            Err(e) => Err(e.into()),
        }
    }

    fn try_lock(&self, file: File) -> Result<File> {
        #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
        {
            let try_lock = if self.read_only {
                file.try_lock_shared()
            } else {
                file.try_lock_exclusive()
            };

            if let Err(e) = try_lock {
                return Err(Error::Io(io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "could not acquire lock on {:?}: {:?}",
                        self.db_path(),
                        e
                    ),
                )));
            }
        }

        Ok(file)
    }

    fn verify_config_changes_ok(&self) -> Result<()> {
        match self.read_config() {
            Ok(Some(old)) => {
                supported!(
                    self.use_compression == old.use_compression,
                    format!(
                        "cannot change compression values across restarts. \
                         old value of use_compression loaded from disk: {}, \
                         currently set value: {}.",
                        old.use_compression, self.use_compression,
                    )
                );

                supported!(
                    self.io_buf_size == old.io_buf_size,
                    format!(
                        "cannot change the io buffer size across restarts. \
                         please change it back to {}",
                        old.io_buf_size
                    )
                );

                supported!(
                    self.version == old.version,
                    format!(
                        "This database was created using \
                         pagecache version {}.{}, but our pagecache \
                         version is {}.{}. Please perform an upgrade \
                         using the sled::Db::export and sled::Db::import \
                         methods.",
                        old.version.0,
                        old.version.1,
                        self.version.0,
                        self.version.1,
                    )
                );
                Ok(())
            }
            Ok(None) => self.write_config(),
            Err(e) => Err(e.into()),
        }
    }

    fn write_config(&self) -> Result<()> {
        let bytes = serialize(&*self).unwrap();
        let crc: u32 = crc32(&*bytes);
        let crc_arr = u32_to_arr(crc);

        let path = self.config_path();

        let mut f =
            fs::OpenOptions::new().write(true).create(true).open(path)?;

        maybe_fail!("write_config bytes");
        f.write_all(&*bytes)?;
        maybe_fail!("write_config crc");
        f.write_all(&crc_arr)?;
        maybe_fail!("write_config post");
        Ok(())
    }

    fn read_config(&self) -> io::Result<Option<Self>> {
        let path = self.config_path();

        let f_res = fs::OpenOptions::new().read(true).open(&path);

        let mut f = match f_res {
            Err(ref e) if e.kind() == ErrorKind::NotFound => {
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
        buf.split_off(len - 4);

        let mut crc_arr = [0_u8; 4];
        f.seek(io::SeekFrom::End(-4)).unwrap();
        f.read_exact(&mut crc_arr).unwrap();
        let crc_expected = arr_to_u32(&crc_arr);

        let crc_actual = crc32(&*buf);

        if crc_expected != crc_actual {
            warn!(
                "crc for settings file {:?} failed! \
                 can't verify that config is safe",
                path
            );
        }

        Ok(deserialize::<Self>(&*buf).ok())
    }

    // Get the path of the database
    #[doc(hidden)]
    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
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
}

/// A finalized `ConfigBuilder` that can be use multiple times
/// to open a `Tree` or `Log`.
#[derive(Debug, Clone)]
pub struct Config(Arc<ConfigInner>);

impl Deref for Config {
    type Target = ConfigInner;

    fn deref(&self) -> &ConfigInner {
        &self.0
    }
}

#[derive(Debug)]
pub struct ConfigInner {
    inner: ConfigBuilder,
    pub(crate) file: File,
    pub(crate) global_error: Atomic<Error>,
    #[cfg(feature = "event_log")]
    /// an event log for concurrent debugging
    pub event_log: event_log::EventLog,
}

unsafe impl Send for Config {}
unsafe impl Sync for Config {}

impl Drop for ConfigInner {
    fn drop(&mut self) {
        if self.print_profile_on_drop {
            M.print_profile();
        }

        if !self.temporary {
            return;
        }

        // Our files are temporary, so nuke them.
        debug!(
            "removing temporary storage file {}",
            self.inner.path.to_string_lossy()
        );
        let _res = fs::remove_dir_all(&self.path);
    }
}

impl Config {
    /// Return the global error if one was encountered during
    /// an asynchronous IO operation.
    pub fn global_error(&self) -> Result<()> {
        let guard = pin();
        let ge = self.global_error.load(Ordering::Relaxed, &guard);
        if ge.is_null() {
            Ok(())
        } else {
            unsafe { Err(ge.deref().clone()) }
        }
    }

    pub(crate) fn reset_global_error(&self) {
        let guard = pin();
        let old =
            self.global_error
                .swap(Shared::default(), Ordering::SeqCst, &guard);
        if !old.is_null() {
            let guard = pin();
            unsafe {
                guard.defer_destroy(old);
            }
        }
    }

    pub(crate) fn set_global_error(&self, error_value: Error) {
        let guard = pin();
        let error = Owned::new(error_value);

        let expected_old = Shared::null();

        let _ = self.global_error.compare_and_set(
            expected_old,
            error,
            Ordering::Release,
            &guard,
        );
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
    pub fn get_snapshot_files(&self) -> io::Result<Vec<PathBuf>> {
        let mut prefix = self.snapshot_prefix();

        prefix.push("snap.");

        let abs_prefix: PathBuf = if Path::new(&prefix).is_absolute() {
            prefix
        } else {
            let mut abs_path = std::env::current_dir()?;
            abs_path.push(prefix.clone());
            abs_path
        };

        let filter = |dir_entry: io::Result<fs::DirEntry>| {
            if let Ok(de) = dir_entry {
                let path_buf = de.path();
                let path = path_buf.as_path();
                let path_str = &*path.to_string_lossy();
                if path_str.starts_with(&*abs_prefix.to_string_lossy())
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
            fs::create_dir_all(snap_dir)?;
        }

        Ok(snap_dir.read_dir()?.filter_map(filter).collect())
    }

    #[doc(hidden)]
    pub fn verify_snapshot(&self) -> Result<()> {
        debug!("generating incremental snapshot");

        let incremental = read_snapshot_or_default(&self)?;

        for snapshot_path in self.get_snapshot_files()? {
            fs::remove_file(snapshot_path)?;
        }

        debug!("generating snapshot without the previous one");
        let regenerated = read_snapshot_or_default(&self)?;

        let verify_messages = |k: &PageId, v: &PageState| {
            for (lsn, ptr, _sz) in v.iter() {
                if let Err(e) = read_message(&self.file, ptr.lid(), lsn, &self) {
                    panic!(
                        "could not read log data for \
                         pid {} at lsn {} ptr {}: {}",
                        k, lsn, ptr, e
                    );
                }
            }
        };

        let verify_pagestate = |x: &FastMap8<PageId, PageState>,
                                y: &FastMap8<PageId, PageState>,
                                typ: &str| {
            for (k, v) in x {
                if !y.contains_key(&k) {
                    panic!(
                        "page only present in {} pagetable: {} -> {:?}",
                        typ, k, v
                    );
                }
                assert_eq!(
                    y.get(&k),
                    Some(v),
                    "page tables differ for pid {}",
                    k
                );
                verify_messages(k, v);
            }
        };

        verify_pagestate(&regenerated.pt, &incremental.pt, "regenerated");
        verify_pagestate(&incremental.pt, &regenerated.pt, "incremental");

        assert_eq!(
            incremental.pt, regenerated.pt,
            "snapshot pagetable diverged"
        );
        assert_eq!(
            incremental.last_lsn, regenerated.last_lsn,
            "snapshot max_lsn diverged"
        );
        assert_eq!(
            incremental.last_lid, regenerated.last_lid,
            "snapshot last_lid diverged"
        );

        /*
        assert_eq!(
            incremental,
            regenerated,
            "snapshots have diverged!"
        );
        */

        Ok(())
    }

    #[doc(hidden)]
    // truncate the underlying file for corruption testing purposes.
    pub fn truncate_corrupt(&self, new_len: u64) {
        self.file
            .set_len(new_len)
            .expect("should be able to truncate");
    }
}

/// See the Kernel's documentation for more information about this subsystem, found at:
///  [Documentation/cgroup-v1/memory.txt](https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt)
///
/// If there's no memory limit specified on the container this may return
/// 0x7FFFFFFFFFFFF000 (2^63-1 rounded down to 4k which is a common page size).
/// So we know we are not running in a memory restricted environment.
#[cfg(target_os = "linux")]
fn get_cgroup_memory_limit() -> io::Result<u64> {
    File::open("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        .and_then(read_u64_from)
}

#[cfg(target_os = "linux")]
fn read_u64_from(mut file: File) -> io::Result<u64> {
    let mut s = String::new();
    file.read_to_string(&mut s).and_then(|_| {
        s.trim()
            .parse()
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
    })
}

/// Returns the maximum size of total available memory of the process, in bytes.
/// If this limit is exceeded, the malloc() and mmap() functions shall fail with errno set
/// to [ENOMEM].
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn get_rlimit_as() -> io::Result<libc::rlimit> {
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();

    let ret = unsafe { libc::getrlimit(libc::RLIMIT_AS, limit.as_mut_ptr()) };

    if ret == 0 {
        Ok(unsafe { limit.assume_init() })
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn get_available_memory() -> io::Result<u64> {
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    if pages == -1 {
        return Err(io::Error::last_os_error());
    }

    let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) };
    if page_size == -1 {
        return Err(io::Error::last_os_error());
    }

    Ok((pages as u64) * (page_size as u64))
}

fn get_memory_limit() -> u64 {
    // Maximum addressable memory space limit in u64
    static MAX_USIZE: u64 = usize::max_value() as u64;

    let mut max: u64 = 0;

    #[cfg(target_os = "linux")]
    {
        if let Ok(mem) = get_cgroup_memory_limit() {
            max = mem;
        }

        // If there's no memory limit specified on the container this
        // actually returns 0x7FFFFFFFFFFFF000 (2^63-1 rounded down to
        // 4k which is a common page size). So we know we are not
        // running in a memory restricted environment.
        // src: https://github.com/dotnet/coreclr/blob/master/src/pal/src/misc/cgroup.cpp#L385-L428
        if max > 0x7FFF_FFFF_0000_0000 {
            return 0;
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        if let Ok(rlim) = get_rlimit_as() {
            let rlim_cur = rlim.rlim_cur as u64;
            if rlim_cur < max || max == 0 {
                max = rlim_cur;
            }
        }

        if let Ok(available) = get_available_memory() {
            if available < max || max == 0 {
                max = available;
            }
        }
    }

    if max > MAX_USIZE {
        // It is observed in practice when the memory is unrestricted, Linux control
        // group returns a physical limit that is bigger than the address space
        max = MAX_USIZE;
    }

    max
}

fn crate_version() -> (usize, usize) {
    let vsn = env!("CARGO_PKG_VERSION");
    let mut parts = vsn.split('.');
    let major = parts.next().unwrap().parse().unwrap();
    let minor = parts.next().unwrap().parse().unwrap();
    (major, minor)
}

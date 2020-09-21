use std::{
    collections::HashMap,
    fs,
    fs::File,
    io,
    io::{BufRead, BufReader, ErrorKind, Read, Seek, Write},
    ops::Deref,
    path::{Path, PathBuf},
    sync::atomic::AtomicUsize,
};

use crate::pagecache::{arr_to_u32, u32_to_arr, Lsn};
use crate::*;

const DEFAULT_PATH: &str = "default.sled";

/// The high-level database mode, according to
/// the trade-offs of the RUM conjecture.
#[derive(Debug, Clone, Copy)]
pub enum Mode {
    /// In this mode, the database will make
    /// decisions that favor using less space
    /// instead of supporting the highest possible
    /// write throughput. This mode will also
    /// rewrite data more frequently as it
    /// strives to reduce fragmentation.
    LowSpace,
    /// In this mode, the database will try
    /// to maximize write throughput while
    /// potentially using more disk space.
    HighThroughput,
}

/// A persisted configuration about high-level
/// storage file information
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
struct StorageParameters {
    pub segment_size: usize,
    pub use_compression: bool,
    pub version: (usize, usize),
}

impl StorageParameters {
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = vec![];

        writeln!(&mut out, "segment_size: {}", self.segment_size).unwrap();
        writeln!(&mut out, "use_compression: {}", self.use_compression)
            .unwrap();
        writeln!(&mut out, "version: {}.{}", self.version.0, self.version.1)
            .unwrap();

        out
    }

    pub fn deserialize(bytes: &[u8]) -> Result<StorageParameters> {
        let reader = BufReader::new(bytes);

        let mut lines = HashMap::new();

        for line in reader.lines() {
            let line = if let Ok(l) = line {
                l
            } else {
                error!(
                    "failed to parse persisted config as UTF-8. \
                     This changed in sled version 0.29"
                );
                return Err(Error::Unsupported(
                    "failed to open database that may \
                     have been created using a sled version \
                     earlier than 0.29"
                        .to_string(),
                ));
            };
            let mut split = line.split(": ").map(String::from);
            let k = if let Some(k) = split.next() {
                k
            } else {
                error!("failed to parse persisted config line: {}", line);
                return Err(Error::corruption(None));
            };
            let v = if let Some(v) = split.next() {
                v
            } else {
                error!("failed to parse persisted config line: {}", line);
                return Err(Error::corruption(None));
            };
            lines.insert(k, v);
        }

        let segment_size: usize = if let Some(raw) = lines.get("segment_size") {
            if let Ok(parsed) = raw.parse() {
                parsed
            } else {
                error!("failed to parse segment_size value: {}", raw);
                return Err(Error::corruption(None));
            }
        } else {
            error!(
                "failed to retrieve required configuration parameter: segment_size"
            );
            return Err(Error::corruption(None));
        };

        let use_compression: bool = if let Some(raw) =
            lines.get("use_compression")
        {
            if let Ok(parsed) = raw.parse() {
                parsed
            } else {
                error!("failed to parse use_compression value: {}", raw);
                return Err(Error::corruption(None));
            }
        } else {
            error!(
                "failed to retrieve required configuration parameter: use_compression"
            );
            return Err(Error::corruption(None));
        };

        let version: (usize, usize) = if let Some(raw) = lines.get("version") {
            let mut split = raw.split('.');
            let major = if let Some(raw_major) = split.next() {
                if let Ok(parsed_major) = raw_major.parse() {
                    parsed_major
                } else {
                    error!(
                        "failed to parse major version value from line: {}",
                        raw
                    );
                    return Err(Error::corruption(None));
                }
            } else {
                error!("failed to parse major version value: {}", raw);
                return Err(Error::corruption(None));
            };

            let minor = if let Some(raw_minor) = split.next() {
                if let Ok(parsed_minor) = raw_minor.parse() {
                    parsed_minor
                } else {
                    error!(
                        "failed to parse minor version value from line: {}",
                        raw
                    );
                    return Err(Error::corruption(None));
                }
            } else {
                error!("failed to parse minor version value: {}", raw);
                return Err(Error::corruption(None));
            };

            (major, minor)
        } else {
            error!(
                "failed to retrieve required configuration parameter: version"
            );
            return Err(Error::corruption(None));
        };

        Ok(StorageParameters { segment_size, use_compression, version })
    }
}

/// Top-level configuration for the system.
///
/// # Examples
///
/// ```
/// let _config = sled::Config::default()
///     .path("/path/to/data".to_owned())
///     .cache_capacity(10_000_000_000)
///     .flush_every_ms(Some(1000));
/// ```
#[derive(Default, Debug, Clone)]
pub struct Config(Arc<Inner>);

impl Deref for Config {
    type Target = Inner;

    fn deref(&self) -> &Inner {
        &self.0
    }
}

#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct Inner {
    #[doc(hidden)]
    pub cache_capacity: u64,
    #[doc(hidden)]
    pub flush_every_ms: Option<u64>,
    #[doc(hidden)]
    pub segment_size: usize,
    #[doc(hidden)]
    pub path: PathBuf,
    #[doc(hidden)]
    pub create_new: bool,
    #[doc(hidden)]
    pub mode: Mode,
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
    tmp_path: PathBuf,
    pub(crate) global_error: Arc<Atomic<Error>>,
    #[cfg(feature = "event_log")]
    /// an event log for concurrent debugging
    pub event_log: Arc<event_log::EventLog>,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            // generally useful
            path: PathBuf::from(DEFAULT_PATH),
            tmp_path: Config::gen_temp_path(),
            create_new: false,
            cache_capacity: 1024 * 1024 * 1024, // 1gb
            mode: Mode::LowSpace,
            use_compression: false,
            compression_factor: 5,
            temporary: false,
            version: crate_version(),

            // useful in testing
            segment_size: 512 * 1024, // 512kb in bytes
            print_profile_on_drop: false,
            flush_every_ms: Some(500),
            idgen_persist_interval: 1_000_000,
            global_error: Arc::new(Atomic::default()),
            #[cfg(feature = "event_log")]
            event_log: Arc::new(crate::event_log::EventLog::default()),
        }
    }
}

impl Inner {
    // Get the path of the database
    #[doc(hidden)]
    pub fn get_path(&self) -> PathBuf {
        if self.temporary && self.path == PathBuf::from(DEFAULT_PATH) {
            self.tmp_path.clone()
        } else {
            self.path.clone()
        }
    }

    pub(crate) fn blob_path(&self, id: Lsn) -> PathBuf {
        self.get_path().join("blobs").join(format!("{}", id))
    }

    fn db_path(&self) -> PathBuf {
        self.get_path().join("db")
    }

    fn config_path(&self) -> PathBuf {
        self.get_path().join("conf")
    }

    pub(crate) fn normalize<T>(&self, value: T) -> T
    where
        T: Copy
            + TryFrom<usize>
            + std::ops::Div<Output = T>
            + std::ops::Mul<Output = T>,
        <T as std::convert::TryFrom<usize>>::Error: Debug,
    {
        let segment_size: T = T::try_from(self.segment_size).unwrap();
        value / segment_size * segment_size
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
                if Arc::strong_count(&self.0) != 1 {
                    error!(
                        "config has already been used to start \
                         the system and probably should not be \
                         mutated",
                    );
                }
                let m = Arc::make_mut(&mut self.0);
                m.$name = to;
                self
            }
        )*
    }
}

impl Config {
    /// Returns a default `Config`
    pub fn new() -> Config {
        Config::default()
    }

    /// Set the path of the database (builder).
    pub fn path<P: AsRef<Path>>(mut self, path: P) -> Config {
        let m = Arc::get_mut(&mut self.0).unwrap();
        m.path = path.as_ref().to_path_buf();
        self
    }

    /// A testing-only method for reducing the io-buffer size
    /// to trigger correctness-critical behavior more often
    /// by shrinking the buffer size. Don't rely on this.
    #[doc(hidden)]
    pub fn segment_size(mut self, segment_size: usize) -> Config {
        if Arc::strong_count(&self.0) != 1 {
            error!(
                "config has already been used to start \
                 the system and probably should not be \
                 mutated",
            );
        }
        let m = Arc::make_mut(&mut self.0);
        m.segment_size = segment_size;
        self
    }

    /// Opens a `Db` based on the provided config.
    pub fn open(&self) -> Result<Db> {
        // only validate, setup directory, and open file once
        self.validate()?;

        let mut config = self.clone();
        config.limit_cache_max_memory();

        let file = config.open_file()?;

        // seal config in a Config
        let config = RunningConfig { inner: config, file: Arc::new(file) };

        Db::start_inner(config)
    }

    #[doc(hidden)]
    #[deprecated(
        since = "0.31.0",
        note = "this does nothing for now. maybe it will come back in the future."
    )]
    pub const fn segment_cleanup_skew(self, _: usize) -> Self {
        self
    }

    #[doc(hidden)]
    #[deprecated(
        since = "0.31.0",
        note = "this does nothing for now. maybe it will come back in the future."
    )]
    pub const fn segment_cleanup_threshold(self, _: u8) -> Self {
        self
    }

    #[doc(hidden)]
    #[deprecated(
        since = "0.31.0",
        note = "this does nothing for now. maybe it will come back in the future."
    )]
    pub const fn snapshot_after_ops(self, _: u64) -> Self {
        self
    }

    #[doc(hidden)]
    #[deprecated(
        since = "0.31.0",
        note = "this does nothing for now. maybe it will come back in the future."
    )]
    pub fn snapshot_path<P>(self, _: P) -> Self {
        self
    }

    #[doc(hidden)]
    pub fn flush_every_ms(mut self, every_ms: Option<u64>) -> Self {
        if Arc::strong_count(&self.0) != 1 {
            error!(
                "config has already been used to start \
                 the system and probably should not be \
                 mutated",
            );
        }
        let m = Arc::make_mut(&mut self.0);
        m.flush_every_ms = every_ms;
        self
    }

    #[doc(hidden)]
    pub fn idgen_persist_interval(mut self, interval: u64) -> Self {
        if Arc::strong_count(&self.0) != 1 {
            error!(
                "config has already been used to start \
                 the system and probably should not be \
                 mutated",
            );
        }
        let m = Arc::make_mut(&mut self.0);
        m.idgen_persist_interval = interval;
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
    #[doc(hidden)]
    #[deprecated(since = "0.29.0", note = "use Config::open instead")]
    pub fn build(mut self) -> RunningConfig {
        // only validate, setup directory, and open file once
        self.validate().unwrap();

        self.limit_cache_max_memory();

        let file = self.open_file().unwrap_or_else(|e| {
            panic!("open file at {:?}: {}", self.db_path(), e);
        });

        // seal config in a Config
        RunningConfig { inner: self, file: Arc::new(file) }
    }

    fn gen_temp_path() -> PathBuf {
        use std::time::SystemTime;

        static SALT_COUNTER: AtomicUsize = AtomicUsize::new(0);

        let seed = SALT_COUNTER.fetch_add(1, SeqCst) as u128;

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            << 48;

        #[cfg(not(miri))]
        let pid = u128::from(std::process::id());

        #[cfg(miri)]
        let pid = 0;

        let salt = (pid << 16) + now + seed;

        if cfg!(target_os = "linux") {
            // use shared memory for temporary linux files
            format!("/dev/shm/pagecache.tmp.{}", salt).into()
        } else {
            std::env::temp_dir().join(format!("pagecache.tmp.{}", salt))
        }
    }

    fn limit_cache_max_memory(&mut self) {
        let limit = sys_limits::get_memory_limit();
        if limit > 0 && self.cache_capacity > limit {
            let m = Arc::make_mut(&mut self.0);
            m.cache_capacity = limit;
            error!(
                "cache capacity is limited to the cgroup memory \
                 limit: {} bytes",
                self.cache_capacity
            );
        }
    }

    builder!(
        (
            cache_capacity,
            u64,
            "maximum size in bytes for the system page cache"
        ),
        (
            mode,
            Mode,
            "specify whether the system should run in \"small\" or \"fast\" mode"
        ),
        (use_compression, bool, "whether to use zstd compression"),
        (
            compression_factor,
            i32,
            "the compression factor to use with zstd compression. Ranges from 1 up to 22. Levels >= 20 are 'ultra'."
        ),
        (
            temporary,
            bool,
            "deletes the database after drop. if no path is set, uses /dev/shm on linux"
        ),
        (
            create_new,
            bool,
            "attempts to exclusively open the database, failing if it already exists"
        ),
        (
            print_profile_on_drop,
            bool,
            "print a performance profile when the Config is dropped"
        )
    );

    // panics if config options are outside of advised range
    fn validate(&self) -> Result<()> {
        supported!(
            self.segment_size.count_ones() == 1,
            "segment_size should be a power of 2"
        );
        supported!(
            self.segment_size >= 256,
            "segment_size should be hundreds of kb at minimum, and we won't start if below 256"
        );
        supported!(
            self.segment_size <= 1 << 24,
            "segment_size should be <= 16mb"
        );
        if self.use_compression {
            supported!(
                cfg!(feature = "compression"),
                "the 'compression' feature must be enabled"
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

    fn open_file(&self) -> Result<File> {
        let blob_dir: PathBuf = self.get_path().join("blobs");

        if !blob_dir.exists() {
            fs::create_dir_all(blob_dir)?;
        }

        self.verify_config()?;

        // open the data file
        let mut options = fs::OpenOptions::new();

        let _ = options.create(true);
        let _ = options.read(true);
        let _ = options.write(true);

        if self.create_new {
            options.create_new(true);
        }

        self.try_lock(options.open(&self.db_path())?)
    }

    fn try_lock(&self, file: File) -> Result<File> {
        #[cfg(all(
            not(miri),
            any(windows, target_os = "linux", target_os = "macos")
        ))]
        {
            use fs2::FileExt;

            let try_lock = if cfg!(feature = "testing") {
                // we block here because during testing
                // there are many filesystem race condition
                // that happen, causing locks to be held
                // for long periods of time, so we should
                // block to wait on reopening files.
                file.lock_exclusive()
            } else {
                file.try_lock_exclusive()
            };

            if let Err(e) = try_lock {
                return Err(Error::Io(io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "could not acquire lock on {:?}: {:?}",
                        self.db_path().to_string_lossy(),
                        e
                    ),
                )));
            }
        }

        Ok(file)
    }

    fn verify_config(&self) -> Result<()> {
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
                    self.segment_size == old.segment_size,
                    format!(
                        "cannot change the io buffer size across restarts. \
                         please change it back to {}",
                        old.segment_size
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
            Err(e) => Err(e),
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let persisted_config = StorageParameters {
            version: self.version,
            segment_size: self.segment_size,
            use_compression: self.use_compression,
        };

        persisted_config.serialize()
    }

    fn write_config(&self) -> Result<()> {
        let bytes = self.serialize();
        let crc: u32 = crc32(&*bytes);
        let crc_arr = u32_to_arr(crc);

        let path = self.config_path();

        let mut f =
            fs::OpenOptions::new().write(true).create(true).open(path)?;

        io_fail!(self, "write_config bytes");
        f.write_all(&*bytes)?;
        io_fail!(self, "write_config crc");
        f.write_all(&crc_arr)?;
        io_fail!(self, "write_config post");
        Ok(())
    }

    fn read_config(&self) -> Result<Option<StorageParameters>> {
        let path = self.config_path();

        let f_res = fs::OpenOptions::new().read(true).open(&path);

        let mut f = match f_res {
            Err(ref e) if e.kind() == ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(other) => {
                return Err(other.into());
            }
            Ok(f) => f,
        };

        if f.metadata()?.len() <= 8 {
            warn!("empty/corrupt configuration file found");
            return Ok(None);
        }

        let mut buf = vec![];
        let _ = f.read_to_end(&mut buf)?;
        let len = buf.len();
        let _ = buf.split_off(len - 4);

        let mut crc_arr = [0_u8; 4];
        let _ = f.seek(io::SeekFrom::End(-4))?;
        f.read_exact(&mut crc_arr)?;
        let crc_expected = arr_to_u32(&crc_arr);

        let crc_actual = crc32(&*buf);

        if crc_expected != crc_actual {
            warn!(
                "crc for settings file {:?} failed! \
                 can't verify that config is safe",
                path
            );
        }

        StorageParameters::deserialize(&buf).map(Some)
    }

    /// Return the global error if one was encountered during
    /// an asynchronous IO operation.
    #[doc(hidden)]
    pub fn global_error(&self) -> Result<()> {
        let guard = pin();
        let ge = self.global_error.load(Acquire, &guard);
        if ge.is_null() {
            Ok(())
        } else {
            #[allow(unsafe_code)]
            unsafe {
                Err(ge.deref().clone())
            }
        }
    }

    pub(crate) fn reset_global_error(&self) {
        let guard = pin();
        let old = self.global_error.swap(Shared::default(), SeqCst, &guard);
        if !old.is_null() {
            let guard = pin();
            #[allow(unsafe_code)]
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
            SeqCst,
            &guard,
        );
    }

    #[cfg(feature = "failpoints")]
    #[cfg(feature = "event_log")]
    #[doc(hidden)]
    // truncate the underlying file for corruption testing purposes.
    pub fn truncate_corrupt(&self, new_len: u64) {
        self.event_log.reset();
        let path = self.db_path();
        let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        f.set_len(new_len).expect("should be able to truncate");
    }
}

/// A Configuration that has an associated opened
/// file.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct RunningConfig {
    inner: Config,
    pub(crate) file: Arc<File>,
}

#[allow(unsafe_code)]
unsafe impl Send for RunningConfig {}

#[allow(unsafe_code)]
unsafe impl Sync for RunningConfig {}

impl Deref for RunningConfig {
    type Target = Config;

    fn deref(&self) -> &Config {
        &self.inner
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.print_profile_on_drop {
            M.print_profile();
        }

        if !self.temporary {
            return;
        }

        // Our files are temporary, so nuke them.
        debug!("removing temporary storage file {:?}", self.get_path());
        let _res = fs::remove_dir_all(&self.get_path());
    }
}

impl RunningConfig {
    // returns the snapshot file paths for this system
    #[doc(hidden)]
    pub fn get_snapshot_files(&self) -> io::Result<Vec<PathBuf>> {
        let path = self.get_path().join("snap.");

        let absolute_path: PathBuf = if Path::new(&path).is_absolute() {
            path
        } else {
            std::env::current_dir()?.join(path)
        };

        let filter = |dir_entry: io::Result<fs::DirEntry>| {
            if let Ok(de) = dir_entry {
                let path_buf = de.path();
                let path = path_buf.as_path();
                let path_str = &*path.to_string_lossy();
                if path_str.starts_with(&*absolute_path.to_string_lossy())
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

        let snap_dir = Path::new(&absolute_path).parent().unwrap();

        if !snap_dir.exists() {
            fs::create_dir_all(snap_dir)?;
        }

        Ok(snap_dir.read_dir()?.filter_map(filter).collect())
    }
}

fn crate_version() -> (usize, usize) {
    let vsn = env!("CARGO_PKG_VERSION");
    let mut parts = vsn.split('.');
    let major = parts.next().unwrap().parse().unwrap();
    let minor = parts.next().unwrap().parse().unwrap();
    (major, minor)
}

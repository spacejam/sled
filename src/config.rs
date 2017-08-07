use std::cell::RefCell;
use std::fs;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use tempfile::NamedTempFile;

use super::*;

/// Top-level configuration for the system.
#[derive(Debug, Clone)]
pub struct Config {
    io_bufs: usize,
    io_buf_size: usize,
    blink_fanout: usize,
    page_consolidation_threshold: usize,
    path: Option<String>,
    cache_bits: usize,
    cache_capacity: usize,
    use_os_cache: bool,
    use_compression: bool,
    tc: Arc<ThreadCache<fs::File>>,
    tmp: Arc<Mutex<NamedTempFile>>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            io_bufs: 3,
            io_buf_size: 2 << 22, // 8mb
            blink_fanout: 128,
            page_consolidation_threshold: 10,
            path: None,
            cache_bits: 6,
            cache_capacity: 1024 * 1024 * 1024,
            use_os_cache: true,
            use_compression: true,
            tc: Arc::new(ThreadCache::default()),
            tmp: Arc::new(Mutex::new(NamedTempFile::new().unwrap())),
        }
    }
}

impl Config {
    /// Get io_bufs
    pub fn get_io_bufs(&self) -> usize {
        self.io_bufs
    }

    /// Get io_buf_size
    pub fn get_io_buf_size(&self) -> usize {
        self.io_buf_size
    }

    /// Get blink_fanout
    pub fn get_blink_fanout(&self) -> usize {
        self.blink_fanout
    }

    /// Get page_consolidation_threshold
    pub fn get_page_consolidation_threshold(&self) -> usize {
        self.page_consolidation_threshold
    }

    /// Get the number of bits used for addressing cache shards.
    pub fn get_cache_bits(&self) -> usize {
        self.cache_bits
    }

    /// Get the cache capacity in bytes.
    pub fn get_cache_capacity(&self) -> usize {
        self.cache_capacity
    }

    /// Get whether the system uses the OS pagecache for
    /// compressed pages.
    pub fn get_use_os_cache(&self) -> bool {
        self.use_os_cache
    }

    /// Get whether the system compresses data written to
    /// secondary storage with the zstd algorithm.
    pub fn get_use_compression(&self) -> bool {
        self.use_compression
    }

    /// Get path
    pub fn get_path(&self) -> Option<String> {
        self.path.clone()
    }

    /// Set io_bufs
    pub fn set_io_bufs(&mut self, io_bufs: usize) {
        self.io_bufs = io_bufs;
    }

    /// Set io_buf_size
    pub fn set_io_buf_size(&mut self, io_buf_size: usize) {
        self.io_buf_size = io_buf_size;
    }

    /// Set blink_fanout
    pub fn set_blink_fanout(&mut self, blink_fanout: usize) {
        self.blink_fanout = blink_fanout;
    }

    /// Set page_consolidation_threshold
    pub fn set_page_consolidation_threshold(&mut self, threshold: usize) {
        self.page_consolidation_threshold = threshold;
    }

    /// Set the number of bits used for addressing cache shards.
    pub fn set_cache_bits(&mut self, cache_bits: usize) {
        self.cache_bits = cache_bits;
    }

    /// Set the cache capacity in bytes.
    pub fn set_cache_capacity(&mut self, cache_capacity: usize) {
        self.cache_capacity = cache_capacity;
    }

    /// Set whether the system uses the OS pagecache for
    /// compressed pages.
    pub fn set_use_os_cache(&mut self, use_os_cache: bool) {
        self.use_os_cache = use_os_cache;
    }

    /// Set whether the system compresses data written to
    /// secondary storage with the zstd algorithm.
    pub fn set_use_compression(&mut self, use_compression: bool) {
        self.use_compression = use_compression;
    }

    /// Set path
    pub fn set_path(&mut self, path: Option<String>) {
        self.path = path;
    }

    /// Builder, set number of io buffers
    pub fn io_bufs(&self, io_bufs: usize) -> Config {
        let mut ret = self.clone();
        ret.io_bufs = io_bufs;
        ret
    }

    /// Builder, set the max io buffer size
    pub fn io_buf_size(&self, io_buf_size: usize) -> Config {
        let mut ret = self.clone();
        ret.io_buf_size = io_buf_size;
        ret
    }

    /// Builder, set the b-link tree node fanout
    pub fn blink_fanout(&self, blink_fanout: usize) -> Config {
        let mut ret = self.clone();
        ret.blink_fanout = blink_fanout;
        ret
    }

    /// Builder, set the pagecache consolidation threshold
    pub fn page_consolidation_threshold(&self, threshold: usize) -> Config {
        let mut ret = self.clone();
        ret.page_consolidation_threshold = threshold;
        ret
    }

    /// Builder, set the number of bits used for addressing cache shards.
    pub fn cache_bits(&self, cache_bits: usize) -> Config {
        let mut ret = self.clone();
        ret.cache_bits = cache_bits;
        ret
    }

    /// Builder, set the cache capacity in bytes.
    pub fn cache_capacity(&self, cache_capacity: usize) -> Config {
        let mut ret = self.clone();
        ret.cache_capacity = cache_capacity;
        ret
    }

    /// Builder, set whether the system uses the OS pagecache for
    /// compressed pages.
    pub fn use_os_cache(&self, use_os_cache: bool) -> Config {
        let mut ret = self.clone();
        ret.use_os_cache = use_os_cache;
        ret
    }

    /// Builder, set whether the system compresses data written to
    /// secondary storage with the zstd algorithm.
    pub fn use_compression(&self, use_compression: bool) -> Config {
        let mut ret = self.clone();
        ret.use_compression = use_compression;
        ret
    }

    /// Builder, set the filesystem path
    pub fn path(&self, path: Option<String>) -> Config {
        let mut ret = self.clone();
        ret.path = path;
        ret
    }

    /// create a new `Tree` based on this configuration
    pub fn tree(&self) -> Tree {
        Tree::new(self.clone())
    }

    /// create a new `LockFreeLog` based on this configuration
    pub fn log(&self) -> LockFreeLog {
        LockFreeLog::start_system(self.clone())
    }

    /// Retrieve a thread-local file handle to the configured underlying storage,
    /// or create a new one if this is the first time the thread is accessing it.
    pub fn cached_file(&self) -> Rc<RefCell<fs::File>> {
        self.tc.get_or_else(|| {
            if let Some(p) = self.get_path() {
                let mut options = fs::OpenOptions::new();
                options.create(true);
                options.read(true);
                options.write(true);

                if !self.use_os_cache && cfg!(unix) {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.custom_flags(libc::O_DIRECT);
                    panic!("O_DIRECT support not sussed out yet.");
                }

                options.open(p).unwrap()
            } else {
                self.tmp.lock().unwrap().reopen().unwrap()
            }
        })
    }
}

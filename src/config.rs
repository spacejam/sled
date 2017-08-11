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
    flush_every_ms: Option<usize>,
    snapshot_after_ops: usize,
    snapshot_path_prefix: Option<String>,
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
            flush_every_ms: Some(100),
            snapshot_after_ops: 1_000_000,
            snapshot_path_prefix: None,
            tc: Arc::new(ThreadCache::default()),
            tmp: Arc::new(Mutex::new(NamedTempFile::new().unwrap())),
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
        (io_buf_size, get_io_buf_size, set_io_buf_size, usize, "size of each io flush buffer"),
        (blink_fanout, get_blink_fanout, set_blink_fanout, usize, "b-link node fanout"),
        (page_consolidation_threshold, get_page_consolidation_threshold, set_page_consolidation_threshold, usize, "page consolidation threshold"),
        (path, get_path, set_path, Option<String>, "path for the main storage file"),
        (cache_bits, get_cache_bits, set_cache_bits, usize, "log base 2 of the number of cache shards"),
        (cache_capacity, get_cache_capacity, set_cache_capacity, usize, "maximum size for the system page cache"),
        (use_os_cache, get_use_os_cache, set_use_os_cache, bool, "whether to use the OS page cache"),
        (use_compression, get_use_compression, set_use_compression, bool, "whether to use zstd compression"),
        (flush_every_ms, get_flush_every_ms, set_flush_every_ms, Option<usize>, "number of ms between IO buffer flushes"),
        (snapshot_after_ops, get_snapshot_after_ops, set_snapshot_after_ops, usize, "number of operations between page table snapshots"),
        (snapshot_path_prefix, get_snapshot_path_prefix, set_snapshot_path_prefix, Option<String>, "snapshot file prefix")
    );

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
        self.tc.get_or_else(|| if let Some(p) = self.get_path() {
            let mut options = fs::OpenOptions::new();
            options.create(true);
            options.read(true);
            options.write(true);

            #[cfg(target_os = "linux")]
            {
                if !self.use_os_cache {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.custom_flags(libc::O_DIRECT);
                    panic!("O_DIRECT support not sussed out yet.");
                }
            }

            options.open(p).unwrap()
        } else {
            self.tmp.lock().unwrap().reopen().unwrap()
        })
    }
}

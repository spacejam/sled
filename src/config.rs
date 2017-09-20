use std::cell::{RefCell, UnsafeCell};
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use std::io::{Error, Read, Seek, SeekFrom};
use std::fs::File;
use std::io::ErrorKind::{Interrupted, Other, UnexpectedEof};

#[cfg(feature = "zstd")]
use zstd::block::decompress;

use super::*;

/// Top-level configuration for the system.
///
/// # Examples
///
/// ```
/// let config = sled::Config::default()
///     .path("/path/to/data".to_owned())
///     .cache_capacity(10_000_000_000)
///     .use_compression(true)
///     .flush_every_ms(Some(1000))
///     .snapshot_after_ops(100_000);
/// ```
#[derive(Debug, Clone)]
pub struct Config {
    inner: Arc<UnsafeCell<ConfigInner>>,
}

unsafe impl Send for Config {}
unsafe impl Sync for Config {}

impl Default for Config {
    fn default() -> Config {
        let now = uptime();
        let nanos = (now.as_secs() * 1_000_000_000) + now.subsec_nanos() as u64;

        // use shared memory for temporary linux files
        #[cfg(target_os = "linux")]
        let tmp_path = format!("/dev/shm/sled.tmp.{}", nanos);

        #[cfg(not(target_os = "linux"))]
        let tmp_path = format!("sled.tmp.{}", nanos);

        let inner = Arc::new(UnsafeCell::new(ConfigInner {
            io_bufs: 3,
            io_buf_size: 2 << 22, // 8mb
            blink_fanout: 32,
            page_consolidation_threshold: 10,
            path: tmp_path.to_owned(),
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
        }));
        Config {
            inner: inner,
        }
    }
}

impl Deref for Config {
    type Target = ConfigInner;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner.get() }
    }
}

impl DerefMut for Config {
    fn deref_mut(&mut self) -> &mut ConfigInner {
        unsafe { &mut *self.inner.get() }
    }
}

impl Config {
    /// create a new `Tree` based on this configuration
    pub fn tree(&self) -> Tree {
        Tree::new(self.clone())
    }

    /// create a new `LockFreeLog` based on this
    /// configuration
    pub fn log(&self) -> LockFreeLog {
        LockFreeLog::start_system(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct ConfigInner {
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
                Config { inner: Arc::new(UnsafeCell::new(ret))}
            }
        )*
    }
}

impl ConfigInner {
    builder!(
        (io_bufs, get_io_bufs, set_io_bufs, usize, "number of io buffers"),
        (io_buf_size, get_io_buf_size, set_io_buf_size, usize, "size of each io flush buffer. MUST be multiple of 512!"),
        (blink_fanout, get_blink_fanout, set_blink_fanout, usize, "b-link node fanout, minimum of 2"),
        (page_consolidation_threshold, get_page_consolidation_threshold, set_page_consolidation_threshold, usize, "page consolidation threshold"),
        (path, get_path, set_path, String, "path for the main storage file"),
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

            #[cfg(target_os = "linux")]
            {
                if !self.use_os_cache {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.custom_flags(libc::O_DIRECT);
                    panic!("O_DIRECT support not sussed out yet.");
                }
            }

            options.open(path).unwrap()
        })
    }

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

        let abs_prefix: String = if Path::new(&prefix).is_absolute() {
            prefix
        } else {
            let mut abs_path =
                std::env::current_dir().expect("could not read current dir, maybe deleted?");
            abs_path.push(prefix.clone());
            abs_path.to_str().unwrap().to_owned()
        };

        let filter = |dir_entry: std::io::Result<std::fs::DirEntry>| if let Ok(de) = dir_entry {
            let path_buf = de.path();
            let path = path_buf.as_path();
            let path_str = path.to_str().unwrap();
            if path_str.starts_with(&abs_prefix) && !path_str.ends_with(".in___motion") {
                Some(path_str.to_owned())
            } else {
                None
            }
        } else {
            None
        };

        let snap_dir = Path::new(&abs_prefix).parent().expect(
            "could not read snapshot directory",
        );

        snap_dir
            .read_dir()
            .expect("could not read snapshot directory")
            .filter_map(filter)
            .collect()
    }

    /// read a segment of log messages. Only call after
    /// pausing segment rewriting on the segment accountant!
    pub fn read_segment(&self, offset: LogID) -> std::io::Result<log::Segment> {
        let segment_header_read = self.read(offset)?;
        if segment_header_read.is_corrupt() {
            return Err(Error::new(Other, "corrupt segment"));
        }
        if segment_header_read.is_zeroed() {
            return Err(Error::new(Other, "empty segment"));
        }
        let (lsn, _, _) = segment_header_read.flush().unwrap();
        assert_eq!(lsn % self.get_io_buf_size() as Lsn, 0);

        let mut buf = vec![];
        let cached_f = self.cached_file();
        let mut f = cached_f.borrow_mut();

        f.seek(SeekFrom::Start(offset))?;
        read_up_to(f, &mut buf, self.get_io_buf_size())?;

        Ok(log::Segment {
            buf: buf,
            lsn: lsn,
            read_offset: HEADER_LEN,
            position: offset,
            max_encountered_lsn: lsn,
        })
    }

    /// read a buffer from the disk
    pub fn read(&self, id: LogID) -> std::io::Result<LogRead> {
        let start = clock();
        let cached_f = self.cached_file();
        let mut f = cached_f.borrow_mut();
        f.seek(SeekFrom::Start(id))?;

        let mut valid_buf = [0u8; 1];
        f.read_exact(&mut valid_buf)?;
        let valid = valid_buf[0] == 1;

        let mut lsn_buf = [0u8; 8];
        f.read_exact(&mut lsn_buf)?;
        let lsn: Lsn = unsafe { std::mem::transmute(lsn_buf) };

        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf)?;

        let len32: u32 = unsafe { std::mem::transmute(len_buf) };
        let mut len = len32 as usize;
        let max = self.get_io_buf_size() - HEADER_LEN;
        if len > max {
            #[cfg(feature = "log")]
            error!("log read invalid message length, {} should be <= {}", len, max);
            M.read.measure(clock() - start);
            return Ok(LogRead::Corrupted(len));
        } else if len == 0 && !valid {
            // skip to next record, which starts with 1
            loop {
                let mut byte = [0u8; 1];
                if let Err(e) = f.read_exact(&mut byte) {
                    if e.kind() == UnexpectedEof {
                        // we've hit the end of the file
                        break;
                    }
                    panic!("{:?}", e);
                }
                if byte[0] != 1 {
                    debug_assert_eq!(byte[0], 0);
                    len += 1;
                } else {
                    break;
                }
            }
        }

        if !valid {
            M.read.measure(clock() - start);
            // we're 2 short when we started seeking for a non-zero byte (crc16)
            return Ok(LogRead::Zeroed(len + HEADER_LEN - 2));
        } else if (lsn % self.get_io_buf_size() as Lsn) != (id % self.get_io_buf_size() as LogID) {
            return Ok(LogRead::Corrupted(len + HEADER_LEN - 2));
        }

        let mut crc16_buf = [0u8; 2];
        f.read_exact(&mut crc16_buf)?;

        let mut buf = Vec::with_capacity(len);
        unsafe {
            buf.set_len(len);
        }
        f.read_exact(&mut buf)?;

        let checksum = crc16_arr(&buf);
        if checksum != crc16_buf {
            M.read.measure(clock() - start);
            return Ok(LogRead::Corrupted(len));
        }

        #[cfg(feature = "zstd")]
        let res = {
            if self.get_use_compression() {
                let start = clock();
                let res = Ok(LogRead::Flush(lsn, decompress(&*buf, max).unwrap(), len));
                M.decompress.measure(clock() - start);
                res
            } else {
                Ok(LogRead::Flush(lsn, buf, len))
            }
        };

        #[cfg(not(feature = "zstd"))]
        let res = Ok(LogRead::Flush(lsn, buf, len));

        M.read.measure(clock() - start);
        res
    }
}

impl Drop for ConfigInner {
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
                    #[cfg(feature = "log")]
                warn!("failed to remove old snapshot file, maybe snapshot race? {}", _e);
            }
        }
    }
}

fn read_up_to(
    mut f: std::cell::RefMut<File>,
    buf: &mut Vec<u8>,
    max: usize,
) -> std::io::Result<usize> {
    buf.reserve(max);
    unsafe {
        buf.set_len(max);
    }
    let mut len = 0;
    loop {
        match f.read(&mut buf[len..max]) {
            Ok(0) => {
                unsafe {
                    buf.set_len(len);
                }
                return Ok(len);
            }
            Ok(n) => {
                len += n;
            }
            Err(ref e) if e.kind() == Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

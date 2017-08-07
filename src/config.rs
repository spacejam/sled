use std::fs;
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
            tmp: Arc::new(Mutex::new(NamedTempFile::new().unwrap())),
        }
    }
}

impl Config {
    /// get io_bufs
    pub fn get_io_bufs(&self) -> usize {
        self.io_bufs
    }

    /// get io_buf_size
    pub fn get_io_buf_size(&self) -> usize {
        self.io_buf_size
    }

    /// get blink_fanout
    pub fn get_blink_fanout(&self) -> usize {
        self.blink_fanout
    }

    /// get page_consolidation_threshold
    pub fn get_page_consolidation_threshold(&self) -> usize {
        self.page_consolidation_threshold
    }

    /// get path
    pub fn get_path(&self) -> Option<String> {
        self.path.clone()
    }

    /// set io_bufs
    pub fn set_io_bufs(&mut self, io_bufs: usize) {
        self.io_bufs = io_bufs;
    }

    /// set io_buf_size
    pub fn set_io_buf_size(&mut self, io_buf_size: usize) {
        self.io_buf_size = io_buf_size;
    }

    /// set blink_fanout
    pub fn set_blink_fanout(&mut self, blink_fanout: usize) {
        self.blink_fanout = blink_fanout;
    }

    /// set page_consolidation_threshold
    pub fn set_page_consolidation_threshold(&mut self, threshold: usize) {
        self.page_consolidation_threshold = threshold;
    }

    /// set path
    pub fn set_path(&mut self, path: Option<String>) {
        self.path = path;
    }

    /// builder, set number of io buffers
    pub fn io_bufs(&self, io_bufs: usize) -> Config {
        let mut ret = self.clone();
        ret.io_bufs = io_bufs;
        ret
    }

    /// builder, set the max io buffer size
    pub fn io_buf_size(&self, io_buf_size: usize) -> Config {
        let mut ret = self.clone();
        ret.io_buf_size = io_buf_size;
        ret
    }

    /// builder, set the b-link tree node fanout
    pub fn blink_fanout(&self, blink_fanout: usize) -> Config {
        let mut ret = self.clone();
        ret.blink_fanout = blink_fanout;
        ret
    }

    /// builder, set the pagecache consolidation threshold
    pub fn page_consolidation_threshold(&self, threshold: usize) -> Config {
        let mut ret = self.clone();
        ret.page_consolidation_threshold = threshold;
        ret
    }

    /// builder, set the filesystem path
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

    /// Open a new file handle to the configured underlying storage.
    pub fn open_file(&self) -> fs::File {
        let mut options = fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);

        if let Some(p) = self.get_path() {
            options.open(p).unwrap()
        } else {
            self.tmp.lock().unwrap().reopen().unwrap()
        }
    }
}

use super::*;

/// Top-level configuration for the system.
#[derive(Debug, Clone)]
pub struct Config {
    io_bufs: usize,
    io_buf_size: usize,
    blink_fanout: usize,
    path: Option<String>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            io_bufs: 3,
            io_buf_size: 2 << 22, // 8mb
            blink_fanout: 128,
            path: None,
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

    /// set path
    pub fn set_path(&mut self, path: Option<String>) {
        self.path = path;
    }

    /// get number of io buffers
    pub fn io_bufs(&self, io_bufs: usize) -> Config {
        let mut ret = self.clone();
        ret.io_bufs = io_bufs;
        ret
    }

    /// get the max io buffer size
    pub fn io_buf_size(&self, io_buf_size: usize) -> Config {
        let mut ret = self.clone();
        ret.io_buf_size = io_buf_size;
        ret
    }

    /// get the b-link tree node fanout
    pub fn blink_fanout(&self, blink_fanout: usize) -> Config {
        let mut ret = self.clone();
        ret.blink_fanout = blink_fanout;
        ret
    }

    /// get the filesystem path
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
}

/// Top-level configuration for the system.

use super::*;

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
    pub fn get_io_bufs(&self) -> usize {
        self.io_bufs
    }

    pub fn get_io_buf_size(&self) -> usize {
        self.io_buf_size
    }

    pub fn get_blink_fanout(&self) -> usize {
        self.blink_fanout
    }

    pub fn get_path(&self) -> Option<String> {
        self.path.clone()
    }

    pub fn set_io_bufs(&mut self, io_bufs: usize) {
        self.io_bufs = io_bufs;
    }

    pub fn set_io_buf_size(&mut self, io_buf_size: usize) {
        self.io_buf_size = io_buf_size;
    }

    pub fn set_blink_fanout(&mut self, blink_fanout: usize) {
        self.blink_fanout = blink_fanout;
    }

    pub fn set_path(&mut self, path: Option<String>) {
        self.path = path;
    }

    pub fn io_bufs(&self, io_bufs: usize) -> Config {
        let mut ret = self.clone();
        ret.io_bufs = io_bufs;
        ret
    }

    pub fn io_buf_size(&self, io_buf_size: usize) -> Config {
        let mut ret = self.clone();
        ret.io_buf_size = io_buf_size;
        ret
    }

    pub fn blink_fanout(&self, blink_fanout: usize) -> Config {
        let mut ret = self.clone();
        ret.blink_fanout = blink_fanout;
        ret
    }

    pub fn path(&self, path: Option<String>) -> Config {
        let mut ret = self.clone();
        ret.path = path;
        ret
    }

    pub fn tree(&self) -> Tree {
        Tree::new(self.clone())
    }

    pub fn log(&self) -> LockFreeLog {
        LockFreeLog::start_system(self.clone())
    }
}

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::LogOffset;

#[cfg(all(not(unix), not(windows)))]
use crate::pagecache::parallel_io_polyfill::{pread_exact, pread_exact_or_eof, pwrite_all};

#[cfg(unix)]
use crate::pagecache::parallel_io_unix::{pread_exact, pread_exact_or_eof, pwrite_all};

#[cfg(windows)]
use crate::pagecache::parallel_io_windows::{pread_exact, pread_exact_or_eof, pwrite_all};

type ReadDirPathsIter = Box<dyn Iterator<Item = std::io::Result<PathBuf>>>;
type ReadDirSizesIter = Box<dyn Iterator<Item = std::io::Result<u64>>>;

#[doc(hidden)]
pub trait IO: std::fmt::Debug + Send + Sync {
    fn create_dir_all(&self, path: &Path) -> std::io::Result<()>;
    fn file_create_new_rw(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>>;
    fn file_create_new_w(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>>;
    fn file_open_or_create_rw(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>>;
    fn file_open_or_create_w(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>>;
    fn file_open_r(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>>;
    fn file_open_w(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>>;
    fn get_memory_limit(&self) -> u64;
    fn path_exists(&self, path: &Path) -> bool;
    fn read_dir_paths(&self, path: &Path) -> std::io::Result<ReadDirPathsIter>;
    fn read_dir_sizes(&self, path: &Path) -> std::io::Result<ReadDirSizesIter>;
    fn remove_dir_all(&self, path: &Path) -> std::io::Result<()>;
    fn remove_file(&self, path: &Path) -> std::io::Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()>;
    fn temp_dir(&self) -> PathBuf;
}

#[doc(hidden)]
pub trait IOFile: std::fmt::Debug + Send + Sync {
    fn len(&self) -> std::io::Result<u64>;
    fn pread_exact(&self, buf: &mut [u8], offset: LogOffset) -> std::io::Result<()>;
    fn pread_exact_or_eof(&self, buf: &mut [u8], offset: LogOffset) -> std::io::Result<usize>;
    fn pwrite_all(&self, buf: &[u8], offset: LogOffset) -> std::io::Result<()>;
    fn read_exact(&mut self, buffer: &mut [u8]) -> std::io::Result<()>;
    fn read_to_end(&mut self, buffer: &mut Vec<u8>) -> std::io::Result<usize>;
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64>;
    fn set_len(&self, length: u64) -> std::io::Result<()>;
    fn sync_all(&self) -> std::io::Result<()>;
    fn sync_file_range_before_write_after(&self, offset: i64, nbytes: i64) -> std::io::Result<()>;
    fn try_lock(&self) -> std::io::Result<()>;
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()>;
}

#[allow(missing_copy_implementations)]
#[derive(Debug)]
pub(crate) struct RealIO ();

impl IO for RealIO {
    fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn file_create_new_rw(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>> {
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true);
        options.read(true);
        options.write(true);
        let file = options.open(path)?;
        Ok(Box::new(RealIOFile(file)))
    }

    fn file_create_new_w(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>> {
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true);
        options.write(true);
        let file = options.open(path)?;
        Ok(Box::new(RealIOFile(file)))
    }

    fn file_open_or_create_rw(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>> {
        let mut options = std::fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);
        let file = options.open(path)?;
        Ok(Box::new(RealIOFile(file)))
    }

    fn file_open_or_create_w(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>> {
        let mut options = std::fs::OpenOptions::new();
        options.create(true);
        options.write(true);
        let file = options.open(path)?;
        Ok(Box::new(RealIOFile(file)))
    }

    fn file_open_r(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>> {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        Ok(Box::new(RealIOFile(file)))
    }

    fn file_open_w(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>> {
        let file = std::fs::OpenOptions::new().write(true).open(path)?;
        Ok(Box::new(RealIOFile(file)))
    }

    fn get_memory_limit(&self) -> u64 {
        crate::sys_limits::get_memory_limit()
    }

    fn path_exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn read_dir_paths(
        &self,
        path: &Path,
    ) -> std::io::Result<ReadDirPathsIter> {
        let read_dir = path.read_dir()?;
        Ok(Box::new(RealReadDirPaths(read_dir)))
    }

    fn read_dir_sizes(
        &self,
        path: &Path,
    ) -> std::io::Result<ReadDirSizesIter> {
        let read_dir = path.read_dir()?;
        Ok(Box::new(RealReadDirSizes(read_dir)))
    }

    fn remove_dir_all(&self, path: &Path) -> std::io::Result<()> {
        std::fs::remove_dir_all(path)
    }

    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        std::fs::remove_file(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        std::fs::rename(from, to)
    }

    fn temp_dir(&self) -> PathBuf {
        std::env::temp_dir()
    }
}

pub struct RealReadDirPaths (std::fs::ReadDir);

impl Iterator for RealReadDirPaths {
    type Item = std::io::Result<PathBuf>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|direntry| Ok(direntry?.path()))
    }
}

pub struct RealReadDirSizes (std::fs::ReadDir);

impl Iterator for RealReadDirSizes {
    type Item = std::io::Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|direntry| Ok(direntry?.metadata()?.len()))
    }
}

#[derive(Debug)]
pub struct RealIOFile (std::fs::File);

impl IOFile for RealIOFile {
    fn len(&self) -> std::io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }

    fn pread_exact(
        &self,
        buf: &mut [u8],
        offset: LogOffset,
    ) -> std::io::Result<()>{
        pread_exact(&self.0, buf, offset)
    }

    fn pread_exact_or_eof(
        &self,
        buf: &mut [u8],
        offset: LogOffset,
    ) -> std::io::Result<usize>{
        pread_exact_or_eof(&self.0, buf, offset)
    }

    fn pwrite_all(
        &self,
        buf: &[u8],
        offset: LogOffset,
    ) -> std::io::Result<()> {
        pwrite_all(&self.0, buf, offset)
    }

    fn read_exact(&mut self, buffer: &mut[u8]) -> std::io::Result<()> {
        self.0.read_exact(buffer)
    }

    fn read_to_end(&mut self, buffer: &mut Vec<u8>) -> std::io::Result<usize> {
        self.0.read_to_end(buffer)
    }

    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.0.seek(pos)
    }

    fn set_len(&self, length: u64) -> std::io::Result<()> {
        self.0.set_len(length)
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.0.sync_all()
    }

    #[allow(unsafe_code)]
    fn sync_file_range_before_write_after(&self, offset: i64, nbytes: i64) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let ret = unsafe {
                libc::sync_file_range(
                    self.0.as_raw_fd(),
                    offset,
                    nbytes,
                    libc::SYNC_FILE_RANGE_WAIT_BEFORE
                        | libc::SYNC_FILE_RANGE_WRITE
                        | libc::SYNC_FILE_RANGE_WAIT_AFTER,
                )
            };
            if ret < 0 {
                let err = std::io::Error::last_os_error();
                if let Some(libc::ENOSYS) = err.raw_os_error() {
                    self.sync_all()
                } else {
                    Err(err)
                }
            } else {
                Ok(())
            }
        }

        #[cfg(not(target_os = "linux"))]
        self.sync_all()
    }

    fn try_lock(&self) -> std::io::Result<()> {
        #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
        {
            use fs2::FileExt;

            if cfg!(feature = "testing") {
                // we block here because during testing
                // there are many filesystem race condition
                // that happen, causing locks to be held
                // for long periods of time, so we should
                // block to wait on reopening files.
                self.0.lock_exclusive()?;
            } else {
                self.0.try_lock_exclusive()?;
            }
        }

        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.0.write_all(buf)
    }
}

use std::path::{Path, PathBuf};
use std::io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};
use std::fs::{self, Metadata};
use std::cmp;
use std::mem;
use std::collections::HashMap;

#[cfg(unix)]
use std::os::unix::fs::{FileExt, OpenOptionsExt};

#[cfg(windows)]
use std::os::windows::fs::{FileExt, OpenOptionsExt};

use rand::{Rng, SeedableRng, StdRng};

use super::*;

#[derive(Clone, Debug)]
pub struct OpenOptions {
    inner: fs::OpenOptions,
}

#[cfg(unix)]
impl OpenOptionsExt for OpenOptions {
    fn mode(&mut self, mode: u32) -> &mut OpenOptions {
        self.inner.mode(mode);
        self
    }

    fn custom_flags(&mut self, flags: i32) -> &mut OpenOptions {
        self.inner.custom_flags(flags);
        self
    }
}

#[cfg(windows)]
impl OpenOptionsExt for OpenOptions {
    fn access_mode(&mut self, access: u32) -> &mut OpenOptions {
        self.inner.access_mode(access);
        self
    }
    fn share_mode(&mut self, share: u32) -> &mut OpenOptions {
        self.inner.share_mode(share);
        self
    }
    fn custom_flags(&mut self, flags: u32) -> &mut OpenOptions {
        self.inner.custom_flags(flags);
        self
    }
    fn attributes(&mut self, attributes: u32) -> &mut OpenOptions {
        self.inner.attributes(attributes);
        self
    }
    fn security_qos_flags(&mut self, flags: u32) -> &mut OpenOptions {
        self.inner.security_quos_flags(flags);
        self
    }
}

impl OpenOptions {
    pub fn new() -> OpenOptions {
        OpenOptions {
            inner: fs::OpenOptions::new(),
        }
    }
    pub fn read(&mut self, read: bool) -> &mut OpenOptions {
        self.inner.read(read);
        self
    }
    pub fn write(&mut self, write: bool) -> &mut OpenOptions {
        self.inner.write(write);
        self
    }
    pub fn append(&mut self, append: bool) -> &mut OpenOptions {
        self.inner.append(append);
        self
    }
    pub fn truncate(&mut self, truncate: bool) -> &mut OpenOptions {
        self.inner.truncate(truncate);
        self
    }
    pub fn create(&mut self, create: bool) -> &mut OpenOptions {
        self.inner.create(create);
        self
    }
    pub fn create_new(&mut self, create_new: bool) -> &mut OpenOptions {
        self.inner.create_new(create_new);
        self
    }
    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<File> {
        Ok(File(Mutex::new(FileInner {
            path: path.as_ref().to_path_buf(),
            inner: self.inner.open(path)?,
            stable: vec![],
            updates: vec![],
            is_crashing: false,
        })))
    }
}

#[derive(Debug)]
pub struct File(Mutex<FileInner>);


#[cfg(unix)]
impl FileExt for File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.with_inner(|f| f.read_at(buf, offset))
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize> {
        self.with_inner(|f| f.write_at(buf, offset))
    }
}

#[cfg(windows)]
impl FileExt for File {
    fn seek_read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.with_inner(|f| f.read_at(buf, offset))
    }

    fn seek_write(&self, buf: &[u8], offset: u64) -> Result<usize> {
        self.with_inner(|f| f.write_at(buf, offset))
    }
}

impl File {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<File> {
        OpenOptions::new().read(true).open(path)
    }

    pub fn create<P: AsRef<Path>>(path: P) -> Result<File> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
    }

    fn with_inner<B, F>(&self, f: F) -> B
        where F: FnOnce(&mut FileInner) -> B
    {
        let mut file = self.0.lock().unwrap();
        f(&mut file)
    }

    pub fn sync_all(&self) -> Result<()> {
        self.with_inner(|f| f.sync_all())
    }

    pub fn sync_data(&self) -> Result<()> {
        self.with_inner(|f| f.sync_data())
    }

    pub fn set_len(&self, size: u64) -> Result<()> {
        self.with_inner(|f| f.set_len(size))
    }

    pub fn metadata(&self) -> Result<Metadata> {
        self.with_inner(|f| f.metadata())
    }

    pub fn tell(&self) -> Result<u64> {
        self.with_inner(|f| f.tell())
    }

    pub fn reset(&mut self) -> Result<()> {
        self.with_inner(|f| f.reset())
    }

    pub fn crash(&mut self) {
        self.with_inner(|f| f.crash())
    }
}

impl Seek for File {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.with_inner(|f| f.seek(pos))
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.with_inner(|f| f.read(buf))
    }

    fn read_exact(&mut self, mut buf: &mut [u8]) -> Result<()> {
        self.with_inner(|f| f.read_exact(buf))
    }
}

impl Write for File {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.with_inner(|f| f.write(buf))
    }

    fn flush(&mut self) -> Result<()> {
        self.with_inner(|f| f.flush())
    }
}

#[derive(Debug)]
struct FileInner {
    path: PathBuf,
    stable: Vec<u8>,
    updates: Vec<(u64, Vec<u8>)>,
    inner: fs::File,
    is_crashing: bool,
}

impl FileInner {
    #[cfg(unix)]
    pub fn read_at(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if self.is_crashing {
            return Err(Error::new(ErrorKind::BrokenPipe, "oh no!"));
        }

        self.inner.read_at(buf, offset)
    }

    #[cfg(unix)]
    pub fn write_at(&mut self, buf: &[u8], offset: u64) -> Result<usize> {
        if self.is_crashing {
            return Err(Error::new(ErrorKind::BrokenPipe, "oh no!"));
        }

        let len = self.inner.write_at(buf, offset)?;
        let write = buf[..len].to_vec();
        self.updates.push((offset, write));
        Ok(len)
    }

    #[cfg(windows)]
    fn seek_read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if self.is_crashing {
            return Err(Error::new(ErrorKind::BrokenPipe, "oh no!"));
        }

        self.inner.seek_read(buf, offset)
    }

    #[cfg(windows)]
    fn seek_write(&self, buf: &[u8], offset: u64) -> Result<usize> {
        if self.is_crashing {
            return Err(Error::new(ErrorKind::BrokenPipe, "oh no!"));
        }

        let len = self.inner.seek_write(buf, offset)?;
        let write = buf[..len].to_vec();
        self.updates.push((offset, write));
        Ok(len)
    }

    pub fn sync_all(&mut self) -> Result<()> {
        if self.is_crashing {
            return Err(Error::new(ErrorKind::BrokenPipe, "oh no!"));
        }

        self.inner.sync_all()?;
        self.updates = vec![];
        self.stable = vec![];
        self.inner.read_to_end(&mut self.stable).map(|_| ())
    }

    pub fn sync_data(&mut self) -> Result<()> {
        if self.is_crashing {
            return Err(Error::new(ErrorKind::BrokenPipe, "oh no!"));
        }

        self.inner.sync_data()?;
        self.updates = vec![];
        self.stable = vec![];
        self.inner.read_to_end(&mut self.stable).map(|_| ())
    }

    pub fn set_len(&mut self, size: u64) -> Result<()> {
        if self.is_crashing {
            return Err(Error::new(ErrorKind::BrokenPipe, "oh no!"));
        }

        self.inner.set_len(size)?;

        self.updates.retain(|&(offset, _)| offset < size);

        for &mut (offset, ref mut write) in &mut self.updates {
            let rel_len = (size - offset) as usize;
            if rel_len < write.len() {
                write.truncate(rel_len);
            }
        }

        Ok(())
    }

    pub fn metadata(&self) -> Result<Metadata> {
        self.inner.metadata()
    }

    fn tell(&mut self) -> Result<u64> {
        self.seek(SeekFrom::Current(0))
    }

    fn reset(&mut self) -> Result<()> {
        self.is_crashing = false;

        if self.updates.is_empty() {
            return Ok(());
        }

        let total_loss = context::thread_rng().gen::<bool>();
        if total_loss {
            self.updates = vec![];
            return Ok(());
        }

        let stabilize = context::thread_rng().gen_range(0, self.updates.len());

        self.inner.set_len(0)?;
        self.inner.write_all(&*self.stable)?;

        for &(offset, ref buf) in self.updates.iter().take(stabilize) {
            self.inner.write_at(&*buf, offset as u64).expect(
                "replayed write should work",
            );
        }

        self.updates = vec![];

        Ok(())
    }

    fn crash(&mut self) {
        self.is_crashing = true;
    }
}

impl Seek for FileInner {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        if self.is_crashing {
            return Err(Error::new(ErrorKind::BrokenPipe, "oh no!"));
        }

        self.inner.seek(pos)
    }
}

impl Read for FileInner {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.is_crashing {
            return Err(Error::new(ErrorKind::BrokenPipe, "oh no!"));
        }

        self.inner.read(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.inner.read_exact(buf)
    }
}

impl Write for FileInner {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.is_crashing {
            return Err(Error::new(ErrorKind::BrokenPipe, "oh no!"));
        }

        let len = self.inner.write(buf)?;
        let write = buf[..len].to_vec();
        let offset = self.tell()?;
        self.updates.push((offset, write));

        Ok(len)
    }

    fn flush(&mut self) -> Result<()> {
        if self.is_crashing {
            return Err(Error::new(ErrorKind::BrokenPipe, "oh no!"));
        }

        self.inner.flush()
    }
}

#[derive(Default, Debug)]
pub struct Filesystem {
    files: HashMap<PathBuf, File>,
}

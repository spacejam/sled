use std::fs::{self, Metadata};
use std::io::{
    Error, ErrorKind, Read, Result, Seek, SeekFrom, Write,
};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::{FileExt, OpenOptionsExt};

#[cfg(windows)]
use std::os::windows::fs::{FileExt, OpenOptionsExt};

use rand::Rng;

use super::*;

/// Options and flags which can be used to configure how a file is opened.
///
/// This builder exposes the ability to configure how a [`File`] is opened and
/// what operations are permitted on the open file. The [`File::open`] and
/// [`File::create`] methods are aliases for commonly used options using this
/// builder.
///
/// [`File`]: struct.File.html
/// [`File::open`]: struct.File.html#method.open
/// [`File::create`]: struct.File.html#method.create
///
/// Generally speaking, when using `OpenOptions`, you'll first call [`new`],
/// then chain calls to methods to set each option, then call [`open`],
/// passing the path of the file you're trying to open. This will give you a
/// [`io::Result`][result] with a [`File`][file] inside that you can further
/// operate on.
///
/// [`new`]: struct.OpenOptions.html#method.new
/// [`open`]: struct.OpenOptions.html#method.open
/// [result]: ../io/type.Result.html
/// [file]: struct.File.html
///
/// # Examples
///
/// Opening a file to read:
///
/// ```no_run
/// use std::fs::OpenOptions;
///
/// let file = OpenOptions::new().read(true).open("foo.txt");
/// ```
///
/// Opening a file for both reading and writing, as well as creating it if it
/// doesn't exist:
///
/// ```no_run
/// use std::fs::OpenOptions;
///
/// let file = OpenOptions::new()
///             .read(true)
///             .write(true)
///             .create(true)
///             .open("foo.txt");
/// ```
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
    fn security_qos_flags(
        &mut self,
        flags: u32,
    ) -> &mut fs::OpenOptions {
        self.inner.security_qos_flags(flags)
    }
}

impl OpenOptions {
    /// Creates a blank new set of options ready for configuration.
    ///
    /// All options are initially set to `false`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::fs::OpenOptions;
    ///
    /// let mut options = OpenOptions::new();
    /// let file = options.read(true).open("foo.txt");
    /// ```
    pub fn new() -> OpenOptions {
        OpenOptions {
            inner: fs::OpenOptions::new(),
        }
    }

    /// Sets the option for read access.
    ///
    /// This option, when true, will indicate that the file should be
    /// `read`-able if opened.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::fs::OpenOptions;
    ///
    /// let file = OpenOptions::new().read(true).open("foo.txt");
    /// ```
    pub fn read(&mut self, read: bool) -> &mut OpenOptions {
        self.inner.read(read);
        self
    }

    /// Sets the option for write access.
    ///
    /// This option, when true, will indicate that the file should be
    /// `write`-able if opened.
    ///
    /// If the file already exists, any write calls on it will overwrite its
    /// contents, without truncating it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::fs::OpenOptions;
    ///
    /// let file = OpenOptions::new().write(true).open("foo.txt");
    /// ```
    pub fn write(&mut self, write: bool) -> &mut OpenOptions {
        self.inner.write(write);
        self
    }

    /// Sets the option for creating a new file.
    ///
    /// This option indicates whether a new file will be created if the file
    /// does not yet already exist.
    ///
    /// In order for the file to be created, [`write`] or [`append`] access must
    /// be used.
    ///
    /// [`write`]: #method.write
    /// [`append`]: #method.append
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::fs::OpenOptions;
    ///
    /// let file = OpenOptions::new().write(true).create(true).open("foo.txt");
    /// ```
    pub fn create(&mut self, create: bool) -> &mut OpenOptions {
        self.inner.create(create);
        self
    }

    /// Sets the option for truncating a previous file.
    ///
    /// If a file is successfully opened with this option set it will truncate
    /// the file to 0 length if it already exists.
    ///
    /// The file must be opened with write access for truncate to work.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::fs::OpenOptions;
    ///
    /// let file = OpenOptions::new().write(true).truncate(true).open("foo.txt");
    /// ```
    pub fn truncate(&mut self, truncate: bool) -> &mut OpenOptions {
        self.inner.truncate(truncate);
        self
    }

    /// Opens a file at `path` with the options specified by `self`.
    ///
    /// # Errors
    ///
    /// This function will return an error under a number of different
    /// circumstances. Some of these error conditions are listed here, together
    /// with their [`ErrorKind`]. The mapping to [`ErrorKind`]s is not part of
    /// the compatibility contract of the function, especially the `Other` kind
    /// might change to more specific kinds in the future.
    ///
    /// * [`NotFound`]: The specified file does not exist and neither `create`
    ///   or `create_new` is set.
    /// * [`NotFound`]: One of the directory components of the file path does
    ///   not exist.
    /// * [`PermissionDenied`]: The user lacks permission to get the specified
    ///   access rights for the file.
    /// * [`PermissionDenied`]: The user lacks permission to open one of the
    ///   directory components of the specified path.
    /// * [`AlreadyExists`]: `create_new` was specified and the file already
    ///   exists.
    /// * [`InvalidInput`]: Invalid combinations of open options (truncate
    ///   without write access, no access mode set, etc.).
    /// * [`Other`]: One of the directory components of the specified file path
    ///   was not, in fact, a directory.
    /// * [`Other`]: Filesystem-level errors: full disk, write permission
    ///   requested on a read-only file system, exceeded disk quota, too many
    ///   open files, too long filename, too many symbolic links in the
    ///   specified path (Unix-like systems only), etc.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::fs::OpenOptions;
    ///
    /// let file = OpenOptions::new().open("foo.txt");
    /// ```
    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<File> {
        Ok(File(Arc::new(Mutex::new(FileInner {
            path: path.as_ref().to_path_buf(),
            inner: self.inner.open(path)?,
            stable: vec![],
            updates: vec![],
            is_crashing: false,
        }))))
    }
}

/// A file with the ability to be "crashed" during test mode.
#[derive(Clone, Debug)]
pub struct File(Arc<Mutex<FileInner>>);

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
    fn seek_read(
        &self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<usize> {
        self.with_inner(|f| f.seek_read(buf, offset))
    }

    fn seek_write(&self, buf: &[u8], offset: u64) -> Result<usize> {
        self.with_inner(|f| f.seek_write(buf, offset))
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
    where
        F: FnOnce(&mut FileInner) -> B,
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

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
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
    pub fn read_at(
        &mut self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<usize> {
        if self.is_crashing {
            return Err(Error::new(ErrorKind::BrokenPipe, "oh no!"));
        }

        self.inner.read_at(buf, offset)
    }

    #[cfg(unix)]
    pub fn write_at(
        &mut self,
        buf: &[u8],
        offset: u64,
    ) -> Result<usize> {
        if self.is_crashing {
            return Err(Error::new(ErrorKind::BrokenPipe, "oh no!"));
        }

        let len = self.inner.write_at(buf, offset)?;
        let write = buf[..len].to_vec();
        self.updates.push((offset, write));
        Ok(len)
    }

    #[cfg(windows)]
    fn seek_read(
        &self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<usize> {
        if self.is_crashing {
            return Err(Error::new(ErrorKind::BrokenPipe, "oh no!"));
        }

        self.inner.seek_read(buf, offset)
    }

    #[cfg(windows)]
    fn seek_write(
        &mut self,
        buf: &[u8],
        offset: u64,
    ) -> Result<usize> {
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

        let stabilize =
            context::thread_rng().gen_range(0, self.updates.len());

        self.inner.set_len(0)?;
        self.inner.write_all(&*self.stable)?;

        for &(offset, ref buf) in self.updates.iter().take(stabilize)
        {
            #[cfg(unix)]
            self.inner
                .write_at(&*buf, offset as u64)
                .expect("replayed write should work");
            #[cfg(windows)]
            self.inner
                .seek_write(&*buf, offset as u64)
                .expect("replayed write should work");
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

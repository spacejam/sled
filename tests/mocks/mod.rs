// Currently, this all leans heavily on mutexes to get things working, though
// this introduces unnecessary serialization that may mask concurrency
// problems in sled itself. As future work, these mocks could be rewritten to
// permit concurrent file operations.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use rand::{Rng, thread_rng};

use sled::{IO, IOFile, LogOffset};

enum CreationMode {
    Create,
    CreateOrExisting,
    Existing,
}

#[derive(Debug)]
struct FilePermissions {
    read: bool,
    write: bool,
}

impl FilePermissions {
    const R: FilePermissions = FilePermissions { read: true, write: false };
    const RW: FilePermissions = FilePermissions { read: true, write: true };
    const W: FilePermissions = FilePermissions { read: false, write: true };
}

#[derive(Debug)]
enum FileContents {
    Present(Vec<u8>),
    Tombstone,
}

impl FileContents {
    fn unwrap(&self) -> &Vec<u8> {
        match self {
            FileContents::Present(vec) => vec,
            FileContents::Tombstone => panic!("file was used after deletion"),
        }
    }

    fn unwrap_mut(&mut self) -> &mut Vec<u8> {
        match self {
            FileContents::Present(vec) => vec,
            FileContents::Tombstone => panic!("file was used after deletion"),
        }
    }
}

#[derive(Debug)]
// always lock in the order dir_paths first, file_paths second to avoid deadlocks
pub struct VirtualFileSystem {
    working_dir: PathBuf,
    dir_paths: Mutex<BTreeSet<PathBuf>>,
    file_paths: Mutex<BTreeMap<PathBuf, Arc<Mutex<FileContents>>>>,
}

impl VirtualFileSystem {
    pub fn new() -> VirtualFileSystem {
        let workdir = PathBuf::from("/opt/sled");
        let mut dir_paths = BTreeSet::new();
        dir_paths.insert(workdir.parent().unwrap().to_owned());
        dir_paths.insert(workdir.clone());
        dir_paths.insert(PathBuf::from("/tmp"));
        let dir_paths = Mutex::new(dir_paths);
        VirtualFileSystem {
            working_dir: workdir,
            dir_paths,
            file_paths: Mutex::new(BTreeMap::new()),
        }
    }

    fn make_absolute(&self, path: &Path) -> PathBuf {
        if path.is_absolute() {
            path.to_owned()
        } else {
            self.working_dir.join(path)
        }
    }

    fn open_file(&self, path: &Path, creation: CreationMode, permissions: FilePermissions) -> std::io::Result<Box<dyn IOFile>> {
        let path = self.make_absolute(path);
        let dir_paths = self.dir_paths.lock();
        if let Some(parent) = path.parent() {
            if !dir_paths.contains(parent) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "parent directory does not exist",
                ));
            }
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "invalid file path",
            ));
        }
        if dir_paths.contains(&path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "cannot open or create file, there is a directory with the same name",
            ));
        }
        let mut file_paths = self.file_paths.lock();
        let contents = match (file_paths.entry(path.to_owned()), creation) {
            (std::collections::btree_map::Entry::Vacant(entry), CreationMode::Create)
                | (std::collections::btree_map::Entry::Vacant(entry), CreationMode::CreateOrExisting) => {
                    entry.insert(Arc::new(Mutex::new(FileContents::Present(Vec::new())))).clone()
                }
            (std::collections::btree_map::Entry::Vacant(_), CreationMode::Existing) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "file does not exist",
                ));
            }
            (std::collections::btree_map::Entry::Occupied(_), CreationMode::Create) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "file already exists",
                ));
            }
            (std::collections::btree_map::Entry::Occupied(entry), CreationMode::CreateOrExisting)
                | (std::collections::btree_map::Entry::Occupied(entry), CreationMode::Existing) => {
                    entry.get().clone()
                }
        };
        Ok(Box::new(VirtualFile {
            offset: 0,
            permissions,
            contents,
        }))
    }
}

impl IO for VirtualFileSystem {
    fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
        let target_dir = self.make_absolute(path);
        let mut component_iter = target_dir.components();
        assert_eq!(component_iter.next(), Some(std::path::Component::RootDir));
        let mut create_path = PathBuf::with_capacity(target_dir.capacity());
        create_path.push("/");
        let mut dir_paths = self.dir_paths.lock();
        let file_paths = self.file_paths.lock();
        for component in component_iter {
            match component {
                std::path::Component::Prefix(_) => unimplemented!(),
                std::path::Component::RootDir => unreachable!(),
                std::path::Component::CurDir => unreachable!(),
                std::path::Component::ParentDir => unimplemented!(),
                std::path::Component::Normal(name) => {
                    create_path.push(name);
                    if file_paths.contains_key(&create_path) {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::AlreadyExists,
                            "directory cannot be created, a file already exists at that path",
                        ));
                    }
                    dir_paths.insert(create_path.clone());
                }
            }
        }
        Ok(())
    }

    fn file_create_new_rw(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>> {
        self.open_file(path, CreationMode::Create, FilePermissions::RW)
    }

    fn file_create_new_w(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>> {
        self.open_file(path, CreationMode::Create, FilePermissions::W)
    }

    fn file_open_or_create_rw(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>> {
        self.open_file(path, CreationMode::CreateOrExisting, FilePermissions::RW)
    }

    fn file_open_or_create_w(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>> {
        self.open_file(path, CreationMode::CreateOrExisting, FilePermissions::W)
    }

    fn file_open_r(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>> {
        self.open_file(path, CreationMode::Existing, FilePermissions::R)
    }

    fn file_open_w(&self, path: &Path) -> std::io::Result<Box<dyn IOFile>> {
        self.open_file(path, CreationMode::Existing, FilePermissions::W)
    }

    fn get_memory_limit(&self) -> u64 {
        1024 * 1024 * 1024
    }

    fn path_exists(&self, path: &Path) -> bool {
        let path = self.make_absolute(path);
        self.dir_paths.lock().contains(&path) || self.file_paths.lock().contains_key(&path)
    }

    fn read_dir_paths(&self, path: &Path) -> std::io::Result<Box<dyn Iterator<Item = std::io::Result<PathBuf>>>> {
        let path = self.make_absolute(path);
        let dir_paths = self.dir_paths.lock();
        if dir_paths.contains(&path) {
            let file_paths = self.file_paths.lock();
            let range = (std::ops::Bound::Included(&path), std::ops::Bound::Unbounded);
            let mut paths: Vec<std::io::Result<PathBuf>> = dir_paths.range::<PathBuf, _>(range)
                .take_while(|candidate| candidate.starts_with(&path))
                .filter(|candidate| candidate.parent() == Some(&path))
                .cloned()
                .map(Result::Ok)
                .collect();
            paths.extend(
                file_paths.range::<PathBuf, _>(range)
                    .map(|(pathbuf, _)| pathbuf)
                    .take_while(|candidate| candidate.starts_with(&path))
                    .filter(|candidate| candidate.parent() == Some(&path))
                    .cloned()
                    .map(Result::Ok)
            );
            Ok(Box::new(paths.into_iter()))
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "directory does not exist"))
        }
    }

    fn read_dir_sizes(&self, path: &Path) -> std::io::Result<Box<dyn Iterator<Item = std::io::Result<u64>>>> {
        let path = self.make_absolute(path);
        let dir_paths = self.dir_paths.lock();
        if dir_paths.contains(&path) {
            let file_paths = self.file_paths.lock();
            let range = (std::ops::Bound::Included(&path), std::ops::Bound::Unbounded);
            let sizes: Vec<std::io::Result<u64>> = file_paths.range::<PathBuf, _>(range)
                .take_while(|(candidate, _)| candidate.starts_with(&path))
                .filter(|(candidate, _)| candidate.parent() == Some(&path))
                .map(|(_, arc)| Ok(arc.lock().unwrap().len().try_into().unwrap()))
                .collect();
            Ok(Box::new(sizes.into_iter()))
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "directory does not exist"))
        }
    }

    fn remove_dir_all(&self, path: &Path) -> std::io::Result<()> {
        let path = self.make_absolute(path);
        let mut dir_paths = self.dir_paths.lock();
        if dir_paths.remove(&path) {
            let mut file_paths = self.file_paths.lock();
            let range = (std::ops::Bound::Included(&path), std::ops::Bound::Unbounded);
            while let Some(member) = dir_paths.range::<PathBuf, _>(range).next() {
                if member.starts_with(&path) {
                    let member = member.clone();
                    dir_paths.remove(&member);
                } else {
                    break;
                }
            }
            while let Some((member, arc)) = file_paths.range::<PathBuf, _>(range).next() {
                if member.starts_with(&path) {
                    let mut contents = arc.lock();
                    *contents = FileContents::Tombstone;
                    drop(contents);
                    let member = member.clone();
                    file_paths.remove(&member);
                } else {
                    break;
                }
            }
            Ok(())
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "directory does not exist"))
        }
    }

    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        let path = self.make_absolute(path);
        let mut file_paths = self.file_paths.lock();
        if let Some(arc) = file_paths.remove(&path) {
            let mut contents = arc.lock();
            *contents = FileContents::Tombstone;
            Ok(())
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "file does not exist"))
        }
    }

    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        let from = self.make_absolute(from);
        let to = self.make_absolute(to);
        let dir_paths = self.dir_paths.lock();
        if dir_paths.contains(&from) {
            // Renaming directories is not supported, as it would be hard, and it's not necessary for tests
            unimplemented!();
        }
        if let Some(parent) = to.parent() {
            if !dir_paths.contains(parent) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "destination directory does not exist",
                ));
            }
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "invalid destination",
            ))
        }
        let mut file_paths = self.file_paths.lock();
        if let Some(contents) = file_paths.remove(&from) {
            file_paths.insert(to, contents);
            Ok(())
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "file does not exist",
            ))
        }
    }

    fn temp_dir(&self) -> PathBuf {
        const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234564789";

        let parent = PathBuf::from("/tmp");
        let mut dir_paths = self.dir_paths.lock();
        let file_paths = self.file_paths.lock();
        assert!(!file_paths.contains_key(&parent));
        if !dir_paths.contains(&parent) {
            dir_paths.insert(parent.clone());
        }

        let mut rng = thread_rng();
        let path = loop {
            let mut directory_bytes = [0u8; 11];
            directory_bytes[0..3].copy_from_slice(b"tmp");
            for byte in &mut directory_bytes[3..11] {
                *byte = CHARS[rng.gen_range(0, CHARS.len())];
            }
            let directory_str = std::str::from_utf8(&directory_bytes[..]).unwrap();
            let path = parent.join(PathBuf::from(directory_str));
            if !dir_paths.contains(&path) {
                break path;
            }
        };
        dir_paths.insert(path.clone());
        path
    }
}

#[derive(Debug)]
pub struct VirtualFile {
    offset: usize,
    permissions: FilePermissions,
    contents: Arc<Mutex<FileContents>>,
}

impl VirtualFile {
    fn check_read_access(&self) -> std::io::Result<()> {
        if self.permissions.read {
            Ok(())
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "file not opened with read permissions"))
        }
    }

    fn check_write_access(&self) -> std::io::Result<()> {
        if self.permissions.write {
            Ok(())
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "file not opened with write permissions"))
        }
    }
}

impl IOFile for VirtualFile {
    fn len(&self) -> std::io::Result<u64> {
        Ok(self.contents.lock().unwrap().len().try_into().unwrap())
    }

    fn pread_exact(&self, buf: &mut [u8], offset: LogOffset) -> std::io::Result<()> {
        self.check_read_access()?;
        let offset = offset.try_into().unwrap();
        let contents = self.contents.lock();
        let v = contents.unwrap();
        if offset > v.len() || buf.len() > v.len() - offset {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "could not read enough data",
            ));
        }
        buf.copy_from_slice(&v[offset..offset + buf.len()]);
        Ok(())
    }

    fn pread_exact_or_eof(&self, buf: &mut [u8], offset: LogOffset) -> std::io::Result<usize> {
        self.check_read_access()?;
        let offset = offset.try_into().unwrap();
        let contents = self.contents.lock();
        let v = contents.unwrap();
        if offset > v.len() {
            return Ok(0);
        }
        let read_len = std::cmp::min(buf.len(), v.len() - offset);
        buf[0..read_len].copy_from_slice(&v[offset..offset + read_len]);
        Ok(read_len)
    }

    fn pwrite_all(&self, buf: &[u8], offset: LogOffset) -> std::io::Result<()> {
        self.check_write_access()?;
        let offset_usize: usize = offset.try_into().unwrap();
        let required_len = offset_usize + buf.len();
        let mut contents = self.contents.lock();
        let v = contents.unwrap_mut();
        if v.len() < required_len {
            v.resize(required_len, 0);
        }
        &v[offset_usize..required_len].copy_from_slice(buf);
        Ok(())
    }

    fn read_exact(&mut self, buffer: &mut [u8]) -> std::io::Result<()> {
        self.check_read_access()?;
        let contents = self.contents.lock();
        let v = contents.unwrap();
        if self.offset > v.len() || buffer.len() > v.len() - self.offset {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "could not read enough data",
            ));
        }
        buffer.copy_from_slice(&v[self.offset..self.offset + buffer.len()]);
        self.offset += buffer.len();
        Ok(())
    }

    fn read_to_end(&mut self, buffer: &mut Vec<u8>) -> std::io::Result<usize> {
        self.check_read_access()?;
        let contents = self.contents.lock();
        let v = contents.unwrap();
        if self.offset > v.len() {
            return Ok(0);
        }
        let read_amount = v.len() - self.offset;
        buffer.extend_from_slice(&v[self.offset..]);
        self.offset = v.len();
        Ok(read_amount)
    }

    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let new_offset: u64 = match pos {
            std::io::SeekFrom::Start(off) => off,
            std::io::SeekFrom::End(off) => {
                let len: i64 = self.contents.lock().unwrap().len().try_into().unwrap();
                (len + off).try_into().unwrap()
            }
            std::io::SeekFrom::Current(off) => {
                let old_offset: i64 = self.offset.try_into().unwrap();
                (old_offset + off).try_into().unwrap()
            }
        };
        self.offset = new_offset.try_into().unwrap();
        Ok(new_offset)
    }

    fn set_len(&self, length: u64) -> std::io::Result<()> {
        self.check_write_access()?;
        self.contents.lock().unwrap_mut().resize(length.try_into().unwrap(), 0);
        Ok(())
    }

    fn sync_all(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn sync_file_range_before_write_after(&self, _offset: i64, _nbytes: i64) -> std::io::Result<()> {
        Ok(())
    }

    fn try_lock(&self) -> std::io::Result<()> {
        // always succeeds, there is only one "process" accessing the VFS
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.check_write_access()?;
        let required_len = self.offset + buf.len();
        let mut contents = self.contents.lock();
        let v = contents.unwrap_mut();
        if v.len() < required_len {
            v.resize(required_len, 0);
        }
        &v[self.offset..required_len].copy_from_slice(buf);
        self.offset += buf.len();
        Ok(())
    }
}

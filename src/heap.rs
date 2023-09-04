use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::Arc;

use ebr::{Ebr, Guard};
use fault_injection::{annotate, fallible, maybe};
use fnv::FnvHashSet;
use fs2::FileExt as _;
use pagetable::PageTable;
use rayon::prelude::*;

use crate::{Allocator, CollectionId, DeferredFree, MetadataStore, NodeId};

const WARN: &str = "DO_NOT_PUT_YOUR_FILES_HERE";
const N_SLABS: usize = 78;

const SLAB_SIZES: [usize; N_SLABS] = [
    64,     // 0x40
    80,     // 0x50
    96,     // 0x60
    112,    // 0x70
    128,    // 0x80
    160,    // 0xa0
    192,    // 0xc0
    224,    // 0xe0
    256,    // 0x100
    320,    // 0x140
    384,    // 0x180
    448,    // 0x1c0
    512,    // 0x200
    640,    // 0x280
    768,    // 0x300
    896,    // 0x380
    1024,   // 0x400
    1280,   // 0x500
    1536,   // 0x600
    1792,   // 0x700
    2048,   // 0x800
    2560,   // 0xa00
    3072,   // 0xc00
    3584,   // 0xe00
    4096,   // 0x1000
    5120,   // 0x1400
    6144,   // 0x1800
    7168,   // 0x1c00
    8192,   // 0x2000
    10240,  // 0x2800
    12288,  // 0x3000
    14336,  // 0x3800
    16384,  // 0x4000
    20480,  // 0x5000
    24576,  // 0x6000
    28672,  // 0x7000
    32768,  // 0x8000
    40960,  // 0xa000
    49152,  // 0xc000
    57344,  // 0xe000
    65536,  // 0x10000
    98304,  // 0x1a000
    131072, // 0x20000
    163840, // 0x28000
    196608,
    262144,
    393216,
    524288,
    786432,
    1048576,
    1572864,
    2097152,
    3145728,
    4194304,
    6291456,
    8388608,
    12582912,
    16777216,
    25165824,
    33554432,
    50331648,
    67108864,
    100663296,
    134217728,
    201326592,
    268435456,
    402653184,
    536870912,
    805306368,
    1073741824,
    1610612736,
    2147483648,
    3221225472,
    4294967296,
    6442450944,
    8589934592,
    12884901888,
    17179869184,
];

const fn overhead_for_size(size: usize) -> usize {
    if size + 5 <= u8::MAX as usize {
        // crc32 + 1 byte frame
        5
    } else if size + 6 <= u16::MAX as usize {
        // crc32 + 2 byte frame
        6
    } else if size + 8 <= u32::MAX as usize {
        // crc32 + 4 byte frame
        8
    } else {
        // crc32 + 8 byte frame
        12
    }
}

fn slab_for_size(size: usize) -> u8 {
    let total_size = size + overhead_for_size(size);
    for idx in 0..SLAB_SIZES.len() {
        if SLAB_SIZES[idx] >= total_size {
            return idx as u8;
        }
    }
    u8::MAX
}

pub use inline_array::InlineArray;

#[derive(Debug, Clone)]
pub struct Stats {}

#[derive(Debug, Clone)]
pub struct Config {}

#[derive(Debug)]
pub(crate) struct NodeRecovery {
    pub node_id: NodeId,
    pub collection_id: CollectionId,
    pub metadata: InlineArray,
}

pub(crate) struct HeapRecovery {
    pub heap: Heap,
    pub recovered_nodes: Vec<NodeRecovery>,
    pub was_recovered: bool,
}

enum PersistentSettings {
    V1 { leaf_fanout: u64 },
}

impl PersistentSettings {
    // NB: should only be called with a directory lock already exclusively acquired
    fn verify_or_store<P: AsRef<Path>>(
        &self,
        path: P,
        _directory_lock: &std::fs::File,
    ) -> io::Result<()> {
        let settings_path = path.as_ref().join("durability_cookie");

        match std::fs::read(&settings_path) {
            Ok(previous_bytes) => {
                let previous =
                    PersistentSettings::deserialize(&previous_bytes)?;
                self.check_compatibility(&previous)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::write(settings_path, &self.serialize())
            }
            Err(e) => Err(e),
        }
    }

    fn deserialize(buf: &[u8]) -> io::Result<PersistentSettings> {
        let mut cursor = buf;
        let mut buf = [0_u8; 64];
        cursor.read_exact(&mut buf)?;

        let version = u16::from_le_bytes([buf[0], buf[1]]);

        let crc_actual = (crc32fast::hash(&buf[0..60]) ^ 0xAF).to_le_bytes();
        let crc_expected = &buf[60..];

        if crc_actual != crc_expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "encountered corrupted settings cookie with mismatched CRC.",
            ));
        }

        match version {
            1 => {
                let leaf_fanout = u64::from_le_bytes(buf[2..10].try_into().unwrap());
                Ok(PersistentSettings::V1 { leaf_fanout })
            }
            _ => {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "encountered unknown version number when reading settings cookie"
                ))
            }
        }
    }

    fn check_compatibility(
        &self,
        other: &PersistentSettings,
    ) -> io::Result<()> {
        use PersistentSettings::*;

        match (self, other) {
            (V1 { leaf_fanout: lf1 }, V1 { leaf_fanout: lf2 }) => {
                if lf1 != lf2 {
                    Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            format!(
                                "sled was already opened with a LEAF_FANOUT const generic of {}, \
                                and this may not be changed after initial creation. Please use \
                                Db::import / Db::export to migrate, if you wish to change the \
                                system's format.", lf2
                            )
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }

    fn serialize(&self) -> Vec<u8> {
        // format: 64 bytes in total, with the last 4 being a LE crc32
        // first 2 are LE version number
        let mut buf = vec![];

        match self {
            PersistentSettings::V1 { leaf_fanout } => {
                // LEAF_FANOUT: 8 bytes LE
                let version: [u8; 2] = 1_u16.to_le_bytes();
                buf.extend_from_slice(&version);

                buf.extend_from_slice(&leaf_fanout.to_le_bytes());
            }
        }

        // zero-pad the buffer
        assert!(buf.len() < 60);
        buf.resize(60, 0);

        let hash: u32 = crc32fast::hash(&buf) ^ 0xAF;
        let hash_bytes: [u8; 4] = hash.to_le_bytes();
        buf.extend_from_slice(&hash_bytes);

        // keep the buffer to 64 bytes for easy parsing over time.
        assert_eq!(buf.len(), 64);

        buf
    }
}

pub(crate) fn recover<P: AsRef<Path>>(
    path: P,
    leaf_fanout: usize,
) -> io::Result<HeapRecovery> {
    let path = path.as_ref();
    log::trace!("recovering Heap at {:?}", path);
    let slabs_dir = path.join("slabs");

    // initialize directories if not present
    let mut was_recovered = true;
    for p in [path, &slabs_dir] {
        if let Err(e) = fs::read_dir(p) {
            if e.kind() == io::ErrorKind::NotFound {
                fallible!(fs::create_dir_all(p));
                was_recovered = false;
            }
        }
    }

    let _ = fs::File::create(path.join(WARN));

    let mut file_lock_opts = fs::OpenOptions::new();
    file_lock_opts.create(false).read(false).write(false);
    let directory_lock = fallible!(fs::File::open(&path));
    fallible!(directory_lock.try_lock_exclusive());

    let persistent_settings =
        PersistentSettings::V1 { leaf_fanout: leaf_fanout as u64 };

    persistent_settings.verify_or_store(path, &directory_lock)?;

    let (metadata_store, recovered_metadata) =
        MetadataStore::recover(path.join("metadata"))?;

    let pt = PageTable::<AtomicU64>::default();
    let mut recovered_nodes =
        Vec::<NodeRecovery>::with_capacity(recovered_metadata.len());
    let mut node_ids: FnvHashSet<u64> = Default::default();
    let mut slots_per_slab: [FnvHashSet<u64>; N_SLABS] =
        core::array::from_fn(|_| Default::default());
    for update_metadata in recovered_metadata {
        match update_metadata {
            UpdateMetadata::Store {
                node_id,
                collection_id,
                location,
                metadata,
            } => {
                node_ids.insert(node_id.0);
                let slab_address = SlabAddress::from(location);
                slots_per_slab[slab_address.slab_id as usize]
                    .insert(slab_address.slot());
                pt.get(node_id.0).store(location.get(), Ordering::Relaxed);
                recovered_nodes.push(NodeRecovery {
                    node_id,
                    collection_id,
                    metadata: metadata.clone(),
                });
            }
            UpdateMetadata::Free { .. } => {
                unreachable!()
            }
        }
    }

    let mut slabs = vec![];
    let mut slab_opts = fs::OpenOptions::new();
    slab_opts.create(true).read(true).write(true);
    for i in 0..N_SLABS {
        let slot_size = SLAB_SIZES[i];
        let slab_path = slabs_dir.join(format!("{}", slot_size));

        let file = fallible!(slab_opts.open(slab_path));

        slabs.push(Slab {
            slot_size,
            file,
            slot_allocator: Arc::new(Allocator::from_allocated(
                &slots_per_slab[i],
            )),
        })
    }

    log::info!("recovery of Heap at {:?} complete", path);

    Ok(HeapRecovery {
        heap: Heap {
            slabs: Arc::new(slabs.try_into().unwrap()),
            path: path.into(),
            object_id_allocator: Arc::new(Allocator::from_allocated(&node_ids)),
            pt,
            global_error: metadata_store.get_global_error_arc(),
            metadata_store: Arc::new(metadata_store),
            directory_lock: Arc::new(directory_lock),
            free_ebr: Ebr::default(),
        },
        recovered_nodes,
        was_recovered,
    })
}

struct SlabAddress {
    slab_id: u8,
    slab_slot: [u8; 7],
}

impl SlabAddress {
    fn from_slab_slot(slab: u8, slot: u64) -> SlabAddress {
        let slot_bytes = slot.to_be_bytes();

        assert_eq!(slot_bytes[0], 0);

        SlabAddress {
            slab_id: slab,
            slab_slot: slot_bytes[1..].try_into().unwrap(),
        }
    }

    fn slot(&self) -> u64 {
        u64::from_be_bytes([
            0,
            self.slab_slot[0],
            self.slab_slot[1],
            self.slab_slot[2],
            self.slab_slot[3],
            self.slab_slot[4],
            self.slab_slot[5],
            self.slab_slot[6],
        ])
    }
}

impl From<NonZeroU64> for SlabAddress {
    fn from(i: NonZeroU64) -> SlabAddress {
        let i = i.get();
        let bytes = i.to_be_bytes();
        SlabAddress {
            slab_id: bytes[0] - 1,
            slab_slot: bytes[1..].try_into().unwrap(),
        }
    }
}

impl Into<NonZeroU64> for SlabAddress {
    fn into(self) -> NonZeroU64 {
        NonZeroU64::new(u64::from_be_bytes([
            self.slab_id + 1,
            self.slab_slot[0],
            self.slab_slot[1],
            self.slab_slot[2],
            self.slab_slot[3],
            self.slab_slot[4],
            self.slab_slot[5],
            self.slab_slot[6],
        ]))
        .unwrap()
    }
}

#[cfg(unix)]
mod sys_io {
    use std::io;
    use std::os::unix::fs::FileExt;

    use super::*;

    pub fn read_exact_at<F: FileExt>(
        file: &F,
        buf: &mut [u8],
        offset: u64,
    ) -> io::Result<()> {
        maybe!(file.read_exact_at(buf, offset))
    }

    pub fn write_all_at<F: FileExt>(
        file: &F,
        buf: &[u8],
        offset: u64,
    ) -> io::Result<()> {
        maybe!(file.write_all_at(buf, offset))
    }
}

#[cfg(windows)]
mod sys_io {
    use std::os::windows::fs::FileExt;

    use super::*;

    pub fn read_exact_at<F: FileExt>(
        file: &F,
        mut buf: &mut [u8],
        mut offset: u64,
    ) -> io::Result<()> {
        while !buf.is_empty() {
            match maybe!(file.seek_read(buf, offset)) {
                Ok(0) => break,
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(annotate!(e)),
            }
        }
        if !buf.is_empty() {
            Err(annotate!(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer"
            )))
        } else {
            Ok(())
        }
    }

    pub fn write_all_at<F: FileExt>(
        file: &F,
        mut buf: &[u8],
        mut offset: u64,
    ) -> io::Result<()> {
        while !buf.is_empty() {
            match maybe!(file.seek_write(buf, offset)) {
                Ok(0) => {
                    return Err(annotate!(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    )));
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(annotate!(e)),
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Slab {
    file: fs::File,
    slot_size: usize,
    slot_allocator: Arc<Allocator>,
}

impl Slab {
    fn maintenance(&self) -> io::Result<usize> {
        // TODO compact
        Ok(0)
    }

    fn read(
        &self,
        slot: u64,
        _guard: &mut Guard<'_, DeferredFree, 1>,
    ) -> io::Result<Vec<u8>> {
        let mut data = Vec::with_capacity(self.slot_size);
        unsafe {
            data.set_len(self.slot_size);
        }

        let whence = self.slot_size as u64 * slot;

        sys_io::read_exact_at(&self.file, &mut data, whence)?;

        let hash_actual: [u8; 4] =
            (crc32fast::hash(&data[..self.slot_size - 4]) ^ 0xAF).to_le_bytes();
        let hash_expected = &data[self.slot_size - 4..];

        if hash_expected != hash_actual {
            return Err(annotate!(io::Error::new(
                io::ErrorKind::InvalidData,
                "crc mismatch - data corruption detected"
            )));
        }

        let len: usize = if self.slot_size <= u8::MAX as usize {
            // crc32 + 1 byte frame
            usize::from(data[self.slot_size - 5])
        } else if self.slot_size <= u16::MAX as usize {
            // crc32 + 2 byte frame
            let mut size_bytes: [u8; 2] = [0; 2];
            size_bytes
                .copy_from_slice(&data[self.slot_size - 6..self.slot_size - 4]);
            usize::from(u16::from_le_bytes(size_bytes))
        } else if self.slot_size <= u32::MAX as usize {
            // crc32 + 4 byte frame
            let mut size_bytes: [u8; 4] = [0; 4];
            size_bytes
                .copy_from_slice(&data[self.slot_size - 8..self.slot_size - 4]);
            usize::try_from(u32::from_le_bytes(size_bytes)).unwrap()
        } else {
            // crc32 + 8 byte frame
            let mut size_bytes: [u8; 8] = [0; 8];
            size_bytes.copy_from_slice(
                &data[self.slot_size - 12..self.slot_size - 4],
            );
            usize::try_from(u64::from_le_bytes(size_bytes)).unwrap()
        };

        data.truncate(len);

        Ok(data)
    }

    fn write(&self, slot: u64, mut data: Vec<u8>) -> io::Result<()> {
        let len = data.len();

        assert!(len + overhead_for_size(data.len()) <= self.slot_size);

        data.resize(self.slot_size, 0);

        if self.slot_size <= u8::MAX as usize {
            // crc32 + 1 byte frame
            data[self.slot_size - 5] = u8::try_from(len).unwrap();
        } else if self.slot_size <= u16::MAX as usize {
            // crc32 + 2 byte frame
            let size_bytes: [u8; 2] = u16::try_from(len).unwrap().to_le_bytes();
            data[self.slot_size - 6..self.slot_size - 4]
                .copy_from_slice(&size_bytes);
        } else if self.slot_size <= u32::MAX as usize {
            // crc32 + 4 byte frame
            let size_bytes: [u8; 4] = u32::try_from(len).unwrap().to_le_bytes();
            data[self.slot_size - 8..self.slot_size - 4]
                .copy_from_slice(&size_bytes);
        } else {
            // crc32 + 8 byte frame
            let size_bytes: [u8; 8] = u64::try_from(len).unwrap().to_le_bytes();
            data[self.slot_size - 12..self.slot_size - 4]
                .copy_from_slice(&size_bytes);
        }

        let hash: [u8; 4] =
            (crc32fast::hash(&data[..self.slot_size - 4]) ^ 0xAF).to_le_bytes();
        data[self.slot_size - 4..].copy_from_slice(&hash);

        let whence = self.slot_size as u64 * slot;

        sys_io::write_all_at(&self.file, &data, whence)
    }
}

fn set_error(
    global_error: &AtomicPtr<(io::ErrorKind, String)>,
    error: &io::Error,
) {
    let kind = error.kind();
    let reason = error.to_string();

    let boxed = Box::new((kind, reason));
    let ptr = Box::into_raw(boxed);

    if global_error
        .compare_exchange(
            std::ptr::null_mut(),
            ptr,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
        .is_err()
    {
        // global fatal error already installed, drop this one
        unsafe {
            drop(Box::from_raw(ptr));
        }
    }
}

#[derive(Debug)]
pub(crate) enum Update {
    Store {
        node_id: NodeId,
        collection_id: CollectionId,
        metadata: InlineArray,
        data: Vec<u8>,
    },
    Free {
        node_id: NodeId,
        collection_id: CollectionId,
    },
}

#[derive(Debug, PartialOrd, Ord, PartialEq, Eq)]
pub(crate) enum UpdateMetadata {
    Store {
        node_id: NodeId,
        collection_id: CollectionId,
        metadata: InlineArray,
        location: NonZeroU64,
    },
    Free {
        node_id: NodeId,
        collection_id: CollectionId,
    },
}

impl UpdateMetadata {
    pub fn node_id(&self) -> NodeId {
        match self {
            UpdateMetadata::Store { node_id, .. }
            | UpdateMetadata::Free { node_id, .. } => *node_id,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Heap {
    path: PathBuf,
    slabs: Arc<[Slab; N_SLABS]>,
    pt: PageTable<AtomicU64>,
    object_id_allocator: Arc<Allocator>,
    metadata_store: Arc<MetadataStore>,
    free_ebr: Ebr<DeferredFree, 1>,
    global_error: Arc<AtomicPtr<(io::ErrorKind, String)>>,
    #[allow(unused)]
    directory_lock: Arc<fs::File>,
}

impl fmt::Debug for Heap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Heap")
            .field("path", &self.path)
            .field("stats", &self.stats())
            .finish()
    }
}

impl Heap {
    pub fn get_global_error_arc(
        &self,
    ) -> Arc<AtomicPtr<(io::ErrorKind, String)>> {
        self.global_error.clone()
    }

    fn check_error(&self) -> io::Result<()> {
        let err_ptr: *const (io::ErrorKind, String) =
            self.global_error.load(Ordering::Acquire);

        if err_ptr.is_null() {
            Ok(())
        } else {
            let deref: &(io::ErrorKind, String) = unsafe { &*err_ptr };
            Err(io::Error::new(deref.0, deref.1.clone()))
        }
    }

    fn set_error(&self, error: &io::Error) {
        set_error(&self.global_error, error);
    }

    pub fn maintenance(&self) -> io::Result<usize> {
        for slab in self.slabs.iter() {
            slab.maintenance()?;
        }

        Ok(0)
    }

    pub fn stats(&self) -> Stats {
        Stats {}
    }

    pub fn read(&self, object_id: u64) -> io::Result<Vec<u8>> {
        self.check_error()?;

        let mut trace_spin = false;
        let mut guard = self.free_ebr.pin();
        let slab_address = loop {
            let location_u64 = self.pt.get(object_id).load(Ordering::Acquire);

            if let Some(nzu) = NonZeroU64::new(location_u64) {
                break SlabAddress::from(nzu);
            } else {
                if !trace_spin {
                    log::warn!("spinning for paged-out object to be persisted");
                    trace_spin = true;
                }
                std::thread::yield_now();
            }
        };

        let slab = &self.slabs[usize::from(slab_address.slab_id)];

        match slab.read(slab_address.slot(), &mut guard) {
            Ok(bytes) => Ok(bytes),
            Err(e) => {
                self.set_error(&e);
                Err(e)
            }
        }
    }

    pub fn write_batch(&self, batch: Vec<Update>) -> io::Result<()> {
        self.check_error()?;
        let mut guard = self.free_ebr.pin();

        let slabs = &self.slabs;

        let map_closure = |update: Update| match update {
            Update::Store { node_id, collection_id, metadata, data } => {
                let slab_id = slab_for_size(data.len());
                let slab = &slabs[usize::from(slab_id)];
                let slot = slab.slot_allocator.allocate();
                let new_location = SlabAddress::from_slab_slot(slab_id, slot);
                let new_location_nzu: NonZeroU64 = new_location.into();

                let complete_durability_pipeline =
                    maybe!(slab.write(slot, data));

                if let Err(e) = complete_durability_pipeline {
                    // can immediately free slot as the
                    slab.slot_allocator.free(slot);
                    return Err(e);
                }
                Ok(UpdateMetadata::Store {
                    node_id,
                    collection_id,
                    metadata,
                    location: new_location_nzu,
                })
            }
            Update::Free { node_id, collection_id } => {
                Ok(UpdateMetadata::Free { node_id, collection_id })
            }
        };

        let metadata_batch_res: io::Result<Vec<UpdateMetadata>> =
            batch.into_par_iter().map(map_closure).collect();

        let metadata_batch = match metadata_batch_res {
            Ok(mut mb) => {
                // TODO evaluate impact : cost ratio of this sort
                mb.par_sort_unstable();
                mb
            }
            Err(e) => {
                self.set_error(&e);
                return Err(e);
            }
        };

        // make metadata durable
        if let Err(e) = self.metadata_store.insert_batch(&metadata_batch) {
            self.set_error(&e);
            return Err(e);
        }

        // reclaim previous disk locations for future writes
        for update_metadata in metadata_batch {
            let (node_id, new_location) = match update_metadata {
                UpdateMetadata::Store { node_id, location, .. } => {
                    (node_id, location.get())
                }
                UpdateMetadata::Free { node_id, .. } => {
                    guard.defer_drop(DeferredFree {
                        allocator: self.object_id_allocator.clone(),
                        freed_slot: node_id.0,
                    });
                    (node_id, 0)
                }
            };

            let last_u64 =
                self.pt.get(node_id.0).swap(new_location, Ordering::Release);

            if let Some(nzu) = NonZeroU64::new(last_u64) {
                let last_address = SlabAddress::from(nzu);

                guard.defer_drop(DeferredFree {
                    allocator: self.slabs[usize::from(last_address.slab_id)]
                        .slot_allocator
                        .clone(),
                    freed_slot: last_address.slot(),
                });
            }
        }

        Ok(())
    }

    pub fn allocate_object_id(&self) -> u64 {
        self.object_id_allocator.allocate()
    }
}

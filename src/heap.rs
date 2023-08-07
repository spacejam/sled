use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt;
use std::fs;
use std::io;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_queue::SegQueue;
use ebr::{Ebr, Guard};
use fault_injection::{annotate, fallible, maybe};
use fnv::FnvHashSet;
use fs2::FileExt as _;
use pagetable::PageTable;
use rayon::prelude::*;

use crate::metadata_store::MetadataStore;

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
pub struct Config {
    pub path: PathBuf,
}

pub fn recover<P: AsRef<Path>>(
    storage_directory: P,
) -> io::Result<(Heap, Vec<(u64, InlineArray)>)> {
    Heap::recover(&Config { path: storage_directory.as_ref().into() })
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

#[derive(Default, Debug)]
struct Allocator {
    free_and_pending: Mutex<BinaryHeap<Reverse<u64>>>,
    free_queue: SegQueue<u64>,
    next_to_allocate: AtomicU64,
}

impl Allocator {
    fn from_allocated(allocated: &FnvHashSet<u64>) -> Allocator {
        let mut heap = BinaryHeap::<Reverse<u64>>::default();
        let max = allocated.iter().copied().max();

        for i in 0..max.unwrap_or(0) {
            if !allocated.contains(&i) {
                heap.push(Reverse(i));
            }
        }

        Allocator {
            free_and_pending: Mutex::new(heap),
            free_queue: SegQueue::default(),
            next_to_allocate: max.map(|m| m + 1).unwrap_or(0).into(),
        }
    }

    fn allocate(&self) -> u64 {
        let mut free = self.free_and_pending.lock().unwrap();
        while let Some(free_id) = self.free_queue.pop() {
            free.push(Reverse(free_id));
        }
        let pop_attempt = free.pop();

        if let Some(id) = pop_attempt {
            id.0
        } else {
            self.next_to_allocate.fetch_add(1, Ordering::Release)
        }
    }

    fn free(&self, id: u64) {
        if let Ok(mut free) = self.free_and_pending.try_lock() {
            while let Some(free_id) = self.free_queue.pop() {
                free.push(Reverse(free_id));
            }
            free.push(Reverse(id));
        } else {
            self.free_queue.push(id);
        }
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
            crc32fast::hash(&data[..self.slot_size - 4]).to_le_bytes();
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
            crc32fast::hash(&data[..self.slot_size - 4]).to_le_bytes();
        data[self.slot_size - 4..].copy_from_slice(&hash);

        let whence = self.slot_size as u64 * slot;

        sys_io::write_all_at(&self.file, &data, whence)
    }
}

struct DeferredFree {
    allocator: Arc<Allocator>,
    freed_slot: u64,
}

impl Drop for DeferredFree {
    fn drop(&mut self) {
        self.allocator.free(self.freed_slot)
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

#[derive(Clone)]
pub struct Heap {
    config: Config,
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
            .field("config", &self.config.path)
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

    pub fn recover(
        config: &Config,
    ) -> io::Result<(Heap, Vec<(u64, InlineArray)>)> {
        log::trace!("recovering Heap at {:?}", config.path);
        let slabs_dir = config.path.join("slabs");

        // initialize directories if not present
        for p in [&config.path, &slabs_dir] {
            if let Err(e) = fs::read_dir(p) {
                if e.kind() == io::ErrorKind::NotFound {
                    fallible!(fs::create_dir_all(p));
                }
            }
        }

        let _ = fs::File::create(config.path.join(WARN));

        let mut file_lock_opts = fs::OpenOptions::new();
        file_lock_opts.create(false).read(false).write(false);
        let directory_lock = fallible!(fs::File::open(&config.path));
        fallible!(directory_lock.try_lock_exclusive());

        let (metadata_store, recovered_metadata) =
            MetadataStore::recover(config.path.join("metadata"))?;

        let pt = PageTable::<AtomicU64>::default();
        let mut user_data =
            Vec::<(u64, InlineArray)>::with_capacity(recovered_metadata.len());
        let mut object_ids: FnvHashSet<u64> = Default::default();
        let mut slots_per_slab: [FnvHashSet<u64>; N_SLABS] =
            core::array::from_fn(|_| Default::default());
        for (k, location, data) in recovered_metadata {
            object_ids.insert(k);
            let slab_address = SlabAddress::from(location);
            slots_per_slab[slab_address.slab_id as usize]
                .insert(slab_address.slot());
            pt.get(k).store(location.get(), Ordering::Relaxed);
            user_data.push((k, data.clone()));
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

        log::info!("recovery of Heap at {:?} complete", config.path);
        Ok((
            Heap {
                slabs: Arc::new(slabs.try_into().unwrap()),
                config: config.clone(),
                object_id_allocator: Arc::new(Allocator::from_allocated(
                    &object_ids,
                )),
                pt,
                global_error: metadata_store.get_global_error_arc(),
                metadata_store: Arc::new(metadata_store),
                directory_lock: Arc::new(directory_lock),
                free_ebr: Ebr::default(),
            },
            user_data,
        ))
    }

    pub fn maintenance(&self) -> io::Result<usize> {
        // TODO
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

    pub fn write_batch<I>(&self, batch: I) -> io::Result<()>
    where
        I: Sized + IntoIterator<Item = (u64, Option<(InlineArray, Vec<u8>)>)>,
    {
        self.check_error()?;
        let mut guard = self.free_ebr.pin();

        let batch: Vec<(u64, Option<(InlineArray, Vec<u8>)>)> = batch
            .into_iter()
            //.map(|(key, val_opt)| (key, val_opt.map(|(user_data, b)| (user_data, b.as_ref()))))
            .collect();

        let slabs = &self.slabs;
        let metadata_batch_res: io::Result<
            Vec<(u64, Option<(NonZeroU64, InlineArray)>)>,
        > = batch
            .into_par_iter()
            .map(
                |(object_id, val_opt): (
                    u64,
                    Option<(InlineArray, Vec<u8>)>,
                )| {
                    let new_meta = if let Some((user_data, bytes)) = val_opt {
                        let slab_id = slab_for_size(bytes.len());
                        let slab = &slabs[usize::from(slab_id)];
                        let slot = slab.slot_allocator.allocate();
                        let new_location =
                            SlabAddress::from_slab_slot(slab_id, slot);
                        let new_location_nzu: NonZeroU64 = new_location.into();

                        let complete_durability_pipeline =
                            maybe!(slab.write(slot, bytes));

                        if let Err(e) = complete_durability_pipeline {
                            // can immediately free slot as the
                            slab.slot_allocator.free(slot);
                            return Err(e);
                        }
                        Some((new_location_nzu, user_data))
                    } else {
                        None
                    };

                    Ok((object_id, new_meta))
                },
            )
            .collect();

        let metadata_batch = match metadata_batch_res {
            Ok(mb) => mb,
            Err(e) => {
                self.set_error(&e);
                return Err(e);
            }
        };

        if let Err(e) = self.metadata_store.insert_batch(metadata_batch.clone())
        {
            self.set_error(&e);

            // this is very cold, so it's fine if it's not fast
            for (_object_id, value_opt) in metadata_batch {
                let (new_location_u64, _user_data) = value_opt.unwrap();
                let new_location = SlabAddress::from(new_location_u64);
                let slab_id = new_location.slab_id;
                let slab = &self.slabs[usize::from(slab_id)];
                slab.slot_allocator.free(new_location.slot());
            }
            return Err(e);
        }

        // now we can update in-memory metadata

        for (object_id, value_opt) in metadata_batch {
            let new_location = if let Some((nl, _user_data)) = value_opt {
                nl.get()
            } else {
                0
            };

            let last_u64 =
                self.pt.get(object_id).swap(new_location, Ordering::Release);

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

    #[allow(unused)]
    pub fn free(&self, object_id: u64) -> io::Result<()> {
        let mut guard = self.free_ebr.pin();
        if let Err(e) = self.metadata_store.insert_batch([(object_id, None)]) {
            self.set_error(&e);
            return Err(e);
        }
        let last_u64 = self.pt.get(object_id).swap(0, Ordering::Release);
        if let Some(nzu) = NonZeroU64::new(last_u64) {
            let last_address = SlabAddress::from(nzu);

            guard.defer_drop(DeferredFree {
                allocator: self.slabs[usize::from(last_address.slab_id)]
                    .slot_allocator
                    .clone(),
                freed_slot: last_address.slot(),
            });
        }

        Ok(())
    }
}

// TODO rm allow(unused)
#![allow(unused)]
#![allow(unsafe_code)]

use std::{
    convert::{TryFrom, TryInto},
    fmt::{self, Debug},
    fs::File,
    mem::{transmute, MaybeUninit},
    path::Path,
    sync::{
        atomic::{AtomicU32, Ordering::Acquire},
        Arc,
    },
};

use crossbeam_epoch::pin;

use crate::{
    pagecache::{pread_exact, pwrite_all, MessageKind},
    stack::Stack,
    Error, Lsn, Result,
};

#[cfg(not(feature = "testing"))]
const MIN_SZ: u64 = 64 * 1024;

#[cfg(feature = "testing")]
const MIN_SZ: u64 = 32;

const MIN_TRAILING_ZEROS: u64 = MIN_SZ.trailing_zeros() as u64;

pub type SlabId = u8;
pub type SlabIdx = u32;

/// A unique identifier for a particular slot in the heap
#[derive(Clone, Copy, PartialOrd, Ord, Eq, PartialEq, Hash)]
pub struct HeapId(pub u64);

impl Debug for HeapId {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        let (slab, idx) = self.decompose();
        f.debug_struct("HeapId")
            .field("slab", &slab)
            .field("idx", &idx)
            .finish()
    }
}

impl HeapId {
    pub fn decompose(&self) -> (SlabId, SlabIdx) {
        const IDX_MASK: u64 = (1 << 32) - 1;
        let slab_id = u8::try_from((self.0 >> 32).trailing_zeros()).unwrap();
        let slab_idx = u32::try_from(self.0 & IDX_MASK).unwrap();
        (slab_id, slab_idx)
    }

    pub fn compose(slab_id: SlabId, slab_idx: SlabIdx) -> HeapId {
        let slab = 1 << (32 + slab_id as u64);
        let heap_id = slab | slab_idx as u64;
        HeapId(heap_id)
    }

    fn offset(&self) -> u64 {
        let (slab_id, idx) = self.decompose();
        slab_id_to_size(slab_id) * idx as u64
    }

    fn slab_size(&self) -> u64 {
        let (slab_id, idx) = self.decompose();
        slab_id_to_size(slab_id)
    }
}

pub(crate) fn slab_size(size: u64) -> u64 {
    slab_id_to_size(size_to_slab_id(size))
}

fn slab_id_to_size(slab_id: u8) -> u64 {
    1 << (MIN_TRAILING_ZEROS + slab_id as u64)
}

fn size_to_slab_id(size: u64) -> SlabId {
    // find the power of 2 that is at least 64k
    let normalized_size = std::cmp::max(MIN_SZ, size.next_power_of_two());

    // drop the lowest unused bits
    let rebased_size = normalized_size >> MIN_TRAILING_ZEROS;

    u8::try_from(rebased_size.trailing_zeros()).unwrap()
}

pub(crate) struct Reservation {
    slab_free: Arc<Stack<u32>>,
    completed: bool,
    file: File,
    pub heap_id: HeapId,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.completed {
            let (slab_id, idx) = self.heap_id.decompose();
            self.slab_free.push(idx, &pin());
        }
    }
}

impl Reservation {
    pub fn complete(mut self, data: &[u8]) -> Result<HeapId> {
        log::trace!("writing heap slab slot {:?}", self.heap_id);
        assert_eq!(data.len() as u64, self.heap_id.slab_size());

        pwrite_all(&self.file, data, self.heap_id.offset())?;
        self.file.sync_all()?;

        // if this is not reached due to an IO error,
        // the offset will be returned to the Slab in Drop
        self.completed = true;

        Ok(self.heap_id)
    }

    pub fn abort(self) {
        // actual logic in Drop
    }
}

#[derive(Debug)]
pub(crate) struct Heap {
    // each slab stores
    // items that are double
    // the size of the previous,
    // ranging from 64k in the
    // smallest slab to 2^48 in
    // the last.
    slabs: [Slab; 32],
}

impl Heap {
    pub fn start<P: AsRef<Path>>(p: P) -> Result<Heap> {
        let mut slabs: [MaybeUninit<Slab>; 32] = unsafe { std::mem::zeroed() };

        for slab_id in 0..32 {
            let slab = Slab::start(&p, slab_id)?;
            slabs[slab_id as usize] = MaybeUninit::new(slab);
        }

        Ok(Heap { slabs: unsafe { transmute(slabs) } })
    }

    pub fn gc_unknown_blobs(
        &self,
        _snapshot: &crate::pagecache::Snapshot,
    ) -> Result<()> {
        //TODO todo!()
        Ok(())
    }

    pub fn read(
        &self,
        heap_id: HeapId,
        original_lsn: Lsn,
        use_compression: bool,
    ) -> Result<(MessageKind, Vec<u8>)> {
        log::trace!("Heap::read({:?})", heap_id);
        let (slab_id, slab_idx) = heap_id.decompose();
        self.slabs[slab_id as usize].read(
            slab_idx,
            original_lsn,
            use_compression,
        )
    }

    pub fn free(&self, heap_id: HeapId) {
        log::trace!("Heap::free({:?})", heap_id);
        let (slab_id, slab_idx) = heap_id.decompose();
        self.slabs[slab_id as usize].free(slab_idx)
    }

    pub fn reserve(&self, size: u64) -> Reservation {
        assert!(size < 1 << 48);
        let slab_id = size_to_slab_id(size);
        let ret = self.slabs[slab_id as usize].reserve(size);
        log::trace!("Heap::reserve({}) -> {:?}", size, ret.heap_id);
        ret
    }
}

#[derive(Debug)]
struct Slab {
    file: File,
    slab_id: u8,
    tip: AtomicU32,
    free: Arc<Stack<u32>>,
}

impl Slab {
    pub fn start<P: AsRef<Path>>(directory: P, slab_id: u8) -> Result<Slab> {
        let bs = slab_id_to_size(slab_id);
        let free = Arc::new(Stack::default());

        let mut options = std::fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);

        let file =
            options.open(directory.as_ref().join(format!("{}", slab_id)))?;
        let len = file.metadata()?.len();
        let max_idx = len / bs;
        log::trace!(
            "starting heap slab for sizes of {}. tip: {} max idx: {}",
            bs,
            len,
            max_idx
        );
        let tip = AtomicU32::new(u32::try_from(max_idx).unwrap());

        Ok(Slab { file, slab_id, tip, free })
    }

    fn read(
        &self,
        slab_idx: SlabIdx,
        original_lsn: Lsn,
        use_compression: bool,
    ) -> Result<(MessageKind, Vec<u8>)> {
        use std::os::unix::fs::FileExt;

        let bs = slab_id_to_size(self.slab_id);
        let offset = slab_idx as u64 * bs;

        log::trace!("reading heap slab slot {} at offset {}", slab_idx, offset);

        let mut heap_buf = vec![0; bs as usize];

        self.file.read_exact_at(&mut heap_buf, offset)?;

        let stored_crc =
            u32::from_le_bytes(heap_buf[1..5].as_ref().try_into().unwrap());

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&heap_buf[0..1]);
        hasher.update(&heap_buf[5..]);
        let actual_crc = hasher.finalize();

        if actual_crc == stored_crc {
            let actual_lsn = Lsn::from_le_bytes(
                heap_buf[5..13].as_ref().try_into().unwrap(),
            );
            if actual_lsn != original_lsn {
                log::error!(
                    "heap slot lsn {} does not match expected original lsn {}",
                    actual_lsn,
                    original_lsn
                );
                return Err(Error::corruption(None));
            }
            let buf = heap_buf[13..].to_vec();
            let buf = if use_compression {
                crate::pagecache::decompress(buf)
            } else {
                buf
            };
            Ok((MessageKind::from(heap_buf[0]), buf))
        } else {
            log::error!(
                "heap message CRC does not match contents. stored: {} actual: {}",
                stored_crc,
                actual_crc
            );
            return Err(Error::corruption(None));
        }
    }

    fn reserve(&self, size: u64) -> Reservation {
        let idx = if let Some(idx) = self.free.pop(&pin()) {
            log::trace!(
                "reusing heap index {} in slab for sizes of {}",
                idx,
                slab_id_to_size(self.slab_id),
            );
            idx
        } else {
            log::trace!(
                "no free heap slots in slab for sizes of {}",
                slab_id_to_size(self.slab_id),
            );
            self.tip.fetch_add(1, Acquire)
        };

        log::trace!(
            "heap reservation for slot {} in the slab for sizes of {}",
            idx,
            slab_id_to_size(self.slab_id),
        );

        let heap_id = HeapId::compose(self.slab_id, idx);

        Reservation {
            slab_free: self.free.clone(),
            completed: false,
            file: self.file.try_clone().unwrap(),
            heap_id,
        }
    }

    fn free(&self, idx: u32) {
        self.punch_hole(idx);
        self.free.push(idx, &pin());
    }

    fn punch_hole(&self, idx: u32) {
        let bs = slab_id_to_size(self.slab_id);
        let offset = idx as u64 * bs;

        #[cfg(target_os = "linux")]
        {
            use std::{
                os::unix::io::AsRawFd,
                sync::atomic::{AtomicBool, Ordering::Relaxed},
            };

            use libc::{fallocate, FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE};

            static HOLE_PUNCHING_ENABLED: AtomicBool = AtomicBool::new(false);

            if HOLE_PUNCHING_ENABLED.load(Relaxed) {
                let mode = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE;

                let fd = self.file.as_raw_fd();

                let ret =
                    unsafe { fallocate(fd, mode, offset as i64, bs as i64) };

                if ret != 0 {
                    let err = std::io::Error::last_os_error();
                    log::error!(
                        "failed to punch hole in heap file: {:?}. disabling hole punching",
                        err
                    );
                    HOLE_PUNCHING_ENABLED.store(false, Relaxed);
                }
            }
        }
    }
}

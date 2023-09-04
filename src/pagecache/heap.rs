#![allow(unsafe_code)]

use std::{
    convert::{TryFrom, TryInto},
    fmt::{self, Debug},
    fs::File,
    path::Path,
    sync::{
        atomic::{AtomicU32, Ordering::Acquire},
        Arc,
    },
};

use crate::{
    ebr::pin,
    pagecache::{pread_exact, pwrite_all, MessageKind},
    stack::Stack,
    Error, Lsn, Result,
};

#[cfg(not(feature = "for-internal-testing-only"))]
pub(crate) const MIN_SZ: u64 = 32 * 1024;

#[cfg(feature = "for-internal-testing-only")]
pub(crate) const MIN_SZ: u64 = 128;

const MIN_TRAILING_ZEROS: u64 = MIN_SZ.trailing_zeros() as u64;

pub type SlabId = u8;
pub type SlabIdx = u32;

/// A unique identifier for a particular slot in the heap
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Copy, PartialOrd, Ord, Eq, PartialEq, Hash)]
pub struct HeapId {
    pub location: u64,
    pub original_lsn: Lsn,
}

impl Debug for HeapId {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        let (slab, idx, original_lsn) = self.decompose();
        f.debug_struct("HeapId")
            .field("slab", &slab)
            .field("idx", &idx)
            .field("original_lsn", &original_lsn)
            .finish()
    }
}

impl HeapId {
    pub fn decompose(&self) -> (SlabId, SlabIdx, Lsn) {
        const IDX_MASK: u64 = (1 << 32) - 1;
        let slab_id =
            u8::try_from((self.location >> 32).trailing_zeros()).unwrap();
        let slab_idx = u32::try_from(self.location & IDX_MASK).unwrap();
        (slab_id, slab_idx, self.original_lsn)
    }

    pub fn compose(
        slab_id: SlabId,
        slab_idx: SlabIdx,
        original_lsn: Lsn,
    ) -> HeapId {
        let slab = 1 << (32 + u64::from(slab_id));
        let heap_id = slab | u64::from(slab_idx);
        HeapId { location: heap_id, original_lsn }
    }

    fn offset(&self) -> u64 {
        let (slab_id, idx, _) = self.decompose();
        slab_id_to_size(slab_id) * u64::from(idx)
    }

    fn slab_size(&self) -> u64 {
        let (slab_id, _idx, _lsn) = self.decompose();
        slab_id_to_size(slab_id)
    }
}

pub(crate) fn slab_size(size: u64) -> u64 {
    slab_id_to_size(size_to_slab_id(size))
}

fn slab_id_to_size(slab_id: u8) -> u64 {
    1 << (MIN_TRAILING_ZEROS + u64::from(slab_id))
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
    from_tip: bool,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.completed {
            let (_slab_id, idx, _) = self.heap_id.decompose();
            self.slab_free.push(idx, &pin());
        }
    }
}

impl Reservation {
    pub fn complete(mut self, data: &[u8]) -> Result<HeapId> {
        log::trace!(
            "Heap::complete({:?}) to offset {} in file {:?}",
            self.heap_id,
            self.heap_id.offset(),
            self.file
        );
        assert_eq!(data.len() as u64, self.heap_id.slab_size());

        // write data
        pwrite_all(&self.file, data, self.heap_id.offset())?;

        // sync data
        if self.from_tip {
            self.file.sync_all()?;
        } else if cfg!(not(target_os = "linux")) {
            self.file.sync_data()?;
        } else {
            #[allow(clippy::assertions_on_constants)]
            {
                assert!(cfg!(target_os = "linux"));
            }

            #[cfg(target_os = "linux")]
            {
                use std::os::unix::io::AsRawFd;
                let ret = unsafe {
                    libc::sync_file_range(
                        self.file.as_raw_fd(),
                        i64::try_from(self.heap_id.offset()).unwrap(),
                        i64::try_from(data.len()).unwrap(),
                        libc::SYNC_FILE_RANGE_WAIT_BEFORE
                            | libc::SYNC_FILE_RANGE_WRITE
                            | libc::SYNC_FILE_RANGE_WAIT_AFTER,
                    )
                };
                if ret < 0 {
                    let err = std::io::Error::last_os_error();
                    if let Some(libc::ENOSYS) = err.raw_os_error() {
                        self.file.sync_all()?;
                    } else {
                        return Err(err.into());
                    }
                }
            }
        }

        // if this is not reached due to an IO error,
        // the offset will be returned to the Slab in Drop
        self.completed = true;

        Ok(self.heap_id)
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
        let mut slabs_vec = vec![];

        for slab_id in 0..32 {
            let slab = Slab::start(&p, slab_id)?;
            slabs_vec.push(slab);
        }

        let slabs: [Slab; 32] = slabs_vec.try_into().unwrap();

        Ok(Heap { slabs })
    }

    pub fn gc_unknown_items(&self, snapshot: &crate::pagecache::Snapshot) {
        let mut bitmaps = vec![];
        for slab in &self.slabs {
            let tip = slab.tip.load(Acquire) as usize;
            bitmaps.push(vec![0_u64; 1 + (tip / 64)]);
        }

        for page_state in &snapshot.pt {
            for heap_id in page_state.heap_ids() {
                let (slab_id, idx, _lsn) = heap_id.decompose();

                // set the bit for this slot
                let block = idx / 64;
                let bit = idx % 64;
                let bitmask = 1 << bit;
                bitmaps[slab_id as usize][block as usize] |= bitmask;
            }
        }

        let iter = self.slabs.iter().zip(bitmaps);

        for (slab, bitmap) in iter {
            let tip = slab.tip.load(Acquire);

            for idx in 0..tip {
                let block = idx / 64;
                let bit = idx % 64;
                let bitmask = 1 << bit;
                let free = bitmap[block as usize] & bitmask == 0;

                if free {
                    slab.free(idx);
                }
            }
        }
    }

    pub fn read(&self, heap_id: HeapId) -> Result<(MessageKind, Vec<u8>)> {
        log::trace!("Heap::read({:?})", heap_id);
        let (slab_id, slab_idx, original_lsn) = heap_id.decompose();
        self.slabs[slab_id as usize].read(slab_idx, original_lsn)
    }

    pub fn free(&self, heap_id: HeapId) {
        log::trace!("Heap::free({:?})", heap_id);
        let (slab_id, slab_idx, _) = heap_id.decompose();
        self.slabs[slab_id as usize].free(slab_idx)
    }

    pub fn reserve(&self, size: u64, original_lsn: Lsn) -> Reservation {
        assert!(size < 1 << 48);
        let slab_id = size_to_slab_id(size);
        let ret = self.slabs[slab_id as usize].reserve(original_lsn);
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
            options.open(directory.as_ref().join(format!("{:02}", slab_id)))?;
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
    ) -> Result<(MessageKind, Vec<u8>)> {
        let bs = slab_id_to_size(self.slab_id);
        let offset = u64::from(slab_idx) * bs;

        log::trace!("reading heap slab slot {} at offset {}", slab_idx, offset);

        let mut heap_buf = vec![0; usize::try_from(bs).unwrap()];

        pread_exact(&self.file, &mut heap_buf, offset)?;

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
                log::debug!(
                    "heap slot lsn {} does not match expected original lsn {}",
                    actual_lsn,
                    original_lsn
                );
                return Err(Error::corruption(None));
            }
            let buf = heap_buf[13..].to_vec();
            Ok((MessageKind::from(heap_buf[0]), buf))
        } else {
            log::debug!(
                "heap message CRC does not match contents. stored: {} actual: {}",
                stored_crc,
                actual_crc
            );
            Err(Error::corruption(None))
        }
    }

    fn reserve(&self, original_lsn: Lsn) -> Reservation {
        let (idx, from_tip) = if let Some(idx) = self.free.pop(&pin()) {
            log::trace!(
                "reusing heap index {} in slab for sizes of {}",
                idx,
                slab_id_to_size(self.slab_id),
            );
            (idx, false)
        } else {
            log::trace!(
                "no free heap slots in slab for sizes of {}",
                slab_id_to_size(self.slab_id),
            );
            (self.tip.fetch_add(1, Acquire), true)
        };

        log::trace!(
            "heap reservation for slot {} in the slab for sizes of {}",
            idx,
            slab_id_to_size(self.slab_id),
        );

        let heap_id = HeapId::compose(self.slab_id, idx, original_lsn);

        Reservation {
            slab_free: self.free.clone(),
            completed: false,
            file: self.file.try_clone().unwrap(),
            from_tip,
            heap_id,
        }
    }

    fn free(&self, idx: u32) {
        self.punch_hole(idx);
        self.free.push(idx, &pin());
    }

    fn punch_hole(&self, #[allow(unused)] idx: u32) {
        #[cfg(all(target_os = "linux", not(miri)))]
        {
            use std::{
                os::unix::io::AsRawFd,
                sync::atomic::{AtomicBool, Ordering::Relaxed},
            };

            use libc::{fallocate, FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE};

            static HOLE_PUNCHING_ENABLED: AtomicBool = AtomicBool::new(true);
            const MODE: i32 = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE;

            if HOLE_PUNCHING_ENABLED.load(Relaxed) {
                let bs = i64::try_from(slab_id_to_size(self.slab_id)).unwrap();
                let offset = i64::from(idx) * bs;

                let fd = self.file.as_raw_fd();

                let ret = unsafe {
                    fallocate(
                        fd,
                        MODE,
                        #[allow(clippy::useless_conversion)]
                        offset.try_into().unwrap(),
                        #[allow(clippy::useless_conversion)]
                        bs.try_into().unwrap(),
                    )
                };

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

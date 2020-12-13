// TODO rm allow(unused)
#![allow(unused)]
#![allow(unsafe_code)]

use std::{
    convert::TryFrom,
    fs::File,
    mem::{transmute, MaybeUninit},
    path::Path,
    sync::{
        atomic::{AtomicU32, Ordering::Acquire},
        Arc,
    },
};

use crossbeam_epoch::pin;

use crate::{pagecache::MessageKind, stack::Stack, Result};

pub type SlabId = u8;
pub type SlabIdx = u32;

#[derive(Debug, Clone, Copy, PartialOrd, Ord, Eq, PartialEq)]
pub struct HeapId(pub u64);

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
}

fn slab_id_to_size(slab_id: u8) -> u64 {
    1 << (16 + slab_id as u64)
}

fn size_to_slab_id(size: u64) -> SlabId {
    // find the power of 2 that is at least 64k
    let normalized_size = std::cmp::max(64 * 1024, size.next_power_of_two());

    // drop the lowest 16 bits
    let rebased_size = normalized_size >> 16;

    u8::try_from(rebased_size.trailing_zeros()).unwrap()
}

pub struct Reservation {
    slab_free: Arc<Stack<u32>>,
    completed: bool,
    file: File,
    idx: u32,
    offset: u64,
    size: u64,
    // a callback that is executed
    // when the reservation is filled
    // and stabilized
    stability_cb: Option<Box<dyn FnOnce(SlabId)>>,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.completed {
            self.slab_free.push(self.idx, &pin());
        }
    }
}

impl Reservation {
    pub fn heap_id(&self) -> HeapId {
        let slab_id = size_to_slab_id(self.size);

        HeapId::compose(slab_id, self.idx)
    }

    pub fn complete(mut self, data: &[u8]) -> Result<HeapId> {
        assert_eq!(data.len() as u64, self.size);

        use std::os::unix::fs::FileExt;
        self.file.write_at(data, self.offset)?;
        self.file.sync_all()?;

        // if this is not reached due to an IO error,
        // the offset will be returned to the Slab in Drop
        self.completed = true;

        let slab_id = size_to_slab_id(self.size);

        if let Some(stability_cb) = self.stability_cb.take() {
            (stability_cb)(slab_id);
        } else {
            unreachable!();
        }

        Ok(HeapId::compose(slab_id, self.idx))
    }

    pub fn abort(self) {
        // actual logic in Drop
    }
}

#[derive(Debug)]
pub struct Heap {
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
        todo!()
    }

    pub fn read(&self, heap_id: HeapId) -> Result<(MessageKind, Vec<u8>)> {
        let (slab_id, slab_idx) = heap_id.decompose();
        self.slabs[slab_id as usize].read(slab_idx)
    }

    pub fn free(&self, heap_id: HeapId) -> Result<()> {
        let (slab_id, slab_idx) = heap_id.decompose();
        self.slabs[slab_id as usize].free(slab_idx)
    }

    pub fn reserve(
        &self,
        size: u64,
        stability_cb: Box<dyn FnOnce(SlabId)>,
    ) -> Reservation {
        assert!(size < 1 << 48);
        let slab_id = size_to_slab_id(size);
        self.slabs[slab_id as usize].reserve(size, stability_cb)
    }
}

#[derive(Debug)]
struct Slab {
    file: File,
    bs: u64,
    tip: AtomicU32,
    free: Arc<Stack<u32>>,
}

impl Slab {
    pub fn start<P: AsRef<Path>>(directory: P, slab_id: u8) -> Result<Slab> {
        let bs = slab_id_to_size(slab_id);
        let free = Arc::new(Stack::default());
        let file = File::open(directory.as_ref().join(format!("{}", slab_id)))?;
        let tip =
            AtomicU32::new(u32::try_from(file.metadata()?.len() / bs).unwrap());

        Ok(Slab { file, bs, tip, free })
    }

    fn read(&self, slab_idx: SlabIdx) -> Result<(MessageKind, Vec<u8>)> {
        let mut ret = vec![0; usize::try_from(self.bs).unwrap()];

        let offset = slab_idx as u64 * self.bs;

        use std::os::unix::fs::FileExt;
        self.file.read_exact_at(&mut ret, offset)?;

        //Ok(ret)
        todo!()
    }

    fn reserve(
        &self,
        size: u64,
        stability_cb: Box<dyn FnOnce(SlabId)>,
    ) -> Reservation {
        let idx = if let Some(idx) = self.free.pop(&pin()) {
            idx
        } else {
            self.tip.fetch_add(1, Acquire)
        };

        let offset = idx as u64 * self.bs;

        Reservation {
            slab_free: self.free.clone(),
            completed: false,
            file: self.file.try_clone().unwrap(),
            idx,
            offset,
            size,
            stability_cb: Some(stability_cb),
        }
    }

    fn free(&self, idx: u32) -> Result<()> {
        self.punch_hole(idx)?;
        self.free.push(idx, &pin());
        Ok(())
    }

    fn punch_hole(&self, idx: u32) -> Result<()> {
        let offset = idx as u64 * self.bs;

        #[cfg(target_os = "linux")]
        {
            use libc::{fallocate, FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE};
            use std::os::unix::io::AsRawFd;

            let mode = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE;

            let fd = self.file.as_raw_fd();

            let ret =
                unsafe { fallocate(fd, mode, offset as i64, self.bs as i64) };

            if ret != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
        Ok(())
    }
}

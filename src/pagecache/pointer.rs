use crate::{pagecache::HeapId, Meta, Node, Shared};

/// A pointer to a page that may be in-memory or paged-out.
///
/// kinds of paged-out pages:
/// ptr -> log
/// ptr -> log -> [free]
/// ptr -> log -> heap
/// ptr -> heap [in snapshot]
///
/// kinds of paged-in pages:
/// 0 -> (meta, base)
/// 1 -> (counter, base)
/// 2.. ptr -> memory -> (base, frags)
///
/// The first byte is the determinant for what
/// kind of item this is. The second byte is
/// a size class, which is the next power of 2.

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct DiskPointer(pub [u8; 8]);

impl std::fmt::Display for DiskPointer {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        write!(f, "DiskPointer({:?})", Pointer(self.0).read())
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct LogOffset([u8; 6]);

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct SizeClass(u8);

#[repr(u8)]
pub(crate) enum PointerKind {
    Log = 0,
    Free = 1,
    LogAndHeap = 2,
    Heap = 3,
    InMemory = 4,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct Pointer(pub [u8; 8]);

impl std::fmt::Display for Pointer {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        write!(f, "Pointer({:?})", self.read())
    }
}

impl Pointer {
    pub fn read<'a>(&'a self) -> PointerRead<'a> {
        let size_po2 = SizeClass(self.0[1]);
        let base = LogOffset([
            self.0[2], self.0[3], self.0[4], self.0[5], self.0[6], self.0[7],
        ]);
        let pointer_kind: PointerKind =
            unsafe { std::mem::transmute(self.0[0]) };
        match pointer_kind {
            PointerKind::InMemory => {
                // don't even worry about low bits. machines today can't
                // even address over 2^48, so using the top 2 bytes for
                // our metadata is fine.
                let ptr_usize = usize::from_le_bytes([
                    0, 0, self.0[7], self.0[6], self.0[5], self.0[4],
                    self.0[3], self.0[2],
                ]);
                let cast: *const PersistedNode = ptr_usize as _;
                let ptr = cast.into();
                PointerRead::InMemory { size_po2, ptr }
            }
            PointerKind::Heap => {
                let slab = self.0[2];
                let index = u32::from_le_bytes([
                    self.0[6], self.0[5], self.0[4], self.0[3],
                ]);
                PointerRead::Heap { size_po2, heap_id: HeapId { slab, index } }
            }
            PointerKind::Log => PointerRead::Log { size_po2, base },
            PointerKind::Free => PointerRead::Free { base },
            PointerKind::LogAndHeap => {
                PointerRead::LogAndHeap { size_po2, base }
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PointerRead<'a> {
    Log { size_po2: SizeClass, base: LogOffset },
    Free { base: LogOffset },
    LogAndHeap { size_po2: SizeClass, base: LogOffset },
    Heap { size_po2: SizeClass, heap_id: HeapId },
    InMemory { size_po2: SizeClass, ptr: Shared<'a, PersistedNode> },
}

impl<'a> PointerRead<'a> {
    pub fn log_size(&self) -> usize {
        use PointerRead::*;
        match self {
            Log { size_po2, .. }
            | LogAndHeap { size_po2, .. }
            | Heap { size_po2, .. }
            | InMemory { size_po2, .. } => 1_usize << size_po2.0,
            _ => 0,
        }
    }

    pub fn as_node(&self) -> &Node {
        if let PointerRead::InMemory { ptr, .. } = self {
            &ptr.deref().node
        } else {
            panic!("called as_node on {:?}", self);
        }
    }

    pub fn as_meta(&self) -> &Meta {
        if let PointerRead::InMemory { ptr, .. } = self {
            &(*(ptr.as_raw() as *const PersistedMeta)).meta
        } else {
            panic!("called as_node on {:?}", self);
        }
    }
}

pub(crate) struct PersistedCounter {
    counter: u64,
    base: DiskPointer,
}

pub(crate) struct PersistedMeta {
    meta: Meta,
    base: DiskPointer,
}

pub(crate) struct PersistedNode {
    node: Node,
    base: DiskPointer,
    frags: Vec<DiskPointer>,
}

pub(crate) struct PersistedFree {
    base: DiskPointer,
}

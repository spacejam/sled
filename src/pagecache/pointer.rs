use std::{convert::TryFrom, fmt, ops::Deref};

use crate::{
    pagecache::heap::{HeapId, MIN_TRAILING_ZEROS},
    LogOffset, Lsn, Meta, Node, Shared,
};

/// A pointer to a page that may be in-memory or paged-out.
///
/// kinds of paged-out pages:
/// ptr -> log
/// ptr -> heap [in log]
/// log -> free [in log]
/// ptr -> heap [in snapshot]
/// ptr -> free [in snapshot]
///
/// kinds of paged-in pages:
/// 0 -> (meta, base)
/// 1 -> (counter, base)
/// 2.. ptr -> memory -> (base, frags, node)
///
/// TODO: separate nodes into dirty and clean,
/// and directly refer to clean nodes to avoid
/// pointer chasing.
///
/// The last byte is the discriminant for what
/// kind of item this is. The second to last byte
/// is a size class, which is the next power of 2.

impl std::fmt::Display for PagePointer {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        write!(f, "PagePointer({:?})", self.read())
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq)]
pub(crate) struct TruncatedLogOffset([u8; 6]);

impl TruncatedLogOffset {
    pub fn to_u64(&self) -> LogOffset {
        u64::from_le_bytes([
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5],
            0, 0,
        ])
    }

    pub fn from_u64(from: u64) -> TruncatedLogOffset {
        let arr = from.to_le_bytes();
        assert_eq!(arr[6..7], [0, 0]);
        TruncatedLogOffset([arr[0], arr[1], arr[2], arr[3], arr[4], arr[5]])
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct SizeClass(u8);

impl SizeClass {
    pub const fn size(&self) -> u64 {
        1 << self.0
    }
}

#[repr(u8)]
#[derive(PartialEq)]
pub(crate) enum PointerKind {
    InMemory = 0,
    Heap = 1,
    Log = 2,
    LogAndHeap = 3,
    Free = 4,
    Unassigned = 5,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) struct PagePointer(pub [u8; 8]);

impl Default for PagePointer {
    fn default() -> PagePointer {
        PagePointer([0, 0, 0, 0, 0, 0, 0, PointerKind::Unassigned as u8])
    }
}

impl fmt::Debug for PagePointer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("PagePointer").field("inner", &self.read()).finish()
    }
}

impl PagePointer {
    pub fn read<'a>(&'a self) -> PointerRead<'a> {
        let size_po2 = SizeClass(self.0[6]);
        let base = TruncatedLogOffset([
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5],
        ]);
        match self.kind() {
            PointerKind::InMemory => {
                // don't even worry about low bits. machines today can't
                // even address over 2^48, so using the top 2 bytes for
                // our metadata is fine.
                let ptr: *const PersistedNode = usize::from_le_bytes([
                    self.0[0], self.0[1], self.0[2], self.0[3], self.0[4],
                    self.0[5], 0, 0,
                ])
                    as *const PersistedNode;
                PointerRead::InMemory { size_po2, ptr: ptr.into() }
            }
            PointerKind::Heap => {
                let heap_index = u32::from_le_bytes([
                    self.0[0], self.0[1], self.0[2], self.0[3],
                ]);
                PointerRead::Heap { size_po2, heap_index }
            }
            PointerKind::Log => PointerRead::Log { size_po2, base },
            PointerKind::Free => PointerRead::Free { base },
            PointerKind::LogAndHeap => {
                let ptr: *const LogAndHeap = usize::from_le_bytes([
                    self.0[0], self.0[1], self.0[2], self.0[3], self.0[4],
                    self.0[5], 0, 0,
                ])
                    as *const LogAndHeap;
                PointerRead::LogAndHeap { size_po2, ptr: ptr.into() }
            }
        }
    }

    pub fn lid(&self) -> Option<LogOffset> {
        match self.read() {
            PointerRead::Log { base, .. } | PointerRead::Free { base } => {
                Some(base.to_u64())
            }
            _ => None,
        }
    }

    const fn kind(&self) -> PointerKind {
        unsafe { std::mem::transmute(self.0[7]) }
    }

    pub fn heap_id(&self) -> HeapId {
        let read = self.read();
        if let PointerRead::Heap { size_po2, heap_index } = read {
            HeapId {
                slab: size_po2.0 - u8::try_from(MIN_TRAILING_ZEROS).unwrap(),
                index: heap_index,
            }
        } else {
            panic!("called heap_id on {:?}", read);
        }
    }

    pub fn new_in_memory(
        size_po2: u8,
        node: Shared<'_, PersistedNode>,
    ) -> PagePointer {
        let kind = PointerKind::InMemory as u8;
        let ptr_arr = (node.as_raw() as usize).to_le_bytes();
        assert_eq!(ptr_arr[6..7], [0, 0]);
        PagePointer([
            ptr_arr[0], ptr_arr[1], ptr_arr[2], ptr_arr[3], ptr_arr[4],
            ptr_arr[5], size_po2, kind,
        ])
    }

    pub fn new_heap(heap_id: HeapId) -> PagePointer {
        let kind = PointerKind::Heap as u8;
        let size_po2 = heap_id.slab + u8::try_from(MIN_TRAILING_ZEROS).unwrap();
        let index_arr = heap_id.index.to_le_bytes();
        PagePointer([
            index_arr[0],
            index_arr[1],
            index_arr[2],
            index_arr[3],
            0,
            0,
            size_po2,
            kind,
        ])
    }

    pub fn new_log(size_po2: u8, at: LogOffset) -> PagePointer {
        let at = TruncatedLogOffset::from_u64(at);
        let kind = PointerKind::LogAndHeap as u8;
        PagePointer([
            at.0[0], at.0[1], at.0[2], at.0[3], at.0[4], at.0[5], size_po2,
            kind,
        ])
    }

    pub fn new_free(at: LogOffset) -> PagePointer {
        let at = TruncatedLogOffset::from_u64(at);
        let kind = PointerKind::Free as u8;
        let size_po2 = 0;
        PagePointer([
            at.0[0], at.0[1], at.0[2], at.0[3], at.0[4], at.0[5], size_po2,
            kind,
        ])
    }

    pub fn new_log_and_heap(size_po2: u8, at: LogOffset) -> PagePointer {
        let kind = PointerKind::LogAndHeap as u8;
        let at = TruncatedLogOffset::from_u64(at);
        PagePointer([
            at.0[0], at.0[1], at.0[2], at.0[3], at.0[4], at.0[5], size_po2,
            kind,
        ])
    }

    pub fn is_lone_log_and_heap(&self) -> bool {
        self.kind() == PointerKind::LogAndHeap
    }

    pub fn is_merged_into_snapshot(&self) -> bool {
        self.kind() != PointerKind::LogAndHeap
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PointerRead<'a> {
    Log { size_po2: SizeClass, base: TruncatedLogOffset },
    Free { base: TruncatedLogOffset },
    LogAndHeap { size_po2: SizeClass, ptr: Shared<'a, LogAndHeap> },
    Heap { size_po2: SizeClass, heap_index: u32 },
    InMemory { size_po2: SizeClass, ptr: Shared<'a, PersistedNode> },
}

impl<'a> PointerRead<'a> {
    pub fn is_free(&self) -> bool {
        if let PointerRead::Free { .. } = self {
            true
        } else {
            false
        }
    }

    pub fn log_size(&self) -> u64 {
        use PointerRead::*;
        match self {
            Heap { size_po2, .. }
            | Log { size_po2, .. }
            | LogAndHeap { size_po2, .. }
            | InMemory { size_po2, .. } => size_po2.size(),
            _ => 0,
        }
    }

    pub fn as_node(&self) -> &PersistedNode {
        if let PointerRead::InMemory { ptr, .. } = self {
            &ptr.deref()
        } else {
            panic!("called as_node on {:?}", self);
        }
    }

    pub fn as_meta(&self) -> &PersistedMeta {
        if let PointerRead::InMemory { ptr, .. } = self {
            &(*(ptr.as_raw() as *const PersistedMeta))
        } else {
            panic!("called as_meta on {:?}", self);
        }
    }

    pub fn as_counter(&self) -> &PersistedCounter {
        if let PointerRead::InMemory { ptr, .. } = self {
            &(*(ptr.as_raw() as *const PersistedCounter))
        } else {
            panic!("called as_counter on {:?}", self);
        }
    }
}

#[derive(Debug)]
pub(crate) struct PersistedCounter {
    pub counter: u64,
    pub page_pointer: PagePointer,
}

impl Deref for PersistedCounter {
    type Target = u64;

    fn deref(&self) -> &u64 {
        &self.counter
    }
}

#[derive(Debug)]
pub(crate) struct LogAndHeap {
    pub log_offset: TruncatedLogOffset,
    pub HeapId: HeapId,
    pub log_lsn: Lsn,
}

#[derive(Debug)]
pub(crate) struct PersistedMeta {
    pub meta: Meta,
    pub page_pointer: PagePointer,
}

impl Deref for PersistedMeta {
    type Target = Meta;

    fn deref(&self) -> &Meta {
        &self.meta
    }
}

#[derive(Debug)]
pub(crate) struct PersistedNode {
    pub node: Node,
    pub base: PagePointer,
    pub frags: Vec<PagePointer>,
    pub ts: u64,
}

impl Deref for PersistedNode {
    type Target = Node;

    fn deref(&self) -> &Node {
        &self.node
    }
}

#[derive(Debug)]
pub(crate) struct PersistedFree {
    pub page_pointer: PagePointer,
}

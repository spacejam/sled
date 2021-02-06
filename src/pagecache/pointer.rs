use std::{convert::TryFrom, fmt, ops::Deref};

use crate::{
    pagecache::heap::{HeapId, MIN_TRAILING_ZEROS},
    Meta, Node, Shared,
};

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

impl std::fmt::Display for PagePointer {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        write!(f, "PagePointer({:?})", self.read())
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct LogOffset([u8; 6]);

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct SizeClass(u8);

#[repr(u8)]
#[derive(PartialEq)]
pub(crate) enum PointerKind {
    InMemory = 0,
    Heap = 1,
    Log = 2,
    LogAndHeap = 3,
    Free = 4,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) struct PagePointer(pub [u8; 8]);

impl fmt::Debug for PagePointer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("PagePointer").field("inner", &self.read()).finish()
    }
}

impl PagePointer {
    pub fn read<'a>(&'a self) -> PointerRead<'a> {
        let size_po2 = SizeClass(self.0[6]);
        let base = LogOffset([
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5],
        ]);
        match self.kind() {
            PointerKind::InMemory => {
                // don't even worry about low bits. machines today can't
                // even address over 2^48, so using the top 2 bytes for
                // our metadata is fine.
                let ptr: *const PersistedNode = unsafe {
                    std::ptr::read_unaligned(
                        &self.0 as *const u8 as *const *const PersistedNode,
                    )
                };
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
                PointerRead::LogAndHeap { size_po2, base }
            }
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
        let kind = PointerKind::LogAndHeap as u8;
        PagePointer([
            at.0[0], at.0[1], at.0[2], at.0[3], at.0[4], at.0[5], size_po2,
            kind,
        ])
    }

    pub fn new_free(at: LogOffset) -> PagePointer {
        let kind = PointerKind::Free as u8;
        let size_po2 = 0;
        PagePointer([
            at.0[0], at.0[1], at.0[2], at.0[3], at.0[4], at.0[5], size_po2,
            kind,
        ])
    }

    pub fn new_log_and_heap(size_po2: u8, at: LogOffset) -> PagePointer {
        let kind = PointerKind::LogAndHeap as u8;
        PagePointer([
            at.0[0], at.0[1], at.0[2], at.0[3], at.0[4], at.0[5], size_po2,
            kind,
        ])
    }

    pub fn is_lone_log_and_heap(&self) -> bool {
        self.kind() == PointerKind::LogAndHeap
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PointerRead<'a> {
    Log { size_po2: SizeClass, base: LogOffset },
    Free { base: LogOffset },
    LogAndHeap { size_po2: SizeClass, base: LogOffset },
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
            | InMemory { size_po2, .. } => 1_u64 << size_po2.0,
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
    pub base_page_pointer: PagePointer,
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

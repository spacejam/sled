use std::num::NonZeroU64;

use super::{HeapId, LogOffset};
use crate::*;

/// A pointer to a location on disk or an off-log heap item.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DiskPtr(DiskPtrInner);

#[derive(Debug, Clone, Copy, PartialEq)]
enum DiskPtrInner {
    /// Points to a value stored in the single-file log.
    Inline(LogOffset),
    /// Points to a value stored off-log in the heap.
    Heap(Option<NonZeroU64>, HeapId),
}

impl DiskPtr {
    pub(crate) const fn new_inline(l: LogOffset) -> Self {
        DiskPtr(DiskPtrInner::Inline(l))
    }

    pub(crate) fn merged_heap_item(heap_id: HeapId) -> Self {
        DiskPtr(DiskPtrInner::Heap(None, heap_id))
    }

    pub(crate) fn new_heap_item(lid: LogOffset, heap_id: HeapId) -> Self {
        DiskPtr(DiskPtrInner::Heap(
            Some(NonZeroU64::new(lid).unwrap()),
            heap_id,
        ))
    }

    pub(crate) const fn is_inline(&self) -> bool {
        matches!(self, DiskPtr(DiskPtrInner::Inline(_)))
    }

    pub(crate) const fn is_heap_item(&self) -> bool {
        matches!(self, DiskPtr(DiskPtrInner::Heap(_, _)))
    }

    pub(crate) const fn heap_id(&self) -> Option<HeapId> {
        if let DiskPtr(DiskPtrInner::Heap(_, heap_id)) = self {
            Some(*heap_id)
        } else {
            None
        }
    }

    #[doc(hidden)]
    pub const fn lid(&self) -> Option<LogOffset> {
        match self.0 {
            DiskPtrInner::Inline(lid) => Some(lid),
            DiskPtrInner::Heap(Some(lid), _) => Some(lid.get()),
            DiskPtrInner::Heap(None, _) => None,
        }
    }

    pub(crate) fn forget_heap_log_coordinates(&mut self) {
        match self.0 {
            DiskPtrInner::Inline(_) => {}
            DiskPtrInner::Heap(ref mut opt, _) => *opt = None,
        }
    }

    pub(crate) fn original_lsn(&self) -> Lsn {
        match self.0 {
            DiskPtrInner::Heap(_, heap_id) => heap_id.original_lsn,
            DiskPtrInner::Inline(_) => {
                panic!("called original_lsn on non-Heap")
            }
        }
    }

    pub(crate) const fn heap_pointer_merged_into_snapshot(&self) -> bool {
        matches!(self, DiskPtr(DiskPtrInner::Heap(None, _)))
    }
}

impl fmt::Display for DiskPtr {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        write!(f, "{:?}", self)
    }
}

impl Serialize for DiskPtr {
    fn serialized_size(&self) -> u64 {
        match self.0 {
            DiskPtrInner::Inline(a) => 1 + a.serialized_size(),
            DiskPtrInner::Heap(a, b) => {
                1 + a.serialized_size() + b.serialized_size()
            }
        }
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        match self.0 {
            DiskPtrInner::Inline(log_offset) => {
                0_u8.serialize_into(buf);
                log_offset.serialize_into(buf);
            }
            DiskPtrInner::Heap(log_offset, heap_id) => {
                1_u8.serialize_into(buf);
                log_offset.serialize_into(buf);
                heap_id.serialize_into(buf);
            }
        }
    }

    fn deserialize(buf: &mut &[u8]) -> Result<DiskPtr> {
        if buf.len() < 2 {
            return Err(Error::corruption(None));
        }
        let discriminant = buf[0];
        *buf = &buf[1..];
        Ok(DiskPtr(match discriminant {
            0 => DiskPtrInner::Inline(u64::deserialize(buf)?),
            1 => DiskPtrInner::Heap(
                Serialize::deserialize(buf)?,
                HeapId::deserialize(buf)?,
            ),
            _ => return Err(Error::corruption(None)),
        }))
    }
}

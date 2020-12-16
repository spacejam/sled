use std::num::NonZeroU64;

use super::{HeapId, LogOffset};
use crate::*;

/// A pointer to a location on disk or an off-log blob.
#[derive(Debug, Clone, PartialOrd, Ord, Copy, Eq, PartialEq)]
pub enum DiskPtr {
    /// Points to a value stored in the single-file log.
    Inline(LogOffset),
    /// Points to a value stored off-log in the blob directory.
    Blob(Option<NonZeroU64>, HeapId),
}

impl DiskPtr {
    pub(crate) fn new_inline(l: LogOffset) -> Self {
        DiskPtr::Inline(l)
    }

    pub(crate) fn new_blob(lid: LogOffset, heap_id: HeapId) -> Self {
        DiskPtr::Blob(Some(NonZeroU64::new(lid).unwrap()), heap_id)
    }

    pub(crate) fn is_inline(&self) -> bool {
        match self {
            DiskPtr::Inline(_) => true,
            DiskPtr::Blob(_, _) => false,
        }
    }

    pub(crate) fn is_blob(&self) -> bool {
        match self {
            DiskPtr::Inline(_) => false,
            DiskPtr::Blob(_, _) => true,
        }
    }

    pub(crate) fn heap_id(&self) -> Option<HeapId> {
        if let DiskPtr::Blob(_, heap_id) = self { Some(*heap_id) } else { None }
    }

    #[doc(hidden)]
    pub fn lid(&self) -> Option<LogOffset> {
        match self {
            DiskPtr::Inline(lid) => Some(*lid),
            DiskPtr::Blob(lid, _) => lid.map(NonZeroU64::get),
        }
    }

    pub(crate) fn forget_blob_log_coordinates(&mut self) {
        match self {
            DiskPtr::Inline(_) => {}
            DiskPtr::Blob(ref mut opt, _) => *opt = None,
        }
    }

    pub(crate) fn original_lsn(&self) -> Lsn {
        match self {
            DiskPtr::Blob(_, heap_id) => heap_id.original_lsn,
            DiskPtr::Inline(_) => panic!("called original_lsn on non-Blob"),
        }
    }

    pub(crate) fn blob_pointer_merged_into_snapshot(&self) -> bool {
        if let DiskPtr::Blob(None, _) = self { true } else { false }
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

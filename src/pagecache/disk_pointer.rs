use std::num::NonZeroU64;

use super::{BlobPointer, LogOffset};
use crate::*;

/// A pointer to a location on disk or an off-log blob.
#[derive(Debug, Clone, PartialOrd, Ord, Copy, Eq, PartialEq)]
pub enum DiskPtr {
    /// Points to a value stored in the single-file log.
    Inline(LogOffset),
    /// Points to a value stored off-log in the blob directory.
    Blob(Option<NonZeroU64>, BlobPointer),
}

impl DiskPtr {
    pub(crate) fn new_inline(l: LogOffset) -> Self {
        DiskPtr::Inline(l)
    }

    pub(crate) fn new_blob(lid: LogOffset, ptr: BlobPointer) -> Self {
        DiskPtr::Blob(Some(NonZeroU64::new(lid).unwrap()), ptr)
    }

    pub(crate) fn is_inline(&self) -> bool {
        match self {
            DiskPtr::Inline(_) => true,
            DiskPtr::Blob(_, _) => false,
        }
    }

    pub(crate) fn is_blob(&self) -> bool {
        match self {
            DiskPtr::Blob(_, _) => true,
            DiskPtr::Inline(_) => false,
        }
    }

    pub(crate) fn blob(&self) -> (Option<LogOffset>, BlobPointer) {
        match self {
            DiskPtr::Blob(lid, ptr) => (lid.map(|l| l.get()), *ptr),
            DiskPtr::Inline(_) => {
                panic!("blob called on Internal disk pointer")
            }
        }
    }

    pub(crate) fn original_lsn(&self) -> Lsn {
        if let DiskPtr::Blob(_, ptr) = self {
            *ptr
        } else {
            panic!("called original_lsn on non-Blob");
        }
    }

    #[doc(hidden)]
    pub fn lid(&self) -> Option<LogOffset> {
        match self {
            DiskPtr::Blob(lid, _) => lid.map(|l| l.get()),
            DiskPtr::Inline(lid) => Some(*lid),
        }
    }

    pub(crate) fn forget_blob_log_coordinates(&mut self) {
        match self {
            DiskPtr::Blob(ref mut opt, _blob_pointer) => *opt = None,
            DiskPtr::Inline(_) => {}
        }
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

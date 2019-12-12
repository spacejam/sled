use super::{BlobPointer, LogOffset};
use crate::*;

/// A pointer to a location on disk or an off-log blob.
#[derive(Debug, Clone, PartialOrd, Ord, Copy, Eq, PartialEq)]
pub enum DiskPtr {
    /// Points to a value stored in the single-file log.
    Inline(LogOffset),
    /// Points to a value stored off-log in the blob directory.
    Blob(LogOffset, BlobPointer),
}

impl DiskPtr {
    pub(crate) fn new_inline(l: LogOffset) -> Self {
        DiskPtr::Inline(l)
    }

    pub(crate) fn new_blob(lid: LogOffset, ptr: BlobPointer) -> Self {
        DiskPtr::Blob(lid, ptr)
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

    pub(crate) fn blob(&self) -> (LogOffset, BlobPointer) {
        match self {
            DiskPtr::Blob(lid, ptr) => (*lid, *ptr),
            DiskPtr::Inline(_) => {
                panic!("blob called on Internal disk pointer")
            }
        }
    }

    #[doc(hidden)]
    pub fn lid(&self) -> LogOffset {
        match self {
            DiskPtr::Blob(lid, _) | DiskPtr::Inline(lid) => *lid,
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

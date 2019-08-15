use super::*;

/// A pointer to a location on disk or an off-log blob.
#[derive(
    Debug, Clone, PartialOrd, Ord, Copy, Eq, PartialEq, Serialize, Deserialize,
)]
pub enum DiskPtr {
    /// Points to a value stored in the single-file log.
    Inline(LogId),
    /// Points to a value stored off-log in the blob directory.
    Blob(LogId, BlobPointer),
}

impl DiskPtr {
    pub(crate) fn new_inline(l: LogId) -> Self {
        Self::Inline(l)
    }

    pub(crate) fn new_blob(lid: LogId, ptr: BlobPointer) -> Self {
        Self::Blob(lid, ptr)
    }

    pub(crate) fn is_inline(&self) -> bool {
        match self {
            Self::Inline(_) => true,
            Self::Blob(_, _) => false,
        }
    }

    pub(crate) fn is_blob(&self) -> bool {
        match self {
            Self::Blob(_, _) => true,
            Self::Inline(_) => false,
        }
    }

    pub(crate) fn blob(&self) -> (LogId, BlobPointer) {
        match self {
            Self::Blob(lid, ptr) => (*lid, *ptr),
            Self::Inline(_) => {
                panic!("blob called on Internal disk pointer")
            }
        }
    }

    #[doc(hidden)]
    pub fn lid(&self) -> LogId {
        match self {
            Self::Blob(lid, _) | Self::Inline(lid) => *lid,
        }
    }
}

impl std::fmt::Display for DiskPtr {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{:?}", self)
    }
}

use super::*;

// Explicitly use this so its use is tool-friendly.
use super::LogReader;

/// A pointer to a location on disk or an off-log blob.
#[derive(
    Debug,
    Clone,
    PartialOrd,
    Ord,
    Copy,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
)]
pub enum DiskPtr {
    /// Points to a value stored in the single-file log.
    Inline(LogId),
    /// Points to a value stored off-log in the blob directory.
    Blob(LogId, BlobPointer),
}

impl DiskPtr {
    pub(crate) fn new_inline(l: LogId) -> DiskPtr {
        DiskPtr::Inline(l)
    }

    pub(crate) fn new_blob(lid: LogId, ptr: BlobPointer) -> DiskPtr {
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

    pub(crate) fn inline(&self) -> LogId {
        match self {
            DiskPtr::Inline(l) => *l,
            DiskPtr::Blob(_, _) => {
                panic!("inline called on Blob disk pointer")
            }
        }
    }

    pub(crate) fn blob(&self) -> (LogId, BlobPointer) {
        match self {
            DiskPtr::Blob(lid, ptr) => (*lid, *ptr),
            DiskPtr::Inline(_) => {
                panic!("blob called on Internal disk pointer")
            }
        }
    }

    #[doc(hidden)]
    pub fn lid(&self) -> LogId {
        match self {
            DiskPtr::Blob(lid, _) | DiskPtr::Inline(lid) => *lid,
        }
    }

    pub(crate) fn read(
        &self,
        config: &Config,
    ) -> Result<Vec<u8>, ()> {
        match self {
            DiskPtr::Blob(_lid, ptr) => read_blob(*ptr, &config),
            DiskPtr::Inline(lid) => {
                let f = config.file()?;

                f.read_message(*lid, &config).map(|log_read| {
                    log_read
                        .inline()
                        .expect(
                            "call to DiskPtr::read with \
                             an Inline pointer should result \
                             in a valid Inline read. It's \
                             possible the DiskPtr outlived \
                             an outer guard.",
                        )
                        .1
                })
            }
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

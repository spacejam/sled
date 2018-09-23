use super::*;

use io::LogReader;

/// A pointer to a location on disk or an off-log blob.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum DiskPtr {
    /// Points to a value stored in the single-file log.
    Inline(LogID),
    /// Points to a value stored off-log in the blob directory.
    External(LogID, ExternalPointer),
}

impl DiskPtr {
    pub(crate) fn new_inline(l: LogID) -> DiskPtr {
        DiskPtr::Inline(l)
    }

    pub(crate) fn new_external(
        lid: LogID,
        ptr: ExternalPointer,
    ) -> DiskPtr {
        DiskPtr::External(lid, ptr)
    }

    pub(crate) fn is_inline(&self) -> bool {
        match self {
            DiskPtr::Inline(_) => true,
            DiskPtr::External(_, _) => false,
        }
    }

    pub(crate) fn is_external(&self) -> bool {
        match self {
            DiskPtr::External(_, _) => true,
            DiskPtr::Inline(_) => false,
        }
    }

    pub(crate) fn inline(&self) -> LogID {
        match self {
            DiskPtr::Inline(l) => *l,
            DiskPtr::External(_, _) => {
                panic!("inline called on External disk pointer")
            }
        }
    }

    pub(crate) fn external(&self) -> (LogID, ExternalPointer) {
        match self {
            DiskPtr::External(lid, ptr) => (*lid, *ptr),
            DiskPtr::Inline(_) => {
                panic!("external called on Internal disk pointer")
            }
        }
    }

    #[doc(hidden)]
    pub fn lid(&self) -> LogID {
        match self {
            DiskPtr::External(lid, _) | DiskPtr::Inline(lid) => *lid,
        }
    }

    pub(crate) fn read(
        &self,
        config: &Config,
    ) -> CacheResult<Vec<u8>, ()> {
        match self {
            DiskPtr::External(_lid, ptr) => read_blob(*ptr, &config),
            DiskPtr::Inline(lid) => {
                let mut f = config.file()?;

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
        f: &mut std::fmt::Formatter,
    ) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self)
    }
}

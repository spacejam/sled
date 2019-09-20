use std::{
    cmp::PartialEq,
    error::Error as StdError,
    fmt::{self, Display},
    io,
};

use crate::pagecache::{DiskPtr, PagePtr};

use crate::ivec::IVec;

/// The top-level result type for dealing with
/// the `PageCache`.
pub type Result<T> = std::result::Result<T, Error>;

/// A compare and swap result.  If the CAS is successful,
/// the new `PagePtr` will be returned as `Ok`.  Otherwise,
/// the `Err` will contain a tuple of the current `PagePtr`
/// and the old value that could not be set atomically.
pub type CasResult<'a, P, R> =
    std::result::Result<PagePtr<'a, P>, Option<(PagePtr<'a, P>, R)>>;

/// An Error type encapsulating various issues that may come up
/// in both the expected and unexpected operation of a `PageCache`.
#[derive(Debug)]
pub enum Error {
    /// The underlying collection no longer exists.
    CollectionNotFound(Vec<u8>),
    /// The system has been used in an unsupported way.
    Unsupported(String),
    /// An unexpected bug has happened. Please open an issue on github!
    ReportableBug(String),
    /// A read or write error has happened when interacting with the file
    /// system.
    Io(io::Error),
    /// Corruption has been detected in the storage file.
    Corruption {
        /// The file location that corrupted data was found at.
        at: DiskPtr,
    },
    // a failpoint has been triggered for testing purposes
    #[doc(hidden)]
    #[cfg(feature = "failpoints")]
    FailPoint,
    /// Compare and swap failure.
    CompareAndSwap {
        /// Current value if present.
        cur: Option<IVec>,
    },
}

impl Clone for Error {
    fn clone(&self) -> Self {
        use self::Error::*;

        match self {
            Io(ioe) => Io(io::Error::new(ioe.kind(), format!("{:?}", ioe))),
            CollectionNotFound(name) => CollectionNotFound(name.clone()),
            Unsupported(why) => Unsupported(why.clone()),
            ReportableBug(what) => ReportableBug(what.clone()),
            Corruption { at } => Corruption { at: *at },
            #[cfg(feature = "failpoints")]
            FailPoint => FailPoint,
            CompareAndSwap { cur } => CompareAndSwap { cur: cur.clone() },
        }
    }
}

impl Eq for Error {}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        use self::Error::*;

        match *self {
            CollectionNotFound(ref l) => {
                if let CollectionNotFound(ref r) = *other {
                    l == r
                } else {
                    false
                }
            }
            Unsupported(ref l) => {
                if let Unsupported(ref r) = *other {
                    l == r
                } else {
                    false
                }
            }
            ReportableBug(ref l) => {
                if let ReportableBug(ref r) = *other {
                    l == r
                } else {
                    false
                }
            }
            #[cfg(feature = "failpoints")]
            FailPoint => {
                if let FailPoint = *other {
                    true
                } else {
                    false
                }
            }
            Corruption { at: l } => {
                if let Corruption { at: r } = *other {
                    l == r
                } else {
                    false
                }
            }
            Io(_) => false,
            CompareAndSwap { cur: ref l } => {
                if let CompareAndSwap { cur: ref r } = *other {
                    l == r
                } else {
                    false
                }
            }
        }
    }
}

impl From<io::Error> for Error {
    #[inline]
    fn from(io_error: io::Error) -> Self {
        Error::Io(io_error)
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        use self::Error::*;

        match *self {
            CollectionNotFound(_) => "Collection does not exist.",
            Unsupported(ref e) | ReportableBug(ref e) => &*e,
            #[cfg(feature = "failpoints")]
            FailPoint => "Fail point has been triggered.",
            Io(ref e) => e.description(),
            Corruption { .. } => "Read corrupted data.",
            CompareAndSwap { .. } => "Compare and swap a value.",
        }
    }
}

impl Display for Error {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        use self::Error::*;

        match *self {
            CollectionNotFound(ref name) => {
                write!(f, "Collection {:?} does not exist", name,)
            }
            Unsupported(ref e) => write!(f, "Unsupported: {}", e),
            ReportableBug(ref e) => write!(
                f,
                "Unexpected bug has happened: {}. \
                 PLEASE REPORT THIS BUG!",
                e
            ),
            #[cfg(feature = "failpoints")]
            FailPoint => write!(f, "Fail point has been triggered."),
            Io(ref e) => write!(f, "IO error: {}", e),
            Corruption { at } => {
                write!(f, "Read corrupted data at file offset {}", at)
            }
            CompareAndSwap { cur: _ } => write!(f, "Compare and swap failed."),
        }
    }
}

use std::{
    cmp::PartialEq,
    error::Error as StdError,
    fmt::{self, Display},
    io,
};

#[cfg(feature = "testing")]
use backtrace::Backtrace;

use crate::{
    pagecache::{DiskPtr, PageView},
    IVec,
};

/// The top-level result type for dealing with
/// fallible operations. The errors tend to
/// be fail-stop, and nested results are used
/// in cases where the outer fail-stop error can
/// have try `?` used on it, exposing the inner
/// operation that is expected to fail under
/// normal operation. The philosophy behind this
/// is detailed [on the sled blog](https://sled.rs/errors).
pub type Result<T> = std::result::Result<T, Error>;

/// A compare and swap result.  If the CAS is successful,
/// the new `PagePtr` will be returned as `Ok`.  Otherwise,
/// the `Err` will contain a tuple of the current `PagePtr`
/// and the old value that could not be set atomically.
pub(crate) type CasResult<'a, R> =
    std::result::Result<PageView<'a>, Option<(PageView<'a>, R)>>;

/// An Error type encapsulating various issues that may come up
/// in the operation of a `Db`.
#[derive(Debug)]
pub enum Error {
    /// The underlying collection no longer exists.
    CollectionNotFound(IVec),
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
        at: Option<DiskPtr>,
        /// A backtrace for where the corruption was encountered.
        #[cfg(feature = "testing")]
        bt: Backtrace,
        /// A backtrace for where the corruption was encountered.
        #[cfg(not(feature = "testing"))]
        bt: (),
    },
    // a failpoint has been triggered for testing purposes
    #[doc(hidden)]
    #[cfg(feature = "failpoints")]
    FailPoint,
}

impl Error {
    pub(crate) fn corruption(at: Option<DiskPtr>) -> Error {
        Error::Corruption {
            at,
            #[cfg(feature = "testing")]
            bt: Backtrace::new(),
            #[cfg(not(feature = "testing"))]
            bt: (),
        }
    }
}

impl Clone for Error {
    fn clone(&self) -> Self {
        use self::Error::*;

        match self {
            Io(ioe) => Io(io::Error::new(ioe.kind(), format!("{:?}", ioe))),
            CollectionNotFound(name) => CollectionNotFound(name.clone()),
            Unsupported(why) => Unsupported(why.clone()),
            ReportableBug(what) => ReportableBug(what.clone()),
            Corruption { at, bt } => Corruption { at: *at, bt: bt.clone() },
            #[cfg(feature = "failpoints")]
            FailPoint => FailPoint,
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
            Corruption { at: l, .. } => {
                if let Corruption { at: r, .. } = *other {
                    l == r
                } else {
                    false
                }
            }
            Io(_) => false,
        }
    }
}

impl From<io::Error> for Error {
    #[inline]
    fn from(io_error: io::Error) -> Self {
        Error::Io(io_error)
    }
}

impl From<Error> for io::Error {
    fn from(error: Error) -> io::Error {
        use self::Error::*;
        use std::io::ErrorKind;
        match error {
            Io(ioe) => ioe,
            CollectionNotFound(name) =>
                io::Error::new(
                ErrorKind::NotFound,
                format!("collection not found: {:?}", name),
            ),
            Unsupported(why) => io::Error::new(
                ErrorKind::InvalidInput,
                format!("operation not supported: {:?}", why),
            ),
            ReportableBug(what) => io::Error::new(
                ErrorKind::Other,
                format!("unexpected bug! please report this bug at <github.rs/spacejam/sled>: {:?}", what),
            ),
            Corruption { .. } => io::Error::new(
                ErrorKind::InvalidData,
                format!("corruption encountered: {:?}", error),
            ),
            #[cfg(feature = "failpoints")]
            FailPoint => io::Error::new(
                ErrorKind::Other,
                "failpoint"
            ),
        }
    }
}

impl StdError for Error {}

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
            Corruption { at, ref bt } => write!(
                f,
                "Read corrupted data at file offset {:?} backtrace {:?}",
                at, bt
            ),
        }
    }
}

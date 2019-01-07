use std::{
    cmp::PartialEq,
    error::Error as StdError,
    fmt::{self, Debug, Display},
    io,
};

use super::*;

/// The top-level result type for dealing with
/// the PageCache.
pub type Result<T, Actual> = std::result::Result<T, Error<Actual>>;

/// An Error type encapsulating various issues that may come up
/// in both the expected and unexpected operation of a PageCache.
#[derive(Debug)]
pub enum Error<Actual> {
    /// An atomic operation has failed, and the current value is provided.
    CasFailed(Actual),
    /// The underlying collection no longer exists.
    CollectionNotFound(Vec<u8>),
    /// The system has been used in an unsupported way.
    Unsupported(String),
    /// An unexpected bug has happened. Please open an issue on github!
    ReportableBug(String),
    /// A read or write error has happened when interacting with the file system.
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
}

use self::Error::*;

impl<Actual> Clone for Error<Actual> {
    fn clone(&self) -> Error<Actual> {
        match self {
            Io(ioe) => Io(std::io::Error::new(
                ioe.kind(),
                format!("{:?}", ioe),
            )),
            other => other.clone(),
        }
    }
}

impl<A> Eq for Error<A> where A: Eq {}

impl<A> PartialEq for Error<A>
where
    A: PartialEq,
{
    fn eq(&self, other: &Error<A>) -> bool {
        match *self {
            CasFailed(ref l) => {
                if let CasFailed(ref r) = *other {
                    l == r
                } else {
                    false
                }
            }
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
        }
    }
}

impl<T> From<io::Error> for Error<T> {
    #[inline]
    fn from(io_error: io::Error) -> Error<T> {
        Error::Io(io_error)
    }
}

impl<T> StdError for Error<T>
where
    T: Debug,
{
    fn description(&self) -> &str {
        match *self {
            CasFailed(_) => {
                "Compare and swap failed to successfully compare."
            }
            CollectionNotFound(_) => "Collection does not exist.",
            Unsupported(ref e) => &*e,
            ReportableBug(ref e) => &*e,
            #[cfg(feature = "failpoints")]
            FailPoint => "Fail point has been triggered.",
            Io(ref e) => e.description(),
            Corruption { .. } => "Read corrupted data.",
        }
    }
}

impl<A> Display for Error<A>
where
    A: Debug,
{
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        match *self {
            CasFailed(ref e) => write!(
                f,
                "Compare and swap failed to successfully compare \
                 with actual value {:?}",
                e
            ),
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
        }
    }
}

// TODO wrangle Into conflicts to handle these with that, if possible
impl<T> Error<T> {
    /// Turns an `Error<A>` into an `Error<B>`.
    ///
    /// # Panics
    ///
    /// Panics if the Error is of type `Error::CasFailed`
    pub fn danger_cast<Other>(self) -> Error<Other> {
        match self {
            CasFailed(_) => {
                panic!(
                    "trying to cast CasFailed(()) into a different Error type"
                )
            }
            CollectionNotFound(n) => CollectionNotFound(n),
            Unsupported(s) => Unsupported(s),
            ReportableBug(s) => ReportableBug(s),
            #[cfg(feature = "failpoints")]
            FailPoint => FailPoint,
            Io(e) => Io(e),
            Corruption {
                at,
            } => Corruption {
                at,
            },
        }
    }

    /// Turns an `Error<A>` into an `Error<B>`.
    pub fn cast<Other>(self) -> Error<Other>
    where
        T: Into<Other>,
    {
        match self {
            CasFailed(other) => CasFailed(other.into()),
            CollectionNotFound(n) => CollectionNotFound(n),
            Unsupported(s) => Unsupported(s),
            ReportableBug(s) => ReportableBug(s),
            #[cfg(feature = "failpoints")]
            FailPoint => FailPoint,
            Io(e) => Io(e),
            Corruption { at } => Corruption { at },
        }
    }
}

use std::cmp::PartialEq;
use std::io;

use super::*;

/// The top-level result type for dealing with
/// the PageCache.
pub type CacheResult<T, Actual> = Result<T, Error<Actual>>;

/// An Error type encapsulating various issues that may come up
/// in both the expected and unexpected operation of a PageCache.
#[derive(Debug)]
pub enum Error<Actual> {
    /// An atomic operation has failed, and the current value is provided
    CasFailed(Actual),
    /// The system has been used in an unsupported way.
    Unsupported(String),
    /// An unexpected bug has happened. Please open an issue on github!
    ReportableBug(String),
    /// A read or write error has happened when interacting with the file system.
    Io(io::Error),
    /// Corruption has been detected in the storage file.
    Corruption {
        /// The file location that corrupted data was found at.
        at: LogID,
    },
}
use Error::*;

impl<A> PartialEq for Error<A> {
    fn eq(&self, other: &Error<A>) -> bool {
        match *self {
            CasFailed(_) |
            Unsupported(_) |
            ReportableBug(_) |
            Corruption {
                ..
            } => self == other,
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
            Unsupported(s) => Unsupported(s),
            ReportableBug(s) => ReportableBug(s),
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
        where Other: From<T>
    {
        match self {
            CasFailed(other) => CasFailed(other.into()),
            Unsupported(s) => Unsupported(s),
            ReportableBug(s) => ReportableBug(s),
            Io(e) => Io(e),
            Corruption {
                at,
            } => Corruption {
                at,
            },
        }
    }
}

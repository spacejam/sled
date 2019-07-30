#![allow(unused)]

use std::{
    error::Error as StdError,
    fmt::{self, Display},
};

use super::*;

/// A transaction-related Result
pub type TxResult<A> = std::result::Result<A, TxError>;

/// A transaction-related error
#[derive(Debug, Clone)]
pub enum TxError {
    /// An error in the underlying IO and Caching system
    PageCache(Error),
    /// An intentionally-aborted transaction
    Abort,
    /// Another transaction interfered with this one
    Conflict,
}

impl From<Error> for TxError {
    fn from(error: Error) -> TxError {
        TxError::PageCache(error)
    }
}

impl Display for TxError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        match *self {
            TxError::PageCache(ref error) => <Error as Display>::fmt(&error, f),
            TxError::Abort => write!(f, "transaction was aborted"),
            TxError::Conflict => write!(
                f,
                "transaction encountered a \
                 conflicting concurrent transaction"
            ),
        }
    }
}

impl StdError for TxError {
    fn description(&self) -> &str {
        match *self {
            TxError::PageCache(ref error) => error.description(),
            TxError::Abort => "transaction was aborted",
            TxError::Conflict => {
                "transaction encountered a \
                 conflicting concurrent transaction"
            }
        }
    }
}

/// A handle to an ongoing pagecache transaction. Ensures
/// that any state which is removed from a shared in-memory
/// data structure is not destroyed until all possible
/// readers have concluded.
pub struct Tx<'a, P>
where
    P: Materializer,
{
    pagecache: &'a PageCache<P>,
    pub(crate) guard: Guard,
    pub(crate) ts: u64,
    pub(crate) pending: FastMap8<PageId, Update<P>>,
    cache: FastMap8<PageId, Vec<&'a P>>,
    read_set: FastMap8<PageId, u64>,
    write_set: FastSet8<PageId>,
}

impl<'a, P> Tx<'a, P>
where
    P: Materializer,
{
    /// Creates a new Tx with a given timestamp.
    pub fn new(pagecache: &'a PageCache<P>, ts: u64) -> Tx<'a, P> {
        Tx {
            pagecache,
            ts,
            guard: pin(),
            pending: Default::default(),
            cache: Default::default(),
            read_set: Default::default(),
            write_set: Default::default(),
        }
    }

    /// Atomically commit this transaction by
    /// checking all read and written pages for
    /// conflicts, and then writing changes in a
    /// way that cannot be partially recovered
    /// (will either be 100% recovered or 100%
    /// aborted in the case of a conflict or
    /// crash that happens before the entire
    /// write set can be persisted to disk).
    ///
    /// This is optimistic, which gets better
    /// performance with many threads that write
    /// to separate pages, but may abort
    /// if threads are writing to the same pages.
    pub fn commit(self) -> TxResult<()> {
        unimplemented!()
    }

    /// Create a new page, trying to reuse old freed pages if possible
    /// to maximize underlying `PageTable` pointer density. Returns
    /// the page ID and its pointer for use in future `replace`
    /// and `link` operations.
    pub fn allocate<'g>(
        &'g self,
        new: P,
    ) -> TxResult<(PageId, PagePtr<'g, P>)> {
        unimplemented!()
    }

    /// Free a particular page.
    pub fn free<'g>(
        &'g self,
        pid: PageId,
        old: PagePtr<'g, P>,
    ) -> TxResult<CasResult<'g, P, ()>> {
        unimplemented!()
    }

    /// Try to atomically add a `PageFrag` to the page.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the atomic append fails.
    pub fn link<'g>(
        &'g self,
        pid: PageId,
        old: PagePtr<'g, P>,
        new: P,
    ) -> TxResult<CasResult<'g, P, P>> {
        unimplemented!()
    }

    /// Replace an existing page with a different set of `PageFrag`s.
    /// Returns `Ok(new_key)` if the operation was successful. Returns
    /// `Err(None)` if the page no longer exists. Returns `Err(Some(actual_key))`
    /// if the atomic swap fails.
    pub fn replace<'g>(
        &'g self,
        pid: PageId,
        old: PagePtr<'g, P>,
        new: P,
    ) -> TxResult<CasResult<'g, P, P>> {
        unimplemented!()
    }

    /// Try to retrieve a page by its logical ID.
    pub fn get<'g>(
        &'g self,
        pid: PageId,
    ) -> TxResult<(PagePtr<'g, P>, Vec<&'g P>)> {
        unimplemented!()
    }

    /// Flushes the underlying EBR guard
    pub fn flush(&self) {
        self.guard.flush()
    }
}

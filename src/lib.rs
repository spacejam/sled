// TODO heap maintenance w/ speculative write followed by CAS in pt
//      maybe with global writer lock that controls flushers too
// TODO allow waiting flusher to start collecting dirty pages
// TODO serialize flush batch in parallel
// TODO add failpoints to writepath
// TODO better formalize the need to only insert into dirty if we have a valid
//      flush epoch guard for the dirty epoch, and only use CAS otherwise
//      to avoid inserting bytes for already-serialized data
// TODO re-enable transaction tests in test_tree.rs
// TODO free empty leaves with try_lock on left sibling, set hi key, remove from indexes, store deletion in metadata_store
// TODO set explicit max key and value sizes w/ corresponding heap

// NB: this macro must appear before the following mod statements
// for it to be usable within them. One of the few places where
// this sort of ordering matters in Rust.
macro_rules! builder {
    ($(($name:ident, $t:ty, $desc:expr)),*) => {
        $(
            #[doc=$desc]
            pub fn $name(mut self, to: $t) -> Self {
                self.$name = to;
                self
            }
        )*
    }
}

mod config;
mod db;
mod flush_epoch;
mod heap;
// mod meta_node;
mod metadata_store;
// mod varint;

#[cfg(any(
    feature = "testing_shred_allocator",
    feature = "testing_count_allocator"
))]
pub mod alloc;

#[cfg(feature = "for-internal-testing-only")]
mod event_verifier;

pub use crate::config::Config;
pub use crate::db::{Batch, Db, Iter};
pub use inline_array::InlineArray;

/// Opens a `Db` with a default configuration at the
/// specified path. This will create a new storage
/// directory at the specified path if it does
/// not already exist. You can use the `Db::was_recovered`
/// method to determine if your database was recovered
/// from a previous instance.
pub fn open<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<Db> {
    Config::new().path(path).open()
}

use crate::flush_epoch::{FlushEpoch, FlushEpochGuard};

/// Compare and swap result.
///
/// It returns `Ok(Ok(()))` if operation finishes successfully and
///     - `Ok(Err(CompareAndSwapError(current, proposed)))` if operation failed
///       to setup a new value. `CompareAndSwapError` contains current and
///       proposed values.
///     - `Err(Error::Unsupported)` if the database is opened in read-only mode.
///       otherwise.
pub type CompareAndSwapResult = std::io::Result<
    std::result::Result<CompareAndSwapSuccess, CompareAndSwapError>,
>;

/// Compare and swap error.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompareAndSwapError {
    /// The current value which caused your CAS to fail.
    pub current: Option<InlineArray>,
    /// Returned value that was proposed unsuccessfully.
    pub proposed: Option<InlineArray>,
}

/// Compare and swap success.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompareAndSwapSuccess {
    /// The current value which was successfully installed.
    pub new_value: Option<InlineArray>,
    /// Returned value that was previously stored.
    pub previous_value: Option<InlineArray>,
}

impl std::fmt::Display for CompareAndSwapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Compare and swap conflict")
    }
}

impl std::error::Error for CompareAndSwapError {}

#[derive(
    Debug,
    Clone,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
)]
struct NodeId(u64);

const fn _assert_public_types_send_sync() {
    use std::fmt::Debug;

    const fn _assert_send<S: Send + Clone + Debug>() {}

    const fn _assert_send_sync<S: Send + Sync + Clone + Debug>() {}

    /*
    _assert_send::<Subscriber>();
    _assert_send_sync::<Event>();
    _assert_send_sync::<Mode>();
    _assert_send_sync::<Tree>();
    */

    _assert_send::<Db>();

    _assert_send_sync::<Batch>();
    _assert_send_sync::<InlineArray>();
    _assert_send_sync::<Config>();
    _assert_send_sync::<CompareAndSwapSuccess>();
    _assert_send_sync::<CompareAndSwapError>();
}

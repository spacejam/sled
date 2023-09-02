// TODO remove all Drop logic that checks Arc::strong_count, all are race conditions
// TODO move dirty tracking, cache to shared level
// TODO write actual CollectionId instead of MIN in Db::flush
// TODO put aborts behind feature flags for hard crashes
// TODO heap maintenance w/ speculative write followed by CAS in pt
//      maybe with global writer lock that controls flushers too
// TODO allow waiting flusher to start collecting dirty pages as soon
//      as it is evacuated - just wait until last flush is done before
//      we persist the batch
// TODO serialize flush batch in parallel
// TODO add failpoints to writepath
// TODO re-enable transaction tests in test_tree.rs
// TODO set explicit max key and value sizes w/ corresponding heap
// TODO skim inlining output of RUSTFLAGS="-Cremark=all -Cdebuginfo=1"
// TODO measure space savings vs cost of zstd in metadata store

mod config;
mod db;
mod flush_epoch;
mod heap;
mod metadata_store;

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

use crate::flush_epoch::{FlushEpoch, FlushEpochGuard, FlushEpochTracker};

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
    Hash,
)]
struct NodeId(u64);

impl concurrent_map::Minimum for NodeId {
    const MIN: NodeId = NodeId(u64::MIN);
}

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
    Hash,
)]
struct CollectionId(u64);

impl concurrent_map::Minimum for CollectionId {
    const MIN: CollectionId = CollectionId(u64::MIN);
}

type CacheBox<const LEAF_FANOUT: usize> =
    std::sync::Arc<parking_lot::RwLock<Option<Box<Leaf<LEAF_FANOUT>>>>>;

#[derive(Debug, Clone)]
struct Node<const LEAF_FANOUT: usize> {
    // used for access in heap::Heap
    id: NodeId,
    inner: CacheBox<LEAF_FANOUT>,
}

impl<const LEAF_FANOUT: usize> PartialEq for Node<LEAF_FANOUT> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Leaf<const LEAF_FANOUT: usize> {
    lo: InlineArray,
    hi: Option<InlineArray>,
    prefix_length: usize,
    data: stack_map::StackMap<InlineArray, InlineArray, LEAF_FANOUT>,
    in_memory_size: usize,
    mutation_count: u64,
    #[serde(skip)]
    dirty_flush_epoch: Option<FlushEpoch>,
    #[serde(skip)]
    page_out_on_flush: Option<FlushEpoch>,
    #[serde(skip)]
    deleted: Option<FlushEpoch>,
}

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

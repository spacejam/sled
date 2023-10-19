// TODO collection_id on page_in checks - it needs to be pinned w/ heap's EBR?
// TODO after defrag, reduce self.tip while popping the max items in the free list
// TODO store low key and collection ID directly on Object
// TODO make the Arc<Option<Box<Leaf just a single pointer chase w/ custom container
// TODO heap maintenance w/ speculative write followed by CAS in pt
//      maybe with global writer lock that controls flushers too
// TODO implement create exclusive
// TODO test concurrent drop_tree when other threads are still using it
// TODO list trees test for recovering empty collections
// TODO put aborts behind feature flags for hard crashes
// TODO allow waiting flusher to start collecting dirty pages as soon
//      as it is evacuated - just wait until last flush is done before
//      we persist the batch
// TODO serialize flush batch in parallel
// TODO add failpoints to writepath
// TODO re-enable transaction tests in test_tree.rs
// TODO set explicit max key and value sizes w/ corresponding heap
// TODO skim inlining output of RUSTFLAGS="-Cremark=all -Cdebuginfo=1"
// TODO measure space savings vs cost of zstd in metadata store
// TODO corrupted data extraction binary
// TODO if the crash_iter test panics, the test doesn't fail as expected

mod config;
mod db;
mod flush_epoch;
mod heap;
mod id_allocator;
mod metadata_store;
mod object_cache;
mod object_location_map;
mod tree;

#[cfg(any(
    feature = "testing_shred_allocator",
    feature = "testing_count_allocator"
))]
pub mod alloc;

#[cfg(feature = "for-internal-testing-only")]
mod event_verifier;

#[inline]
fn debug_delay() {
    #[cfg(debug_assertions)]
    {
        let rand =
            std::time::SystemTime::UNIX_EPOCH.elapsed().unwrap().as_nanos();

        if rand % 128 > 100 {
            for _ in 0..rand % 16 {
                std::thread::yield_now();
            }
        }
    }
}

pub use crate::config::Config;
pub use crate::db::Db;
pub use crate::tree::{Batch, Iter, Tree};
pub use inline_array::InlineArray;

const NAME_MAPPING_COLLECTION_ID: CollectionId = CollectionId(0);
const DEFAULT_COLLECTION_ID: CollectionId = CollectionId(1);
const INDEX_FANOUT: usize = 64;
const EBR_LOCAL_GC_BUFFER_SIZE: usize = 128;

use std::ops::Bound;

use crate::heap::{
    recover, Heap, HeapRecovery, ObjectRecovery, SlabAddress, Stats, Update,
};
use crate::id_allocator::{Allocator, DeferredFree};
use crate::metadata_store::MetadataStore;
use crate::object_cache::{Dirty, ObjectCache};

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

type Index<const LEAF_FANOUT: usize> = concurrent_map::ConcurrentMap<
    InlineArray,
    Object<LEAF_FANOUT>,
    INDEX_FANOUT,
    EBR_LOCAL_GC_BUFFER_SIZE,
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
struct ObjectId(u64);

impl concurrent_map::Minimum for ObjectId {
    const MIN: ObjectId = ObjectId(u64::MIN);
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
struct Object<const LEAF_FANOUT: usize> {
    // used for access in heap::Heap
    object_id: ObjectId,
    collection_id: CollectionId,
    low_key: InlineArray,
    inner: CacheBox<LEAF_FANOUT>,
}

impl<const LEAF_FANOUT: usize> PartialEq for Object<LEAF_FANOUT> {
    fn eq(&self, other: &Self) -> bool {
        self.object_id == other.object_id
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

impl<const LEAF_FANOUT: usize> Default for Leaf<LEAF_FANOUT> {
    fn default() -> Leaf<LEAF_FANOUT> {
        Leaf {
            lo: InlineArray::default(),
            hi: None,
            prefix_length: 0,
            data: stack_map::StackMap::default(),
            // this does not need to be marked as dirty until it actually
            // receives inserted data
            dirty_flush_epoch: None,
            in_memory_size: std::mem::size_of::<Leaf<LEAF_FANOUT>>(),
            mutation_count: 0,
            page_out_on_flush: None,
            deleted: None,
        }
    }
}

/// Stored on `Db` and `Tree` in an Arc, so that when the
/// last "high-level" struct is dropped, the flusher thread
/// is cleaned up.
struct ShutdownDropper<const LEAF_FANOUT: usize> {
    shutdown_sender: parking_lot::Mutex<
        std::sync::mpsc::Sender<std::sync::mpsc::Sender<()>>,
    >,
    cache: parking_lot::Mutex<object_cache::ObjectCache<LEAF_FANOUT>>,
}

impl<const LEAF_FANOUT: usize> Drop for ShutdownDropper<LEAF_FANOUT> {
    fn drop(&mut self) {
        let (tx, rx) = std::sync::mpsc::channel();
        log::debug!("sending shutdown signal to flusher");
        if self.shutdown_sender.lock().send(tx).is_ok() {
            if let Err(e) = rx.recv() {
                log::error!("failed to shut down flusher thread: {:?}", e);
            } else {
                log::debug!("flush thread successfully terminated");
            }
        } else {
            log::debug!(
                "failed to shut down flusher, manually flushing ObjectCache"
            );
            let cache = self.cache.lock();
            if let Err(e) = cache.flush() {
                log::error!(
                    "Db flusher encountered error while flushing: {:?}",
                    e
                );
                cache.set_error(&e);
            }
        }
    }
}

fn map_bound<T, U, F: FnOnce(T) -> U>(bound: Bound<T>, f: F) -> Bound<U> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(x) => Bound::Included(f(x)),
        Bound::Excluded(x) => Bound::Excluded(f(x)),
    }
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

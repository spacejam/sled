// 1.0 blockers
//
// bugs
// * object_cache violation of flush responsibility for second read of expected cooperative serialization. leaf in question's dirty_flush_epoch is Some(FlushEpoch(3)), our expected key was (FlushEpoch(2), ObjectId(263)). node.deleted: None
//
// reliability
// TODO test concurrent drop_tree when other threads are still using it
// TODO list trees test for recovering empty collections
// TODO set explicit max key and value sizes w/ corresponding heap
// TODO add failpoints to writepath
//
// performance
// TODO handle prefix encoding
//
// features
// TODO multi-collection batch
//
// misc
// TODO skim inlining output of RUSTFLAGS="-Cremark=all -Cdebuginfo=1"
//
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~ 1.0 cutoff ~~~~~~~~~~~~~~~~~~~~~~~~~~~
//
// post-1.0 improvements
//
// reliability
// TODO bug hiding: if the crash_iter test panics, the test doesn't fail as expected
// TODO event log assertion for testing heap location bidirectional referential integrity,
//      particularly in the object location mapper.
// TODO ensure nothing "from the future" gets copied into earlier epochs during GC
// TODO collection_id on page_in checks - it needs to be pinned w/ heap's EBR?
// TODO put aborts behind feature flags for hard crashes
// TODO re-enable transaction tests in test_tree.rs
//
// performance
// TODO force writers to flush when some number of dirty epochs have built up
// TODO serialize flush batch in parallel
// TODO concurrent serialization of NotYetSerialized dirty objects
// TODO make the Arc<Option<Box<Leaf just a single pointer chase w/ custom container
// TODO allow waiting flusher to start collecting dirty pages as soon
//      as it is evacuated - just wait until last flush is done before
//      we persist the batch
// TODO measure space savings vs cost of zstd in metadata store
// TODO make EBR and index fanout consts as small as possible to reduce memory usage
// TODO make leaf fanout as small as possible while retaining perf
// TODO dynamically sized fanouts for reducing fragmentation
//
// features
// TODO transactions
// TODO implement create exclusive
// TODO temporary trees for transactional in-memory coordination
// TODO corrupted data extraction binary
//

//! `sled` is a high-performance embedded database with
//! an API that is similar to a `BTreeMap<[u8], [u8]>`,
//! but with several additional capabilities for
//! assisting creators of stateful systems.
//!
//! It is fully thread-safe, and all operations are
//! atomic. Multiple `Tree`s with isolated keyspaces
//! are supported with the
//! [`Db::open_tree`](struct.Db.html#method.open_tree) method.
//!
//! `sled` is built by experienced database engineers
//! who think users should spend less time tuning and
//! working against high-friction APIs. Expect
//! significant ergonomic and performance improvements
//! over time. Most surprises are bugs, so please
//! [let us know](mailto:tylerneely@gmail.com?subject=sled%20sucks!!!)
//! if something is high friction.
//!
//! # Examples
//!
//! ```
//! # let _ = std::fs::remove_dir_all("my_db");
//! let db: sled::Db = sled::open("my_db").unwrap();
//!
//! // insert and get
//! db.insert(b"yo!", b"v1");
//! assert_eq!(&db.get(b"yo!").unwrap().unwrap(), b"v1");
//!
//! // Atomic compare-and-swap.
//! db.compare_and_swap(
//!     b"yo!",      // key
//!     Some(b"v1"), // old value, None for not present
//!     Some(b"v2"), // new value, None for delete
//! )
//! .unwrap();
//!
//! // Iterates over key-value pairs, starting at the given key.
//! let scan_key: &[u8] = b"a non-present key before yo!";
//! let mut iter = db.range(scan_key..);
//! assert_eq!(&iter.next().unwrap().unwrap().0, b"yo!");
//! assert!(iter.next().is_none());
//!
//! db.remove(b"yo!");
//! assert!(db.get(b"yo!").unwrap().is_none());
//!
//! let other_tree: sled::Tree = db.open_tree(b"cool db facts").unwrap();
//! other_tree.insert(
//!     b"k1",
//!     &b"a Db acts like a Tree due to implementing Deref<Target = Tree>"[..]
//! ).unwrap();
//! # let _ = std::fs::remove_dir_all("my_db");
//! ```
mod config;
mod db;
mod flush_epoch;
mod heap;
mod id_allocator;
mod leaf;
mod metadata_store;
mod object_cache;
mod object_location_mapper;
mod tree;
mod trigger;

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
pub use crate::trigger::{Trigger, TriggerTransform};
pub use inline_array::InlineArray;

const NAME_MAPPING_COLLECTION_ID: CollectionId = CollectionId(0);
const DEFAULT_COLLECTION_ID: CollectionId = CollectionId(1);
const INDEX_FANOUT: usize = 64;
const EBR_LOCAL_GC_BUFFER_SIZE: usize = 128;

use std::num::NonZeroU64;
use std::ops::Bound;

use crate::heap::{
    recover, Heap, HeapRecovery, ObjectRecovery, SlabAddress, Stats, Update,
};
use crate::id_allocator::{Allocator, DeferredFree};
use crate::leaf::Leaf;
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

use crate::flush_epoch::{
    FlushEpoch, FlushEpochGuard, FlushEpochTracker, FlushInvariants,
};

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
struct ObjectId(NonZeroU64);

impl ObjectId {
    fn new(from: u64) -> Option<ObjectId> {
        NonZeroU64::new(from).map(ObjectId)
    }
}

impl std::ops::Deref for ObjectId {
    type Target = u64;

    fn deref(&self) -> &u64 {
        let self_ref: &NonZeroU64 = &self.0;

        // NonZeroU64 is repr(transparent) where it wraps a u64
        // so it is guaranteed to match the binary layout. This
        // makes it safe to cast a reference to one as a reference
        // to the other like this.
        let self_ptr: *const NonZeroU64 = self_ref as *const _;
        let reference: *const u64 = self_ptr as *const u64;

        unsafe { &*reference }
    }
}

impl concurrent_map::Minimum for ObjectId {
    const MIN: ObjectId = ObjectId(NonZeroU64::MIN);
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
pub struct TriggerId(u64);

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
pub struct CollectionId(u64);

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

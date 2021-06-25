//! `sled` is an embedded database with
//! an API that is similar to a `BTreeMap<[u8], [u8]>`,
//! but with several additional capabilities for
//! assisting creators of stateful systems.
//!
//! It is fully thread-safe, and all operations are
//! atomic. Most are fully non-blocking. Multiple
//! `Tree`s with isolated keyspaces are supported with the
//! [`Db::open_tree`](struct.Db.html#method.open_tree) method.
//!
//! ACID transactions involving reads and writes to
//! multiple items are supported with the
//! [`Tree::transaction`](struct.Tree.html#method.transaction)
//! method. Transactions may also operate over
//! multiple `Tree`s (see
//! [`Tree::transaction`](struct.Tree.html#method.transaction)
//! docs for more info).
//!
//! Users may also subscribe to updates on individual
//! `Tree`s by using the
//! [`Tree::watch_prefix`](struct.Tree.html#method.watch_prefix)
//! method, which returns a blocking `Iterator` over
//! updates to keys that begin with the provided
//! prefix. You may supply an empty prefix to subscribe
//! to everything.
//!
//! [Merge operators](https://github.com/spacejam/sled/wiki/merge-operators)
//! (aka read-modify-write operators) are supported. A
//! merge operator is a function that specifies
//! how new data can be merged into an existing value
//! without requiring both a read and a write.
//! Using the
//! [`Tree::merge`](struct.Tree.html#method.merge)
//! method, you may "push" data to a `Tree` value
//! and have the provided merge operator combine
//! it with the existing value, if there was one.
//! They are set on a per-`Tree` basis, and essentially
//! allow any sort of data structure to be built
//! using merges as an atomic high-level operation.
//!
//! `sled` is built by experienced database engineers
//! who think users should spend less time tuning and
//! working against high-friction APIs. Expect
//! significant ergonomic and performance improvements
//! over time. Most surprises are bugs, so please
//! [let us know](mailto:t@jujit.su?subject=sled%20sucks!!!) if something
//! is high friction.
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
//! assert_eq!(iter.next(), None);
//!
//! db.remove(b"yo!");
//! assert_eq!(db.get(b"yo!"), Ok(None));
//!
//! let other_tree: sled::Tree = db.open_tree(b"cool db facts").unwrap();
//! other_tree.insert(
//!     b"k1",
//!     &b"a Db acts like a Tree due to implementing Deref<Target = Tree>"[..]
//! ).unwrap();
//! # let _ = std::fs::remove_dir_all("my_db");
//! ```
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/spacejam/sled/main/art/tree_face_anti-transphobia.png"
)]
#![cfg_attr(
    feature = "testing",
    deny(
        missing_docs,
        future_incompatible,
        nonstandard_style,
        rust_2018_idioms,
        missing_copy_implementations,
        trivial_casts,
        trivial_numeric_casts,
        unsafe_code,
        unused_qualifications,
    )
)]
#![cfg_attr(feature = "testing", deny(
    // over time, consider enabling the commented-out lints below
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::decimal_literal_representation,
    clippy::doc_markdown,
    // clippy::else_if_without_else,
    clippy::empty_enum,
    clippy::explicit_into_iter_loop,
    clippy::explicit_iter_loop,
    clippy::expl_impl_clone_on_copy,
    clippy::fallible_impl_from,
    clippy::filter_map_next,
    clippy::float_arithmetic,
    clippy::get_unwrap,
    clippy::if_not_else,
    // clippy::indexing_slicing,
    clippy::inline_always,
    //clippy::integer_arithmetic,
    clippy::invalid_upcast_comparisons,
    clippy::items_after_statements,
    clippy::manual_find_map,
    clippy::map_entry,
    clippy::map_flatten,
    clippy::match_like_matches_macro,
    clippy::match_same_arms,
    clippy::maybe_infinite_iter,
    clippy::mem_forget,
    // clippy::missing_docs_in_private_items,
    clippy::module_name_repetitions,
    clippy::multiple_inherent_impl,
    clippy::mut_mut,
    clippy::needless_borrow,
    clippy::needless_continue,
    clippy::needless_pass_by_value,
    clippy::non_ascii_literal,
    clippy::path_buf_push_overwrite,
    clippy::print_stdout,
    clippy::pub_enum_variant_names,
    clippy::redundant_closure_for_method_calls,
    clippy::shadow_reuse,
    clippy::shadow_same,
    clippy::shadow_unrelated,
    clippy::single_match_else,
    clippy::string_add,
    clippy::string_add_assign,
    clippy::type_repetition_in_bounds,
    clippy::unicode_not_nfc,
    // clippy::unimplemented,
    clippy::unseparated_literal_suffix,
    clippy::used_underscore_binding,
    clippy::wildcard_dependencies,
    clippy::wrong_pub_self_convention,
))]
#![cfg_attr(
    feature = "testing",
    warn(
        clippy::missing_const_for_fn,
        clippy::multiple_crate_versions,
        // clippy::wildcard_enum_match_arm,
    )
)]

macro_rules! io_fail {
    ($config:expr, $e:expr) => {
        #[cfg(feature = "failpoints")]
        {
            debug_delay();
            if fail::is_active($e) {
                $config.set_global_error(Error::FailPoint);
                return Err(Error::FailPoint).into();
            }
        }
    };
}

macro_rules! testing_assert {
    ($($e:expr),*) => {
        #[cfg(feature = "testing")]
        assert!($($e),*)
    };
}

mod atomic_shim;
mod backoff;
mod batch;
mod cache_padded;
mod concurrency_control;
mod config;
mod context;
mod db;
mod dll;
mod ebr;
mod fastcmp;
mod fastlock;
mod fnv;
mod histogram;
mod iter;
mod ivec;
mod lazy;
mod lru;
mod meta;
#[cfg(feature = "metrics")]
mod metrics;
mod node;
mod oneshot;
mod pagecache;
mod result;
mod serialization;
mod stack;
mod subscriber;
mod sys_limits;
mod threadpool;
pub mod transaction;
mod tree;
mod varint;

/// Functionality for conditionally triggering failpoints under test.
#[cfg(feature = "failpoints")]
pub mod fail;

#[cfg(feature = "docs")]
pub mod doc;

#[cfg(all(
    not(miri),
    any(
        windows,
        target_os = "linux",
        target_os = "macos",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "ios",
    )
))]
mod flusher;

#[cfg(feature = "event_log")]
/// The event log helps debug concurrency issues.
pub mod event_log;

/// Opens a `Db` with a default configuration at the
/// specified path. This will create a new storage
/// directory at the specified path if it does
/// not already exist. You can use the `Db::was_recovered`
/// method to determine if your database was recovered
/// from a previous instance. You can use `Config::create_new`
/// if you want to increase the chances that the database
/// will be freshly created.
pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Db> {
    Config::new().path(path).open()
}

/// Print a performance profile to standard out
/// detailing what the internals of the system are doing.
///
/// Requires the `metrics` feature to be enabled,
/// which may introduce a bit of memory and overall
/// performance overhead as lots of metrics are
/// tallied up. Nevertheless, it is a useful
/// tool for quickly understanding the root of
/// a performance problem, and it can be invaluable
/// for including in any opened issues.
#[cfg(feature = "metrics")]
#[allow(clippy::print_stdout)]
pub fn print_profile() {
    println!("{}", M.format_profile());
}

/// hidden re-export of items for testing purposes
#[doc(hidden)]
pub use self::{
    config::RunningConfig,
    lazy::Lazy,
    pagecache::{
        constants::{
            MAX_MSG_HEADER_LEN, MAX_SPACE_AMPLIFICATION, SEG_HEADER_LEN,
        },
        BatchManifest, DiskPtr, Log, LogKind, LogOffset, LogRead, Lsn,
        PageCache, PageId,
    },
    serialization::Serialize,
};

pub use self::{
    batch::Batch,
    config::{Config, Mode},
    db::Db,
    iter::Iter,
    ivec::IVec,
    result::{Error, Result},
    subscriber::{Event, Subscriber},
    transaction::Transactional,
    tree::{CompareAndSwapError, Tree},
};

#[cfg(feature = "metrics")]
use self::{
    histogram::Histogram,
    metrics::{clock, Measure, M},
};

use {
    self::{
        atomic_shim::{AtomicI64 as AtomicLsn, AtomicU64},
        backoff::Backoff,
        cache_padded::CachePadded,
        concurrency_control::Protector,
        context::Context,
        ebr::{
            pin as crossbeam_pin, Atomic, Guard as CrossbeamGuard, Owned,
            Shared,
        },
        fastcmp::fastcmp,
        lru::Lru,
        meta::Meta,
        node::Node,
        oneshot::{OneShot, OneShotFiller},
        result::CasResult,
        subscriber::Subscribers,
        tree::TreeInner,
    },
    log::{debug, error, trace, warn},
    pagecache::{constants::MAX_BLOB, RecoveryGuard},
    parking_lot::{Condvar, Mutex, RwLock},
    std::{
        collections::BTreeMap,
        convert::TryFrom,
        fmt::{self, Debug},
        io::{Read, Write},
        sync::{
            atomic::{
                AtomicUsize,
                Ordering::{Acquire, Relaxed, Release, SeqCst},
            },
            Arc,
        },
    },
};

#[doc(hidden)]
pub fn pin() -> Guard {
    Guard { inner: crossbeam_pin() }
}

#[doc(hidden)]
pub struct Guard {
    inner: CrossbeamGuard,
}

impl std::ops::Deref for Guard {
    type Target = CrossbeamGuard;

    fn deref(&self) -> &CrossbeamGuard {
        &self.inner
    }
}

#[derive(Debug)]
struct Conflict;

type Conflictable<T> = std::result::Result<T, Conflict>;

fn crc32(buf: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(buf);
    hasher.finalize()
}

fn calculate_message_crc32(header: &[u8], body: &[u8]) -> u32 {
    trace!(
        "calculating crc32 for header len {} body len {}",
        header.len(),
        body.len()
    );
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(body);
    hasher.update(&header[4..]);
    let crc32 = hasher.finalize();
    crc32 ^ 0xFFFF_FFFF
}

#[cfg(any(test, feature = "lock_free_delays"))]
mod debug_delay;

#[cfg(any(test, feature = "lock_free_delays"))]
use debug_delay::debug_delay;

/// This function is useful for inducing random jitter into our atomic
/// operations, shaking out more possible interleavings quickly. It gets
/// fully eliminated by the compiler in non-test code.
#[cfg(not(any(test, feature = "lock_free_delays")))]
const fn debug_delay() {}

/// Link denotes a tree node or its modification fragment such as
/// key addition or removal.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Link {
    /// A new value is set for a given key
    Set(IVec, IVec),
    /// The kv pair at a particular index is removed
    Del(IVec),
    /// A child of this Index node is marked as mergable
    ParentMergeIntention(PageId),
    /// The merging child has been completely merged into its left sibling
    ParentMergeConfirm,
    /// A Node is marked for being merged into its left sibling
    ChildMergeCap,
}

/// A fast map that is not resistant to collision attacks. Works
/// on 8 bytes at a time.
#[cfg(not(feature = "testing"))]
pub(crate) type FastMap8<K, V> =
    std::collections::HashMap<K, V, std::hash::BuildHasherDefault<fnv::Hasher>>;

#[cfg(feature = "testing")]
pub(crate) type FastMap8<K, V> = BTreeMap<K, V>;

/// A fast set that is not resistant to collision attacks. Works
/// on 8 bytes at a time.
#[cfg(not(feature = "testing"))]
pub(crate) type FastSet8<V> =
    std::collections::HashSet<V, std::hash::BuildHasherDefault<fnv::Hasher>>;

#[cfg(feature = "testing")]
pub(crate) type FastSet8<V> = std::collections::BTreeSet<V>;

#[cfg(not(feature = "testing"))]
use std::collections::HashMap as Map;

// we avoid HashMap while testing because
// it makes tests non-deterministic
#[cfg(feature = "testing")]
use std::collections::{BTreeMap as Map, BTreeSet as Set};

/// A function that may be configured on a particular shared `Tree`
/// that will be applied as a kind of read-modify-write operator
/// to any values that are written using the `Tree::merge` method.
///
/// The first argument is the key. The second argument is the
/// optional existing value that was in place before the
/// merged value being applied. The Third argument is the
/// data being merged into the item.
///
/// You may return `None` to delete the value completely.
///
/// Merge operators are shared by all instances of a particular
/// `Tree`. Different merge operators may be set on different
/// `Tree`s.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use sled::{Config, IVec};
///
/// fn concatenate_merge(
///   _key: &[u8],               // the key being merged
///   old_value: Option<&[u8]>,  // the previous value, if one existed
///   merged_bytes: &[u8]        // the new bytes being merged in
/// ) -> Option<Vec<u8>> {       // set the new value, return None to delete
///   let mut ret = old_value
///     .map(|ov| ov.to_vec())
///     .unwrap_or_else(|| vec![]);
///
///   ret.extend_from_slice(merged_bytes);
///
///   Some(ret)
/// }
///
/// let config = Config::new()
///   .temporary(true);
///
/// let tree = config.open()?;
/// tree.set_merge_operator(concatenate_merge);
///
/// let k = b"k1";
///
/// tree.insert(k, vec![0]);
/// tree.merge(k, vec![1]);
/// tree.merge(k, vec![2]);
/// assert_eq!(tree.get(k), Ok(Some(IVec::from(vec![0, 1, 2]))));
///
/// // Replace previously merged data. The merge function will not be called.
/// tree.insert(k, vec![3]);
/// assert_eq!(tree.get(k), Ok(Some(IVec::from(vec![3]))));
///
/// // Merges on non-present values will cause the merge function to be called
/// // with `old_value == None`. If the merge function returns something (which it
/// // does, in this case) a new value will be inserted.
/// tree.remove(k);
/// tree.merge(k, vec![4]);
/// assert_eq!(tree.get(k), Ok(Some(IVec::from(vec![4]))));
/// # Ok(()) }
/// ```
pub trait MergeOperator:
    Send + Sync + Fn(&[u8], Option<&[u8]>, &[u8]) -> Option<Vec<u8>>
{
}
impl<F> MergeOperator for F where
    F: Send + Sync + Fn(&[u8], Option<&[u8]>, &[u8]) -> Option<Vec<u8>>
{
}

mod compile_time_assertions {
    use crate::*;

    #[allow(unreachable_code)]
    fn _assert_public_types_send_sync() {
        _assert_send::<Subscriber>(unreachable!());

        _assert_send_sync::<Iter>(unreachable!());
        _assert_send_sync::<Tree>(unreachable!());
        _assert_send_sync::<Db>(unreachable!());
        _assert_send_sync::<Batch>(unreachable!());
        _assert_send_sync::<IVec>(unreachable!());
        _assert_send_sync::<Config>(unreachable!());
        _assert_send_sync::<CompareAndSwapError>(unreachable!());
        _assert_send_sync::<Error>(unreachable!());
        _assert_send_sync::<Event>(unreachable!());
        _assert_send_sync::<Mode>(unreachable!());
    }

    fn _assert_send<S: Send>(_: &S) {}

    fn _assert_send_sync<S: Send + Sync>(_: &S) {}
}

#[cfg(all(unix, not(miri)))]
fn maybe_fsync_directory<P: AsRef<std::path::Path>>(
    path: P,
) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

#[cfg(any(not(unix), miri))]
fn maybe_fsync_directory<P: AsRef<std::path::Path>>(
    _: P,
) -> std::io::Result<()> {
    Ok(())
}

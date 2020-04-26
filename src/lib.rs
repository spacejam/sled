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
//! let t = sled::open("my_db").unwrap();
//!
//! // insert and get
//! t.insert(b"yo!", b"v1");
//! assert_eq!(&t.get(b"yo!").unwrap().unwrap(), b"v1");
//!
//! // Atomic compare-and-swap.
//! t.compare_and_swap(
//!     b"yo!",      // key
//!     Some(b"v1"), // old value, None for not present
//!     Some(b"v2"), // new value, None for delete
//! )
//! .unwrap();
//!
//! // Iterates over key-value pairs, starting at the given key.
//! let scan_key: &[u8] = b"a non-present key before yo!";
//! let mut iter = t.range(scan_key..);
//! assert_eq!(&iter.next().unwrap().unwrap().0, b"yo!");
//! assert_eq!(iter.next(), None);
//!
//! t.remove(b"yo!");
//! assert_eq!(t.get(b"yo!"), Ok(None));
//! # let _ = std::fs::remove_dir_all("my_db");
//! ```
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/spacejam/sled/master/art/tree_face_anti-transphobia.png"
)]
#![cfg_attr(test, deny(warnings))]
#![deny(
    missing_docs,
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unused_qualifications
)]
#![deny(
    // over time, consider enabling the commented-out lints below
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::checked_conversions,
    clippy::decimal_literal_representation,
    clippy::doc_markdown,
    // clippy::else_if_without_else,
    clippy::empty_enum,
    clippy::explicit_into_iter_loop,
    clippy::explicit_iter_loop,
    clippy::expl_impl_clone_on_copy,
    clippy::fallible_impl_from,
    clippy::filter_map,
    clippy::filter_map_next,
    clippy::find_map,
    clippy::float_arithmetic,
    clippy::get_unwrap,
    clippy::if_not_else,
    // clippy::indexing_slicing,
    clippy::inline_always,
    //clippy::integer_arithmetic,
    clippy::invalid_upcast_comparisons,
    clippy::items_after_statements,
    clippy::map_flatten,
    clippy::match_same_arms,
    clippy::maybe_infinite_iter,
    clippy::mem_forget,
    // clippy::missing_const_for_fn,
    // clippy::missing_docs_in_private_items,
    clippy::module_name_repetitions,
    clippy::multiple_crate_versions,
    clippy::multiple_inherent_impl,
    clippy::mut_mut,
    clippy::needless_borrow,
    clippy::needless_continue,
    clippy::needless_pass_by_value,
    clippy::non_ascii_literal,
    clippy::option_map_unwrap_or,
    clippy::option_map_unwrap_or_else,
    clippy::path_buf_push_overwrite,
    clippy::print_stdout,
    clippy::pub_enum_variant_names,
    clippy::redundant_closure_for_method_calls,
    clippy::replace_consts,
    clippy::result_map_unwrap_or_else,
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
    // clippy::wildcard_enum_match_arm,
    clippy::wrong_pub_self_convention,
)]
#![allow(clippy::mem_replace_with_default)] // Not using std::mem::take() due to MSRV of 1.37
#![recursion_limit = "128"]

macro_rules! io_fail {
    ($config:expr, $e:expr) => {
        #[cfg(feature = "failpoints")]
        {
            if fail::is_active($e) {
                $config.set_global_error(Error::FailPoint);
                return Err(Error::FailPoint).into();
            }
        }
    };
}

macro_rules! testing_assert {
    ($($e:expr),*) => {
        #[cfg(feature = "lock_free_delays")]
        assert!($($e),*)
    };
}

mod batch;
mod binary_search;
mod concurrency_control;
mod config;
mod context;
mod db;
mod dll;
mod fastcmp;
mod fastlock;
mod histogram;
mod iter;
mod ivec;
mod lazy;
mod lru;
mod meta;
mod metrics;
mod node;
mod oneshot;
mod pagecache;
mod prefix;
mod result;
mod serialization;
mod stack;
mod stackvec;
mod subscriber;
mod sys_limits;
pub mod transaction;
mod tree;

/// Functionality for conditionally triggering failpoints under test.
#[cfg(feature = "failpoints")]
pub mod fail;

#[cfg(feature = "docs")]
pub mod doc;

#[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
mod threadpool {
    use super::OneShot;

    /// Just execute a task without involving threads.
    pub fn spawn<F, R>(work: F) -> OneShot<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (promise_filler, promise) = OneShot::pair();
        promise_filler.fill((work)());
        return promise;
    }
}

#[cfg(any(windows, target_os = "linux", target_os = "macos"))]
mod threadpool;

#[cfg(any(windows, target_os = "linux", target_os = "macos"))]
mod flusher;

#[cfg(feature = "event_log")]
/// The event log helps debug concurrency issues.
pub mod event_log;

#[cfg(feature = "measure_allocs")]
mod measure_allocs;

#[cfg(feature = "measure_allocs")]
#[global_allocator]
static ALLOCATOR: measure_allocs::TrackingAllocator =
    measure_allocs::TrackingAllocator;

const DEFAULT_TREE_ID: &[u8] = b"__sled__default";

/// hidden re-export of items for testing purposes
#[doc(hidden)]
pub use {
    self::{
        config::RunningConfig,
        lazy::Lazy,
        pagecache::{
            constants::{
                MAX_MSG_HEADER_LEN, MAX_SPACE_AMPLIFICATION,
                MINIMUM_ITEMS_PER_SEGMENT, SEG_HEADER_LEN,
            },
            BatchManifest, DiskPtr, Log, LogKind, LogOffset, LogRead, Lsn,
            PageCache, PageId,
        },
        serialization::Serialize,
    },
    crossbeam_epoch::{pin, Atomic, Guard, Owned, Shared},
};

pub use self::{
    batch::Batch,
    config::{Config, Mode},
    db::{open, Db},
    iter::Iter,
    ivec::IVec,
    result::{Error, Result},
    subscriber::{Event, Subscriber},
    transaction::Transactional,
    tree::{CompareAndSwapError, Tree},
};

use {
    self::{
        binary_search::binary_search_lub,
        concurrency_control::{ConcurrencyControl, Protector},
        context::Context,
        fastcmp::fastcmp,
        histogram::Histogram,
        lru::Lru,
        meta::Meta,
        metrics::{clock, Measure, M},
        node::{Data, Node},
        oneshot::{OneShot, OneShotFiller},
        result::CasResult,
        stackvec::StackVec,
        subscriber::Subscribers,
        tree::TreeInner,
    },
    crossbeam_utils::{Backoff, CachePadded},
    log::{debug, error, trace, warn},
    pagecache::RecoveryGuard,
    parking_lot::{Condvar, Mutex, RwLock},
    std::{
        collections::BTreeMap,
        convert::TryFrom,
        fmt::{self, Debug},
        io::{Read, Write},
        sync::{
            atomic::{
                AtomicI64 as AtomicLsn, AtomicU64, AtomicUsize,
                Ordering::{Acquire, Relaxed, Release, SeqCst},
            },
            Arc,
        },
    },
};

fn crc32(buf: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(buf);
    hasher.finalize()
}

fn calculate_message_crc32(header: &[u8], body: &[u8]) -> u32 {
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
    /// The associated value is removed for a given key
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
pub(crate) type FastMap8<K, V> = std::collections::HashMap<
    K,
    V,
    std::hash::BuildHasherDefault<fxhash::FxHasher64>,
>;

/// A fast set that is not resistant to collision attacks. Works
/// on 8 bytes at a time.
pub(crate) type FastSet8<V> = std::collections::HashSet<
    V,
    std::hash::BuildHasherDefault<fxhash::FxHasher64>,
>;

/// Allows arbitrary logic to be injected into mere operations of the
/// `PageCache`.
pub trait MergeOperator:
    Fn(&[u8], Option<&[u8]>, &[u8]) -> Option<Vec<u8>>
{
}
impl<F> MergeOperator for F where
    F: Fn(&[u8], Option<&[u8]>, &[u8]) -> Option<Vec<u8>>
{
}

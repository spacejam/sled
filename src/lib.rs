//! `sled` is a high-performance embedded database with
//! an API that is similar to a `BTreeMap<[u8], [u8]>`,
//! but with several additional capabilities for
//! assisting creators of stateful systems.
//!
//! It is fully thread-safe, and all operations are
//! atomic. Multiple `Tree`s are supported with the
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
//! use sled::Db;
//!
//! let t = Db::open("my_db").unwrap();
//!
//! // insert and get
//! t.insert(b"yo!", b"v1");
//! assert_eq!(&t.get(b"yo!").unwrap().unwrap(), b"v1");
//!
//! // Atomic compare-and-swap.
//! t.cas(
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
//! ```
#![cfg_attr(test, deny(warnings))]
#![deny(missing_docs)]
#![deny(future_incompatible)]
#![deny(nonstandard_style)]
#![deny(rust_2018_idioms)]
#![deny(missing_copy_implementations)]
#![deny(trivial_casts)]
#![deny(trivial_numeric_casts)]
#![deny(unsafe_code)]

#[cfg(feature = "failpoints")]
use fail::fail_point;

macro_rules! maybe_fail {
    ($e:expr) => {
        #[cfg(feature = "failpoints")]
        fail_point!($e, |_| Err(Error::FailPoint));
    };
}

mod batch;
mod binary_search;
mod config;
mod context;
mod data;
mod db;
mod dll;
mod frag;
mod histogram;
mod iter;
mod ivec;
mod lazy;
mod lru;
mod materializer;
mod meta;
mod metrics;
mod node;
mod oneshot;
mod pagecache;
mod pagetable;
mod prefix;
mod result;
mod stack;
mod subscription;
mod threadpool;
mod tree;
mod tx;
mod vecset;

#[cfg(any(windows, target_os = "linux", target_os = "macos"))]
mod flusher;

#[cfg(feature = "event_log")]
/// The event log helps debug concurrency issues.
pub mod event_log;

const DEFAULT_TREE_ID: &[u8] = b"__sled__default";

/// hidden re-export of items for testing purposes
#[doc(hidden)]
pub use {
    self::{
        lazy::Lazy,
        pagecache::{
            constants::{
                MAX_SPACE_AMPLIFICATION, MINIMUM_ITEMS_PER_SEGMENT,
                MSG_HEADER_LEN, SEG_HEADER_LEN,
            },
            DiskPtr, Log, LogId, LogKind, LogRead, Lsn, Materializer,
            PageCache, PageId, SegmentMode,
        },
        pagetable::PAGETABLE_NODE_SZ,
    },
    crossbeam_epoch::{pin, Atomic, Guard, Owned, Shared},
};

pub use self::{
    batch::Batch,
    config::{Config, ConfigBuilder},
    db::Db,
    iter::Iter,
    ivec::IVec,
    result::{Error, Result},
    subscription::{Event, Subscriber},
    tree::Tree,
    tx::{
        TransactionError, TransactionResult, Transactional, TransactionalTree,
    },
};

use {
    self::{
        binary_search::binary_search_lub,
        config::PersistedConfig,
        context::Context,
        data::Data,
        frag::Frag,
        histogram::Histogram,
        lru::Lru,
        meta::Meta,
        metrics::{clock, measure, Measure, M},
        node::Node,
        oneshot::{OneShot, OneShotFiller},
        pagetable::PageTable,
        prefix::{
            prefix_cmp, prefix_cmp_encoded, prefix_decode, prefix_encode,
            prefix_reencode,
        },
        result::CasResult,
        stack::{node_from_frag_vec, Stack, StackIter},
        subscription::Subscriptions,
        tree::TreeInner,
        vecset::VecSet,
    },
    bincode::{deserialize, serialize},
    crossbeam_utils::{Backoff, CachePadded},
    log::{debug, error, trace, warn},
    pagecache::RecoveryGuard,
    parking_lot::{Condvar, Mutex, RwLock},
    serde::{de::DeserializeOwned, Deserialize, Serialize},
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
    hasher.update(&buf);
    hasher.finalize()
}

type TreePtr<'g> = pagecache::PagePtr<'g, Frag>;

#[cfg(any(test, feature = "lock_free_delays"))]
mod debug_delay;

#[cfg(any(test, feature = "lock_free_delays"))]
use debug_delay::debug_delay;

/// This function is useful for inducing random jitter into our atomic
/// operations, shaking out more possible interleavings quickly. It gets
/// fully eliminated by the compiler in non-test code.
#[cfg(not(any(test, feature = "lock_free_delays")))]
fn debug_delay() {}

/// A fast map that is not resistant to collision attacks. Works
/// on 8 bytes at a time.
pub type FastMap8<K, V> = std::collections::HashMap<
    K,
    V,
    std::hash::BuildHasherDefault<fxhash::FxHasher64>,
>;

/// A fast set that is not resistant to collision attacks. Works
/// on 8 bytes at a time.
pub type FastSet8<V> = std::collections::HashSet<
    V,
    std::hash::BuildHasherDefault<fxhash::FxHasher64>,
>;

/// Allows arbitrary logic to be injected into mere operations of the
/// `PageCache`.
pub type MergeOperator = fn(
    key: &[u8],
    last_value: Option<&[u8]>,
    new_merge: &[u8],
) -> Option<Vec<u8>>;

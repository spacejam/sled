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
#![deny(rust_2018_compatibility)]
#![deny(rust_2018_idioms)]

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
mod context;
mod threadpool;
mod lazy;
mod data;
mod db;
mod frag;
mod iter;
mod ivec;
mod materializer;
mod meta;
mod node;
mod prefix;
mod subscription;
mod tree;
mod tx;
mod histogram;
mod metrics;
mod pagecache;
mod result;
mod oneshot;
mod config;

#[cfg(any(windows, target_os = "linux", target_os = "macos"))]
mod flusher;

#[cfg(feature = "event_log")]
/// The event log helps debug concurrency issues.
pub mod event_log;

const DEFAULT_TREE_ID: &[u8] = b"__sled__default";

pub use {
    self::{
        batch::Batch,
        db::Db,
        iter::Iter,
        ivec::IVec,
        subscription::{Event, Subscriber},
        tree::Tree,
        tx::{
            TransactionError, TransactionResult, Transactional,
            TransactionalTree,
        },
        config::{Config, ConfigBuilder},
        result::{Error, Result},
    },
};

use {
    self::{
        config::PersistedConfig,
        binary_search::binary_search_lub,
        context::Context,
        data::Data,
        result::CasResult,
        frag::Frag,
        node::Node,
        meta::Meta,
        prefix::{
            prefix_cmp, prefix_cmp_encoded, prefix_decode, prefix_encode,
            prefix_reencode,
        },
        subscription::Subscriptions,
        oneshot::{OneShot, OneShotFiller},
        histogram::Histogram,
        tree::TreeInner,
        lazy::Lazy,
        metrics::{M, clock, measure, Measure},
    },
    std::{
        fmt::{self, Debug},
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{
                AtomicUsize,
                AtomicI64 as AtomicLsn, AtomicU64,
                Ordering::{Acquire, Relaxed, Release, SeqCst},
            },
        },
        convert::TryFrom,
        io::{Read, Write},
    },
    log::{debug, error, trace, warn},
    bincode::{deserialize, serialize},

    parking_lot::{Condvar, Mutex, RwLock},
    pagecache::{
        Materializer, PageCache, PageId, RecoveryGuard,
    },
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    crossbeam_utils::{Backoff, CachePadded},
    crossbeam_epoch::{
        pin, Atomic, Guard, Owned, Shared,
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
pub type FastMap8<K, V> = std::collections::HashMap<K, V, std::hash::BuildHasherDefault<fxhash::FxHasher64>>;

/// A fast set that is not resistant to collision attacks. Works
/// on 8 bytes at a time.
pub type FastSet8<V> = std::collections::HashSet<V, std::hash::BuildHasherDefault<fxhash::FxHasher64>>;

/// Allows arbitrary logic to be injected into mere operations of the
/// `PageCache`.
pub type MergeOperator = fn(
    key: &[u8],
    last_value: Option<&[u8]>,
    new_merge: &[u8],
) -> Option<Vec<u8>>;

#[cfg(test)]
mod tests;

mod dll;
mod lru;
mod pagetable;
mod stack;
mod vecset;

use self::lru::Lru;
use self::pagetable::PageTable;
use self::stack::{node_from_frag_vec, Stack, StackIter};
use self::vecset::VecSet;

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
//! t.insert(b"yo!", b"v1");
//! assert_eq!(&t.get(b"yo!").unwrap().unwrap(), b"v1");
//!
//! // Atomic compare-and-swap.
//! t.cas(
//!     b"yo!",       // key
//!     Some(b"v1"),  // old value, None for not present
//!     Some(b"v2"),  // new value, None for delete
//! ).unwrap();
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

#![allow(clippy::cognitive_complexity)]
#![allow(clippy::default_trait_access)]

mod batch;
mod binary_search;
mod context;
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

#[cfg(any(windows, target_os = "linux", target_os = "macos"))]
mod flusher;

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
    },
    pagecache::{Config, ConfigBuilder, Error, Result},
};

use {
    self::{
        binary_search::binary_search_lub,
        context::Context,
        data::Data,
        frag::Frag,
        node::Node,
        prefix::{
            prefix_cmp, prefix_cmp_encoded, prefix_decode, prefix_encode,
            prefix_reencode,
        },
        subscription::Subscriptions,
    },
    log::{debug, error, trace},
    pagecache::{
        debug::debug_delay, pin, Materializer, Measure, PageCache, PageId,
        RecoveryGuard, M,
    },
    serde::{Deserialize, Serialize},
};

type TreePtr<'g> = pagecache::PagePtr<'g, Frag>;

/// Allows arbitrary logic to be injected into mere operations of the
/// `PageCache`.
pub type MergeOperator = fn(
    key: &[u8],
    last_value: Option<&[u8]>,
    new_merge: &[u8],
) -> Option<Vec<u8>>;

//! `sled` is a flash-sympathetic persistent lock-free B+ tree.
//!
//! # Examples
//!
//! ```
//! use sled::{Db, IVec};
//!
//! let t = Db::open("my_db").unwrap();
//! t.insert(b"yo!", b"v1".to_vec());
//! assert_eq!(t.get(b"yo!"), Ok(Some(IVec::from(b"v1"))));
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
//! assert_eq!(iter.next().unwrap(), Ok((IVec::from(b"yo!"), IVec::from(b"v2"))));
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

mod batch;
mod binary_search;
mod context;
mod data;
mod db;
mod frag;
mod iter;
mod ivec;
mod dynamic_arc;
mod lazy_deserialized;
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
        lazy_deserialized::LazyDeserialized,
        dynamic_arc::DynamicArc,
        node::Node,
        prefix::{
            prefix_cmp, prefix_cmp_encoded, prefix_decode, prefix_encode,
            prefix_reencode,
        },
        subscription::Subscriptions,
    },
    log::{debug, error, trace},
    pagecache::{
        debug_delay, pin, Materializer, Measure, PageCache, PageId,
        RecoveryGuard, M,
    },
    serde::{Deserialize, Serialize},
};

type TreePtr<'g> = pagecache::PagePtr<'g, Frag>;

/// Allows arbitrary logic to be injected into mere operations of the `PageCache`.
pub type MergeOperator = fn(
    key: &[u8],
    last_value: Option<&[u8]>,
    new_merge: &[u8],
) -> Option<Vec<u8>>;

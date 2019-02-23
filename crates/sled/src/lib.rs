//! `sled` is a flash-sympathetic persistent lock-free B+ tree.
//!
//! # Examples
//!
//! ```
//! let t = sled::Db::start_default("my_db").unwrap();
//!
//! t.set(b"yo!", b"v1".to_vec());
//! assert!(t.get(b"yo!").unwrap().unwrap() == &*b"v1".to_vec());
//!
//! t.cas(
//!     b"yo!",                // key
//!     Some(b"v1"),           // old value, None for not present
//!     Some(b"v2".to_vec()),  // new value, None for delete
//! ).unwrap();
//!
//! let mut iter = t.scan(b"a non-present key before yo!");
//! // assert_eq!(iter.next(), Some(Ok((b"yo!".to_vec(), b"v2".to_vec()))));
//! // assert_eq!(iter.next(), None);
//!
//! t.del(b"yo!");
//! assert_eq!(t.get(b"yo!"), Ok(None));
//! ```

#![deny(missing_docs)]
#![cfg_attr(test, deny(clippy::warnings))]
#![cfg_attr(test, deny(clippy::bad_style))]
#![cfg_attr(test, deny(clippy::future_incompatible))]
#![cfg_attr(test, deny(clippy::nonstandard_style))]
#![cfg_attr(test, deny(clippy::rust_2018_compatibility))]
#![cfg_attr(test, deny(clippy::rust_2018_idioms))]

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
mod pinned_value;
mod prefix;
mod recovery;
mod subscription;
mod tree;

const META_PID: PageId = 0;
const COUNTER_PID: PageId = 1;

const DEFAULT_TREE_ID: &[u8] = b"__sled__default";

pub use {
    self::{
        db::Db,
        iter::Iter,
        pinned_value::PinnedValue,
        subscription::{Event, Subscriber},
        tree::Tree,
    },
    pagecache::{Config, ConfigBuilder, Error, Result},
};

use {
    self::{
        binary_search::{
            binary_search_gt, binary_search_lt, binary_search_lub,
            leaf_search,
        },
        context::Context,
        data::Data,
        frag::{ChildSplit, Frag, ParentSplit},
        ivec::IVec,
        materializer::BLinkMaterializer,
        node::Node,
        prefix::{
            prefix_cmp, prefix_cmp_encoded, prefix_decode,
            prefix_encode,
        },
        recovery::Recovery,
        subscription::Subscriptions,
    },
    log::{debug, error, trace},
    pagecache::{
        Materializer, Measure, MergeOperator, Meta, PageCache,
        PageGet, PageId, M,
    },
    serde::{Deserialize, Serialize},
    sled_sync::{debug_delay, pin, Guard},
};

type Key = Vec<u8>;
type Value = Vec<u8>;

type TreePtr<'g> = pagecache::PagePtr<'g, Frag>;

//! `sled` is a flash-sympathetic persistent lock-free B+ tree.
//!
//! # Examples
//!
//! ```
//! let t = sled::Tree::start_default("my_db").unwrap();
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
#![cfg_attr(test, deny(warnings))]
#![cfg_attr(test, deny(bad_style))]
#![cfg_attr(test, deny(future_incompatible))]
#![cfg_attr(test, deny(nonstandard_style))]
#![cfg_attr(test, deny(rust_2018_compatibility))]
#![cfg_attr(test, deny(rust_2018_idioms))]
#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]
#![cfg_attr(feature = "clippy", allow(inline_always))]

mod binary_search;
mod data;
mod frag;
mod iter;
mod materializer;
mod node;
mod pinned_value;
mod prefix;
mod recovery;
mod subscription;
mod tree;

pub use self::iter::{Iter, Keys, Values};
pub use self::pinned_value::PinnedValue;
pub use self::subscription::{Event, Subscriber};
/// atomic lock-free tree
pub use self::tree::Tree;

use pagecache::{
    Materializer, Measure, MergeOperator, PageCache, PageGet, PageId,
    M,
};

pub use pagecache::{Config, ConfigBuilder, Error, Result};

use self::{
    binary_search::{
        binary_search_gt, binary_search_lt, binary_search_lub,
        leaf_search,
    },
    data::Data,
    frag::{ChildSplit, ParentSplit},
    node::Node,
    prefix::{
        prefix_cmp, prefix_cmp_encoded, prefix_decode, prefix_encode,
    },
    recovery::Recovery,
    subscription::Subscriptions,
};

use sled_sync::{debug_delay, pin, Guard};

use log::{debug, error, trace};
use serde::{Deserialize, Serialize};

pub(crate) use self::frag::Frag;
pub(crate) use self::materializer::BLinkMaterializer;

type Key = Vec<u8>;
type KeyRef<'a> = &'a [u8];
type Value = Vec<u8>;

type TreePtr<'g> = pagecache::PagePtr<'g, Frag>;

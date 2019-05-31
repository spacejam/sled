//! `sled` is a flash-sympathetic persistent lock-free B+ tree.
//!
//! # Examples
//!
//! ```
//! use sled::{Db, IVec};
//!
//! let t = Db::start_default("my_db").unwrap();
//! t.set(b"yo!", b"v1".to_vec());
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
//! let mut iter = t.scan(b"a non-present key before yo!");
//! assert_eq!(iter.next().unwrap(), Ok((b"yo!".to_vec(), IVec::from(b"v2"))));
//! assert_eq!(iter.next(), None);
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
mod flusher;
mod frag;
mod iter;
mod ivec;
mod materializer;
mod meta;
mod node;
mod prefix;
mod subscription;
mod tree;
mod view;

const DEFAULT_TREE_ID: &[u8] = b"__sled__default";

pub use {
    self::{
        db::Db,
        iter::Iter,
        ivec::IVec,
        subscription::{Event, Subscriber},
        tree::Tree,
    },
    pagecache::{Config, ConfigBuilder, Error, Result},
};

use {
    self::{
        binary_search::binary_search_lub,
        context::Context,
        data::Data,
        frag::Frag,
        materializer::BLinkMaterializer,
        node::Node,
        prefix::{
            prefix_cmp, prefix_cmp_encoded, prefix_decode, prefix_encode,
            prefix_reencode,
        },
        subscription::Subscriptions,
        view::View,
    },
    log::{debug, error, trace},
    pagecache::{
        debug_delay, Materializer, Measure, MergeOperator, PageCache, PageId,
        Tx, M,
    },
    serde::{Deserialize, Serialize},
};

type TreePtr<'g> = pagecache::PagePtr<'g, Frag>;

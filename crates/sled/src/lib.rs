//! `sled` is a flash-sympathetic persistent lock-free B+ tree.
//!
//! # Examples
//!
//! ```
//! let config = sled::ConfigBuilder::new().temporary(true).build();
//!
//! let t = sled::Tree::start(config).unwrap();
//!
//! t.set(b"yo!".to_vec(), b"v1".to_vec());
//! assert_eq!(t.get(b"yo!"), Ok(Some(b"v1".to_vec())));
//!
//! t.cas(
//!     b"yo!".to_vec(),       // key
//!     Some(b"v1".to_vec()),  // old value, None for not present
//!     Some(b"v2".to_vec()),  // new value, None for delete
//! ).unwrap();
//!
//! let mut iter = t.scan(b"a non-present key before yo!");
//! assert_eq!(iter.next(), Some(Ok((b"yo!".to_vec(), b"v2".to_vec()))));
//! assert_eq!(iter.next(), None);
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
// TODO turn this on closer to the migration.
// #![cfg_attr(test, deny(rust_2018_idioms))]
#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]
#![cfg_attr(feature = "clippy", allow(inline_always))]

extern crate pagecache;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate crossbeam_epoch as epoch;
extern crate serde;
#[macro_use]
extern crate log as _log;

/// atomic lock-free tree
pub use tree::{Iter, Tree};

use pagecache::*;

pub use pagecache::{Config, ConfigBuilder, Error, Result};

mod tree;

type Key = Vec<u8>;
type KeyRef<'a> = &'a [u8];
type Value = Vec<u8>;

type TreePtr<'g> = pagecache::PagePtr<'g, tree::Frag>;

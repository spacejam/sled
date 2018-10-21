//! `pagecache` is a lock-free pagecache and log for building high-performance databases.
#![deny(missing_docs)]
#![cfg_attr(test, deny(warnings))]
#![cfg_attr(test, deny(bad_style))]
#![cfg_attr(test, deny(future_incompatible))]
#![cfg_attr(test, deny(nonstandard_style))]
#![cfg_attr(test, deny(rust_2018_compatibility))]
#![cfg_attr(test, deny(rust_2018_idioms))]
#![cfg_attr(test, deny(unused))]
#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]
#![cfg_attr(feature = "clippy", allow(inline_always))]
#![cfg_attr(feature = "nightly", feature(integer_atomics))]

#[cfg(all(not(feature = "nightly"), target_pointer_width = "32"))]
compile_error!(
    "32 bit architectures require a nightly compiler for now. \
     See https://github.com/spacejam/sled/issues/145"
);

#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate crossbeam_epoch as epoch;
extern crate historian;
extern crate serde;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log as _log;
#[cfg(unix)]
extern crate libc;
#[cfg(
    any(test, feature = "failpoints", feature = "lock_free_delays")
)]
extern crate rand;
#[cfg(feature = "rayon")]
extern crate rayon;
#[cfg(feature = "zstd")]
extern crate zstd;
#[cfg(feature = "failpoints")]
#[macro_use]
extern crate fail;
extern crate pagetable;
extern crate uptime_lib;

macro_rules! maybe_fail {
    ($e:expr) => {
        #[cfg(feature = "failpoints")]
        fail_point!($e, |_| Err(Error::FailPoint));
    };
}

macro_rules! rep_no_copy {
    ($e:expr; $n:expr) => {{
        let mut v = Vec::with_capacity($n);
        for _ in 0..$n {
            v.push($e);
        }
        v
    }};
}

mod blob_io;
mod config;
mod constants;
mod diskptr;
mod ds;
mod ebr;
mod hash;
mod iobuf;
mod iterator;
mod materializer;
mod metrics;
mod pagecache;
mod parallel_io;
mod periodic;
mod reader;
mod reservation;
mod result;
mod segment;
mod snapshot;
mod tx;
mod util;

pub mod log;

use std::cell::UnsafeCell;
use std::fmt::{self, Debug};
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use bincode::{deserialize, serialize};
use serde::de::DeserializeOwned;
use serde::Serialize;

use self::blob_io::{gc_blobs, read_blob, remove_blob, write_blob};
use self::ds::{node_from_frag_vec, Lru, Node, Stack, StackIter};
use self::ebr::pin_log;
use self::hash::{crc16_arr, crc64};
use self::iobuf::IoBufs;
use self::iterator::LogIter;
#[doc(hidden)]
use self::log::{
    MessageHeader, MessageKind, SegmentHeader, SegmentTrailer,
};
use self::metrics::{clock, measure};
use self::pagecache::{LoggedUpdate, Update};
use self::parallel_io::Pio;
use self::reader::LogReader;
use self::segment::{raw_segment_iter_from, SegmentAccountant};
use self::snapshot::{advance_snapshot, PageState};
use self::util::{arr_to_u32, arr_to_u64, u32_to_arr, u64_to_arr};

pub use self::config::{Config, ConfigBuilder};
pub use self::diskptr::DiskPtr;
pub use self::ebr::{pin, Guard};
pub use self::log::{Log, LogRead};
pub use self::materializer::{Materializer, NullMaterializer};
#[doc(hidden)]
pub use self::metrics::Measure;
pub use self::metrics::M;
pub use self::pagecache::{CacheEntry, PageCache, PageGet, PagePtr};
pub use self::reservation::Reservation;
pub use self::result::{Error, Result};
pub use self::segment::SegmentMode;
#[doc(hidden)]
pub use self::snapshot::{read_snapshot_or_default, Snapshot};
pub use self::tx::Tx;
pub use self::util::debug_delay;
#[doc(hidden)]
pub use constants::{
    BLOB_FLUSH, BLOB_INLINE_LEN, EVIL_BYTE, FAILED_FLUSH,
    INLINE_FLUSH, MSG_HEADER_LEN, SEGMENT_PAD, SEG_HEADER_LEN,
    SEG_TRAILER_LEN,
};

/// An offset for a storage file segment.
pub type SegmentId = usize;

/// A log file offset.
pub type LogId = u64;

/// A pointer to an blob blob.
pub type BlobPointer = Lsn;

/// A logical sequence number.
pub type Lsn = i64;

/// A page identifier.
pub type PageId = usize;

/// Allows arbitrary logic to be injected into mere operations of the `PageCache`.
pub type MergeOperator =
    fn(key: &[u8], last_value: Option<&[u8]>, new_merge: &[u8])
        -> Option<Vec<u8>>;

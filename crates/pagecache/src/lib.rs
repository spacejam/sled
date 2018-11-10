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
extern crate fs2;
extern crate historian;
extern crate serde;
extern crate sled_sync as sync;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log as _log;
#[cfg(unix)]
extern crate libc;
extern crate rayon;
#[cfg(feature = "zstd")]
extern crate zstd;
#[cfg(feature = "failpoints")]
#[macro_use]
extern crate fail;
extern crate pagetable;

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

#[cfg(feature = "event_log")]
/// The event log helps debug concurrency issues.
pub mod event_log;

pub mod log;

use std::{
    cell::UnsafeCell,
    fmt::{self, Debug},
    io,
    sync::atomic::AtomicUsize,
    sync::atomic::Ordering::SeqCst,
};

use bincode::{deserialize, serialize};
use serde::{de::DeserializeOwned, Serialize};

#[doc(hidden)]
use self::log::{
    MessageHeader, MessageKind, SegmentHeader, SegmentTrailer,
};

#[cfg(not(unix))]
use self::metrics::uptime;

use self::{
    blob_io::{gc_blobs, read_blob, remove_blob, write_blob},
    ds::{node_from_frag_vec, Lru, Node, Stack, StackIter},
    hash::{crc16_arr, crc64},
    iobuf::IoBufs,
    iterator::LogIter,
    metrics::{clock, measure},
    pagecache::{LoggedUpdate, Update},
    parallel_io::Pio,
    reader::LogReader,
    segment::{raw_segment_iter_from, SegmentAccountant},
    snapshot::{advance_snapshot, PageState},
    util::{arr_to_u32, arr_to_u64, u32_to_arr, u64_to_arr},
};

pub use self::{
    config::{Config, ConfigBuilder},
    diskptr::DiskPtr,
    log::{Log, LogRead},
    materializer::{Materializer, NullMaterializer},
    metrics::M,
    pagecache::{CacheEntry, PageCache, PageGet, PagePtr},
    reservation::Reservation,
    result::{Error, Result},
    segment::SegmentMode,
    sync::{debug_delay, pin, unprotected, Guard},
    tx::Tx,
};

#[doc(hidden)]
pub use self::{
    constants::{
        BLOB_FLUSH, BLOB_INLINE_LEN, EVIL_BYTE, FAILED_FLUSH,
        INLINE_FLUSH, MINIMUM_ITEMS_PER_SEGMENT, MSG_HEADER_LEN,
        SEGMENT_PAD, SEG_HEADER_LEN, SEG_TRAILER_LEN,
    },
    metrics::Measure,
    snapshot::{read_snapshot_or_default, Snapshot},
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

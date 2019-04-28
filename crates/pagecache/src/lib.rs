//! `pagecache` is a lock-free pagecache and log for building high-performance databases.
#![deny(missing_docs)]
#![cfg_attr(test, deny(clippy::warnings))]
#![cfg_attr(test, deny(clippy::bad_style))]
#![cfg_attr(test, deny(clippy::future_incompatible))]
#![cfg_attr(test, deny(clippy::nonstandard_style))]
#![cfg_attr(test, deny(clippy::rust_2018_compatibility))]
#![cfg_attr(test, deny(clippy::rust_2018_idioms))]
#![cfg_attr(test, deny(clippy::unused))]

#[cfg(feature = "failpoints")]
use fail::fail_point;

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
mod iobuf;
mod iterator;
mod map;
mod materializer;
mod meta;
mod metrics;
mod pagecache;
mod parallel_io;
mod reader;
mod reservation;
mod result;
mod segment;
mod snapshot;
mod tx;
mod util;

#[cfg(feature = "measure_allocs")]
mod measure_allocs;

const META_PID: PageId = 0;
const COUNTER_PID: PageId = 1;

#[cfg(feature = "measure_allocs")]
#[global_allocator]
static ALLOCATOR: measure_allocs::TrackingAllocator =
    measure_allocs::TrackingAllocator;

#[cfg(feature = "event_log")]
/// The event log helps debug concurrency issues.
pub mod event_log;

pub mod logger;

use std::{
    cell::UnsafeCell,
    convert::TryFrom,
    fmt::{self, Debug},
    io,
    sync::atomic::{AtomicI64 as AtomicLsn, AtomicU64, Ordering::SeqCst},
};

use bincode::{deserialize, serialize};
use lazy_static::lazy_static;
use log::{debug, error, trace, warn};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[doc(hidden)]
use self::logger::{MessageHeader, MessageKind, SegmentHeader, SegmentTrailer};

#[cfg(not(unix))]
use self::metrics::uptime;

use self::{
    blob_io::{gc_blobs, read_blob, remove_blob, write_blob},
    iobuf::IoBufs,
    iterator::LogIter,
    metrics::{clock, measure},
    pagecache::{LoggedUpdate, Update},
    parallel_io::Pio,
    reader::LogReader,
    segment::{raw_segment_iter_from, SegmentAccountant},
    snapshot::{advance_snapshot, PageState},
    util::{arr_to_u32, arr_to_u64, maybe_decompress, u32_to_arr, u64_to_arr},
};

pub use self::{
    config::{Config, ConfigBuilder},
    diskptr::DiskPtr,
    ds::{node_from_frag_vec, Lru, Node, PageTable, Stack, StackIter, VecSet},
    logger::{Log, LogRead},
    map::{FastMap1, FastMap4, FastMap8, FastSet1, FastSet4, FastSet8},
    materializer::{Materializer, NullMaterializer},
    meta::Meta,
    metrics::M,
    pagecache::{CacheEntry, PageCache, PageGet, PagePtr},
    reservation::Reservation,
    result::{CasResult, Error, Result},
    segment::SegmentMode,
    tx::Tx,
};

#[doc(hidden)]
pub use self::{
    constants::{
        BATCH_MANIFEST, BATCH_MANIFEST_INLINE_LEN, BLOB_FLUSH, BLOB_INLINE_LEN,
        EVIL_BYTE, FAILED_FLUSH, INLINE_FLUSH, MINIMUM_ITEMS_PER_SEGMENT,
        MSG_HEADER_LEN, SEGMENT_PAD, SEG_HEADER_LEN, SEG_TRAILER_LEN,
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
pub type PageId = u64;

/// Allows arbitrary logic to be injected into mere operations of the `PageCache`.
pub type MergeOperator = fn(
    key: &[u8],
    last_value: Option<&[u8]>,
    new_merge: &[u8],
) -> Option<Vec<u8>>;

pub(crate) fn crc32(buf: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&buf);
    hasher.finalize()
}

#[cfg(any(test, feature = "lock_free_delays"))]
mod debug_delay;

#[cfg(any(test, feature = "lock_free_delays"))]
pub use self::debug_delay::debug_delay;

/// This function is useful for inducing random jitter into our atomic
/// operations, shaking out more possible interleavings quickly. It gets
/// fully elliminated by the compiler in non-test code.
#[cfg(not(any(test, feature = "lock_free_delays")))]
pub fn debug_delay() {}

pub use crossbeam_epoch::{
    pin, unprotected, Atomic, Collector, CompareAndSetError, Guard,
    LocalHandle, Owned, Shared,
};

pub use crossbeam_utils::{Backoff, CachePadded};

fn assert_usize<T>(from: T) -> usize
where
    usize: std::convert::TryFrom<T, Error = std::num::TryFromIntError>,
{
    usize::try_from(from).expect("lost data cast while converting to usize")
}

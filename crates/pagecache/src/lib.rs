//! `pagecache` is a lock-free pagecache and log for building high-performance
//! databases.
#![cfg_attr(test, deny(warnings))]

#![deny(missing_docs)]
#![deny(future_incompatible)]
#![deny(nonstandard_style)]
#![deny(rust_2018_compatibility)]
#![deny(rust_2018_idioms)]

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cognitive_complexity)]
#![allow(clippy::default_trait_access)]
#![allow(clippy::integer_division)]
#![allow(clippy::module_name_repetitions)]

#[cfg(feature = "failpoints")]
use fail::fail_point;

macro_rules! maybe_fail {
    ($e:expr) => {
        #[cfg(feature = "failpoints")]
        fail_point!($e, |_| Err(Error::FailPoint));
    };
}

mod blob_io;
mod config;
mod constants;
/// Debug helps test concurrent issues with random jittering and another instruments.
pub mod debug;
mod diskptr;
mod ds;
mod iobuf;
mod iterator;
mod lazy;
mod map;
mod materializer;
mod meta;
mod metrics;
mod pagecache;
mod parallel_io;
mod promise;
mod reader;
mod reservation;
mod result;
mod segment;
mod snapshot;
mod threadpool;
mod util;

#[cfg(feature = "measure_allocs")]
mod measure_allocs;

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
    sync::atomic::{
        AtomicI64 as AtomicLsn, AtomicU64,
        Ordering::{Acquire, Relaxed, Release, SeqCst},
    },
};

use bincode::{deserialize, serialize};
use log::{debug, error, trace, warn};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[doc(hidden)]
use self::logger::{MessageHeader, SegmentHeader};

use self::{
    blob_io::{gc_blobs, read_blob, remove_blob, write_blob},
    config::PersistedConfig,
    constants::{BATCH_MANIFEST_PID, CONFIG_PID, COUNTER_PID, META_PID},
    iobuf::{IoBuf, IoBufs},
    iterator::{raw_segment_iter_from, LogIter},
    metrics::{clock, measure},
    pagecache::Update,
    parallel_io::Pio,
    reader::LogReader,
    segment::SegmentAccountant,
    snapshot::{advance_snapshot, PageState},
    util::{arr_to_u32, arr_to_u64, maybe_decompress, u32_to_arr, u64_to_arr},
};

pub use self::{
    config::{Config, ConfigBuilder},
    diskptr::DiskPtr,
    ds::{node_from_frag_vec, Lru, Node, PageTable, Stack, StackIter, VecSet},
    lazy::Lazy,
    logger::{Log, LogRead},
    map::{FastMap1, FastMap4, FastMap8, FastSet1, FastSet4, FastSet8},
    materializer::Materializer,
    meta::Meta,
    metrics::M,
    pagecache::{PageCache, PagePtr, RecoveryGuard},
    promise::{Promise, PromiseFiller},
    reservation::Reservation,
    result::{CasResult, Error, Result},
    segment::SegmentMode,
};

#[doc(hidden)]
pub use self::{
    constants::{
        BATCH_MANIFEST_INLINE_LEN, BLOB_INLINE_LEN, MAX_SPACE_AMPLIFICATION,
        MINIMUM_ITEMS_PER_SEGMENT, MSG_HEADER_LEN, SEG_HEADER_LEN,
    },
    ds::PAGETABLE_NODE_SZ,
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

#[doc(hidden)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum MessageKind {
    /// The EVIL_BYTE is written as a canary to help
    /// detect torn writes.
    Corrupted = 0,
    /// Indicates that the following buffer corresponds
    /// to a reservation for an in-memory operation that
    /// failed to complete. It should be skipped during
    /// recovery.
    Cancelled = 1,
    /// Indicates that the following buffer is used
    /// as padding to fill out the rest of the segment
    /// before sealing it.
    Pad = 2,
    /// Indicates that the following buffer contains
    /// an Lsn for the last write in an atomic writebatch.
    BatchManifest = 3,
    /// Indicates that this page was freed from the pagetable.
    Free = 4,
    /// Indicates that the last persisted ID was at least
    /// this high.
    Counter = 5,
    /// The meta page, stored inline
    InlineMeta = 6,
    /// The meta page, stored blobly
    BlobMeta = 7,
    /// The config page, stored inline
    InlineConfig = 8,
    /// The config page, stored blobly
    BlobConfig = 9,
    /// A consolidated page replacement, stored inline
    InlineReplace = 10,
    /// A consolidated page replacement, stored blobly
    BlobReplace = 11,
    /// A partial page update, stored inline
    InlineAppend = 12,
    /// A partial page update, stored blobly
    BlobAppend = 13,
}

impl MessageKind {
    pub(crate) const fn into(self) -> u8 {
        self as u8
    }
}

impl From<u8> for MessageKind {
    fn from(byte: u8) -> Self {
        use MessageKind::*;
        match byte {
            0 => Corrupted,
            1 => Cancelled,
            2 => Pad,
            3 => BatchManifest,
            4 => Free,
            5 => Counter,
            6 => InlineMeta,
            7 => BlobMeta,
            8 => InlineConfig,
            9 => BlobConfig,
            10 => InlineReplace,
            11 => BlobReplace,
            12 => InlineAppend,
            13 => BlobAppend,
            other => {
                debug!("encountered unexpected message kind byte {}", other);
                Corrupted
            }
        }
    }
}

/// The high-level types of stored information
/// about pages and their mutations
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogKind {
    /// Persisted data containing a page replacement
    Replace,
    /// Persisted immutable update
    Append,
    /// Freeing of a page
    Free,
    /// Some state indicating this should be skipped
    Skip,
    /// Unexpected corruption
    Corrupted,
}

fn log_kind_from_update<PageFrag>(update: &Update<PageFrag>) -> LogKind
where
    PageFrag: DeserializeOwned + Serialize,
{
    use Update::*;

    match update {
        Update::Free => LogKind::Free,
        Append(..) => LogKind::Append,
        Compact(..) | Counter(..) | Meta(..) | Config(..) => LogKind::Replace,
    }
}

impl From<MessageKind> for LogKind {
    fn from(kind: MessageKind) -> Self {
        use MessageKind::*;
        match kind {
            Free => LogKind::Free,

            InlineReplace | Counter | BlobReplace | InlineMeta | BlobMeta
            | InlineConfig | BlobConfig => LogKind::Replace,

            InlineAppend | BlobAppend => LogKind::Append,

            Cancelled | Pad | BatchManifest => LogKind::Skip,
            other => {
                debug!("encountered unexpected message kind byte {:?}", other);
                LogKind::Corrupted
            }
        }
    }
}

pub(crate) fn crc32(buf: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&buf);
    hasher.finalize()
}

use self::debug::debug_delay;

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

// TODO remove this when atomic fetch_max stabilizes in #48655
fn bump_atomic_lsn(atomic_lsn: &AtomicLsn, to: Lsn) {
    let mut current = atomic_lsn.load(SeqCst);
    loop {
        if current >= to {
            return;
        }
        let last = atomic_lsn.compare_and_swap(current, to, SeqCst);
        if last == current {
            // we succeeded.
            return;
        }
        current = last;
    }
}

fn pagecache_crate_version() -> (usize, usize) {
    let vsn = env!("CARGO_PKG_VERSION");
    let mut parts = vsn.split('.');
    let major = parts.next().unwrap().parse().unwrap();
    let minor = parts.next().unwrap().parse().unwrap();
    (major, minor)
}

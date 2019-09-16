//! `pagecache` is a lock-free pagecache and log for building high-performance
//! databases.
#![cfg_attr(test, deny(warnings))]
#![deny(missing_docs)]
#![deny(future_incompatible)]
#![deny(nonstandard_style)]
#![deny(rust_2018_compatibility)]
#![deny(rust_2018_idioms)]

pub mod constants;
pub mod logger;

mod blob_io;
mod diskptr;
mod iobuf;
mod iterator;
mod materializer;
mod pagecache;
mod parallel_io;
mod reader;
mod reservation;
mod segment;
mod snapshot;

#[cfg(feature = "measure_allocs")]
mod measure_allocs;

#[cfg(feature = "measure_allocs")]
#[global_allocator]
static ALLOCATOR: measure_allocs::TrackingAllocator =
    measure_allocs::TrackingAllocator;

use crate::{debug, DeserializeOwned, AtomicLsn, Serialize, SeqCst};

use self::{
    blob_io::{gc_blobs, read_blob, remove_blob, write_blob},
    constants::{BATCH_MANIFEST_PID, CONFIG_PID, COUNTER_PID, META_PID},
    iobuf::{IoBuf, IoBufs},
    iterator::{raw_segment_iter_from, LogIter},
    pagecache::Update,
    parallel_io::Pio,
    segment::SegmentAccountant,
    snapshot::{advance_snapshot},
};

pub(crate) use self::{
    reader::{read_segment_header, read_message},
    logger::{MessageHeader, SegmentHeader},
    reservation::Reservation,
    snapshot::{read_snapshot_or_default, Snapshot, PageState},
};

pub use self::{
    diskptr::DiskPtr,
    segment::SegmentMode,
    pagecache::{PageCache, PagePtr, RecoveryGuard},
    materializer::Materializer,
    logger::{LogRead, Log},
    constants::{
        BATCH_MANIFEST_INLINE_LEN, BLOB_INLINE_LEN, MAX_SPACE_AMPLIFICATION,
        MINIMUM_ITEMS_PER_SEGMENT, MSG_HEADER_LEN, SEG_HEADER_LEN,
    },
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

/// A byte used to disambiguate log message types
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum MessageKind {
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

use std::convert::{TryFrom, TryInto};

#[cfg(feature = "compression")]
use zstd::block::decompress;

#[inline]
pub(crate) fn u64_to_arr(number: u64) -> [u8; 8] {
    number.to_le_bytes()
}

#[inline]
pub(crate) fn arr_to_u64(arr: &[u8]) -> u64 {
    arr.try_into().map(u64::from_le_bytes).unwrap()
}

#[inline]
pub(crate) fn arr_to_u32(arr: &[u8]) -> u32 {
    arr.try_into().map(u32::from_le_bytes).unwrap()
}

#[inline]
pub(crate) fn u32_to_arr(number: u32) -> [u8; 4] {
    number.to_le_bytes()
}

pub(crate) fn maybe_decompress(buf: Vec<u8>) -> std::io::Result<Vec<u8>> {
    #[cfg(feature = "compression")]
    {
        use std::sync::atomic::AtomicUsize;

        use super::*;

        static MAX_COMPRESSION_RATIO: AtomicUsize = AtomicUsize::new(1);
        use std::sync::atomic::Ordering::{Acquire, Release};

        let _measure = Measure::new(&M.decompress);
        loop {
            let ratio = MAX_COMPRESSION_RATIO.load(Acquire);
            match decompress(&*buf, buf.len() * ratio) {
                Err(ref e) if e.kind() == std::io::ErrorKind::Other => {
                    debug!(
                        "bumping expected compression \
                         ratio up from {} to {}: {:?}",
                        ratio,
                        ratio + 1,
                        e
                    );
                    MAX_COMPRESSION_RATIO.compare_and_swap(
                        ratio,
                        ratio + 1,
                        Release,
                    );
                }
                other => return other,
            }
        }
    }

    #[cfg(not(feature = "compression"))]
    Ok(buf)
}

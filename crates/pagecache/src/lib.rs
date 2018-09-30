//! `pagecache` is a lock-free pagecache and log for building high-performance databases.
#![deny(missing_docs)]
#![cfg_attr(test, deny(warnings))]
#![cfg_attr(test, deny(bad_style))]
#![cfg_attr(test, deny(future_incompatible))]
#![cfg_attr(test, deny(nonstandard_style))]
#![cfg_attr(test, deny(rust_2018_compatibility))]
// TODO turn this on closer to the migration.
// #![cfg_attr(test, deny(rust_2018_idioms))]
#![cfg_attr(test, deny(unused))]
#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]
#![cfg_attr(feature = "clippy", allow(inline_always))]
#![cfg_attr(feature = "nightly", feature(integer_atomics))]
#[cfg(all(not(feature = "nightly"), target_pointer_width = "32"))]
compile_error!(
    "32 bit architectures require a nightly compiler for now.
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

use ds::Stack;

/// general-purpose configuration
pub use config::{Config, ConfigBuilder};
pub use diskptr::DiskPtr;

#[doc(hidden)]
pub use self::log::{
    MSG_HEADER_LEN, SEG_HEADER_LEN, SEG_TRAILER_LEN,
};

pub use result::{Error, Result};
pub use tx::Tx;

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

pub mod log;

use ds::*;
use hash::{crc16_arr, crc64};
use historian::Histo;
use metrics::Metrics;

use std::cell::UnsafeCell;
use std::fmt::{self, Debug};
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use bincode::{deserialize, serialize};
use serde::de::DeserializeOwned;
use serde::Serialize;
/// An offset for a storage file segment.
pub type SegmentID = usize;

/// A log file offset.
pub type LogID = u64;

/// A pointer to an external blob.
pub type ExternalPointer = Lsn;

/// A logical sequence number.
pub type Lsn = i64;

/// A page identifier.
pub type PageID = usize;

type PagePtrInner<'g, P> = epoch::Shared<'g, Node<CacheEntry<P>>>;

/// A pointer to shared lock-free state bound by a pinned epoch's lifetime.
#[derive(Debug, Clone, PartialEq)]
pub struct PagePtr<'g, P>(PagePtrInner<'g, P>)
where
    P: 'static + Send;

impl<'g, P> PagePtr<'g, P>
where
    P: 'static + Send,
{
    /// Create a null `PagePtr`
    pub fn allocated() -> PagePtr<'g, P> {
        PagePtr(epoch::Shared::null())
    }

    /// Whether this pointer is Allocated
    pub fn is_allocated(&self) -> bool {
        self.0.is_null()
    }
}

unsafe impl<'g, P> Send for PagePtr<'g, P> where P: Send {}
unsafe impl<'g, P> Sync for PagePtr<'g, P> where P: Send + Sync {}

lazy_static! {
    /// A metric collector for all pagecache users running in this
    /// process.
    pub static ref M: Metrics = Metrics::default();
}

fn clock() -> f64 {
    let u = uptime();
    (u.as_secs() * 1_000_000_000) as f64 + f64::from(u.subsec_nanos())
}

// not correct, since it starts counting at the first observance...
fn uptime() -> std::time::Duration {
    lazy_static! {
        static ref START: std::time::Instant =
            std::time::Instant::now();
    }

    START.elapsed()
}

/// Measure the duration of an event, and call `Histo::measure()`.
struct Measure<'h> {
    histo: &'h Histo,
    start: f64,
}

impl<'h> Measure<'h> {
    /// The time delta from ctor to dtor is recorded in `histo`.
    #[inline(always)]
    pub fn new(histo: &'h Histo) -> Measure<'h> {
        Measure {
            histo,
            start: clock(),
        }
    }
}

impl<'h> Drop for Measure<'h> {
    #[inline(always)]
    fn drop(&mut self) {
        self.histo.measure(clock() - self.start);
    }
}

/// Measure the time spent on calling a given function in a given `Histo`.
#[inline(always)]
fn measure<F: FnOnce() -> R, R>(histo: &Histo, f: F) -> R {
    let _measure = Measure::new(histo);
    f()
}

/// This function is useful for inducing random jitter into our atomic
/// operations, shaking out more possible interleavings quickly. It gets
/// fully elliminated by the compiler in non-test code.
#[inline(always)]
pub fn debug_delay() {
    #[cfg(any(test, feature = "lock_free_delays"))]
    {
        use rand::{thread_rng, Rng};

        if thread_rng().gen_bool(1. / 1000.) {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

/// Allows arbitrary logic to be injected into mere operations of the `PageCache`.
pub type MergeOperator =
    fn(key: &[u8], last_value: Option<&[u8]>, new_merge: &[u8])
        -> Option<Vec<u8>>;

fn u64_to_arr(u: u64) -> [u8; 8] {
    [
        u as u8,
        (u >> 8) as u8,
        (u >> 16) as u8,
        (u >> 24) as u8,
        (u >> 32) as u8,
        (u >> 40) as u8,
        (u >> 48) as u8,
        (u >> 56) as u8,
    ]
}

fn arr_to_u64(arr: [u8; 8]) -> u64 {
    arr[0] as u64
        + ((arr[1] as u64) << 8)
        + ((arr[2] as u64) << 16)
        + ((arr[3] as u64) << 24)
        + ((arr[4] as u64) << 32)
        + ((arr[5] as u64) << 40)
        + ((arr[6] as u64) << 48)
        + ((arr[7] as u64) << 56)
}

fn arr_to_u32(arr: [u8; 4]) -> u32 {
    arr[0] as u32
        + ((arr[1] as u32) << 8)
        + ((arr[2] as u32) << 16)
        + ((arr[3] as u32) << 24)
}

fn u32_to_arr(u: u32) -> [u8; 4] {
    [u as u8, (u >> 8) as u8, (u >> 16) as u8, (u >> 24) as u8]
}

use self::log::EXTERNAL_VALUE_LEN;

pub use self::log::LogRead;

use self::blob_io::{gc_blobs, read_blob, remove_blob, write_blob};

use self::reader::LogReader;

#[doc(hidden)]
pub use self::snapshot::{read_snapshot_or_default, Snapshot};

#[doc(hidden)]
use self::log::{
    MessageHeader, MessageKind, SegmentHeader, SegmentTrailer,
};

pub use self::log::Log;
pub use self::materializer::{Materializer, NullMaterializer};
pub use self::pagecache::{CacheEntry, PageCache, PageGet};
pub use self::reservation::Reservation;
pub use self::segment::SegmentMode;

use self::iobuf::IoBufs;
use self::iterator::LogIter;
use self::pagecache::{LoggedUpdate, Update};
use self::parallel_io::Pio;
use self::segment::{raw_segment_iter_from, SegmentAccountant};
use self::snapshot::{advance_snapshot, PageState};

// This message should be skipped to preserve linearizability.
const FAILED_FLUSH: u8 = 0;

// This message represents valid data, stored inline.
const SUCCESSFUL_FLUSH: u8 = 1;

// This message represents valid data, stored externally.
const SUCCESSFUL_EXTERNAL_FLUSH: u8 = 2;

// This message represents a pad.
const SEGMENT_PAD: u8 = 3;

// The EVIL_BYTE is written to force detection of
// a corruption when dealing with unused segment space.
const EVIL_BYTE: u8 = 6;
